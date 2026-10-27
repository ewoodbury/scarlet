package com.ewoodbury.scarlet.columnar

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec

/**
 * Runs one worker per morsel. Each thread takes indexes from its own deque and steals from the
 * others, so one long morsel does not keep the rest of the queue behind it.
 *
 * Results line up with `morsels`. A worker failure, including an `Error`, fails the call with that
 * throwable. `parallelism` of `1` runs on the calling thread. Each morsel handed to the worker
 * counts as one visit on the installed [[ColumnMetrics]] sink.
 */
object MorselScheduler:

  def map[M, A](morsels: Vector[M], parallelism: Int)(
      worker: (Int, M) => A,
  ): Vector[A] =
    dispatch(morsels, parallelism, byThread = false, (_, index, morsel) => worker(index, morsel))

  /**
   * Same schedule as [[map]]. The worker receives the thread id instead of the morsel index.
   *
   * Ids run from 0 until `min(parallelism, morsels.length)`. One thread keeps one id for every
   * morsel it runs, including a morsel it steals. `parallelism` of 1, or a single morsel, uses id
   * 0 on the calling thread. The call joins before it returns, so a buffer written by id can be
   * read afterwards with plain stores.
   */
  def mapByThread[M, A](morsels: Vector[M], parallelism: Int)(
      worker: (Int, M) => A,
  ): Vector[A] =
    dispatch(morsels, parallelism, byThread = true, (id, _, morsel) => worker(id, morsel))

  private def dispatch[M, A](
      morsels: Vector[M],
      parallelism: Int,
      byThread: Boolean,
      worker: (Int, Int, M) => A,
  ): Vector[A] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (morsels.isEmpty) then Vector.empty
    else if (parallelism == 1 || morsels.length == 1) then
      morsels.iterator.zipWithIndex.map { (morsel, index) =>
        ColumnMetrics.visit()
        val id = if (byThread) then 0 else index
        worker(id, index, morsel)
      }.toVector
    else
      val workers = math.min(parallelism, morsels.length)
      val deques = Array.fill(workers)(new ConcurrentLinkedDeque[Int])
      morsels.indices.foreach(index => deques(index % workers).addLast(index))
      val slots = Array.fill(morsels.length)(new AtomicReference[Option[A]](None))
      val failure = new AtomicReference[Option[Throwable]](None)
      val threads = Array.tabulate(workers) { id =>
        val thread = new Thread(
          () => drain(id, deques, slots, failure, morsels, worker),
          s"scarlet-morsel-$id",
        )
        thread.setDaemon(true)
        thread
      }
      threads.foreach(_.start())
      threads.foreach(_.join())
      failure.get() match
        case Some(thrown) => throw thrown
        case None => ()
      slots.iterator.map { slot =>
        slot.get() match
          case Some(value) => value
          case None => throw new IllegalStateException("morsel finished without a result")
      }.toVector

  @tailrec
  private def drain[M, A](
      id: Int,
      deques: Array[ConcurrentLinkedDeque[Int]],
      slots: Array[AtomicReference[Option[A]]],
      failure: AtomicReference[Option[Throwable]],
      morsels: Vector[M],
      worker: (Int, Int, M) => A,
  ): Unit =
    if (failure.get().isEmpty) then
      take(id, deques, failure) match
        case None => ()
        case Some(index) =>
          val morsel = morsels.lift(index).getOrElse {
            throw new IllegalArgumentException(s"missing morsel $index")
          }
          try
            ColumnMetrics.visit()
            slots(index).set(Some(worker(id, index, morsel)))
          catch case thrown: Throwable => failure.compareAndSet(None, Some(thrown))
          drain(id, deques, slots, failure, morsels, worker)

  private def take(
      id: Int,
      deques: Array[ConcurrentLinkedDeque[Int]],
      failure: AtomicReference[Option[Throwable]],
  ): Option[Int] =
    if (failure.get().nonEmpty) then None
    else Option(deques(id).pollFirst()).orElse(steal(id, deques))

  private def steal(id: Int, deques: Array[ConcurrentLinkedDeque[Int]]): Option[Int] =
    deques.indices.iterator
      .filter(_ != id)
      .map(other => Option(deques(other).pollLast()))
      .collectFirst { case Some(index) => index }
