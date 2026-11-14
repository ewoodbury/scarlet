package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{
  ConcurrentHashMap,
  ConcurrentLinkedDeque,
  CountDownLatch,
  ForkJoinPool,
}

import scala.annotation.tailrec

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Runs morsels on a process-wide [[ForkJoinPool]]. Each parallel dispatch queues one index per
 * morsel, striped across `min(parallelism, morsels.length)` deques, and submits one pool task per
 * deque. A task takes indexes from its own deque and steals from the tails of the others, so one
 * long morsel does not keep the rest of that deque behind it. The dispatch waits on its own latch.
 *
 * The pool is created on the first parallel dispatch, sized to `threadPoolSize`, and then reused.
 * A later change to that setting does not resize it. Production callers pass the same value as
 * `parallelism`. Workers are daemon threads named `scarlet-morsel-$id`. There is no shutdown hook
 * and no `close`. A hook would have nothing to release, and shutting the pool down would reject a
 * dispatch that runs after an action returns. Daemon threads do not keep the JVM alive.
 *
 * Two dispatches may run at once. They share the workers and nothing else: each has its own
 * deques, result slots, failure, and thread ids. A dispatch does not take a process-wide lock.
 *
 * Re-entrancy: a dispatch whose caller is already a worker of this pool runs inline on that
 * worker, in morsel order. It does not submit, and it does not wait for a free worker. The pool
 * would compensate if that worker blocked inside the pool, but the nested call must not need the
 * pool to be free. [[mapByThread]] reports id 0 for every morsel of that nested dispatch. Those
 * morsels share the outer morsel's thread locals.
 *
 * Results line up with `morsels`. A worker failure, including an `Error`, fails the call with that
 * throwable. The first one wins, and the other workers stop taking morsels once it is recorded.
 * `parallelism` of 1 or below, or a single morsel, runs on the calling thread and does not touch
 * the pool. Each morsel handed to a worker counts as one visit on the installed [[ColumnMetrics]]
 * sink.
 *
 * [[mapByThread]] ids run from 0 until `min(parallelism, morsels.length)`. A pool thread records
 * its id on the dispatch's `Thread -> id` map the first time it takes a slot, and keeps that id
 * for every morsel it runs in the dispatch, including one it steals and one from a later slot. The
 * same thread in another dispatch reads that dispatch's map, so it can carry a different id. A
 * nested inline dispatch does not consult the map.
 *
 * A pool worker drops its [[HashAggregate]] sum holder and its [[LongHashReduce]] table when its
 * task for the dispatch returns. The thread now outlives the call, and those holders are keyed by
 * the thread. Within the task the holder stays, including across a nested inline dispatch and a
 * morsel the worker steals. The inline path does not drop it: the outer morsel may still read it
 * after the nested call returns.
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
   * 0 on the calling thread. A nested dispatch on a pool worker also uses id 0, on that same
   * worker. The call joins before it returns, so a buffer written by id can be read afterwards
   * with plain stores.
   */
  def mapByThread[M, A](morsels: Vector[M], parallelism: Int)(
      worker: (Int, M) => A,
  ): Vector[A] =
    dispatch(morsels, parallelism, byThread = true, (id, _, morsel) => worker(id, morsel))

  private val nextWorkerId = new AtomicInteger(0)

  private val factory: ForkJoinPool.ForkJoinWorkerThreadFactory =
    (target: ForkJoinPool) =>
      Option(ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(target)) match
        case Some(created) =>
          val workerId = nextWorkerId.getAndIncrement()
          created.setDaemon(true)
          created.setName(s"scarlet-morsel-$workerId")
          created
        case None =>
          throw new IllegalStateException("fork join pool did not create a worker")

  /** Backstop for a throwable that escapes a task. Tasks record their own failures first. */
  private val uncaught: Thread.UncaughtExceptionHandler =
    (thread: Thread, thrown: Throwable) =>
      Option(thread.getThreadGroup).foreach(_.uncaughtException(thread, thrown))

  /**
   * Sized once, from `threadPoolSize` at the first parallel dispatch. `asyncMode` is the FIFO mode
   * for tasks that are never joined: this pool waits on a latch, and a later dispatch should not
   * bury an earlier one under a stack.
   */
  private lazy val pool: ForkJoinPool =
    val size = ScarletConf.get.threadPoolSize
    require(size > 0, s"thread pool size must be positive, got $size")
    new ForkJoinPool(size, factory, uncaught, true)

  /** True on a thread that is inside a pool task. Nested dispatch reads this and stays inline. */
  private val onPool: ThreadLocal[Boolean] = new ThreadLocal[Boolean]:
    override def initialValue(): Boolean = false

  private def dispatch[M, A](
      morsels: Vector[M],
      parallelism: Int,
      byThread: Boolean,
      worker: (Int, Int, M) => A,
  ): Vector[A] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (morsels.isEmpty) then Vector.empty
    else if (parallelism <= 1 || morsels.length == 1 || onPool.get()) then
      onCaller(morsels, byThread, worker)
    else
      val width = math.min(parallelism, morsels.length)
      run(width, morsels, worker)

  private def onCaller[M, A](
      morsels: Vector[M],
      byThread: Boolean,
      worker: (Int, Int, M) => A,
  ): Vector[A] =
    morsels.iterator.zipWithIndex.map { (morsel, index) =>
      ColumnMetrics.visit()
      val id = if (byThread) then 0 else index
      worker(id, index, morsel)
    }.toVector

  private def run[M, A](
      width: Int,
      morsels: Vector[M],
      worker: (Int, Int, M) => A,
  ): Vector[A] =
    val deques = Array.fill(width)(new ConcurrentLinkedDeque[Int])
    morsels.indices.foreach(index => deques(index % width).addLast(index))
    val slots = Array.fill(morsels.length)(new AtomicReference[Option[A]](None))
    val failure = new AtomicReference[Option[Throwable]](None)
    val ids = new ConcurrentHashMap[Thread, Integer]
    val done = new CountDownLatch(width)
    (0 until width).foreach { slot =>
      val task: Runnable = () =>
        onPool.set(true)
        try
          val id = claim(ids, slot)
          try drain(slot, id, deques, slots, failure, morsels, worker)
          catch
            case thrown: Throwable =>
              val _ = failure.compareAndSet(None, Some(thrown))
          finally
            HashAggregate.releaseSumWorkers()
            LongHashReduce.releaseWorkers()
        finally
          onPool.set(false)
          done.countDown()
      pool.execute(task)
    }
    await(done)
    failure.get() match
      case Some(thrown) => throw thrown
      case None =>
        slots.iterator.map { slotRef =>
          slotRef.get() match
            case Some(value) => value
            case None => throw new IllegalStateException("morsel finished without a result")
        }.toVector

  /** First slot this thread takes in the dispatch. A later slot on the same thread keeps it. */
  private def claim(ids: ConcurrentHashMap[Thread, Integer], slot: Int): Int =
    Option(ids.putIfAbsent(Thread.currentThread, Integer.valueOf(slot))) match
      case Some(existing) => existing.intValue
      case None => slot

  private def await(done: CountDownLatch): Unit =
    try done.await()
    catch
      case _: InterruptedException =>
        await(done)
        Thread.currentThread.interrupt()

  @tailrec
  private def drain[M, A](
      slot: Int,
      id: Int,
      deques: Array[ConcurrentLinkedDeque[Int]],
      slots: Array[AtomicReference[Option[A]]],
      failure: AtomicReference[Option[Throwable]],
      morsels: Vector[M],
      worker: (Int, Int, M) => A,
  ): Unit =
    if (failure.get().isEmpty) then
      take(slot, deques, failure) match
        case None => ()
        case Some(index) =>
          val morsel = morsels.lift(index).getOrElse {
            throw new IllegalArgumentException(s"missing morsel $index")
          }
          try
            ColumnMetrics.visit()
            slots(index).set(Some(worker(id, index, morsel)))
          catch case thrown: Throwable => failure.compareAndSet(None, Some(thrown))
          drain(slot, id, deques, slots, failure, morsels, worker)

  private def take(
      slot: Int,
      deques: Array[ConcurrentLinkedDeque[Int]],
      failure: AtomicReference[Option[Throwable]],
  ): Option[Int] =
    if (failure.get().nonEmpty) then None
    else Option(deques(slot).pollFirst()).orElse(steal(slot, deques))

  private def steal(slot: Int, deques: Array[ConcurrentLinkedDeque[Int]]): Option[Int] =
    deques.indices.iterator
      .filter(_ != slot)
      .map(other => Option(deques(other).pollLast()))
      .collectFirst { case Some(index) => index }
