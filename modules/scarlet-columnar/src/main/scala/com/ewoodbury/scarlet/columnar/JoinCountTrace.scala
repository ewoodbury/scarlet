package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicReference

/**
 * Phase notes for an int inner-join count.
 *
 * [[install]] is the only way a note is recorded. With no probe, [[time]] and [[labeled]] do not
 * read a clock and do not allocate. The count calls them once per phase, outside the row loop. A
 * probe receives one note per phase, on the calling thread, after that phase returns. Nested
 * [[time]] calls are separate notes. [[labeled]] prefixes those notes on the same thread and is
 * invisible to a nested dispatch.
 */
private[scarlet] object JoinCountTrace:

  trait Probe:
    def add(phase: String, nanos: Long, submits: Long): Unit

  def install(probe: Probe): Option[Probe] =
    current.getAndSet(Some(probe))

  def restore(previous: Option[Probe]): Unit =
    current.set(previous)

  /**
   * Prefix [[time]] notes on this thread. A missing probe runs `body` and does not store `name`.
   */
  def labeled[A](name: String)(body: => A): A =
    current.get() match
      case None => body
      case Some(_) =>
        val previous = labels.get()
        labels.set(name)
        try body
        finally labels.set(previous)

  /** Note `phase` when a probe is installed. Otherwise run `body` and return. */
  def time[A](phase: String)(body: => A): A =
    current.get() match
      case None => body
      case Some(probe) =>
        val before = MorselScheduler.submittedOnCaller
        val started = System.nanoTime()
        val value = body
        val elapsed = System.nanoTime() - started
        val submits = MorselScheduler.submittedOnCaller - before
        probe.add(qualified(phase), elapsed, submits)
        value

  private val current = new AtomicReference[Option[Probe]](None)

  private val labels = new ThreadLocal[String]:
    override def initialValue(): String = ""

  private def qualified(phase: String): String =
    val prefix = labels.get()
    if (prefix.isEmpty) then phase else s"$prefix-$phase"
