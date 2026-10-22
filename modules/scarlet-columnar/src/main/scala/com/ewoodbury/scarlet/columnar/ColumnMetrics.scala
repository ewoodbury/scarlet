package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

/**
 * Counts published batches, morsel visits, and decoded values while armed. Disarmed by default, so
 * a study does not record. A test arms around one action and disarms afterwards, including from
 * `afterEach`.
 */
object ColumnMetrics:

  private val armed = new AtomicBoolean(false)
  private val batches = new AtomicLong(0L)
  private val visits = new AtomicLong(0L)
  private val decoded = new AtomicLong(0L)

  /** Start from zero and record until [[disarm]]. */
  def arm(): Unit =
    batches.set(0L)
    visits.set(0L)
    decoded.set(0L)
    armed.set(true)

  /** Stop recording. The last counts stay readable. */
  def disarm(): Unit =
    armed.set(false)

  def batchCount: Long = batches.get()

  def visitCount: Long = visits.get()

  def decodedValueCount: Long = decoded.get()

  private[columnar] def noteBatch(): Unit =
    if (armed.get()) then batches.incrementAndGet()
    ()

  private[columnar] def noteVisit(): Unit =
    if (armed.get()) then visits.incrementAndGet()
    ()

  private[columnar] def noteDecoded(count: Int): Unit =
    if (armed.get()) then decoded.addAndGet(count.toLong)
    ()
