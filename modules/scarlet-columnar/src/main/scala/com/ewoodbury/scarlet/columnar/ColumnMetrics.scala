package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

/**
 * Counts published batches, morsel visits, decoded values, and primitive source encodes while
 * armed. Disarmed by default, so a study does not record. A test arms around one action and
 * disarms afterwards, including from `afterEach`.
 *
 * A primitive encode is one `ArraySeq.ofInt`, `ArraySeq.ofDouble`, owned primitive array, or raw
 * `intPairs(Array, Array)` copied into columns. One note may copy two arrays: a pair encode copies
 * the keys and the values and still counts once. A boxed iterable does not count.
 */
object ColumnMetrics:

  private val armed = new AtomicBoolean(false)
  private val batches = new AtomicLong(0L)
  private val visits = new AtomicLong(0L)
  private val decoded = new AtomicLong(0L)
  private val primitiveEncodes = new AtomicLong(0L)

  /** Start from zero and record until [[disarm]]. */
  def arm(): Unit =
    batches.set(0L)
    visits.set(0L)
    decoded.set(0L)
    primitiveEncodes.set(0L)
    armed.set(true)

  /** Stop recording. The last counts stay readable. */
  def disarm(): Unit =
    armed.set(false)

  def batchCount: Long = batches.get()

  def visitCount: Long = visits.get()

  def decodedValueCount: Long = decoded.get()

  /**
   * Owned, `ArraySeq`, or raw int-pair primitive encodes recorded since [[arm]]. A pair counts
   * once.
   */
  def primitiveEncodeCount: Long = primitiveEncodes.get()

  private[columnar] def noteBatch(): Unit =
    if (armed.get()) then batches.incrementAndGet()
    ()

  private[columnar] def noteVisit(): Unit =
    if (armed.get()) then visits.incrementAndGet()
    ()

  private[columnar] def noteDecoded(count: Int): Unit =
    if (armed.get()) then decoded.addAndGet(count.toLong)
    ()

  /**
   * One primitive encode. A pair encode copies both arrays under this single note. The iterator
   * fill does not call this.
   */
  private[columnar] def notePrimitiveEncode(): Unit =
    if (armed.get()) then primitiveEncodes.incrementAndGet()
    ()
