package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.annotation.unused

/**
 * Receives one note per published batch, morsel visit, decoded row group, or primitive encode.
 *
 * [[ColumnMetrics]] holds one sink for the process. A note reads that reference once and calls it.
 * Morsel workers call [[visit]] and [[batch]] from several threads at once. [[decoded]] and
 * [[primitiveEncode]] run on those workers or on the task thread, and those calls overlap. An
 * implementation must be thread-safe. A plain field drops a note when two workers write it
 * together. [[CountingSink]] uses an atomic counter for each note for that reason.
 */
trait MetricsSink:

  /** One published [[ColumnBatch]]. */
  def batch(): Unit

  /** One morsel handed to a worker. */
  def visit(): Unit

  /** `rows` decoded. A pair or triple row counts once, not once per cell. */
  def decoded(rows: Int): Unit

  /**
   * One owned, `ArraySeq`, or raw int-pair copy. A pair copies two arrays and counts once. The
   * iterator fill does not call this.
   */
  def primitiveEncode(): Unit

/** Ignores every note. This is the sink installed at startup. */
object NoOpSink extends MetricsSink:

  def batch(): Unit = ()

  def visit(): Unit = ()

  def decoded(@unused rows: Int): Unit = ()

  def primitiveEncode(): Unit = ()

/**
 * Counts notes for one installed window.
 *
 * Counters are atomic because [[MetricsSink]] notes arrive from several threads. A later
 * [[ColumnMetrics.install]] still replaces this sink for every thread.
 */
final class CountingSink extends MetricsSink:

  private val batches = new AtomicLong(0L)
  private val visits = new AtomicLong(0L)
  private val decodedRows = new AtomicLong(0L)
  private val primitiveEncodes = new AtomicLong(0L)

  def batch(): Unit =
    val _ = batches.incrementAndGet()

  def visit(): Unit =
    val _ = visits.incrementAndGet()

  def decoded(rows: Int): Unit =
    val _ = decodedRows.addAndGet(rows.toLong)

  def primitiveEncode(): Unit =
    val _ = primitiveEncodes.incrementAndGet()

  def batchCount: Long = batches.get()

  def visitCount: Long = visits.get()

  /** Rows decoded. A pair or triple row counts once, not once per cell. */
  def decodedValueCount: Long = decodedRows.get()

  /** Owned, `ArraySeq`, or raw int-pair encodes. A pair counts once. */
  def primitiveEncodeCount: Long = primitiveEncodes.get()

/**
 * Counts published batches, morsel visits, decoded rows, and primitive source encodes on the
 * installed [[MetricsSink]]. [[NoOpSink]] starts installed, so nothing is recorded until a caller
 * installs a sink. [[withSink]] installs for one body and restores the previous sink afterwards,
 * when the body returns and when the body throws.
 *
 * The slot is one atomic reference. [[install]] is a single swap, so two overlapping installs
 * cannot observe the same previous sink. [[restore]] writes its argument and the last writer wins.
 * Only non-overlapping, properly nested use is supported. Overlapping restores can put back a sink
 * that another caller has already replaced. Tests run sequentially and restore [[NoOpSink]] in
 * `afterEach`.
 *
 * A primitive encode is one `ArraySeq.ofInt`, `ArraySeq.ofDouble`, owned primitive array, or raw
 * `intPairs(Array, Array)` copied into columns. One note may copy two arrays: a pair encode copies
 * the keys and the values and still counts once. A boxed iterable does not count. A pair or triple
 * row counts once when decoded.
 */
object ColumnMetrics:

  private val current: AtomicReference[MetricsSink] = new AtomicReference(NoOpSink)

  /**
   * Install `sink` and return the sink it replaced.
   *
   * One `getAndSet`. [[restore]] is last-writer-wins, and only non-overlapping, properly nested
   * use is supported.
   */
  def install(sink: MetricsSink): MetricsSink =
    current.getAndSet(sink)

  /** Put `previous` back. `previous` is the value [[install]] returned. The last restore wins. */
  def restore(previous: MetricsSink): Unit =
    current.set(previous)

  /**
   * Install `sink` for `body`, then restore the previous sink. The previous sink is restored when
   * `body` returns and when `body` throws. [[restore]] is last-writer-wins, so only
   * non-overlapping, properly nested use is supported.
   */
  def withSink[A](sink: MetricsSink)(body: => A): A =
    val previous = install(sink)
    try body
    finally restore(previous)

  /** Batches on the installed [[CountingSink]], or zero when the sink is anything else. */
  def batchCount: Long = counting.fold(0L)(_.batchCount)

  /** Morsel visits on the installed [[CountingSink]], or zero when the sink is anything else. */
  def visitCount: Long = counting.fold(0L)(_.visitCount)

  /**
   * Rows decoded on the installed [[CountingSink]], or zero otherwise. A pair or triple row counts
   * once, not once per cell.
   */
  def decodedValueCount: Long = counting.fold(0L)(_.decodedValueCount)

  /**
   * Owned, `ArraySeq`, or raw int-pair encodes on the installed [[CountingSink]], or zero
   * otherwise. A pair counts once.
   */
  def primitiveEncodeCount: Long = counting.fold(0L)(_.primitiveEncodeCount)

  private def counting: Option[CountingSink] = current.get() match
    case sink: CountingSink => Some(sink)
    case _: MetricsSink => None

  private[columnar] def batch(): Unit =
    current.get().batch()

  private[columnar] def visit(): Unit =
    current.get().visit()

  private[columnar] def decoded(rows: Int): Unit =
    current.get().decoded(rows)

  private[columnar] def primitiveEncode(): Unit =
    current.get().primitiveEncode()
