package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicLong

import scala.annotation.unused

/**
 * Receives one note per published batch, morsel visit, decoded row group, or primitive encode.
 *
 * [[ColumnMetrics]] holds one sink for the process. A note reads that reference once and calls it.
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
 * One thread installs the sink. Morsel workers and task threads then note into that same sink, so
 * each counter is an `AtomicLong`. A plain `Long` drops a visit or a batch when those threads
 * interleave. A later [[ColumnMetrics.install]] still replaces this sink for every thread.
 */
final class CountingSink extends MetricsSink:

  private val batches = new AtomicLong(0L)
  private val visits = new AtomicLong(0L)
  private val decodedRows = new AtomicLong(0L)
  private val primitiveEncodes = new AtomicLong(0L)

  def batch(): Unit =
    batches.incrementAndGet()
    ()

  def visit(): Unit =
    visits.incrementAndGet()
    ()

  def decoded(rows: Int): Unit =
    decodedRows.addAndGet(rows.toLong)
    ()

  def primitiveEncode(): Unit =
    primitiveEncodes.incrementAndGet()
    ()

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
 * including when the body throws.
 *
 * The slot is one process-wide window. [[install]] reads the current sink and writes the new one,
 * with no lock. Two overlapping installs can observe the same previous sink, and the later write
 * is what every thread notes into until the matching [[restore]]. Nested or concurrent actions
 * therefore cross-contaminate. Tests run sequentially and restore in `afterEach`, which is why one
 * action's counts stay separate today. Giving each action its own sink means passing that sink
 * into the codec and the morsel workers, instead of reading it from this slot.
 *
 * A primitive encode is one `ArraySeq.ofInt`, `ArraySeq.ofDouble`, owned primitive array, or raw
 * `intPairs(Array, Array)` copied into columns. One note may copy two arrays: a pair encode copies
 * the keys and the values and still counts once. A boxed iterable does not count. A pair or triple
 * row counts once when decoded.
 */
object ColumnMetrics:

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var current: MetricsSink = NoOpSink

  /** Install `sink` and return the sink that was current. */
  def install(sink: MetricsSink): MetricsSink =
    val previous = current
    current = sink
    previous

  /** Put `previous` back. `previous` is the value [[install]] returned. */
  def restore(previous: MetricsSink): Unit =
    current = previous

  /**
   * Install `sink` for `body`, then restore the previous sink. The previous sink is restored when
   * `body` throws.
   */
  def withSink[A](sink: MetricsSink)(body: => A): A =
    val previous = install(sink)
    try body
    finally restore(previous)

  /** Batches on the installed [[CountingSink]], or zero when the sink is anything else. */
  def batchCount: Long = current match
    case sink: CountingSink => sink.batchCount
    case _ => 0L

  /** Morsel visits on the installed [[CountingSink]], or zero when the sink is anything else. */
  def visitCount: Long = current match
    case sink: CountingSink => sink.visitCount
    case _ => 0L

  /**
   * Rows decoded on the installed [[CountingSink]], or zero otherwise. A pair or triple row counts
   * once, not once per cell.
   */
  def decodedValueCount: Long = current match
    case sink: CountingSink => sink.decodedValueCount
    case _ => 0L

  /**
   * Owned, `ArraySeq`, or raw int-pair encodes on the installed [[CountingSink]], or zero
   * otherwise. A pair counts once.
   */
  def primitiveEncodeCount: Long = current match
    case sink: CountingSink => sink.primitiveEncodeCount
    case _ => 0L

  private[columnar] def batch(): Unit =
    current.batch()

  private[columnar] def visit(): Unit =
    current.visit()

  private[columnar] def decoded(rows: Int): Unit =
    current.decoded(rows)

  private[columnar] def primitiveEncode(): Unit =
    current.primitiveEncode()

  /** Installs a fresh [[CountingSink]] and drops whatever was current. */
  @deprecated("Install a CountingSink and restore the previous sink.", "0.1.0")
  def arm(): Unit =
    val _ = install(new CountingSink)

  /** Restores [[NoOpSink]]. Counts stay on the removed sink and read back as zero. */
  @deprecated("Restore the sink that was current before install.", "0.1.0")
  def disarm(): Unit =
    restore(NoOpSink)
