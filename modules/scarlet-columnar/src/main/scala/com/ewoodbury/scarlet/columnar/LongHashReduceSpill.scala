package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Spills one post-exchange `reduceByKey` bucket of `(Long, Long)` pairs that does not fit in its
 * budget.
 *
 * [[retainedBytes]] estimates the table [[LongHashReduce.int64]] would keep. It is an estimate,
 * not a measurement of the live set, and it ignores object headers. A bucket at or under the
 * budget, and every bucket while spill is disabled, is reduced in memory. A bucket over the budget
 * is sub-partitioned with [[HashExchange.partitionAllInt64]], which uses `Long.hashCode` and
 * `Math.floorMod` and drops a null key. Each piece is cut into morsels, and each morsel is written
 * to its own spill file. The morsels are not kept. Each file is read back and reduced alone, then
 * [[LongHashReduce.int64]] on [[ColumnBatches.concat]] of the running partial and the new partial
 * folds them. Concatenation keeps validity bits, so a null group stays null until a later value
 * arrives, and a present group stays present. Null rules stay in the kernel: a null key is
 * dropped, one null value stays a null group, and a second value, present or null, unboxes each
 * null as `0` and applies the operator. The merged batch is the only result, so a failure during
 * the fold publishes nothing.
 *
 * Only [[LongHashReduce.byKey]] and [[LongHashReduce.byKeySelected]] call this, and only for the
 * post-exchange bucket. The partial combine before the exchange stays in memory. The spill path
 * never holds more than the input bucket, its sub-partitioned copy, the current piece's morsels,
 * one read-back copy, the running partial, the fold table, and one open file. The running partial
 * is the result so far, so it is `O(distinct keys)` and cannot be bounded by the budget.
 * [[HashExchange.partitionAllInt64]] materialises the sub-partitioned copy, and
 * [[ColumnBatches.slice]] materialises one piece's morsels at a time as the fold reaches that
 * piece.
 */
private[scarlet] object LongHashReduceSpill:

  /** Message of the exception thrown when a test asks the next spill merge to fail. */
  private[scarlet] val MergeFailed: String = "spill merge failed"

  private val filesWritten = new AtomicLong
  private val mergeFault = new AtomicBoolean(false)
  private val lastDirectory = new AtomicReference("")

  /**
   * Estimated retained bytes of the open reduce table for `rows` bucket rows.
   *
   * `rows * (8 + 8 + 1)` is the key, the `Int64` value, and one validity byte per row. Validity is
   * stored in bits; charging a byte rounds it up and covers both bitmaps. The slot allowance is
   * [[ColumnHash.tableSize]] slots, each charged `8 + 8 + 1 + 4` for the key, the value, the
   * occupancy state byte, and the slot index. There is no separate seen byte. An empty bucket is
   * `0`: the in-memory reduce returns before it builds a table.
   */
  private[scarlet] def retainedBytes(rows: Int): Long =
    if (rows <= 0) then 0L
    else
      val rowBytes = rows.toLong * (8L + 8L + 1L)
      val slots = ColumnHash.tableSize(rows).toLong
      val slotBytes = slots * (8L + 8L + 1L + 4L)
      rowBytes + slotBytes

  /** True when spill is on and this bucket's estimate is strictly above the budget. */
  private[scarlet] def exceeds(bucket: ColumnBatch, conf: ScarletConf): Boolean =
    conf.spillEnabled && retainedBytes(bucket.length) > conf.spillBudgetBytes

  /**
   * Reduces one exchanged bucket. Over budget, spills it and returns the merged partial. The
   * caller reduces buckets one at a time, so this store is closed before the next bucket starts.
   */
  private[scarlet] def finish(
      bucket: ColumnBatch,
      parallelism: Int,
      batchSize: Int,
      conf: ScarletConf,
      op: LongHashReduce.LongBinOp,
  ): ColumnBatch =
    if (exceeds(bucket, conf)) then spill(bucket, parallelism, batchSize, conf, op)
    else LongHashReduce.int64(bucket, keyOrdinal = 0, valueOrdinal = 1, op)

  /** Spill files published since the last [[resetForTest]]. */
  private[scarlet] def filesWrittenForTest: Long = filesWritten.get

  /** Directory of the store most recently opened, or empty when none has been. */
  private[scarlet] def lastDirectoryForTest: String = lastDirectory.get

  /** The next spill merge throws after it has folded its first partial and before it returns. */
  private[scarlet] def armMergeFailure(): Unit =
    mergeFault.set(true)

  /** Clears the file counter, the recorded directory, and an armed merge failure. */
  private[scarlet] def resetForTest(): Unit =
    filesWritten.set(0L)
    mergeFault.set(false)
    lastDirectory.set("")

  private def spill(
      bucket: ColumnBatch,
      parallelism: Int,
      batchSize: Int,
      conf: ScarletConf,
      op: LongHashReduce.LongBinOp,
  ): ColumnBatch =
    val parts = SpillStore.splitCount(bucket.length, retainedBytes(bucket.length), conf)
    val directory = SpillStore.directory(conf)
    lastDirectory.set(directory.toString)
    SpillStore.withSpill(directory, conf.spillMaxFiles, conf.spillMaxBytes) { store =>
      val split = HashExchange.partitionAllInt64(
        Vector(bucket),
        keyOrdinal = 0,
        numPartitions = parts,
        parallelism = parallelism,
      )
      // One morsel at a time: publish, read back, reduce, delete. The store holds at most one
      // file, so `spillMaxFiles` is not a function of the row count. The sub-partitioned pieces
      // are materialised before this loop, and one piece's morsels are materialised as the fold
      // reaches that piece, so live memory is the input bucket, that copy, those morsels, one
      // read-back copy, the running partial, the fold table, and one open file.
      val morsels = split.iterator
        .filter(part => part.length > 0)
        .flatMap(part => ColumnBatches.slice(part, batchSize).iterator)
      val merged = morsels.foldLeft(Option.empty[ColumnBatch]) { (accumulator, morsel) =>
        val file = publish(store, morsel)
        val partial = reduceFile(file, op)
        file.delete()
        Some(accumulator.fold(partial)(running => fold(running, partial, op)))
      }
      merged.getOrElse {
        throw new IllegalStateException("spill produced no morsels for a non-empty bucket")
      }
    }

  private def publish(store: SpillStore, morsel: ColumnBatch): SpillFile =
    val file = store.write(Vector(morsel))
    val _ = filesWritten.incrementAndGet()
    file

  private def reduceFile(file: SpillFile, op: LongHashReduce.LongBinOp): ColumnBatch =
    file.read() match
      case Vector(morsel) =>
        LongHashReduce.int64(morsel, keyOrdinal = 0, valueOrdinal = 1, op)
      case other =>
        throw new IllegalStateException(s"spill file holds ${other.length} batches")

  /**
   * Folds the running partial with the morsel just reduced. An armed failure folds the running
   * partial alone, then throws, so the full fold never returns a batch. The store deletes its
   * files on the way out. [[ColumnBatches.concat]] keeps each partial's validity bits, so a null
   * group is still null when the kernel applies the operator.
   */
  private def fold(
      running: ColumnBatch,
      partial: ColumnBatch,
      op: LongHashReduce.LongBinOp,
  ): ColumnBatch =
    val parts = Vector(running, partial)
    if (mergeFault.compareAndSet(true, false)) then
      val _ = LongHashReduce.int64(
        ColumnBatches.concat(parts.take(1)),
        keyOrdinal = 0,
        valueOrdinal = 1,
        op,
      )
      throw new IllegalStateException(MergeFailed)
    else LongHashReduce.int64(ColumnBatches.concat(parts), keyOrdinal = 0, valueOrdinal = 1, op)
