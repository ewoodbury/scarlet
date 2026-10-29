package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Spills one post-exchange grouped-sum bucket that does not fit in its budget.
 *
 * [[retainedBytes]] estimates the table [[HashAggregate.sumInt64]] would keep. It is an estimate,
 * not a measurement of the live set, and it ignores object headers. A bucket at or under the
 * budget, and every bucket while spill is disabled, is summed in memory. A bucket over the budget
 * is sub-partitioned with [[HashExchange.partitionAll]], which uses `Math.floorMod` and drops a
 * null key. Each piece is cut into morsels, and each morsel is written to its own spill file. The
 * morsels are not kept. Each file is read back and summed alone, then
 * [[HashAggregate.mergeSumPartials]] folds those partials. A null sum stays null. The merged batch
 * is the only result, so a failure during the fold publishes nothing.
 *
 * Only [[ColumnPipeline]]'s grouped sum calls this. `reduceByKey` and min/max do not.
 */
private[scarlet] object SumSpill:

  /** Thrown when a test asks the next spill merge to fail before it publishes. */
  private[scarlet] val MergeFailed: String = "spill merge failed"

  private val filesWritten = new AtomicLong
  private val mergeFault = new AtomicBoolean(false)
  private val lastDirectory = new AtomicReference("")

  /**
   * Estimated retained bytes of the open sum table for `rows` bucket rows.
   *
   * `rows * (4 + 8 + 1)` is the key, the `Int64` value, and one validity byte per row. Validity is
   * stored in bits; charging a byte rounds it up. The slot allowance is [[ColumnHash.tableSize]]
   * slots, each charged `4 + 8 + 1 + 1` for the key, the sum, the occupancy byte, and the seen
   * byte. That is the table a non-empty [[HashAggregate.sumInt64]] allocates. An empty bucket is
   * `0`: the in-memory sum returns before it builds a table.
   */
  private[scarlet] def retainedBytes(rows: Int): Long =
    if (rows <= 0) then 0L
    else
      val rowBytes = rows.toLong * (4L + 8L + 1L)
      val slots = ColumnHash.tableSize(rows).toLong
      val slotBytes = slots * (4L + 8L + 1L + 1L)
      rowBytes + slotBytes

  /** True when spill is on and this bucket's estimate is strictly above the budget. */
  private[scarlet] def exceeds(bucket: ColumnBatch, conf: ScarletConf): Boolean =
    conf.spillEnabled && retainedBytes(bucket.length) > conf.spillBudgetBytes

  /**
   * Sums one exchanged bucket. Over budget, spills it and returns the merged partial. The caller
   * sums buckets one at a time, so this store is closed before the next bucket starts.
   */
  private[scarlet] def finish(
      bucket: ColumnBatch,
      parallelism: Int,
      batchSize: Int,
      conf: ScarletConf,
  ): ColumnBatch =
    if (exceeds(bucket, conf)) then spill(bucket, parallelism, batchSize, conf)
    else HashAggregate.sumInt64(bucket, keyOrdinal = 0, valueOrdinal = 1)

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
  ): ColumnBatch =
    val parts = SpillStore.splitCount(bucket.length, retainedBytes(bucket.length), conf)
    val directory = SpillStore.directory(conf)
    lastDirectory.set(directory.toString)
    SpillStore.withSpill(directory, conf.spillMaxFiles, conf.spillMaxBytes) { store =>
      val split = HashExchange.partitionAll(
        Vector(bucket),
        keyOrdinal = 0,
        numPartitions = parts,
        parallelism = parallelism,
      )
      // One morsel at a time: publish, read back, sum, delete. The store holds at most one file,
      // so `spillMaxFiles` is not a function of the row count. The sub-partitioned pieces and
      // their morsels are already materialised by the exchange and the slice, so live memory is
      // still one bucket plus the running partial; this loop does not add to it.
      val morsels = split.iterator
        .filter(part => part.length > 0)
        .flatMap(part => ColumnBatches.slice(part, batchSize).iterator)
      val merged = morsels.foldLeft(Option.empty[ColumnBatch]) { (accumulator, morsel) =>
        val file = publish(store, morsel)
        val partial = sumFile(file)
        file.delete()
        Some(accumulator.fold(partial)(previous => merge(Vector(previous, partial))))
      }
      merged.getOrElse(HashAggregate.sumInt64(bucket, keyOrdinal = 0, valueOrdinal = 1))
    }

  private def publish(store: SpillStore, morsel: ColumnBatch): SpillFile =
    val file = store.write(Vector(morsel))
    val _ = filesWritten.incrementAndGet()
    file

  private def sumFile(file: SpillFile): ColumnBatch =
    file.read() match
      case Vector(morsel) =>
        HashAggregate.sumInt64(morsel, keyOrdinal = 0, valueOrdinal = 1)
      case other =>
        throw new IllegalStateException(s"spill file holds ${other.length} batches")

  /**
   * Folds every per-file partial. An armed failure merges the first partial, then throws, so the
   * full fold never returns a batch. The store deletes its files on the way out.
   */
  private def merge(partials: Vector[ColumnBatch]): ColumnBatch =
    if (mergeFault.compareAndSet(true, false)) then
      val _ = HashAggregate.mergeSumPartials(partials.take(1), keyOrdinal = 0, valueOrdinal = 1)
      throw new IllegalStateException(MergeFailed)
    else HashAggregate.mergeSumPartials(partials, keyOrdinal = 0, valueOrdinal = 1)
