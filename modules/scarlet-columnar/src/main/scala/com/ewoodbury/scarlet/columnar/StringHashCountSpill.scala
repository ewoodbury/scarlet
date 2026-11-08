package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Spills one post-exchange string `countByKey` bucket that does not fit in its budget.
 *
 * [[StringHashCount.inputRowBytes]] estimates the table [[StringHashCount.addCounts]] would keep.
 * It charges every row about to be inserted as its own key, plus that bucket's dictionary, so the
 * bound overestimates when partials share a key. That overestimate is intentional. The charge is a
 * 4-byte dictionary code and the dictionary's UTF-8 bytes, not the source value column. A bucket
 * at or under the budget, and every bucket while spill is disabled, is counted in memory. A bucket
 * over the budget is sub-partitioned with [[HashExchange.partitionAllUtf8]], which uses
 * `StringHash.hash` and drops a null key. This is not [[HashExchange.partitionAll]], and it is not
 * the within-parent quotient a spilled join uses. Each non-empty piece is cut into morsels, and
 * each morsel is written to its own spill file. The morsels are not kept. Each file is read back
 * and counted alone, then [[StringHashCount.addCounts]] on [[ColumnBatches.concat]] of the running
 * partial and the new partial sums counts for the same key. Concatenation remaps unlike
 * dictionaries, so a code compared in the fold is the same string. The value column is never read:
 * the bucket is already `(key, count)` partials. A clear validity bit is not an absence bitmap,
 * and this store does not write one. The merged batch is the only result, so a failure during the
 * fold publishes nothing.
 *
 * Only [[StringHashCount.byKey]] and [[StringHashCount.byKeySelected]] call this, and only for the
 * post-exchange bucket. The partial combine before the exchange stays in memory. One store is open
 * per over-budget bucket and is closed before the next bucket. Under-budget buckets open no store.
 * There is no probe side. The running partial is the result so far, so it is `O(distinct keys)`
 * and cannot be bounded by the budget. A morsel or a fold whose [[StringHashCount.inputRowBytes]]
 * estimate is over the budget throws [[ClosedKernelRefusal]] before its table is allocated. The
 * store deletes its files on that throw. [[HashExchange.partitionAllUtf8]] materialises the
 * sub-partitioned copy, and [[ColumnBatches.slice]] materialises one piece's morsels as the fold
 * reaches that piece.
 */
private[scarlet] object StringHashCountSpill:

  private val filesWritten = new AtomicLong
  private val lastDirectory = new AtomicReference("")

  /**
   * True when spill is on and this bucket's [[StringHashCount.inputRowBytes]] estimate is strictly
   * above the budget. An empty bucket is false: the in-memory count allocates nothing, and a
   * dictionary is not charged when the row count is zero.
   */
  private[scarlet] def exceeds(bucket: ColumnBatch, conf: ScarletConf): Boolean =
    conf.spillEnabled && estimate(bucket) > conf.spillBudgetBytes

  /**
   * Counts one exchanged bucket. Over budget, spills it and returns the merged partial. The caller
   * counts buckets one at a time, so this store is closed before the next bucket starts.
   */
  private[scarlet] def finish(
      bucket: ColumnBatch,
      parallelism: Int,
      batchSize: Int,
      conf: ScarletConf,
  ): ColumnBatch =
    if (exceeds(bucket, conf)) then spill(bucket, parallelism, batchSize, conf)
    else StringHashCount.addCounts(bucket)

  /** Spill files published since the last [[resetForTest]]. */
  private[scarlet] def filesWrittenForTest: Long = filesWritten.get

  /** Directory of the store most recently opened, or empty when none has been. */
  private[scarlet] def lastDirectoryForTest: String = lastDirectory.get

  /** Clears the file counter and the recorded directory. */
  private[scarlet] def resetForTest(): Unit =
    filesWritten.set(0L)
    lastDirectory.set("")

  private def spill(
      bucket: ColumnBatch,
      parallelism: Int,
      batchSize: Int,
      conf: ScarletConf,
  ): ColumnBatch =
    val estimated = estimate(bucket)
    val parts = SpillStore.splitCount(bucket.length, estimated, conf)
    val directory = SpillStore.directory(conf)
    lastDirectory.set(directory.toString)
    SpillStore.withSpill(directory, conf.spillMaxFiles, conf.spillMaxBytes) { store =>
      val split = HashExchange.partitionAllUtf8(
        Vector(bucket),
        keyOrdinal = 0,
        numPartitions = parts,
        parallelism = parallelism,
      )
      // One morsel at a time: publish, read back, count, delete. The store holds at most one
      // file, so `spillMaxFiles` is not a function of the row count. The sub-partitioned pieces
      // are materialised before this loop, and one piece's morsels are materialised as the fold
      // reaches that piece, so live memory is the input bucket, that copy, those morsels, one
      // read-back copy, the running partial, the fold table, and one open file.
      val morsels = split.iterator
        .filter(part => part.length > 0)
        .flatMap(part => ColumnBatches.slice(part, batchSize).iterator)
      val merged = morsels.foldLeft(Option.empty[ColumnBatch]) { (accumulator, morsel) =>
        val file = publish(store, morsel)
        val partial = countFile(file)
        file.delete()
        Some(accumulator.fold(partial)(running => fold(running, partial)))
      }
      merged.getOrElse {
        throw new IllegalStateException("spill produced no morsels for a non-empty bucket")
      }
    }

  private def publish(store: SpillStore, morsel: ColumnBatch): SpillFile =
    val file = store.write(Vector(morsel))
    val _ = filesWritten.incrementAndGet()
    file

  private def countFile(file: SpillFile): ColumnBatch =
    file.read() match
      case Vector(morsel) => StringHashCount.addCounts(morsel)
      case other =>
        throw new IllegalStateException(s"spill file holds ${other.length} batches")

  /**
   * Sums the running `(key, count)` partial with the morsel just counted. [[ColumnBatches.concat]]
   * remaps unlike dictionaries before [[StringHashCount.addCounts]] compares codes. That kernel
   * refuses when the rows about to be inserted do not fit, before it allocates the table. The
   * store deletes its files on the way out.
   */
  private def fold(running: ColumnBatch, partial: ColumnBatch): ColumnBatch =
    StringHashCount.addCounts(ColumnBatches.concat(Vector(running, partial)))

  private def estimate(bucket: ColumnBatch): Long =
    StringHashCount.inputRowBytes(
      bucket.length.toLong,
      StringHashCount.dictionaryEntries(bucket).toLong,
      StringHashCount.dictionaryBytes(bucket),
    )
