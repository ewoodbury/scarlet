package com.ewoodbury.scarlet.columnar

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Spills one post-exchange `reduceByKey` bucket of `(String, Int)` pairs that does not fit in its
 * budget.
 *
 * [[retainedBytes]] estimates the table [[StringHashReduce.reduce]] would keep. It is an estimate,
 * not a measurement of the live set, and it ignores object headers. A bucket at or under the
 * budget, and every bucket while spill is disabled, is reduced in memory. A bucket over the budget
 * is sub-partitioned with [[HashExchange.partitionAllUtf8]], which uses `StringHash.hash` and
 * `Math.floorMod` and drops a null key. Each piece is cut into morsels, and each morsel is written
 * to its own spill file. The morsels are not kept. Each file is read back and reduced alone, then
 * [[StringHashReduce.reduce]] on [[ColumnBatches.concat]] of the running partial and the new
 * partial folds them. Concatenation remaps unlike dictionaries, so a code compared in the fold is
 * the same string. Null rules stay in the kernel: a null key is dropped, and `""` is kept. A null
 * value never reaches the kernel. The merged batch is the only result, so a failure during the
 * fold publishes nothing.
 *
 * Only [[StringHashReduce.byKey]] calls this, and only for the post-exchange bucket. The partial
 * combine before the exchange stays in memory. The spill path never holds more than the input
 * bucket, its sub-partitioned copy, the current piece's morsels, one read-back copy, the running
 * partial, the fold table and its dictionary, and one open file. The running partial is the result
 * so far, so it is `O(distinct keys)` and cannot be bounded by the budget.
 * [[HashExchange.partitionAllUtf8]] materialises the sub-partitioned copy, and
 * [[ColumnBatches.slice]] materialises one piece's morsels as the fold reaches that piece.
 */
private[scarlet] object StringHashReduceSpill:

  /** Message of the exception thrown when a test asks the next spill merge to fail. */
  private[scarlet] val MergeFailed: String = "spill merge failed"

  private val filesWritten = new AtomicLong
  private val mergeFault = new AtomicBoolean(false)
  private val lastDirectory = new AtomicReference("")

  /**
   * Estimated retained bytes of the open reduce table for `rows` bucket rows, `dictionaryEntries`
   * dictionary entries, and `dictionaryBytes` of UTF-8 text.
   *
   * `rows * (4 + 4 + 1)` is a dictionary code, the `Int32` value, and one validity byte per row.
   * Validity is stored in bits; charging a byte rounds it up and covers both bitmaps. The
   * dictionary term is `dictionaryEntries * 8` for the reference array plus `dictionaryBytes` for
   * the UTF-8 payload, charged once per entry, including an entry no row uses. A post-exchange
   * bucket can share a source dictionary larger than its row count, so the reference term is per
   * entry, not per row. The slot allowance is [[ColumnHash.tableSize]] slots, each charged
   * `4 + 4 + 1 + 4` for the code, the value, the occupancy state byte, and the slot index.
   * [[StringHashReduce]]'s table has no seen byte. An empty bucket is `0`: the in-memory reduce
   * returns before it builds a table, and the dictionary is not added.
   */
  private[scarlet] def retainedBytes(
      rows: Int,
      dictionaryEntries: Int,
      dictionaryBytes: Long,
  ): Long =
    if (rows <= 0) then 0L
    else
      val rowBytes = rows.toLong * (4L + 4L + 1L)
      val dictionaryRefs = dictionaryEntries.toLong * 8L
      val slots = ColumnHash.tableSize(rows).toLong
      val slotBytes = slots * (4L + 4L + 1L + 4L)
      rowBytes + dictionaryRefs + dictionaryBytes + slotBytes

  /**
   * UTF-8 payload of every dictionary entry in the key column. A missing key column is rejected. A
   * null key is an empty slot, not a dictionary entry.
   */
  private[scarlet] def dictionaryBytes(bucket: ColumnBatch): Long =
    val keys = ColumnKernel.asUtf8(ColumnKernel.columnAt(bucket, 0), 0)
    keys.dictionary.iterator
      .map(entry => entry.getBytes(StandardCharsets.UTF_8).length.toLong)
      .sum

  /** Number of dictionary entries in the key column, including entries no row uses. */
  private[scarlet] def dictionaryEntries(bucket: ColumnBatch): Int =
    ColumnKernel.asUtf8(ColumnKernel.columnAt(bucket, 0), 0).dictionary.length

  /** True when spill is on and this bucket's estimate is strictly above the budget. */
  private[scarlet] def exceeds(bucket: ColumnBatch, conf: ScarletConf): Boolean =
    conf.spillEnabled &&
      retainedBytes(
        bucket.length,
        dictionaryEntries(bucket),
        dictionaryBytes(bucket),
      ) > conf.spillBudgetBytes

  /**
   * Reduces one exchanged bucket. Over budget, spills it and returns the merged partial. The
   * caller reduces buckets one at a time, so this store is closed before the next bucket starts.
   */
  private[scarlet] def finish(
      bucket: ColumnBatch,
      parallelism: Int,
      batchSize: Int,
      conf: ScarletConf,
      op: (Int, Int) => Int,
  ): ColumnBatch =
    if (exceeds(bucket, conf)) then spill(bucket, parallelism, batchSize, conf, op)
    else StringHashReduce.reduce(bucket, op)

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
      op: (Int, Int) => Int,
  ): ColumnBatch =
    val estimated =
      retainedBytes(bucket.length, dictionaryEntries(bucket), dictionaryBytes(bucket))
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

  private def reduceFile(file: SpillFile, op: (Int, Int) => Int): ColumnBatch =
    file.read() match
      case Vector(morsel) =>
        StringHashReduce.reduce(morsel, op)
      case other =>
        throw new IllegalStateException(s"spill file holds ${other.length} batches")

  /**
   * Folds the running partial with the morsel just reduced. An armed failure folds the running
   * partial alone, then throws, so the full fold never returns a batch. The store deletes its
   * files on the way out. [[ColumnBatches.concat]] remaps unlike dictionaries, so code equality in
   * the fold is string equality.
   */
  private def fold(
      running: ColumnBatch,
      partial: ColumnBatch,
      op: (Int, Int) => Int,
  ): ColumnBatch =
    val parts = Vector(running, partial)
    if (mergeFault.compareAndSet(true, false)) then
      val _ = StringHashReduce.reduce(ColumnBatches.concat(parts.take(1)), op)
      throw new IllegalStateException(MergeFailed)
    else StringHashReduce.reduce(ColumnBatches.concat(parts), op)
