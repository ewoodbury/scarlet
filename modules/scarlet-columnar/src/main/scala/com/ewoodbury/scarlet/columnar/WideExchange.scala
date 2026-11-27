package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Spills one exchange of a wide batch with the existing spill store.
 *
 * The trigger is the estimate the int or long exchange already uses.
 * [[HashJoin.retainedBuildBytes]] prices an int key. [[LongHashJoin.retainedBuildBytes]] prices a
 * long key. Spill is off unless [[ScarletConf.spillEnabled]] is set. A bucket at or under
 * [[ScarletConf.spillBudgetBytes]], an empty bucket, and every bucket while spill is off, stays in
 * memory and opens no store. Float64, Float32, and Bool stay in memory too: the store refuses
 * them, and this path does not add a codec. Int32, Int64, and dictionary strings are written with
 * their validity words.
 *
 * [[exchange]] partitions with [[HashExchange.partitionAll]] or
 * [[HashExchange.partitionAllInt64]]. A null key is dropped. A null in another column is copied.
 * Each over-budget bucket is one spill file, read back before the next bucket, and deleted before
 * the call returns. [[hold]] is that decision for buckets [[WideJoin]] has already split. It does
 * not repartition.
 *
 * `groupByKey` spill is a different object. This path does not call it, and a clear value bit
 * there is still refused when that spill finishes.
 */
private[scarlet] object WideExchange:

  private val filesWritten = new AtomicLong
  private val lastDirectory = new AtomicReference("")

  /**
   * Byte estimate the matching exchange already uses. An empty row count is `0`. A positive count
   * is [[HashJoin.retainedBuildBytes]] or [[LongHashJoin.retainedBuildBytes]].
   */
  private[scarlet] def retainedBytes(rows: Int, longKey: Boolean): Long =
    if (rows <= 0) then 0L
    else if (longKey) then LongHashJoin.retainedBuildBytes(rows)
    else HashJoin.retainedBuildBytes(rows)

  /**
   * True when spill is on, the bucket has a row, every column is one the store can write, and the
   * exchange estimate is strictly above the budget.
   */
  private[scarlet] def exceeds(bucket: ColumnBatch, longKey: Boolean, conf: ScarletConf): Boolean =
    conf.spillEnabled && bucket.length > 0 && spillable(bucket) &&
      retainedBytes(bucket.length, longKey) > conf.spillBudgetBytes

  /**
   * Partitions `batches` and spills each bucket that [[exceeds]] the budget. Empty buckets are
   * returned in place. The result has one batch per partition, including empty partitions.
   */
  private[scarlet] def exchange(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      longKey: Boolean,
      partitions: Int,
      parallelism: Int,
      conf: ScarletConf,
  ): Vector[ColumnBatch] =
    require(batches.nonEmpty, "a wide exchange needs a batch")
    require(partitions > 0, s"partitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    val scattered =
      if (longKey) then
        HashExchange.partitionAllInt64(batches, keyOrdinal, partitions, parallelism)
      else HashExchange.partitionAll(batches, keyOrdinal, partitions, parallelism)
    scattered.map { bucket =>
      if (exceeds(bucket, longKey, conf)) then readOne(bucket, conf)
      else bucket
    }

  /**
   * Spills partitions [[WideJoin]] already built. Spill off returns `parts` unchanged. A partition
   * whose columns the store cannot write stays in memory.
   */
  private[scarlet] def hold(
      parts: Array[Seq[ColumnBatch]],
      longKey: Boolean,
  ): Array[Seq[ColumnBatch]] =
    val conf = ScarletConf.get
    if (!conf.spillEnabled) then parts
    else parts.map(batches => holdOne(batches, longKey, conf))

  /** Spill files published since the last [[resetForTest]]. */
  private[scarlet] def filesWrittenForTest: Long = filesWritten.get

  /** Directory of the store most recently opened, or empty when none has been. */
  private[scarlet] def lastDirectoryForTest: String = lastDirectory.get

  /** Clears the file counter and the recorded directory. */
  private[scarlet] def resetForTest(): Unit =
    filesWritten.set(0L)
    lastDirectory.set("")

  private def holdOne(
      batches: Seq[ColumnBatch],
      longKey: Boolean,
      conf: ScarletConf,
  ): Seq[ColumnBatch] =
    val rows = batches.foldLeft(0L)((sum, batch) => sum + batch.length.toLong)
    if (rows <= 0L || !batches.forall(spillable)) then batches
    else if (over(rows, longKey, conf)) then spillRead(batches.toVector, conf)
    else batches

  private def over(rows: Long, longKey: Boolean, conf: ScarletConf): Boolean =
    rows > Int.MaxValue.toLong || retainedBytes(rows.toInt, longKey) > conf.spillBudgetBytes

  private def readOne(bucket: ColumnBatch, conf: ScarletConf): ColumnBatch =
    spillRead(Vector(bucket), conf) match
      case Vector(one) => one
      case other =>
        throw new IllegalStateException(s"spill file holds ${other.length} batches")

  private def spillRead(batches: Vector[ColumnBatch], conf: ScarletConf): Vector[ColumnBatch] =
    val directory = SpillStore.directory(conf)
    lastDirectory.set(directory.toString)
    SpillStore.withSpill(directory, conf.spillMaxFiles, conf.spillMaxBytes) { store =>
      val file = store.write(batches)
      val _ = filesWritten.incrementAndGet()
      val read = file.read()
      if (read.length == batches.length) then read
      else
        throw new IllegalStateException(
          s"spill file holds ${read.length} batches, wrote ${batches.length}",
        )
    }

  private def spillable(batch: ColumnBatch): Boolean =
    batch.schema.forall {
      case LogicalType.Int32 | LogicalType.Int64 | LogicalType.Utf8Dict => true
      case _ => false
    }
