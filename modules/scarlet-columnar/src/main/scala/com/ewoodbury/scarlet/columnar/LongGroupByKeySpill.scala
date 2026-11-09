package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Spills one post-exchange long `groupByKey` bucket that does not fit in its budget.
 *
 * The estimate is the [[GroupByKey]] long group table. Each slot is an 8-byte key, an occupancy
 * byte, a slot index, a group index, and a count. This is not [[GroupByKeySpill.inputRowBytes]],
 * which charges 4-byte keys. Zero rows allocate nothing. A bucket at or under the budget, and
 * every bucket while spill is off, is grouped in memory and opens no store. Spill off does not
 * throw [[ClosedKernelRefusal]].
 *
 * A bucket over the budget is cut with [[ColumnBatches.slice]] in input order. The bucket is not
 * rehashed. Each morsel is written, read back, and grouped with [[GroupByKey.groupLongs]]. A known
 * key appends its values. A new key goes at the end. `1L` and `1L << 32` stay distinct. The
 * appended groups are not bounded by the budget.
 *
 * A morsel whose table does not fit throws [[ClosedKernelRefusal]] before that table is allocated.
 * The store deletes its files on that throw.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.MutableDataStructures",
  ),
)
private[scarlet] object LongGroupByKeySpill:

  private val filesWritten = new AtomicLong
  private val lastDirectory = new AtomicReference("")

  /**
   * Estimated bytes of the long group table for `rows` input rows.
   *
   * The key in each slot is 8 bytes. Repeated keys are still charged as their own slots, because
   * the table is sized from the row count. The published arrays are not included. Zero rows
   * publish an empty batch and allocate nothing, so the estimate is zero.
   */
  private[scarlet] def inputRowBytes(rows: Long): Long =
    if (rows <= 0L) then 0L
    else
      val capped = if (rows > Int.MaxValue.toLong) Int.MaxValue else rows.toInt
      val slots = ColumnHash.tableSize(capped).toLong
      slots * (8L + 1L + 4L + 4L + 4L)

  /** True when spill is on and this bucket's table estimate is strictly above the budget. */
  private[scarlet] def exceeds(bucket: ColumnBatch, conf: ScarletConf): Boolean =
    conf.spillEnabled && inputRowBytes(bucket.length.toLong) > conf.spillBudgetBytes

  /**
   * Groups one exchanged bucket. Over budget, spills it and returns the merged groups. The caller
   * groups buckets one at a time, so this store is closed before the next bucket starts.
   */
  private[scarlet] def finish(bucket: ColumnBatch, conf: ScarletConf): GroupedLongBatch =
    if (exceeds(bucket, conf)) then spill(bucket, conf)
    else GroupByKey.groupLongs(bucket)

  /** Spill files published since the last [[resetForTest]]. */
  private[scarlet] def filesWrittenForTest: Long = filesWritten.get

  /** Directory of the store most recently opened, or empty when none has been. */
  private[scarlet] def lastDirectoryForTest: String = lastDirectory.get

  /** Clears the file counter and the recorded directory. */
  private[scarlet] def resetForTest(): Unit =
    filesWritten.set(0L)
    lastDirectory.set("")

  private def spill(bucket: ColumnBatch, conf: ScarletConf): GroupedLongBatch =
    val batchSize = conf.columnarBatchSize
    require(batchSize > 0, s"columnar batch size must be positive, got $batchSize")
    val morsels = ColumnBatches.slice(bucket, batchSize)
    val directory = SpillStore.directory(conf)
    lastDirectory.set(directory.toString)
    SpillStore.withSpill(directory, conf.spillMaxFiles, conf.spillMaxBytes) { store =>
      morsels.foldLeft(GroupedLongBatch.empty) { (accumulator, morsel) =>
        val rows = morsel.length.toLong
        val bytes = inputRowBytes(rows)
        if (bytes > conf.spillBudgetBytes) then
          throw new ClosedKernelRefusal(rows, bytes, conf.spillBudgetBytes)
        val file = publish(store, morsel)
        val grouped = GroupByKey.groupLongs(readOne(file))
        file.delete()
        append(accumulator, grouped)
      }
    }

  private def publish(store: SpillStore, morsel: ColumnBatch): SpillFile =
    val file = store.write(Vector(morsel))
    val _ = filesWritten.incrementAndGet()
    file

  private def readOne(file: SpillFile): ColumnBatch =
    file.read() match
      case Vector(morsel) => morsel
      case other =>
        throw new IllegalStateException(s"spill file holds ${other.length} batches")

  /** Known keys append values. A new key is added at the end. Equality is `Long` equality. */
  private def append(base: GroupedLongBatch, more: GroupedLongBatch): GroupedLongBatch =
    if (base.groups == 0) then more
    else if (more.groups == 0) then base
    else
      val order = LinkedHashMap.empty[Long, ArrayBuffer[Long]]
      var group = 0
      while (group < base.groups) {
        order.put(
          base.keys.values(group),
          copyValues(base.values, base.offsets(group), base.offsets(group + 1)),
        )
        group += 1
      }
      group = 0
      while (group < more.groups) {
        val values = order.getOrElseUpdate(more.keys.values(group), ArrayBuffer.empty[Long])
        val from = more.offsets(group)
        val until = more.offsets(group + 1)
        var row = from
        while (row < until) {
          values += more.values(row)
          row += 1
        }
        group += 1
      }
      publishGroups(order)

  private def copyValues(values: Array[Long], from: Int, until: Int): ArrayBuffer[Long] =
    val copied = ArrayBuffer.empty[Long]
    var row = from
    while (row < until) {
      copied += values(row)
      row += 1
    }
    copied

  private def publishGroups(order: LinkedHashMap[Long, ArrayBuffer[Long]]): GroupedLongBatch =
    val groups = order.size
    val keys = new Array[Long](groups)
    val offsets = new Array[Int](groups + 1)
    val total = order.valuesIterator.map(_.length).sum
    val values = new Array[Long](total)
    var group = 0
    var cursor = 0
    order.foreach { (key, grouped) =>
      keys(group) = key
      offsets(group) = cursor
      val groupedValues = grouped.iterator
      while (groupedValues.hasNext) {
        values(cursor) = groupedValues.next()
        cursor += 1
      }
      group += 1
    }
    offsets(groups) = cursor
    GroupedLongBatch(Int64Column.of(keys, Validity.allValid(groups), groups), offsets, values)
