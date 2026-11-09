package com.ewoodbury.scarlet.columnar

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Spills one post-exchange string `groupByKey` bucket that does not fit in its budget.
 *
 * The estimate is the int group table for the dictionary codes, plus `dictionaryEntries * 8` and
 * the UTF-8 payload. An entry no row uses is charged. A null key is an empty slot, not a
 * dictionary entry. Zero rows allocate nothing, and the dictionary is not added. A bucket at or
 * under the budget, and every bucket while spill is off, is grouped in memory and opens no store.
 * Spill off does not throw [[ClosedKernelRefusal]].
 *
 * A bucket over the budget is cut with [[ColumnBatches.slice]] in input order. The bucket is not
 * rehashed. A slice shares the parent dictionary, so each morsel is compacted before it is charged
 * or written. Otherwise one row would be charged for strings that belong to other pieces. Each
 * compacted morsel is written, read back, and grouped with [[GroupByKey.groupStrings]]. Groups are
 * merged by string equality, so codes from two dictionaries are not compared. `""` is a present
 * key. `"é"` and `"e\u0301"` stay distinct. The appended groups are not bounded by the budget.
 *
 * A compacted morsel whose estimate does not fit throws [[ClosedKernelRefusal]] before
 * [[GroupByKey.groupStrings]] allocates that table. The store deletes its files on that throw.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.MutableDataStructures",
  ),
)
private[scarlet] object StringGroupByKeySpill:

  private val filesWritten = new AtomicLong
  private val lastDirectory = new AtomicReference("")

  /**
   * Estimated bytes of the string group table for this batch.
   *
   * The open-addressed table is charged at [[ColumnHash.tableSize]] of the row count. Each slot is
   * a 4-byte code, an occupancy byte, a slot index, a group index, and a count. The dictionary
   * term is `entries * 8` plus the UTF-8 payload, including an entry no row uses. Zero rows
   * publish an empty batch and allocate nothing, so the estimate is zero and the dictionary is not
   * added.
   */
  private[scarlet] def inputRowBytes(batch: ColumnBatch): Long =
    val rows = batch.length.toLong
    if (rows <= 0L) then 0L
    else
      val capped = if (rows > Int.MaxValue.toLong) Int.MaxValue else rows.toInt
      val slots = ColumnHash.tableSize(capped).toLong
      val keys = utf8At(batch)
      slots * (4L + 1L + 4L + 4L + 4L) + keys.dictionary.length.toLong * 8L +
        utf8Payload(keys.dictionary)

  /** True when spill is on and this bucket's estimate is strictly above the budget. */
  private[scarlet] def exceeds(bucket: ColumnBatch, conf: ScarletConf): Boolean =
    conf.spillEnabled && inputRowBytes(bucket) > conf.spillBudgetBytes

  /**
   * Groups one exchanged bucket. Over budget, spills it and returns the merged groups. The caller
   * groups buckets one at a time, so this store is closed before the next bucket starts.
   */
  private[scarlet] def finish(bucket: ColumnBatch, conf: ScarletConf): GroupedStringBatch =
    if (exceeds(bucket, conf)) then spill(bucket, conf)
    else GroupByKey.groupStrings(bucket)

  /** Spill files published since the last [[resetForTest]]. */
  private[scarlet] def filesWrittenForTest: Long = filesWritten.get

  /** Directory of the store most recently opened, or empty when none has been. */
  private[scarlet] def lastDirectoryForTest: String = lastDirectory.get

  /** Clears the file counter and the recorded directory. */
  private[scarlet] def resetForTest(): Unit =
    filesWritten.set(0L)
    lastDirectory.set("")

  private def spill(bucket: ColumnBatch, conf: ScarletConf): GroupedStringBatch =
    val batchSize = conf.columnarBatchSize
    require(batchSize > 0, s"columnar batch size must be positive, got $batchSize")
    val morsels = ColumnBatches.slice(bucket, batchSize)
    val directory = SpillStore.directory(conf)
    lastDirectory.set(directory.toString)
    SpillStore.withSpill(directory, conf.spillMaxFiles, conf.spillMaxBytes) { store =>
      morsels.foldLeft(GroupedStringBatch.empty) { (accumulator, morsel) =>
        val compact = ColumnBatches.compactDictionaries(morsel)
        val rows = compact.length.toLong
        val bytes = inputRowBytes(compact)
        if (bytes > conf.spillBudgetBytes) then
          throw new ClosedKernelRefusal(rows, bytes, conf.spillBudgetBytes)
        val file = publish(store, compact)
        val grouped = GroupByKey.groupStrings(readOne(file))
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

  /**
   * Known keys append values. A new key is added at the end. Match is string equality, so two
   * dictionaries can merge. `""` matches `""`. `"é"` does not match `"e\u0301"`.
   */
  private def append(base: GroupedStringBatch, more: GroupedStringBatch): GroupedStringBatch =
    if (base.groups == 0) then more
    else if (more.groups == 0) then base
    else
      val order = LinkedHashMap.empty[String, ArrayBuffer[Int]]
      var group = 0
      while (group < base.groups) {
        order.put(
          base.keys.dictionary(base.keys.codes(group)),
          copyValues(base.values, base.offsets(group), base.offsets(group + 1)),
        )
        group += 1
      }
      group = 0
      while (group < more.groups) {
        val key = more.keys.dictionary(more.keys.codes(group))
        val values = order.getOrElseUpdate(key, ArrayBuffer.empty[Int])
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

  private def copyValues(values: Array[Int], from: Int, until: Int): ArrayBuffer[Int] =
    val copied = ArrayBuffer.empty[Int]
    var row = from
    while (row < until) {
      copied += values(row)
      row += 1
    }
    copied

  private def publishGroups(order: LinkedHashMap[String, ArrayBuffer[Int]]): GroupedStringBatch =
    val groups = order.size
    val dictionary = new Array[String](groups)
    val codes = new Array[Int](groups)
    val offsets = new Array[Int](groups + 1)
    val total = order.valuesIterator.map(_.length).sum
    val values = new Array[Int](total)
    var group = 0
    var cursor = 0
    order.foreach { (key, grouped) =>
      dictionary(group) = key
      codes(group) = group
      offsets(group) = cursor
      val groupedValues = grouped.iterator
      while (groupedValues.hasNext) {
        values(cursor) = groupedValues.next()
        cursor += 1
      }
      group += 1
    }
    offsets(groups) = cursor
    GroupedStringBatch(
      DictUtf8Column.of(dictionary, codes, Validity.allValid(groups), groups),
      offsets,
      values,
    )

  private def utf8Payload(dictionary: Array[String]): Long =
    dictionary.iterator
      .map(entry => entry.getBytes(StandardCharsets.UTF_8).length.toLong)
      .sum

  private def utf8At(batch: ColumnBatch): DictUtf8Column =
    ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
