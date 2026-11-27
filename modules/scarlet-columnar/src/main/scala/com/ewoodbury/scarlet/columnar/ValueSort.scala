package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * In-memory value sort of published long pairs and string pairs.
 *
 * Order is effective value descending, then effective key ascending, then input order. A null long
 * orders as zero and stays null. A null string key orders as `""` and stays null. A null int value
 * orders as zero and stays null. Rank boundaries are [[RankBounds]]. There is no spill. This is
 * not [[RankSplitLong]] and not [[StringRankSplit]].
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object ValueSort:

  def longPairs(batches: Vector[ColumnBatch], partitions: Int): Vector[ColumnBatch] =
    require(partitions > 0, s"numPartitions must be positive, got $partitions")
    require(batches.nonEmpty, "value sort needs at least one batch")
    val rows = rowCount(batches)
    if (rows == 0) then
      batches.foreach(checkedLong)
      Vector.fill(partitions)(ColumnBatches.emptyLongPairs)
    else
      val order = longOrder(batches, rows)
      Arrays.sort(order, longCompare)
      emit(rows, partitions, ColumnBatches.emptyLongPairs) { (start, size) =>
        gatherLong(batches, order, start, size)
      }

  def stringPairs(batches: Vector[ColumnBatch], partitions: Int): Vector[ColumnBatch] =
    require(partitions > 0, s"numPartitions must be positive, got $partitions")
    require(batches.nonEmpty, "value sort needs at least one batch")
    val rows = rowCount(batches)
    if (rows == 0) then
      batches.foreach(checkedString)
      Vector.fill(partitions)(ColumnBatches.emptyStringPairs)
    else
      val order = stringOrder(batches, rows)
      Arrays.sort(order, stringCompare)
      emit(rows, partitions, ColumnBatches.emptyStringPairs) { (start, size) =>
        gatherString(batches, order, start, size)
      }

  private def emit(
      rows: Int,
      partitions: Int,
      empty: => ColumnBatch,
  )(slice: (Int, Int) => ColumnBatch): Vector[ColumnBatch] =
    val sizes = RankBounds.outputSizes(rows, partitions)
    Vector.tabulate(partitions) { part =>
      val size = sizes(part)
      if (size == 0) then empty
      else slice(RankBounds.firstRank(part, rows, partitions), size)
    }

  private def rowCount(batches: Vector[ColumnBatch]): Int =
    batches.foldLeft(0)((total, batch) => total + batch.length)

  private def longOrder(batches: Vector[ColumnBatch], rows: Int): Array[LongRef] =
    val order = new Array[LongRef](rows)
    var batch = 0
    var cursor = 0
    while (batch < batches.length) {
      val (keys, values) = checkedLong(batches(batch))
      val keyWords = keys.validity.words
      val valueWords = values.validity.words
      var row = 0
      while (row < batches(batch).length) {
        val key = if (Validity.isSet(keyWords, row)) keys.values(row) else 0L
        val value = if (Validity.isSet(valueWords, row)) values.values(row) else 0L
        order(cursor) = new LongRef(batch, row, key, value)
        cursor += 1
        row += 1
      }
      batch += 1
    }
    order

  private def stringOrder(batches: Vector[ColumnBatch], rows: Int): Array[StringRef] =
    val order = new Array[StringRef](rows)
    var batch = 0
    var cursor = 0
    while (batch < batches.length) {
      val (keys, values) = checkedString(batches(batch))
      val keyWords = keys.validity.words
      val valueWords = values.validity.words
      var row = 0
      while (row < batches(batch).length) {
        val key =
          if (Validity.isSet(keyWords, row)) keys.dictionary(keys.codes(row)) else ""
        val value = if (Validity.isSet(valueWords, row)) values.values(row) else 0
        order(cursor) = new StringRef(batch, row, key, value)
        cursor += 1
        row += 1
      }
      batch += 1
    }
    order

  private def gatherLong(
      batches: Vector[ColumnBatch],
      order: Array[LongRef],
      start: Int,
      size: Int,
  ): ColumnBatch =
    val keys = new Array[Long](size)
    val values = new Array[Long](size)
    val keyWords = Validity.allocate(size)
    val valueWords = Validity.allocate(size)
    var out = 0
    while (out < size) {
      val ref = order(start + out)
      val (srcKeys, srcValues) = checkedLong(batches(ref.batch))
      if (Validity.isSet(srcKeys.validity.words, ref.row)) then
        keys(out) = srcKeys.values(ref.row)
        Validity.setBit(keyWords, out)
      if (Validity.isSet(srcValues.validity.words, ref.row)) then
        values(out) = srcValues.values(ref.row)
        Validity.setBit(valueWords, out)
      out += 1
    }
    RankScatterLong.pairs(keys, keyWords, values, valueWords, size)

  private def gatherString(
      batches: Vector[ColumnBatch],
      order: Array[StringRef],
      start: Int,
      size: Int,
  ): ColumnBatch =
    val sink = new StringRankScatter.Sink(size)
    var out = 0
    while (out < size) {
      val ref = order(start + out)
      val (srcKeys, srcValues) = checkedString(batches(ref.batch))
      val keyOn = Validity.isSet(srcKeys.validity.words, ref.row)
      val valueOn = Validity.isSet(srcValues.validity.words, ref.row)
      val key = if (keyOn) srcKeys.dictionary(srcKeys.codes(ref.row)) else ""
      val value = if (valueOn) srcValues.values(ref.row) else 0
      sink.write(keyOn, key, valueOn, value)
      out += 1
    }
    sink.toBatch

  private def checkedLong(batch: ColumnBatch): (Int64Column, Int64Column) =
    require(
      batch.columns.length == 2,
      s"value sort expected two columns, got ${batch.columns.length}",
    )
    val keys = ColumnHash.int64At(batch, 0)
    val values = ColumnHash.int64At(batch, 1)
    require(
      keys.length == batch.length && values.length == batch.length,
      s"key length ${keys.length} and value length ${values.length} disagree with ${batch.length}",
    )
    (keys, values)

  private def checkedString(batch: ColumnBatch): (DictUtf8Column, Int32Column) =
    require(
      batch.columns.length == 2,
      s"value sort expected two columns, got ${batch.columns.length}",
    )
    val keys = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
    val values = ColumnHash.int32At(batch, 1)
    require(
      keys.length == batch.length && values.length == batch.length,
      s"key length ${keys.length} and value length ${values.length} disagree with ${batch.length}",
    )
    (keys, values)

  private val longCompare: java.util.Comparator[LongRef] = (left, right) =>
    val byValue = java.lang.Long.compare(right.value, left.value)
    if (byValue != 0) then byValue
    else
      val byKey = java.lang.Long.compare(left.key, right.key)
      if (byKey != 0) then byKey
      else
        val byBatch = java.lang.Integer.compare(left.batch, right.batch)
        if (byBatch != 0) then byBatch else java.lang.Integer.compare(left.row, right.row)

  private val stringCompare: java.util.Comparator[StringRef] = (left, right) =>
    val byValue = java.lang.Integer.compare(right.value, left.value)
    if (byValue != 0) then byValue
    else
      val byKey = left.key.compareTo(right.key)
      if (byKey != 0) then byKey
      else
        val byBatch = java.lang.Integer.compare(left.batch, right.batch)
        if (byBatch != 0) then byBatch else java.lang.Integer.compare(left.row, right.row)

  private final class LongRef(val batch: Int, val row: Int, val key: Long, val value: Long)

  private final class StringRef(val batch: Int, val row: Int, val key: String, val value: Int)
