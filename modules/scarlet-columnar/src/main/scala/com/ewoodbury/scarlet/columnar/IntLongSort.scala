package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * In-memory sort of published `(Int32, Int64)` pairs.
 *
 * Key order is the int ascending. Value order is the long descending, then the int ascending. A
 * null orders as zero and stays null. Ties keep input order. Rank boundaries are [[RankBounds]].
 * There is no spill. This is not [[RankSplitLong]] and not [[ValueSort]].
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object IntLongSort:

  private val Schema = Vector(LogicalType.Int32, LogicalType.Int64)

  def byKey(batches: Vector[ColumnBatch], partitions: Int): Vector[ColumnBatch] =
    sorted(batches, partitions, keyCompare)

  def byValueDesc(batches: Vector[ColumnBatch], partitions: Int): Vector[ColumnBatch] =
    sorted(batches, partitions, valueCompare)

  private def sorted(
      batches: Vector[ColumnBatch],
      partitions: Int,
      compare: java.util.Comparator[Ref],
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"numPartitions must be positive, got $partitions")
    require(batches.nonEmpty, "int-long sort needs at least one batch")
    val rows = batches.foldLeft(0)((total, batch) => total + batch.length)
    if (rows == 0) then
      batches.foreach(checked)
      Vector.fill(partitions)(empty)
    else
      val order = refs(batches, rows)
      Arrays.sort(order, compare)
      val sizes = RankBounds.outputSizes(rows, partitions)
      Vector.tabulate(partitions) { part =>
        val size = sizes(part)
        if (size == 0) then empty
        else gather(batches, order, RankBounds.firstRank(part, rows, partitions), size)
      }

  private def refs(batches: Vector[ColumnBatch], rows: Int): Array[Ref] =
    val order = new Array[Ref](rows)
    var batch = 0
    var cursor = 0
    while (batch < batches.length) {
      val (keys, values) = checked(batches(batch))
      val keyWords = keys.validity.words
      val valueWords = values.validity.words
      var row = 0
      while (row < batches(batch).length) {
        val key = if (Validity.isSet(keyWords, row)) keys.values(row) else 0
        val value = if (Validity.isSet(valueWords, row)) values.values(row) else 0L
        order(cursor) = new Ref(batch, row, key, value)
        cursor += 1
        row += 1
      }
      batch += 1
    }
    order

  private def gather(
      batches: Vector[ColumnBatch],
      order: Array[Ref],
      start: Int,
      size: Int,
  ): ColumnBatch =
    val keys = new Array[Int](size)
    val values = new Array[Long](size)
    val keyWords = Validity.allocate(size)
    val valueWords = Validity.allocate(size)
    var out = 0
    while (out < size) {
      val ref = order(start + out)
      val (srcKeys, srcValues) = checked(batches(ref.batch))
      if (Validity.isSet(srcKeys.validity.words, ref.row)) then
        keys(out) = srcKeys.values(ref.row)
        Validity.setBit(keyWords, out)
      if (Validity.isSet(srcValues.validity.words, ref.row)) then
        values(out) = srcValues.values(ref.row)
        Validity.setBit(valueWords, out)
      out += 1
    }
    new ColumnBatch(
      Schema,
      Vector(
        Int32Column.of(keys, Validity.fromWords(keyWords, size), size),
        Int64Column.of(values, Validity.fromWords(valueWords, size), size),
      ),
      size,
    )

  private def checked(batch: ColumnBatch): (Int32Column, Int64Column) =
    require(
      batch.columns.length == 2,
      s"int-long sort expected two columns, got ${batch.columns.length}",
    )
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int64At(batch, 1)
    require(
      keys.length == batch.length && values.length == batch.length,
      s"key length ${keys.length} and value length ${values.length} disagree with ${batch.length}",
    )
    (keys, values)

  private def empty: ColumnBatch =
    val keys = Int32Column.of(Array.empty[Int], Validity.allValid(0), 0)
    val values = Int64Column.of(Array.empty[Long], Validity.allValid(0), 0)
    new ColumnBatch(Schema, Vector(keys, values), 0)

  private val keyCompare: java.util.Comparator[Ref] = (left, right) =>
    val byKey = java.lang.Integer.compare(left.key, right.key)
    if (byKey != 0) then byKey else tie(left, right)

  private val valueCompare: java.util.Comparator[Ref] = (left, right) =>
    val byValue = java.lang.Long.compare(right.value, left.value)
    if (byValue != 0) then byValue
    else
      val byKey = java.lang.Integer.compare(left.key, right.key)
      if (byKey != 0) then byKey else tie(left, right)

  private def tie(left: Ref, right: Ref): Int =
    val byBatch = java.lang.Integer.compare(left.batch, right.batch)
    if (byBatch != 0) then byBatch else java.lang.Integer.compare(left.row, right.row)

  private final class Ref(val batch: Int, val row: Int, val key: Int, val value: Long)
