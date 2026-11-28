package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.TextRule

/**
 * Global sort and limit of wide batches.
 *
 * Sort concatenates batches in order, then orders one int, long, or string column and an optional
 * second of those kinds. A null number compares as zero and stays null. A null string compares as
 * empty and stays null. The last key keeps the input index, and a descending first key does not
 * reverse that index. The result is one batch. String dictionaries are rebuilt, because codes
 * belong to one batch. String order is [[TextRule.compare]], not dictionary code order.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object WideOrder:

  def sort(
      batches: Seq[ColumnBatch],
      ordinal: Int,
      descending: Boolean,
      second: Option[Int] = None,
  ): ColumnBatch =
    require(batches.nonEmpty, "sort needs a batch")
    val indexed = batches.toIndexedSeq
    sameWidth(indexed)
    val key = columnAt(atBatch(indexed, 0), ordinal)
    val total = indexed.foldLeft(0)((sum, batch) => sum + batch.length)
    second.foreach(next => requireSortable(indexed, next))
    key.logicalType match
      case LogicalType.Int32 | LogicalType.Int64 | LogicalType.Utf8Dict =>
        if total == 0 then atBatch(indexed, 0)
        else gather(indexed, order(indexed, ordinal, descending, second, total))
      case other =>
        throw new IllegalArgumentException(s"sort column $ordinal is $other")

  def limit(batches: Seq[ColumnBatch], count: Int): ColumnBatch =
    require(count >= 0, "limit must not be negative")
    require(batches.nonEmpty, "limit needs a batch")
    val indexed = batches.toIndexedSeq
    val total = indexed.foldLeft(0)((sum, batch) => sum + batch.length)
    val kept = if count < total then count else total
    val head = atBatch(indexed, 0)
    if kept == head.length && indexed.size == 1 then head
    else
      val order = new Array[Int](kept)
      var slot = 0
      while slot < kept do
        order(slot) = slot
        slot += 1
      gather(indexed, order)

  private def keysOf(batches: IndexedSeq[ColumnBatch], ordinal: Int, total: Int): Array[Long] =
    val keys = new Array[Long](total)
    var batch = 0
    var at = 0
    while batch < batches.size do
      val column = columnAt(atBatch(batches, batch), ordinal)
      val length = atBatch(batches, batch).length
      column match
        case ints: Int32Column =>
          var row = 0
          while row < length do
            keys(at) = if ints.isValid(row) then ints.values(row).toLong else 0L
            at += 1
            row += 1
        case longs: Int64Column =>
          var row = 0
          while row < length do
            keys(at) = if longs.isValid(row) then longs.values(row) else 0L
            at += 1
            row += 1
        case other =>
          throw new IllegalArgumentException(s"sort column $ordinal is ${other.logicalType}")
      batch += 1
    keys

  private def orderOf(keys: Array[Long], descending: Boolean): Array[Int] =
    sortIndices(
      keys.length,
      new IndexOrder:
        def before(left: Int, right: Int): Boolean =
          val cmp =
            if descending then java.lang.Long.compare(keys(right), keys(left))
            else java.lang.Long.compare(keys(left), keys(right))
          if cmp != 0 then cmp < 0 else left < right,
    )

  private def textsOf(batches: IndexedSeq[ColumnBatch], ordinal: Int, total: Int): Array[String] =
    val keys = new Array[String](total)
    var batch = 0
    var at = 0
    while batch < batches.size do
      val column = columnAt(atBatch(batches, batch), ordinal)
      val length = atBatch(batches, batch).length
      column match
        case texts: DictUtf8Column =>
          var row = 0
          while row < length do
            keys(at) = if texts.isValid(row) then texts.dictionary(texts.codes(row)) else ""
            at += 1
            row += 1
        case other =>
          throw new IllegalArgumentException(s"sort column $ordinal is ${other.logicalType}")
      batch += 1
    keys

  private def order(
      batches: IndexedSeq[ColumnBatch],
      ordinal: Int,
      descending: Boolean,
      second: Option[Int],
      total: Int,
  ): Array[Int] =
    val first = sortKey(batches, ordinal, descending, total)
    second match
      case None => orderOfKey(first)
      case Some(next) =>
        orderOfPair(first, sortKey(batches, next, descending = false, total))

  private def orderOfKey(key: SortKey): Array[Int] = key match
    case SortKey.Number(values, descending) => orderOf(values, descending)
    case SortKey.Text(values, descending) => orderOfText(values, descending)

  private def orderOfPair(first: SortKey, second: SortKey): Array[Int] =
    sortIndices(
      keyLength(first),
      new IndexOrder:
        def before(left: Int, right: Int): Boolean =
          val primary = keyCompare(first, left, right)
          if primary != 0 then primary < 0
          else
            val next = keyCompare(second, left, right)
            if next != 0 then next < 0 else left < right,
    )

  private def sortKey(
      batches: IndexedSeq[ColumnBatch],
      ordinal: Int,
      descending: Boolean,
      total: Int,
  ): SortKey =
    columnAt(atBatch(batches, 0), ordinal).logicalType match
      case LogicalType.Int32 | LogicalType.Int64 =>
        SortKey.Number(keysOf(batches, ordinal, total), descending)
      case LogicalType.Utf8Dict =>
        SortKey.Text(textsOf(batches, ordinal, total), descending)
      case other =>
        throw new IllegalArgumentException(s"sort column $ordinal is $other")

  private def keyCompare(key: SortKey, left: Int, right: Int): Int = key match
    case SortKey.Number(values, descending) =>
      if descending then java.lang.Long.compare(values(right), values(left))
      else java.lang.Long.compare(values(left), values(right))
    case SortKey.Text(values, descending) =>
      if descending then TextRule.compare(values(right), values(left))
      else TextRule.compare(values(left), values(right))

  private def keyLength(key: SortKey): Int = key match
    case SortKey.Number(values, _) => values.length
    case SortKey.Text(values, _) => values.length

  private def requireSortable(batches: IndexedSeq[ColumnBatch], ordinal: Int): Unit =
    columnAt(atBatch(batches, 0), ordinal).logicalType match
      case LogicalType.Int32 | LogicalType.Int64 | LogicalType.Utf8Dict => ()
      case other =>
        throw new IllegalArgumentException(s"sort column $ordinal is $other")

  private enum SortKey:
    case Number(values: Array[Long], descending: Boolean)
    case Text(values: Array[String], descending: Boolean)

  private def orderOfText(keys: Array[String], descending: Boolean): Array[Int] =
    sortIndices(
      keys.length,
      new IndexOrder:
        def before(left: Int, right: Int): Boolean =
          val cmp =
            if descending then TextRule.compare(keys(right), keys(left))
            else TextRule.compare(keys(left), keys(right))
          if cmp != 0 then cmp < 0 else left < right,
    )

  /** Orders row indexes. `before` is a strict total order, including the input index. */
  private abstract class IndexOrder:
    def before(left: Int, right: Int): Boolean

  private def sortIndices(length: Int, orderBy: IndexOrder): Array[Int] =
    val order = new Array[Int](length)
    var index = 0
    while index < length do
      order(index) = index
      index += 1
    if length < 2 then order
    else
      val scratch = new Array[Int](length)
      var width = 1
      var src = order
      var dst = scratch
      var swapped = false
      while width < length do
        var start = 0
        while start < length do
          val mid = math.min(start + width, length)
          val end = math.min(start + width + width, length)
          var left = start
          var right = mid
          var out = start
          while out < end do
            val takeLeft =
              if right == end then true
              else if left == mid then false
              else orderBy.before(src(left), src(right))
            if takeLeft then
              dst(out) = src(left)
              left += 1
            else
              dst(out) = src(right)
              right += 1
            out += 1
          start = end
        val swap = src
        src = dst
        dst = swap
        swapped = !swapped
        width += width
      if swapped then System.arraycopy(src, 0, order, 0, length)
      order

  private def gather(batches: IndexedSeq[ColumnBatch], order: Array[Int]): ColumnBatch =
    val starts = prefix(batches)
    val width = atBatch(batches, 0).columns.size
    val columns = Vector.tabulate(width)(ordinal => gatherColumn(batches, starts, ordinal, order))
    new ColumnBatch(columns.map(_.logicalType), columns, order.length)

  private def gatherColumn(
      batches: IndexedSeq[ColumnBatch],
      starts: Array[Int],
      ordinal: Int,
      order: Array[Int],
  ): Column =
    columnAt(atBatch(batches, 0), ordinal) match
      case _: Int32Column => gatherInts(batches, starts, ordinal, order)
      case _: Int64Column => gatherLongs(batches, starts, ordinal, order)
      case _: Float64Column => gatherDoubles(batches, starts, ordinal, order)
      case _: Float32Column => gatherFloats(batches, starts, ordinal, order)
      case _: BoolColumn => gatherBools(batches, starts, ordinal, order)
      case _: DictUtf8Column => gatherStrings(batches, starts, ordinal, order)
      case first: DecimalColumn => gatherDecimals(batches, starts, ordinal, order, first)

  private def gatherInts(
      batches: IndexedSeq[ColumnBatch],
      starts: Array[Int],
      ordinal: Int,
      order: Array[Int],
  ): Int32Column =
    val values = new Array[Int](order.length)
    val present = new Array[Boolean](order.length)
    var slot = 0
    while slot < order.length do
      val (batch, row) = locate(starts, order(slot))
      columnAt(atBatch(batches, batch), ordinal) match
        case column: Int32Column if column.isValid(row) =>
          values(slot) = column.values(row)
          present(slot) = true
        case _: Int32Column => ()
        case other => refuse(ordinal, other)
      slot += 1
    Int32Column.of(values, Validity.pack(order.length, row => present(row)), order.length)

  private def gatherLongs(
      batches: IndexedSeq[ColumnBatch],
      starts: Array[Int],
      ordinal: Int,
      order: Array[Int],
  ): Int64Column =
    val values = new Array[Long](order.length)
    val present = new Array[Boolean](order.length)
    var slot = 0
    while slot < order.length do
      val (batch, row) = locate(starts, order(slot))
      columnAt(atBatch(batches, batch), ordinal) match
        case column: Int64Column if column.isValid(row) =>
          values(slot) = column.values(row)
          present(slot) = true
        case _: Int64Column => ()
        case other => refuse(ordinal, other)
      slot += 1
    Int64Column.of(values, Validity.pack(order.length, row => present(row)), order.length)

  private def gatherDoubles(
      batches: IndexedSeq[ColumnBatch],
      starts: Array[Int],
      ordinal: Int,
      order: Array[Int],
  ): Float64Column =
    val values = new Array[Double](order.length)
    val present = new Array[Boolean](order.length)
    var slot = 0
    while slot < order.length do
      val (batch, row) = locate(starts, order(slot))
      columnAt(atBatch(batches, batch), ordinal) match
        case column: Float64Column if column.isValid(row) =>
          values(slot) = column.values(row)
          present(slot) = true
        case _: Float64Column => ()
        case other => refuse(ordinal, other)
      slot += 1
    Float64Column.of(values, Validity.pack(order.length, row => present(row)), order.length)

  private def gatherFloats(
      batches: IndexedSeq[ColumnBatch],
      starts: Array[Int],
      ordinal: Int,
      order: Array[Int],
  ): Float32Column =
    val values = new Array[Float](order.length)
    val present = new Array[Boolean](order.length)
    var slot = 0
    while slot < order.length do
      val (batch, row) = locate(starts, order(slot))
      columnAt(atBatch(batches, batch), ordinal) match
        case column: Float32Column if column.isValid(row) =>
          values(slot) = column.values(row)
          present(slot) = true
        case _: Float32Column => ()
        case other => refuse(ordinal, other)
      slot += 1
    Float32Column.of(values, Validity.pack(order.length, row => present(row)), order.length)

  private def gatherBools(
      batches: IndexedSeq[ColumnBatch],
      starts: Array[Int],
      ordinal: Int,
      order: Array[Int],
  ): BoolColumn =
    val values = new Array[Byte](order.length)
    val present = new Array[Boolean](order.length)
    var slot = 0
    while slot < order.length do
      val (batch, row) = locate(starts, order(slot))
      columnAt(atBatch(batches, batch), ordinal) match
        case column: BoolColumn if column.isValid(row) =>
          values(slot) = column.values(row)
          present(slot) = true
        case _: BoolColumn => ()
        case other => refuse(ordinal, other)
      slot += 1
    BoolColumn.of(values, Validity.pack(order.length, row => present(row)), order.length)

  private def gatherStrings(
      batches: IndexedSeq[ColumnBatch],
      starts: Array[Int],
      ordinal: Int,
      order: Array[Int],
  ): DictUtf8Column =
    val cells = Vector.tabulate(order.length) { slot =>
      val (batch, row) = locate(starts, order(slot))
      columnAt(atBatch(batches, batch), ordinal) match
        case column: DictUtf8Column if column.isValid(row) =>
          Some(column.dictionary(column.codes(row)))
        case _: DictUtf8Column => None
        case other => refuse(ordinal, other)
    }
    DictUtf8Column.fromNullable(cells)

  private def gatherDecimals(
      batches: IndexedSeq[ColumnBatch],
      starts: Array[Int],
      ordinal: Int,
      order: Array[Int],
      first: DecimalColumn,
  ): DecimalColumn =
    val values = new Array[Long](order.length)
    val present = new Array[Boolean](order.length)
    var slot = 0
    while slot < order.length do
      val (batch, row) = locate(starts, order(slot))
      columnAt(atBatch(batches, batch), ordinal) match
        case column: DecimalColumn
            if column.precision == first.precision && column.scale == first.scale =>
          if column.isValid(row) then
            values(slot) = column.values(row)
            present(slot) = true
        case other => refuse(ordinal, other)
      slot += 1
    DecimalColumn.of(
      first.precision,
      first.scale,
      values,
      Validity.pack(order.length, row => present(row)),
      order.length,
    )

  private def prefix(batches: IndexedSeq[ColumnBatch]): Array[Int] =
    val starts = new Array[Int](batches.size)
    var index = 1
    while index < batches.size do
      starts(index) = starts(index - 1) + atBatch(batches, index - 1).length
      index += 1
    starts

  private def locate(starts: Array[Int], index: Int): (Int, Int) =
    var batch = starts.length - 1
    while batch > 0 && index < starts(batch) do batch -= 1
    (batch, index - starts(batch))

  private def sameWidth(batches: IndexedSeq[ColumnBatch]): Unit =
    val width = atBatch(batches, 0).columns.size
    var index = 1
    while index < batches.size do
      if atBatch(batches, index).columns.size != width then
        throw new IllegalArgumentException("sort batches have different widths")
      index += 1

  private def atBatch(batches: IndexedSeq[ColumnBatch], index: Int): ColumnBatch =
    batches.lift(index).getOrElse {
      throw new IllegalArgumentException(s"batch $index outside ${batches.size}")
    }

  private def columnAt(batch: ColumnBatch, ordinal: Int): Column =
    batch.columns.lift(ordinal).getOrElse {
      throw new IllegalArgumentException(s"column $ordinal outside width ${batch.columns.size}")
    }

  private def refuse(ordinal: Int, column: Column): Nothing =
    throw new IllegalArgumentException(s"column $ordinal is ${column.logicalType}")
