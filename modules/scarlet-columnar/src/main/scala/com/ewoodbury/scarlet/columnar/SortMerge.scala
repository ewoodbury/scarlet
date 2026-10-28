package com.ewoodbury.scarlet.columnar

/**
 * In-memory sort-merge inner join of one co-located `Int32` pair partition.
 *
 * Null keys are dropped on both sides before the sort. A null slot stores payload `0`, so a reader
 * that skips the validity bit would match key `0`. The sort orders row indexes with a primitive
 * comparator and then gathers keys and values. It does not box, and it does not read a null
 * payload.
 *
 * The merge walks two cursors. The smaller key advances. On equality, [[groupEnd]] drains the
 * whole equal-key group on both sides, the cartesian product is written, and both cursors advance
 * past those groups. That drain is the only group walk. [[SortMergeRuns]] applies it across every
 * spilled run. Stability is not required for the multiset. This sort is stable so equal keys keep
 * input order.
 *
 * Output columns are `(key, left, right)`, the same schema as the hash join. `build` is the left
 * side and `probe` is the right side. [[joinAll]] zips exchanged partitions by index. An empty
 * partition keeps its index, which is what makes the two sides line up. Each pair is one batch.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object SortMerge:

  def join(build: ColumnBatch, probe: ColumnBatch): ColumnBatch =
    val left = sortedSide(build)
    val right = sortedSide(probe)
    if (left.length == 0 || right.length == 0) then emptyJoin
    else emit(left, right, matchCount(left, right))

  /**
   * Merge partition `i` of `build` with partition `i` of `probe`. The vectors are the same length,
   * one batch per exchanged bucket, including a bucket with no rows.
   */
  def joinAll(build: Vector[ColumnBatch], probe: Vector[ColumnBatch]): Vector[ColumnBatch] =
    require(
      build.length == probe.length,
      s"partition counts ${build.length} and ${probe.length} differ",
    )
    Vector.tabulate(build.length) { index =>
      val left = build.lift(index).getOrElse {
        throw new IllegalArgumentException(s"missing build partition $index")
      }
      val right = probe.lift(index).getOrElse {
        throw new IllegalArgumentException(s"missing probe partition $index")
      }
      join(left, right)
    }

  /**
   * Drops null keys and returns the remaining pairs sorted by key. A spilled morsel uses this when
   * the partition was not sorted as one side. Equal keys keep their input order.
   */
  private[scarlet] def sortedPairs(batch: ColumnBatch): ColumnBatch =
    val side = sortedSide(batch)
    if (side.length == 0) then emptyPairs
    else
      new ColumnBatch(
        pairSchema,
        Vector(
          Int32Column.of(side.keys, Validity.allValid(side.length), side.length),
          Int32Column
            .of(side.values, Validity.fromWords(side.valueWords, side.length), side.length),
        ),
        side.length,
      )

  /**
   * First row after the equal-key group that starts at `start`. The side is sorted, so the group
   * is a contiguous run. Callers advance to this index. They do not step inside the group.
   */
  private def groupEnd(keys: Array[Int], start: Int, length: Int): Int =
    val key = keys(start)
    var end = start + 1
    while (end < length && keys(end) == key) {
      end += 1
    }
    end

  private def matchCount(left: SortedSide, right: SortedSide): Int =
    var leftAt = 0
    var rightAt = 0
    var total = 0
    while (leftAt < left.length && rightAt < right.length) {
      val order = Integer.compare(left.keys(leftAt), right.keys(rightAt))
      if (order < 0) then leftAt = groupEnd(left.keys, leftAt, left.length)
      else if (order > 0) then rightAt = groupEnd(right.keys, rightAt, right.length)
      else
        val leftEnd = groupEnd(left.keys, leftAt, left.length)
        val rightEnd = groupEnd(right.keys, rightAt, right.length)
        total = addMatches(total, leftEnd - leftAt, rightEnd - rightAt)
        leftAt = leftEnd
        rightAt = rightEnd
    }
    total

  private def emit(left: SortedSide, right: SortedSide, rows: Int): ColumnBatch =
    if (rows == 0) then emptyJoin
    else
      val keys = new Array[Int](rows)
      val leftValues = new Array[Int](rows)
      val rightValues = new Array[Int](rows)
      val leftWords = Validity.allocate(rows)
      val rightWords = Validity.allocate(rows)
      var leftAt = 0
      var rightAt = 0
      var filled = 0
      while (leftAt < left.length && rightAt < right.length) {
        val order = Integer.compare(left.keys(leftAt), right.keys(rightAt))
        if (order < 0) then leftAt = groupEnd(left.keys, leftAt, left.length)
        else if (order > 0) then rightAt = groupEnd(right.keys, rightAt, right.length)
        else
          val leftEnd = groupEnd(left.keys, leftAt, left.length)
          val rightEnd = groupEnd(right.keys, rightAt, right.length)
          val key = left.keys(leftAt)
          var leftRow = leftAt
          while (leftRow < leftEnd) {
            var rightRow = rightAt
            while (rightRow < rightEnd) {
              keys(filled) = key
              if (Validity.isSet(left.valueWords, leftRow)) then
                leftValues(filled) = left.values(leftRow)
                Validity.setBit(leftWords, filled)
              if (Validity.isSet(right.valueWords, rightRow)) then
                rightValues(filled) = right.values(rightRow)
                Validity.setBit(rightWords, filled)
              filled += 1
              rightRow += 1
            }
            leftRow += 1
          }
          leftAt = leftEnd
          rightAt = rightEnd
      }
      require(filled == rows, s"sort-merge wrote $filled rows and counted $rows")
      new ColumnBatch(
        joinSchema,
        Vector(
          Int32Column.of(keys, Validity.allValid(rows), rows),
          Int32Column.of(leftValues, Validity.fromWords(leftWords, rows), rows),
          Int32Column.of(rightValues, Validity.fromWords(rightWords, rows), rows),
        ),
        rows,
      )

  private def addMatches(total: Int, leftRows: Int, rightRows: Int): Int =
    val next = total.toLong + leftRows.toLong * rightRows.toLong
    if (next > Int.MaxValue.toLong) then
      throw new IllegalStateException(s"sort-merge output reached $next rows")
    else next.toInt

  private def sortedSide(batch: ColumnBatch): SortedSide =
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    val rows = keys.length
    val keyWords = keys.validity.words
    var present = 0
    var row = 0
    while (row < rows) {
      if (Validity.isSet(keyWords, row)) then present += 1
      row += 1
    }
    if (present == 0) then SortedSide.empty
    else
      val order = new Array[Int](present)
      var at = 0
      row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          order(at) = row
          at += 1
        row += 1
      }
      sortIndices(keys.values, order)
      gather(keys.values, values.values, values.validity.words, order)

  /** Sort `order` by `keys(order(i))`. `order` holds only rows whose key is present. */
  private def sortIndices(keys: Array[Int], order: Array[Int]): Unit =
    val rows = order.length
    if (rows > 1) then
      val buffer = new Array[Int](rows)
      var src = order
      var dst = buffer
      var sortedInOrder = true
      var width = 1
      while (width < rows) {
        var start = 0
        while (start < rows) {
          val mid = math.min(start.toLong + width.toLong, rows.toLong).toInt
          val end = math.min(start.toLong + (width.toLong << 1), rows.toLong).toInt
          mergeIndices(keys, src, dst, start, mid, end)
          start = end
        }
        val previous = src
        src = dst
        dst = previous
        sortedInOrder = !sortedInOrder
        val grown = width.toLong << 1
        width = if (grown >= rows.toLong) rows else grown.toInt
      }
      if (!sortedInOrder) then System.arraycopy(src, 0, order, 0, rows)

  private def mergeIndices(
      keys: Array[Int],
      src: Array[Int],
      dst: Array[Int],
      start: Int,
      mid: Int,
      end: Int,
  ): Unit =
    var left = start
    var right = mid
    var out = start
    while (left < mid && right < end) {
      if (Integer.compare(keys(src(left)), keys(src(right))) <= 0) then
        dst(out) = src(left)
        left += 1
      else
        dst(out) = src(right)
        right += 1
      out += 1
    }
    while (left < mid) {
      dst(out) = src(left)
      left += 1
      out += 1
    }
    while (right < end) {
      dst(out) = src(right)
      right += 1
      out += 1
    }

  private def gather(
      keys: Array[Int],
      values: Array[Int],
      valueWords: Array[Long],
      order: Array[Int],
  ): SortedSide =
    val count = order.length
    val gatheredKeys = new Array[Int](count)
    val gatheredValues = new Array[Int](count)
    val gatheredWords = Validity.allocate(count)
    var out = 0
    while (out < count) {
      val row = order(out)
      gatheredKeys(out) = keys(row)
      if (Validity.isSet(valueWords, row)) then
        gatheredValues(out) = values(row)
        Validity.setBit(gatheredWords, out)
      out += 1
    }
    SortedSide(gatheredKeys, gatheredValues, gatheredWords, count)

  private def emptyPairs: ColumnBatch =
    new ColumnBatch(
      pairSchema,
      Vector(
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
      ),
      0,
    )

  private def emptyJoin: ColumnBatch =
    new ColumnBatch(
      joinSchema,
      Vector(
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
      ),
      0,
    )

  private val pairSchema: Vector[LogicalType] =
    Vector(LogicalType.Int32, LogicalType.Int32)

  private val joinSchema: Vector[LogicalType] =
    Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32)

  private final class SortedSide(
      val keys: Array[Int],
      val values: Array[Int],
      val valueWords: Array[Long],
      val length: Int,
  )

  private object SortedSide:
    def empty: SortedSide =
      SortedSide(Array.emptyIntArray, Array.emptyIntArray, Array.emptyLongArray, 0)
