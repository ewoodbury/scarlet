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
 * past those groups. That drain is the only group walk. [[SortMergeRuns]] applies it to the two
 * streams that remain after each spilled side is merged down to one run. Stability is not required
 * for the multiset. This sort is stable so equal keys keep input order.
 *
 * Output columns are `(key, left, right)`, the same schema as the hash join. `build` is the left
 * side and `probe` is the right side. [[joinAll]] zips exchanged partitions by index. An empty
 * partition keeps its index, which is what makes the two sides line up. Each pair is one batch.
 *
 * Outer joins use a separate walk. A null key is not sorted, so it cannot match key `0`. A
 * preserved null key is emitted with a clear bit on the key column. The other value's clear bit is
 * absence. A clear value bit means absence, so both value columns must be fully present.
 * [[leftOuter]], [[rightOuter]], and [[fullOuter]] fail when a value is null, rather than
 * publishing that null as absence. [[rightOuter]] is [[leftOuter]] on swapped inputs, with the
 * value columns swapped back. [[fullOuter]] emits a group that exists on only one side, and emits
 * both sides when one of them is empty.
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
   * Left outer join of one co-located partition. A null key does not match. Each left row whose
   * key is missing on the right is emitted once, including a null key, and the right value's bit
   * is clear. A right null key is dropped. A clear key bit is a null key. The payload is not read.
   */
  def leftOuter(left: ColumnBatch, right: ColumnBatch): ColumnBatch =
    outer(left, right, preserveRight = false)

  /**
   * Right outer join. Runs [[leftOuter]] with the inputs swapped, then swaps the value columns
   * back to `(key, left, right)`. Absence moves with the left value.
   */
  def rightOuter(left: ColumnBatch, right: ColumnBatch): ColumnBatch =
    swapValues(leftOuter(right, left))

  /**
   * Full outer join of one co-located partition. Equal keys emit the cartesian product. A group on
   * only one side emits that side, with the other value absent. A null key on either side is
   * emitted once. An empty side emits every row of the other side.
   */
  def fullOuter(left: ColumnBatch, right: ColumnBatch): ColumnBatch =
    outer(left, right, preserveRight = true)

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

  /** Left outer when `preserveRight` is false. Full outer when it is true. */
  private def outer(
      left: ColumnBatch,
      right: ColumnBatch,
      preserveRight: Boolean,
  ): ColumnBatch =
    // A clear value bit is absence. A null value must not be published as that bit.
    require(
      valuesPresent(left) && valuesPresent(right),
      "an outer join needs fully present value columns",
    )
    val leftSide = sortedSide(left)
    val rightSide = sortedSide(right)
    val leftNulls = nullKeys(left)
    val rightNulls = if (preserveRight) then nullKeys(right) else NullKeys.empty
    val rows = countOuter(
      leftSide,
      rightSide,
      preserveRight,
      leftNulls.length,
      rightNulls.length,
    )
    if (rows == 0) then emptyJoin
    else writeOuter(leftSide, rightSide, preserveRight, leftNulls, rightNulls, rows)

  private def countOuter(
      left: SortedSide,
      right: SortedSide,
      preserveRight: Boolean,
      leftNulls: Int,
      rightNulls: Int,
  ): Int =
    var leftAt = 0
    var rightAt = 0
    var total = 0
    while (leftAt < left.length && rightAt < right.length) {
      val order = Integer.compare(left.keys(leftAt), right.keys(rightAt))
      if (order < 0) then
        val end = groupEnd(left.keys, leftAt, left.length)
        total = addMatches(total, end - leftAt, 1)
        leftAt = end
      else if (order > 0) then
        val end = groupEnd(right.keys, rightAt, right.length)
        if (preserveRight) then total = addMatches(total, end - rightAt, 1)
        rightAt = end
      else
        val leftEnd = groupEnd(left.keys, leftAt, left.length)
        val rightEnd = groupEnd(right.keys, rightAt, right.length)
        total = addMatches(total, leftEnd - leftAt, rightEnd - rightAt)
        leftAt = leftEnd
        rightAt = rightEnd
    }
    while (leftAt < left.length) {
      val end = groupEnd(left.keys, leftAt, left.length)
      total = addMatches(total, end - leftAt, 1)
      leftAt = end
    }
    if (preserveRight) then
      while (rightAt < right.length) {
        val end = groupEnd(right.keys, rightAt, right.length)
        total = addMatches(total, end - rightAt, 1)
        rightAt = end
      }
    val withLeftNulls = addMatches(total, leftNulls, 1)
    if (preserveRight) then addMatches(withLeftNulls, rightNulls, 1) else withLeftNulls

  private def writeOuter(
      left: SortedSide,
      right: SortedSide,
      preserveRight: Boolean,
      leftNulls: NullKeys,
      rightNulls: NullKeys,
      rows: Int,
  ): ColumnBatch =
    val keys = new Array[Int](rows)
    val leftValues = new Array[Int](rows)
    val rightValues = new Array[Int](rows)
    val keyWords = Validity.allocate(rows)
    val leftWords = Validity.allocate(rows)
    val rightWords = Validity.allocate(rows)
    var leftAt = 0
    var rightAt = 0
    var filled = 0

    def emitMatched(leftRow: Int, rightRow: Int): Unit =
      keys(filled) = left.keys(leftRow)
      Validity.setBit(keyWords, filled)
      copyPresent(left.values, left.valueWords, leftRow, leftValues, leftWords, filled)
      copyPresent(right.values, right.valueWords, rightRow, rightValues, rightWords, filled)
      filled += 1

    def emitLeftOnly(leftRow: Int): Unit =
      keys(filled) = left.keys(leftRow)
      Validity.setBit(keyWords, filled)
      copyPresent(left.values, left.valueWords, leftRow, leftValues, leftWords, filled)
      filled += 1

    def emitRightOnly(rightRow: Int): Unit =
      keys(filled) = right.keys(rightRow)
      Validity.setBit(keyWords, filled)
      copyPresent(right.values, right.valueWords, rightRow, rightValues, rightWords, filled)
      filled += 1

    def emitNull(nulls: NullKeys, index: Int, onLeft: Boolean): Unit =
      val destValues = if (onLeft) then leftValues else rightValues
      val destWords = if (onLeft) then leftWords else rightWords
      copyPresent(nulls.values, nulls.valueWords, index, destValues, destWords, filled)
      filled += 1

    while (leftAt < left.length && rightAt < right.length) {
      val order = Integer.compare(left.keys(leftAt), right.keys(rightAt))
      if (order < 0) then
        val end = groupEnd(left.keys, leftAt, left.length)
        var row = leftAt
        while (row < end) {
          emitLeftOnly(row)
          row += 1
        }
        leftAt = end
      else if (order > 0) then
        val end = groupEnd(right.keys, rightAt, right.length)
        if (preserveRight) then
          var row = rightAt
          while (row < end) {
            emitRightOnly(row)
            row += 1
          }
        rightAt = end
      else
        val leftEnd = groupEnd(left.keys, leftAt, left.length)
        val rightEnd = groupEnd(right.keys, rightAt, right.length)
        var leftRow = leftAt
        while (leftRow < leftEnd) {
          var rightRow = rightAt
          while (rightRow < rightEnd) {
            emitMatched(leftRow, rightRow)
            rightRow += 1
          }
          leftRow += 1
        }
        leftAt = leftEnd
        rightAt = rightEnd
    }
    while (leftAt < left.length) {
      val end = groupEnd(left.keys, leftAt, left.length)
      var row = leftAt
      while (row < end) {
        emitLeftOnly(row)
        row += 1
      }
      leftAt = end
    }
    if (preserveRight) then
      while (rightAt < right.length) {
        val end = groupEnd(right.keys, rightAt, right.length)
        var row = rightAt
        while (row < end) {
          emitRightOnly(row)
          row += 1
        }
        rightAt = end
      }
    var nullAt = 0
    while (nullAt < leftNulls.length) {
      emitNull(leftNulls, nullAt, onLeft = true)
      nullAt += 1
    }
    if (preserveRight) then
      nullAt = 0
      while (nullAt < rightNulls.length) {
        emitNull(rightNulls, nullAt, onLeft = false)
        nullAt += 1
      }
    require(filled == rows, s"sort-merge wrote $filled rows and counted $rows")
    new ColumnBatch(
      joinSchema,
      Vector(
        Int32Column.of(keys, Validity.fromWords(keyWords, rows), rows),
        Int32Column.of(leftValues, Validity.fromWords(leftWords, rows), rows),
        Int32Column.of(rightValues, Validity.fromWords(rightWords, rows), rows),
      ),
      rows,
    )

  private def copyPresent(
      values: Array[Int],
      words: Array[Long],
      from: Int,
      dest: Array[Int],
      destWords: Array[Long],
      at: Int,
  ): Unit =
    if (Validity.isSet(words, from)) then
      dest(at) = values(from)
      Validity.setBit(destWords, at)

  /** Null-key values in input order. These rows are not part of [[sortedSide]]. */
  private def nullKeys(batch: ColumnBatch): NullKeys =
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    val width = keys.length
    var count = 0
    var row = 0
    while (row < width) {
      if (!Validity.isSet(keyWords, row)) then count += 1
      row += 1
    }
    if (count == 0) then NullKeys.empty
    else
      val kept = new Array[Int](count)
      val keptWords = Validity.allocate(count)
      var at = 0
      row = 0
      while (row < width) {
        if (!Validity.isSet(keyWords, row)) then
          if (Validity.isSet(valueWords, row)) then
            kept(at) = values.values(row)
            Validity.setBit(keptWords, at)
          at += 1
        row += 1
      }
      NullKeys(kept, keptWords, count)

  /** True when every live value cell is present. A null value is not absence. */
  private def valuesPresent(batch: ColumnBatch): Boolean =
    val values = ColumnHash.int32At(batch, 1)
    Validity.livePresent(values.validity, values.length)

  /** Restores `(key, left, right)` after a right outer ran as a left outer on swapped inputs. */
  private[scarlet] def swapValues(batch: ColumnBatch): ColumnBatch =
    batch.columns match
      case Seq(key, leftValue, rightValue) =>
        new ColumnBatch(
          Vector(key.logicalType, rightValue.logicalType, leftValue.logicalType),
          Vector(key, rightValue, leftValue),
          batch.length,
        )
      case other =>
        throw new IllegalArgumentException(s"join output has ${other.size} columns")

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

  /** Values whose key bit is clear. The key payload is not kept. */
  private final class NullKeys(
      val values: Array[Int],
      val valueWords: Array[Long],
      val length: Int,
  )

  private object NullKeys:
    def empty: NullKeys =
      NullKeys(Array.emptyIntArray, Array.emptyLongArray, 0)
