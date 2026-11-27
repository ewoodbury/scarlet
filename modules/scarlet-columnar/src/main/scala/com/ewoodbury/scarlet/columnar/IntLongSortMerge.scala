package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Sort-merge joins of an `Int32` key and an `Int64` value on both sides.
 *
 * Each side is hash-partitioned on the int key, then each bucket is ordered by key and merged.
 * Equal keys emit the cartesian product in input order. A null key is not ordered, so it is not
 * key `0`, and it does not match. The preserved side keeps it and the other value is absent. A
 * null long is a clear validity bit and is not absence. Output is `(key, left, right)`. There is
 * no spill. This is not [[LongSortMerge]]: that kernel throws when a value bit is clear.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object IntLongSortMerge:

  private enum Place:
    case Inner, Left, Right, Full

  def inner(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    run(
      left,
      right,
      numPartitions,
      parallelism,
      carryLeft = false,
      carryRight = false,
      Place.Inner,
    )

  def leftOuter(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    run(left, right, numPartitions, parallelism, carryLeft = true, carryRight = false, Place.Left)

  def rightOuter(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    run(left, right, numPartitions, parallelism, carryLeft = false, carryRight = true, Place.Right)

  def fullOuter(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    run(left, right, numPartitions, parallelism, carryLeft = true, carryRight = true, Place.Full)

  private def run(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      carryLeft: Boolean,
      carryRight: Boolean,
      place: Place,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"shuffle partitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (skip(left, right, place)) then Vector.fill(numPartitions)(IntLongHashOuter.emptyBatch)
    else
      val leftBuckets = buckets(left, numPartitions, parallelism, carryLeft)
      val rightBuckets = buckets(right, numPartitions, parallelism, carryRight)
      MorselScheduler.map(leftBuckets, parallelism) { (part, leftBatch) =>
        merge(leftBatch, rightBuckets(part), place)
      }

  /** Left outer with no left rows, or right outer with no right rows, emits nothing. */
  private def skip(left: Vector[ColumnBatch], right: Vector[ColumnBatch], place: Place): Boolean =
    (left.isEmpty && right.isEmpty) || (place == Place.Inner && (left.isEmpty || right.isEmpty)) ||
      (place == Place.Left && left.isEmpty) || (place == Place.Right && right.isEmpty)

  private def buckets(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      carryNullKeys: Boolean,
  ): Vector[ColumnBatch] =
    if (batches.isEmpty) then Vector.fill(numPartitions)(IntLongHashOuter.emptyBatch)
    else
      HashExchange.partitionAll(
        batches,
        keyOrdinal = 0,
        numPartitions,
        parallelism,
        carryNullKeys = carryNullKeys,
      )

  private def merge(left: ColumnBatch, right: ColumnBatch, place: Place): ColumnBatch =
    val leftKeys = ColumnHash.int32At(left, 0)
    val leftValues = ColumnHash.int64At(left, 1)
    val rightKeys = ColumnHash.int32At(right, 0)
    val rightValues = ColumnHash.int64At(right, 1)
    val keepLeft = place == Place.Left || place == Place.Full
    val keepRight = place == Place.Right || place == Place.Full
    val leftOrder = present(leftKeys)
    val rightOrder = present(rightKeys)
    var outCapacity = math.max(16, leftKeys.length + rightKeys.length)
    var filled = 0
    var outKeys = new Array[Int](outCapacity)
    var leftVals = new Array[Long](outCapacity)
    var rightVals = new Array[Long](outCapacity)
    var keyWords = Validity.allocate(outCapacity)
    var leftWords = Validity.allocate(outCapacity)
    var rightWords = Validity.allocate(outCapacity)
    var leftAbs = Validity.allocate(outCapacity)
    var rightAbs = Validity.allocate(outCapacity)

    def grow(): Unit =
      if (filled == outCapacity) then
        outCapacity = ColumnHash.doubled(outCapacity)
        outKeys = Arrays.copyOf(outKeys, outCapacity)
        leftVals = Arrays.copyOf(leftVals, outCapacity)
        rightVals = Arrays.copyOf(rightVals, outCapacity)
        keyWords = Validity.growWords(keyWords, outCapacity)
        leftWords = Validity.growWords(leftWords, outCapacity)
        rightWords = Validity.growWords(rightWords, outCapacity)
        leftAbs = Validity.growWords(leftAbs, outCapacity)
        rightAbs = Validity.growWords(rightAbs, outCapacity)

    def add(
        key: Int,
        keyValid: Boolean,
        leftVal: Long,
        leftValid: Boolean,
        leftMiss: Boolean,
        rightVal: Long,
        rightValid: Boolean,
        rightMiss: Boolean,
    ): Unit =
      grow()
      outKeys(filled) = key
      if (keyValid) then Validity.setBit(keyWords, filled)
      if (leftValid) then
        leftVals(filled) = leftVal
        Validity.setBit(leftWords, filled)
      if (leftMiss) then Validity.setBit(leftAbs, filled)
      if (rightValid) then
        rightVals(filled) = rightVal
        Validity.setBit(rightWords, filled)
      if (rightMiss) then Validity.setBit(rightAbs, filled)
      filled += 1

    def valueAt(column: Int64Column, row: Int): Long = column.values(row)

    def validAt(column: Int64Column, row: Int): Boolean =
      Validity.isSet(column.validity.words, row)

    def matched(leftRow: Int, rightRow: Int): Unit =
      add(
        leftKeys.values(leftRow),
        true,
        valueAt(leftValues, leftRow),
        validAt(leftValues, leftRow),
        false,
        valueAt(rightValues, rightRow),
        validAt(rightValues, rightRow),
        false,
      )

    def leftOnly(row: Int, keyValid: Boolean): Unit =
      add(
        leftKeys.values(row),
        keyValid,
        valueAt(leftValues, row),
        validAt(leftValues, row),
        false,
        0L,
        false,
        true,
      )

    def rightOnly(row: Int, keyValid: Boolean): Unit =
      add(
        rightKeys.values(row),
        keyValid,
        0L,
        false,
        true,
        valueAt(rightValues, row),
        validAt(rightValues, row),
        false,
      )

    var i = 0
    var j = 0
    while (i < leftOrder.length && j < rightOrder.length) {
      val leftRow = leftOrder(i)
      val rightRow = rightOrder(j)
      val compared = Integer.compare(leftKeys.values(leftRow), rightKeys.values(rightRow))
      if (compared == 0) then
        val key = leftKeys.values(leftRow)
        var iEnd = i + 1
        while (iEnd < leftOrder.length && leftKeys.values(leftOrder(iEnd)) == key) iEnd += 1
        var jEnd = j + 1
        while (jEnd < rightOrder.length && rightKeys.values(rightOrder(jEnd)) == key) jEnd += 1
        var a = i
        while (a < iEnd) {
          var b = j
          while (b < jEnd) {
            matched(leftOrder(a), rightOrder(b))
            b += 1
          }
          a += 1
        }
        i = iEnd
        j = jEnd
      else if (compared < 0) then
        if (keepLeft) then leftOnly(leftRow, true)
        i += 1
      else
        if (keepRight) then rightOnly(rightRow, true)
        j += 1
    }
    while (i < leftOrder.length) {
      if (keepLeft) then leftOnly(leftOrder(i), true)
      i += 1
    }
    while (j < rightOrder.length) {
      if (keepRight) then rightOnly(rightOrder(j), true)
      j += 1
    }
    if (keepLeft) then emitNulls(leftKeys, leftOnly)
    if (keepRight) then emitNulls(rightKeys, rightOnly)
    if (filled == 0) then IntLongHashOuter.emptyBatch
    else
      publish(
        outKeys,
        leftVals,
        rightVals,
        keyWords,
        leftWords,
        rightWords,
        leftAbs,
        rightAbs,
        outCapacity,
        filled,
      )

  /** Present keys only. A clear bit stays out of the order, so a null is not sorted as zero. */
  private def present(keys: Int32Column): Array[Int] =
    val words = keys.validity.words
    var count = 0
    var row = 0
    while (row < keys.length) {
      if (Validity.isSet(words, row)) then count += 1
      row += 1
    }
    val order = new Array[Int](count)
    row = 0
    var filled = 0
    while (row < keys.length) {
      if (Validity.isSet(words, row)) then
        order(filled) = row
        filled += 1
      row += 1
    }
    heapSort(order, keys.values)
    order

  private def emitNulls(keys: Int32Column, emit: (Int, Boolean) => Unit): Unit =
    val words = keys.validity.words
    var row = 0
    while (row < keys.length) {
      if (!Validity.isSet(words, row)) then emit(row, false)
      row += 1
    }

  private def before(keys: Array[Int], left: Int, right: Int): Boolean =
    val compared = Integer.compare(keys(left), keys(right))
    compared < 0 || (compared == 0 && left < right)

  private def heapSort(order: Array[Int], keys: Array[Int]): Unit =
    val length = order.length
    var start = length / 2 - 1
    while (start >= 0) {
      sift(order, keys, start, length)
      start -= 1
    }
    var end = length
    while (end > 1) {
      end -= 1
      val top = order(0)
      order(0) = order(end)
      order(end) = top
      sift(order, keys, 0, end)
    }

  private def sift(order: Array[Int], keys: Array[Int], start: Int, end: Int): Unit =
    var root = start
    var sinking = true
    while (sinking) {
      val child = root * 2 + 1
      if (child >= end) then sinking = false
      else
        var swap = root
        if (before(keys, order(swap), order(child))) then swap = child
        if (child + 1 < end && before(keys, order(swap), order(child + 1))) then swap = child + 1
        if (swap == root) then sinking = false
        else
          val kept = order(root)
          order(root) = order(swap)
          order(swap) = kept
          root = swap
    }

  private def publish(
      keys: Array[Int],
      leftValues: Array[Long],
      rightValues: Array[Long],
      keyWords: Array[Long],
      leftWords: Array[Long],
      rightWords: Array[Long],
      leftAbs: Array[Long],
      rightAbs: Array[Long],
      capacity: Int,
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int32Column.of(keys, Validity.fromWords(keyWords, capacity), rows),
        valueColumn(leftValues, leftWords, leftAbs, capacity, rows),
        valueColumn(rightValues, rightWords, rightAbs, capacity, rows),
      ),
      rows,
    )

  private def valueColumn(
      values: Array[Long],
      words: Array[Long],
      absence: Array[Long],
      capacity: Int,
      rows: Int,
  ): Int64Column =
    val validity = Validity.fromWords(words, capacity)
    if (Validity.anySet(absence, rows)) then
      Int64Column.withAbsence(values, validity, Validity.fromWords(absence, capacity), rows)
    else Int64Column.of(values, validity, rows)
