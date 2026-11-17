package com.ewoodbury.scarlet.columnar

import scala.annotation.tailrec

/**
 * Key scan for `countByKey` after `mapIntKey`.
 *
 * Filters see the source columns. `mapKey` runs once on a survivor whose key is present, and the
 * count stores that result. A null key is dropped and is not passed to `mapKey`. The function is
 * not inspected. Rows that fail a step are not mapped. This is the same order as the row path:
 * filter, then the key function, then the count.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object MappedKeyFold:

  def scan(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      steps: List[Selection.Step],
      add: (Int, Int) => Unit,
      mapKey: Int => Int,
  ): Unit =
    if (length == 0) then ()
    else
      val readMask = ordinalMask(steps)
      val key = ColumnHash.int32At(batch, 0)
      Ranges.check(start, length, key.length)
      val value =
        if ((readMask & 2) != 0) then
          val column = ColumnHash.int32At(batch, 1)
          Ranges.check(start, length, column.length)
          require(key.length == column.length, "key and value columns differ in length")
          column
        else key
      val stepsDense = ColumnHash.denseRange(batch, start, length, readMask)
      // A key step already drops a null key. Otherwise the key bitmap does.
      val skipNullKey = ((readMask & 1) != 0) || Validity.rangePresent(key.validity, start, length)
      val keyValues = key.values
      val valueValues = value.values
      steps match
        case Selection.Step.Int32(left, leftKeep) :: Selection.Step.Int32(right, rightKeep) :: Nil
            if stepsDense && skipNullKey =>
          scanTwo(
            start,
            length,
            left,
            leftKeep,
            right,
            rightKeep,
            keyValues,
            valueValues,
            add,
            mapKey,
          )
        case Selection.Step.Int32(ordinal, keep) :: Nil if stepsDense && skipNullKey =>
          scanOne(start, length, ordinal, keep, keyValues, valueValues, add, mapKey)
        case _ =>
          scanEach(
            start,
            length,
            steps,
            keyValues,
            key.validity.words,
            valueValues,
            value.validity.words,
            stepsDense,
            skipNullKey,
            add,
            mapKey,
          )

  private def scanTwo(
      start: Int,
      length: Int,
      leftOrdinal: Int,
      leftKeep: ColumnKernel.Int32Predicate,
      rightOrdinal: Int,
      rightKeep: ColumnKernel.Int32Predicate,
      keyValues: Array[Int],
      valueValues: Array[Int],
      add: (Int, Int) => Unit,
      mapKey: Int => Int,
  ): Unit =
    val leftValues = if (leftOrdinal == 0) keyValues else valueValues
    val rightValues = if (rightOrdinal == 0) keyValues else valueValues
    val end = start + length
    var row = start
    while (row < end) {
      if (leftKeep(leftValues(row)) && rightKeep(rightValues(row))) then
        add(mapKey(keyValues(row)), 1)
      row += 1
    }

  private def scanOne(
      start: Int,
      length: Int,
      ordinal: Int,
      keep: ColumnKernel.Int32Predicate,
      keyValues: Array[Int],
      valueValues: Array[Int],
      add: (Int, Int) => Unit,
      mapKey: Int => Int,
  ): Unit =
    val values = if (ordinal == 0) keyValues else valueValues
    val end = start + length
    var row = start
    while (row < end) {
      if (keep(values(row))) then add(mapKey(keyValues(row)), 1)
      row += 1
    }

  private def scanEach(
      start: Int,
      length: Int,
      steps: List[Selection.Step],
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
      stepsDense: Boolean,
      skipNullKey: Boolean,
      add: (Int, Int) => Unit,
      mapKey: Int => Int,
  ): Unit =
    val end = start + length
    var row = start
    while (row < end) {
      val kept =
        if (stepsDense) then keepsDense(steps, row, keyValues, valueValues)
        else keepsChecked(steps, row, keyValues, keyWords, valueValues, valueWords)
      if (kept && (skipNullKey || Validity.isSet(keyWords, row))) then
        add(mapKey(keyValues(row)), 1)
      row += 1
    }

  @tailrec
  private def keepsDense(
      steps: List[Selection.Step],
      row: Int,
      keyValues: Array[Int],
      valueValues: Array[Int],
  ): Boolean =
    steps match
      case Nil => true
      case step :: rest =>
        if (keepsDenseStep(step, row, keyValues, valueValues)) then
          keepsDense(rest, row, keyValues, valueValues)
        else false

  @tailrec
  private def keepsChecked(
      steps: List[Selection.Step],
      row: Int,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
  ): Boolean =
    steps match
      case Nil => true
      case step :: rest =>
        if (keepsCheckedStep(step, row, keyValues, keyWords, valueValues, valueWords)) then
          keepsChecked(rest, row, keyValues, keyWords, valueValues, valueWords)
        else false

  private def keepsDenseStep(
      step: Selection.Step,
      row: Int,
      keyValues: Array[Int],
      valueValues: Array[Int],
  ): Boolean = step match
    case Selection.Step.Int32(ordinal, predicate) =>
      val values = if (ordinal == 0) keyValues else valueValues
      predicate(values(row))
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
      rejected

  private def keepsCheckedStep(
      step: Selection.Step,
      row: Int,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
  ): Boolean = step match
    case Selection.Step.Int32(ordinal, predicate) =>
      val values = if (ordinal == 0) keyValues else valueValues
      val words = if (ordinal == 0) keyWords else valueWords
      Validity.isSet(words, row) && predicate(values(row))
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
      rejected

  private def rejected: Boolean =
    require(false, "mapped key count steps are int slots")
    false

  private def ordinalMask(steps: List[Selection.Step], mask: Int = 0): Int = steps match
    case Nil => mask
    case Selection.Step.Int32(column, _) :: rest if column == 0 || column == 1 =>
      ordinalMask(rest, mask | (1 << column))
    case _ :: rest => ordinalMask(rest, mask)
