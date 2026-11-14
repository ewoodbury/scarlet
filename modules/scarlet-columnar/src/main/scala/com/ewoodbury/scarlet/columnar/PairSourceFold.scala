package com.ewoodbury.scarlet.columnar

import scala.annotation.tailrec

/**
 * Count and sum over owned int-pair arrays, with no validity bitmap and no copy.
 *
 * The row rules match [[Selection.count]] and [[Selection.sumInt32]] on a dense range. Steps run
 * in order and a failure stops the row. A sum widens each kept `Int` before the add. The caller
 * promises every row in the range is present, which is true of an
 * [[com.ewoodbury.scarlet.core.IntPairArrays]] partition. An empty range returns zero and does not
 * call a predicate. Nothing here is allocated and nothing is written.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object PairSourceFold:

  def count(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      length: Int,
      steps: List[Selection.Step],
  ): Long =
    prepare(keys, values, start, length, steps)
    if (length == 0) then 0L
    else
      steps match
        case Selection.Step.Int32(left, leftKeep) :: Selection.Step
              .Int32(right, rightKeep) :: Nil =>
          countTwo(
            start,
            length,
            captured(leftKeep),
            column(left, keys, values),
            captured(rightKeep),
            column(right, keys, values),
          )
        case Selection.Step.Int32(ordinal, keep) :: Nil =>
          countOne(start, length, captured(keep), column(ordinal, keys, values))
        case Selection.Step.IntPair(keep) :: Nil =>
          countRow(start, length, keep, keys, values)
        case _ =>
          countEach(start, length, steps, keys, values)

  def sum(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      length: Int,
      steps: List[Selection.Step],
      measureOrdinal: Int,
  ): Long =
    require(
      measureOrdinal == 0 || measureOrdinal == 1,
      s"sum ordinal $measureOrdinal is not a pair column",
    )
    prepare(keys, values, start, length, steps)
    if (length == 0) then 0L
    else
      val measure = column(measureOrdinal, keys, values)
      steps match
        case Selection.Step.Int32(left, leftKeep) :: Selection.Step
              .Int32(right, rightKeep) :: Nil =>
          sumTwo(
            start,
            length,
            captured(leftKeep),
            column(left, keys, values),
            captured(rightKeep),
            column(right, keys, values),
            measure,
          )
        case Selection.Step.Int32(ordinal, keep) :: Nil =>
          sumOne(start, length, captured(keep), column(ordinal, keys, values), measure)
        case Selection.Step.IntPair(keep) :: Nil =>
          sumRow(start, length, keep, keys, values, measure)
        case _ =>
          sumEach(start, length, steps, keys, values, measure)

  private def prepare(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      length: Int,
      steps: List[Selection.Step],
  ): Unit =
    Ranges.check(start, length, keys.length)
    Ranges.check(start, length, values.length)
    require(keys.length == values.length, "key and value columns differ in length")
    require(steps.nonEmpty, "a filter stack needs a predicate")
    requireSteps(steps)

  @tailrec
  private def requireSteps(steps: List[Selection.Step]): Unit =
    steps match
      case Nil => ()
      case Selection.Step.Int32(column, _) :: rest =>
        require(column == 0 || column == 1, s"count ordinal $column is not a pair column")
        requireSteps(rest)
      case Selection.Step.IntPair(_) :: rest =>
        requireSteps(rest)
      case _ =>
        require(false, "count steps are int slots or int pairs")

  private def column(ordinal: Int, keys: Array[Int], values: Array[Int]): Array[Int] =
    if (ordinal == 0) then keys else values

  /** One function for the row loop. A bare trait is wrapped once here, not once per row. */
  private def captured(predicate: ColumnKernel.Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        value => fallback(value)

  private def countTwo(
      start: Int,
      length: Int,
      keepLeft: Int => Boolean,
      left: Array[Int],
      keepRight: Int => Boolean,
      right: Array[Int],
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    while (row < end) {
      if (keepLeft(left(row)) && keepRight(right(row))) then total += 1L
      row += 1
    }
    total

  private def countOne(
      start: Int,
      length: Int,
      keep: Int => Boolean,
      values: Array[Int],
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    while (row < end) {
      if (keep(values(row))) then total += 1L
      row += 1
    }
    total

  private def countRow(
      start: Int,
      length: Int,
      keep: PairKernel.IntPairPredicate,
      keys: Array[Int],
      values: Array[Int],
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    while (row < end) {
      if (keep(keys(row), values(row))) then total += 1L
      row += 1
    }
    total

  private def countEach(
      start: Int,
      length: Int,
      steps: List[Selection.Step],
      keys: Array[Int],
      values: Array[Int],
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    while (row < end) {
      if (keeps(steps, row, keys, values)) then total += 1L
      row += 1
    }
    total

  private def sumTwo(
      start: Int,
      length: Int,
      keepLeft: Int => Boolean,
      left: Array[Int],
      keepRight: Int => Boolean,
      right: Array[Int],
      measure: Array[Int],
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    while (row < end) {
      if (keepLeft(left(row)) && keepRight(right(row))) then total += measure(row).toLong
      row += 1
    }
    total

  private def sumOne(
      start: Int,
      length: Int,
      keep: Int => Boolean,
      filtered: Array[Int],
      measure: Array[Int],
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    while (row < end) {
      if (keep(filtered(row))) then total += measure(row).toLong
      row += 1
    }
    total

  private def sumRow(
      start: Int,
      length: Int,
      keep: PairKernel.IntPairPredicate,
      keys: Array[Int],
      values: Array[Int],
      measure: Array[Int],
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    while (row < end) {
      if (keep(keys(row), values(row))) then total += measure(row).toLong
      row += 1
    }
    total

  private def sumEach(
      start: Int,
      length: Int,
      steps: List[Selection.Step],
      keys: Array[Int],
      values: Array[Int],
      measure: Array[Int],
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    while (row < end) {
      if (keeps(steps, row, keys, values)) then total += measure(row).toLong
      row += 1
    }
    total

  @tailrec
  private def keeps(
      steps: List[Selection.Step],
      row: Int,
      keys: Array[Int],
      values: Array[Int],
  ): Boolean =
    steps match
      case Nil => true
      case step :: rest =>
        if (keepsStep(step, row, keys, values)) then keeps(rest, row, keys, values) else false

  private def keepsStep(
      step: Selection.Step,
      row: Int,
      keys: Array[Int],
      values: Array[Int],
  ): Boolean =
    step match
      case Selection.Step.Int32(ordinal, predicate) =>
        predicate(column(ordinal, keys, values)(row))
      case Selection.Step.IntPair(predicate) =>
        predicate(keys(row), values(row))
      case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
          Selection.Step.Utf8(_, _) | Selection.Step.IntTriple(_, _) | Selection.Step.LongPair(_) |
          Selection.Step.StringPair(_) =>
        rejected

  private def rejected: Boolean =
    require(false, "count steps are int slots or int pairs")
    false
