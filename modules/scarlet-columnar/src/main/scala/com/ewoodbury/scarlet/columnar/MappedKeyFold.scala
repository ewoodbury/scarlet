package com.ewoodbury.scarlet.columnar

import scala.annotation.tailrec

/**
 * Key scan for `countByKey` after `mapIntKey`.
 *
 * Filters under the map see the source columns. `mapKey` runs once on a survivor whose key is
 * present, and the count stores that result. A null key is dropped and is not passed to `mapKey`.
 * [[scanAbove]] then applies one slot: a key slot sees the mapped key and is not called on a null,
 * and a value slot sees the original value, including a null-key row. The function is not
 * inspected. Rows that fail a step under the map are not mapped. That is the row-path order:
 * under-map filters, then the key function, then the slot above the map, then the count.
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

  /**
   * [[scan]] plus one slot after `mapKey`. `above` is an int key or value. A null key is not
   * passed to `mapKey` or to a key slot. A value slot still sees that row and is not stored.
   */
  def scanAbove(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      steps: List[Selection.Step],
      add: (Int, Int) => Unit,
      mapKey: Int => Int,
      above: Selection.Step,
  ): Unit =
    if (length == 0) then ()
    else
      val (aboveKey, aboveKeep) = aboveSlot(above)
      val key = ColumnHash.int32At(batch, 0)
      Ranges.check(start, length, key.length)
      val value = ColumnHash.int32At(batch, 1)
      Ranges.check(start, length, value.length)
      require(key.length == value.length, "key and value columns differ in length")
      val underMask = ordinalMask(steps)
      val underDense = ColumnHash.denseRange(batch, start, length, underMask)
      val keysDense = Validity.rangePresent(key.validity, start, length)
      val valuesDense = Validity.rangePresent(value.validity, start, length)
      val keyValues = key.values
      val valueValues = value.values
      if (keysDense && underDense && (aboveKey || valuesDense)) then
        fastAbove(
          start,
          length,
          steps,
          aboveKey,
          aboveKeep,
          keyValues,
          valueValues,
          add,
          mapKey,
        )
      else
        scanAboveEach(
          start,
          length,
          steps,
          aboveKey,
          aboveKeep,
          keyValues,
          key.validity.words,
          valueValues,
          value.validity.words,
          underDense,
          keysDense,
          valuesDense,
          add,
          mapKey,
        )

  private def aboveSlot(step: Selection.Step): (Boolean, ColumnKernel.Int32Predicate) = step match
    case Selection.Step.Int32(0, keep) => (true, keep)
    case Selection.Step.Int32(1, keep) => (false, keep)
    case Selection.Step.Int32(_, _) | Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) |
        Selection.Step.Bool(_, _) | Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) |
        Selection.Step.LongPair(_) | Selection.Step.StringPair(_) |
        Selection.Step.IntTriple(_, _) =>
      require(false, "slot above a mapped key is an int key or value")
      (false, _ => false)

  private def fastAbove(
      start: Int,
      length: Int,
      steps: List[Selection.Step],
      aboveKey: Boolean,
      aboveKeep: ColumnKernel.Int32Predicate,
      keyValues: Array[Int],
      valueValues: Array[Int],
      add: (Int, Int) => Unit,
      mapKey: Int => Int,
  ): Unit =
    steps match
      case Selection.Step.Int32(left, leftKeep) :: Selection.Step.Int32(right, rightKeep) :: Nil
          if aboveKey =>
        val leftValues = if (left == 0) keyValues else valueValues
        val rightValues = if (right == 0) keyValues else valueValues
        val end = start + length
        var row = start
        while (row < end) {
          if (leftKeep(leftValues(row)) && rightKeep(rightValues(row))) then
            val mapped = mapKey(keyValues(row))
            if (aboveKeep(mapped)) then add(mapped, 1)
          row += 1
        }
      case Nil if aboveKey =>
        val end = start + length
        var row = start
        while (row < end) {
          val mapped = mapKey(keyValues(row))
          if (aboveKeep(mapped)) then add(mapped, 1)
          row += 1
        }
      case _ =>
        scanAboveEach(
          start,
          length,
          steps,
          aboveKey,
          aboveKeep,
          keyValues,
          Array.emptyLongArray,
          valueValues,
          Array.emptyLongArray,
          underDense = true,
          keysDense = true,
          valuesDense = true,
          add,
          mapKey,
        )

  private def scanAboveEach(
      start: Int,
      length: Int,
      steps: List[Selection.Step],
      aboveKey: Boolean,
      aboveKeep: ColumnKernel.Int32Predicate,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
      underDense: Boolean,
      keysDense: Boolean,
      valuesDense: Boolean,
      add: (Int, Int) => Unit,
      mapKey: Int => Int,
  ): Unit =
    val end = start + length
    var row = start
    while (row < end) {
      val kept =
        if (steps.isEmpty) then true
        else if (underDense) then keepsDense(steps, row, keyValues, valueValues)
        else keepsChecked(steps, row, keyValues, keyWords, valueValues, valueWords)
      if (kept) then
        val keyPresent = keysDense || Validity.isSet(keyWords, row)
        if (aboveKey) then
          if (keyPresent) then
            val mapped = mapKey(keyValues(row))
            if (aboveKeep(mapped)) then add(mapped, 1)
        else
          val valuePresent = valuesDense || Validity.isSet(valueWords, row)
          if (keyPresent) then
            val mapped = mapKey(keyValues(row))
            if (valuePresent && aboveKeep(valueValues(row))) then add(mapped, 1)
          else if (valuePresent) then
            val _ = aboveKeep(valueValues(row))
      row += 1
    }
