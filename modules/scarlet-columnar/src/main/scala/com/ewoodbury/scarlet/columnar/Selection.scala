package com.ewoodbury.scarlet.columnar

import scala.annotation.tailrec

/**
 * One pass over a stack of filters on a multi-column batch.
 *
 * [[com.ewoodbury.scarlet.execution.FilterChain]] uses this for pair and triple batches. A
 * single-column batch stays on chained kernels. The survivor index is four bytes per row, which
 * costs more than the intermediate column a chained kernel writes for the next filter to read. A
 * boolean column is one bit per row.
 *
 * The survivor index is an `Array[Int]` of absolute row ids in the source batch, plus a count. The
 * array is allocated once for the morsel invocation and reused for every step. It is not a field:
 * morsels run in parallel, and each invocation owns its scratch. The first predicate seeds the
 * index. Each later predicate reads those ids and writes the rows it keeps back into the same
 * array, so the count shrinks. A later predicate is a per-row call on the survivors. It is not a
 * word-level AND.
 *
 * A primitive, boolean, or pair slot skips a null and does not call the predicate. A present zero
 * is still a candidate. A key or value slot looks at that ordinal only, so a null on the other
 * ordinal can survive. A pair row needs both ordinals. A long-pair row needs both ordinals and
 * calls only when both bits are set. A string-pair row passes a null key and does not turn a null
 * int into zero. A triple needs all three present and names the absolute row when one is null,
 * before any predicate call. A general string predicate is called on every candidate, including an
 * empty cell. An empty cell that passes stays absent in the output.
 *
 * [[applySelected]] returns that index and does not gather. [[materialize]] gathers every column
 * by absolute row and copies the source validity bit. A clear bit stores zero and leaves the
 * output bit clear. [[Validity.prefixValid]] is not used: a survivor may be absent, and a dense
 * present prefix is what the gather writes when every survivor is present. The output arrays are
 * sized to the index capacity, which is the morsel width. The batch length is the survivor count.
 *
 * [[count]] walks the same int-slot and int-pair steps and returns the survivor total. A long-pair
 * row and a string-pair row are not count steps. [[sumInt32]] walks those steps and adds one
 * ordinal. Neither allocates the index or gathers a batch. When every ordinal those steps read is
 * dense on the range, the walk skips the per-row bit test. An ordinal no step reads is not tested.
 * [[sumInt32]] still skips a null measure slot when that ordinal is not one of those steps, or
 * when its own range has a hole.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object Selection:

  enum Step:
    case Int32(column: Int, predicate: ColumnKernel.Int32Predicate)
    case Int64(column: Int, predicate: ColumnKernel.Int64Predicate)
    case Float64(column: Int, predicate: ColumnKernel.Float64Predicate)
    case Bool(column: Int, predicate: ColumnKernel.BoolPredicate)
    case Utf8(column: Int, predicate: ColumnKernel.Utf8Predicate)
    case IntPair(predicate: PairKernel.IntPairPredicate)
    case LongPair(predicate: LongPairKernel.LongPairPredicate)
    case StringPair(predicate: StringPairRow.Predicate)
    case IntTriple(column: Int, predicate: ColumnKernel.Int32Predicate)

  /**
   * Applies `steps` to `[start, start + length)`. `steps` is read in order and is not copied. An
   * empty range publishes an empty batch and does not call a predicate.
   */
  def apply(batch: ColumnBatch, start: Int, length: Int, steps: Seq[Step]): ColumnBatch =
    materialize(applySelected(batch, start, length, steps))

  /**
   * Same walk as [[apply]], without the gather. The returned ids are absolute rows of `batch`. The
   * index capacity is `length`, the morsel width, and `count` is the live prefix.
   */
  def applySelected(batch: ColumnBatch, start: Int, length: Int, steps: Seq[Step]): Selected =
    // The range message names the window. `require` is the check for an empty stack.
    Ranges.check(start, length, batch.length)
    require(steps.nonEmpty, "a filter stack needs a predicate")
    val index = new Array[Int](length)
    val iterator = steps.iterator
    var count = seed(batch, start, length, iterator.next(), index)
    while (iterator.hasNext) {
      count = restrict(batch, iterator.next(), index, count)
    }
    Selected(batch, index, count)

  /**
   * Counts rows in `[start, start + length)` that pass every step. Steps run in order and a
   * failure stops the row. A null int slot fails its step and does not call the predicate. An
   * [[Step.IntPair]] row calls its predicate only when both ordinals are present. An empty range
   * counts zero and does not call a predicate.
   */
  def count(batch: ColumnBatch, start: Int, length: Int, steps: List[Step]): Long =
    Ranges.check(start, length, batch.length)
    require(steps.nonEmpty, "a filter stack needs a predicate")
    requireCountSteps(steps)
    val mask = ordinalMask(steps)
    val key = columnIfRead(batch, mask, bit = 1, ordinal = 0)
    val value = columnIfRead(batch, mask, bit = 2, ordinal = 1)
    if ((mask & 1) != 0) then Ranges.check(start, length, key.length)
    if ((mask & 2) != 0) then Ranges.check(start, length, value.length)
    if ((mask & 3) == 3) then
      require(key.length == value.length, "key and value columns differ in length")
    val keyValues = key.values
    val keyWords = key.validity.words
    val valueValues = value.values
    val valueWords = value.validity.words
    if (denseRange(batch, start, length, mask)) then
      tally(start, length, steps, keyValues, keyWords, valueValues, valueWords, false)
    else tally(start, length, steps, keyValues, keyWords, valueValues, valueWords, true)

  /**
   * Sums `measureOrdinal` for rows in `[start, start + length)` that pass every step. Steps run in
   * order and a failure adds nothing. A null int slot fails its step and does not call the
   * predicate. An [[Step.IntPair]] row calls its predicate only when both ordinals are present. A
   * null measure slot adds nothing and does not itself fail the row. A filter step on that same
   * ordinal already rejected it. The total is a `Long` and each `Int` is widened before the add.
   * An empty range sums zero and does not call a predicate. The walk does not allocate.
   */
  def sumInt32(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      steps: List[Step],
      measureOrdinal: Int,
  ): Long =
    Ranges.check(start, length, batch.length)
    require(steps.nonEmpty, "a filter stack needs a predicate")
    require(
      measureOrdinal == 0 || measureOrdinal == 1,
      s"sum ordinal $measureOrdinal is not a pair column",
    )
    requireCountSteps(steps)
    val mask = ordinalMask(steps)
    val measureBit = 1 << measureOrdinal
    val readMask = mask | measureBit
    val key = columnIfRead(batch, readMask, bit = 1, ordinal = 0)
    val value = columnIfRead(batch, readMask, bit = 2, ordinal = 1)
    if ((readMask & 1) != 0) then Ranges.check(start, length, key.length)
    if ((readMask & 2) != 0) then Ranges.check(start, length, value.length)
    if ((readMask & 3) == 3) then
      require(key.length == value.length, "key and value columns differ in length")
    val stepsDense = denseRange(batch, start, length, mask)
    val measure = if (measureOrdinal == 0) key else value
    // A step on the measure ordinal already drops a null. Otherwise the measure bitmap decides.
    val skipMeasureNull =
      (mask & measureBit) != 0 || Validity.rangePresent(measure.validity, start, length)
    addTally(
      start,
      length,
      steps,
      key.values,
      key.validity.words,
      value.values,
      value.validity.words,
      !stepsDense,
      measure.values,
      measure.validity.words,
      skipMeasureNull,
    )

  /** Gathers [[applySelected]]'s prefix. Output arrays stay the index capacity. */
  def materialize(selected: Selected): ColumnBatch =
    publish(selected.batch, selected.rows, selected.count, selected.rows.length)

  /** Stand-in for an ordinal the steps do not read. The row loop never indexes it. */
  private val unusedInt32: Int32Column =
    Int32Column.of(Array.empty[Int], Validity.allValid(0), 0)

  @tailrec
  private def requireCountSteps(stack: List[Step]): Unit =
    stack match
      case Nil => ()
      case Step.Int32(column, _) :: rest =>
        require(column == 0 || column == 1, s"count ordinal $column is not a pair column")
        requireCountSteps(rest)
      case Step.IntPair(_) :: rest =>
        requireCountSteps(rest)
      case _ :: rest =>
        require(false, "count steps are int slots or int pairs")
        requireCountSteps(rest)

  @tailrec
  private def ordinalMask(stack: List[Step], mask: Int = 0): Int =
    stack match
      case Nil => mask
      case Step.Int32(column, _) :: rest => ordinalMask(rest, mask | (1 << column))
      case Step.IntPair(_) :: rest => ordinalMask(rest, mask | 3)
      case _ :: rest => ordinalMask(rest, mask)

  private def columnIfRead(
      batch: ColumnBatch,
      mask: Int,
      bit: Int,
      ordinal: Int,
  ): Int32Column =
    if ((mask & bit) != 0) then int32At(batch, ordinal) else unusedInt32

  /** One [[Validity.rangePresent]] per ordinal named by `mask`. Unset bits are not read. */
  private def denseRange(batch: ColumnBatch, start: Int, length: Int, mask: Int): Boolean =
    var pending = mask
    var ordinal = 0
    var dense = true
    while (dense && pending != 0) {
      if ((pending & 1) != 0) then
        dense = Validity.rangePresent(int32At(batch, ordinal).validity, start, length)
      pending >>>= 1
      ordinal += 1
    }
    dense

  private def tally(
      start: Int,
      length: Int,
      stack: List[Step],
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
      checkBits: Boolean,
  ): Long =
    stack match
      case Step.Int32(left, leftKeep) :: Step.Int32(right, rightKeep) :: Nil =>
        if (checkBits) then
          countTwoInt32Checked(
            start,
            length,
            left,
            leftKeep,
            right,
            rightKeep,
            keyValues,
            keyWords,
            valueValues,
            valueWords,
          )
        else
          countTwoInt32Dense(
            start,
            length,
            left,
            leftKeep,
            right,
            rightKeep,
            keyValues,
            valueValues,
          )
      case _ =>
        if (checkBits) then
          countEachChecked(start, length, stack, keyValues, keyWords, valueValues, valueWords)
        else countEachDense(start, length, stack, keyValues, valueValues)

  private def countTwoInt32Dense(
      start: Int,
      length: Int,
      leftOrdinal: Int,
      leftKeep: ColumnKernel.Int32Predicate,
      rightOrdinal: Int,
      rightKeep: ColumnKernel.Int32Predicate,
      keyValues: Array[Int],
      valueValues: Array[Int],
  ): Long =
    val leftValues = if (leftOrdinal == 0) keyValues else valueValues
    val rightValues = if (rightOrdinal == 0) keyValues else valueValues
    val end = start + length
    var row = start
    var total = 0L
    while (row < end) {
      if (leftKeep(leftValues(row)) && rightKeep(rightValues(row))) then total += 1L
      row += 1
    }
    total

  private def countTwoInt32Checked(
      start: Int,
      length: Int,
      leftOrdinal: Int,
      leftKeep: ColumnKernel.Int32Predicate,
      rightOrdinal: Int,
      rightKeep: ColumnKernel.Int32Predicate,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
  ): Long =
    val leftValues = if (leftOrdinal == 0) keyValues else valueValues
    val leftWords = if (leftOrdinal == 0) keyWords else valueWords
    val rightValues = if (rightOrdinal == 0) keyValues else valueValues
    val rightWords = if (rightOrdinal == 0) keyWords else valueWords
    val end = start + length
    var row = start
    var total = 0L
    while (row < end) {
      if (
          Validity.isSet(leftWords, row) && leftKeep(leftValues(row)) &&
          Validity.isSet(rightWords, row) && rightKeep(rightValues(row))
        )
      then total += 1L
      row += 1
    }
    total

  private def countEachDense(
      start: Int,
      length: Int,
      stack: List[Step],
      keyValues: Array[Int],
      valueValues: Array[Int],
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    while (row < end) {
      if (keepsAllDense(stack, row, keyValues, valueValues)) then total += 1L
      row += 1
    }
    total

  private def countEachChecked(
      start: Int,
      length: Int,
      stack: List[Step],
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    while (row < end) {
      if (keepsAllChecked(stack, row, keyValues, keyWords, valueValues, valueWords)) then
        total += 1L
      row += 1
    }
    total

  private def addTally(
      start: Int,
      length: Int,
      stack: List[Step],
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
      checkBits: Boolean,
      measureValues: Array[Int],
      measureWords: Array[Long],
      skipMeasureNull: Boolean,
  ): Long =
    stack match
      case Step.Int32(left, leftKeep) :: Step.Int32(right, rightKeep) :: Nil =>
        sumTwo(
          start,
          length,
          left,
          leftKeep,
          right,
          rightKeep,
          keyValues,
          keyWords,
          valueValues,
          valueWords,
          checkBits,
          measureValues,
          measureWords,
          skipMeasureNull,
        )
      case _ =>
        sumEach(
          start,
          length,
          stack,
          keyValues,
          keyWords,
          valueValues,
          valueWords,
          checkBits,
          measureValues,
          measureWords,
          skipMeasureNull,
        )

  private def sumTwo(
      start: Int,
      length: Int,
      leftOrdinal: Int,
      leftKeep: ColumnKernel.Int32Predicate,
      rightOrdinal: Int,
      rightKeep: ColumnKernel.Int32Predicate,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
      checkBits: Boolean,
      measureValues: Array[Int],
      measureWords: Array[Long],
      skipMeasureNull: Boolean,
  ): Long =
    val leftValues = if (leftOrdinal == 0) keyValues else valueValues
    val rightValues = if (rightOrdinal == 0) keyValues else valueValues
    val leftWords = if (leftOrdinal == 0) keyWords else valueWords
    val rightWords = if (rightOrdinal == 0) keyWords else valueWords
    val end = start + length
    var row = start
    var total = 0L
    if (!checkBits && skipMeasureNull) then
      while (row < end) {
        if (leftKeep(leftValues(row)) && rightKeep(rightValues(row))) then
          total += measureValues(row).toLong
        row += 1
      }
    else if (!checkBits) then
      while (row < end) {
        if (
            leftKeep(leftValues(row)) && rightKeep(rightValues(row)) &&
            Validity.isSet(measureWords, row)
          )
        then total += measureValues(row).toLong
        row += 1
      }
    else if (skipMeasureNull) then
      while (row < end) {
        if (
            Validity.isSet(leftWords, row) && leftKeep(leftValues(row)) &&
            Validity.isSet(rightWords, row) && rightKeep(rightValues(row))
          )
        then total += measureValues(row).toLong
        row += 1
      }
    else
      while (row < end) {
        if (
            Validity.isSet(leftWords, row) && leftKeep(leftValues(row)) &&
            Validity.isSet(rightWords, row) && rightKeep(rightValues(row)) &&
            Validity.isSet(measureWords, row)
          )
        then total += measureValues(row).toLong
        row += 1
      }
    total

  private def sumEach(
      start: Int,
      length: Int,
      stack: List[Step],
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
      checkBits: Boolean,
      measureValues: Array[Int],
      measureWords: Array[Long],
      skipMeasureNull: Boolean,
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    if (!checkBits && skipMeasureNull) then
      while (row < end) {
        if (keepsAllDense(stack, row, keyValues, valueValues)) then
          total += measureValues(row).toLong
        row += 1
      }
    else if (!checkBits) then
      while (row < end) {
        if (
            keepsAllDense(stack, row, keyValues, valueValues) &&
            Validity.isSet(measureWords, row)
          )
        then total += measureValues(row).toLong
        row += 1
      }
    else if (skipMeasureNull) then
      while (row < end) {
        if (keepsAllChecked(stack, row, keyValues, keyWords, valueValues, valueWords)) then
          total += measureValues(row).toLong
        row += 1
      }
    else
      while (row < end) {
        if (
            keepsAllChecked(stack, row, keyValues, keyWords, valueValues, valueWords) &&
            Validity.isSet(measureWords, row)
          )
        then total += measureValues(row).toLong
        row += 1
      }
    total

  @tailrec
  private def keepsAllDense(
      stack: List[Step],
      row: Int,
      keyValues: Array[Int],
      valueValues: Array[Int],
  ): Boolean =
    stack match
      case Nil => true
      case step :: rest =>
        if (keepsDense(step, row, keyValues, valueValues)) then
          keepsAllDense(rest, row, keyValues, valueValues)
        else false

  @tailrec
  private def keepsAllChecked(
      stack: List[Step],
      row: Int,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
  ): Boolean =
    stack match
      case Nil => true
      case step :: rest =>
        if (keepsChecked(step, row, keyValues, keyWords, valueValues, valueWords)) then
          keepsAllChecked(rest, row, keyValues, keyWords, valueValues, valueWords)
        else false

  private def keepsDense(
      step: Step,
      row: Int,
      keyValues: Array[Int],
      valueValues: Array[Int],
  ): Boolean = step match
    case Step.Int32(ordinal, predicate) =>
      val values = if (ordinal == 0) keyValues else valueValues
      predicate(values(row))
    case Step.IntPair(predicate) =>
      predicate(keyValues(row), valueValues(row))
    case Step.Int64(_, _) | Step.Float64(_, _) | Step.Bool(_, _) | Step.Utf8(_, _) |
        Step.IntTriple(_, _) | Step.LongPair(_) | Step.StringPair(_) =>
      rejectedCountStep

  private def keepsChecked(
      step: Step,
      row: Int,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
  ): Boolean = step match
    case Step.Int32(ordinal, predicate) =>
      val values = if (ordinal == 0) keyValues else valueValues
      val words = if (ordinal == 0) keyWords else valueWords
      Validity.isSet(words, row) && predicate(values(row))
    case Step.IntPair(predicate) =>
      Validity.isSet(keyWords, row) &&
      Validity.isSet(valueWords, row) &&
      predicate(keyValues(row), valueValues(row))
    case Step.Int64(_, _) | Step.Float64(_, _) | Step.Bool(_, _) | Step.Utf8(_, _) |
        Step.IntTriple(_, _) | Step.LongPair(_) | Step.StringPair(_) =>
      rejectedCountStep

  private def rejectedCountStep: Boolean =
    require(false, "count steps are int slots or int pairs")
    false

  private def seed(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      step: Step,
      index: Array[Int],
  ): Int = step match
    case Step.Int32(ordinal, predicate) =>
      seedInt32(batch, ordinal, start, length, predicate, index)
    case Step.Int64(ordinal, predicate) =>
      seedInt64(batch, ordinal, start, length, predicate, index)
    case Step.Float64(ordinal, predicate) =>
      seedFloat64(batch, ordinal, start, length, predicate, index)
    case Step.Bool(ordinal, predicate) =>
      seedBool(batch, ordinal, start, length, predicate, index)
    case Step.Utf8(ordinal, predicate) =>
      seedUtf8(batch, ordinal, start, length, predicate, index)
    case Step.IntPair(predicate) =>
      seedPair(batch, start, length, predicate, index)
    case Step.LongPair(predicate) =>
      WholeRowSelection.seedLong(batch, start, length, predicate, index)
    case Step.StringPair(predicate) =>
      WholeRowSelection.seedString(batch, start, length, predicate, index)
    case Step.IntTriple(ordinal, predicate) =>
      seedTriple(batch, ordinal, start, length, predicate, index)

  private def restrict(batch: ColumnBatch, step: Step, index: Array[Int], count: Int): Int =
    step match
      case Step.Int32(ordinal, predicate) =>
        restrictInt32(batch, ordinal, predicate, index, count)
      case Step.Int64(ordinal, predicate) =>
        restrictInt64(batch, ordinal, predicate, index, count)
      case Step.Float64(ordinal, predicate) =>
        restrictFloat64(batch, ordinal, predicate, index, count)
      case Step.Bool(ordinal, predicate) =>
        restrictBool(batch, ordinal, predicate, index, count)
      case Step.Utf8(ordinal, predicate) =>
        restrictUtf8(batch, ordinal, predicate, index, count)
      case Step.IntPair(predicate) =>
        restrictPair(batch, predicate, index, count)
      case Step.LongPair(predicate) =>
        WholeRowSelection.restrictLong(batch, predicate, index, count)
      case Step.StringPair(predicate) =>
        WholeRowSelection.restrictString(batch, predicate, index, count)
      case Step.IntTriple(ordinal, predicate) =>
        restrictTriple(batch, ordinal, predicate, index, count)

  private def seedInt32(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: ColumnKernel.Int32Predicate,
      index: Array[Int],
  ): Int =
    val column = int32At(batch, ordinal)
    Ranges.check(start, length, column.length)
    val values = column.values
    val words = column.validity.words
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (Validity.isSet(words, row) && predicate(values(row))) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictInt32(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: ColumnKernel.Int32Predicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val column = int32At(batch, ordinal)
    val values = column.values
    val words = column.validity.words
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (Validity.isSet(words, row) && predicate(values(row))) then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private def seedInt64(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: ColumnKernel.Int64Predicate,
      index: Array[Int],
  ): Int =
    val column = ColumnKernel.asInt64(ColumnKernel.columnAt(batch, ordinal), ordinal)
    Ranges.check(start, length, column.length)
    val values = column.values
    val words = column.validity.words
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (Validity.isSet(words, row) && predicate(values(row))) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictInt64(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: ColumnKernel.Int64Predicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val column = ColumnKernel.asInt64(ColumnKernel.columnAt(batch, ordinal), ordinal)
    val values = column.values
    val words = column.validity.words
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (Validity.isSet(words, row) && predicate(values(row))) then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private def seedFloat64(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: ColumnKernel.Float64Predicate,
      index: Array[Int],
  ): Int =
    val column = ColumnKernel.asFloat64(ColumnKernel.columnAt(batch, ordinal), ordinal)
    Ranges.check(start, length, column.length)
    val values = column.values
    val words = column.validity.words
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (Validity.isSet(words, row) && predicate(values(row))) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictFloat64(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: ColumnKernel.Float64Predicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val column = ColumnKernel.asFloat64(ColumnKernel.columnAt(batch, ordinal), ordinal)
    val values = column.values
    val words = column.validity.words
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (Validity.isSet(words, row) && predicate(values(row))) then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private def seedBool(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: ColumnKernel.BoolPredicate,
      index: Array[Int],
  ): Int =
    val column = boolAt(batch, ordinal)
    Ranges.check(start, length, column.length)
    val values = column.values
    val words = column.validity.words
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (Validity.isSet(words, row) && predicate(values(row) != 0)) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictBool(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: ColumnKernel.BoolPredicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val column = boolAt(batch, ordinal)
    val values = column.values
    val words = column.validity.words
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (Validity.isSet(words, row) && predicate(values(row) != 0)) then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private def seedUtf8(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: ColumnKernel.Utf8Predicate,
      index: Array[Int],
  ): Int =
    val column = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)
    Ranges.check(start, length, column.length)
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (predicate(ColumnKernel.utf8String(column, row))) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictUtf8(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: ColumnKernel.Utf8Predicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val column = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (predicate(ColumnKernel.utf8String(column, row))) then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private def seedPair(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      predicate: PairKernel.IntPairPredicate,
      index: Array[Int],
  ): Int =
    val (keys, values) = pairColumns(batch)
    Ranges.check(start, length, keys.length)
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (bothPresent(keyWords, valueWords, row) && predicate(keyValues(row), valueValues(row)))
      then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictPair(
      batch: ColumnBatch,
      predicate: PairKernel.IntPairPredicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val (keys, values) = pairColumns(batch)
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (bothPresent(keyWords, valueWords, row) && predicate(keyValues(row), valueValues(row)))
      then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private def seedTriple(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: ColumnKernel.Int32Predicate,
      index: Array[Int],
  ): Int =
    val (keys, left, right) = tripleColumns(batch)
    Ranges.check(start, length, keys.length)
    requireTripleRange(keys, left, right, start, length)
    seedInt32(batch, ordinal, start, length, predicate, index)

  private def restrictTriple(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: ColumnKernel.Int32Predicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val (keys, left, right) = tripleColumns(batch)
    requireTripleIds(keys, left, right, index, count)
    restrictInt32(batch, ordinal, predicate, index, count)

  private def requireTripleRange(
      keys: Int32Column,
      left: Int32Column,
      right: Int32Column,
      start: Int,
      length: Int,
  ): Unit =
    val end = start + length
    var row = start
    while (row < end) {
      require(triplePresent(keys, left, right, row), s"null Int32 join value at row $row")
      row += 1
    }

  private def requireTripleIds(
      keys: Int32Column,
      left: Int32Column,
      right: Int32Column,
      index: Array[Int],
      count: Int,
  ): Unit =
    var read = 0
    while (read < count) {
      val row = index(read)
      require(triplePresent(keys, left, right, row), s"null Int32 join value at row $row")
      read += 1
    }

  private def triplePresent(
      keys: Int32Column,
      left: Int32Column,
      right: Int32Column,
      row: Int,
  ): Boolean =
    Validity.isSet(keys.validity.words, row) &&
      Validity.isSet(left.validity.words, row) &&
      Validity.isSet(right.validity.words, row)

  private inline def bothPresent(
      keyWords: Array[Long],
      valueWords: Array[Long],
      row: Int,
  ): Boolean =
    Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row)

  private def pairColumns(batch: ColumnBatch): (Int32Column, Int32Column) =
    require(
      batch.columns.length == 2,
      s"pair filter expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val values = int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    (keys, values)

  private def tripleColumns(batch: ColumnBatch): (Int32Column, Int32Column, Int32Column) =
    require(
      batch.columns.length == 3,
      s"triple filter expected 3 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val left = int32At(batch, 1)
    val right = int32At(batch, 2)
    require(left.length == keys.length, "key and left value columns differ in length")
    require(right.length == keys.length, "key and right value columns differ in length")
    (keys, left, right)

  private def int32At(batch: ColumnBatch, ordinal: Int): Int32Column =
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def boolAt(batch: ColumnBatch, ordinal: Int): BoolColumn =
    ColumnKernel.columnAt(batch, ordinal) match
      case bool: BoolColumn => bool
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Bool",
        )

  /**
   * One gather of every column. Arrays are sized to the morsel. A set source bit copies the value
   * and the bit. A clear source bit stores zero and leaves the bit clear.
   */
  private def publish(
      batch: ColumnBatch,
      index: Array[Int],
      count: Int,
      capacity: Int,
  ): ColumnBatch =
    val columns = Vector.tabulate(batch.columns.length) { ordinal =>
      gather(ColumnKernel.columnAt(batch, ordinal), index, count, capacity)
    }
    new ColumnBatch(batch.schema, columns, count)

  private def gather(column: Column, index: Array[Int], count: Int, capacity: Int): Column =
    column match
      case int32: Int32Column => gatherInt32(int32, index, count, capacity)
      case int64: Int64Column => gatherInt64(int64, index, count, capacity)
      case float64: Float64Column => gatherFloat64(float64, index, count, capacity)
      case bool: BoolColumn => gatherBool(bool, index, count, capacity)
      case utf8: DictUtf8Column => gatherUtf8(utf8, index, count, capacity)

  private def gatherInt32(
      column: Int32Column,
      index: Array[Int],
      count: Int,
      capacity: Int,
  ): Int32Column =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    var slot = 0
    while (slot < count) {
      val row = index(slot)
      if (Validity.isSet(srcWords, row)) then
        out(slot) = in(row)
        Validity.setBit(dstWords, slot)
      else out(slot) = 0
      slot += 1
    }
    Int32Column.of(out, Validity.fromWords(dstWords, capacity), count)

  private def gatherInt64(
      column: Int64Column,
      index: Array[Int],
      count: Int,
      capacity: Int,
  ): Int64Column =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Long](capacity)
    val dstWords = Validity.allocate(capacity)
    var slot = 0
    while (slot < count) {
      val row = index(slot)
      if (Validity.isSet(srcWords, row)) then
        out(slot) = in(row)
        Validity.setBit(dstWords, slot)
      else out(slot) = 0L
      slot += 1
    }
    Int64Column.of(out, Validity.fromWords(dstWords, capacity), count)

  private def gatherFloat64(
      column: Float64Column,
      index: Array[Int],
      count: Int,
      capacity: Int,
  ): Float64Column =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Double](capacity)
    val dstWords = Validity.allocate(capacity)
    var slot = 0
    while (slot < count) {
      val row = index(slot)
      if (Validity.isSet(srcWords, row)) then
        out(slot) = in(row)
        Validity.setBit(dstWords, slot)
      else out(slot) = 0.0
      slot += 1
    }
    Float64Column.of(out, Validity.fromWords(dstWords, capacity), count)

  private def gatherBool(
      column: BoolColumn,
      index: Array[Int],
      count: Int,
      capacity: Int,
  ): BoolColumn =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Byte](capacity)
    val dstWords = Validity.allocate(capacity)
    var slot = 0
    while (slot < count) {
      val row = index(slot)
      if (Validity.isSet(srcWords, row)) then
        out(slot) = if (in(row) != 0) 1.toByte else 0.toByte
        Validity.setBit(dstWords, slot)
      else out(slot) = 0.toByte
      slot += 1
    }
    BoolColumn.of(out, Validity.fromWords(dstWords, capacity), count)

  private def gatherUtf8(
      column: DictUtf8Column,
      index: Array[Int],
      count: Int,
      capacity: Int,
  ): DictUtf8Column =
    val in = column.codes
    val srcWords = column.validity.words
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    var slot = 0
    while (slot < count) {
      val row = index(slot)
      if (Validity.isSet(srcWords, row)) then
        out(slot) = in(row)
        Validity.setBit(dstWords, slot)
      else out(slot) = 0
      slot += 1
    }
    DictUtf8Column.of(column.dictionary, out, Validity.fromWords(dstWords, capacity), count)
