package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Counts int keys through a filter stack without building a survivor index.
 *
 * The row walk matches [[Selection.count]]. Steps run in order and a failure stops the row. A null
 * int slot fails its step and does not call the predicate. When every ordinal the steps read is
 * present on the morsel, the bit test is skipped. Each survivor is inserted with
 * [[ColumnHash.mix]] and the same linear probe as [[HashCount]]. A null key is dropped even when
 * no step reads the key. A null value still counts when no step reads the value. The stored count
 * is an `Int` and wraps on overflow.
 *
 * [[count]] returns `None` before any table when `steps` is empty or is not a stack of
 * [[Selection.Step.Int32]] slot atoms, and when the source-row estimate does not fit the budget.
 * Survivor rows are not known until the scan, so that estimate is an upper bound on the charge
 * [[HashCount.byKeySelected]] uses. The caller keeps the index path, which still refuses or spills
 * from the survivor count. A folded partial that does fit is exchanged like [[HashCount]], and an
 * over-budget post-exchange bucket still finishes in [[HashCountSpill]].
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object FoldedCount:

  private val passes = new AtomicInteger

  /**
   * True when every step is an int key or value slot. Empty, whole-row, and string steps are not.
   */
  def admits(steps: Seq[Selection.Step]): Boolean =
    steps.nonEmpty && steps.forall {
      case Selection.Step.Int32(column, _) => column == 0 || column == 1
      case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
          Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
          Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
        false
    }

  /**
   * True when a batch key is missing, is not an int, or has a null inside its live prefix. Spare
   * bits past `length` are ignored, matching the sort fallback in the index path.
   */
  def absentIntKey(batches: Vector[ColumnBatch]): Boolean =
    batches.exists { batch =>
      batch.columns.headOption match
        case Some(keys: Int32Column) => !liveKeys(keys)
        case _ => true
    }

  /** Accepted [[count]] calls since the last [[resetPassesForTest]]. */
  def passesForTest: Int = passes.get

  /** Clears [[passesForTest]]. */
  def resetPassesForTest(): Unit = passes.set(0)

  /**
   * Bytes [[HashCount]] charges for `rows` keys. The budget tests set the spill limit from this.
   */
  def chargedBytes(rows: Long): Long = HashCount.inputRowBytes(rows)

  /**
   * Partial-count `batches` by applying `steps` inside each morsel, then exchange the partials.
   * `None` means the caller should use [[HashCount.byKey]] or [[HashCount.byKeySelected]]. No id
   * array is allocated on the `Some` path.
   */
  def count(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (!admits(steps)) then None
    else if (batches.isEmpty) then
      notePass()
      Some(Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs))
    else
      batches.foreach(requireKey)
      if (!withinBudget(batches)) then None
      else
        notePass()
        Some(execute(batches, steps, numPartitions, parallelism, batchSize))

  private def execute(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    val groups = batches.map(batch => Morsels.of(batch, batchSize))
    val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
      countMorsel(morsel, steps)
    }
    val combined = PartialCombine.mergeMorsels(
      groups.map(_.length),
      partials,
      parallelism,
      HashCount.addCounts,
    )
    finish(combined, numPartitions, parallelism, batchSize)

  private def countMorsel(morsel: Morsel, steps: List[Selection.Step]): ColumnBatch =
    val length = morsel.length
    if (length == 0) then ColumnBatches.emptyIntPairs
    else
      refuse(length.toLong)
      val table = new Table(ColumnHash.tableSize(length))
      table.scan(morsel.batch, morsel.start, length, steps)
      table.compact()

  /**
   * Same exchange as [[HashCount]]. Spill stays on the post-exchange bucket. The partial combine
   * above has already refused any table that does not fit.
   */
  private def finish(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    if (combined.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      val buckets = HashExchange.partitionAll(combined, keyOrdinal = 0, numPartitions, parallelism)
      val conf = ScarletConf.get
      if (buckets.exists(bucket => HashCountSpill.exceeds(bucket, conf))) then
        buckets.map(bucket => HashCountSpill.finish(bucket, parallelism, batchSize, conf))
      else MorselScheduler.map(buckets, parallelism)((_, bucket) => HashCount.addCounts(bucket))

  /**
   * Source rows stand in for survivors. The hash path charges survivors, which are never more than
   * this, and [[HashCount.inputRowBytes]] grows with the row count. A batch that fits therefore
   * fits every morsel cut from it and the merged partial, whose row count is at most the batch
   * length.
   */
  private def withinBudget(batches: Vector[ColumnBatch]): Boolean =
    val conf = ScarletConf.get
    val budget = conf.spillBudgetBytes
    val rows = batches.iterator.map(_.length.toLong).sum
    if (!conf.spillEnabled || HashCount.inputRowBytes(1L) > budget) then
      HashCount.inputRowBytes(rows) <= budget
    else batches.forall(batch => HashCount.inputRowBytes(batch.length.toLong) <= budget)

  private def refuse(rows: Long): Unit =
    val budget = ScarletConf.get.spillBudgetBytes
    val bytes = HashCount.inputRowBytes(rows)
    if (bytes > budget) then throw new ClosedKernelRefusal(rows, bytes, budget)

  private def notePass(): Unit =
    val _ = passes.incrementAndGet()

  /** Live key bits are all set, and the bitmap covers that prefix. */
  private def liveKeys(keys: Int32Column): Boolean =
    val live = keys.length
    val words = keys.validity.words
    val fullWords = live >>> 6
    val tail = live & 63
    val covered = fullWords + (if (tail == 0) then 0 else 1)
    words.length >= covered && Validity.livePresent(keys.validity, live)

  private def requirePositive(numPartitions: Int, parallelism: Int, batchSize: Int): Unit =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")

  private def requireKey(batch: ColumnBatch): Unit =
    val keyType = batch.schema.headOption.getOrElse {
      throw new ColumnarFallback("countByKey expects an Int32 key column")
    }
    keyType match
      case LogicalType.Int32 => ()
      case LogicalType.Int64 | LogicalType.Float64 | LogicalType.Bool | LogicalType.Utf8Dict =>
        throw new ColumnarFallback(
          s"countByKey expects an Int32 key column, found $keyType",
        )

  private def ordinalMask(stack: List[Selection.Step], mask: Int = 0): Int =
    stack match
      case Nil => mask
      case Selection.Step.Int32(column, _) :: rest => ordinalMask(rest, mask | (1 << column))
      case _ :: rest => ordinalMask(rest, mask)

  /** One [[Validity.rangePresent]] per ordinal named by `mask`. */
  private def denseRange(batch: ColumnBatch, start: Int, length: Int, mask: Int): Boolean =
    var pending = mask
    var ordinal = 0
    var dense = true
    while (dense && pending != 0) {
      if ((pending & 1) != 0) then
        dense = Validity.rangePresent(ColumnHash.int32At(batch, ordinal).validity, start, length)
      pending >>>= 1
      ordinal += 1
    }
    dense

  private final class Table(capacity: Int):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Int] = new Array[Int](capacity)
    private val counts: Array[Int] = new Array[Int](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def scan(batch: ColumnBatch, start: Int, length: Int, steps: List[Selection.Step]): Unit =
      val readMask = ordinalMask(steps)
      val key = ColumnHash.int32At(batch, 0)
      Ranges.check(start, length, key.length)
      val value =
        if ((readMask & 2) != 0) then
          val column = ColumnHash.int32At(batch, 1)
          Ranges.check(start, length, column.length)
          require(key.length == column.length, "key and value columns differ in length")
          column
        else unusedInt32
      val stepsDense = denseRange(batch, start, length, readMask)
      // A key step already drops a null key. Otherwise the key bitmap does, unless the range is
      // dense and there is nothing to drop.
      val skipNullKey = ((readMask & 1) != 0) || Validity.rangePresent(key.validity, start, length)
      val keyValues = key.values
      val valueValues = value.values
      steps match
        case Selection.Step.Int32(left, leftKeep) :: Selection.Step
              .Int32(right, rightKeep) :: Nil =>
          scanTwo(
            start,
            length,
            left,
            leftKeep,
            right,
            rightKeep,
            keyValues,
            key.validity.words,
            valueValues,
            value.validity.words,
            stepsDense,
            skipNullKey,
          )
        case Selection.Step.Int32(ordinal, keep) :: Nil =>
          scanOne(
            start,
            length,
            ordinal,
            keep,
            keyValues,
            key.validity.words,
            valueValues,
            value.validity.words,
            stepsDense,
            skipNullKey,
          )
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
          )

    def compact(): ColumnBatch =
      val rows = occupied
      val outKeys = new Array[Int](rows)
      val outCounts = new Array[Int](rows)
      var cursor = 0
      while (cursor < rows) {
        val slot = slots(cursor)
        outKeys(cursor) = keys(slot)
        outCounts(cursor) = counts(slot)
        cursor += 1
      }
      val present = Validity.allValid(rows)
      new ColumnBatch(
        Vector(LogicalType.Int32, LogicalType.Int32),
        Vector(Int32Column.of(outKeys, present, rows), Int32Column.of(outCounts, present, rows)),
        rows,
      )

    private def scanTwo(
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
        stepsDense: Boolean,
        skipNullKey: Boolean,
    ): Unit =
      if (stepsDense && skipNullKey) then
        addTwoDense(
          start,
          length,
          leftOrdinal,
          leftKeep,
          rightOrdinal,
          rightKeep,
          keyValues,
          valueValues,
        )
      else if (stepsDense) then
        addTwoDropNullKey(
          start,
          length,
          leftOrdinal,
          leftKeep,
          rightOrdinal,
          rightKeep,
          keyValues,
          keyWords,
          valueValues,
        )
      else
        addTwoChecked(
          start,
          length,
          leftOrdinal,
          leftKeep,
          rightOrdinal,
          rightKeep,
          keyValues,
          keyWords,
          valueValues,
          valueWords,
          skipNullKey,
        )

    private def addTwoDense(
        start: Int,
        length: Int,
        leftOrdinal: Int,
        leftKeep: ColumnKernel.Int32Predicate,
        rightOrdinal: Int,
        rightKeep: ColumnKernel.Int32Predicate,
        keyValues: Array[Int],
        valueValues: Array[Int],
    ): Unit =
      val leftValues = if (leftOrdinal == 0) keyValues else valueValues
      val rightValues = if (rightOrdinal == 0) keyValues else valueValues
      val end = start + length
      var row = start
      while (row < end) {
        if (leftKeep(leftValues(row)) && rightKeep(rightValues(row))) then add(keyValues(row), 1)
        row += 1
      }

    private def addTwoDropNullKey(
        start: Int,
        length: Int,
        leftOrdinal: Int,
        leftKeep: ColumnKernel.Int32Predicate,
        rightOrdinal: Int,
        rightKeep: ColumnKernel.Int32Predicate,
        keyValues: Array[Int],
        keyWords: Array[Long],
        valueValues: Array[Int],
    ): Unit =
      val leftValues = if (leftOrdinal == 0) keyValues else valueValues
      val rightValues = if (rightOrdinal == 0) keyValues else valueValues
      val end = start + length
      var row = start
      while (row < end) {
        if (
            leftKeep(leftValues(row)) && rightKeep(rightValues(row)) &&
            Validity.isSet(keyWords, row)
          )
        then add(keyValues(row), 1)
        row += 1
      }

    private def addTwoChecked(
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
        skipNullKey: Boolean,
    ): Unit =
      val leftValues = if (leftOrdinal == 0) keyValues else valueValues
      val rightValues = if (rightOrdinal == 0) keyValues else valueValues
      val leftWords = if (leftOrdinal == 0) keyWords else valueWords
      val rightWords = if (rightOrdinal == 0) keyWords else valueWords
      val end = start + length
      var row = start
      if (skipNullKey) then
        while (row < end) {
          if (
              Validity.isSet(leftWords, row) && leftKeep(leftValues(row)) &&
              Validity.isSet(rightWords, row) && rightKeep(rightValues(row))
            )
          then add(keyValues(row), 1)
          row += 1
        }
      else
        while (row < end) {
          if (
              Validity.isSet(leftWords, row) && leftKeep(leftValues(row)) &&
              Validity.isSet(rightWords, row) && rightKeep(rightValues(row)) &&
              Validity.isSet(keyWords, row)
            )
          then add(keyValues(row), 1)
          row += 1
        }

    private def scanOne(
        start: Int,
        length: Int,
        ordinal: Int,
        keep: ColumnKernel.Int32Predicate,
        keyValues: Array[Int],
        keyWords: Array[Long],
        valueValues: Array[Int],
        valueWords: Array[Long],
        stepsDense: Boolean,
        skipNullKey: Boolean,
    ): Unit =
      val values = if (ordinal == 0) keyValues else valueValues
      val words = if (ordinal == 0) keyWords else valueWords
      val end = start + length
      var row = start
      if (stepsDense && skipNullKey) then
        while (row < end) {
          if (keep(values(row))) then add(keyValues(row), 1)
          row += 1
        }
      else if (stepsDense) then
        while (row < end) {
          if (keep(values(row)) && Validity.isSet(keyWords, row)) then add(keyValues(row), 1)
          row += 1
        }
      else if (skipNullKey) then
        while (row < end) {
          if (Validity.isSet(words, row) && keep(values(row))) then add(keyValues(row), 1)
          row += 1
        }
      else
        while (row < end) {
          if (Validity.isSet(words, row) && keep(values(row)) && Validity.isSet(keyWords, row))
          then add(keyValues(row), 1)
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
    ): Unit =
      val end = start + length
      var row = start
      while (row < end) {
        val kept =
          if (stepsDense) then keepsDense(steps, row, keyValues, valueValues)
          else keepsChecked(steps, row, keyValues, keyWords, valueValues, valueWords)
        if (kept && (skipNullKey || Validity.isSet(keyWords, row))) then add(keyValues(row), 1)
        row += 1
      }

    private def add(key: Int, weight: Int): Unit =
      val slot = locate(key)
      if (state(slot) == 0) then
        if (occupied >= limit) then
          throw new IllegalStateException(s"hash count table is full at $occupied groups")
        state(slot) = 1
        keys(slot) = key
        counts(slot) = weight
        slots(occupied) = slot
        occupied += 1
      else counts(slot) = counts(slot) + weight

    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      while (state(slot) != 0 && keys(slot) != key) slot = (slot + 1) & mask
      slot

  @tailrec
  private def keepsDense(
      stack: List[Selection.Step],
      row: Int,
      keyValues: Array[Int],
      valueValues: Array[Int],
  ): Boolean =
    stack match
      case Nil => true
      case step :: rest =>
        if (keepsDenseStep(step, row, keyValues, valueValues)) then
          keepsDense(rest, row, keyValues, valueValues)
        else false

  @tailrec
  private def keepsChecked(
      stack: List[Selection.Step],
      row: Int,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
  ): Boolean =
    stack match
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
      rejectedStep

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
      rejectedStep

  private def rejectedStep: Boolean =
    require(false, "folded count steps are int slots")
    false

  private val unusedInt32: Int32Column =
    Int32Column.of(Array.empty[Int], Validity.allValid(0), 0)
