package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Hash aggregate over one batch. Group by an `Int32` key. Null keys are dropped and the key is not
 * read.
 *
 * On the plain overloads, a null value does not update a measure and the group still exists. Sum,
 * min, and max stay null until a value is seen. Count stays `0`.
 *
 * The overloads that take `keep` and `map` run filter, then project, then aggregate, in one scan.
 * A null value is dropped before `keep`, and a rejected row never creates a group. A key whose
 * rows are all dropped is absent. A `keep` that accepts every present value is therefore not the
 * plain overload.
 *
 * `sumInt32` reads `Int32` values and writes an `Int64` sum. `sumInt64` reads `Int64` values. One
 * batch of partial sums, or of partial counts, merges by calling `sumInt64` on that column.
 * Several partial-sum batches fold through [[mergeSumPartials]] without being concatenated. Min
 * and max are not merged here.
 *
 * Output order is first-seen order, not sorted. Sums do not wrap at `Int` width. The value map
 * itself is still `Int` arithmetic.
 *
 * One open-addressed table serves every measure set. A batch-sized table is at least twice the row
 * count, so one batch does not rehash. The range scans reuse one small table per worker and grow
 * it, and `grow` keeps first-seen order. [[GroupedSumKernel]] sums a bounded key range through
 * [[DenseSum]] on this same worker, and leaves this table for a wide range or a pair step that can
 * rewrite the key. Direct-index partials come out in ascending key order. This table stays
 * first-seen. The kernel drops both when the action returns. A shorter capacity exists so tests
 * can force `grow`. Loops keep the row index in a `var` so the bodies stay allocation-free,
 * matching `ColumnKernel`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashAggregate:

  /**
   * Measures emitted after the key, in this order: sum, count, min, max. Build it with named
   * arguments. The four booleans are easy to swap if they are passed by position.
   */
  final case class Int32Aggs(sum: Boolean, count: Boolean, min: Boolean, max: Boolean)

  /** Keeps a group whose values are all null, and leaves its sum null. */
  def sumInt32(batch: ColumnBatch, keyOrdinal: Int, valueOrdinal: Int): ColumnBatch =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    sumColumns(
      keys,
      values,
      ColumnHash.tableSize(keys.length),
      filtered = false,
      KeepAll,
      Identity,
    )

  /**
   * Drops a null value before `keep`. A key whose rows are all dropped is absent, unlike the plain
   * `sumInt32`.
   */
  def sumInt32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    sumColumns(keys, values, ColumnHash.tableSize(keys.length), filtered = true, keep, map)

  /**
   * Runs `steps` on each present pair, then aggregates, in one scan. A null key or a null value is
   * dropped before the steps, and a rejected row never creates a group. Value-only filters use the
   * `keep` and `map` overload instead, because those functions do not read the key.
   */
  def sumInt32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      steps: Vector[PairStep],
  ): ColumnBatch =
    require(steps.nonEmpty, "pair sum needs a filter or a map")
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then ColumnBatches.emptyIntLongSum
    else
      val table = new GroupTable(ColumnHash.tableSize(keys.length), SumOnly)
      val slot = new PairSlot
      val planned = steps.toArray
      val end = keys.length
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
        scanSumDenseSteps(table, keys, values, planned, slot, 0, end)
      else scanSumSparseSteps(table, keys, values, planned, slot, 0, end)
      table.compact()

  /**
   * Sums `[start, start + length)` of the two columns into the worker's table. The columns are
   * read in place. The same table is cleared and reused for the next range on this thread.
   * [[releaseSumWorkers]] drops it when the action returns.
   */
  private[columnar] def sumRange(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
  ): ColumnBatch =
    checkRange(keys, values, start, length)
    val end = start + length
    val dense = denseRange(keys, values, start, length)
    withSumTable { (table, _) =>
      if (length != 0) then
        if (dense) then scanSumDense(table, keys, values, start, end)
        else scanSumSparse(table, keys, values, filtered = false, KeepAll, Identity, start, end)
    }

  /** Same range as [[sumRange]], after `keep` and `map`. A null value is dropped before `keep`. */
  private[columnar] def sumRange(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    checkRange(keys, values, start, length)
    val end = start + length
    val dense = denseRange(keys, values, start, length)
    withSumTable { (table, _) =>
      if (length != 0) then
        if (dense) then scanSumDenseFiltered(table, keys, values, keep, map, start, end)
        else scanSumSparse(table, keys, values, filtered = true, keep, map, start, end)
    }

  /**
   * Same range as [[sumRange]], after `steps`. A null key or a null value is dropped before the
   * steps. `steps` is shared by every worker and is not mutated.
   */
  private[columnar] def sumRange(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      steps: Array[PairStep],
  ): ColumnBatch =
    require(steps.nonEmpty, "pair sum needs a filter or a map")
    checkRange(keys, values, start, length)
    val end = start + length
    val dense = denseRange(keys, values, start, length)
    withSumTable { (table, slot) =>
      if (length != 0) then
        if (dense) then scanSumDenseSteps(table, keys, values, steps, slot, start, end)
        else scanSumSparseSteps(table, keys, values, steps, slot, start, end)
    }

  /**
   * Sums a morsel into the worker's current zero-origin window when that window still fits.
   * Returns `None` when the window is missing, the origin is not zero, or a key or a null forces a
   * new survey. A rejected attempt leaves the table empty.
   */
  private[columnar] def tryReuseDirect(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      denseCap: Int,
  ): Option[ColumnBatch] =
    if (length <= 0) then None
    else
      checkRange(keys, values, start, length)
      val dense = sumWorkers.get().dense
      if (!dense.reuseZero(denseCap, length)) then None
      else
        var accepted = false
        guard(dense) {
          accepted = dense.sumReuse(keys, values, start, start + length)
        }
        if (accepted) then Some(dense.compact())
        else
          dense.abandonDirect()
          None

  /**
   * Sums `[start, start + length)` into the worker's direct-index table. `minKey` and `keyRange`
   * are the classified window, and the table is that window rather than the cap. A key outside the
   * window fails the scan. A failure after slots were written clears them before the exception
   * propagates. The next range on this thread calls `prepare` again. [[releaseSumWorkers]] drops
   * the table.
   */
  private[columnar] def indexSumRange(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      minKey: Int,
      keyRange: Int,
      columnsPresent: Boolean,
  ): ColumnBatch =
    checkRange(keys, values, start, length)
    val dense = sumWorkers.get().dense
    dense.prepare(minKey, keyRange)
    guard(dense) {
      if (length != 0) then
        val end = start + length
        if (columnsPresent && minKey == 0) then dense.addPresentZero(keys, values, start, end)
        else if (columnsPresent) then dense.addTrusted(keys, values, start, end)
        else dense.scanPlain(keys, values, start, end)
    }
    dense.compact()

  /**
   * Same window as [[indexSumRange]], after `keep` and `map`. A null value is dropped before
   * `keep`.
   */
  private[columnar] def indexSumRange(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      minKey: Int,
      keyRange: Int,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    checkRange(keys, values, start, length)
    val dense = sumWorkers.get().dense
    dense.prepare(minKey, keyRange)
    guard(dense) {
      if (length != 0) then dense.scanFiltered(keys, values, start, start + length, keep, map)
    }
    dense.compact()

  /** The next direct scan on this thread throws once `afterGroups` new groups are recorded. */
  private[columnar] def armDirectFailure(afterGroups: Int): Unit =
    sumWorkers.get().dense.failAfter(afterGroups)

  /** Groups currently recorded in this thread's direct-index table. */
  private[columnar] def directTouchedSlots: Int =
    sumWorkers.get().dense.touchedSlots

  /** A failed scan must not leave sums for the next morsel that reuses this window. */
  private def guard(dense: DenseSum.Accumulator)(body: => Unit): Unit =
    try body
    catch
      case ex: RuntimeException =>
        dense.abandonDirect()
        throw ex

  /**
   * Sums an `Int64` column into an `Int64` sum. This merges one batch of partial sums, and partial
   * counts when `valueOrdinal` points at the count column. A null value does not update the sum,
   * and the group still exists. Several partial-sum batches use [[mergeSumPartials]].
   */
  def sumInt64(batch: ColumnBatch, keyOrdinal: Int, valueOrdinal: Int): ColumnBatch =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int64At(batch, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then ColumnBatches.emptyIntLongSum
    else
      val table = new GroupTable(ColumnHash.tableSize(keys.length), SumOnly)
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
        scanSumLongDense(table, keys, values)
      else scanSumLongSparse(table, keys, values)
      table.compact()

  /**
   * Folds partial-sum batches into one partial, in order. The table starts at
   * `tableSize(2 * widest)`, where `widest` is the longest partial. A later partial that
   * introduces new groups may grow the table. Disjoint partials rehash: each grow doubles the
   * table, so the number of grows is at most `log2(total / widest)` when `total` is the number of
   * distinct keys. A widest partial above the seed limit is rejected before that table is
   * allocated.
   *
   * Each partial picks its own scan: dense only when that partial's key and value columns are
   * fully present, otherwise sparse. A null sum is not read as zero. A null key is dropped. A
   * group that is null and then valued becomes valued; the reverse stays valued; a group that is
   * only ever null stays present with a null sum.
   *
   * The slots of the most recent dense key sequence are reused when the next dense partial repeats
   * that sequence. `grow` and `release` drop the cached sequence.
   */
  private[columnar] def mergeSumPartials(
      partials: Vector[ColumnBatch],
      keyOrdinal: Int,
      valueOrdinal: Int,
  ): ColumnBatch =
    if (partials.isEmpty) then ColumnBatches.emptyIntLongSum
    else
      val batches = partials.toArray
      val table = new GroupTable(mergeSeed(widestPartial(batches)), SumOnly)
      var index = 0
      val count = batches.length
      while (index < count) {
        foldSumPartial(table, batches(index), keyOrdinal, valueOrdinal)
        index += 1
      }
      noteMergeCapacity(table.currentCapacity)
      table.compact()

  /**
   * Slot count of the table the last [[mergeSumPartials]] on this thread finished with.
   *
   * The slot is thread-local. A merge that runs on a worker updates that worker's value. A read on
   * the caller, including a read while those workers are still in the merge, is the caller's
   * previous value.
   */
  private[columnar] def mergeSumTableCapacity: Int =
    mergeSlots.get().capacity

  /** Keeps a group whose values are all null. Sum, min, and max are null, and count is 0. */
  def aggregateInt32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      aggs: Int32Aggs,
  ): ColumnBatch =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    aggregateColumns(
      keys,
      values,
      aggs,
      filtered = false,
      KeepAll,
      Identity,
      ColumnHash.tableSize(keys.length),
    )

  /**
   * Drops a null value before `keep`. A key whose rows are all dropped is absent, unlike the plain
   * `aggregateInt32`.
   */
  def aggregateInt32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      aggs: Int32Aggs,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    aggregateColumns(
      keys,
      values,
      aggs,
      filtered = true,
      keep,
      map,
      ColumnHash.tableSize(keys.length),
    )

  /**
   * Same as `aggregateInt32`, but the table starts at `initialCapacity` instead of `tableSize`.
   * Tests pass a short power of two so `grow` runs. Production callers use the row-sized table.
   */
  private[columnar] def aggregateInt32Rehashing(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      aggs: Int32Aggs,
      initialCapacity: Int,
  ): ColumnBatch =
    require(
      Integer.bitCount(initialCapacity) == 1 &&
        initialCapacity >= 16 &&
        initialCapacity <= ColumnHash.MaxCapacity,
      s"initial capacity must be a power of two from 16 to ${ColumnHash.MaxCapacity}",
    )
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    aggregateColumns(keys, values, aggs, filtered = false, KeepAll, Identity, initialCapacity)

  private object KeepAll extends ColumnKernel.Int32Predicate:
    def apply(value: Int): Boolean = true

  private object Identity extends ColumnKernel.Int32Map:
    def apply(value: Int): Int = value

  private val SumOnly = Int32Aggs(sum = true, count = false, min = false, max = false)

  /** Smallest power of two `GroupTable` accepts. The worker table grows from here. */
  private[columnar] val SumTableStart: Int = 16

  private final class SumWorker:
    val table: GroupTable = new GroupTable(SumTableStart, SumOnly)
    val slot: PairSlot = new PairSlot
    val dense: DenseSum.Accumulator = new DenseSum.Accumulator

  private val sumWorkers: ThreadLocal[SumWorker] = new ThreadLocal[SumWorker]:
    override def initialValue(): SumWorker = new SumWorker

  private final class MergeSlots:
    var capacity: Int = 0

  private val mergeSlots: ThreadLocal[MergeSlots] = new ThreadLocal[MergeSlots]:
    override def initialValue(): MergeSlots = new MergeSlots

  private def noteMergeCapacity(capacity: Int): Unit =
    mergeSlots.get().capacity = capacity

  private def widestPartial(partials: Array[ColumnBatch]): Int =
    var widest = 0
    var index = 0
    val count = partials.length
    while (index < count) {
      val rows = partials(index).length
      if (rows > widest) then widest = rows
      index += 1
    }
    widest

  /**
   * Capacity for a table sized from the longest partial. `tableSize` doubles the count again and
   * rounds up, so a widest partial above `MaxCapacity >>> 3` would seed the maximum table and
   * allocate it before the first probe. `MaxCapacity >>> 2` still rounds up to that table.
   */
  private[columnar] def mergeSeed(widest: Int): Int =
    val limit = ColumnHash.MaxCapacity >>> 3
    require(widest <= limit, s"widest partial $widest exceeds $limit")
    ColumnHash.tableSize(widest << 1)

  private def foldSumPartial(
      table: GroupTable,
      partial: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
  ): Unit =
    val keys = ColumnHash.int32At(partial, keyOrdinal)
    val values = ColumnHash.int64At(partial, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length != 0) then
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
        scanSumLongDense(table, keys, values, remember = true)
      else scanSumLongSparse(table, keys, values)

  /**
   * Drops this thread's reused sum table and its direct-index accumulator. [[GroupedSumKernel]]
   * calls this when the action returns. A range runs on the calling thread when `parallelism` is 1
   * or there is one morsel, and that thread outlives the action.
   */
  private[columnar] def releaseSumWorkers(): Unit =
    sumWorkers.remove()

  /** Slot count of this thread's reused sum table. The first read on a thread allocates it. */
  private[columnar] def sumWorkerCapacity: Int =
    sumWorkers.get().table.currentCapacity

  private def withSumTable(body: (GroupTable, PairSlot) => Unit): ColumnBatch =
    val worker = sumWorkers.get()
    val table = worker.table
    // The next range clears before it reads, including a range that follows a failed scan.
    table.release()
    body(table, worker.slot)
    table.compact()

  private def checkRange(keys: Int32Column, values: Int32Column, start: Int, length: Int): Unit =
    require(keys.length == values.length, "key and value columns differ in length")
    require(
      start >= 0 && length >= 0 && start <= keys.length && length <= keys.length - start,
      s"range ($start, $length) outside ${keys.length} rows",
    )

  private def denseRange(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
  ): Boolean =
    Validity.rangePresent(keys.validity, start, length) &&
      Validity.rangePresent(values.validity, start, length)

  private def sumColumns(
      keys: Int32Column,
      values: Int32Column,
      capacity: Int,
      filtered: Boolean,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then ColumnBatches.emptyIntLongSum
    else
      val table = new GroupTable(capacity, SumOnly)
      val end = keys.length
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
        if (filtered) then scanSumDenseFiltered(table, keys, values, keep, map, 0, end)
        else scanSumDense(table, keys, values, 0, end)
      else scanSumSparse(table, keys, values, filtered, keep, map, 0, end)
      table.compact()

  private def aggregateColumns(
      keys: Int32Column,
      values: Int32Column,
      aggs: Int32Aggs,
      filtered: Boolean,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
      capacity: Int,
  ): ColumnBatch =
    require(aggs.sum || aggs.count || aggs.min || aggs.max, "aggregate needs at least one measure")
    if (aggs.sum && !aggs.count && !aggs.min && !aggs.max) then
      sumColumns(keys, values, capacity, filtered, keep, map)
    else
      require(keys.length == values.length, "key and value columns differ in length")
      if (keys.length == 0) then empty(schema(aggs))
      else
        val table = new GroupTable(capacity, aggs)
        if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
          if (filtered) then scanMeasureDenseFiltered(table, keys, values, keep, map)
          else scanMeasureDense(table, keys, values)
        else scanMeasureSparse(table, keys, values, filtered, keep, map)
        table.compact()

  private def scanSumDense(
      table: GroupTable,
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      end: Int,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    var row = start
    while (row < end) {
      table.addSum(keyValues(row), valueValues(row).toLong)
      row += 1
    }

  private def scanSumDenseFiltered(
      table: GroupTable,
      keys: Int32Column,
      values: Int32Column,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
      start: Int,
      end: Int,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    var row = start
    while (row < end) {
      val value = valueValues(row)
      if (keep(value)) then table.addSum(keyValues(row), map(value).toLong)
      row += 1
    }

  private def scanSumSparse(
      table: GroupTable,
      keys: Int32Column,
      values: Int32Column,
      filtered: Boolean,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
      start: Int,
      end: Int,
  ): Unit =
    val keyValues = keys.values
    val keyWords = keys.validity.words
    val valueValues = values.values
    val valueWords = values.validity.words
    var row = start
    while (row < end) {
      if (Validity.isSet(keyWords, row)) then
        val valuePresent = Validity.isSet(valueWords, row)
        if (filtered) then
          if (valuePresent) then
            val value = valueValues(row)
            if (keep(value)) then table.addSum(keyValues(row), map(value).toLong)
        else
          val slot = table.ensure(keyValues(row))
          if (valuePresent) then table.accumulate(slot, valueValues(row).toLong)
      row += 1
    }

  /** One reused pair so a step scan does not allocate a tuple per row. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class PairSlot:
    var key: Int = 0
    var value: Int = 0
    var kept: Boolean = false

  private def scanSumDenseSteps(
      table: GroupTable,
      keys: Int32Column,
      values: Int32Column,
      steps: Array[PairStep],
      slot: PairSlot,
      start: Int,
      end: Int,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    var row = start
    while (row < end) {
      applySteps(keyValues(row), valueValues(row), steps, slot)
      if (slot.kept) then table.addSum(slot.key, slot.value.toLong)
      row += 1
    }

  private def scanSumSparseSteps(
      table: GroupTable,
      keys: Int32Column,
      values: Int32Column,
      steps: Array[PairStep],
      slot: PairSlot,
      start: Int,
      end: Int,
  ): Unit =
    val keyValues = keys.values
    val keyWords = keys.validity.words
    val valueValues = values.values
    val valueWords = values.validity.words
    var row = start
    while (row < end) {
      if (Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row)) then
        applySteps(keyValues(row), valueValues(row), steps, slot)
        if (slot.kept) then table.addSum(slot.key, slot.value.toLong)
      row += 1
    }

  private def applySteps(key0: Int, value0: Int, steps: Array[PairStep], slot: PairSlot): Unit =
    val width = steps.length
    var key = key0
    var value = value0
    var kept = true
    var index = 0
    while (kept && index < width) {
      steps(index) match
        case PairStep.KeepKey(predicate) => kept = predicate(key)
        case PairStep.KeepValue(predicate) => kept = predicate(value)
        case PairStep.KeepRow(predicate) => kept = predicate(key, value)
        case PairStep.MapPair(mapper) =>
          val projected = mapper(key, value)
          key = projected._1
          value = projected._2
      index += 1
    }
    slot.key = key
    slot.value = value
    slot.kept = kept

  private def scanSumLongDense(
      table: GroupTable,
      keys: Int32Column,
      values: Int64Column,
      remember: Boolean = false,
  ): Unit =
    table.addPresentSums(keys.values, values.values, keys.length, remember)

  private def scanSumLongSparse(table: GroupTable, keys: Int32Column, values: Int64Column): Unit =
    val keyValues = keys.values
    val keyWords = keys.validity.words
    val valueValues = values.values
    val valueWords = values.validity.words
    val rows = keys.length
    var row = 0
    while (row < rows) {
      if (Validity.isSet(keyWords, row)) then
        val slot = table.ensure(keyValues(row))
        if (Validity.isSet(valueWords, row)) then table.accumulate(slot, valueValues(row))
      row += 1
    }

  private def scanMeasureDense(table: GroupTable, keys: Int32Column, values: Int32Column): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val rows = keys.length
    var row = 0
    while (row < rows) {
      table.observe(table.ensure(keyValues(row)), valueValues(row))
      row += 1
    }

  private def scanMeasureDenseFiltered(
      table: GroupTable,
      keys: Int32Column,
      values: Int32Column,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val rows = keys.length
    var row = 0
    while (row < rows) {
      val value = valueValues(row)
      if (keep(value)) then table.observe(table.ensure(keyValues(row)), map(value))
      row += 1
    }

  private def scanMeasureSparse(
      table: GroupTable,
      keys: Int32Column,
      values: Int32Column,
      filtered: Boolean,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): Unit =
    val keyValues = keys.values
    val keyWords = keys.validity.words
    val valueValues = values.values
    val valueWords = values.validity.words
    val rows = keys.length
    var row = 0
    while (row < rows) {
      if (Validity.isSet(keyWords, row)) then
        val valuePresent = Validity.isSet(valueWords, row)
        if (filtered) then
          if (valuePresent) then
            val value = valueValues(row)
            if (keep(value)) then table.observe(table.ensure(keyValues(row)), map(value))
        else
          val slot = table.ensure(keyValues(row))
          if (valuePresent) then table.observe(slot, valueValues(row))
      row += 1
    }

  private final class GroupTable(initialCapacity: Int, aggs: Int32Aggs):
    private var capacity: Int = initialCapacity
    private var mask: Int = initialCapacity - 1
    private var occupied: Int = 0
    private var limit: Int = initialCapacity >>> 1
    private var missing: Int = 0
    private var keys: Array[Int] = new Array[Int](initialCapacity)
    private var state: Array[Byte] = new Array[Byte](initialCapacity)
    private var sums: Array[Long] =
      if (aggs.sum) new Array[Long](initialCapacity) else Array.emptyLongArray
    private var counts: Array[Long] =
      if (aggs.count) new Array[Long](initialCapacity) else Array.emptyLongArray
    private var mins: Array[Int] =
      if (aggs.min) new Array[Int](initialCapacity) else Array.emptyIntArray
    private var maxs: Array[Int] =
      if (aggs.max) new Array[Int](initialCapacity) else Array.emptyIntArray
    private var seen: Array[Byte] = new Array[Byte](initialCapacity)
    private var slots: Array[Int] = new Array[Int](16)
    private var cachedKeys: Array[Int] = Array.emptyIntArray
    private var cachedSlots: Array[Int] = Array.emptyIntArray
    // Row count of the most recent dense key sequence. Zero after grow and release, which also
    // drop `cachedKeys` and `cachedSlots`.
    private var cachedRows: Int = 0
    private val hasSum: Boolean = aggs.sum
    private val hasCount: Boolean = aggs.count
    private val hasMin: Boolean = aggs.min
    private val hasMax: Boolean = aggs.max

    def currentCapacity: Int = capacity

    def addSum(key: Int, value: Long): Unit =
      accumulate(ensure(key), value)

    /**
     * Adds a present value for every row. A key that is already in the table updates its sum in
     * this loop. A new key goes through [[ensure]], which may grow the table. When `remember` is
     * set, the slots from this probe are kept. Only that most recent dense sequence is cached. A
     * later partial reuses the slots when its keys match.
     */
    def addPresentSums(
        keyValues: Array[Int],
        valueValues: Array[Long],
        rows: Int,
        remember: Boolean,
    ): Unit =
      if (
          remember && cachedRows > 0 && rows == cachedRows && sameKeys(cachedKeys, keyValues, rows)
        )
      then addCachedSums(valueValues, rows)
      else probePresentSums(keyValues, valueValues, rows, remember)

    private def addCachedSums(valueValues: Array[Long], rows: Int): Unit =
      assert(rows > 0, "cached sequence is empty")
      val sumSlots = sums
      val placed = cachedSlots
      val seenSlots = seen
      var row = 0
      while (row < rows) {
        val slot = placed(row)
        // The probe that filled the cache noticed this slot. compact drops a sum whose seen bit
        // is clear, so a slot that was not noticed must not reuse.
        assert(seenSlots(slot) != 0, "cached slot was not seen")
        sumSlots(slot) += valueValues(row)
        row += 1
      }

    private def probePresentSums(
        keyValues: Array[Int],
        valueValues: Array[Long],
        rows: Int,
        remember: Boolean,
    ): Unit =
      val placed = if (remember) new Array[Int](rows) else Array.emptyIntArray
      val started = capacity
      var keySlots = keys
      var occupiedState = state
      var sumSlots = sums
      var seenSlots = seen
      var probeMask = mask
      val trackSeen = missing != 0
      var stable = true
      var row = 0
      while (row < rows) {
        val key = keyValues(row)
        var slot = ColumnHash.mix(key) & probeMask
        while (occupiedState(slot) != 0 && keySlots(slot) != key)
          slot = (slot + 1) & probeMask
        if (occupiedState(slot) != 0) then
          sumSlots(slot) += valueValues(row)
          if (trackSeen && seenSlots(slot) == 0) then
            seenSlots(slot) = 1
            missing -= 1
          if (remember && stable) then placed(row) = slot
        else
          val inserted = ensure(key)
          accumulate(inserted, valueValues(row))
          if (capacity != started) then stable = false
          if (remember && stable) then placed(row) = inserted
          if (capacity != started) then
            keySlots = keys
            occupiedState = state
            sumSlots = sums
            seenSlots = seen
            probeMask = mask
        row += 1
      }
      if (remember && stable) then
        cachedKeys = Arrays.copyOf(keyValues, rows)
        cachedSlots = placed
        cachedRows = rows

    private def sameKeys(left: Array[Int], right: Array[Int], rows: Int): Boolean =
      Arrays.equals(left, 0, rows, right, 0, rows)

    def accumulate(slot: Int, value: Long): Unit =
      sums(slot) += value
      notice(slot)

    def ensure(key: Int): Int =
      val found = locate(key)
      if (state(found) != 0) then found
      else
        // A duplicate of a live key must not grow. Grow only when this key is absent and the
        // table is already at its load limit.
        val slot = slotForNew(key, found)
        state(slot) = 1
        keys(slot) = key
        missing += 1
        remember(slot)
        slot

    /**
     * `found` is empty. At the load limit, rehash before inserting so the probe still terminates.
     */
    private def slotForNew(key: Int, found: Int): Int =
      if (occupied < limit) then found
      else
        grow()
        locate(key)

    def observe(slot: Int, value: Int): Unit =
      if (seen(slot) == 0) then
        if (hasSum) then sums(slot) = value.toLong
        if (hasCount) then counts(slot) = 1L
        if (hasMin) then mins(slot) = value
        if (hasMax) then maxs(slot) = value
        notice(slot)
      else
        if (hasSum) then sums(slot) += value.toLong
        if (hasCount) then counts(slot) += 1L
        if (hasMin && value < mins(slot)) then mins(slot) = value
        if (hasMax && value > maxs(slot)) then maxs(slot) = value

    def compact(): ColumnBatch =
      val rows = occupied
      val outKeys = new Array[Int](rows)
      val outSums = if (hasSum) new Array[Long](rows) else Array.emptyLongArray
      val outCounts = if (hasCount) new Array[Long](rows) else Array.emptyLongArray
      val outMins = if (hasMin) new Array[Int](rows) else Array.emptyIntArray
      val outMaxs = if (hasMax) new Array[Int](rows) else Array.emptyIntArray
      val sharesValidity = hasSum || hasMin || hasMax
      val sparse = sharesValidity && missing != 0
      val words = if (sparse) Validity.allocate(rows) else Array.emptyLongArray
      var cursor = 0
      while (cursor < rows) {
        val slot = slots(cursor)
        outKeys(cursor) = keys(slot)
        if (seen(slot) != 0) then
          if (hasSum) then outSums(cursor) = sums(slot)
          if (hasCount) then outCounts(cursor) = counts(slot)
          if (hasMin) then outMins(cursor) = mins(slot)
          if (hasMax) then outMaxs(cursor) = maxs(slot)
          if (sparse) then Validity.setBit(words, cursor)
        cursor += 1
      }
      val keyColumn = Int32Column.of(outKeys, Validity.allValid(rows), rows)
      val valueValidity =
        if (sparse) Validity.fromWords(words, rows) else Validity.allValid(rows)
      // An unseen count stays 0 because `outCounts` is zero-filled.
      // Sum, min, and max share `valueValidity`. A published bitmap is not mutated.
      val measures = Vector(
        Option.when(hasSum) {
          LogicalType.Int64 -> (Int64Column.of(outSums, valueValidity, rows): Column)
        },
        Option.when(hasCount) {
          LogicalType.Int64 -> (Int64Column.of(outCounts, Validity.allValid(rows), rows): Column)
        },
        Option.when(hasMin) {
          LogicalType.Int32 -> (Int32Column.of(outMins, valueValidity, rows): Column)
        },
        Option.when(hasMax) {
          LogicalType.Int32 -> (Int32Column.of(outMaxs, valueValidity, rows): Column)
        },
      ).flatten
      new ColumnBatch(
        LogicalType.Int32 +: measures.map(_._1),
        keyColumn +: measures.map(_._2),
        rows,
      )

    /**
     * Drops every group this morsel inserted. Walking `slots` leaves the rest of the arrays alone.
     * `compact` publishes count, min, and max once a slot is seen, and the sum scan marks a slot
     * seen without rewriting those measures, so a reused slot must not keep them. Only the sum
     * table is reused today.
     */
    def release(): Unit =
      var cursor = 0
      val rows = occupied
      while (cursor < rows) {
        val slot = slots(cursor)
        state(slot) = 0
        seen(slot) = 0
        if (hasSum) then sums(slot) = 0L
        if (hasCount) then counts(slot) = 0L
        if (hasMin) then mins(slot) = 0
        if (hasMax) then maxs(slot) = 0
        cursor += 1
      }
      occupied = 0
      missing = 0
      dropCachedSequence()

    private def dropCachedSequence(): Unit =
      cachedKeys = Array.emptyIntArray
      cachedSlots = Array.emptyIntArray
      cachedRows = 0

    private def notice(slot: Int): Unit =
      if (seen(slot) == 0) then
        seen(slot) = 1
        missing -= 1

    /** Doubles the table and reinserts `slots(0 until occupied)`, preserving first-seen order. */
    private def grow(): Unit =
      dropCachedSequence()
      if (capacity >= ColumnHash.MaxCapacity) then
        throw new IllegalStateException(
          s"hash aggregate table is already at capacity $capacity",
        )
      val oldKeys = keys
      val oldSums = sums
      val oldCounts = counts
      val oldMins = mins
      val oldMaxs = maxs
      val oldSeen = seen
      val live = occupied
      capacity = capacity << 1
      mask = capacity - 1
      limit = capacity >>> 1
      keys = new Array[Int](capacity)
      state = new Array[Byte](capacity)
      sums = if (hasSum) new Array[Long](capacity) else Array.emptyLongArray
      counts = if (hasCount) new Array[Long](capacity) else Array.emptyLongArray
      mins = if (hasMin) new Array[Int](capacity) else Array.emptyIntArray
      maxs = if (hasMax) new Array[Int](capacity) else Array.emptyIntArray
      seen = new Array[Byte](capacity)
      occupied = 0
      var cursor = 0
      while (cursor < live) {
        val oldSlot = slots(cursor)
        val placed = locate(oldKeys(oldSlot))
        state(placed) = 1
        keys(placed) = oldKeys(oldSlot)
        if (hasSum) then sums(placed) = oldSums(oldSlot)
        if (hasCount) then counts(placed) = oldCounts(oldSlot)
        if (hasMin) then mins(placed) = oldMins(oldSlot)
        if (hasMax) then maxs(placed) = oldMaxs(oldSlot)
        seen(placed) = oldSeen(oldSlot)
        slots(cursor) = placed
        occupied += 1
        cursor += 1
      }

    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      while (state(slot) != 0 && keys(slot) != key) slot = (slot + 1) & mask
      slot

    private def remember(slot: Int): Unit =
      if (occupied == slots.length) then
        val grown = slots.length << 1
        if (grown <= 0) then
          throw new IllegalStateException(s"group list is already at ${slots.length}")
        slots = Arrays.copyOf(slots, grown)
      slots(occupied) = slot
      occupied += 1

  private def empty(types: Vector[LogicalType]): ColumnBatch =
    val columns = types.map { logicalType =>
      logicalType match
        case LogicalType.Int32 =>
          Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0): Column
        case LogicalType.Int64 =>
          Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0): Column
        case other =>
          throw new IllegalArgumentException(s"aggregate schema cannot contain $other")
    }
    new ColumnBatch(types, columns, 0)

  private def schema(aggs: Int32Aggs): Vector[LogicalType] =
    val measures = Vector(
      Option.when(aggs.sum)(LogicalType.Int64),
      Option.when(aggs.count)(LogicalType.Int64),
      Option.when(aggs.min)(LogicalType.Int32),
      Option.when(aggs.max)(LogicalType.Int32),
    ).flatten
    LogicalType.Int32 +: measures
