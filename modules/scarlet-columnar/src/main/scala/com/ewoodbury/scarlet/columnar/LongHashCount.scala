package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Counts `(Long, _)` rows by key.
 *
 * Null keys are dropped. The value column is never read, so a null value still counts. Each
 * present key contributes one. The stored count is an `Int` and wraps on overflow, the same
 * two's-complement wrap [[HashCount]] uses when it adds. The count is not widened to `Long`.
 *
 * [[byKey]] counts dense batches. [[byKeySelected]] counts a survivor index. Both partial-count
 * with [[PartialCombine]] and then exchange the `(Long, Int)` partials with
 * [[HashExchange.partitionAllInt64]]. The exchange does not see the source rows. The partial
 * combine before the exchange stays in memory.
 *
 * Spill is off unless [[ScarletConf.spillEnabled]] is set, and that flag defaults off. While it is
 * off, the input-row estimate is compared with [[ScarletConf.spillBudgetBytes]] before any table,
 * partial combine, or store. [[refuse]] throws [[ClosedKernelRefusal]] when the estimate is over
 * the budget, and the action fails instead of rerunning on rows. While spill is on, that
 * whole-input check is skipped when a one-row table fits in the budget. A budget that cannot hold
 * one row still refuses the input row count: every morsel would refuse, and the reported count is
 * the input rather than whichever morsel failed. [[int64]] and [[int64Selected]] still refuse a
 * morsel whose estimate is over the budget. After the exchange, a bucket at or under the budget is
 * added in memory. A bucket over the budget is finished by [[LongHashCountSpill]]. A table that
 * still does not fit throws [[ClosedKernelRefusal]] before it is allocated. The running partial is
 * `O(distinct keys)`. [[inputRowBytes]] charges every row about to be inserted as its own key,
 * which overestimates when partials share a key. That overestimate is intentional. It charges
 * 8-byte keys, not the 4-byte keys [[HashCount.inputRowBytes]] uses.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object LongHashCount:

  /**
   * Count one batch. A null key is skipped. Column 1 is not read. An empty batch publishes an
   * empty count batch and does not allocate a table. A non-empty batch whose row estimate is over
   * the budget throws [[ClosedKernelRefusal]] before the table is allocated.
   */
  def int64(batch: ColumnBatch): ColumnBatch =
    requireKey(batch)
    val keys = ColumnHash.int64At(batch, 0)
    if (keys.length == 0) then ColumnBatches.emptyLongCounts
    else
      refuse(keys.length.toLong)
      countKeys(keys)

  /**
   * [[int64]] over `rows(from until until)`. A null key in that prefix is skipped. Slots at
   * `count` and beyond are not read. An empty range publishes an empty count batch. A non-empty
   * range whose row estimate is over the budget throws [[ClosedKernelRefusal]] before the table is
   * allocated.
   */
  def int64Selected(selected: Selected, from: Int, until: Int): ColumnBatch =
    require(from >= 0, s"selection start must be >= 0, got $from")
    require(until >= from, s"selection end $until is before start $from")
    require(until <= selected.count, s"selection end $until exceeds count ${selected.count}")
    requireKey(selected.batch)
    val keys = ColumnHash.int64At(selected.batch, 0)
    if (until == from) then ColumnBatches.emptyLongCounts
    else
      refuse((until - from).toLong)
      val table = new Table(ColumnHash.tableSize(until - from))
      if (ColumnHash.fullyPresent(keys)) then
        table.scanDenseSelected(keys, selected.rows, from, until)
      else table.scanSparseSelected(keys, selected.rows, from, until)
      table.compact()

  /**
   * Partial-count each morsel, merge those partials per input batch, exchange on the key, then add
   * the partial counts in each bucket. An empty input is `numPartitions` empty count batches.
   *
   * The source value column is dropped before the morsel split, so the split does not copy it and
   * a null value is not unboxed. Spill off refuses the input-row estimate before that split. Spill
   * on skips that refusal when a one-row table fits, spills an over-budget bucket after the
   * exchange, and still refuses a morsel or a fold that does not fit.
   */
  def byKey(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongCounts)
    else
      batches.foreach(requireKey)
      val rows = batches.iterator.map(_.length.toLong).sum
      if (refuseBeforeCombine) then refuse(rows)
      val keysOnly = batches.map(batch => ColumnBatches.takeColumn(batch, 0))
      val combined = PartialCombine.perBatch(keysOnly, batchSize, parallelism, int64, addCounts)
      finish(combined, numPartitions, parallelism, batchSize)

  /**
   * [[byKey]] for a selection. Each live prefix is partial-counted, the dense partials are
   * exchanged, and each bucket adds those counts. Filtered rows die in the scan. An empty input is
   * `numPartitions` empty count batches. The budget check charges survivor rows, not the rows the
   * filter dropped. Spill matches [[byKey]]. The value column is not read.
   */
  def byKeySelected(
      selected: Vector[Selected],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (selected.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongCounts)
    else
      selected.foreach(one => requireKey(one.batch))
      val rows = selected.iterator.map(_.count.toLong).sum
      if (refuseBeforeCombine) then refuse(rows)
      val combined = PartialCombine.perBatchSelected(
        selected,
        batchSize,
        parallelism,
        (one, from, until) => int64Selected(one, from, until),
        addCounts,
      )
      finish(combined, numPartitions, parallelism, batchSize)

  /**
   * Estimated bytes of a count table for `rows` input keys.
   *
   * Every input row is charged as its own key: 8 bytes plus one validity byte. The open-addressed
   * table is charged at [[ColumnHash.tableSize]] slots, each an 8-byte key, an `Int` count, an
   * occupancy byte, and a slot index. The source value column is not included. Zero rows publish
   * an empty batch and allocate nothing, so the estimate is zero. This is not
   * [[HashCount.inputRowBytes]], which charges 4-byte keys.
   */
  private[columnar] def inputRowBytes(rows: Long): Long =
    if (rows <= 0L) then 0L
    else
      val capped = if (rows > Int.MaxValue.toLong) Int.MaxValue else rows.toInt
      val slots = ColumnHash.tableSize(capped).toLong
      rows * (8L + 1L) + slots * (8L + 4L + 1L + 4L)

  private def finish(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    if (combined.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongCounts)
    else
      val buckets =
        HashExchange.partitionAllInt64(combined, keyOrdinal = 0, numPartitions, parallelism)
      val conf = ScarletConf.get
      if (buckets.exists(bucket => LongHashCountSpill.exceeds(bucket, conf))) then
        buckets.map(bucket => LongHashCountSpill.finish(bucket, parallelism, batchSize, conf))
      else MorselScheduler.map(buckets, parallelism)((_, bucket) => addCounts(bucket))

  /**
   * Add `(key, count)` partials. This reads the count this operator stored, not the source value.
   * A non-empty batch is refused before the table is allocated. [[LongHashCountSpill]] folds with
   * this kernel, so a partial that does not fit throws [[ClosedKernelRefusal]] and does not
   * allocate.
   */
  private[scarlet] def addCounts(batch: ColumnBatch): ColumnBatch =
    if (batch.length == 0) then ColumnBatches.emptyLongCounts
    else
      refuse(batch.length.toLong)
      val keys = ColumnHash.int64At(batch, 0)
      val weights = ColumnHash.int32At(batch, 1)
      require(keys.length == weights.length, "key and count columns differ in length")
      val table = new Table(ColumnHash.tableSize(keys.length))
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(weights)) then
        table.scanWeights(keys, weights)
      else table.scanWeightsSparse(keys, weights)
      table.compact()

  private def countKeys(keys: Int64Column): ColumnBatch =
    val table = new Table(ColumnHash.tableSize(keys.length))
    if (ColumnHash.fullyPresent(keys)) then table.scanDense(keys)
    else table.scanSparse(keys)
    table.compact()

  private def requirePositive(numPartitions: Int, parallelism: Int, batchSize: Int): Unit =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")

  private def requireKey(batch: ColumnBatch): Unit =
    val keyType = batch.schema.headOption.getOrElse {
      throw new ColumnarFallback("countByKey expects an Int64 key column")
    }
    keyType match
      case LogicalType.Int64 => ()
      case LogicalType.Int32 | LogicalType.Float64 | LogicalType.Bool | LogicalType.Utf8Dict =>
        throw new ColumnarFallback(
          s"countByKey expects an Int64 key column, found $keyType",
        )

  /**
   * Whether the whole input is refused before a partial combine.
   *
   * Spill off always checks. Spill on checks only when a one-row table does not fit in the budget.
   * Every morsel would refuse in that case, and a parallel morsel failure would report whichever
   * morsel lost the race. The input count is what the action already publishes for that budget. A
   * budget that can hold one row skips this check. Morsels still refuse themselves, and an
   * over-budget bucket spills after the exchange.
   */
  private def refuseBeforeCombine: Boolean =
    val conf = ScarletConf.get
    !conf.spillEnabled || inputRowBytes(1L) > conf.spillBudgetBytes

  /**
   * Refuse before a table exists. This does not read [[ScarletConf.spillEnabled]]. The throw is
   * not a [[ColumnarFallback]], so the action fails instead of rerunning on rows. Callers skip the
   * whole-input call when spill is on and a one-row table fits. A morsel, a partial combine, and a
   * spill fold still call this.
   */
  private def refuse(rows: Long): Unit =
    val budget = ScarletConf.get.spillBudgetBytes
    val bytes = inputRowBytes(rows)
    if (bytes > budget) then throw new ClosedKernelRefusal(rows, bytes, budget)

  private final class Table(capacity: Int):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Long] = new Array[Long](capacity)
    private val counts: Array[Int] = new Array[Int](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def scanDense(keyColumn: Int64Column): Unit =
      val keyValues = keyColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        add(keyValues(row), 1)
        row += 1
      }

    def scanSparse(keyColumn: Int64Column): Unit =
      val keyValues = keyColumn.values
      val keyWords = keyColumn.validity.words
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then add(keyValues(row), 1)
        row += 1
      }

    def scanDenseSelected(
        keyColumn: Int64Column,
        ids: Array[Int],
        from: Int,
        until: Int,
    ): Unit =
      val keyValues = keyColumn.values
      var slot = from
      while (slot < until) {
        add(keyValues(ids(slot)), 1)
        slot += 1
      }

    def scanSparseSelected(
        keyColumn: Int64Column,
        ids: Array[Int],
        from: Int,
        until: Int,
    ): Unit =
      val keyValues = keyColumn.values
      val keyWords = keyColumn.validity.words
      var slot = from
      while (slot < until) {
        val row = ids(slot)
        if (Validity.isSet(keyWords, row)) then add(keyValues(row), 1)
        slot += 1
      }

    def scanWeights(keyColumn: Int64Column, weightColumn: Int32Column): Unit =
      val keyValues = keyColumn.values
      val weightValues = weightColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        add(keyValues(row), weightValues(row))
        row += 1
      }

    def scanWeightsSparse(keyColumn: Int64Column, weightColumn: Int32Column): Unit =
      val keyValues = keyColumn.values
      val weightValues = weightColumn.values
      val keyWords = keyColumn.validity.words
      val weightWords = weightColumn.validity.words
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row) && Validity.isSet(weightWords, row)) then
          add(keyValues(row), weightValues(row))
        row += 1
      }

    def compact(): ColumnBatch =
      val rows = occupied
      val outKeys = new Array[Long](rows)
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
        Vector(LogicalType.Int64, LogicalType.Int32),
        Vector(Int64Column.of(outKeys, present, rows), Int32Column.of(outCounts, present, rows)),
        rows,
      )

    private def add(key: Long, weight: Int): Unit =
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

    /** Probe with `Long.hashCode`, then keep two longs that share a hash code distinct. */
    private def locate(key: Long): Int =
      var slot = ColumnHash.mix(java.lang.Long.hashCode(key)) & mask
      while (state(slot) != 0 && keys(slot) != key) slot = (slot + 1) & mask
      slot
