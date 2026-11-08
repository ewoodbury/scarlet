package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Counts `(Int, _)` rows by key.
 *
 * Null keys are dropped. The value column is never read, so a null value still counts. Each
 * present key contributes one. The stored count is an `Int` and wraps on overflow, the same
 * two's-complement wrap [[HashReduce]] uses when it adds.
 *
 * [[byKey]] counts dense batches. [[byKeySelected]] counts a survivor index. Both partial-count
 * with [[PartialCombine]] and then exchange the `(key, count)` partials. The exchange does not see
 * the source rows.
 *
 * There is no spill path. Before any table is allocated, the input-row byte estimate is compared
 * with [[ScarletConf.spillBudgetBytes]]. The check ignores [[ScarletConf.spillEnabled]]. Over
 * budget this throws [[ColumnarFallback]] and the action reruns on rows.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashCount:

  /**
   * Count one batch. A null key is skipped. Column 1 is not read. An empty batch publishes an
   * empty pair batch and does not allocate a table.
   */
  def int32(batch: ColumnBatch): ColumnBatch =
    requireKey(batch)
    val keys = ColumnHash.int32At(batch, 0)
    if (keys.length == 0) then ColumnBatches.emptyIntPairs
    else
      refuse(keys.length.toLong)
      countKeys(keys)

  /**
   * [[int32]] over `rows(from until until)`. A null key in that prefix is skipped. Slots at
   * `count` and beyond are not read. An empty range publishes an empty pair batch.
   */
  def int32Selected(selected: Selected, from: Int, until: Int): ColumnBatch =
    require(from >= 0, s"selection start must be >= 0, got $from")
    require(until >= from, s"selection end $until is before start $from")
    require(until <= selected.count, s"selection end $until exceeds count ${selected.count}")
    requireKey(selected.batch)
    val keys = ColumnHash.int32At(selected.batch, 0)
    if (until == from) then ColumnBatches.emptyIntPairs
    else
      refuse((until - from).toLong)
      val table = new Table(ColumnHash.tableSize(until - from))
      if (ColumnHash.fullyPresent(keys)) then
        table.scanDenseSelected(keys, selected.rows, from, until)
      else table.scanSparseSelected(keys, selected.rows, from, until)
      table.compact()

  /**
   * Partial-count each morsel, merge those partials per input batch, exchange on the key, then add
   * the partial counts in each bucket. An empty input is `numPartitions` empty pair batches.
   *
   * The source value column is dropped before the morsel split, so the split does not copy it. A
   * run whose input-row estimate is above [[ScarletConf.spillBudgetBytes]] throws
   * [[ColumnarFallback]] before that split. `spillEnabled` is not consulted.
   */
  def byKey(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      batches.foreach(requireKey)
      refuse(batches.iterator.map(_.length.toLong).sum)
      val keysOnly = batches.map(batch => ColumnBatches.takeColumn(batch, 0))
      val combined = PartialCombine.perBatch(keysOnly, batchSize, parallelism, int32, addCounts)
      finish(combined, numPartitions, parallelism)

  /**
   * [[byKey]] for a selection. Each live prefix is partial-counted, the dense partials are
   * exchanged, and each bucket adds those counts. Filtered rows die in the scan. An empty input is
   * `numPartitions` empty pair batches. The budget check charges survivor rows, not the rows the
   * filter dropped.
   */
  def byKeySelected(
      selected: Vector[Selected],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (selected.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      selected.foreach(one => requireKey(one.batch))
      refuse(selected.iterator.map(_.count.toLong).sum)
      val combined = PartialCombine.perBatchSelected(
        selected,
        batchSize,
        parallelism,
        (one, from, until) => int32Selected(one, from, until),
        addCounts,
      )
      finish(combined, numPartitions, parallelism)

  /**
   * Estimated bytes of a count table for `rows` input keys.
   *
   * Every input row is charged as its own key: 4 bytes plus one validity byte. The open-addressed
   * table is charged at [[ColumnHash.tableSize]] slots, each a key, an `Int` count, an occupancy
   * byte, and a slot index. The source value column is not included. Zero rows publish an empty
   * batch and allocate nothing, so the estimate is zero.
   */
  private[columnar] def inputRowBytes(rows: Long): Long =
    if (rows <= 0L) then 0L
    else
      val capped = if (rows > Int.MaxValue.toLong) Int.MaxValue else rows.toInt
      val slots = ColumnHash.tableSize(capped).toLong
      rows * (4L + 1L) + slots * (4L + 4L + 1L + 4L)

  private def finish(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    if (combined.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      val buckets = HashExchange.partitionAll(combined, keyOrdinal = 0, numPartitions, parallelism)
      MorselScheduler.map(buckets, parallelism)((_, bucket) => addCounts(bucket))

  /**
   * Add `(key, count)` partials. This reads the count this operator stored, not the source value.
   */
  private def addCounts(batch: ColumnBatch): ColumnBatch =
    if (batch.length == 0) then ColumnBatches.emptyIntPairs
    else
      refuse(batch.length.toLong)
      val keys = ColumnHash.int32At(batch, 0)
      val weights = ColumnHash.int32At(batch, 1)
      require(keys.length == weights.length, "key and count columns differ in length")
      val table = new Table(ColumnHash.tableSize(keys.length))
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(weights)) then
        table.scanWeights(keys, weights)
      else table.scanWeightsSparse(keys, weights)
      table.compact()

  private def countKeys(keys: Int32Column): ColumnBatch =
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
      throw new IllegalArgumentException("countByKey expects an Int32 key column")
    }
    keyType match
      case LogicalType.Int32 => ()
      case LogicalType.Int64 | LogicalType.Float64 | LogicalType.Bool | LogicalType.Utf8Dict =>
        throw new IllegalArgumentException(
          s"countByKey expects an Int32 key column, found $keyType",
        )

  /**
   * Refuse before a table exists. `spillEnabled` is ignored: this operator has no spill copy, and
   * a disabled spill flag must not keep an over-budget input on the columnar path.
   */
  private def refuse(rows: Long): Unit =
    val budget = ScarletConf.get.spillBudgetBytes
    val bytes = inputRowBytes(rows)
    if (bytes > budget) then
      throw new ColumnarFallback(
        s"countByKey keeps no spill copy; $rows input rows ($bytes bytes) exceed the $budget byte budget",
      )

  private final class Table(capacity: Int):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Int] = new Array[Int](capacity)
    private val counts: Array[Int] = new Array[Int](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def scanDense(keyColumn: Int32Column): Unit =
      val keyValues = keyColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        add(keyValues(row), 1)
        row += 1
      }

    def scanSparse(keyColumn: Int32Column): Unit =
      val keyValues = keyColumn.values
      val keyWords = keyColumn.validity.words
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then add(keyValues(row), 1)
        row += 1
      }

    def scanDenseSelected(
        keyColumn: Int32Column,
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
        keyColumn: Int32Column,
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

    def scanWeights(keyColumn: Int32Column, weightColumn: Int32Column): Unit =
      val keyValues = keyColumn.values
      val weightValues = weightColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        add(keyValues(row), weightValues(row))
        row += 1
      }

    def scanWeightsSparse(keyColumn: Int32Column, weightColumn: Int32Column): Unit =
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
