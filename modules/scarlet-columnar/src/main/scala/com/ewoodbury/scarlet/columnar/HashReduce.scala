package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Hash reduce over `Int32` keys and `Int32` values.
 *
 * The binary operator is the caller's. It is applied as values are inserted, including when
 * partial groups are merged, so it has to be associative and commutative. Arithmetic wraps at
 * `Int`. This is the `DistCollection.reduceByKey` kernel. Grouped sums that must not wrap use
 * [[HashAggregate.sumInt32]] and [[HashAggregate.sumInt64]] instead.
 *
 * Null keys are dropped. A lone null value stays null and does not call the operator. A null that
 * meets a present value, and two nulls, call the operator with that null unboxed to `0`. Output
 * order is first-seen order inside the batch. The table is at least twice the row count, so one
 * batch does not rehash.
 *
 * [[int32Selected]] scans a survivor index. [[byKeySelected]] partial-reduces that index, then
 * exchanges and spills the dense partials the same way [[byKey]] does. The exchange does not see
 * the source rows.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashReduce:

  trait IntBinOp:
    def apply(left: Int, right: Int): Int

  def int32(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      op: IntBinOp,
  ): ColumnBatch =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then ColumnBatches.emptyIntPairs
    else reduce(keys, values, op)

  /**
   * [[int32]] over `rows(from until until)`. A null key is dropped. A lone null value stays null.
   * A null that meets a present value calls the operator with `0`. Slots at `count` and beyond are
   * not read. An empty range publishes an empty pair batch.
   */
  def int32Selected(
      selected: Selected,
      from: Int,
      until: Int,
      keyOrdinal: Int,
      valueOrdinal: Int,
      op: IntBinOp,
  ): ColumnBatch =
    require(from >= 0, s"selection start must be >= 0, got $from")
    require(until >= from, s"selection end $until is before start $from")
    require(
      until <= selected.count,
      s"selection end $until exceeds count ${selected.count}",
    )
    val keys = ColumnHash.int32At(selected.batch, keyOrdinal)
    val values = ColumnHash.int32At(selected.batch, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    if (until == from) then ColumnBatches.emptyIntPairs
    else
      val table = new Table(ColumnHash.tableSize(until - from), op)
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
        table.scanDenseSelected(keys, values, selected.rows, from, until)
      else table.scanSparseSelected(keys, values, selected.rows, from, until)
      table.compact()

  /**
   * Partial-reduce each morsel, merge those partials per input batch, exchange on the key, then
   * reduce each bucket. An empty input is `numPartitions` empty pair batches.
   *
   * Spill is off unless [[ScarletConf.spillEnabled]] is set. A bucket whose
   * [[HashReduceSpill.retainedBytes]] estimate is above [[ScarletConf.spillBudgetBytes]] is
   * sub-partitioned, written morsel by morsel, and folded with [[HashReduce.int32]] on the
   * concatenation of the running partial and the new one. That path runs one bucket at a time, so
   * the bucket's file is deleted before the next bucket opens a store. A run whose buckets all fit
   * stays on [[MorselScheduler]], including when spill is enabled and none of them exceed the
   * budget. Spill off is the in-memory [[int32]] result.
   */
  def byKey(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: IntBinOp,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val _ = byKeyCalls.incrementAndGet()
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      val combined = PartialCombine.perBatch(
        batches,
        batchSize,
        parallelism,
        morsel => int32(morsel, keyOrdinal = 0, valueOrdinal = 1, op),
        partials => int32(partials, keyOrdinal = 0, valueOrdinal = 1, op),
      )
      reduceExchanged(combined, numPartitions, parallelism, batchSize, op)

  /**
   * [[byKey]] for a selection. Each live prefix is partial-reduced, the dense partials are
   * exchanged, and each bucket is reduced. Filtered rows die in the scan. An empty input is
   * `numPartitions` empty pair batches. Spill matches [[byKey]].
   */
  def byKeySelected(
      selected: Vector[Selected],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: IntBinOp,
  ): Vector[ColumnBatch] =
    val _ = byKeySelectedCalls.incrementAndGet()
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (selected.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      val combined = PartialCombine.perBatchSelected(
        selected,
        batchSize,
        parallelism,
        (one, from, until) =>
          int32Selected(one, from, until, keyOrdinal = 0, valueOrdinal = 1, op),
        partials => int32(partials, keyOrdinal = 0, valueOrdinal = 1, op),
      )
      reduceExchanged(combined, numPartitions, parallelism, batchSize, op)

  /**
   * Reduce pairs a caller supplies. No pair column is read.
   *
   * `rows` sizes the table the way [[int32]] sizes one morsel. A zero estimate publishes an empty
   * pair batch and does not call `supply`. `supply` receives `add` and calls it once per present
   * pair. A null key is the caller's to skip. A null value is
   * [[IntPairProject.IntPairSink.addMissing]]. This is not [[byKey]]: nothing here is a published
   * pair project.
   */
  private[scarlet] def partialPairs(
      rows: Int,
      op: IntBinOp,
  )(supply: IntPairProject.IntPairSink => Unit): ColumnBatch =
    require(rows >= 0, s"row estimate must be >= 0, got $rows")
    if (rows == 0) then ColumnBatches.emptyIntPairs
    else
      val table = new Table(ColumnHash.tableSize(rows), op)
      table.supplyPairs(supply)
      table.compact()

  /** [[byKey]] entries since the last [[resetByKeyForTest]]. [[byKeySelected]] does not count. */
  private[scarlet] def byKeyCallsForTest: Int = byKeyCalls.get

  /**
   * [[byKeySelected]] entries since the last [[resetByKeyForTest]]. [[byKey]] does not count.
   */
  private[scarlet] def byKeySelectedCallsForTest: Int = byKeySelectedCalls.get

  /** Clears [[byKeyCallsForTest]] and [[byKeySelectedCallsForTest]]. */
  private[scarlet] def resetByKeyForTest(): Unit =
    byKeyCalls.set(0)
    byKeySelectedCalls.set(0)

  /**
   * Exchange combined partials and reduce each bucket. Shared by [[byKey]], [[byKeySelected]], and
   * a projected pair reduce. [[byKey]] is not called.
   */
  private[scarlet] def reduceExchanged(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: IntBinOp,
  ): Vector[ColumnBatch] =
    val buckets = HashExchange.partitionAll(combined, keyOrdinal = 0, numPartitions, parallelism)
    val conf = ScarletConf.get
    if (buckets.exists(bucket => HashReduceSpill.exceeds(bucket, conf))) then
      buckets.map(bucket => HashReduceSpill.finish(bucket, parallelism, batchSize, conf, op))
    else
      MorselScheduler.map(buckets, parallelism) { (_, bucket) =>
        int32(bucket, keyOrdinal = 0, valueOrdinal = 1, op)
      }

  private def reduce(keys: Int32Column, values: Int32Column, op: IntBinOp): ColumnBatch =
    val table = new Table(ColumnHash.tableSize(keys.length), op)
    if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
      table.scanDense(keys, values)
    else table.scanSparse(keys, values)
    table.compact()

  /** Empty slot, a present value, or a null value that has not yet met another value. */
  private val empty: Byte = 0
  private val present: Byte = 1
  private val missing: Byte = 2

  private final class Table(capacity: Int, op: IntBinOp):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Int] = new Array[Int](capacity)
    private val values: Array[Int] = new Array[Int](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def scanDense(keyColumn: Int32Column, valueColumn: Int32Column): Unit =
      val keyValues = keyColumn.values
      val valueValues = valueColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        addPresent(keyValues(row), valueValues(row))
        row += 1
      }

    def scanSparse(keyColumn: Int32Column, valueColumn: Int32Column): Unit =
      val keyValues = keyColumn.values
      val valueValues = valueColumn.values
      val keyWords = keyColumn.validity.words
      val valueWords = valueColumn.validity.words
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          if (Validity.isSet(valueWords, row)) then addPresent(keyValues(row), valueValues(row))
          else addMissing(keyValues(row))
        row += 1
      }

    def scanDenseSelected(
        keyColumn: Int32Column,
        valueColumn: Int32Column,
        ids: Array[Int],
        from: Int,
        until: Int,
    ): Unit =
      val keyValues = keyColumn.values
      val valueValues = valueColumn.values
      var slot = from
      while (slot < until) {
        val row = ids(slot)
        addPresent(keyValues(row), valueValues(row))
        slot += 1
      }

    def scanSparseSelected(
        keyColumn: Int32Column,
        valueColumn: Int32Column,
        ids: Array[Int],
        from: Int,
        until: Int,
    ): Unit =
      val keyValues = keyColumn.values
      val valueValues = valueColumn.values
      val keyWords = keyColumn.validity.words
      val valueWords = valueColumn.validity.words
      var slot = from
      while (slot < until) {
        val row = ids(slot)
        if (Validity.isSet(keyWords, row)) then
          if (Validity.isSet(valueWords, row)) then addPresent(keyValues(row), valueValues(row))
          else addMissing(keyValues(row))
        slot += 1
      }

    def compact(): ColumnBatch =
      val rows = occupied
      val outKeys = new Array[Int](rows)
      val outValues = new Array[Int](rows)
      var cursor = 0
      var hole = false
      while (cursor < rows && !hole) {
        if (state(slots(cursor)) != present) then hole = true
        cursor += 1
      }
      val valueValidity =
        if (!hole) then Validity.allValid(rows)
        else
          val words = Validity.allocate(rows)
          cursor = 0
          while (cursor < rows) {
            val slot = slots(cursor)
            if (state(slot) == present) then
              outValues(cursor) = values(slot)
              Validity.setBit(words, cursor)
            cursor += 1
          }
          Validity.fromWords(words, rows)
      if (!hole) then
        cursor = 0
        while (cursor < rows) {
          val slot = slots(cursor)
          outKeys(cursor) = keys(slot)
          outValues(cursor) = values(slot)
          cursor += 1
        }
      else
        cursor = 0
        while (cursor < rows) {
          outKeys(cursor) = keys(slots(cursor))
          cursor += 1
        }
      val keyValidity = Validity.allValid(rows)
      new ColumnBatch(
        Vector(LogicalType.Int32, LogicalType.Int32),
        Vector(
          Int32Column.of(outKeys, keyValidity, rows),
          Int32Column.of(outValues, valueValidity, rows),
        ),
        rows,
      )

    /** Hands `add` and `addMissing` to `supply`. The lambda stays inside [[Table]]. */
    def supplyPairs(supply: IntPairProject.IntPairSink => Unit): Unit =
      val sink = new IntPairProject.IntPairSink:
        def add(key: Int, value: Int): Unit = addPresent(key, value)
        def addMissing(key: Int): Unit = Table.this.addMissing(key)
      supply(sink)

    private def addPresent(key: Int, value: Int): Unit =
      val slot = locate(key)
      if (state(slot) == empty) then occupy(slot, key, value, present)
      else if (state(slot) == present) then values(slot) = op(values(slot), value)
      else
        values(slot) = op(0, value)
        state(slot) = present

    /** A null value. A lone null stays missing. Meeting a value unboxes the null to `0`. */
    private def addMissing(key: Int): Unit =
      val slot = locate(key)
      if (state(slot) == empty) then occupy(slot, key, 0, missing)
      else if (state(slot) == present) then values(slot) = op(values(slot), 0)
      else
        values(slot) = op(0, 0)
        state(slot) = present

    private def occupy(slot: Int, key: Int, value: Int, mark: Byte): Unit =
      if (occupied >= limit) then
        throw new IllegalStateException(s"hash reduce table is full at $occupied groups")
      state(slot) = mark
      keys(slot) = key
      values(slot) = value
      slots(occupied) = slot
      occupied += 1

    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      while (state(slot) != empty && keys(slot) != key) slot = (slot + 1) & mask
      slot

  private val byKeyCalls = new AtomicInteger
  private val byKeySelectedCalls = new AtomicInteger
