package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Hash reduce over `Int64` keys and `Int64` values.
 *
 * The binary operator is the caller's. It is applied as values are inserted, including when
 * partial groups are merged, so it has to be associative and commutative. Arithmetic wraps at
 * `Long`. This is the `(Long, Long)` `reduceByKey` kernel. Spill is off unless
 * [[ScarletConf.spillEnabled]] is set. Only the post-exchange bucket spills; the partial combine
 * before the exchange stays in memory.
 *
 * A null key is dropped. A null value stays a null group until another value for that key arrives.
 * The row reducer unboxes a null `Long` to `0` when the operator runs, and leaves a group of one
 * null as null. Partial combines use that same rule, so a later merge still sees the null. This
 * kernel preserves input order: output order is first-seen order inside the batch. Keys in the
 * output are present. A null group clears the value bit and does not publish a payload. The table
 * is at least twice the row count, so one batch does not rehash.
 *
 * [[int64Selected]] scans a survivor index with that same null rule. [[byKeySelected]]
 * partial-reduces the index, then exchanges and spills the dense partials the same way [[byKey]]
 * does. The exchange does not see the source rows. Spill charges the dense partial, not the
 * survivor count.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object LongHashReduce:

  trait LongBinOp:
    def apply(left: Long, right: Long): Long

  def int64(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      op: LongBinOp,
  ): ColumnBatch =
    val keys = ColumnHash.int64At(batch, keyOrdinal)
    val values = ColumnHash.int64At(batch, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then ColumnBatches.emptyLongPairs
    else reduce(keys, values, op)

  /**
   * [[int64]] over `rows(from until until)`. A null key is skipped. A present key with a null
   * value calls the missing-value rule. A present value is combined. A second null becomes present
   * through `op(0, 0)`. Slots at `count` and beyond are not read. An empty range publishes an
   * empty long-pair batch.
   */
  def int64Selected(
      selected: Selected,
      from: Int,
      until: Int,
      keyOrdinal: Int,
      valueOrdinal: Int,
      op: LongBinOp,
  ): ColumnBatch =
    require(from >= 0, s"selection start must be >= 0, got $from")
    require(until >= from, s"selection end $until is before start $from")
    require(
      until <= selected.count,
      s"selection end $until exceeds count ${selected.count}",
    )
    val keys = ColumnHash.int64At(selected.batch, keyOrdinal)
    val values = ColumnHash.int64At(selected.batch, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    if (until == from) then ColumnBatches.emptyLongPairs
    else
      val table = new Table(ColumnHash.tableSize(until - from), op)
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
        table.scanDenseSelected(keys, values, selected.rows, from, until)
      else table.scanSparseSelected(keys, values, selected.rows, from, until)
      table.compact()

  /**
   * Partial-reduce each morsel, merge those partials per input batch, exchange on the key, then
   * reduce each bucket. An empty input is `numPartitions` empty long-pair batches. The exchange
   * bucket is `Long.hashCode`, the same rule `HashPartitioner` uses for a `Long` key.
   *
   * Spill is off unless [[ScarletConf.spillEnabled]] is set. A bucket whose
   * [[LongHashReduceSpill.retainedBytes]] estimate is above [[ScarletConf.spillBudgetBytes]] is
   * sub-partitioned, written morsel by morsel, and folded with [[int64]] on the concatenation of
   * the running partial and the new one. That path runs one bucket at a time, so the bucket's file
   * is deleted before the next bucket opens a store. A run whose buckets all fit stays on
   * [[MorselScheduler]], including when spill is enabled and none of them exceed the budget. Spill
   * off is the in-memory [[int64]] result. A spilled bucket may change first-seen output order.
   */
  def byKey(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: LongBinOp,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongPairs)
    else
      val combined = PartialCombine.perBatch(
        batches,
        batchSize,
        parallelism,
        morsel => int64(morsel, keyOrdinal = 0, valueOrdinal = 1, op),
        partials => int64(partials, keyOrdinal = 0, valueOrdinal = 1, op),
      )
      reduceExchanged(combined, numPartitions, parallelism, batchSize, op)

  /**
   * [[byKey]] for a selection. Each live prefix is partial-reduced, the dense partials are
   * exchanged, and each bucket is reduced with [[int64]]. Filtered rows die in the scan. An empty
   * input is `numPartitions` empty long-pair batches. Spill matches [[byKey]]: the bucket is dense
   * partials, so the estimate is [[LongHashReduceSpill.retainedBytes]] of that length.
   */
  def byKeySelected(
      selected: Vector[Selected],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: LongBinOp,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (selected.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongPairs)
    else
      val combined = PartialCombine.perBatchSelected(
        selected,
        batchSize,
        parallelism,
        (one, from, until) =>
          int64Selected(one, from, until, keyOrdinal = 0, valueOrdinal = 1, op),
        partials => int64(partials, keyOrdinal = 0, valueOrdinal = 1, op),
      )
      reduceExchanged(combined, numPartitions, parallelism, batchSize, op)

  /**
   * Exchange combined partials and reduce each bucket. Shared by [[byKey]] and [[byKeySelected]].
   */
  private def reduceExchanged(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: LongBinOp,
  ): Vector[ColumnBatch] =
    val buckets =
      HashExchange.partitionAllInt64(combined, keyOrdinal = 0, numPartitions, parallelism)
    val conf = ScarletConf.get
    if (buckets.exists(bucket => LongHashReduceSpill.exceeds(bucket, conf))) then
      buckets.map(bucket => LongHashReduceSpill.finish(bucket, parallelism, batchSize, conf, op))
    else
      MorselScheduler.map(buckets, parallelism) { (_, bucket) =>
        int64(bucket, keyOrdinal = 0, valueOrdinal = 1, op)
      }

  private def reduce(keys: Int64Column, values: Int64Column, op: LongBinOp): ColumnBatch =
    val table = new Table(ColumnHash.tableSize(keys.length), op)
    if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
      table.scanDense(keys, values)
    else table.scanSparse(keys, values)
    table.compact()

  /** Empty slot, a present value, or a null value that has not yet met another value. */
  private val empty: Byte = 0
  private val present: Byte = 1
  private val missing: Byte = 2
  private val zero: Long = 0L

  private final class Table(capacity: Int, op: LongBinOp):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Long] = new Array[Long](capacity)
    private val values: Array[Long] = new Array[Long](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def scanDense(keyColumn: Int64Column, valueColumn: Int64Column): Unit =
      val keyValues = keyColumn.values
      val valueValues = valueColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        addPresent(keyValues(row), valueValues(row))
        row += 1
      }

    def scanSparse(keyColumn: Int64Column, valueColumn: Int64Column): Unit =
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
        keyColumn: Int64Column,
        valueColumn: Int64Column,
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
        keyColumn: Int64Column,
        valueColumn: Int64Column,
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
      val outKeys = new Array[Long](rows)
      val outValues = new Array[Long](rows)
      val valueWords = Validity.allocate(rows)
      var cursor = 0
      while (cursor < rows) {
        val slot = slots(cursor)
        outKeys(cursor) = keys(slot)
        if (state(slot) == present) then
          outValues(cursor) = values(slot)
          Validity.setBit(valueWords, cursor)
        cursor += 1
      }
      new ColumnBatch(
        Vector(LogicalType.Int64, LogicalType.Int64),
        Vector(
          Int64Column.of(outKeys, Validity.allValid(rows), rows),
          Int64Column.of(outValues, Validity.fromWords(valueWords, rows), rows),
        ),
        rows,
      )

    private def addPresent(key: Long, value: Long): Unit =
      val slot = locate(key)
      val mark = state(slot)
      if (mark == empty) then occupy(slot, key, value, present)
      else if (mark == present) then values(slot) = op(values(slot), value)
      else
        values(slot) = op(zero, value)
        state(slot) = present

    private def addMissing(key: Long): Unit =
      val slot = locate(key)
      val mark = state(slot)
      if (mark == empty) then occupy(slot, key, zero, missing)
      else if (mark == present) then values(slot) = op(values(slot), zero)
      else
        values(slot) = op(zero, zero)
        state(slot) = present

    private def occupy(slot: Int, key: Long, value: Long, mark: Byte): Unit =
      if (occupied >= limit) then
        throw new IllegalStateException(s"hash reduce table is full at $occupied groups")
      state(slot) = mark
      keys(slot) = key
      values(slot) = value
      slots(occupied) = slot
      occupied += 1

    private def locate(key: Long): Int =
      var slot = ColumnHash.mix(java.lang.Long.hashCode(key)) & mask
      while (state(slot) != empty && keys(slot) != key) slot = (slot + 1) & mask
      slot
