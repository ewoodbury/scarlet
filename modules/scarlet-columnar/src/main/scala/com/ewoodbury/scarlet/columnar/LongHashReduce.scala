package com.ewoodbury.scarlet.columnar

/**
 * Hash reduce over `Int64` keys and `Int64` values.
 *
 * The binary operator is the caller's. It is applied as values are inserted, including when
 * partial groups are merged, so it has to be associative and commutative. Arithmetic wraps at
 * `Long`. This is the `(Long, Long)` `reduceByKey` kernel. There is no spill path: an input that
 * does not fit stays in memory.
 *
 * A null key is dropped. A null value stays a null group until another value for that key arrives.
 * The row reducer unboxes a null `Long` to `0` when the operator runs, and leaves a group of one
 * null as null. Partial combines use that same rule, so a later merge still sees the null. This
 * kernel preserves input order: output order is first-seen order inside the batch. Keys in the
 * output are present. A null group clears the value bit and does not publish a payload. The table
 * is at least twice the row count, so one batch does not rehash.
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
   * Partial-reduce each morsel, merge those partials per input batch, exchange on the key, then
   * reduce each bucket. An empty input is `numPartitions` empty long-pair batches. The exchange
   * bucket is `Long.hashCode`, the same rule `HashPartitioner` uses for a `Long` key.
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
      val buckets =
        HashExchange.partitionAllInt64(combined, keyOrdinal = 0, numPartitions, parallelism)
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
