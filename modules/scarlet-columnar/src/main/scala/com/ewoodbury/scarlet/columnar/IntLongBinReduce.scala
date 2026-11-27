package com.ewoodbury.scarlet.columnar

/**
 * Hash reduce over `Int32` keys and `Int64` values.
 *
 * The binary operator is the caller's. It is applied as values are inserted, including when
 * partial groups are merged, so it has to be associative. Arithmetic wraps at `Long`. This is the
 * non-adding `(Int, Long)` `reduceByKey`. Addition stays on [[IntLongMap.reduce]]. There is no
 * spill path.
 *
 * A null key is dropped. A null value stays a null group until another value for that key arrives.
 * One null value stays null and the operator is not called. The next value unboxes a null as `0`:
 * null then present is `op(0, present)`, present then null is `op(present, 0)`, and two nulls is
 * `op(0, 0)`. Output order is first-seen order inside the batch. Keys in the output are present. A
 * null group clears the value bit.
 *
 * Partials are combined per input batch, exchanged with [[HashExchange.partitionAll]] on the int
 * key, then reduced again. Empty input is `numPartitions` empty `(Int32, Int64)` batches.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object IntLongBinReduce:

  trait LongBinOp:
    def apply(left: Long, right: Long): Long

  private val Schema = Vector(LogicalType.Int32, LogicalType.Int64)

  private[scarlet] def byKey(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: LongBinOp,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else
      val combined = PartialCombine.perBatch(
        batches,
        batchSize,
        parallelism,
        morsel => reduce(morsel, op),
        partials => reduce(partials, op),
      )
      finish(combined, numPartitions, parallelism, op)

  private def finish(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      op: LongBinOp,
  ): Vector[ColumnBatch] =
    if (combined.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else
      val buckets = HashExchange.partitionAll(combined, keyOrdinal = 0, numPartitions, parallelism)
      MorselScheduler.map(buckets, parallelism)((_, bucket) => reduce(bucket, op))

  private def reduce(batch: ColumnBatch, op: LongBinOp): ColumnBatch =
    if (batch.length == 0) then emptyBatch()
    else
      val keys = ColumnHash.int32At(batch, 0)
      val values = ColumnHash.int64At(batch, 1)
      val table = new Table(ColumnHash.tableSize(keys.length), op)
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
        table.scanDense(keys, values)
      else table.scanSparse(keys, values)
      table.compact()

  private def emptyBatch(): ColumnBatch =
    val present = Validity.allValid(0)
    new ColumnBatch(
      Schema,
      Vector(
        Int32Column.of(Array.emptyIntArray, present, 0),
        Int64Column.of(Array.emptyLongArray, present, 0),
      ),
      0,
    )

  private final class Table(capacity: Int, op: LongBinOp):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Int] = new Array[Int](capacity)
    private val values: Array[Long] = new Array[Long](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def scanDense(keyColumn: Int32Column, valueColumn: Int64Column): Unit =
      val keyValues = keyColumn.values
      val valueValues = valueColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        add(keyValues(row), present = true, valueValues(row))
        row += 1
      }

    def scanSparse(keyColumn: Int32Column, valueColumn: Int64Column): Unit =
      val keyValues = keyColumn.values
      val valueValues = valueColumn.values
      val keyWords = keyColumn.validity.words
      val valueWords = valueColumn.validity.words
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          add(keyValues(row), Validity.isSet(valueWords, row), valueValues(row))
        row += 1
      }

    def compact(): ColumnBatch =
      val rows = occupied
      val outKeys = new Array[Int](rows)
      val outValues = new Array[Long](rows)
      val words = Validity.allocate(rows)
      var cursor = 0
      while (cursor < rows) {
        val slot = slots(cursor)
        outKeys(cursor) = keys(slot)
        if (state(slot) == 1) then
          outValues(cursor) = values(slot)
          Validity.setBit(words, cursor)
        cursor += 1
      }
      val keysPresent = Validity.allValid(rows)
      val valuesPresent = Validity.fromWords(words, rows)
      new ColumnBatch(
        Schema,
        Vector(
          Int32Column.of(outKeys, keysPresent, rows),
          Int64Column.of(outValues, valuesPresent, rows),
        ),
        rows,
      )

    /** `0` empty, `1` a present value, `2` a null that has not met another value. */
    private def add(key: Int, present: Boolean, value: Long): Unit =
      val slot = ensure(key)
      state(slot) match
        case 0 =>
          if (present) then
            state(slot) = 1
            values(slot) = value
          else state(slot) = 2
        case 1 =>
          val right = if (present) then value else 0L
          values(slot) = op(values(slot), right)
        case _ =>
          val right = if (present) then value else 0L
          values(slot) = op(0L, right)
          state(slot) = 1

    private def ensure(key: Int): Int =
      val slot = locate(key)
      if (state(slot) == 0) then
        if (occupied >= limit) then
          throw new IllegalStateException(s"int-long reduce table is full at $occupied groups")
        slots(occupied) = slot
        occupied += 1
        keys(slot) = key
      slot

    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(java.lang.Integer.hashCode(key)) & mask
      while (state(slot) != 0 && keys(slot) != key) slot = (slot + 1) & mask
      slot
