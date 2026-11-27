package com.ewoodbury.scarlet.columnar

/**
 * Count and sum of `Int64` values grouped by an `Int32` key.
 *
 * A null key is dropped. A null value increments the count and is not added. The sum of an
 * all-null group is `0` and valid. The count is stored as a `Long` and narrowed to `Int` at
 * decode, the same two's-complement wrap the row path uses. The sum is a `Long`.
 *
 * Partials are combined per input batch, exchanged with [[HashExchange.partitionAll]], then
 * merged. Empty input is `numPartitions` empty batches. There is no spill path. This is not
 * [[CountSum.byKeyOwned]]: that walk adds `Int` values, and this kernel reads published
 * `(Int32, Int64)` pairs.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object IntLongCountSum:

  private val Schema = Vector(LogicalType.Int32, LogicalType.Int64, LogicalType.Int64)

  /**
   * Partial-aggregate each morsel, merge those partials per input batch, exchange on the key, then
   * merge each bucket. An empty input is `numPartitions` empty count-sum batches.
   */
  private[scarlet] def byKey(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else
      val combined = PartialCombine.perBatch(batches, batchSize, parallelism, partial, merge)
      finish(combined, numPartitions, parallelism)

  private def finish(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    if (combined.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else
      val buckets = HashExchange.partitionAll(combined, keyOrdinal = 0, numPartitions, parallelism)
      MorselScheduler.map(buckets, parallelism)((_, bucket) => merge(bucket))

  /** One source batch of `(Int, Long)`. A null key is dropped. A null value is not added. */
  private def partial(batch: ColumnBatch): ColumnBatch =
    if (batch.length == 0) then emptyBatch()
    else
      val keys = ColumnHash.int32At(batch, 0)
      val values = ColumnHash.int64At(batch, 1)
      val table = new Table(ColumnHash.tableSize(keys.length))
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
        table.scanDense(keys, values)
      else table.scanSparse(keys, values)
      table.compact()

  /**
   * Merge `(key, count, sum)` partials. Every column of a partial this kernel published is
   * present.
   */
  private def merge(batch: ColumnBatch): ColumnBatch =
    if (batch.length == 0) then emptyBatch()
    else
      val keys = ColumnHash.int32At(batch, 0)
      val counts = ColumnHash.int64At(batch, 1)
      val sums = ColumnHash.int64At(batch, 2)
      val table = new Table(ColumnHash.tableSize(keys.length))
      table.scanPartials(keys, counts, sums)
      table.compact()

  private def emptyBatch(): ColumnBatch =
    val present = Validity.allValid(0)
    new ColumnBatch(
      Schema,
      Vector(
        Int32Column.of(Array.emptyIntArray, present, 0),
        Int64Column.of(Array.emptyLongArray, present, 0),
        Int64Column.of(Array.emptyLongArray, present, 0),
      ),
      0,
    )

  private final class Table(capacity: Int):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Int] = new Array[Int](capacity)
    private val counts: Array[Long] = new Array[Long](capacity)
    private val sums: Array[Long] = new Array[Long](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def scanDense(keyColumn: Int32Column, valueColumn: Int64Column): Unit =
      val keyValues = keyColumn.values
      val valueValues = valueColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        addPresent(keyValues(row), valueValues(row))
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
          if (Validity.isSet(valueWords, row)) then addPresent(keyValues(row), valueValues(row))
          else addMissing(keyValues(row))
        row += 1
      }

    def scanPartials(
        keyColumn: Int32Column,
        countColumn: Int64Column,
        sumColumn: Int64Column,
    ): Unit =
      val keyValues = keyColumn.values
      val countValues = countColumn.values
      val sumValues = sumColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        addPartial(keyValues(row), countValues(row), sumValues(row))
        row += 1
      }

    def compact(): ColumnBatch =
      val rows = occupied
      val outKeys = new Array[Int](rows)
      val outCounts = new Array[Long](rows)
      val outSums = new Array[Long](rows)
      var cursor = 0
      while (cursor < rows) {
        val slot = slots(cursor)
        outKeys(cursor) = keys(slot)
        outCounts(cursor) = counts(slot)
        outSums(cursor) = sums(slot)
        cursor += 1
      }
      val present = Validity.allValid(rows)
      new ColumnBatch(
        Schema,
        Vector(
          Int32Column.of(outKeys, present, rows),
          Int64Column.of(outCounts, present, rows),
          Int64Column.of(outSums, present, rows),
        ),
        rows,
      )

    private def addPresent(key: Int, value: Long): Unit =
      val slot = ensure(key)
      counts(slot) += 1L
      sums(slot) += value

    private def addMissing(key: Int): Unit =
      val slot = ensure(key)
      counts(slot) += 1L

    private def addPartial(key: Int, count: Long, sum: Long): Unit =
      val slot = ensure(key)
      counts(slot) += count
      sums(slot) += sum

    /** First sight of `key` stores a zero count and a zero sum. The caller then adds. */
    private def ensure(key: Int): Int =
      val slot = locate(key)
      if (state(slot) == 0) then
        if (occupied >= limit) then
          throw new IllegalStateException(s"int-long count-sum table is full at $occupied groups")
        state(slot) = 1
        keys(slot) = key
        slots(occupied) = slot
        occupied += 1
      slot

    /** Probe with `Integer.hashCode`. Two ints that share a hash code stay distinct. */
    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(java.lang.Integer.hashCode(key)) & mask
      while (state(slot) != 0 && keys(slot) != key) slot = (slot + 1) & mask
      slot
