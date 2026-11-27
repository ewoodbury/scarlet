package com.ewoodbury.scarlet.columnar

/**
 * Count and sum of `Int32` values grouped by a dictionary string key.
 *
 * A null key is a clear validity bit and is not inserted. `""` is a present key. A dictionary code
 * of `0` is not a null. A null value increments the count and is not added. The sum of an all-null
 * group is `0` and valid. The count is stored as a `Long` and narrowed to `Int` at decode, the
 * same two's-complement wrap the row path uses. The sum is a `Long`.
 *
 * One batch has one dictionary, so a code compared in the table is one string. Codes from two
 * dictionaries are not comparable. [[HashExchange.partitionAllUtf8]] remaps them, and a merge runs
 * only on that concatenated batch.
 *
 * Partials are combined per input batch, exchanged on the key, then merged. Empty input is
 * `numPartitions` empty batches. There is no spill path.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object StringCountSum:

  private val Schema = Vector(LogicalType.Utf8Dict, LogicalType.Int64, LogicalType.Int64)

  /**
   * `(key, count, sum)` rows. `count` is the low 32 bits of the stored `Long`. `maxRows` caps the
   * prefix. A zero cap or an empty batch returns an empty sequence. Keys in a published batch are
   * present.
   */
  private[scarlet] def rows(batch: ColumnBatch, maxRows: Int): Seq[(String, Int, Long)] =
    val live = math.min(math.max(maxRows, 0), batch.length)
    if (live == 0) then Seq.empty
    else
      val keys = utf8At(batch, 0)
      val counts = ColumnHash.int64At(batch, 1)
      val sums = ColumnHash.int64At(batch, 2)
      ColumnMetrics.decoded(live)
      Seq.tabulate(live) { row =>
        (keys.dictionary(keys.codes(row)), counts.values(row).toInt, sums.values(row))
      }

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
      val buckets =
        HashExchange.partitionAllUtf8(combined, keyOrdinal = 0, numPartitions, parallelism)
      MorselScheduler.map(buckets, parallelism)((_, bucket) => merge(bucket))

  /** One source batch of `(String, Int)`. A null key is dropped. A null value is not added. */
  private def partial(batch: ColumnBatch): ColumnBatch =
    if (batch.length == 0) then emptyBatch()
    else
      val keys = utf8At(batch, 0)
      val values = ColumnHash.int32At(batch, 1)
      val table = new Table(ColumnHash.tableSize(keys.length))
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
        table.scanDense(keys, values)
      else table.scanSparse(keys, values)
      table.compact(keys.dictionary)

  /** Merge `(key, count, sum)` partials. The batch is one dictionary. */
  private def merge(batch: ColumnBatch): ColumnBatch =
    if (batch.length == 0) then emptyBatch()
    else
      val keys = utf8At(batch, 0)
      val counts = ColumnHash.int64At(batch, 1)
      val sums = ColumnHash.int64At(batch, 2)
      val table = new Table(ColumnHash.tableSize(keys.length))
      table.scanPartials(keys, counts, sums)
      table.compact(keys.dictionary)

  private def utf8At(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def emptyBatch(): ColumnBatch =
    val present = Validity.allValid(0)
    new ColumnBatch(
      Schema,
      Vector(
        DictUtf8Column.of(Array.empty[String], Array.emptyIntArray, present, 0),
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

    def scanDense(keyColumn: DictUtf8Column, valueColumn: Int32Column): Unit =
      val codes = keyColumn.codes
      val valueValues = valueColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        addPresent(codes(row), valueValues(row).toLong)
        row += 1
      }

    def scanSparse(keyColumn: DictUtf8Column, valueColumn: Int32Column): Unit =
      val codes = keyColumn.codes
      val valueValues = valueColumn.values
      val keyWords = keyColumn.validity.words
      val valueWords = valueColumn.validity.words
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then
          if (Validity.isSet(valueWords, row)) then addPresent(codes(row), valueValues(row).toLong)
          else addMissing(codes(row))
        row += 1
      }

    def scanPartials(
        keyColumn: DictUtf8Column,
        countColumn: Int64Column,
        sumColumn: Int64Column,
    ): Unit =
      val codes = keyColumn.codes
      val countValues = countColumn.values
      val sumValues = sumColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        addPartial(codes(row), countValues(row), sumValues(row))
        row += 1
      }

    def compact(dictionary: Array[String]): ColumnBatch =
      val rows = occupied
      val outCodes = new Array[Int](rows)
      val outCounts = new Array[Long](rows)
      val outSums = new Array[Long](rows)
      var cursor = 0
      while (cursor < rows) {
        val slot = slots(cursor)
        outCodes(cursor) = keys(slot)
        outCounts(cursor) = counts(slot)
        outSums(cursor) = sums(slot)
        cursor += 1
      }
      val present = Validity.allValid(rows)
      new ColumnBatch(
        Schema,
        Vector(
          DictUtf8Column.of(dictionary, outCodes, present, rows),
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
          throw new IllegalStateException(s"string count-sum table is full at $occupied groups")
        state(slot) = 1
        keys(slot) = key
        slots(occupied) = slot
        occupied += 1
      slot

    /** Probe with the mixed code. Two strings that share a hash stay distinct codes. */
    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      while (state(slot) != 0 && keys(slot) != key) slot = (slot + 1) & mask
      slot
