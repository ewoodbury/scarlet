package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Hash reduce over dictionary string keys and `Int32` values.
 *
 * The binary operator is the caller's. It is applied as values are inserted, including when
 * partial groups are merged, so it has to be associative and commutative. Arithmetic wraps at
 * `Int`. This is the `(String, Int)` `reduceByKey` kernel. Spill is off unless
 * [[ScarletConf.spillEnabled]] is set. Only the post-exchange bucket spills; the partial combine
 * before the exchange stays in memory.
 *
 * A null key is dropped. `""` is a present key. One batch has one dictionary, so a table keyed by
 * dictionary code is keyed by the string. The exchange bucket is
 * `floorMod(StringHash.hash(text), numPartitions)`, the same rule `HashPartitioner` uses, and each
 * bucket is one dictionary: unlike dictionaries are remapped when the bucket is concatenated. The
 * value column is all valid. A clear value bit is not a null this kernel understands. Output order
 * is first-seen inside the bucket. The table is at least twice the row count, so one batch does
 * not rehash. A spilled bucket may change first-seen output order.
 *
 * [[reduceSelected]] scans a survivor index. [[byKeySelected]] partial-reduces that index, then
 * exchanges and spills the dense partials the same way [[byKey]] does. The exchange does not see
 * the source rows.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object StringHashReduce:

  /**
   * Partial-reduce each morsel, merge those partials per input batch, exchange on the string key,
   * then reduce each bucket. An empty input is `numPartitions` empty string-pair batches.
   *
   * Spill is off unless [[ScarletConf.spillEnabled]] is set. A bucket whose
   * [[StringHashReduceSpill.retainedBytes]] estimate is above [[ScarletConf.spillBudgetBytes]] is
   * sub-partitioned, written morsel by morsel, and folded with [[reduce]] on the concatenation of
   * the running partial and the new one. That path runs one bucket at a time, so the bucket's file
   * is deleted before the next bucket opens a store. A run whose buckets all fit stays on
   * [[MorselScheduler]], including when spill is enabled and none of them exceed the budget. Spill
   * off is the in-memory [[reduce]] result. A spilled bucket may change first-seen output order.
   */
  def byKey(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: (Int, Int) => Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyStringPairs)
    else
      val combined = PartialCombine.perBatch(
        batches,
        batchSize,
        parallelism,
        morsel => reduceBatch(morsel, op),
        partials => reduceBatch(partials, op),
      )
      reduceExchanged(combined, numPartitions, parallelism, batchSize, op)

  /**
   * [[byKey]] for a selection. Each live prefix is partial-reduced, the dense partials are
   * exchanged, and each bucket is reduced. Filtered rows die in the scan. An empty input is
   * `numPartitions` empty string-pair batches. Spill matches [[byKey]].
   */
  private[scarlet] def byKeySelected(
      selected: Vector[Selected],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: (Int, Int) => Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (selected.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyStringPairs)
    else
      val combined = PartialCombine.perBatchSelected(
        selected,
        batchSize,
        parallelism,
        (one, from, until) => reduceSelected(one, from, until, op),
        partials => reduce(partials, op),
      )
      reduceExchanged(combined, numPartitions, parallelism, batchSize, op)

  /**
   * [[reduce]] over `rows(from until until)`. A null key is dropped. `""` is a present key. A
   * clear value bit is not a null this kernel understands, matching [[reduce]]: the stored int is
   * combined. Slots at `count` and beyond are not read. An empty range publishes an empty
   * string-pair batch.
   */
  private[scarlet] def reduceSelected(
      selected: Selected,
      from: Int,
      until: Int,
      op: (Int, Int) => Int,
  ): ColumnBatch =
    require(from >= 0, s"selection start must be >= 0, got $from")
    require(until >= from, s"selection end $until is before start $from")
    require(
      until <= selected.count,
      s"selection end $until exceeds count ${selected.count}",
    )
    val keys = utf8At(selected.batch, 0)
    val values = ColumnHash.int32At(selected.batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    if (until == from) then ColumnBatches.emptyStringPairs
    else
      val table = new Table(ColumnHash.tableSize(until - from), op)
      if (ColumnHash.fullyPresent(keys)) then
        table.scanDenseSelected(keys, values, selected.rows, from, until)
      else table.scanSparseSelected(keys, values, selected.rows, from, until)
      table.compact(keys.dictionary)

  /** One batch. [[StringHashReduceSpill]] reduces a morsel and folds partials through this. */
  private[scarlet] def reduce(batch: ColumnBatch, op: (Int, Int) => Int): ColumnBatch =
    reduceBatch(batch, op)

  /**
   * Exchange combined partials and reduce each bucket. Shared by [[byKey]] and [[byKeySelected]].
   */
  private def reduceExchanged(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: (Int, Int) => Int,
  ): Vector[ColumnBatch] =
    val buckets =
      HashExchange.partitionAllUtf8(combined, keyOrdinal = 0, numPartitions, parallelism)
    val conf = ScarletConf.get
    if (buckets.exists(bucket => StringHashReduceSpill.exceeds(bucket, conf))) then
      buckets.map { bucket =>
        StringHashReduceSpill.finish(bucket, parallelism, batchSize, conf, op)
      }
    else
      MorselScheduler.map(buckets, parallelism) { (_, bucket) =>
        reduceBatch(bucket, op)
      }

  private def reduceBatch(batch: ColumnBatch, op: (Int, Int) => Int): ColumnBatch =
    val keys = utf8At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    if (keys.length == 0) then ColumnBatches.emptyStringPairs
    else reduce(keys, values, op)

  private def reduce(
      keys: DictUtf8Column,
      values: Int32Column,
      op: (Int, Int) => Int,
  ): ColumnBatch =
    val table = new Table(ColumnHash.tableSize(keys.length), op)
    if (ColumnHash.fullyPresent(keys)) then table.scanDense(keys, values)
    else table.scanSparse(keys, values)
    table.compact(keys.dictionary)

  private def utf8At(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private final class Table(capacity: Int, op: (Int, Int) => Int):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Int] = new Array[Int](capacity)
    private val values: Array[Int] = new Array[Int](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def scanDense(keyColumn: DictUtf8Column, valueColumn: Int32Column): Unit =
      val codes = keyColumn.codes
      val valueValues = valueColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        add(codes(row), valueValues(row))
        row += 1
      }

    def scanSparse(keyColumn: DictUtf8Column, valueColumn: Int32Column): Unit =
      val codes = keyColumn.codes
      val valueValues = valueColumn.values
      val keyWords = keyColumn.validity.words
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then add(codes(row), valueValues(row))
        row += 1
      }

    def scanDenseSelected(
        keyColumn: DictUtf8Column,
        valueColumn: Int32Column,
        ids: Array[Int],
        from: Int,
        until: Int,
    ): Unit =
      val codes = keyColumn.codes
      val valueValues = valueColumn.values
      var slot = from
      while (slot < until) {
        val row = ids(slot)
        add(codes(row), valueValues(row))
        slot += 1
      }

    def scanSparseSelected(
        keyColumn: DictUtf8Column,
        valueColumn: Int32Column,
        ids: Array[Int],
        from: Int,
        until: Int,
    ): Unit =
      val codes = keyColumn.codes
      val valueValues = valueColumn.values
      val keyWords = keyColumn.validity.words
      var slot = from
      while (slot < until) {
        val row = ids(slot)
        if (Validity.isSet(keyWords, row)) then add(codes(row), valueValues(row))
        slot += 1
      }

    /**
     * Surviving codes keep `dictionary`. One batch has one dictionary, so a code is one string.
     */
    def compact(dictionary: Array[String]): ColumnBatch =
      val rows = occupied
      if (rows == 0) then ColumnBatches.emptyStringPairs
      else
        val outCodes = new Array[Int](rows)
        val outValues = new Array[Int](rows)
        var cursor = 0
        while (cursor < rows) {
          val slot = slots(cursor)
          outCodes(cursor) = keys(slot)
          outValues(cursor) = values(slot)
          cursor += 1
        }
        val present = Validity.allValid(rows)
        new ColumnBatch(
          Vector(LogicalType.Utf8Dict, LogicalType.Int32),
          Vector(
            DictUtf8Column.of(dictionary, outCodes, present, rows),
            Int32Column.of(outValues, present, rows),
          ),
          rows,
        )

    private def add(key: Int, value: Int): Unit =
      val slot = locate(key)
      if (state(slot) == 0) then
        if (occupied >= limit) then
          throw new IllegalStateException(s"hash reduce table is full at $occupied groups")
        state(slot) = 1
        keys(slot) = key
        values(slot) = value
        slots(occupied) = slot
        occupied += 1
      else values(slot) = op(values(slot), value)

    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      while (state(slot) != 0 && keys(slot) != key) slot = (slot + 1) & mask
      slot
