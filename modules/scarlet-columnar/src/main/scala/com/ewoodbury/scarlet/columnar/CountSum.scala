package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

/**
 * Count and sum of `Int32` values grouped by an `Int32` key.
 *
 * Reuses [[HashAggregate.GroupTable]] with [[HashAggregate.Int32Aggs]] sum and count. The table
 * starts at [[HashAggregate.SumTableStart]] and grows with the distinct keys. It is not sized from
 * `2 * rows`. A null key is dropped. A null value increments the count and is not added; the sum
 * of an all-null group is `0` and valid. That is not the plain [[HashAggregate.aggregateInt32]]
 * rule, which leaves the count at `0` and the sum null.
 *
 * Output columns are the aggregate's key, count, and sum, reordered. `compact` emits key, then
 * sum, then count. This publishes key, then count, then sum and shares those arrays. The count
 * stored in the table is a `Long`. Callers narrow it to `Int` at decode, which is the same
 * two's-complement wrap the row path uses.
 *
 * Partials are combined per input batch, exchanged on the key, then merged. Empty input is
 * `numPartitions` empty batches of this schema. There is no spill path: a partial is
 * `O(distinct keys)`.
 *
 * [[byKeyOwned]] is that aggregate over fully present arrays. The stripe walk aliases those
 * arrays, applies an int slot stack inside the scan, and observes into the thread-local table. It
 * does not copy either column and it does not build a survivor index. A null key or a null value
 * never reaches that walk. The encoded scans below still drop a null key and count a null value
 * without adding it.
 *
 * [[byKeyOwnedMapped]] is [[byKeyOwned]] after an int key function. [[byKeyMapped]] is the same
 * aggregate over encoded morsels: a null key is not passed to the function, and a null value is
 * counted and not added. [[byKeyOwnedMappedAbove]] and [[byKeyMappedAbove]] apply one int slot
 * after that function. A null input key is not passed to the function or to a key slot. A value
 * slot still sees that row. A null mapped key is dropped. A null value that the slot keeps is
 * counted and not added. [[suppliedPartial]] is that aggregate for pairs a caller projects. A null
 * projected key is the caller's to drop. A null projected value is counted and not added. None of
 * these call [[byKey]], so a published pair batch is not the input.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object CountSum:

  private val Aggs =
    HashAggregate.Int32Aggs(sum = true, count = true, min = false, max = false)

  private val byKeyCalls = new AtomicInteger

  private val Schema = Vector(LogicalType.Int32, LogicalType.Int64, LogicalType.Int64)

  /**
   * `(key, count, sum)` rows. `count` is the low 32 bits of the table's `Long`. `maxRows` caps the
   * prefix. A zero cap or an empty batch returns an empty sequence.
   */
  private[scarlet] def rows(batch: ColumnBatch, maxRows: Int): Seq[(Int, Int, Long)] =
    val live = math.min(math.max(maxRows, 0), batch.length)
    if (live == 0) then Seq.empty
    else
      val keys = ColumnHash.int32At(batch, 0)
      val counts = ColumnHash.int64At(batch, 1)
      val sums = ColumnHash.int64At(batch, 2)
      ColumnMetrics.decoded(live)
      Seq.tabulate(live) { row =>
        (keys.values(row), counts.values(row).toInt, sums.values(row))
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
    val _ = byKeyCalls.incrementAndGet()
    if (batches.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else
      val combined = PartialCombine.perBatch(
        batches,
        batchSize,
        parallelism,
        countSumPairs,
        merge,
      )
      finish(combined, numPartitions, parallelism)

  /**
   * [[byKey]] for a survivor index. Filtered rows die in the scan. An empty input is
   * `numPartitions` empty count-sum batches.
   */
  private[scarlet] def byKeySelected(
      selected: Vector[Selected],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (selected.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else
      val combined = PartialCombine.perBatchSelected(
        selected,
        batchSize,
        parallelism,
        (one, from, until) => countSumSelected(one, from, until),
        merge,
      )
      finish(combined, numPartitions, parallelism)

  /**
   * [[byKey]] for fully present owned arrays.
   *
   * `steps` may be empty, which observes every row. A non-empty stack is int key and value slots
   * in source order. One worker stripe folds into one partial, so a repeated key updates the
   * thread-local table instead of being concatenated across morsels. [[HashExchange.partitionAll]]
   * still concatenates each shuffle bucket. `batchSize` is the planner morsel width. The stripe is
   * not cut into those morsels: the table grows with distinct keys, not with the morsel.
   */
  private[scarlet] def byKeyOwned(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val partials = OwnedCountSum.fold(parts, steps, parallelism, () => open(), publish)
    if (partials.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else finish(partials, numPartitions, parallelism)

  /**
   * [[byKeyOwned]] after an int key function. Filters see the source key. `mapKey` runs once on
   * each survivor and is not inspected. This does not call [[byKey]].
   */
  private[scarlet] def byKeyOwnedMapped(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      mapKey: Int => Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val partials =
      OwnedMappedCountSum.fold(parts, steps, mapKey, parallelism, () => open(), publish)
    if (partials.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else finish(partials, numPartitions, parallelism)

  /**
   * [[byKeyOwned]] after an int-pair project. Filters see the source pair. `mapper` runs once on
   * each survivor and is not inspected. A null projected key is dropped. A null projected value is
   * counted and not added. This does not call [[byKey]] and does not publish the pair batch.
   */
  private[scarlet] def byKeyOwnedProjected(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      mapper: IntPairProject.IntPairMap,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val partials =
      OwnedProjectedCountSum.fold(parts, steps, mapper, parallelism, () => open(), publish)
    if (partials.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else finish(partials, numPartitions, parallelism)

  /**
   * [[byKeyOwnedProjected]] plus one int slot on the projected pair. Filters in `steps` see the
   * source pair. `above` sees the projected key or value and is not inspected. A null projected
   * key is dropped. A null projected value that the slot keeps is counted and not added. This does
   * not call [[byKey]] and does not publish the pair batch.
   */
  private[scarlet] def byKeyOwnedAbove(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      mapper: IntPairProject.IntPairMap,
      above: Selection.Step,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val (column, keep) = ProjectedAboveCountSum.slot(above)
    val partials = OwnedProjectedCountSum.foldAbove(
      parts,
      steps,
      mapper,
      column,
      keep,
      parallelism,
      () => open(),
      publish,
    )
    if (partials.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else finish(partials, numPartitions, parallelism)

  /**
   * [[byKey]] for encoded morsels under an int key function. `steps` may be empty. A null key is
   * dropped and `mapKey` is not called. A null value is counted and not added. This does not call
   * [[byKey]]: the morsel is the source, not a published map.
   */
  private[scarlet] def byKeyMapped(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      mapKey: Int => Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else
      val slots = MappedCountSum.prepare(steps)
      val combined = PartialCombine.perBatch(
        batches,
        batchSize,
        parallelism,
        batch => mappedPartial(batch, slots, mapKey),
        merge,
      )
      finish(combined, numPartitions, parallelism)

  /**
   * [[byKeyOwnedMapped]] plus one int slot after `mapKey`. Filters in `steps` see the source pair.
   * `above` sees the mapped key or the original value and is not inspected. Every cell is present.
   * This does not call [[byKey]] and does not publish the pair batch.
   */
  private[scarlet] def byKeyOwnedMappedAbove(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      mapKey: Int => Int,
      above: Selection.Step,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val (column, keep) = MappedAboveCountSum.slot(above)
    val partials = OwnedMappedAboveCountSum.fold(
      parts,
      steps,
      mapKey,
      column,
      keep,
      parallelism,
      () => open(),
      publish,
    )
    if (partials.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else finish(partials, numPartitions, parallelism)

  /**
   * [[byKeyMapped]] plus one int slot after `mapKey`. A null input key is not passed to `mapKey`
   * or to a key slot. A value slot still sees that row. A null mapped key is dropped. A null value
   * that the slot keeps is counted and not added. This does not call [[byKey]].
   */
  private[scarlet] def byKeyMappedAbove(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      mapKey: Int => Int,
      above: Selection.Step,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else
      val combined = MappedAboveCountSum.combine(
        batches,
        steps,
        mapKey,
        above,
        parallelism,
        batchSize,
        () => open(),
        publish,
      )
      finish(combined, numPartitions, parallelism)

  /**
   * One partial from rows the caller supplies. No pair column is read.
   *
   * `rows` of zero publishes an empty count-sum batch and does not call `supply`. `supply`
   * receives a sink and calls [[IntPairProject.IntPairCountSink.observe]] or
   * [[IntPairProject.IntPairCountSink.countNull]] once per projected pair. A null key is the
   * caller's to skip. This is not [[byKey]]: nothing here is a published pair project.
   */
  private[scarlet] def suppliedPartial(
      rows: Int,
  )(supply: IntPairProject.IntPairCountSink => Unit): ColumnBatch =
    require(rows >= 0, s"row estimate must be >= 0, got $rows")
    if (rows == 0) then emptyBatch()
    else
      val table = open()
      val sink = new IntPairProject.IntPairCountSink:
        def observe(key: Int, value: Int): Unit =
          table.observe(table.ensure(key), value)
        def countNull(key: Int): Unit =
          table.countNull(table.ensure(key))
      supply(sink)
      publish(table)

  /** [[merge]] for a projected count-sum. [[byKey]] is not called. */
  private[scarlet] def mergeCountSums(batch: ColumnBatch): ColumnBatch = merge(batch)

  /** [[finish]] for a projected count-sum. [[byKey]] is not called. */
  private[scarlet] def exchangeCountSums(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (combined.isEmpty) then Vector.fill(numPartitions)(emptyBatch())
    else finish(combined, numPartitions, parallelism)

  /** `numPartitions` empty count-sum batches. [[byKey]] is not called. */
  private[scarlet] def emptyCountSums(numPartitions: Int): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    Vector.fill(numPartitions)(emptyBatch())

  /** [[byKey]] entries since the last [[resetByKeyForTest]]. */
  private[scarlet] def byKeyCallsForTest: Int = byKeyCalls.get

  /** Clears [[byKeyCallsForTest]]. */
  private[scarlet] def resetByKeyForTest(): Unit =
    byKeyCalls.set(0)

  private def mappedPartial(
      batch: ColumnBatch,
      slots: Array[MappedCountSum.Slot],
      mapKey: Int => Int,
  ): ColumnBatch =
    if (batch.length == 0) then emptyBatch()
    else
      val table = open()
      MappedCountSum.scan(batch, slots, mapKey, table)
      publish(table)

  private def finish(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    val buckets = HashExchange.partitionAll(combined, keyOrdinal = 0, numPartitions, parallelism)
    MorselScheduler.map(buckets, parallelism)((_, bucket) => merge(bucket))

  private def countSumPairs(batch: ColumnBatch): ColumnBatch =
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    if (keys.length == 0) then emptyBatch()
    else
      val table = open()
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
        scanDense(table, keys, values)
      else scanSparse(table, keys, values)
      publish(table)

  private def countSumSelected(selected: Selected, from: Int, until: Int): ColumnBatch =
    require(from >= 0, s"selection start must be >= 0, got $from")
    require(until >= from, s"selection end $until is before start $from")
    require(until <= selected.count, s"selection end $until exceeds count ${selected.count}")
    if (until == from) then emptyBatch()
    else
      val keys = ColumnHash.int32At(selected.batch, 0)
      val values = ColumnHash.int32At(selected.batch, 1)
      val table = open()
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
        scanDenseSelected(table, keys, values, selected.rows, from, until)
      else scanSparseSelected(table, keys, values, selected.rows, from, until)
      publish(table)

  /** One partial count and sum per row. The batch is already `(key, count, sum)`. */
  private def merge(batch: ColumnBatch): ColumnBatch =
    if (batch.length == 0) then emptyBatch()
    else
      val keys = ColumnHash.int32At(batch, 0)
      val counts = ColumnHash.int64At(batch, 1)
      val sums = ColumnHash.int64At(batch, 2)
      val table = open()
      val keyValues = keys.values
      val countValues = counts.values
      val sumValues = sums.values
      val rows = keys.length
      var row = 0
      while (row < rows) {
        table.addCountSum(keyValues(row), countValues(row), sumValues(row))
        row += 1
      }
      publish(table)

  private def scanDense(
      table: HashAggregate.GroupTable,
      keys: Int32Column,
      values: Int32Column,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val rows = keys.length
    var row = 0
    while (row < rows) {
      table.observe(table.ensure(keyValues(row)), valueValues(row))
      row += 1
    }

  private def scanSparse(
      table: HashAggregate.GroupTable,
      keys: Int32Column,
      values: Int32Column,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    val rows = keys.length
    var row = 0
    while (row < rows) {
      if (Validity.isSet(keyWords, row)) then
        val slot = table.ensure(keyValues(row))
        if (Validity.isSet(valueWords, row)) then table.observe(slot, valueValues(row))
        else table.countNull(slot)
      row += 1
    }

  private def scanDenseSelected(
      table: HashAggregate.GroupTable,
      keys: Int32Column,
      values: Int32Column,
      ids: Array[Int],
      from: Int,
      until: Int,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    var slot = from
    while (slot < until) {
      val row = ids(slot)
      table.observe(table.ensure(keyValues(row)), valueValues(row))
      slot += 1
    }

  private def scanSparseSelected(
      table: HashAggregate.GroupTable,
      keys: Int32Column,
      values: Int32Column,
      ids: Array[Int],
      from: Int,
      until: Int,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    var index = from
    while (index < until) {
      val row = ids(index)
      if (Validity.isSet(keyWords, row)) then
        val slot = table.ensure(keyValues(row))
        if (Validity.isSet(valueWords, row)) then table.observe(slot, valueValues(row))
        else table.countNull(slot)
      index += 1
    }

  /**
   * One table per worker. The first morsel grows it from [[HashAggregate.SumTableStart]] as keys
   * arrive. Later morsels on that thread reuse the grown arrays. `release` clears the previous
   * groups before the next fill.
   */
  private val tables: ThreadLocal[HashAggregate.GroupTable] =
    new ThreadLocal[HashAggregate.GroupTable]:
      override def initialValue(): HashAggregate.GroupTable =
        new HashAggregate.GroupTable(HashAggregate.SumTableStart, Aggs)

  private def open(): HashAggregate.GroupTable =
    val table = tables.get()
    table.release()
    table

  /** [[HashAggregate.GroupTable.compact]] is key, sum, count. Publish key, count, sum. */
  private def publish(table: HashAggregate.GroupTable): ColumnBatch =
    val packed = table.compact()
    new ColumnBatch(
      Schema,
      Vector(
        ColumnKernel.columnAt(packed, 0),
        ColumnKernel.columnAt(packed, 2),
        ColumnKernel.columnAt(packed, 1),
      ),
      packed.length,
    )

  private def emptyBatch(): ColumnBatch =
    new ColumnBatch(
      Schema,
      Vector(
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
        Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0),
        Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0),
      ),
      0,
    )
