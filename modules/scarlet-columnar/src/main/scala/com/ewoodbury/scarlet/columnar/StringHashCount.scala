package com.ewoodbury.scarlet.columnar

import java.nio.charset.StandardCharsets

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Counts `(String, _)` rows by dictionary code.
 *
 * A null key is a clear validity bit and is not inserted. `""` is a present key. A dictionary code
 * of `0` is not a null. The value column is never read, so a null int still counts. Each present
 * key contributes one. The stored count is an `Int` and wraps on overflow, the same
 * two's-complement wrap [[HashCount]] uses when it adds. The count is not widened to `Long`.
 *
 * One batch has one dictionary, so a code compared in the table is one string.
 * [[StringHashReduce]] mixes that code and compares the stored code. This kernel does the same.
 * Codes from two dictionaries are not comparable. [[ColumnBatches.concat]] remaps them, and a
 * merge runs only on that concatenated batch.
 *
 * [[byKey]] counts dense batches. [[byKeySelected]] counts a survivor index. Both partial-count
 * with [[PartialCombine]] and then exchange the `(String, Int)` partials with
 * [[HashExchange.partitionAllUtf8]]. The exchange does not see the source rows. The partial
 * combine before the exchange stays in memory.
 *
 * Spill is off unless [[ScarletConf.spillEnabled]] is set, and that flag defaults off. While it is
 * off, the input-row estimate is compared with [[ScarletConf.spillBudgetBytes]] before any table,
 * partial combine, or store. [[refuse]] throws [[ClosedKernelRefusal]] when the estimate is over
 * the budget, and the action fails instead of rerunning on rows. While spill is on, that
 * whole-input check is skipped when a one-row table with one dictionary entry and no UTF-8 bytes
 * fits. A budget that cannot hold that one-row table still refuses the input row count: every
 * morsel would refuse, and the reported count is the input rather than whichever morsel failed.
 * [[utf8]] and [[utf8Selected]] still refuse a morsel whose estimate is over the budget. After the
 * exchange, a bucket at or under the budget is added in memory. A bucket over the budget is
 * finished by [[StringHashCountSpill]]. A table that still does not fit throws
 * [[ClosedKernelRefusal]] before it is allocated. The running partial is `O(distinct keys)`.
 * [[inputRowBytes]] charges every row about to be inserted as its own key, which overestimates
 * when partials share a key. That overestimate is intentional. It charges dictionary bytes.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object StringHashCount:

  /**
   * Count one batch. A null key is skipped. Column 1 is not read. An empty batch publishes an
   * empty string-pair batch and does not allocate a table. A non-empty batch whose row estimate is
   * over the budget throws [[ClosedKernelRefusal]] before the table is allocated.
   */
  private[scarlet] def utf8(batch: ColumnBatch): ColumnBatch =
    requireKey(batch)
    val keys = utf8At(batch, 0)
    if (keys.length == 0) then ColumnBatches.emptyStringPairs
    else
      refuseColumn(keys.length.toLong, keys)
      countKeys(keys)

  /**
   * [[utf8]] over `rows(from until until)`. A null key in that prefix is skipped. Slots at `count`
   * and beyond are not read. An empty range publishes an empty string-pair batch. A non-empty
   * range whose row estimate is over the budget throws [[ClosedKernelRefusal]] before the table is
   * allocated. The dictionary charged is the batch dictionary, including an entry the range does
   * not use.
   */
  private[scarlet] def utf8Selected(selected: Selected, from: Int, until: Int): ColumnBatch =
    require(from >= 0, s"selection start must be >= 0, got $from")
    require(until >= from, s"selection end $until is before start $from")
    require(until <= selected.count, s"selection end $until exceeds count ${selected.count}")
    requireKey(selected.batch)
    val keys = utf8At(selected.batch, 0)
    if (until == from) then ColumnBatches.emptyStringPairs
    else
      refuseColumn((until - from).toLong, keys)
      val table = new Table(ColumnHash.tableSize(until - from))
      if (ColumnHash.fullyPresent(keys)) then
        table.scanDenseSelected(keys, selected.rows, from, until)
      else table.scanSparseSelected(keys, selected.rows, from, until)
      table.compact(keys.dictionary)

  /**
   * Partial-count each morsel, merge those partials per input batch, exchange on the key, then add
   * the partial counts in each bucket. An empty input is `numPartitions` fresh empty string-pair
   * batches.
   *
   * The source value column is dropped before the morsel split, so the split does not copy it and
   * a null int is not unboxed. Spill off refuses the input-row estimate before that split. Spill
   * on skips that refusal when a one-row table fits, spills an over-budget bucket after the
   * exchange, and still refuses a morsel or a fold that does not fit. The whole-input estimate is
   * one table for the total row count plus the summed dictionary entries and UTF-8 bytes. It is
   * not the sum of per-batch table charges.
   */
  def byKey(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyStringPairs)
    else
      batches.foreach(requireKey)
      val rows = batches.iterator.map(_.length.toLong).sum
      if (refuseBeforeCombine) then refuseTotals(rows, batches)
      val keysOnly = batches.map(batch => ColumnBatches.takeColumn(batch, 0))
      val combined = PartialCombine.perBatch(keysOnly, batchSize, parallelism, utf8, addCounts)
      finish(combined, numPartitions, parallelism, batchSize)

  /**
   * [[byKey]] for a selection. Each live prefix is partial-counted, the dense partials are
   * exchanged, and each bucket adds those counts. Filtered rows die in the scan. An empty input is
   * `numPartitions` fresh empty string-pair batches. The budget check charges survivor rows, not
   * the rows the filter dropped, and each input dictionary, including an entry no survivor uses.
   * Spill matches [[byKey]]. The value column is dropped before the scan.
   */
  def byKeySelected(
      selected: Vector[Selected],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (selected.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyStringPairs)
    else
      selected.foreach(one => requireKey(one.batch))
      val rows = selected.iterator.map(_.count.toLong).sum
      if (refuseBeforeCombine) then refuseTotals(rows, selected.map(_.batch))
      val keysOnly = selected.map { one =>
        Selected(ColumnBatches.takeColumn(one.batch, 0), one.rows, one.count)
      }
      val combined = PartialCombine.perBatchSelected(
        keysOnly,
        batchSize,
        parallelism,
        (one, from, until) => utf8Selected(one, from, until),
        addCounts,
      )
      finish(combined, numPartitions, parallelism, batchSize)

  /**
   * Estimated bytes of a count table for `rows` input keys.
   *
   * Every input row is charged as its own key: a 4-byte dictionary code plus one validity byte.
   * The source value column is not included. The dictionary term is `dictionaryEntries * 8` for
   * the reference array plus `dictionaryBytes` for the UTF-8 payload, including an entry no row
   * uses. A null key is an empty slot, not a dictionary entry. The open-addressed table is charged
   * at [[ColumnHash.tableSize]] of the capped row count. Each slot is a 4-byte code, an `Int`
   * count, an occupancy byte, and a slot index. Zero rows publish an empty batch and allocate
   * nothing, so the estimate is zero and the dictionary is not added.
   *
   * A whole input passes the total row count and the sums of dictionary entries and UTF-8 bytes
   * across its batches. That is one table, not the sum of per-batch table charges. A morsel and a
   * post-exchange bucket pass their own row count and their own dictionary.
   */
  private[columnar] def inputRowBytes(
      rows: Long,
      dictionaryEntries: Long,
      dictionaryBytes: Long,
  ): Long =
    if (rows <= 0L) then 0L
    else
      val capped = if (rows > Int.MaxValue.toLong) Int.MaxValue else rows.toInt
      val slots = ColumnHash.tableSize(capped).toLong
      val rowBytes = rows * (4L + 1L)
      val dictionaryRefs = dictionaryEntries * 8L
      val slotBytes = slots * (4L + 4L + 1L + 4L)
      rowBytes + dictionaryRefs + dictionaryBytes + slotBytes

  /** Number of dictionary entries in the key column, including entries no row uses. */
  private[columnar] def dictionaryEntries(batch: ColumnBatch): Int =
    utf8At(batch, 0).dictionary.length

  /**
   * UTF-8 payload of every dictionary entry in the key column. A null key is an empty slot, not a
   * dictionary entry. Measured the same way [[StringHashReduceSpill.dictionaryBytes]] measures it.
   */
  private[columnar] def dictionaryBytes(batch: ColumnBatch): Long =
    utf8Payload(utf8At(batch, 0).dictionary)

  private def finish(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    if (combined.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyStringPairs)
    else
      val buckets =
        HashExchange.partitionAllUtf8(combined, keyOrdinal = 0, numPartitions, parallelism)
      val conf = ScarletConf.get
      if (buckets.exists(bucket => StringHashCountSpill.exceeds(bucket, conf))) then
        buckets.map(bucket => StringHashCountSpill.finish(bucket, parallelism, batchSize, conf))
      else MorselScheduler.map(buckets, parallelism)((_, bucket) => addCounts(bucket))

  /**
   * Add `(key, count)` partials. This reads the count this operator stored, not the source value.
   * The batch is one dictionary: callers concatenate unlike dictionaries first. A non-empty batch
   * is refused before the table is allocated. [[StringHashCountSpill]] folds with this kernel, so
   * a partial that does not fit throws [[ClosedKernelRefusal]] and does not allocate.
   */
  private[scarlet] def addCounts(batch: ColumnBatch): ColumnBatch =
    if (batch.length == 0) then ColumnBatches.emptyStringPairs
    else
      val keys = utf8At(batch, 0)
      refuseColumn(batch.length.toLong, keys)
      val weights = ColumnHash.int32At(batch, 1)
      require(keys.length == weights.length, "key and count columns differ in length")
      val table = new Table(ColumnHash.tableSize(keys.length))
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(weights)) then
        table.scanWeights(keys, weights)
      else table.scanWeightsSparse(keys, weights)
      table.compact(keys.dictionary)

  private def countKeys(keys: DictUtf8Column): ColumnBatch =
    val table = new Table(ColumnHash.tableSize(keys.length))
    if (ColumnHash.fullyPresent(keys)) then table.scanDense(keys)
    else table.scanSparse(keys)
    table.compact(keys.dictionary)

  private def requirePositive(numPartitions: Int, parallelism: Int, batchSize: Int): Unit =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")

  private def requireKey(batch: ColumnBatch): Unit =
    val keyType = batch.schema.headOption.getOrElse {
      throw new ColumnarFallback("countByKey expects a Utf8Dict key column")
    }
    keyType match
      case LogicalType.Utf8Dict => ()
      case LogicalType.Int32 | LogicalType.Int64 | LogicalType.Float64 | LogicalType.Bool =>
        throw new ColumnarFallback(
          s"countByKey expects a Utf8Dict key column, found $keyType",
        )

  /**
   * Whether the whole input is refused before a partial combine.
   *
   * Spill off always checks. Spill on checks only when a one-row table with one dictionary entry
   * and no UTF-8 text does not fit in the budget. Every morsel would refuse in that case, and a
   * parallel morsel failure would report whichever morsel lost the race. The input count is what
   * the action already publishes for that budget. A budget that can hold one row skips this check.
   * Morsels still refuse themselves, and an over-budget bucket spills after the exchange.
   */
  private def refuseBeforeCombine: Boolean =
    val conf = ScarletConf.get
    !conf.spillEnabled || inputRowBytes(1L, 1L, 0L) > conf.spillBudgetBytes

  /** One table for `rows`, plus the dictionaries of `batches` summed rather than unioned. */
  private def refuseTotals(rows: Long, batches: Iterable[ColumnBatch]): Unit =
    val entries = batches.iterator.map(batch => dictionaryEntries(batch).toLong).sum
    val text = batches.iterator.map(dictionaryBytes).sum
    refuse(rows, entries, text)

  /** Refuse one morsel or bucket from the dictionary it actually carries. */
  private def refuseColumn(rows: Long, keys: DictUtf8Column): Unit =
    refuse(rows, keys.dictionary.length.toLong, utf8Payload(keys.dictionary))

  /**
   * Refuse before a table exists. This does not read [[ScarletConf.spillEnabled]]. The throw is
   * not a [[ColumnarFallback]], so the action fails instead of rerunning on rows. Callers skip the
   * whole-input call when spill is on and a one-row table fits. A morsel, a partial combine, and a
   * spill fold still call this. `rows` is the count the refusal reports.
   */
  private def refuse(rows: Long, entries: Long, text: Long): Unit =
    val budget = ScarletConf.get.spillBudgetBytes
    val bytes = inputRowBytes(rows, entries, text)
    if (bytes > budget) then throw new ClosedKernelRefusal(rows, bytes, budget)

  private def utf8Payload(dictionary: Array[String]): Long =
    dictionary.iterator
      .map(entry => entry.getBytes(StandardCharsets.UTF_8).length.toLong)
      .sum

  private def utf8At(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private final class Table(capacity: Int):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Int] = new Array[Int](capacity)
    private val counts: Array[Int] = new Array[Int](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def scanDense(keyColumn: DictUtf8Column): Unit =
      val codes = keyColumn.codes
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        add(codes(row), 1)
        row += 1
      }

    def scanSparse(keyColumn: DictUtf8Column): Unit =
      val codes = keyColumn.codes
      val keyWords = keyColumn.validity.words
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then add(codes(row), 1)
        row += 1
      }

    def scanDenseSelected(
        keyColumn: DictUtf8Column,
        ids: Array[Int],
        from: Int,
        until: Int,
    ): Unit =
      val codes = keyColumn.codes
      var slot = from
      while (slot < until) {
        add(codes(ids(slot)), 1)
        slot += 1
      }

    def scanSparseSelected(
        keyColumn: DictUtf8Column,
        ids: Array[Int],
        from: Int,
        until: Int,
    ): Unit =
      val codes = keyColumn.codes
      val keyWords = keyColumn.validity.words
      var slot = from
      while (slot < until) {
        val row = ids(slot)
        if (Validity.isSet(keyWords, row)) then add(codes(row), 1)
        slot += 1
      }

    def scanWeights(keyColumn: DictUtf8Column, weightColumn: Int32Column): Unit =
      val codes = keyColumn.codes
      val weightValues = weightColumn.values
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        add(codes(row), weightValues(row))
        row += 1
      }

    def scanWeightsSparse(keyColumn: DictUtf8Column, weightColumn: Int32Column): Unit =
      val codes = keyColumn.codes
      val weightValues = weightColumn.values
      val keyWords = keyColumn.validity.words
      val weightWords = weightColumn.validity.words
      val rows = keyColumn.length
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row) && Validity.isSet(weightWords, row)) then
          add(codes(row), weightValues(row))
        row += 1
      }

    /**
     * Surviving codes keep `dictionary`. One batch has one dictionary, so a code is one string.
     */
    def compact(dictionary: Array[String]): ColumnBatch =
      val rows = occupied
      if (rows == 0) then ColumnBatches.emptyStringPairs
      else
        val outCodes = new Array[Int](rows)
        val outCounts = new Array[Int](rows)
        var cursor = 0
        while (cursor < rows) {
          val slot = slots(cursor)
          outCodes(cursor) = keys(slot)
          outCounts(cursor) = counts(slot)
          cursor += 1
        }
        val present = Validity.allValid(rows)
        new ColumnBatch(
          Vector(LogicalType.Utf8Dict, LogicalType.Int32),
          Vector(
            DictUtf8Column.of(dictionary, outCodes, present, rows),
            Int32Column.of(outCounts, present, rows),
          ),
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

    /** Probe with the mixed code. Two strings that share a hash stay distinct codes. */
    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      while (state(slot) != 0 && keys(slot) != key) slot = (slot + 1) & mask
      slot
