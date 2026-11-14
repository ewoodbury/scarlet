package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicLong

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
 * is at least twice the row count, so one batch does not rehash. One table lives on the thread
 * that scans a morsel. The next morsel on that thread clears it and reuses it when the operator is
 * the same and the capacity still fits. [[releaseWorkers]] drops it.
 *
 * [[int64Selected]] scans a survivor index with that same null rule. [[int64FilteredGroup]]
 * applies long key, value, or row steps during the insert, so a filtered scan does not build that
 * index. Several ranges of one batch share the table and compact once: a low-cardinality key is
 * inserted on the first range and updated after that. The table grows when a later range might not
 * fit. A step drops a null it reads. A null key is still dropped when no step reads the key. A
 * null value on a kept key stays missing until another value arrives. [[byKeySelected]]
 * partial-reduces the index, then exchanges and spills the dense partials the same way [[byKey]]
 * does. The filtered fold uses that same exchange. The exchange does not see the source rows.
 * Spill charges the dense partial, not the survivor count.
 *
 * Both paths read the key at ordinal 0 and the value at ordinal 1, and the exchange partitions on
 * that layout. A change to the pair layout has to change both together.
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
      val table = borrow(until - from, op)
      if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
        table.scanDenseSelected(keys, values, selected.rows, from, until)
      else table.scanSparseSelected(keys, values, selected.rows, from, until)
      table.compact()

  /**
   * Fold each start and length through `steps` into one table and compact once. `dense` means both
   * columns are fully present, so the scan skips the bit test. An empty range is skipped and does
   * not call a predicate. A group of only empty ranges publishes an empty long-pair batch. The
   * table starts large enough for the longest range and grows when a later range could not fit
   * beside the keys already inserted. Callers keep ranges in source order so first-seen order
   * matches a single scan.
   */
  private[columnar] def int64FilteredGroup(
      batch: ColumnBatch,
      dense: Boolean,
      starts: Array[Int],
      lengths: Array[Int],
      steps: List[Selection.Step],
      op: LongBinOp,
  ): ColumnBatch =
    require(starts.length == lengths.length, "filtered ranges disagree")
    require(steps.nonEmpty, "a filter stack needs a predicate")
    val count = starts.length
    var maxLength = 0
    var cursor = 0
    var any = false
    while (cursor < count) do
      val length = lengths(cursor)
      if (length > maxLength) then maxLength = length
      if (length > 0) then any = true
      cursor += 1
    if (!any) then ColumnBatches.emptyLongPairs
    else
      val keys = ColumnHash.int64At(batch, 0)
      val values = ColumnHash.int64At(batch, 1)
      require(keys.length == values.length, "key and value columns differ in length")
      val table = borrow(maxLength, op)
      cursor = 0
      while (cursor < count) do
        val length = lengths(cursor)
        if (length > 0) then
          val start = starts(cursor)
          Ranges.check(start, length, keys.length)
          table.reserve(length)
          val end = start + length
          if (dense) then table.scanDenseSteps(keys.values, values.values, start, end, steps)
          else
            table.scanSparseSteps(
              keys.values,
              values.values,
              keys.validity.words,
              values.validity.words,
              start,
              end,
              steps,
            )
        cursor += 1
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
      try
        val combined = PartialCombine.perBatch(
          batches,
          batchSize,
          parallelism,
          morsel => int64(morsel, keyOrdinal = 0, valueOrdinal = 1, op),
          partials => int64(partials, keyOrdinal = 0, valueOrdinal = 1, op),
        )
        reduceExchanged(combined, numPartitions, parallelism, batchSize, op)
      finally releaseWorkers()

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
      try
        val combined = PartialCombine.perBatchSelected(
          selected,
          batchSize,
          parallelism,
          (one, from, until) =>
            int64Selected(one, from, until, keyOrdinal = 0, valueOrdinal = 1, op),
          partials => int64(partials, keyOrdinal = 0, valueOrdinal = 1, op),
        )
        reduceExchanged(combined, numPartitions, parallelism, batchSize, op)
      finally releaseWorkers()

  /**
   * Exchange combined partials and reduce each bucket. Shared by [[byKey]], [[byKeySelected]], and
   * the filtered fold. An empty input is `numPartitions` empty long-pair batches.
   */
  private[columnar] def exchangeReduced(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: LongBinOp,
  ): Vector[ColumnBatch] =
    if (combined.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongPairs)
    else reduceExchanged(combined, numPartitions, parallelism, batchSize, op)

  /** Exchange combined partials and reduce each bucket. */
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
    val table = borrow(keys.length, op)
    if (ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)) then
      table.scanDense(keys, values)
    else table.scanSparse(keys, values)
    table.compact()

  /** Empty slot, a present value, or a null value that has not yet met another value. */
  private val empty: Byte = 0
  private val present: Byte = 1
  private val missing: Byte = 2
  private val zero: Long = 0L

  private val allocatedTables = new AtomicLong
  private val reusedTables = new AtomicLong

  private val locals: ThreadLocal[Table | Null] = new ThreadLocal[Table | Null]()

  /**
   * Drops this thread's table. [[MorselScheduler]] calls this when a pool task returns, and
   * [[byKey]] calls it on the caller. A cleared table is not a published batch.
   */
  private[columnar] def releaseWorkers(): Unit =
    locals.remove()

  /** Zeros the reuse counters and drops this thread's table. */
  private[columnar] def resetReuseForTest(): Unit =
    allocatedTables.set(0L)
    reusedTables.set(0L)
    releaseWorkers()

  /** Tables constructed since the last [[resetReuseForTest]], on every thread. */
  private[columnar] def tablesAllocatedForTest: Long = allocatedTables.get

  /** Morsels that cleared an existing table instead of allocating one. */
  private[columnar] def tablesReusedForTest: Long = reusedTables.get

  private def borrow(rows: Int, op: LongBinOp): Table =
    val need = ColumnHash.tableSize(rows)
    locals.get() match
      case current: Table if current.accepts(need, op) =>
        val _ = reusedTables.incrementAndGet()
        current.clear()
        current
      case _ =>
        val created = new Table(need, op)
        locals.set(created)
        val _ = allocatedTables.incrementAndGet()
        created

  private final class Table(initial: Int, val op: LongBinOp):
    private var capacity: Int = initial
    private var mask: Int = initial - 1
    private var limit: Int = initial >>> 1
    private var keys: Array[Long] = new Array[Long](initial)
    private var values: Array[Long] = new Array[Long](initial)
    private var state: Array[Byte] = new Array[Byte](initial)
    private var slots: Array[Int] = new Array[Int](initial)
    private var occupied: Int = 0

    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    def accepts(need: Int, bin: LongBinOp): Boolean =
      capacity >= need && (op eq bin)

    /**
     * Makes room so `incoming` new keys could be inserted beside the ones already present. A range
     * whose keys are already in the table does not fill that room. Growing keeps first-seen order.
     */
    def reserve(incoming: Int): Unit =
      val need = occupied.toLong + incoming.toLong
      while (need > limit.toLong) do
        if (capacity >= ColumnHash.MaxCapacity) then
          throw new IllegalStateException(s"hash reduce table is full at $occupied groups")
        grow()

    /**
     * Drops every group this morsel inserted. The arrays stay, so the next morsel does not zero
     * them.
     */
    def clear(): Unit =
      var cursor = 0
      val rows = occupied
      while (cursor < rows) {
        state(slots(cursor)) = empty
        cursor += 1
      }
      occupied = 0

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

    /**
     * Both columns are present on the whole batch. The key-then-value pair is the filtered
     * `reduceByKey` loop: the value is read only after the key predicate keeps the row.
     */
    def scanDenseSteps(
        keyValues: Array[Long],
        valueValues: Array[Long],
        start: Int,
        end: Int,
        steps: List[Selection.Step],
    ): Unit =
      steps match
        case Selection.Step.Int64(0, keyKeep) :: Selection.Step.Int64(1, valueKeep) :: Nil =>
          scanKeyThenValue(keyValues, valueValues, start, end, keyKeep, valueKeep)
        case _ =>
          scanDenseGeneral(keyValues, valueValues, start, end, steps.toArray)

    def scanSparseSteps(
        keyValues: Array[Long],
        valueValues: Array[Long],
        keyWords: Array[Long],
        valueWords: Array[Long],
        start: Int,
        end: Int,
        steps: List[Selection.Step],
    ): Unit =
      val stack = steps.toArray
      var row = start
      while (row < end) {
        if (
            passesSparse(stack, row, keyValues, keyWords, valueValues, valueWords) &&
            Validity.isSet(keyWords, row)
          )
        then
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

    private def scanKeyThenValue(
        keyValues: Array[Long],
        valueValues: Array[Long],
        start: Int,
        end: Int,
        keyKeep: ColumnKernel.Int64Predicate,
        valueKeep: ColumnKernel.Int64Predicate,
    ): Unit =
      var row = start
      while (row < end) {
        val key = keyValues(row)
        if (keyKeep(key)) then
          val value = valueValues(row)
          if (valueKeep(value)) then addPresent(key, value)
        row += 1
      }

    private def scanDenseGeneral(
        keyValues: Array[Long],
        valueValues: Array[Long],
        start: Int,
        end: Int,
        steps: Array[Selection.Step],
    ): Unit =
      var row = start
      while (row < end) {
        if (passesDense(steps, row, keyValues, valueValues)) then
          addPresent(keyValues(row), valueValues(row))
        row += 1
      }

    private def passesDense(
        steps: Array[Selection.Step],
        row: Int,
        keyValues: Array[Long],
        valueValues: Array[Long],
    ): Boolean =
      var index = 0
      var keep = true
      while (keep && index < steps.length) {
        keep = steps(index) match
          case Selection.Step.Int64(0, pred) => pred(keyValues(row))
          case Selection.Step.Int64(1, pred) => pred(valueValues(row))
          case Selection.Step.LongPair(pred) => pred(keyValues(row), valueValues(row))
          case Selection.Step.Int64(_, _) | Selection.Step.Int32(_, _) |
              Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
              Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) |
              Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
            false
        index += 1
      }
      keep

    private def passesSparse(
        steps: Array[Selection.Step],
        row: Int,
        keyValues: Array[Long],
        keyWords: Array[Long],
        valueValues: Array[Long],
        valueWords: Array[Long],
    ): Boolean =
      var index = 0
      var keep = true
      while (keep && index < steps.length) {
        keep = steps(index) match
          case Selection.Step.Int64(0, pred) =>
            Validity.isSet(keyWords, row) && pred(keyValues(row))
          case Selection.Step.Int64(1, pred) =>
            Validity.isSet(valueWords, row) && pred(valueValues(row))
          case Selection.Step.LongPair(pred) =>
            Validity.isSet(keyWords, row) &&
            Validity.isSet(valueWords, row) &&
            pred(keyValues(row), valueValues(row))
          case Selection.Step.Int64(_, _) | Selection.Step.Int32(_, _) |
              Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
              Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) |
              Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
            false
        index += 1
      }
      keep

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

    /** Doubles the table and reinserts occupied groups in first-seen order. */
    private def grow(): Unit =
      val next = capacity << 1
      if (next <= 0 || next > ColumnHash.MaxCapacity) then
        throw new IllegalStateException(s"hash reduce table is full at $occupied groups")
      val nextMask = next - 1
      val nextKeys = new Array[Long](next)
      val nextValues = new Array[Long](next)
      val nextState = new Array[Byte](next)
      val nextSlots = new Array[Int](next)
      var cursor = 0
      while (cursor < occupied) do
        val from = slots(cursor)
        val key = keys(from)
        var slot = ColumnHash.mix(java.lang.Long.hashCode(key)) & nextMask
        while (nextState(slot) != empty) do slot = (slot + 1) & nextMask
        nextState(slot) = state(from)
        nextKeys(slot) = key
        nextValues(slot) = values(from)
        nextSlots(cursor) = slot
        cursor += 1
      keys = nextKeys
      values = nextValues
      state = nextState
      slots = nextSlots
      capacity = next
      mask = nextMask
      limit = next >>> 1
