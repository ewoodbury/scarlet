package com.ewoodbury.scarlet.columnar

/**
 * `reduceByKey` over one string slot above a string-pair [[StringPairProject]].
 *
 * The project's input morsels are already filtered. Each row is handed to the same adapter the
 * interpreter uses, including a null key and a null value. The slot then sees the projected pair.
 * A key predicate is not called on a null projected key. A value predicate is not called on a null
 * projected value. A present projected key and int value that the slot keeps are inserted into the
 * [[StringHashReduce]] table. A null projected key is dropped. A null projected value throws
 * [[ColumnarFallback]]. `""` is a present key. `"é"` and `"e\u0301"` stay distinct. The pair batch
 * is never published, and [[StringHashReduce.byKey]] does not see it. Neither function is
 * inspected.
 *
 * Each partial owns its dictionary. A code is comparable only inside that dictionary.
 * [[ColumnBatches.concat]] remaps unlike dictionaries before a merge. Partials are one reduce per
 * morsel, merged per input batch, then exchanged. Spill stays on the post-exchange bucket.
 *
 * A source dictionary code whose projected key never changes combines with one `equals` against
 * the first projected string. The open-addressed table is filled from those codes at the end. A
 * null input key has no code. A projected key that changes for the same code is hashed from that
 * row on. The function is still called on every row, and its body is not inspected.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object ProjectedStringAboveReduce:

  def reduce(
      batches: Vector[ColumnBatch],
      mapper: StringPairProject.StringPairMap,
      above: Selection.Step,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: (Int, Int) => Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val slot = stringSlot(above)
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyStringPairs)
    else
      val groups = batches.map(batch => Morsels.of(batch, batchSize))
      val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
        partial(morsel, mapper, slot, op)
      }
      val combined = PartialCombine.mergeMorsels(
        groups.map(_.length),
        partials,
        parallelism,
        batch => StringHashReduce.reduce(batch, op),
      )
      StringHashReduce.exchangeReduced(combined, numPartitions, parallelism, batchSize, op)

  /** The predicate is the function the planner stored, not a second wrapper. */
  private def stringSlot(step: Selection.Step): StringPairProject.Slot = step match
    case Selection.Step.Utf8(0, predicate) => StringPairProject.Slot.Key(predicate)
    case Selection.Step.Int32(1, predicate) => StringPairProject.Slot.Value(predicate)
    case Selection.Step.Utf8(column, _) =>
      throw new IllegalStateException(s"pair slot must be a key or a value, got $column")
    case Selection.Step.Int32(column, _) =>
      throw new IllegalStateException(s"pair slot must be a key or a value, got $column")
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.IntPair(_) | Selection.Step.LongPair(_) | Selection.Step.StringPair(_) |
        Selection.Step.IntTriple(_, _) =>
      throw new IllegalStateException(
        "slot above a string pair project is a string key or an int value",
      )

  private def partial(
      morsel: Morsel,
      mapper: StringPairProject.StringPairMap,
      above: StringPairProject.Slot,
      op: (Int, Int) => Int,
  ): ColumnBatch =
    val rows = morsel.length
    if (rows == 0) then ColumnBatches.emptyStringPairs
    else
      val dictionaryLength =
        ColumnKernel.asUtf8(ColumnKernel.columnAt(morsel.batch, 0), 0).dictionary.length
      val table = new Table(rows, dictionaryLength, op)
      StringPairProject.visitReduced(morsel.batch, morsel.start, rows, mapper, above, table)
      ColumnMetrics.fused()
      table.publish()

  /**
   * Combines projected pairs for one morsel.
   *
   * `seen(code)` is the first projected string for that input dictionary code. A later row that
   * returns the same string combines into `sums`. The hash table exists only for a null input key,
   * a code whose projection changes, or the final fold. A null slot is empty. `""` is stored. The
   * combine wraps the way [[StringHashReduce]] wraps. Capacity stays at least twice the morsel
   * length, so the table does not rehash.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Null"))
  private final class Table(rows: Int, dictionaryLength: Int, op: (Int, Int) => Int)
      extends StringPairProject.StringPairSink:
    private val track: Boolean = dictionaryLength > 0 && dictionaryLength <= rows
    private val seen: Array[String] =
      if (track) then new Array[String](dictionaryLength) else null
    private val sums: Array[Int] =
      if (track) then new Array[Int](dictionaryLength) else null
    private var stable: Boolean = track
    private var ready: Boolean = false
    private var mask: Int = 0
    private var limit: Int = 0
    private var keys: Array[String] = null
    private var values: Array[Int] = null
    private var order: Array[Int] = null
    private var occupied: Int = 0

    def add(sourceCode: Int, key: String, value: Int): Unit =
      if (sourceCode < 0 || !stable || sourceCode >= seen.length) then hashAdd(key, value)
      else
        val first = seen(sourceCode)
        if (BatchCodec.isAbsent(first)) then
          seen(sourceCode) = key
          sums(sourceCode) = value
        else if (first.equals(key)) then sums(sourceCode) = op(sums(sourceCode), value)
        else
          stable = false
          fold()
          hashAdd(key, value)

    /** First-seen order inside the hash table. Code `i` is dictionary entry `i`. */
    def publish(): ColumnBatch =
      if (stable) then fold()
      if (!ready || occupied == 0) then ColumnBatches.emptyStringPairs
      else
        val distinct = occupied
        val dictionary = new Array[String](distinct)
        val codes = new Array[Int](distinct)
        val outValues = new Array[Int](distinct)
        var cursor = 0
        while (cursor < distinct) {
          val slot = order(cursor)
          dictionary(cursor) = keys(slot)
          codes(cursor) = cursor
          outValues(cursor) = values(slot)
          cursor += 1
        }
        val present = Validity.allValid(distinct)
        new ColumnBatch(
          Vector(LogicalType.Utf8Dict, LogicalType.Int32),
          Vector(
            DictUtf8Column.of(dictionary, codes, present, distinct),
            Int32Column.of(outValues, present, distinct),
          ),
          distinct,
        )

    /** Moves stable code sums into the hash table. Skips codes this morsel never projected. */
    private def fold(): Unit =
      var code = 0
      val width = seen.length
      while (code < width) {
        val key = seen(code)
        if (!BatchCodec.isAbsent(key)) then hashAdd(key, sums(code))
        code += 1
      }

    private def hashAdd(key: String, weight: Int): Unit =
      ensure()
      val slot = locate(key)
      if (BatchCodec.isAbsent(keys(slot))) then
        if (occupied >= limit) then
          throw new IllegalStateException(s"string reduce table is full at $occupied groups")
        keys(slot) = key
        values(slot) = weight
        order(occupied) = slot
        occupied += 1
      else values(slot) = op(values(slot), weight)

    private def ensure(): Unit =
      if (!ready) then
        val capacity = ColumnHash.tableSize(math.max(rows, 1))
        mask = capacity - 1
        limit = capacity >>> 1
        keys = new Array[String](capacity)
        values = new Array[Int](capacity)
        order = new Array[Int](math.max(rows, 1))
        ready = true

    /** Probe with the mixed `hashCode`. Distinct strings that share a hash stay distinct. */
    private def locate(key: String): Int =
      var slot = ColumnHash.mix(key.hashCode) & mask
      var current = keys(slot)
      while (!BatchCodec.isAbsent(current) && !current.equals(key)) {
        slot = (slot + 1) & mask
        current = keys(slot)
      }
      slot
