package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * `countByKey` over one string-pair [[StringPairProject]].
 *
 * The project's input morsels are already filtered. Each row is handed to the same adapter the
 * interpreter uses, including a null key and a null value. Only a present projected key is
 * inserted. The value cell is checked and dropped, so the pair batch is never published and
 * [[StringHashCount.byKey]] does not see it. The function is not inspected.
 *
 * Each partial owns its dictionary. A code is comparable only inside that dictionary.
 * [[ColumnBatches.concat]] remaps unlike dictionaries before a merge, the same way
 * [[StringHashCount]] merges published batches. Partials are one count per morsel, merged per
 * input batch, then exchanged. Spill stays on the post-exchange bucket. A morsel whose row
 * estimate does not fit throws [[ClosedKernelRefusal]] before its table exists. With spill off,
 * the whole input is charged the same way [[StringHashCount.byKey]] charges it: one table for the
 * input rows, plus the source dictionaries summed rather than unioned.
 *
 * A source dictionary code whose projected key never changes is counted with one `equals` against
 * the first projected string. The open-addressed table is filled from those codes at the end. A
 * null input key has no code. A projected key that changes for the same code is hashed from that
 * row on. The function is still called on every row, and its body is not inspected.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object ProjectedStringPairCount:

  def count(
      batches: Vector[ColumnBatch],
      mapper: StringPairProject.StringPairMap,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyStringPairs)
    else
      batches.foreach(StringHashCount.requireKey)
      val rows = batches.iterator.map(_.length.toLong).sum
      if (StringHashCount.refuseBeforeCombine) then
        val entries =
          batches.iterator.map(batch => StringHashCount.dictionaryEntries(batch).toLong).sum
        val text = batches.iterator.map(batch => StringHashCount.dictionaryBytes(batch)).sum
        StringHashCount.refuse(rows, entries, text)
      val groups = batches.map(batch => Morsels.of(batch, batchSize))
      val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
        partial(morsel, mapper)
      }
      val combined = PartialCombine.mergeMorsels(
        groups.map(_.length),
        partials,
        parallelism,
        StringHashCount.addCounts,
      )
      finish(combined, numPartitions, parallelism, batchSize)

  private def partial(
      morsel: Morsel,
      mapper: StringPairProject.StringPairMap,
  ): ColumnBatch =
    val rows = morsel.length
    if (rows == 0) then ColumnBatches.emptyStringPairs
    else
      StringHashCount.refuse(
        rows.toLong,
        StringHashCount.dictionaryEntries(morsel.batch).toLong,
        StringHashCount.dictionaryBytes(morsel.batch),
      )
      val dictionaryLength =
        ColumnKernel.asUtf8(ColumnKernel.columnAt(morsel.batch, 0), 0).dictionary.length
      val table = new Table(rows, dictionaryLength)
      StringPairProject.visitKeys(morsel.batch, morsel.start, rows, mapper, table)
      ColumnMetrics.fused()
      table.publish()

  /**
   * Counts projected keys for one morsel.
   *
   * `seen(code)` is the first projected string for that input dictionary code. A later row that
   * returns the same string increments `tallies`. The hash table exists only for a null input key,
   * a code whose projection changes, or the final fold. A null slot is empty. `""` is stored. The
   * count wraps on overflow, the same way [[StringHashCount]] wraps. Capacity stays at least twice
   * the morsel length, so the table does not rehash.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Null"))
  private final class Table(rows: Int, dictionaryLength: Int) extends StringPairProject.PresentKey:
    private val track: Boolean = dictionaryLength > 0 && dictionaryLength <= rows
    private val seen: Array[String] =
      if (track) then new Array[String](dictionaryLength) else null
    private val tallies: Array[Int] =
      if (track) then new Array[Int](dictionaryLength) else null
    private var stable: Boolean = track
    private var ready: Boolean = false
    private var mask: Int = 0
    private var limit: Int = 0
    private var keys: Array[String] = null
    private var counts: Array[Int] = null
    private var order: Array[Int] = null
    private var occupied: Int = 0

    def add(sourceCode: Int, key: String): Unit =
      if (sourceCode < 0 || !stable) then hashAdd(key, 1)
      else
        val first = seen(sourceCode)
        if (BatchCodec.isAbsent(first)) then
          seen(sourceCode) = key
          tallies(sourceCode) = 1
        else if (first.equals(key)) then tallies(sourceCode) = tallies(sourceCode) + 1
        else
          stable = false
          fold()
          hashAdd(key, 1)

    /** First-seen order inside the hash table. Code `i` is dictionary entry `i`. */
    def publish(): ColumnBatch =
      if (stable) then fold()
      if (!ready || occupied == 0) then ColumnBatches.emptyStringPairs
      else
        val distinct = occupied
        val dictionary = new Array[String](distinct)
        val codes = new Array[Int](distinct)
        val outCounts = new Array[Int](distinct)
        var cursor = 0
        while (cursor < distinct) {
          val slot = order(cursor)
          dictionary(cursor) = keys(slot)
          codes(cursor) = cursor
          outCounts(cursor) = counts(slot)
          cursor += 1
        }
        val present = Validity.allValid(distinct)
        new ColumnBatch(
          Vector(LogicalType.Utf8Dict, LogicalType.Int32),
          Vector(
            DictUtf8Column.of(dictionary, codes, present, distinct),
            Int32Column.of(outCounts, present, distinct),
          ),
          distinct,
        )

    /** Moves stable code counts into the hash table. Skips codes this morsel never projected. */
    private def fold(): Unit =
      var code = 0
      val width = seen.length
      while (code < width) {
        val key = seen(code)
        if (!BatchCodec.isAbsent(key)) then hashAdd(key, tallies(code))
        code += 1
      }

    private def hashAdd(key: String, weight: Int): Unit =
      ensure()
      val slot = locate(key)
      if (BatchCodec.isAbsent(keys(slot))) then
        if (occupied >= limit) then
          throw new IllegalStateException(s"string count table is full at $occupied groups")
        keys(slot) = key
        counts(slot) = weight
        order(occupied) = slot
        occupied += 1
      else counts(slot) = counts(slot) + weight

    private def ensure(): Unit =
      if (!ready) then
        val capacity = ColumnHash.tableSize(math.max(rows, 1))
        mask = capacity - 1
        limit = capacity >>> 1
        keys = new Array[String](capacity)
        counts = new Array[Int](capacity)
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
      else
        MorselScheduler.map(buckets, parallelism)((_, bucket) => StringHashCount.addCounts(bucket))
