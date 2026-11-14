package com.ewoodbury.scarlet.columnar

import java.util.Arrays
import java.util.concurrent.atomic.AtomicLong

/**
 * Inner hash join of `Int32` keys. Null keys are dropped on both sides. A null value is preserved
 * on a matched row and stored as `0`. The published join is the multiset of matches: multiplicity,
 * values, and null semantics. Order is unspecified.
 *
 * [[innerInt32]] builds from one batch and writes `(key, build value, probe value)`. Within a
 * probe row the build chain is newest-first. That walk is a kernel detail, not a public contract.
 *
 * [[openInt32]] fills one table from every build batch, then probes. A later batch is inserted
 * after an earlier batch, and a later row after an earlier row, so the chain walk is newest-first.
 * That walk is a kernel detail. `probe` writes `(key, probe value, build value)`. The inner
 * broadcast caller restores `(key, left, right)` when the logical left input was the build side.
 * The table is not mutated by a probe, so probes may run together. [[innerInt32]] does not use
 * this table. A non-empty build is estimated before the table is allocated. The estimate covers
 * that table only. An estimate above `buildBudgetBytes` throws `IllegalStateException`.
 *
 * Both tables start at [[InitialBuildSlots]], or at [[ColumnHash.tableSize]] when that is smaller,
 * and double when a new key would push occupied slots above half the capacity. A duplicate key
 * does not grow the table. [[retainedBuildBytes]] still prices `tableSize` slots, so a build that
 * refuses today still refuses. A growth step checks that same price and throws before allocating
 * when the step would exceed the budget.
 *
 * Each side names its key and value ordinals. The probe writes matches directly, with the row
 * index in a `var`, so the hot loop stays allocation-free like `ColumnKernel`. Output columns keep
 * the grown capacity and `length` is the match count, so the result is not copied down to size.
 *
 * A full outer probe records each matched build row in a caller-owned bitmap. The table stays
 * immutable, so each probe thread supplies its own bitmap and the caller ORs them after the probes
 * join. Null build keys are removed before insert: they have no slot and no bit. The bitmap is not
 * part of `buildBudgetBytes`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object HashJoin:

  /** Slots allocated by build tables since the last [[resetBuiltSlots]]. */
  private val builtSlots = new AtomicLong

  private[scarlet] def resetBuiltSlots(): Unit =
    builtSlots.set(0L)

  private[scarlet] def builtSlotsTotal: Long = builtSlots.get()

  private def noteSlots(slots: Int): Unit =
    val _ = builtSlots.addAndGet(slots.toLong)

  /**
   * First open-addressed capacity. A build whose ideal table is smaller starts at that size and
   * does not grow. A larger build starts here.
   */
  private[scarlet] val InitialBuildSlots: Int = 4096

  /** One side of the join. `key` and `value` are column ordinals in `batch`. */
  final case class JoinSide(batch: ColumnBatch, key: Int, value: Int)

  def innerInt32(build: JoinSide, probe: JoinSide): ColumnBatch =
    val buildKeys = ColumnHash.int32At(build.batch, build.key)
    val buildValues = ColumnHash.int32At(build.batch, build.value)
    val probeKeys = ColumnHash.int32At(probe.batch, probe.key)
    val probeValues = ColumnHash.int32At(probe.batch, probe.value)
    require(buildKeys.length == buildValues.length, "build key and value columns differ in length")
    require(probeKeys.length == probeValues.length, "probe key and value columns differ in length")
    if (buildKeys.length == 0 || probeKeys.length == 0) then ColumnBatches.emptyIntJoin
    else indexed(buildKeys, buildValues).write(probeKeys, probeValues)

  /**
   * One build table for every batch in `sides`, in vector order. An empty input, or batches whose
   * live lengths sum to zero, probes to an empty batch and allocates nothing. A non-int column or
   * a bad ordinal fails before the table is built. Otherwise [[retainedBuildBytes]] is compared
   * with `buildBudgetBytes` before any table array is allocated.
   */
  def openInt32(sides: Vector[JoinSide], buildBudgetBytes: Long): Int32Build =
    val checked = sides.map(checkedSide)
    val totalLong = checked.iterator.map(side => side.keys.length.toLong).sum
    require(totalLong <= Int.MaxValue.toLong, s"build side has $totalLong rows")
    val total = totalLong.toInt
    if (total == 0) then Int32Build.empty
    else
      val estimate = retainedBuildBytes(total)
      if (estimate > buildBudgetBytes) then refuseBuild(total, estimate, buildBudgetBytes)
      else
        val flat = copyBuild(checked, total)
        val table = Int32Build(flat, buildBudgetBytes)
        table.insert()
        table

  /**
   * Retained bytes of the open build table for `rows` live build rows. Each row keeps a 4-byte
   * key, a 4-byte value, and one byte for the validity bitmaps. Each slot keeps a 4-byte key and a
   * 4-byte head; a slot is free when its head is -1. Each row also keeps a 4-byte chain link. The
   * validity charge is whole bytes, which rounds those bitsets up, and a fixed allowance covers
   * the array headers and alignment. This is the build table only. A full outer join's matched-row
   * bitmaps are allocated beside the table and are not included.
   */
  private[scarlet] def retainedBuildBytes(rows: Int): Long =
    retainedAt(rows, ColumnHash.tableSize(rows))

  /**
   * [[retainedBuildBytes]] with an explicit slot count. The per-slot chain-length array is not
   * included, so a growth check prices the same bytes as the upfront refusal.
   */
  private[columnar] def retainedAt(rows: Int, slots: Int): Long =
    val rowBytes = 4L + 4L + 1L
    val slotBytes = 4L + 4L
    val chainBytes = 4L
    val headers = 256L
    rows.toLong * rowBytes + slots.toLong * slotBytes + rows.toLong * chainBytes + headers

  private[columnar] def refuseSized(rows: Int, estimate: Long, budget: Long): Nothing =
    refuseBuild(rows, estimate, budget)

  private def refuseBuild(rows: Int, estimate: Long, budget: Long): Nothing =
    throw new IllegalStateException(
      s"broadcast build table retains about $estimate bytes for $rows rows, above the budget of $budget bytes",
    )

  private def checkedSide(side: JoinSide): CheckedSide =
    val keys = ColumnHash.int32At(side.batch, side.key)
    val values = ColumnHash.int32At(side.batch, side.value)
    require(keys.length == values.length, "build key and value columns differ in length")
    CheckedSide(keys, values)

  private def copyBuild(sides: Vector[CheckedSide], total: Int): FlatBuild =
    val keys = new Array[Int](total)
    val values = new Array[Int](total)
    val keyWords = Validity.allocate(total)
    val valueWords = Validity.allocate(total)
    var cursor = 0
    sides.foreach { side =>
      val count = side.keys.length
      System.arraycopy(side.keys.values, 0, keys, cursor, count)
      System.arraycopy(side.values.values, 0, values, cursor, count)
      var row = 0
      while (row < count) {
        if (Validity.isSet(side.keys.validity.words, row)) then
          Validity.setBit(keyWords, cursor + row)
        if (Validity.isSet(side.values.validity.words, row)) then
          Validity.setBit(valueWords, cursor + row)
        row += 1
      }
      cursor += count
    }
    FlatBuild(total, keys, keyWords, values, valueWords)

  private final case class CheckedSide(keys: Int32Column, values: Int32Column)

  private final case class FlatBuild(
      rows: Int,
      keys: Array[Int],
      keyWords: Array[Long],
      values: Array[Int],
      valueWords: Array[Long],
  )

  private def indexed(buildKeys: Int32Column, buildValues: Int32Column): BuildTable =
    val table = new BuildTable(buildKeys, buildValues)
    table.insert()
    table

  /** Slot count of one shuffle-hash build. An empty side reports zero. */
  private[scarlet] def innerSlotCount(build: JoinSide): Int =
    val keys = ColumnHash.int32At(build.batch, build.key)
    val values = ColumnHash.int32At(build.batch, build.value)
    require(keys.length == values.length, "build key and value columns differ in length")
    if (keys.length == 0) then 0 else indexed(keys, values).slotCount

  private final class BuildTable(buildKeys: Int32Column, buildValues: Int32Column):
    private val rows: Int = buildKeys.length
    private val slots = new JoinSlots(rows, Long.MaxValue)
    private val buildKeyValues: Array[Int] = buildKeys.values
    private val keyWords: Array[Long] = buildKeys.validity.words

    def insert(): Unit =
      var row = 0
      while (row < rows) {
        if (Validity.isSet(keyWords, row)) then slots.insert(buildKeyValues(row), row)
        row += 1
      }
      noteSlots(slots.slots)

    def slotCount: Int = slots.slots

    def write(probeKeys: Int32Column, probeValues: Int32Column): ColumnBatch =
      val buildValueValues = buildValues.values
      val buildValueWords = buildValues.validity.words
      val probeValueValues = probeValues.values
      val probeValueWords = probeValues.validity.words
      val probeKeyValues = probeKeys.values
      val probeKeyWords = probeKeys.validity.words
      val probeRows = probeKeys.length
      val reserved =
        matchReserve(
          Some(slots),
          probeKeyValues,
          probeKeyWords,
          probeRows,
          rows,
          keepMisses = false,
        )
      if (reserved == 0) then ColumnBatches.emptyIntJoin
      else
        var outCapacity = reserved
        var filled = 0
        var outKeys = new Array[Int](outCapacity)
        var outBuild = new Array[Int](outCapacity)
        var outProbe = new Array[Int](outCapacity)
        var buildWords = Validity.allocate(outCapacity)
        var probeWords = Validity.allocate(outCapacity)
        var row = 0
        while (row < probeRows) {
          if (Validity.isSet(probeKeyWords, row)) then
            val key = probeKeyValues(row)
            val slot = slots.locate(key)
            if (slot >= 0) then
              var buildRow = slots.headRow(slot)
              while (buildRow != -1) {
                if (filled == outCapacity) then
                  outCapacity = doubled(outCapacity)
                  outKeys = Arrays.copyOf(outKeys, outCapacity)
                  outBuild = Arrays.copyOf(outBuild, outCapacity)
                  outProbe = Arrays.copyOf(outProbe, outCapacity)
                  buildWords = growWords(buildWords, outCapacity)
                  probeWords = growWords(probeWords, outCapacity)
                outKeys(filled) = buildKeyValues(buildRow)
                if (Validity.isSet(buildValueWords, buildRow)) then
                  outBuild(filled) = buildValueValues(buildRow)
                  Validity.setBit(buildWords, filled)
                if (Validity.isSet(probeValueWords, row)) then
                  outProbe(filled) = probeValueValues(row)
                  Validity.setBit(probeWords, filled)
                filled += 1
                buildRow = slots.nextRow(buildRow)
              }
          row += 1
        }
        if (filled == 0) then ColumnBatches.emptyIntJoin
        else publish(outKeys, outBuild, outProbe, buildWords, probeWords, outCapacity, filled)

  private def publish(
      keys: Array[Int],
      buildValues: Array[Int],
      probeValues: Array[Int],
      buildWords: Array[Long],
      probeWords: Array[Long],
      capacity: Int,
      rows: Int,
  ): ColumnBatch =
    publish(
      keys,
      buildValues,
      probeValues,
      buildWords,
      probeWords,
      capacity,
      rows,
      Validity.prefixValid(capacity, rows).words,
    )

  /**
   * `firstAbsence` follows the first value array and `secondAbsence` the second. That order is the
   * array order, which an outer probe passes as `(probe, build)`. The inner path passes neither,
   * and this does not allocate a bitmap for it.
   */
  private def publish(
      keys: Array[Int],
      buildValues: Array[Int],
      probeValues: Array[Int],
      buildWords: Array[Long],
      probeWords: Array[Long],
      capacity: Int,
      rows: Int,
      keyWords: Array[Long],
      firstAbsence: Option[Array[Long]] = None,
      secondAbsence: Option[Array[Long]] = None,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.fromWords(keyWords, capacity), rows),
        valueColumn(buildValues, buildWords, firstAbsence, capacity, rows),
        valueColumn(probeValues, probeWords, secondAbsence, capacity, rows),
      ),
      rows,
    )

  private def valueColumn(
      values: Array[Int],
      words: Array[Long],
      absence: Option[Array[Long]],
      capacity: Int,
      rows: Int,
  ): Int32Column =
    val validity = Validity.fromWords(words, capacity)
    absence match
      case Some(bits) if Validity.anySet(bits, rows) =>
        Int32Column.withAbsence(values, validity, Validity.fromWords(bits, capacity), rows)
      case _ => Int32Column.of(values, validity, rows)

  private def doubled(capacity: Int): Int =
    if (capacity >= ColumnHash.MaxCapacity) then
      throw new IllegalStateException(s"join output reached $capacity rows")
    capacity << 1

  /**
   * Rows to allocate before a probe writes. A chain longer than one stores a multiplicity, so the
   * reserve is the exact match count, including one row per miss when `keepMisses` is set. A
   * unique key has multiplicity one and reserves the probe length. With no chain, the reserve
   * starts at twice the build length, and at least the probe length, and the writer doubles if
   * that is short. Zero means the probe emits nothing.
   */
  private def matchReserve(
      index: Option[JoinSlots],
      probeKeys: Array[Int],
      probeWords: Array[Long],
      probeRows: Int,
      buildRows: Int,
      keepMisses: Boolean,
  ): Int =
    if (probeRows == 0) then 0
    else
      index match
        case Some(table) if table.maxChain > 1 =>
          table.countOutput(probeKeys, probeWords, probeRows, keepMisses)
        case Some(table) if table.maxChain == 0 && !keepMisses => 0
        case Some(_) => math.max(16, probeRows)
        case None =>
          val multiple = math.max(16, buildRows) << 1
          math.max(multiple, math.max(16, probeRows))

  private def growWords(words: Array[Long], rowCapacity: Int): Array[Long] =
    val needed = (rowCapacity + 63) >>> 6
    if (words.length == needed) words else Arrays.copyOf(words, needed)

  /**
   * Build table shared by every probe. `probe` writes `(key, probe value, build value)`. An empty
   * build returns an empty batch when `preserveProbe` is false. `preserveProbe` keeps the probe
   * side: each probe-side row with no partner is emitted, including a null probe key, and the
   * build value's absence bit is set. Its validity bit stays clear. A null value is not absence. A
   * null build key is not inserted, so it does not match probe key 0. The chain walk is
   * newest-first, which is a kernel detail.
   */
  final class Int32Build private (
      index: Option[JoinSlots],
      buildKeys: Array[Int],
      buildKeyWords: Array[Long],
      buildValues: Array[Int],
      buildValueWords: Array[Long],
      rows: Int,
  ):

    private[HashJoin] def insert(): Unit =
      index match
        case None => ()
        case Some(slots) =>
          var row = 0
          while (row < rows) {
            // A null key stays out of the table. Its payload is 0, and inserting it would match
            // probe key 0.
            if (Validity.isSet(buildKeyWords, row)) then slots.insert(buildKeys(row), row)
            row += 1
          }
          noteSlots(slots.slots)

    /** Slots in the open-addressed table. An empty build reports zero. */
    private[scarlet] def slotCount: Int =
      index match
        case None => 0
        case Some(slots) => slots.slots

    /** Inner probe. A probe row with no build match, including a null key, is dropped. */
    def probe(side: JoinSide): ColumnBatch = probe(side, preserveProbe = false)

    /**
     * `preserveProbe` appends each probe-side row with no partner. The build value's absence bit
     * is set on that row, and its validity bit stays clear. A null probe key does not match and is
     * kept only when `preserveProbe` is set. A null value is not absence.
     */
    def probe(side: JoinSide, preserveProbe: Boolean): ColumnBatch =
      val probeKeys = ColumnHash.int32At(side.batch, side.key)
      val probeValues = ColumnHash.int32At(side.batch, side.value)
      require(
        probeKeys.length == probeValues.length,
        "probe key and value columns differ in length",
      )
      if (probeKeys.length == 0 || (rows == 0 && !preserveProbe)) then ColumnBatches.emptyIntJoin
      else writeProbe(probeKeys, probeValues, preserveProbe)

    private def writeProbe(
        probeKeys: Int32Column,
        probeValues: Int32Column,
        preserveProbe: Boolean,
    ): ColumnBatch =
      val probeKeyValuesEarly = probeKeys.values
      val probeKeyWordsEarly = probeKeys.validity.words
      val reserved = matchReserve(
        index,
        probeKeyValuesEarly,
        probeKeyWordsEarly,
        probeKeys.length,
        rows,
        preserveProbe,
      )
      if (reserved == 0) then ColumnBatches.emptyIntJoin
      else writeProbeReserved(probeKeys, probeValues, preserveProbe, reserved)

    private def writeProbeReserved(
        probeKeys: Int32Column,
        probeValues: Int32Column,
        preserveProbe: Boolean,
        reserved: Int,
    ): ColumnBatch =
      var outCapacity = reserved
      var filled = 0
      var outKeys = new Array[Int](outCapacity)
      var outProbe = new Array[Int](outCapacity)
      var outBuild = new Array[Int](outCapacity)
      var keyWords = Validity.allocate(outCapacity)
      var probeWords = Validity.allocate(outCapacity)
      var buildWords = Validity.allocate(outCapacity)
      var buildAbsence =
        if (preserveProbe) then Some(Validity.allocate(outCapacity)) else None
      val probeValueValues = probeValues.values
      val probeValueWords = probeValues.validity.words
      val probeKeyValues = probeKeys.values
      val probeKeyWords = probeKeys.validity.words
      val probeRows = probeKeys.length

      def grow(): Unit =
        outCapacity = doubled(outCapacity)
        outKeys = Arrays.copyOf(outKeys, outCapacity)
        outProbe = Arrays.copyOf(outProbe, outCapacity)
        outBuild = Arrays.copyOf(outBuild, outCapacity)
        keyWords = growWords(keyWords, outCapacity)
        probeWords = growWords(probeWords, outCapacity)
        buildWords = growWords(buildWords, outCapacity)
        buildAbsence = buildAbsence.map(bits => growWords(bits, outCapacity))

      def append(
          key: Int,
          keyPresent: Boolean,
          probeValue: Int,
          probePresent: Boolean,
          buildValue: Int,
          buildPresent: Boolean,
      ): Unit =
        if (filled == outCapacity) then grow()
        outKeys(filled) = key
        if (keyPresent) then Validity.setBit(keyWords, filled)
        if (probePresent) then
          outProbe(filled) = probeValue
          Validity.setBit(probeWords, filled)
        if (buildPresent) then
          outBuild(filled) = buildValue
          Validity.setBit(buildWords, filled)
        filled += 1

      def appendUnmatched(probeRow: Int): Unit =
        append(
          probeKeyValues(probeRow),
          Validity.isSet(probeKeyWords, probeRow),
          probeValueValues(probeRow),
          Validity.isSet(probeValueWords, probeRow),
          0,
          false,
        )
        buildAbsence.foreach(bits => Validity.setBit(bits, filled - 1))

      var row = 0
      while (row < probeRows) {
        if (!Validity.isSet(probeKeyWords, row)) then
          if (preserveProbe) then appendUnmatched(row)
        else if (rows == 0) then
          if (preserveProbe) then appendUnmatched(row)
        else
          val key = probeKeyValues(row)
          index match
            case None =>
              if (preserveProbe) then appendUnmatched(row)
            case Some(slots) =>
              val slot = slots.locate(key)
              if (slot < 0) then
                if (preserveProbe) then appendUnmatched(row)
              else
                var buildRow = slots.headRow(slot)
                while (buildRow != -1) {
                  val buildPresent = Validity.isSet(buildValueWords, buildRow)
                  append(
                    buildKeys(buildRow),
                    true,
                    probeValueValues(row),
                    Validity.isSet(probeValueWords, row),
                    if (buildPresent) then buildValues(buildRow) else 0,
                    buildPresent,
                  )
                  buildRow = slots.nextRow(buildRow)
                }
        row += 1
      }
      if (filled == 0) then ColumnBatches.emptyIntJoin
      // publish takes its value arrays as (first, second). Passing probe first writes
      // (key, probe, build), which the broadcast path turns into (key, left, right).
      // The missing build value is that second column. An inner probe passes no absence words.
      else if (preserveProbe) then
        publish(
          outKeys,
          outProbe,
          outBuild,
          probeWords,
          buildWords,
          outCapacity,
          filled,
          keyWords,
          secondAbsence = buildAbsence,
        )
      else
        publish(
          outKeys,
          outProbe,
          outBuild,
          probeWords,
          buildWords,
          outCapacity,
          filled,
          keyWords,
        )

    /** Live build rows in this table. Null keys are not included. */
    private[scarlet] def buildRows: Int = rows

    /**
     * Full-outer probe. A probe row with no partner is kept, including a null probe key. Each
     * emitted match sets one bit in `matched`. `matched` is owned by this probe: plain stores, and
     * no other probe writes it. A null probe key sets no bit. Output is `(key, probe, build)`.
     */
    def probeFull(side: JoinSide, matched: Array[Long]): ColumnBatch =
      require(
        matched.length == ((rows + 63) >>> 6),
        s"matched bitmap has ${matched.length} words for $rows build rows",
      )
      val probeKeys = ColumnHash.int32At(side.batch, side.key)
      val probeValues = ColumnHash.int32At(side.batch, side.value)
      require(
        probeKeys.length == probeValues.length,
        "probe key and value columns differ in length",
      )
      if (probeKeys.length == 0 || rows == 0) then probe(side, preserveProbe = true)
      else writeTracked(probeKeys, probeValues, matched)

    /**
     * Build rows whose bit is clear, in table order. Output is `(key, absent probe, build value)`.
     * `None` means every build row was matched, or the table is empty. A null build key is not a
     * row of this table.
     */
    def unmatchedBuild(matched: Array[Long]): Option[ColumnBatch] =
      require(
        matched.length == ((rows + 63) >>> 6),
        s"matched bitmap has ${matched.length} words for $rows build rows",
      )
      var clear = 0
      var row = 0
      while (row < rows) {
        if (!Validity.isSet(matched, row)) then clear += 1
        row += 1
      }
      if (clear == 0) then None else Some(writeUnmatched(matched, clear))

    private def writeTracked(
        probeKeys: Int32Column,
        probeValues: Int32Column,
        matched: Array[Long],
    ): ColumnBatch =
      val reserved = matchReserve(
        index,
        probeKeys.values,
        probeKeys.validity.words,
        probeKeys.length,
        rows,
        keepMisses = true,
      )
      if (reserved == 0) then ColumnBatches.emptyIntJoin
      else writeTrackedReserved(probeKeys, probeValues, matched, reserved)

    private def writeTrackedReserved(
        probeKeys: Int32Column,
        probeValues: Int32Column,
        matched: Array[Long],
        reserved: Int,
    ): ColumnBatch =
      var outCapacity = reserved
      var filled = 0
      var outKeys = new Array[Int](outCapacity)
      var outProbe = new Array[Int](outCapacity)
      var outBuild = new Array[Int](outCapacity)
      var keyWords = Validity.allocate(outCapacity)
      var probeWords = Validity.allocate(outCapacity)
      var buildWords = Validity.allocate(outCapacity)
      var buildAbsence = Validity.allocate(outCapacity)
      val probeValueValues = probeValues.values
      val probeValueWords = probeValues.validity.words
      val probeKeyValues = probeKeys.values
      val probeKeyWords = probeKeys.validity.words
      val probeRows = probeKeys.length

      def grow(): Unit =
        outCapacity = doubled(outCapacity)
        outKeys = Arrays.copyOf(outKeys, outCapacity)
        outProbe = Arrays.copyOf(outProbe, outCapacity)
        outBuild = Arrays.copyOf(outBuild, outCapacity)
        keyWords = growWords(keyWords, outCapacity)
        probeWords = growWords(probeWords, outCapacity)
        buildWords = growWords(buildWords, outCapacity)
        buildAbsence = growWords(buildAbsence, outCapacity)

      def append(
          key: Int,
          keyPresent: Boolean,
          probeValue: Int,
          probePresent: Boolean,
          buildValue: Int,
          buildPresent: Boolean,
      ): Unit =
        if (filled == outCapacity) then grow()
        outKeys(filled) = key
        if (keyPresent) then Validity.setBit(keyWords, filled)
        if (probePresent) then
          outProbe(filled) = probeValue
          Validity.setBit(probeWords, filled)
        if (buildPresent) then
          outBuild(filled) = buildValue
          Validity.setBit(buildWords, filled)
        filled += 1

      def appendUnmatched(probeRow: Int): Unit =
        append(
          probeKeyValues(probeRow),
          Validity.isSet(probeKeyWords, probeRow),
          probeValueValues(probeRow),
          Validity.isSet(probeValueWords, probeRow),
          0,
          false,
        )
        Validity.setBit(buildAbsence, filled - 1)

      var row = 0
      while (row < probeRows) {
        if (!Validity.isSet(probeKeyWords, row)) then appendUnmatched(row)
        else
          val key = probeKeyValues(row)
          index match
            case None => appendUnmatched(row)
            case Some(slots) =>
              val slot = slots.locate(key)
              if (slot < 0) then appendUnmatched(row)
              else
                var buildRow = slots.headRow(slot)
                while (buildRow != -1) {
                  val buildPresent = Validity.isSet(buildValueWords, buildRow)
                  append(
                    buildKeys(buildRow),
                    true,
                    probeValueValues(row),
                    Validity.isSet(probeValueWords, row),
                    if (buildPresent) then buildValues(buildRow) else 0,
                    buildPresent,
                  )
                  // One plain store per emitted pair. `matched` is not shared.
                  Validity.setBit(matched, buildRow)
                  buildRow = slots.nextRow(buildRow)
                }
        row += 1
      }
      if (filled == 0) then ColumnBatches.emptyIntJoin
      else
        publish(
          outKeys,
          outProbe,
          outBuild,
          probeWords,
          buildWords,
          outCapacity,
          filled,
          keyWords,
          secondAbsence = Some(buildAbsence),
        )

    private def writeUnmatched(matched: Array[Long], clear: Int): ColumnBatch =
      val outKeys = new Array[Int](clear)
      val outProbe = new Array[Int](clear)
      val outBuild = new Array[Int](clear)
      val keyWords = Validity.allocate(clear)
      val probeWords = Validity.allocate(clear)
      val buildWords = Validity.allocate(clear)
      val probeAbsence = Validity.allocate(clear)
      var filled = 0
      var row = 0
      while (row < rows) {
        if (!Validity.isSet(matched, row)) then
          outKeys(filled) = buildKeys(row)
          Validity.setBit(keyWords, filled)
          if (Validity.isSet(buildValueWords, row)) then
            outBuild(filled) = buildValues(row)
            Validity.setBit(buildWords, filled)
          // The probe value is the missing side. The build value stays present.
          Validity.setBit(probeAbsence, filled)
          filled += 1
        row += 1
      }
      publish(
        outKeys,
        outProbe,
        outBuild,
        probeWords,
        buildWords,
        clear,
        filled,
        keyWords,
        firstAbsence = Some(probeAbsence),
      )

  private object Int32Build:

    def empty: Int32Build =
      new Int32Build(
        index = None,
        buildKeys = Array.emptyIntArray,
        buildKeyWords = Array.emptyLongArray,
        buildValues = Array.emptyIntArray,
        buildValueWords = Array.emptyLongArray,
        rows = 0,
      )

    def apply(flat: FlatBuild, budget: Long): Int32Build =
      new Int32Build(
        index = Some(new JoinSlots(flat.rows, budget)),
        buildKeys = flat.keys,
        buildKeyWords = flat.keyWords,
        buildValues = flat.values,
        buildValueWords = flat.valueWords,
        rows = flat.rows,
      )

  /**
   * Pulls null-key build rows out of `sides`. Insert skips a null key, so it has no slot and no
   * matched-row bit. `None` means every live key is present, and `sides` is returned unchanged.
   * Otherwise the present-key rows are one batch and the null-key rows are carried beside it, in
   * input order.
   */
  private[scarlet] def separateNullBuildKeys(
      sides: Vector[JoinSide],
  ): (Vector[JoinSide], Option[ColumnBatch]) =
    val columns = sides.map { side =>
      val keys = ColumnHash.int32At(side.batch, side.key)
      val values = ColumnHash.int32At(side.batch, side.value)
      require(keys.length == values.length, "build key and value columns differ in length")
      (keys, values)
    }
    val nullRows = columns.foldLeft(0L) { (total, column) =>
      total + clearCount(column._1.validity.words, column._1.length).toLong
    }
    require(nullRows <= Int.MaxValue.toLong, s"null build keys has $nullRows rows")
    if (nullRows == 0L) then (sides, None)
    else
      val presentRows = columns.foldLeft(0L)((total, column) => total + column._1.length.toLong) -
        nullRows
      require(presentRows <= Int.MaxValue.toLong, s"present build keys has $presentRows rows")
      val carried = copyBuildRows(columns, keepNullKeys = true, nullRows.toInt)
      val present: Vector[JoinSide] =
        if (presentRows == 0L) then Vector.empty
        else
          Vector(
            JoinSide(
              copyBuildRows(columns, keepNullKeys = false, presentRows.toInt),
              key = 0,
              value = 1,
            ),
          )
      (present, Some(carried))

  /**
   * One `(key, absent probe, build value)` row per null-key build row. The key bit stays clear.
   * The probe value's absence bit is set and its validity bit stays clear. The build value bit is
   * copied. These rows are not inserted.
   */
  private[scarlet] def emitNullBuildKeys(batch: ColumnBatch): ColumnBatch =
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "build key and value columns differ in length")
    val count = keys.length
    if (count == 0) then ColumnBatches.emptyIntJoin
    else
      val outKeys = Arrays.copyOf(keys.values, count)
      val outProbe = new Array[Int](count)
      val outBuild = Arrays.copyOf(values.values, count)
      val keyWords = Validity.allocate(count)
      val probeWords = Validity.allocate(count)
      val buildWords = Validity.allocate(count)
      val probeAbsence = Validity.allocate(count)
      var row = 0
      while (row < count) {
        if (Validity.isSet(keys.validity.words, row)) then Validity.setBit(keyWords, row)
        if (Validity.isSet(values.validity.words, row)) then Validity.setBit(buildWords, row)
        Validity.setBit(probeAbsence, row)
        row += 1
      }
      publish(
        outKeys,
        outProbe,
        outBuild,
        probeWords,
        buildWords,
        count,
        count,
        keyWords,
        firstAbsence = Some(probeAbsence),
      )

  /**
   * OR `bitsets` into the first array. The caller has joined the threads that wrote them, so the
   * words are plain loads and stores.
   */
  private[scarlet] def orMatched(bitsets: Array[Array[Long]]): Array[Long] = {
    require(bitsets.length > 0, "matched bitsets are empty")
    val merged = bitsets(0)
    var index = 1
    while (index < bitsets.length) {
      val from = bitsets(index)
      require(from.length == merged.length, "matched bitsets differ in length")
      var word = 0
      while (word < merged.length) {
        merged(word) = merged(word) | from(word)
        word += 1
      }
      index += 1
    }
    merged
  }

  private def clearCount(words: Array[Long], live: Int): Int = {
    var row = 0
    var count = 0
    while (row < live) {
      if (!Validity.isSet(words, row)) then count += 1
      row += 1
    }
    count
  }

  private def copyBuildRows(
      columns: Vector[(Int32Column, Int32Column)],
      keepNullKeys: Boolean,
      total: Int,
  ): ColumnBatch =
    val keys = new Array[Int](total)
    val values = new Array[Int](total)
    val keyWords = Validity.allocate(total)
    val valueWords = Validity.allocate(total)
    var cursor = 0
    columns.foreach { case (keyColumn, valueColumn) =>
      val keyValues = keyColumn.values
      val keyValidity = keyColumn.validity.words
      val valueValues = valueColumn.values
      val valueValidity = valueColumn.validity.words
      var row = 0
      while (row < keyColumn.length) {
        val keyPresent = Validity.isSet(keyValidity, row)
        if (keyPresent != keepNullKeys) then
          keys(cursor) = keyValues(row)
          values(cursor) = valueValues(row)
          if (keyPresent) then Validity.setBit(keyWords, cursor)
          if (Validity.isSet(valueValidity, row)) then Validity.setBit(valueWords, cursor)
          cursor += 1
        row += 1
      }
    }
    require(cursor == total, s"copied $cursor build rows, expected $total")
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.fromWords(keyWords, total), total),
        Int32Column.of(values, Validity.fromWords(valueWords, total), total),
      ),
      total,
    )
