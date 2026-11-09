package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Outer hash join of `Int64` keys and `Int64` values.
 *
 * [[open]] fills one table from every build batch, then probes. A null key is not inserted, so a
 * stored `0` does not match key `0`. `probe` with `preserveProbe` writes `(key, probe value, build
 * value)` and keeps a probe row with no partner, including a null probe key. The build value's
 * absence bit is set on that row and its validity bit stays clear. A null value is not absence.
 * [[probeFull]] records each matched build row in a caller-owned bitmap. Null build keys are
 * removed by [[separateNullBuildKeys]] before insert and emitted once by [[emitNullBuildKeys]].
 *
 * The chain walk is newest-first. That walk is a kernel detail, not a public contract. The table
 * is not mutated by a probe, so probes may run together. A non-empty build is estimated with
 * [[LongHashJoin.retainedBuildBytes]] before any table array is allocated. An estimate above
 * `buildBudgetBytes` throws `IllegalStateException`. The matched-row bitmap is not part of that
 * budget. This kernel does not spill.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object LongHashOuter:

  def open(sides: Vector[HashJoin.JoinSide], buildBudgetBytes: Long): LongBuild =
    val checked = sides.map(checkedSide)
    val totalLong = checked.iterator.map(side => side.keys.length.toLong).sum
    require(totalLong <= Int.MaxValue.toLong, s"build side has $totalLong rows")
    val total = totalLong.toInt
    if (total == 0) then LongBuild.empty
    else
      val estimate = LongHashJoin.retainedBuildBytes(total)
      if (estimate > buildBudgetBytes) then refuseBuild(total, estimate, buildBudgetBytes)
      else
        val flat = copyBuild(checked, total)
        val table = LongBuild(flat)
        table.insert()
        table

  private def refuseBuild(rows: Int, estimate: Long, budget: Long): Nothing =
    throw new IllegalStateException(
      s"broadcast build table retains about $estimate bytes for $rows rows, above the budget of $budget bytes",
    )

  private def checkedSide(side: HashJoin.JoinSide): CheckedSide =
    val keys = ColumnHash.int64At(side.batch, side.key)
    val values = ColumnHash.int64At(side.batch, side.value)
    require(keys.length == values.length, "build key and value columns differ in length")
    CheckedSide(keys, values)

  private def copyBuild(sides: Vector[CheckedSide], total: Int): FlatBuild =
    val keys = new Array[Long](total)
    val values = new Array[Long](total)
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

  private final case class CheckedSide(keys: Int64Column, values: Int64Column)

  private final case class FlatBuild(
      rows: Int,
      keys: Array[Long],
      keyWords: Array[Long],
      values: Array[Long],
      valueWords: Array[Long],
  )

  /**
   * Build table shared by every probe. An empty build returns an empty batch when `preserveProbe`
   * is false, and emits every probe row with the build value absent when it is true.
   */
  final class LongBuild private (
      mask: Int,
      slotKey: Array[Long],
      head: Array[Int],
      next: Array[Int],
      buildKeys: Array[Long],
      buildKeyWords: Array[Long],
      buildValues: Array[Long],
      buildValueWords: Array[Long],
      rows: Int,
  ):

    private[LongHashOuter] def insert(): Unit =
      var row = 0
      while (row < rows) {
        // A null key stays out of the table. Its payload is 0, and inserting it would match
        // probe key 0.
        if (Validity.isSet(buildKeyWords, row)) then
          val key = buildKeys(row)
          var slot = slotOf(key)
          var placed = false
          while (!placed) {
            if (head(slot) == -1) then
              head(slot) = row
              slotKey(slot) = key
              placed = true
            else if (slotKey(slot) == key) then
              next(row) = head(slot)
              head(slot) = row
              placed = true
            else slot = (slot + 1) & mask
          }
        row += 1
      }

    /**
     * Copied build rows. A null key the caller did not strip is included. [[unmatchedBuild]] skips
     * that row, so it is not published as key `0`.
     */
    def buildRows: Int = rows

    /**
     * `preserveProbe` appends each probe-side row with no partner. The build value's absence bit
     * is set on that row, and its validity bit stays clear. A null probe key does not match and is
     * kept only when `preserveProbe` is set.
     */
    def probe(side: HashJoin.JoinSide, preserveProbe: Boolean): ColumnBatch =
      val probeKeys = ColumnHash.int64At(side.batch, side.key)
      val probeValues = ColumnHash.int64At(side.batch, side.value)
      require(
        probeKeys.length == probeValues.length,
        "probe key and value columns differ in length",
      )
      if (probeKeys.length == 0 || (rows == 0 && !preserveProbe)) then ColumnBatches.emptyLongJoin
      else writeRows(probeKeys, probeValues, preserveProbe, matched = None)

    /**
     * Full-outer probe. A probe row with no partner is kept, including a null probe key. Each
     * emitted match sets one bit in `matched`. `matched` is owned by this probe. A null probe key
     * sets no bit. Output is `(key, probe, build)`.
     */
    def probeFull(side: HashJoin.JoinSide, matched: Array[Long]): ColumnBatch =
      require(
        matched.length == ((rows + 63) >>> 6),
        s"matched bitmap has ${matched.length} words for $rows build rows",
      )
      val probeKeys = ColumnHash.int64At(side.batch, side.key)
      val probeValues = ColumnHash.int64At(side.batch, side.value)
      require(
        probeKeys.length == probeValues.length,
        "probe key and value columns differ in length",
      )
      if (probeKeys.length == 0 || rows == 0) then probe(side, preserveProbe = true)
      else writeRows(probeKeys, probeValues, preserveProbe = true, matched = Some(matched))

    /**
     * Present build rows whose bit is clear, in table order. Output is `(key, absent probe, build
     * value)`. `None` means every present build row was matched, or the table is empty. A clear
     * key bit is skipped, so a null key is not published as key `0`.
     */
    def unmatchedBuild(matched: Array[Long]): Option[ColumnBatch] =
      require(
        matched.length == ((rows + 63) >>> 6),
        s"matched bitmap has ${matched.length} words for $rows build rows",
      )
      var clear = 0
      var row = 0
      while (row < rows) {
        if (presentUnmatched(row, matched)) then clear += 1
        row += 1
      }
      if (clear == 0) then None else Some(writeUnmatched(matched, clear))

    private def presentUnmatched(row: Int, matched: Array[Long]): Boolean =
      Validity.isSet(buildKeyWords, row) && !Validity.isSet(matched, row)

    private def slotOf(key: Long): Int =
      ColumnHash.mix(java.lang.Long.hashCode(key)) & mask

    private def writeRows(
        probeKeys: Int64Column,
        probeValues: Int64Column,
        preserveProbe: Boolean,
        matched: Option[Array[Long]],
    ): ColumnBatch =
      var outCapacity = math.max(16, probeKeys.length)
      var filled = 0
      var outKeys = new Array[Long](outCapacity)
      var outProbe = new Array[Long](outCapacity)
      var outBuild = new Array[Long](outCapacity)
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
          key: Long,
          keyPresent: Boolean,
          probeValue: Long,
          probePresent: Boolean,
          buildValue: Long,
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
          0L,
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
          var slot = slotOf(key)
          var resolved = false
          while (!resolved) {
            if (head(slot) == -1) then
              if (preserveProbe) then appendUnmatched(row)
              resolved = true
            else if (slotKey(slot) == key) then
              var buildRow = head(slot)
              while (buildRow != -1) {
                val buildPresent = Validity.isSet(buildValueWords, buildRow)
                append(
                  buildKeys(buildRow),
                  true,
                  probeValueValues(row),
                  Validity.isSet(probeValueWords, row),
                  if (buildPresent) then buildValues(buildRow) else 0L,
                  buildPresent,
                )
                // One plain store per emitted pair. `matched` is not shared.
                matched.foreach(bits => Validity.setBit(bits, buildRow))
                buildRow = next(buildRow)
              }
              resolved = true
            else slot = (slot + 1) & mask
          }
        row += 1
      }
      if (filled == 0) then ColumnBatches.emptyLongJoin
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
          secondAbsence = buildAbsence,
        )

    private def writeUnmatched(matched: Array[Long], clear: Int): ColumnBatch =
      val outKeys = new Array[Long](clear)
      val outProbe = new Array[Long](clear)
      val outBuild = new Array[Long](clear)
      val keyWords = Validity.allocate(clear)
      val probeWords = Validity.allocate(clear)
      val buildWords = Validity.allocate(clear)
      val probeAbsence = Validity.allocate(clear)
      var filled = 0
      var row = 0
      while (row < rows) {
        if (presentUnmatched(row, matched)) then
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

  private object LongBuild:

    def empty: LongBuild =
      new LongBuild(
        mask = 0,
        slotKey = Array.emptyLongArray,
        head = Array.emptyIntArray,
        next = Array.emptyIntArray,
        buildKeys = Array.emptyLongArray,
        buildKeyWords = Array.emptyLongArray,
        buildValues = Array.emptyLongArray,
        buildValueWords = Array.emptyLongArray,
        rows = 0,
      )

    def apply(flat: FlatBuild): LongBuild =
      val slotCapacity = ColumnHash.tableSize(flat.rows)
      val head = new Array[Int](slotCapacity)
      val next = new Array[Int](flat.rows)
      Arrays.fill(head, -1)
      Arrays.fill(next, -1)
      new LongBuild(
        mask = slotCapacity - 1,
        slotKey = new Array[Long](slotCapacity),
        head = head,
        next = next,
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
      sides: Vector[HashJoin.JoinSide],
  ): (Vector[HashJoin.JoinSide], Option[ColumnBatch]) =
    val columns = sides.map { side =>
      val keys = ColumnHash.int64At(side.batch, side.key)
      val values = ColumnHash.int64At(side.batch, side.value)
      require(keys.length == values.length, "build key and value columns differ in length")
      (keys, values)
    }
    val nullRows = columns.foldLeft(0L) { (total, column) =>
      total + clearCount(column._1.validity.words, column._1.length).toLong
    }
    require(nullRows <= Int.MaxValue.toLong, s"null build keys has $nullRows rows")
    if (nullRows == 0L) then (sides, None)
    else
      val presentRows =
        columns.foldLeft(0L)((total, column) => total + column._1.length.toLong) - nullRows
      require(presentRows <= Int.MaxValue.toLong, s"present build keys has $presentRows rows")
      val carried = copyBuildRows(columns, keepNullKeys = true, nullRows.toInt)
      val present: Vector[HashJoin.JoinSide] =
        if (presentRows == 0L) then Vector.empty
        else
          Vector(
            HashJoin.JoinSide(
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
    val keys = ColumnHash.int64At(batch, 0)
    val values = ColumnHash.int64At(batch, 1)
    require(keys.length == values.length, "build key and value columns differ in length")
    val count = keys.length
    if (count == 0) then ColumnBatches.emptyLongJoin
    else
      val outKeys = Arrays.copyOf(keys.values, count)
      val outProbe = new Array[Long](count)
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
  private[scarlet] def orMatched(bitsets: Array[Array[Long]]): Array[Long] =
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

  private def clearCount(words: Array[Long], live: Int): Int =
    var row = 0
    var count = 0
    while (row < live) {
      if (!Validity.isSet(words, row)) then count += 1
      row += 1
    }
    count

  private def copyBuildRows(
      columns: Vector[(Int64Column, Int64Column)],
      keepNullKeys: Boolean,
      total: Int,
  ): ColumnBatch =
    val keys = new Array[Long](total)
    val values = new Array[Long](total)
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
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.of(keys, Validity.fromWords(keyWords, total), total),
        Int64Column.of(values, Validity.fromWords(valueWords, total), total),
      ),
      total,
    )

  private def publish(
      keys: Array[Long],
      probeValues: Array[Long],
      buildValues: Array[Long],
      probeWords: Array[Long],
      buildWords: Array[Long],
      capacity: Int,
      rows: Int,
      keyWords: Array[Long],
      firstAbsence: Option[Array[Long]] = None,
      secondAbsence: Option[Array[Long]] = None,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.of(keys, Validity.fromWords(keyWords, capacity), rows),
        valueColumn(probeValues, probeWords, firstAbsence, capacity, rows),
        valueColumn(buildValues, buildWords, secondAbsence, capacity, rows),
      ),
      rows,
    )

  private def valueColumn(
      values: Array[Long],
      words: Array[Long],
      absence: Option[Array[Long]],
      capacity: Int,
      rows: Int,
  ): Int64Column =
    val validity = Validity.fromWords(words, capacity)
    absence match
      case Some(bits) if Validity.anySet(bits, rows) =>
        Int64Column.withAbsence(values, validity, Validity.fromWords(bits, capacity), rows)
      case _ => Int64Column.of(values, validity, rows)

  private def doubled(capacity: Int): Int =
    if (capacity >= ColumnHash.MaxCapacity) then
      throw new IllegalStateException(s"join output reached $capacity rows")
    capacity << 1

  private def growWords(words: Array[Long], rowCapacity: Int): Array[Long] =
    val needed = (rowCapacity + 63) >>> 6
    if (words.length == needed) words else Arrays.copyOf(words, needed)
