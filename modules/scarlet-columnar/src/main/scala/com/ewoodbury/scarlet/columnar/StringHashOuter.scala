package com.ewoodbury.scarlet.columnar

import java.util.{Arrays, HashMap}

/**
 * Outer hash join of dictionary string keys and `Int32` values.
 *
 * [[open]] fills one table from every build batch, then probes. The table matches strings, not
 * codes. `""` matches `""`. A null key is not inserted, so a stored code `0` does not match
 * `dictionary(0)`. `probe` with `preserveProbe` writes `(key, probe value, build value)` and keeps
 * a probe row with no partner, including a null probe key. The key column is the probe dictionary
 * and the probe codes. The build value's absence bit is set on an unmatched row and its validity
 * bit stays clear. A null value is not absence. [[probeFull]] records each matched build row in a
 * caller-owned bitmap. Null build keys are removed by [[separateNullBuildKeys]] before insert and
 * emitted once by [[emitNullBuildKeys]].
 *
 * The chain walk is newest-first. That walk is a kernel detail, not a public contract. A non-empty
 * build is estimated with [[StringHashJoinSpill.retainedBytes]] before the map or the chain is
 * allocated. Several batches sum that estimate and are not concatenated first. An estimate above
 * `buildBudgetBytes` throws `IllegalStateException`. The matched-row bitmap and the null-key copy
 * are not part of that budget. This kernel does not spill.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.MutableDataStructures",
  ),
)
object StringHashOuter:

  def open(sides: Vector[ColumnBatch], buildBudgetBytes: Long): StringBuild =
    val checked = sides.map(checkedSide)
    val totalLong = checked.iterator.map(side => side.keys.length.toLong).sum
    require(totalLong <= Int.MaxValue.toLong, s"build side has $totalLong rows")
    val total = totalLong.toInt
    if (total == 0) then StringBuild.empty
    else
      val estimate = checked.iterator.map { side =>
        val rows = side.keys.length
        if (rows <= 0) then 0L
        else
          StringHashJoinSpill.retainedBytes(
            rows,
            side.keys.dictionary.length,
            StringHashJoinSpill.dictionaryBytes(side.batch),
          )
      }.sum
      if (estimate > buildBudgetBytes) then refuseBuild(total, estimate, buildBudgetBytes)
      else
        val flat = copyBuild(checked, total)
        val table = StringBuild(flat)
        table.insert()
        table

  private def refuseBuild(rows: Int, estimate: Long, budget: Long): Nothing =
    throw new IllegalStateException(
      s"string shuffle-hash build piece retains about $estimate bytes for $rows rows, " +
        s"above the budget of $budget bytes",
    )

  private def checkedSide(batch: ColumnBatch): CheckedSide =
    val keys = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "build key and value columns differ in length")
    CheckedSide(batch, keys, values)

  private def copyBuild(sides: Vector[CheckedSide], total: Int): FlatBuild =
    val texts = new Array[String](total)
    val values = new Array[Int](total)
    val keyWords = Validity.allocate(total)
    val valueWords = Validity.allocate(total)
    var cursor = 0
    sides.foreach { side =>
      val count = side.keys.length
      val codes = side.keys.codes
      val dictionary = side.keys.dictionary
      val keyValidity = side.keys.validity.words
      System.arraycopy(side.values.values, 0, values, cursor, count)
      var row = 0
      while (row < count) {
        if (Validity.isSet(keyValidity, row)) then
          texts(cursor + row) = dictionary(codes(row))
          Validity.setBit(keyWords, cursor + row)
        if (Validity.isSet(side.values.validity.words, row)) then
          Validity.setBit(valueWords, cursor + row)
        row += 1
      }
      cursor += count
    }
    FlatBuild(total, texts, keyWords, values, valueWords)

  private final case class CheckedSide(
      batch: ColumnBatch,
      keys: DictUtf8Column,
      values: Int32Column,
  )

  private final case class FlatBuild(
      rows: Int,
      texts: Array[String],
      keyWords: Array[Long],
      values: Array[Int],
      valueWords: Array[Long],
  )

  /**
   * Build table shared by every probe. An empty build returns an empty batch when `preserveProbe`
   * is false, and emits every probe row with the build value absent when it is true.
   */
  final class StringBuild private (
      head: Array[Int],
      next: Array[Int],
      ids: HashMap[String, Integer],
      buildTexts: Array[String],
      buildKeyWords: Array[Long],
      buildValues: Array[Int],
      buildValueWords: Array[Long],
      rows: Int,
  ):

    private[StringHashOuter] def insert(): Unit =
      var row = 0
      while (row < rows) {
        // A null key stays out of the table. Its code may be 0, and inserting it would match
        // whatever string lives at dictionary slot 0.
        if (Validity.isSet(buildKeyWords, row)) then
          val id = idOf(buildTexts(row))
          next(row) = head(id)
          head(id) = row
        row += 1
      }

    /** Copied build rows. A null key the caller did not strip is included and then skipped. */
    def buildRows: Int = rows

    /**
     * `preserveProbe` appends each probe-side row with no partner. The build value's absence bit
     * is set on that row, and its validity bit stays clear. A null probe key does not match and is
     * kept only when `preserveProbe` is set. The key column is the probe dictionary.
     */
    def probe(batch: ColumnBatch, preserveProbe: Boolean): ColumnBatch =
      val probeKeys = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
      val probeValues = ColumnHash.int32At(batch, 1)
      require(
        probeKeys.length == probeValues.length,
        "probe key and value columns differ in length",
      )
      if (probeKeys.length == 0 || (rows == 0 && !preserveProbe)) then
        ColumnBatches.emptyStringJoin
      else writeRows(probeKeys, probeValues, preserveProbe, matched = None)

    /**
     * Full-outer probe. A probe row with no partner is kept, including a null probe key. Each
     * emitted match sets one bit in `matched`. `matched` is owned by this probe. A null probe key
     * sets no bit. Output is `(key, probe, build)`.
     */
    def probeFull(batch: ColumnBatch, matched: Array[Long]): ColumnBatch =
      require(
        matched.length == ((rows + 63) >>> 6),
        s"matched bitmap has ${matched.length} words for $rows build rows",
      )
      val probeKeys = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
      val probeValues = ColumnHash.int32At(batch, 1)
      require(
        probeKeys.length == probeValues.length,
        "probe key and value columns differ in length",
      )
      if (probeKeys.length == 0 || rows == 0) then probe(batch, preserveProbe = true)
      else writeRows(probeKeys, probeValues, preserveProbe = true, matched = Some(matched))

    /**
     * Present build rows whose bit is clear, in table order. Output is `(key, absent probe, build
     * value)`. `None` means every present build row was matched, or the table is empty. A clear
     * key bit is skipped, so a null key is not published as `dictionary(0)`.
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

    /** One id per distinct build string. A later row with the same string reuses that id. */
    private def idOf(text: String): Int =
      val existing = ids.get(text)
      if (existing eq null) then
        val assigned = ids.size
        ids.put(text, Integer.valueOf(assigned))
        assigned
      else existing.intValue()

    /** `-1` when the probe string was never built. One map lookup, and no insert. */
    private def lookup(text: String): Int =
      val existing = ids.get(text)
      if (existing eq null) then -1 else existing.intValue()

    private def writeRows(
        probeKeys: DictUtf8Column,
        probeValues: Int32Column,
        preserveProbe: Boolean,
        matched: Option[Array[Long]],
    ): ColumnBatch =
      var outCapacity = math.max(16, probeKeys.length)
      var filled = 0
      var outCodes = new Array[Int](outCapacity)
      var outProbe = new Array[Int](outCapacity)
      var outBuild = new Array[Int](outCapacity)
      var keyWords = Validity.allocate(outCapacity)
      var probeWords = Validity.allocate(outCapacity)
      var buildWords = Validity.allocate(outCapacity)
      var buildAbsence =
        if (preserveProbe) then Some(Validity.allocate(outCapacity)) else None
      val probeValueValues = probeValues.values
      val probeValueWords = probeValues.validity.words
      val probeCodes = probeKeys.codes
      val probeDictionary = probeKeys.dictionary
      val probeKeyWords = probeKeys.validity.words
      val probeRows = probeKeys.length

      def grow(): Unit =
        outCapacity = doubled(outCapacity)
        outCodes = Arrays.copyOf(outCodes, outCapacity)
        outProbe = Arrays.copyOf(outProbe, outCapacity)
        outBuild = Arrays.copyOf(outBuild, outCapacity)
        keyWords = growWords(keyWords, outCapacity)
        probeWords = growWords(probeWords, outCapacity)
        buildWords = growWords(buildWords, outCapacity)
        buildAbsence = buildAbsence.map(bits => growWords(bits, outCapacity))

      def append(
          code: Int,
          keyPresent: Boolean,
          probeValue: Int,
          probePresent: Boolean,
          buildValue: Int,
          buildPresent: Boolean,
      ): Unit =
        if (filled == outCapacity) then grow()
        outCodes(filled) = code
        if (keyPresent) then Validity.setBit(keyWords, filled)
        if (probePresent) then
          outProbe(filled) = probeValue
          Validity.setBit(probeWords, filled)
        if (buildPresent) then
          outBuild(filled) = buildValue
          Validity.setBit(buildWords, filled)
        filled += 1

      def appendUnmatched(probeRow: Int): Unit =
        val keyPresent = Validity.isSet(probeKeyWords, probeRow)
        append(
          if (keyPresent) then probeCodes(probeRow) else 0,
          keyPresent,
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
          val id = lookup(probeDictionary(probeCodes(row)))
          if (id < 0) then
            if (preserveProbe) then appendUnmatched(row)
          else
            var buildRow = head(id)
            while (buildRow != -1) {
              val buildPresent = Validity.isSet(buildValueWords, buildRow)
              append(
                probeCodes(row),
                true,
                probeValueValues(row),
                Validity.isSet(probeValueWords, row),
                if (buildPresent) then buildValues(buildRow) else 0,
                buildPresent,
              )
              matched.foreach(bits => Validity.setBit(bits, buildRow))
              buildRow = next(buildRow)
            }
        row += 1
      }
      if (filled == 0) then ColumnBatches.emptyStringJoin
      else
        publishProbe(
          probeDictionary,
          outCodes,
          outProbe,
          outBuild,
          probeWords,
          buildWords,
          outCapacity,
          filled,
          keyWords,
          buildAbsence,
        )

    private def writeUnmatched(matched: Array[Long], clear: Int): ColumnBatch =
      val words = DictUtf8Column.Words.empty
      val outCodes = new Array[Int](clear)
      val outProbe = new Array[Int](clear)
      val outBuild = new Array[Int](clear)
      val keyWords = Validity.allocate(clear)
      val probeWords = Validity.allocate(clear)
      val buildWords = Validity.allocate(clear)
      val probeAbsence = Validity.allocate(clear)
      var filled = 0
      var row = 0
      while (row < rows) {
        if (presentUnmatched(row, matched)) then
          outCodes(filled) = words.intern(buildTexts(row))
          Validity.setBit(keyWords, filled)
          if (Validity.isSet(buildValueWords, row)) then
            outBuild(filled) = buildValues(row)
            Validity.setBit(buildWords, filled)
          Validity.setBit(probeAbsence, filled)
          filled += 1
        row += 1
      }
      publishFresh(
        words.toArray,
        outCodes,
        outProbe,
        outBuild,
        probeWords,
        buildWords,
        clear,
        filled,
        keyWords,
        firstAbsence = Some(probeAbsence),
      )

  private object StringBuild:

    def empty: StringBuild =
      new StringBuild(
        head = Array.emptyIntArray,
        next = Array.emptyIntArray,
        ids = null,
        buildTexts = Array.empty[String],
        buildKeyWords = Array.emptyLongArray,
        buildValues = Array.emptyIntArray,
        buildValueWords = Array.emptyLongArray,
        rows = 0,
      )

    def apply(flat: FlatBuild): StringBuild =
      val head = new Array[Int](flat.rows)
      val next = new Array[Int](flat.rows)
      Arrays.fill(head, -1)
      Arrays.fill(next, -1)
      new StringBuild(
        head = head,
        next = next,
        ids = new HashMap[String, Integer](math.max(1, math.min(flat.rows, 1024))),
        buildTexts = flat.texts,
        buildKeyWords = flat.keyWords,
        buildValues = flat.values,
        buildValueWords = flat.valueWords,
        rows = flat.rows,
      )

  /**
   * Pulls null-key build rows out of `sides`. Insert skips a null key, so it has no chain and no
   * matched-row bit. `None` means every live key is present, and `sides` is returned unchanged.
   * Otherwise the present-key rows are one batch and the null-key rows are carried beside it, in
   * input order. A clear bit is copied and is not used as a dictionary index.
   */
  private[scarlet] def separateNullBuildKeys(
      sides: Vector[ColumnBatch],
  ): (Vector[ColumnBatch], Option[ColumnBatch]) =
    if (sides.isEmpty) then (sides, None)
    else
      val columns = sides.map(checkedSide)
      val nullRows = columns.foldLeft(0L) { (total, side) =>
        total + clearCount(side.keys.validity.words, side.keys.length).toLong
      }
      require(nullRows <= Int.MaxValue.toLong, s"null build keys has $nullRows rows")
      if (nullRows == 0L) then (sides, None)
      else
        val presentPieces = columns.flatMap { side =>
          val present = side.keys.length - clearCount(side.keys.validity.words, side.keys.length)
          if (present == 0) then None
          else Some(takeRows(side.keys, side.values, keepNull = false, present))
        }
        val nullPieces = columns.flatMap { side =>
          val cleared = clearCount(side.keys.validity.words, side.keys.length)
          if (cleared == 0) then None
          else Some(takeRows(side.keys, side.values, keepNull = true, cleared))
        }
        val present =
          if (presentPieces.isEmpty) then Vector.empty
          else if (presentPieces.length == 1) then presentPieces
          else Vector(ColumnBatches.concat(presentPieces))
        (present, Some(combine(nullPieces)))

  /**
   * One `(key, absent probe, build value)` row per null-key build row. The key bit stays clear.
   * The stored code may be `0`. The probe value's absence bit is set and its validity bit stays
   * clear. The build value bit is copied. The dictionary is the source dictionary.
   */
  private[scarlet] def emitNullBuildKeys(batch: ColumnBatch): ColumnBatch =
    val keys = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "build key and value columns differ in length")
    val count = keys.length
    if (count == 0) then ColumnBatches.emptyStringJoin
    else
      val outCodes = Arrays.copyOf(keys.codes, count)
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
      publishFresh(
        keys.dictionary,
        outCodes,
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

  private def takeRows(
      keys: DictUtf8Column,
      values: Int32Column,
      keepNull: Boolean,
      total: Int,
  ): ColumnBatch =
    val outCodes = new Array[Int](total)
    val outValues = new Array[Int](total)
    val keyWords = Validity.allocate(total)
    val valueWords = Validity.allocate(total)
    val codes = keys.codes
    val keyValidity = keys.validity.words
    val valueValidity = values.validity.words
    val valueValues = values.values
    var cursor = 0
    var row = 0
    while (row < keys.length) {
      val keyPresent = Validity.isSet(keyValidity, row)
      if (keyPresent != keepNull) then
        outCodes(cursor) = codes(row)
        outValues(cursor) = valueValues(row)
        if (keyPresent) then Validity.setBit(keyWords, cursor)
        if (Validity.isSet(valueValidity, row)) then Validity.setBit(valueWords, cursor)
        cursor += 1
      row += 1
    }
    require(cursor == total, s"copied $cursor build rows, expected $total")
    new ColumnBatch(
      pairSchema,
      Vector(
        DictUtf8Column.of(keys.dictionary, outCodes, Validity.fromWords(keyWords, total), total),
        Int32Column.of(outValues, Validity.fromWords(valueWords, total), total),
      ),
      total,
    )

  private def combine(pieces: Vector[ColumnBatch]): ColumnBatch =
    if (pieces.length == 1) then pieces(0)
    else ColumnBatches.concat(pieces)

  private def publishProbe(
      dictionary: Array[String],
      codes: Array[Int],
      probeValues: Array[Int],
      buildValues: Array[Int],
      probeWords: Array[Long],
      buildWords: Array[Long],
      capacity: Int,
      rows: Int,
      keyWords: Array[Long],
      buildAbsence: Option[Array[Long]],
  ): ColumnBatch =
    publishFresh(
      dictionary,
      codes,
      probeValues,
      buildValues,
      probeWords,
      buildWords,
      capacity,
      rows,
      keyWords,
      secondAbsence = buildAbsence,
    )

  private def publishFresh(
      dictionary: Array[String],
      codes: Array[Int],
      probeValues: Array[Int],
      buildValues: Array[Int],
      probeWords: Array[Long],
      buildWords: Array[Long],
      capacity: Int,
      rows: Int,
      keyWords: Array[Long],
      firstAbsence: Option[Array[Long]] = None,
      secondAbsence: Option[Array[Long]] = None,
  ): ColumnBatch =
    new ColumnBatch(
      tripleSchema,
      Vector(
        DictUtf8Column.of(dictionary, codes, Validity.fromWords(keyWords, capacity), rows),
        valueColumn(probeValues, probeWords, firstAbsence, capacity, rows),
        valueColumn(buildValues, buildWords, secondAbsence, capacity, rows),
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

  private def growWords(words: Array[Long], rowCapacity: Int): Array[Long] =
    val needed = (rowCapacity + 63) >>> 6
    if (words.length == needed) words else Arrays.copyOf(words, needed)

  private val pairSchema: Vector[LogicalType] =
    Vector(LogicalType.Utf8Dict, LogicalType.Int32)

  private val tripleSchema: Vector[LogicalType] =
    Vector(LogicalType.Utf8Dict, LogicalType.Int32, LogicalType.Int32)
