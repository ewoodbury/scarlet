package com.ewoodbury.scarlet.columnar

/**
 * Filter and project over columns.
 *
 * Filter builds a selection of rows whose predicate column passes the predicate. For ints, longs,
 * floats, and bools, nulls are dropped and the predicate is not called. A string filter calls the
 * predicate on every row, including an empty cell, and an empty cell may survive. Survivors stay
 * in input order. Every column is compacted. The output length is the survivor count. A
 * whole-batch scan sizes its output to the input live length. A range scan reads
 * `[start, start + length)`; the filtered column holds its survivors, and a multi-column batch
 * records absolute row indexes so the other columns compact from the source batch. A string filter
 * keeps the input dictionary.
 *
 * Project maps one column and shares the others. For the primitive columns, a null stays null, the
 * new slot stores `0`, and the function is not called. A whole-column project reuses the input
 * validity bitmap, so null positions do not change. A long or float project over
 * `[start, start + length)` reads validity at those absolute rows and writes `length` rows from
 * index 0. The whole-column entry is that scan with start 0 and the batch length. A string project
 * calls the function on every row of that range, including an empty cell, stores an empty result
 * as an empty cell, and builds a new dictionary. Callers do not mutate a published bitmap.
 *
 * Primitive loops keep the row index in a `var` so the bodies stay allocation-free. Each primitive
 * has its own loop so the copy stays monomorphic. Fused runs of two to four filters and projects
 * are [[Int32Pipeline]], [[Int64Pipeline]], [[Float64Pipeline]], and [[Utf8Pipeline]].
 *
 * [[countByValues]] resolves the same literal mask as [[filterUtf8ByValues]] and counts codes in
 * place. [[countByGroups]] intersects one mask per literal group and counts the same way. Neither
 * builds a selection or publishes a column.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object ColumnKernel:

  trait Int32Predicate:
    def apply(value: Int): Boolean

  trait Int64Predicate:
    def apply(value: Long): Boolean

  trait Float64Predicate:
    def apply(value: Double): Boolean

  trait BoolPredicate:
    def apply(value: Boolean): Boolean

  trait Int32Map:
    def apply(value: Int): Int

  trait Int64Map:
    def apply(value: Long): Long

  trait Float64Map:
    def apply(value: Double): Double

  trait BoolMap:
    def apply(value: Boolean): Boolean

  trait Utf8Predicate:
    def apply(value: String): Boolean

  trait Utf8Map:
    def apply(value: String): String

  def filterInt32(batch: ColumnBatch, ordinal: Int, predicate: Int32Predicate): ColumnBatch =
    filterInt32(batch, ordinal, 0, batch.length, predicate)

  /** Scans `[start, start + length)`. A multi-column batch records absolute row indexes. */
  def filterInt32(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: Int32Predicate,
  ): ColumnBatch =
    val column = asInt32(columnAt(batch, ordinal), ordinal)
    requireRange(start, length, column.length)
    if (batch.columns.length == 1) then
      val scanned =
        scanInt32(column, start, length, predicate, record = false, selection = noSelection)
      publish(batch, scanned.column, scanned.count)
    else
      val selection = new Array[Int](length)
      val scanned =
        scanInt32(column, start, length, predicate, record = true, selection = selection)
      assemble(batch, ordinal, scanned.column, selection, scanned.count)

  def filterInt64(batch: ColumnBatch, ordinal: Int, predicate: Int64Predicate): ColumnBatch =
    filterInt64(batch, ordinal, 0, batch.length, predicate)

  /** Scans `[start, start + length)`. A multi-column batch records absolute row indexes. */
  def filterInt64(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: Int64Predicate,
  ): ColumnBatch =
    val column = asInt64(columnAt(batch, ordinal), ordinal)
    requireRange(start, length, column.length)
    if (batch.columns.length == 1) then
      val scanned =
        scanInt64(column, start, length, predicate, record = false, selection = noSelection)
      publish(batch, scanned.column, scanned.count)
    else
      val selection = new Array[Int](length)
      val scanned =
        scanInt64(column, start, length, predicate, record = true, selection = selection)
      assemble(batch, ordinal, scanned.column, selection, scanned.count)

  def filterFloat64(
      batch: ColumnBatch,
      ordinal: Int,
      predicate: Float64Predicate,
  ): ColumnBatch =
    filterFloat64(batch, ordinal, 0, batch.length, predicate)

  /** Scans `[start, start + length)`. A multi-column batch records absolute row indexes. */
  def filterFloat64(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: Float64Predicate,
  ): ColumnBatch =
    val column = asFloat64(columnAt(batch, ordinal), ordinal)
    requireRange(start, length, column.length)
    if (batch.columns.length == 1) then
      val scanned =
        scanFloat64(column, start, length, predicate, record = false, selection = noSelection)
      publish(batch, scanned.column, scanned.count)
    else
      val selection = new Array[Int](length)
      val scanned =
        scanFloat64(column, start, length, predicate, record = true, selection = selection)
      assemble(batch, ordinal, scanned.column, selection, scanned.count)

  def filterUtf8(batch: ColumnBatch, ordinal: Int, predicate: Utf8Predicate): ColumnBatch =
    filterUtf8(batch, ordinal, 0, batch.length, predicate)

  /** Scans `[start, start + length)`. A multi-column batch records absolute row indexes. */
  def filterUtf8(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: Utf8Predicate,
  ): ColumnBatch =
    val column = asUtf8(columnAt(batch, ordinal), ordinal)
    requireRange(start, length, column.length)
    if (batch.columns.length == 1) then
      val scanned =
        scanUtf8(column, start, length, predicate, record = false, selection = noSelection)
      publish(batch, scanned.column, scanned.count)
    else
      val selection = new Array[Int](length)
      val scanned =
        scanUtf8(column, start, length, predicate, record = true, selection = selection)
      assemble(batch, ordinal, scanned.column, selection, scanned.count)

  /**
   * Resolves literals against this batch dictionary once, then filters codes with an unboxed mask.
   * A one-column batch publishes the filtered dictionary column directly. The dictionary is
   * shared.
   */
  def filterUtf8ByValues(
      batch: ColumnBatch,
      ordinal: Int,
      values: Iterable[String],
  ): ColumnBatch =
    filterUtf8ByValues(batch, ordinal, 0, batch.length, values)

  /**
   * Scans codes on `[start, start + length)`. The mask is built from the whole dictionary, as in
   * the whole-batch overload. Survivors keep that dictionary, and a multi-column batch records
   * absolute row indexes.
   */
  def filterUtf8ByValues(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      values: Iterable[String],
  ): ColumnBatch =
    val column = asUtf8(columnAt(batch, ordinal), ordinal)
    requireRange(start, length, column.length)
    val mask = literalCodes(column, values)
    val keep: Int32Predicate = code => selected(mask, code)
    val codeColumn = Int32Column.of(column.codes, column.validity, column.length)
    if (batch.columns.length == 1) then
      val scanned =
        scanInt32(codeColumn, start, length, keep, record = false, selection = noSelection)
      publish(batch, filteredUtf8(column, scanned, ordinal), scanned.count)
    else
      val selection = new Array[Int](length)
      val scanned =
        scanInt32(codeColumn, start, length, keep, record = true, selection = selection)
      assemble(batch, ordinal, filteredUtf8(column, scanned, ordinal), selection, scanned.count)

  /**
   * Counts rows whose code is selected by `values`. The mask matches [[filterUtf8ByValues]]. A
   * null slot is skipped. A code of `-1`, or any other code outside the dictionary, is not
   * counted. A literal that is not in the dictionary adds no hit.
   */
  def countByValues(batch: ColumnBatch, ordinal: Int, values: Iterable[String]): Long =
    countByGroups(batch, ordinal, Seq(values))

  /**
   * Counts rows selected by every literal group. The masks are intersected, so group order does
   * not change the count. An empty group selects nothing. An empty sequence of groups selects
   * nothing. The scan stays on the caller thread.
   */
  private[scarlet] def countByGroups(
      batch: ColumnBatch,
      ordinal: Int,
      groups: Seq[Iterable[String]],
  ): Long =
    if (groups.isEmpty) then 0L
    else
      val column = asUtf8(columnAt(batch, ordinal), ordinal)
      val mask = intersectedCodes(column, groups)
      if (selectsAny(mask)) then countMasked(column, mask) else 0L

  private def literalCodes(column: DictUtf8Column, values: Iterable[String]): Array[Boolean] =
    val mask = new Array[Boolean](column.dictionary.length)
    values.iterator.foreach { value =>
      val code = column.codeOf(value)
      if (code >= 0) then mask(code) = true
    }
    mask

  /**
   * Intersects one mask per group. An empty sequence selects nothing. When `groups` is non-empty,
   * the first mask is reused as the accumulator and is caller-local: nothing else retains it.
   */
  private def intersectedCodes(
      column: DictUtf8Column,
      groups: Seq[Iterable[String]],
  ): Array[Boolean] =
    groups.headOption match
      case None => Array.emptyBooleanArray
      case Some(first) =>
        val mask = literalCodes(column, first)
        groups.drop(1).foreach(group => intersectInto(mask, literalCodes(column, group)))
        mask

  /** Clears bits in `mask` that `next` does not set. `mask` is the caller-local accumulator. */
  private def intersectInto(mask: Array[Boolean], next: Array[Boolean]): Unit =
    var code = 0
    while (code < mask.length) {
      if (mask(code) && !next(code)) then mask(code) = false
      code += 1
    }

  /** A code of `-1`, or any code outside the dictionary, is not selected. */
  private inline def selected(mask: Array[Boolean], code: Int): Boolean =
    code >= 0 && code < mask.length && mask(code)

  private def selectsAny(mask: Array[Boolean]): Boolean =
    var index = 0
    var found = false
    while (index < mask.length && !found) {
      found = mask(index)
      index += 1
    }
    found

  private def countMasked(column: DictUtf8Column, mask: Array[Boolean]): Long =
    val live = column.length
    if (live == 0) then 0L
    else countRange(column, mask, Validity.livePresent(column.validity, live), 0, live)

  private def countRange(
      column: DictUtf8Column,
      mask: Array[Boolean],
      dense: Boolean,
      start: Int,
      length: Int,
  ): Long =
    val codes = column.codes
    val end = start + length
    var total = 0L
    var row = start
    if (dense) then
      while (row < end) {
        val code = codes(row)
        if (selected(mask, code)) then total += 1L
        row += 1
      }
    else
      val words = column.validity.words
      while (row < end) {
        if (Validity.isSet(words, row)) then
          val code = codes(row)
          if (selected(mask, code)) then total += 1L
        row += 1
      }
    total

  private def filteredUtf8(
      column: DictUtf8Column,
      scanned: Scanned,
      ordinal: Int,
  ): DictUtf8Column =
    val filteredCodes = asInt32(scanned.column, ordinal)
    DictUtf8Column.of(
      column.dictionary,
      filteredCodes.values,
      filteredCodes.validity,
      scanned.count,
    )

  def filterBool(batch: ColumnBatch, ordinal: Int, predicate: BoolPredicate): ColumnBatch =
    filterBool(batch, ordinal, 0, batch.length, predicate)

  /** Scans `[start, start + length)`. A multi-column batch records absolute row indexes. */
  def filterBool(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: BoolPredicate,
  ): ColumnBatch =
    val column = asBool(columnAt(batch, ordinal), ordinal)
    requireRange(start, length, column.length)
    if (batch.columns.length == 1) then
      val scanned =
        scanBool(column, start, length, predicate, record = false, selection = noSelection)
      publish(batch, scanned.column, scanned.count)
    else
      val selection = new Array[Int](length)
      val scanned =
        scanBool(column, start, length, predicate, record = true, selection = selection)
      assemble(batch, ordinal, scanned.column, selection, scanned.count)

  def projectInt32(batch: ColumnBatch, ordinal: Int, mapper: Int32Map): ColumnBatch =
    replace(batch, ordinal, mapInt32(asInt32(columnAt(batch, ordinal), ordinal), mapper))

  def projectInt64(batch: ColumnBatch, ordinal: Int, mapper: Int64Map): ColumnBatch =
    projectInt64(batch, ordinal, 0, batch.length, mapper)

  /**
   * Maps `[start, start + length)`. Output row 0 is source row `start`. The output length is
   * `length`, including nulls. A null stays null and the mapper is not called. The whole-column
   * scan still reuses the input validity bitmap.
   */
  def projectInt64(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      mapper: Int64Map,
  ): ColumnBatch =
    val column = asInt64(columnAt(batch, ordinal), ordinal)
    requireRange(start, length, column.length)
    val mapped =
      if (start == 0 && length == column.length) then mapInt64(column, mapper)
      else mapInt64Range(column, start, length, mapper)
    if (batch.columns.length == 1) then publish(batch, mapped, mapped.length)
    else if (start == 0 && length == batch.length) then replace(batch, ordinal, mapped)
    else
      val selection = Array.tabulate(length)(offset => start + offset)
      assemble(batch, ordinal, mapped, selection, length)

  def projectFloat64(batch: ColumnBatch, ordinal: Int, mapper: Float64Map): ColumnBatch =
    projectFloat64(batch, ordinal, 0, batch.length, mapper)

  /**
   * Maps `[start, start + length)`. Output row 0 is source row `start`. The output length is
   * `length`, including nulls. A null stays null and the mapper is not called. The whole-column
   * scan still reuses the input validity bitmap. NaN and `-0.0` are copied as bits.
   */
  def projectFloat64(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      mapper: Float64Map,
  ): ColumnBatch =
    val column = asFloat64(columnAt(batch, ordinal), ordinal)
    requireRange(start, length, column.length)
    val mapped =
      if (start == 0 && length == column.length) then mapFloat64(column, mapper)
      else mapFloat64Range(column, start, length, mapper)
    if (batch.columns.length == 1) then publish(batch, mapped, mapped.length)
    else if (start == 0 && length == batch.length) then replace(batch, ordinal, mapped)
    else
      val selection = Array.tabulate(length)(offset => start + offset)
      assemble(batch, ordinal, mapped, selection, length)

  def projectBool(batch: ColumnBatch, ordinal: Int, mapper: BoolMap): ColumnBatch =
    replace(batch, ordinal, mapBool(asBool(columnAt(batch, ordinal), ordinal), mapper))

  def projectUtf8(batch: ColumnBatch, ordinal: Int, mapper: Utf8Map): ColumnBatch =
    projectUtf8(batch, ordinal, 0, batch.length, mapper)

  /**
   * Maps `[start, start + length)`. Output row 0 is source row `start`. The output length is
   * `length`. The mapper runs on every row in the range, including an empty cell. An empty result
   * stays an empty cell. The dictionary is new. The whole-column scan builds that dictionary the
   * same way.
   */
  def projectUtf8(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      mapper: Utf8Map,
  ): ColumnBatch =
    val column = asUtf8(columnAt(batch, ordinal), ordinal)
    requireRange(start, length, column.length)
    val mapped =
      if (start == 0 && length == column.length) then mapUtf8(column, mapper)
      else mapUtf8Range(column, start, length, mapper)
    if (batch.columns.length == 1) then publish(batch, mapped, mapped.length)
    else if (start == 0 && length == batch.length) then replace(batch, ordinal, mapped)
    else
      val selection = Array.tabulate(length)(offset => start + offset)
      assemble(batch, ordinal, mapped, selection, length)

  private val noSelection: Array[Int] = Array.emptyIntArray

  private final case class Scanned(column: Column, count: Int)

  private def requireRange(start: Int, length: Int, rows: Int): Unit =
    require(start >= 0, s"range start must be >= 0, got $start")
    require(length >= 0, s"filter length must be >= 0, got $length")
    require(
      start.toLong + length.toLong <= rows.toLong,
      s"range [$start, ${start.toLong + length.toLong}) outside column length $rows",
    )

  private inline def scanInt32(
      column: Int32Column,
      start: Int,
      length: Int,
      predicate: Int32Predicate,
      inline record: Boolean,
      selection: Array[Int],
  ): Scanned =
    val in = column.values
    val words = column.validity.words
    val out = new Array[Int](length)
    val end = start + length
    var written = 0
    var row = start
    while (row < end) {
      if (Validity.isSet(words, row)) then
        val value = in(row)
        if (predicate(value)) then
          inline if (record) then selection(written) = row
          out(written) = value
          written += 1
      row += 1
    }
    val built = Int32Column.of(out, Validity.prefixValid(length, written), written)
    Scanned(built, written)

  private inline def scanInt64(
      column: Int64Column,
      start: Int,
      length: Int,
      predicate: Int64Predicate,
      inline record: Boolean,
      selection: Array[Int],
  ): Scanned =
    val in = column.values
    val words = column.validity.words
    val out = new Array[Long](length)
    val end = start + length
    var written = 0
    var row = start
    while (row < end) {
      if (Validity.isSet(words, row)) then
        val value = in(row)
        if (predicate(value)) then
          inline if (record) then selection(written) = row
          out(written) = value
          written += 1
      row += 1
    }
    val built = Int64Column.of(out, Validity.prefixValid(length, written), written)
    Scanned(built, written)

  private inline def scanFloat64(
      column: Float64Column,
      start: Int,
      length: Int,
      predicate: Float64Predicate,
      inline record: Boolean,
      selection: Array[Int],
  ): Scanned =
    val in = column.values
    val words = column.validity.words
    val out = new Array[Double](length)
    val end = start + length
    var written = 0
    var row = start
    while (row < end) {
      if (Validity.isSet(words, row)) then
        val value = in(row)
        if (predicate(value)) then
          inline if (record) then selection(written) = row
          out(written) = value
          written += 1
      row += 1
    }
    val built = Float64Column.of(out, Validity.prefixValid(length, written), written)
    Scanned(built, written)

  private inline def scanBool(
      column: BoolColumn,
      start: Int,
      length: Int,
      predicate: BoolPredicate,
      inline record: Boolean,
      selection: Array[Int],
  ): Scanned =
    val in = column.values
    val words = column.validity.words
    val out = new Array[Byte](length)
    val end = start + length
    var written = 0
    var row = start
    while (row < end) {
      if (Validity.isSet(words, row)) then
        val value = in(row) != 0
        if (predicate(value)) then
          inline if (record) then selection(written) = row
          out(written) = if (value) 1.toByte else 0.toByte
          written += 1
      row += 1
    }
    val built = BoolColumn.of(out, Validity.prefixValid(length, written), written)
    Scanned(built, written)

  private inline def scanUtf8(
      column: DictUtf8Column,
      start: Int,
      length: Int,
      predicate: Utf8Predicate,
      inline record: Boolean,
      selection: Array[Int],
  ): Scanned =
    val in = column.codes
    val words = column.validity.words
    val out = new Array[Int](length)
    val dstWords = Validity.allocate(length)
    val end = start + length
    var written = 0
    var row = start
    while (row < end) {
      if (predicate(utf8String(column, row))) then
        inline if (record) then selection(written) = row
        if (Validity.isSet(words, row)) then
          out(written) = in(row)
          Validity.setBit(dstWords, written)
        written += 1
      row += 1
    }
    val built = DictUtf8Column.of(
      column.dictionary,
      out,
      Validity.fromWords(dstWords, length),
      written,
    )
    Scanned(built, written)

  private def mapInt32(column: Int32Column, mapper: Int32Map): Int32Column =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Int](in.length)
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then out(row) = mapper(in(row))
      row += 1
    }
    Int32Column.of(out, column.validity, live)

  private def mapInt64(column: Int64Column, mapper: Int64Map): Int64Column =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Long](in.length)
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then out(row) = mapper(in(row))
      row += 1
    }
    Int64Column.of(out, column.validity, live)

  /** Writes the range at indexes `[0, length)`. Null slots stay 0 and are left invalid. */
  private def mapInt64Range(
      column: Int64Column,
      start: Int,
      length: Int,
      mapper: Int64Map,
  ): Int64Column =
    val in = column.values
    val words = column.validity.words
    val out = new Array[Long](length)
    val dstWords = Validity.allocate(length)
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (Validity.isSet(words, row)) then
        out(written) = mapper(in(row))
        Validity.setBit(dstWords, written)
      else out(written) = 0
      written += 1
      row += 1
    }
    Int64Column.of(out, Validity.fromWords(dstWords, length), length)

  private def mapFloat64(column: Float64Column, mapper: Float64Map): Float64Column =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Double](in.length)
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then out(row) = mapper(in(row))
      row += 1
    }
    Float64Column.of(out, column.validity, live)

  /** Writes the range at indexes `[0, length)`. Null slots stay 0 and are left invalid. */
  private def mapFloat64Range(
      column: Float64Column,
      start: Int,
      length: Int,
      mapper: Float64Map,
  ): Float64Column =
    val in = column.values
    val words = column.validity.words
    val out = new Array[Double](length)
    val dstWords = Validity.allocate(length)
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (Validity.isSet(words, row)) then
        out(written) = mapper(in(row))
        Validity.setBit(dstWords, written)
      written += 1
      row += 1
    }
    Float64Column.of(out, Validity.fromWords(dstWords, length), length)

  private def mapBool(column: BoolColumn, mapper: BoolMap): BoolColumn =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Byte](in.length)
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        out(row) = if (mapper(in(row) != 0)) 1.toByte else 0.toByte
      row += 1
    }
    BoolColumn.of(out, column.validity, live)

  private def mapUtf8(column: DictUtf8Column, mapper: Utf8Map): DictUtf8Column =
    val live = column.length
    val capacity = column.codes.length
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    val words = DictUtf8Column.Words.empty
    var row = 0
    while (row < live) {
      val text = mapper(utf8String(column, row))
      if (!BatchCodec.isAbsent(text)) then
        out(row) = words.intern(text)
        Validity.setBit(dstWords, row)
      row += 1
    }
    DictUtf8Column.of(words.toArray, out, Validity.fromWords(dstWords, capacity), live)

  /** Writes the range at indexes `[0, length)`. An empty cell is passed to the mapper. */
  private def mapUtf8Range(
      column: DictUtf8Column,
      start: Int,
      length: Int,
      mapper: Utf8Map,
  ): DictUtf8Column =
    val out = new Array[Int](length)
    val dstWords = Validity.allocate(length)
    val words = DictUtf8Column.Words.empty
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      val text = mapper(utf8String(column, row))
      if (!BatchCodec.isAbsent(text)) then
        out(written) = words.intern(text)
        Validity.setBit(dstWords, written)
      written += 1
      row += 1
    }
    DictUtf8Column.of(words.toArray, out, Validity.fromWords(dstWords, length), length)

  /** An empty cell does not index the dictionary. */
  private[columnar] def utf8String(column: DictUtf8Column, row: Int): String =
    if (Validity.isSet(column.validity.words, row)) then column.dictionary(column.codes(row))
    else BatchCodec.stringElement(None)

  private def assemble(
      batch: ColumnBatch,
      ordinal: Int,
      predicateColumn: Column,
      selection: Array[Int],
      count: Int,
  ): ColumnBatch =
    require(selection.length >= count, s"selection ${selection.length} shorter than $count rows")
    val capacity = batch.length
    val columns = Vector.tabulate(batch.columns.length) { index =>
      if (index == ordinal) then predicateColumn
      else compact(columnAt(batch, index), selection, count, capacity)
    }
    new ColumnBatch(batch.schema, columns, count)

  private def compact(
      column: Column,
      selection: Array[Int],
      count: Int,
      capacity: Int,
  ): Column =
    column match
      case int32: Int32Column => compactInt32(int32, selection, count, capacity)
      case int64: Int64Column => compactInt64(int64, selection, count, capacity)
      case float64: Float64Column => compactFloat64(float64, selection, count, capacity)
      case bool: BoolColumn => compactBool(bool, selection, count, capacity)
      case utf8: DictUtf8Column => compactUtf8(utf8, selection, count, capacity)

  private def compactInt32(
      column: Int32Column,
      selection: Array[Int],
      count: Int,
      capacity: Int,
  ): Int32Column =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    var index = 0
    while (index < count) {
      val row = selection(index)
      if (Validity.isSet(srcWords, row)) then
        out(index) = in(row)
        Validity.setBit(dstWords, index)
      index += 1
    }
    Int32Column.of(out, Validity.fromWords(dstWords, capacity), count)

  private def compactInt64(
      column: Int64Column,
      selection: Array[Int],
      count: Int,
      capacity: Int,
  ): Int64Column =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Long](capacity)
    val dstWords = Validity.allocate(capacity)
    var index = 0
    while (index < count) {
      val row = selection(index)
      if (Validity.isSet(srcWords, row)) then
        out(index) = in(row)
        Validity.setBit(dstWords, index)
      index += 1
    }
    Int64Column.of(out, Validity.fromWords(dstWords, capacity), count)

  private def compactFloat64(
      column: Float64Column,
      selection: Array[Int],
      count: Int,
      capacity: Int,
  ): Float64Column =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Double](capacity)
    val dstWords = Validity.allocate(capacity)
    var index = 0
    while (index < count) {
      val row = selection(index)
      if (Validity.isSet(srcWords, row)) then
        out(index) = in(row)
        Validity.setBit(dstWords, index)
      index += 1
    }
    Float64Column.of(out, Validity.fromWords(dstWords, capacity), count)

  private def compactBool(
      column: BoolColumn,
      selection: Array[Int],
      count: Int,
      capacity: Int,
  ): BoolColumn =
    val in = column.values
    val srcWords = column.validity.words
    val out = new Array[Byte](capacity)
    val dstWords = Validity.allocate(capacity)
    var index = 0
    while (index < count) {
      val row = selection(index)
      if (Validity.isSet(srcWords, row)) then
        out(index) = if (in(row) != 0) 1.toByte else 0.toByte
        Validity.setBit(dstWords, index)
      index += 1
    }
    BoolColumn.of(out, Validity.fromWords(dstWords, capacity), count)

  private def compactUtf8(
      column: DictUtf8Column,
      selection: Array[Int],
      count: Int,
      capacity: Int,
  ): DictUtf8Column =
    val in = column.codes
    val srcWords = column.validity.words
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    var index = 0
    while (index < count) {
      val row = selection(index)
      if (Validity.isSet(srcWords, row)) then
        out(index) = in(row)
        Validity.setBit(dstWords, index)
      index += 1
    }
    DictUtf8Column.of(column.dictionary, out, Validity.fromWords(dstWords, capacity), count)

  private def publish(batch: ColumnBatch, column: Column, count: Int): ColumnBatch =
    new ColumnBatch(batch.schema, Vector(column), count)

  private def replace(batch: ColumnBatch, ordinal: Int, column: Column): ColumnBatch =
    val columns = Vector.tabulate(batch.columns.length) { index =>
      if (index == ordinal) then column else columnAt(batch, index)
    }
    new ColumnBatch(batch.schema, columns, batch.length)

  private[columnar] def columnAt(batch: ColumnBatch, ordinal: Int): Column =
    batch.columns.lift(ordinal).getOrElse {
      throw new IllegalArgumentException(
        s"ordinal $ordinal outside width ${batch.columns.length}",
      )
    }

  private[columnar] def asInt32(column: Column, ordinal: Int): Int32Column =
    column match
      case int32: Int32Column => int32
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Int32",
        )

  private[columnar] def asInt64(column: Column, ordinal: Int): Int64Column =
    column match
      case int64: Int64Column => int64
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Int64",
        )

  private[columnar] def asFloat64(column: Column, ordinal: Int): Float64Column =
    column match
      case float64: Float64Column => float64
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Float64",
        )

  private def asBool(column: Column, ordinal: Int): BoolColumn =
    column match
      case bool: BoolColumn => bool
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Bool",
        )

  private[columnar] def asUtf8(column: Column, ordinal: Int): DictUtf8Column =
    column match
      case utf8: DictUtf8Column => utf8
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Utf8Dict",
        )
