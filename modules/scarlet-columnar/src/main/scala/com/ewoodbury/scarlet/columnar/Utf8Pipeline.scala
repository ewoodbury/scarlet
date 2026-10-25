package com.ewoodbury.scarlet.columnar

import ColumnKernel.{Utf8Map, Utf8Predicate}

/**
 * One loop per string filter/project pattern of length 2–4.
 *
 * The scan is `[start, start + length)` over dictionary codes. A whole batch is
 * [[Morsel]]`(batch, 0, batch.length)`. [[Validity.rangePresent]] decides the dense path, so a
 * range with no holes does not test the validity bit. A hole is an empty cell: filters and
 * projects see it, and a filter that accepts it keeps a null slot. A filter-only run shares the
 * input dictionary. A run that projects interns only the strings it stores.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object Utf8Pipeline:

  def FF(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Predicate,
  ): ColumnBatch =
    fuseFilters(morsel, text => first(text) && second(text))

  def PF(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) && accept(gate, mapped0)
      },
    )

  def FP(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Map,
  ): ColumnBatch =
    fuseMapped(morsel, true, (text, gate) => first(text) && accept(gate, second(text)))

  def PP(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Map,
  ): ColumnBatch =
    fuseMapped(morsel, false, (text, gate) => accept(gate, second(first(text))))

  def FFF(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Predicate,
  ): ColumnBatch =
    fuseFilters(morsel, text => first(text) && second(text) && third(text))

  def PFF(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) && third(mapped0) && accept(gate, mapped0)
      },
    )

  def FPF(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) =>
        first(text) &&
          {
            val mapped1 = second(text)
            third(mapped1) && accept(gate, mapped1)
          },
    )

  def PPF(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => {
        val mapped1 = second(first(text))
        third(mapped1) && accept(gate, mapped1)
      },
    )

  def FFP(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => first(text) && second(text) && accept(gate, third(text)),
    )

  def PFP(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) && accept(gate, third(mapped0))
      },
    )

  def FPP(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Map,
  ): ColumnBatch =
    fuseMapped(morsel, true, (text, gate) => first(text) && accept(gate, third(second(text))))

  def PPP(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Map,
  ): ColumnBatch =
    fuseMapped(morsel, false, (text, gate) => accept(gate, third(second(first(text)))))

  def FFFF(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Predicate,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseFilters(morsel, text => first(text) && second(text) && third(text) && fourth(text))

  def PFFF(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Predicate,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) && third(mapped0) && fourth(mapped0) && accept(gate, mapped0)
      },
    )

  def FPFF(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Predicate,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) =>
        first(text) &&
          {
            val mapped1 = second(text)
            third(mapped1) && fourth(mapped1) && accept(gate, mapped1)
          },
    )

  def PPFF(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Predicate,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => {
        val mapped1 = second(first(text))
        third(mapped1) && fourth(mapped1) && accept(gate, mapped1)
      },
    )

  def FFPF(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Map,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) =>
        first(text) &&
          second(text) &&
          {
            val mapped2 = third(text)
            fourth(mapped2) && accept(gate, mapped2)
          },
    )

  def PFPF(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Map,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) &&
        {
          val mapped2 = third(mapped0)
          fourth(mapped2) && accept(gate, mapped2)
        }
      },
    )

  def FPPF(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Map,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) =>
        first(text) &&
          {
            val mapped2 = third(second(text))
            fourth(mapped2) && accept(gate, mapped2)
          },
    )

  def PPPF(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Map,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => {
        val mapped2 = third(second(first(text)))
        fourth(mapped2) && accept(gate, mapped2)
      },
    )

  def FFFP(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Predicate,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => first(text) && second(text) && third(text) && accept(gate, fourth(text)),
    )

  def PFFP(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Predicate,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) && third(mapped0) && accept(gate, fourth(mapped0))
      },
    )

  def FPFP(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Predicate,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) =>
        first(text) &&
          {
            val mapped1 = second(text)
            third(mapped1) && accept(gate, fourth(mapped1))
          },
    )

  def PPFP(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Predicate,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => {
        val mapped1 = second(first(text))
        third(mapped1) && accept(gate, fourth(mapped1))
      },
    )

  def FFPP(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Map,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => first(text) && second(text) && accept(gate, fourth(third(text))),
    )

  def PFPP(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Map,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) && accept(gate, fourth(third(mapped0)))
      },
    )

  def FPPP(
      morsel: Morsel,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Map,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      morsel,
      true,
      (text, gate) => first(text) && accept(gate, fourth(third(second(text)))),
    )

  def PPPP(
      morsel: Morsel,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Map,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(morsel, false, (text, gate) => accept(gate, fourth(third(second(first(text))))))

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class Gate:
    var present: Boolean = false
    var text: String = ""

  /** Returns whether the row survives, so a pattern that forgets the decision does not compile. */
  private inline def accept(gate: Gate, value: String): Boolean =
    if (BatchCodec.isAbsent(value)) then gate.present = false
    else
      gate.present = true
      gate.text = value
    true

  private inline def fuseFilters(
      morsel: Morsel,
      inline keep: String => Boolean,
  ): ColumnBatch =
    val batch = morsel.batch
    require(
      batch.columns.length == 1,
      s"utf8 pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
    val start = morsel.start
    val length = morsel.length
    Ranges.check(start, length, column.length)
    val codes = column.codes
    val validityWords = column.validity.words
    val out = new Array[Int](length)
    val dstWords = Validity.allocate(length)
    val end = start + length
    var written = 0
    var row = start
    if (Validity.rangePresent(column.validity, start, length)) then
      while (row < end) {
        if (keep(column.dictionary(codes(row)))) then
          out(written) = codes(row)
          Validity.setBit(dstWords, written)
          written += 1
        row += 1
      }
    else
      while (row < end) {
        if (keep(ColumnKernel.utf8String(column, row))) then
          if (Validity.isSet(validityWords, row)) then
            out(written) = codes(row)
            Validity.setBit(dstWords, written)
          written += 1
        row += 1
      }
    val result = DictUtf8Column.of(
      column.dictionary,
      out,
      Validity.fromWords(dstWords, length),
      written,
    )
    new ColumnBatch(batch.schema, Vector(result), written)

  /**
   * Projects build one dictionary of the strings this loop stores. A later filter therefore does
   * not retain a string it dropped, because the intermediate column never existed. A whole-column
   * map keeps the input code-array capacity and writes at absolute rows. Any other range writes
   * `[0, length)`.
   */
  private inline def fuseMapped(
      morsel: Morsel,
      inline compact: Boolean,
      inline step: (String, Gate) => Boolean,
  ): ColumnBatch =
    val batch = morsel.batch
    require(
      batch.columns.length == 1,
      s"utf8 pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
    val start = morsel.start
    val length = morsel.length
    Ranges.check(start, length, column.length)
    val wholeColumn = start == 0 && length == column.length
    val capacity =
      inline if (compact) then length
      else if (wholeColumn) then column.codes.length
      else length
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    val stored = DictUtf8Column.Words.empty
    val gate = new Gate
    val end = start + length
    var written = 0
    var row = start
    if (Validity.rangePresent(column.validity, start, length)) then
      while (row < end) {
        gate.present = false
        val kept = step(column.dictionary(column.codes(row)), gate)
        inline if (compact) then
          if (kept) then
            if (gate.present) then
              out(written) = stored.intern(gate.text)
              Validity.setBit(dstWords, written)
            written += 1
        else if (kept && gate.present) then
          val index = if (wholeColumn) then row else row - start
          out(index) = stored.intern(gate.text)
          Validity.setBit(dstWords, index)
        row += 1
      }
    else
      while (row < end) {
        gate.present = false
        val kept = step(ColumnKernel.utf8String(column, row), gate)
        inline if (compact) then
          if (kept) then
            if (gate.present) then
              out(written) = stored.intern(gate.text)
              Validity.setBit(dstWords, written)
            written += 1
        else if (kept && gate.present) then
          val index = if (wholeColumn) then row else row - start
          out(index) = stored.intern(gate.text)
          Validity.setBit(dstWords, index)
        row += 1
      }
    val outLength = inline if (compact) then written else length
    val result =
      DictUtf8Column.of(stored.toArray, out, Validity.fromWords(dstWords, capacity), outLength)
    new ColumnBatch(batch.schema, Vector(result), outLength)
