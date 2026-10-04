package com.ewoodbury.scarlet.columnar

import ColumnKernel.{Utf8Map, Utf8Predicate}

/**
 * One loop per string filter/project pattern of length 2–4.
 *
 * Filters and projects see empty cells. A filter-only run shares the input dictionary. A run that
 * projects interns only the strings it stores.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object Utf8Pipeline:

  def FF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
  ): ColumnBatch =
    fuseFilters(batch, text => first(text) && second(text))

  def PF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) && accept(gate, mapped0)
      },
    )

  def FP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
  ): ColumnBatch =
    fuseMapped(batch, true, (text, gate) => first(text) && accept(gate, second(text)))

  def PP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
  ): ColumnBatch =
    fuseMapped(batch, false, (text, gate) => accept(gate, second(first(text))))

  def FFF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Predicate,
  ): ColumnBatch =
    fuseFilters(batch, text => first(text) && second(text) && third(text))

  def PFF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) && third(mapped0) && accept(gate, mapped0)
      },
    )

  def FPF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) =>
        first(text) &&
          {
            val mapped1 = second(text)
            third(mapped1) && accept(gate, mapped1)
          },
    )

  def PPF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => {
        val mapped1 = second(first(text))
        third(mapped1) && accept(gate, mapped1)
      },
    )

  def FFP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => first(text) && second(text) && accept(gate, third(text)),
    )

  def PFP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) && accept(gate, third(mapped0))
      },
    )

  def FPP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Map,
  ): ColumnBatch =
    fuseMapped(batch, true, (text, gate) => first(text) && accept(gate, third(second(text))))

  def PPP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Map,
  ): ColumnBatch =
    fuseMapped(batch, false, (text, gate) => accept(gate, third(second(first(text)))))

  def FFFF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Predicate,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseFilters(batch, text => first(text) && second(text) && third(text) && fourth(text))

  def PFFF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Predicate,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) && third(mapped0) && fourth(mapped0) && accept(gate, mapped0)
      },
    )

  def FPFF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Predicate,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) =>
        first(text) &&
          {
            val mapped1 = second(text)
            third(mapped1) && fourth(mapped1) && accept(gate, mapped1)
          },
    )

  def PPFF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Predicate,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => {
        val mapped1 = second(first(text))
        third(mapped1) && fourth(mapped1) && accept(gate, mapped1)
      },
    )

  def FFPF(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Map,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      batch,
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
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Map,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      batch,
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
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Map,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) =>
        first(text) &&
          {
            val mapped2 = third(second(text))
            fourth(mapped2) && accept(gate, mapped2)
          },
    )

  def PPPF(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Map,
      fourth: Utf8Predicate,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => {
        val mapped2 = third(second(first(text)))
        fourth(mapped2) && accept(gate, mapped2)
      },
    )

  def FFFP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Predicate,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => first(text) && second(text) && third(text) && accept(gate, fourth(text)),
    )

  def PFFP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Predicate,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) && third(mapped0) && accept(gate, fourth(mapped0))
      },
    )

  def FPFP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Predicate,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) =>
        first(text) &&
          {
            val mapped1 = second(text)
            third(mapped1) && accept(gate, fourth(mapped1))
          },
    )

  def PPFP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Predicate,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => {
        val mapped1 = second(first(text))
        third(mapped1) && accept(gate, fourth(mapped1))
      },
    )

  def FFPP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Predicate,
      third: Utf8Map,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => first(text) && second(text) && accept(gate, fourth(third(text))),
    )

  def PFPP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Predicate,
      third: Utf8Map,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => {
        val mapped0 = first(text)
        second(mapped0) && accept(gate, fourth(third(mapped0)))
      },
    )

  def FPPP(
      batch: ColumnBatch,
      first: Utf8Predicate,
      second: Utf8Map,
      third: Utf8Map,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(
      batch,
      true,
      (text, gate) => first(text) && accept(gate, fourth(third(second(text)))),
    )

  def PPPP(
      batch: ColumnBatch,
      first: Utf8Map,
      second: Utf8Map,
      third: Utf8Map,
      fourth: Utf8Map,
  ): ColumnBatch =
    fuseMapped(batch, false, (text, gate) => accept(gate, fourth(third(second(first(text))))))

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
      batch: ColumnBatch,
      inline keep: String => Boolean,
  ): ColumnBatch =
    require(
      batch.columns.length == 1,
      s"utf8 pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
    val live = column.length
    val codes = column.codes
    val words = column.validity.words
    val out = new Array[Int](live)
    val dstWords = Validity.allocate(live)
    var written = 0
    var row = 0
    while (row < live) {
      if (keep(ColumnKernel.utf8String(column, row))) then
        if (Validity.isSet(words, row)) then
          out(written) = codes(row)
          Validity.setBit(dstWords, written)
        written += 1
      row += 1
    }
    val result = DictUtf8Column.of(
      column.dictionary,
      out,
      Validity.fromWords(dstWords, live),
      written,
    )
    new ColumnBatch(batch.schema, Vector(result), written)

  /**
   * Projects build one dictionary of the strings this loop stores. A later filter therefore does
   * not retain a string it dropped, because the intermediate column never existed.
   */
  private inline def fuseMapped(
      batch: ColumnBatch,
      inline compact: Boolean,
      inline step: (String, Gate) => Boolean,
  ): ColumnBatch =
    require(
      batch.columns.length == 1,
      s"utf8 pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
    val live = column.length
    val capacity = inline if (compact) then live else column.codes.length
    val out = new Array[Int](capacity)
    val dstWords = Validity.allocate(capacity)
    val words = DictUtf8Column.Words.empty
    val gate = new Gate
    var written = 0
    var row = 0
    while (row < live) {
      gate.present = false
      val kept = step(ColumnKernel.utf8String(column, row), gate)
      inline if (compact) then
        if (kept) then
          if (gate.present) then
            out(written) = words.intern(gate.text)
            Validity.setBit(dstWords, written)
          written += 1
      else if (kept && gate.present) then
        out(row) = words.intern(gate.text)
        Validity.setBit(dstWords, row)
      row += 1
    }
    val length = inline if (compact) then written else live
    val result =
      DictUtf8Column.of(words.toArray, out, Validity.fromWords(dstWords, capacity), length)
    new ColumnBatch(batch.schema, Vector(result), length)
