package com.ewoodbury.scarlet.columnar

import ColumnKernel.{Int32Map, Int32Predicate}

/**
 * One loop per int filter/project pattern of length 2–4.
 *
 * A filter drops a null without calling the predicate. A project skips a null and stores 0. When
 * every live row is present, the loop does not test the validity bit. [[sumPPPPP]] folds one more
 * map into the sum. [[sumEncoded]] and [[countEncoded]] fold a chain that did not get its own
 * pattern. None of those sums or counts allocate an output column.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object Int32Pipeline:

  def FF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value => if (first(value) && second(value)) then accepted(value) else 0L,
    )

  def FP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
  ): ColumnBatch =
    fuseInt32(batch, true, value => if (first(value)) then accepted(second(value)) else 0L)

  def PF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then accepted(mapped) else 0L,
    )

  def PP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
  ): ColumnBatch = fuseInt32(batch, false, value => accepted(second(first(value))))

  def FFF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value => if (first(value) && second(value) && third(value)) then accepted(value) else 0L,
    )

  def FFP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value => if (first(value) && second(value)) then accepted(third(value)) else 0L,
    )

  def FPF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def FPP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
  ): ColumnBatch =
    fuseInt32(batch, true, value => if (first(value)) then accepted(third(second(value))) else 0L)

  def PFF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped)) then accepted(mapped) else 0L,
    )

  def PFP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then accepted(third(mapped)) else 0L,
    )

  def PPF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped)) then accepted(mapped) else 0L,
    )

  def PPP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
  ): ColumnBatch = fuseInt32(batch, false, value => accepted(third(second(first(value)))))

  def FFFF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value) && second(value) && third(value) && fourth(value)) then accepted(value)
        else 0L,
    )

  def FFFP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value) && second(value) && third(value)) then accepted(fourth(value)) else 0L,
    )

  def FFPF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value) && second(value)) then
          val mapped = third(value)
          if (fourth(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def FFPP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value => if (first(value) && second(value)) then accepted(fourth(third(value))) else 0L,
    )

  def FPFF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped) && fourth(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def FPFP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped)) then accepted(fourth(mapped)) else 0L
        else 0L,
    )

  def FPPF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        if (first(value)) then
          val mapped = third(second(value))
          if (fourth(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def FPPP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value => if (first(value)) then accepted(fourth(third(second(value)))) else 0L,
    )

  def PFFF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped) && fourth(mapped)) then accepted(mapped) else 0L,
    )

  def PFFP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped)) then accepted(fourth(mapped)) else 0L,
    )

  def PFPF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then
          val next = third(mapped)
          if (fourth(next)) then accepted(next) else 0L
        else 0L,
    )

  def PFPP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then accepted(fourth(third(mapped))) else 0L,
    )

  def PPFF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped) && fourth(mapped)) then accepted(mapped) else 0L,
    )

  def PPFP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped)) then accepted(fourth(mapped)) else 0L,
    )

  def PPPF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      batch,
      true,
      value =>
        val mapped = third(second(first(value)))
        if (fourth(mapped)) then accepted(mapped) else 0L,
    )

  def PPPP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(batch, false, value => accepted(fourth(third(second(first(value))))))

  def sumFF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value => if (first(value) && second(value)) then accepted(value) else 0L,
    )

  def sumFP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
  ): Long =
    sumInt32(batch, true, value => if (first(value)) then accepted(second(value)) else 0L)

  def sumPF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then accepted(mapped) else 0L,
    )

  def sumPP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
  ): Long = sumInt32(batch, false, value => accepted(second(first(value))))

  def sumFFF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value => if (first(value) && second(value) && third(value)) then accepted(value) else 0L,
    )

  def sumFFP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
  ): Long =
    sumInt32(
      batch,
      true,
      value => if (first(value) && second(value)) then accepted(third(value)) else 0L,
    )

  def sumFPF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def sumFPP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
  ): Long =
    sumInt32(batch, true, value => if (first(value)) then accepted(third(second(value))) else 0L)

  def sumPFF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped)) then accepted(mapped) else 0L,
    )

  def sumPFP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then accepted(third(mapped)) else 0L,
    )

  def sumPPF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped)) then accepted(mapped) else 0L,
    )

  def sumPPP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
  ): Long = sumInt32(batch, false, value => accepted(third(second(first(value)))))

  def sumFFFF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        if (first(value) && second(value) && third(value) && fourth(value)) then accepted(value)
        else 0L,
    )

  def sumFFFP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        if (first(value) && second(value) && third(value)) then accepted(fourth(value)) else 0L,
    )

  def sumFFPF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        if (first(value) && second(value)) then
          val mapped = third(value)
          if (fourth(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def sumFFPP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      batch,
      true,
      value => if (first(value) && second(value)) then accepted(fourth(third(value))) else 0L,
    )

  def sumFPFF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped) && fourth(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def sumFPFP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped)) then accepted(fourth(mapped)) else 0L
        else 0L,
    )

  def sumFPPF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        if (first(value)) then
          val mapped = third(second(value))
          if (fourth(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def sumFPPP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      batch,
      true,
      value => if (first(value)) then accepted(fourth(third(second(value)))) else 0L,
    )

  def sumPFFF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped) && fourth(mapped)) then accepted(mapped) else 0L,
    )

  def sumPFFP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped)) then accepted(fourth(mapped)) else 0L,
    )

  def sumPFPF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then
          val next = third(mapped)
          if (fourth(next)) then accepted(next) else 0L
        else 0L,
    )

  def sumPFPP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then accepted(fourth(third(mapped))) else 0L,
    )

  def sumPPFF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped) && fourth(mapped)) then accepted(mapped) else 0L,
    )

  def sumPPFP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped)) then accepted(fourth(mapped)) else 0L,
    )

  def sumPPPF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      batch,
      true,
      value =>
        val mapped = third(second(first(value)))
        if (fourth(mapped)) then accepted(mapped) else 0L,
    )

  def sumPPPP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
  ): Long =
    sumInt32(batch, false, value => accepted(fourth(third(second(first(value))))))

  /** Five maps in one sum. The fifth map is not written as a column. */
  def sumPPPPP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
      fifth: Int32Map,
  ): Long =
    sumInt32(batch, false, value => accepted(fifth(fourth(third(second(first(value)))))))

  def sumFiltered(batch: ColumnBatch, predicate: Int32Predicate): Long =
    sumInt32(batch, true, value => if (predicate(value)) then accepted(value) else 0L)

  def sumProjected(batch: ColumnBatch, mapper: Int32Map): Long =
    sumInt32(batch, false, value => accepted(mapper(value)))

  /**
   * Survivor encoding shared by the fused loops and by [[sumEncoded]] / [[countEncoded]]. The low
   * bit is set. The other bits are the signed int. A dropped row is `0`, which is not a kept zero.
   */
  inline def encoded(value: Int): Long = (value.toLong << 1) | 1L

  /**
   * Sum the survivor encoding of `process`. A dropped row is `0`. A kept row is [[encoded]].
   * `compact` is true when a filter can drop a row. Does not allocate.
   *
   * This is the [[sumInt32]] walk with a runtime `compact` flag. [[sumInt32]] inlines that flag,
   * which a function value passed in from the planner cannot do.
   */
  def sumEncoded(batch: ColumnBatch, compact: Boolean, process: Int => Long): Long =
    val column = singleInt(batch)
    val live = column.length
    val values = column.values
    val words = column.validity.words
    var total = 0L
    var row = 0
    if (Validity.livePresent(column.validity, live)) then
      while (row < live) {
        val result = process(values(row))
        if (!compact || (result & 1L) != 0L) then total += result >> 1
        row += 1
      }
    else
      while (row < live) {
        if (Validity.isSet(words, row)) then
          val result = process(values(row))
          if (!compact || (result & 1L) != 0L) then total += result >> 1
        row += 1
      }
    total

  /**
   * Count rows of `process` without writing them. `compact` counts survivors and drops nulls. A
   * map-only chain returns `batch.length`, which still includes null slots, after calling
   * `process` on the present ones.
   *
   * When `compact` is false, `kept` is discarded and the low-bit test does not decide the count.
   * `process` still has to run on every present row: a map-only count calls the mappers for their
   * effects, and dropping that call would skip them.
   */
  def countEncoded(batch: ColumnBatch, compact: Boolean, process: Int => Long): Long =
    val column = singleInt(batch)
    val live = column.length
    val values = column.values
    val words = column.validity.words
    var kept = 0L
    var row = 0
    if (Validity.livePresent(column.validity, live)) then
      while (row < live) {
        val result = process(values(row))
        if ((result & 1L) != 0L) then kept += 1L
        row += 1
      }
    else
      while (row < live) {
        if (Validity.isSet(words, row)) then
          val result = process(values(row))
          if ((result & 1L) != 0L) then kept += 1L
        row += 1
      }
    // Map-only discards `kept`. The `process` call above is the mapper's only run.
    if (compact) then kept else live.toLong

  private def singleInt(batch: ColumnBatch): Int32Column =
    require(
      batch.columns.length == 1,
      s"int pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)

  /** Add the fused values. Does not allocate an output column. */
  private inline def sumInt32(
      batch: ColumnBatch,
      inline compact: Boolean,
      inline process: Int => Long,
  ): Long =
    require(
      batch.columns.length == 1,
      s"int pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)
    val live = column.length
    val values = column.values
    val words = column.validity.words
    var total = 0L
    var row = 0
    if (Validity.livePresent(column.validity, live)) then
      while (row < live) {
        val result = process(values(row))
        inline if (compact) then
          if ((result & 1L) != 0L) then total += result >> 1
        else total += result >> 1
        row += 1
      }
    else
      while (row < live) {
        if (Validity.isSet(words, row)) then
          val result = process(values(row))
          inline if (compact) then
            if ((result & 1L) != 0L) then total += result >> 1
          else total += result >> 1
        row += 1
      }
    total

  private inline def fuseInt32(
      batch: ColumnBatch,
      inline compact: Boolean,
      inline process: Int => Long,
  ): ColumnBatch =
    require(
      batch.columns.length == 1,
      s"int pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)
    val live = column.length
    val in = column.values
    val words = column.validity.words
    // Compaction only needs live-row capacity; map-only pipelines preserve input capacity.
    val capacity = inline if (compact) then live else in.length
    val out = new Array[Int](capacity)
    var written = 0
    var row = 0
    if (Validity.livePresent(column.validity, live)) then
      while (row < live) {
        val result = process(in(row))
        inline if (compact) then
          if ((result & 1L) != 0L) then
            out(written) = (result >> 1).toInt
            written += 1
        else out(row) = (result >> 1).toInt
        row += 1
      }
    else
      while (row < live) {
        if (Validity.isSet(words, row)) then
          val result = process(in(row))
          inline if (compact) then
            if ((result & 1L) != 0L) then
              out(written) = (result >> 1).toInt
              written += 1
          else out(row) = (result >> 1).toInt
        row += 1
      }
    val length = inline if (compact) then written else live
    val validity =
      inline if (compact) then Validity.prefixValid(capacity, written)
      else column.validity
    val result = Int32Column.of(out, validity, length)
    new ColumnBatch(batch.schema, Vector(result), length)

  /** Inlined into [[sumInt32]]. The formula lives on [[encoded]]. */
  private inline def accepted(value: Int): Long = encoded(value)
