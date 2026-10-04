package com.ewoodbury.scarlet.columnar

import ColumnKernel.{Int32Map, Int32Predicate}

/**
 * One loop per int filter/project pattern of length 2–4.
 *
 * A filter drops a null without calling the predicate. A project skips a null and stores 0.
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

  // The low bit marks a surviving row; the remaining bits preserve the signed int payload.
  private inline def accepted(value: Int): Long = (value.toLong << 1) | 1L
