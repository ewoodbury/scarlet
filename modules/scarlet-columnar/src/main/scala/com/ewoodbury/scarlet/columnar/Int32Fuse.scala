package com.ewoodbury.scarlet.columnar

import ColumnKernel.{Int32Map, Int32Predicate}

/**
 * Writes one int filter/project pattern of length 2–4 over a morsel range.
 *
 * [[Int32Pipeline]] forwards these names. The scan is `[start, start + length)`. The output covers
 * only that range. A filter drops a null without calling the predicate. A project skips a null and
 * stores 0. When every row of the range is present, the loop does not test the validity bit.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object Int32Fuse:

  def FF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value => if (first(value) && second(value)) then Int32Pipeline.encoded(value) else 0L,
    )

  def FP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value => if (first(value)) then Int32Pipeline.encoded(second(value)) else 0L,
    )

  def PF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then Int32Pipeline.encoded(mapped) else 0L,
    )

  def PP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
  ): ColumnBatch = fuseInt32(morsel, false, value => Int32Pipeline.encoded(second(first(value))))

  def FFF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        if (first(value) && second(value) && third(value)) then Int32Pipeline.encoded(value)
        else 0L,
    )

  def FFP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value => if (first(value) && second(value)) then Int32Pipeline.encoded(third(value)) else 0L,
    )

  def FPF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped)) then Int32Pipeline.encoded(mapped) else 0L
        else 0L,
    )

  def FPP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value => if (first(value)) then Int32Pipeline.encoded(third(second(value))) else 0L,
    )

  def PFF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped)) then Int32Pipeline.encoded(mapped) else 0L,
    )

  def PFP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then Int32Pipeline.encoded(third(mapped)) else 0L,
    )

  def PPF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped)) then Int32Pipeline.encoded(mapped) else 0L,
    )

  def PPP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
  ): ColumnBatch =
    fuseInt32(morsel, false, value => Int32Pipeline.encoded(third(second(first(value)))))

  def FFFF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        if (first(value) && second(value) && third(value) && fourth(value)) then
          Int32Pipeline.encoded(value)
        else 0L,
    )

  def FFFP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        if (first(value) && second(value) && third(value)) then
          Int32Pipeline.encoded(fourth(value))
        else 0L,
    )

  def FFPF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        if (first(value) && second(value)) then
          val mapped = third(value)
          if (fourth(mapped)) then Int32Pipeline.encoded(mapped) else 0L
        else 0L,
    )

  def FFPP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        if (first(value) && second(value)) then Int32Pipeline.encoded(fourth(third(value))) else 0L,
    )

  def FPFF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped) && fourth(mapped)) then Int32Pipeline.encoded(mapped) else 0L
        else 0L,
    )

  def FPFP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped)) then Int32Pipeline.encoded(fourth(mapped)) else 0L
        else 0L,
    )

  def FPPF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        if (first(value)) then
          val mapped = third(second(value))
          if (fourth(mapped)) then Int32Pipeline.encoded(mapped) else 0L
        else 0L,
    )

  def FPPP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value => if (first(value)) then Int32Pipeline.encoded(fourth(third(second(value)))) else 0L,
    )

  def PFFF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped) && fourth(mapped)) then Int32Pipeline.encoded(mapped)
        else 0L,
    )

  def PFFP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped)) then Int32Pipeline.encoded(fourth(mapped)) else 0L,
    )

  def PFPF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then
          val next = third(mapped)
          if (fourth(next)) then Int32Pipeline.encoded(next) else 0L
        else 0L,
    )

  def PFPP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then Int32Pipeline.encoded(fourth(third(mapped))) else 0L,
    )

  def PPFF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped) && fourth(mapped)) then Int32Pipeline.encoded(mapped) else 0L,
    )

  def PPFP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped)) then Int32Pipeline.encoded(fourth(mapped)) else 0L,
    )

  def PPPF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch =
    fuseInt32(
      morsel,
      true,
      value =>
        val mapped = third(second(first(value)))
        if (fourth(mapped)) then Int32Pipeline.encoded(mapped) else 0L,
    )

  def PPPP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch =
    fuseInt32(morsel, false, value => Int32Pipeline.encoded(fourth(third(second(first(value))))))

  private inline def fuseInt32(
      morsel: Morsel,
      inline compact: Boolean,
      inline process: Int => Long,
  ): ColumnBatch =
    val batch = morsel.batch
    require(
      batch.columns.length == 1,
      s"int pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)
    val start = morsel.start
    val length = morsel.length
    requireRange(start, length, column.length)
    val in = column.values
    val words = column.validity.words
    // The output is the range. Compaction and map-only both size to that length.
    val capacity = length
    val out = new Array[Int](capacity)
    val end = start + length
    if (Validity.rangePresent(column.validity, start, length)) then
      var written = 0
      var row = start
      while (row < end) {
        val result = process(in(row))
        inline if (compact) then
          if ((result & 1L) != 0L) then
            out(written) = (result >> 1).toInt
            written += 1
        else
          out(written) = (result >> 1).toInt
          written += 1
        row += 1
      }
      val outLength = inline if (compact) then written else length
      val validity =
        inline if (compact) then Validity.prefixValid(capacity, written)
        else Validity.allValid(capacity)
      finish(batch, out, validity, outLength)
    else inline if (compact) then
      var written = 0
      var row = start
      while (row < end) {
        if (Validity.isSet(words, row)) then
          val result = process(in(row))
          if ((result & 1L) != 0L) then
            out(written) = (result >> 1).toInt
            written += 1
        row += 1
      }
      finish(batch, out, Validity.prefixValid(capacity, written), written)
    else
      val dstWords = Validity.allocate(capacity)
      var written = 0
      var row = start
      while (row < end) {
        if (Validity.isSet(words, row)) then
          val result = process(in(row))
          out(written) = (result >> 1).toInt
          Validity.setBit(dstWords, written)
        else out(written) = 0
        written += 1
        row += 1
      }
      finish(batch, out, Validity.fromWords(dstWords, capacity), length)

  private def finish(
      batch: ColumnBatch,
      out: Array[Int],
      validity: Validity,
      length: Int,
  ): ColumnBatch =
    val column = Int32Column.of(out, validity, length)
    new ColumnBatch(batch.schema, Vector(column), length)

  private def requireRange(start: Int, length: Int, rows: Int): Unit =
    require(start >= 0, s"pipeline start must be >= 0, got $start")
    require(length >= 0, s"pipeline length must be >= 0, got $length")
    require(
      start.toLong + length.toLong <= rows.toLong,
      s"pipeline [$start, ${start.toLong + length.toLong}) outside column length $rows",
    )
