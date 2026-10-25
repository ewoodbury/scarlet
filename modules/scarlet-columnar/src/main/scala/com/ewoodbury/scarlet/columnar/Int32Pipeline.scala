package com.ewoodbury.scarlet.columnar

import ColumnKernel.{Int32Map, Int32Predicate}

/**
 * Sum and count loops for each int filter/project pattern.
 *
 * A filter drops a null without calling the predicate. A project skips a null and stores 0. When
 * every row of the scanned range is present, the loop does not test the validity bit. A sum or
 * count reads `[start, start + length)` on the column. The batch entry uses the whole column.
 * [[sumPPPPP]] folds one more map into the sum. [[sumEncoded]] and [[countEncoded]] fold a chain
 * that did not get its own pattern. None of those sums or counts allocate an output column.
 * Collect patterns of length 2–4 are [[Int32Fuse]], forwarded here under the same names. Each
 * collect scans the morsel range. The output batch covers only that range.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object Int32Pipeline:

  def FF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
  ): ColumnBatch = Int32Fuse.FF(morsel, first, second)

  def FP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
  ): ColumnBatch = Int32Fuse.FP(morsel, first, second)

  def PF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
  ): ColumnBatch = Int32Fuse.PF(morsel, first, second)

  def PP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
  ): ColumnBatch = Int32Fuse.PP(morsel, first, second)

  def FFF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
  ): ColumnBatch = Int32Fuse.FFF(morsel, first, second, third)

  def FFP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
  ): ColumnBatch = Int32Fuse.FFP(morsel, first, second, third)

  def FPF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
  ): ColumnBatch = Int32Fuse.FPF(morsel, first, second, third)

  def FPP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
  ): ColumnBatch = Int32Fuse.FPP(morsel, first, second, third)

  def PFF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
  ): ColumnBatch = Int32Fuse.PFF(morsel, first, second, third)

  def PFP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
  ): ColumnBatch = Int32Fuse.PFP(morsel, first, second, third)

  def PPF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
  ): ColumnBatch = Int32Fuse.PPF(morsel, first, second, third)

  def PPP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
  ): ColumnBatch = Int32Fuse.PPP(morsel, first, second, third)

  def FFFF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch = Int32Fuse.FFFF(morsel, first, second, third, fourth)

  def FFFP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch = Int32Fuse.FFFP(morsel, first, second, third, fourth)

  def FFPF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch = Int32Fuse.FFPF(morsel, first, second, third, fourth)

  def FFPP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch = Int32Fuse.FFPP(morsel, first, second, third, fourth)

  def FPFF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch = Int32Fuse.FPFF(morsel, first, second, third, fourth)

  def FPFP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch = Int32Fuse.FPFP(morsel, first, second, third, fourth)

  def FPPF(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch = Int32Fuse.FPPF(morsel, first, second, third, fourth)

  def FPPP(
      morsel: Morsel,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch = Int32Fuse.FPPP(morsel, first, second, third, fourth)

  def PFFF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch = Int32Fuse.PFFF(morsel, first, second, third, fourth)

  def PFFP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch = Int32Fuse.PFFP(morsel, first, second, third, fourth)

  def PFPF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch = Int32Fuse.PFPF(morsel, first, second, third, fourth)

  def PFPP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch = Int32Fuse.PFPP(morsel, first, second, third, fourth)

  def PPFF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): ColumnBatch = Int32Fuse.PPFF(morsel, first, second, third, fourth)

  def PPFP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Map,
  ): ColumnBatch = Int32Fuse.PPFP(morsel, first, second, third, fourth)

  def PPPF(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Predicate,
  ): ColumnBatch = Int32Fuse.PPPF(morsel, first, second, third, fourth)

  def PPPP(
      morsel: Morsel,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
  ): ColumnBatch = Int32Fuse.PPPP(morsel, first, second, third, fourth)

  def sumFF(batch: ColumnBatch, first: Int32Predicate, second: Int32Predicate): Long =
    sumFF(singleInt(batch), 0, batch.length, first, second)

  private[scarlet] def sumFF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
      true,
      value => if (first(value) && second(value)) then accepted(value) else 0L,
    )

  def sumFP(batch: ColumnBatch, first: Int32Predicate, second: Int32Map): Long =
    sumFP(singleInt(batch), 0, batch.length, first, second)

  private[scarlet] def sumFP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Map,
  ): Long =
    sumInt32(
      column,
      start,
      length,
      true,
      value => if (first(value)) then accepted(second(value)) else 0L,
    )

  def sumPF(batch: ColumnBatch, first: Int32Map, second: Int32Predicate): Long =
    sumPF(singleInt(batch), 0, batch.length, first, second)

  private[scarlet] def sumPF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then accepted(mapped) else 0L,
    )

  def sumPP(batch: ColumnBatch, first: Int32Map, second: Int32Map): Long =
    sumPP(singleInt(batch), 0, batch.length, first, second)

  private[scarlet] def sumPP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Map,
  ): Long = sumInt32(column, start, length, false, value => accepted(second(first(value))))

  def sumFFF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
  ): Long =
    sumFFF(singleInt(batch), 0, batch.length, first, second, third)

  private[scarlet] def sumFFF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
      true,
      value => if (first(value) && second(value) && third(value)) then accepted(value) else 0L,
    )

  def sumFFP(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
  ): Long =
    sumFFP(singleInt(batch), 0, batch.length, first, second, third)

  private[scarlet] def sumFFP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
  ): Long =
    sumInt32(
      column,
      start,
      length,
      true,
      value => if (first(value) && second(value)) then accepted(third(value)) else 0L,
    )

  def sumFPF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
  ): Long =
    sumFPF(singleInt(batch), 0, batch.length, first, second, third)

  private[scarlet] def sumFPF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
      true,
      value =>
        if (first(value)) then
          val mapped = second(value)
          if (third(mapped)) then accepted(mapped) else 0L
        else 0L,
    )

  def sumFPP(batch: ColumnBatch, first: Int32Predicate, second: Int32Map, third: Int32Map): Long =
    sumFPP(singleInt(batch), 0, batch.length, first, second, third)

  private[scarlet] def sumFPP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
  ): Long =
    sumInt32(
      column,
      start,
      length,
      true,
      value => if (first(value)) then accepted(third(second(value))) else 0L,
    )

  def sumPFF(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
  ): Long =
    sumPFF(singleInt(batch), 0, batch.length, first, second, third)

  private[scarlet] def sumPFF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped) && third(mapped)) then accepted(mapped) else 0L,
    )

  def sumPFP(batch: ColumnBatch, first: Int32Map, second: Int32Predicate, third: Int32Map): Long =
    sumPFP(singleInt(batch), 0, batch.length, first, second, third)

  private[scarlet] def sumPFP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
  ): Long =
    sumInt32(
      column,
      start,
      length,
      true,
      value =>
        val mapped = first(value)
        if (second(mapped)) then accepted(third(mapped)) else 0L,
    )

  def sumPPF(batch: ColumnBatch, first: Int32Map, second: Int32Map, third: Int32Predicate): Long =
    sumPPF(singleInt(batch), 0, batch.length, first, second, third)

  private[scarlet] def sumPPF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
      true,
      value =>
        val mapped = second(first(value))
        if (third(mapped)) then accepted(mapped) else 0L,
    )

  def sumPPP(batch: ColumnBatch, first: Int32Map, second: Int32Map, third: Int32Map): Long =
    sumPPP(singleInt(batch), 0, batch.length, first, second, third)

  private[scarlet] def sumPPP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
  ): Long = sumInt32(column, start, length, false, value => accepted(third(second(first(value)))))

  def sumFFFF(
      batch: ColumnBatch,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): Long =
    sumFFFF(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumFFFF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumFFFP(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumFFFP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumFFPF(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumFFPF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumFFPP(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumFFPP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumFPFF(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumFPFF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumFPFP(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumFPFP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumFPPF(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumFPPF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumFPPP(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumFPPP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Predicate,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumPFFF(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumPFFF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumPFFP(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumPFFP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Predicate,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumPFPF(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumPFPF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumPFPP(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumPFPP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Predicate,
      third: Int32Map,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumPPFF(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumPPFF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumPPFP(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumPPFP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Map,
      third: Int32Predicate,
      fourth: Int32Map,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumPPPF(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumPPPF(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
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
    sumPPPP(singleInt(batch), 0, batch.length, first, second, third, fourth)

  private[scarlet] def sumPPPP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
  ): Long =
    sumInt32(column, start, length, false, value => accepted(fourth(third(second(first(value))))))

  /** Five maps in one sum. The fifth map is not written as a column. */

  def sumPPPPP(
      batch: ColumnBatch,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
      fifth: Int32Map,
  ): Long =
    sumPPPPP(singleInt(batch), 0, batch.length, first, second, third, fourth, fifth)

  private[scarlet] def sumPPPPP(
      column: Int32Column,
      start: Int,
      length: Int,
      first: Int32Map,
      second: Int32Map,
      third: Int32Map,
      fourth: Int32Map,
      fifth: Int32Map,
  ): Long =
    sumInt32(
      column,
      start,
      length,
      false,
      value => accepted(fifth(fourth(third(second(first(value)))))),
    )

  def sumFiltered(batch: ColumnBatch, predicate: Int32Predicate): Long =
    sumFiltered(singleInt(batch), 0, batch.length, predicate)

  private[scarlet] def sumFiltered(
      column: Int32Column,
      start: Int,
      length: Int,
      predicate: Int32Predicate,
  ): Long =
    sumInt32(
      column,
      start,
      length,
      true,
      value => if (predicate(value)) then accepted(value) else 0L,
    )

  def sumProjected(batch: ColumnBatch, mapper: Int32Map): Long =
    sumProjected(singleInt(batch), 0, batch.length, mapper)

  private[scarlet] def sumProjected(
      column: Int32Column,
      start: Int,
      length: Int,
      mapper: Int32Map,
  ): Long =
    sumInt32(column, start, length, false, value => accepted(mapper(value)))

  /**
   * Survivor encoding shared by the fused loops and by [[sumEncoded]] / [[countEncoded]]. The low
   * bit is set. The other bits are the signed int. A dropped row is `0`, which is not a kept zero.
   */
  inline def encoded(value: Int): Long = (value.toLong << 1) | 1L

  /**
   * Sum the survivor encoding of `process` over `[start, start + length)`. A dropped row is `0`. A
   * kept row is [[encoded]]. `compact` is true when a filter can drop a row. Does not allocate.
   *
   * This is the [[sumInt32]] walk with a runtime `compact` flag. [[sumInt32]] inlines that flag,
   * which a function value passed in from the planner cannot do. The batch entry scans from row 0.
   */

  def sumEncoded(batch: ColumnBatch, compact: Boolean, process: Int => Long): Long =
    val column = singleInt(batch)
    sumEncoded(column, 0, column.length, compact, process)

  private[scarlet] def sumEncoded(
      column: Int32Column,
      start: Int,
      length: Int,
      compact: Boolean,
      process: Int => Long,
  ): Long =
    val end = start + length
    val values = column.values
    val words = column.validity.words
    var total = 0L
    var row = start
    if (Validity.rangePresent(column.validity, start, length)) then
      while (row < end) {
        val result = process(values(row))
        if (!compact || (result & 1L) != 0L) then total += result >> 1
        row += 1
      }
    else
      while (row < end) {
        if (Validity.isSet(words, row)) then
          val result = process(values(row))
          if (!compact || (result & 1L) != 0L) then total += result >> 1
        row += 1
      }
    total

  /**
   * Count rows of `process` over `[start, start + length)` without writing them. `compact` counts
   * survivors and drops nulls. A map-only chain returns `length`, which still includes null slots,
   * after calling `process` on the present ones. The batch entry scans from row 0.
   *
   * When `compact` is false, `kept` is discarded and the low-bit test does not decide the count.
   * `process` still has to run on every present row: a map-only count calls the mappers for their
   * effects, and dropping that call would skip them.
   */

  def countEncoded(batch: ColumnBatch, compact: Boolean, process: Int => Long): Long =
    val column = singleInt(batch)
    countEncoded(column, 0, column.length, compact, process)

  private[scarlet] def countEncoded(
      column: Int32Column,
      start: Int,
      length: Int,
      compact: Boolean,
      process: Int => Long,
  ): Long =
    val end = start + length
    val values = column.values
    val words = column.validity.words
    var kept = 0L
    var row = start
    if (Validity.rangePresent(column.validity, start, length)) then
      while (row < end) {
        val result = process(values(row))
        if ((result & 1L) != 0L) then kept += 1L
        row += 1
      }
    else
      while (row < end) {
        if (Validity.isSet(words, row)) then
          val result = process(values(row))
          if ((result & 1L) != 0L) then kept += 1L
        row += 1
      }
    // Map-only discards `kept`. The `process` call above is the mapper's only run.
    if (compact) then kept else length.toLong

  private[scarlet] def singleInt(batch: ColumnBatch): Int32Column =
    require(
      batch.columns.length == 1,
      s"int pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)

  /** Add the fused values in `[start, start + length)`. Does not allocate an output column. */
  private inline def sumInt32(
      column: Int32Column,
      start: Int,
      length: Int,
      inline compact: Boolean,
      inline process: Int => Long,
  ): Long =
    val end = start + length
    val values = column.values
    val words = column.validity.words
    var total = 0L
    var row = start
    if (Validity.rangePresent(column.validity, start, length)) then
      while (row < end) {
        val result = process(values(row))
        inline if (compact) then
          if ((result & 1L) != 0L) then total += result >> 1
        else total += result >> 1
        row += 1
      }
    else
      while (row < end) {
        if (Validity.isSet(words, row)) then
          val result = process(values(row))
          inline if (compact) then
            if ((result & 1L) != 0L) then total += result >> 1
          else total += result >> 1
        row += 1
      }
    total

  /** Inlined into [[sumInt32]]. The formula lives on [[encoded]]. */
  private inline def accepted(value: Int): Long = encoded(value)
