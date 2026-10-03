package com.ewoodbury.scarlet.columnar

import ColumnKernel.{Float64Map, Float64Predicate}

/**
 * One loop per double filter/project pattern of length 2–4.
 *
 * A filter drops a null without calling the predicate. A project skips a null and stores 0. The
 * step returns whether the row survives, so a missing keep decision does not compile.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object Float64Pipeline:

  def FF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
  ): ColumnBatch =
    fuse(batch, true, (value, gate) => first(value) && second(value) && accept(gate, value))

  def PF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && accept(gate, mapped0)
      },
    )

  def FP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
  ): ColumnBatch =
    fuse(batch, true, (value, gate) => first(value) && accept(gate, second(value)))

  def PP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
  ): ColumnBatch =
    fuse(batch, false, (value, gate) => accept(gate, second(first(value))))

  def FFF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => first(value) && second(value) && third(value) && accept(gate, value),
    )

  def PFF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && third(mapped0) && accept(gate, mapped0)
      },
    )

  def FPF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) =>
        first(value) &&
          {
            val mapped1 = second(value)
            third(mapped1) && accept(gate, mapped1)
          },
    )

  def PPF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => {
        val mapped1 = second(first(value))
        third(mapped1) && accept(gate, mapped1)
      },
    )

  def FFP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Map,
  ): ColumnBatch =
    fuse(batch, true, (value, gate) => first(value) && second(value) && accept(gate, third(value)))

  def PFP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Map,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && accept(gate, third(mapped0))
      },
    )

  def FPP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Map,
  ): ColumnBatch =
    fuse(batch, true, (value, gate) => first(value) && accept(gate, third(second(value))))

  def PPP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Map,
  ): ColumnBatch =
    fuse(batch, false, (value, gate) => accept(gate, third(second(first(value)))))

  def FFFF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) =>
        first(value) && second(value) && third(value) && fourth(value) && accept(gate, value),
    )

  def PFFF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && third(mapped0) && fourth(mapped0) && accept(gate, mapped0)
      },
    )

  def FPFF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) =>
        first(value) &&
          {
            val mapped1 = second(value)
            third(mapped1) && fourth(mapped1) && accept(gate, mapped1)
          },
    )

  def PPFF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => {
        val mapped1 = second(first(value))
        third(mapped1) && fourth(mapped1) && accept(gate, mapped1)
      },
    )

  def FFPF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) =>
        first(value) &&
          second(value) &&
          {
            val mapped2 = third(value)
            fourth(mapped2) && accept(gate, mapped2)
          },
    )

  def PFPF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) &&
        {
          val mapped2 = third(mapped0)
          fourth(mapped2) && accept(gate, mapped2)
        }
      },
    )

  def FPPF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) =>
        first(value) &&
          {
            val mapped2 = third(second(value))
            fourth(mapped2) && accept(gate, mapped2)
          },
    )

  def PPPF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => {
        val mapped2 = third(second(first(value)))
        fourth(mapped2) && accept(gate, mapped2)
      },
    )

  def FFFP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => first(value) && second(value) && third(value) && accept(gate, fourth(value)),
    )

  def PFFP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && third(mapped0) && accept(gate, fourth(mapped0))
      },
    )

  def FPFP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) =>
        first(value) &&
          {
            val mapped1 = second(value)
            third(mapped1) && accept(gate, fourth(mapped1))
          },
    )

  def PPFP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => {
        val mapped1 = second(first(value))
        third(mapped1) && accept(gate, fourth(mapped1))
      },
    )

  def FFPP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => first(value) && second(value) && accept(gate, fourth(third(value))),
    )

  def PFPP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && accept(gate, fourth(third(mapped0)))
      },
    )

  def FPPP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(batch, true, (value, gate) => first(value) && accept(gate, fourth(third(second(value)))))

  def PPPP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(batch, false, (value, gate) => accept(gate, fourth(third(second(first(value))))))

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class Gate:
    var value: Double = 0.0

  /** Returns whether the row survives, so a pattern that forgets the decision does not compile. */
  private inline def accept(gate: Gate, value: Double): Boolean =
    gate.value = value
    true

  private inline def fuse(
      batch: ColumnBatch,
      inline compact: Boolean,
      inline step: (Double, Gate) => Boolean,
  ): ColumnBatch =
    require(
      batch.columns.length == 1,
      s"float pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = ColumnKernel.asFloat64(ColumnKernel.columnAt(batch, 0), 0)
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val capacity = inline if (compact) then live else in.length
    val out = new Array[Double](capacity)
    val gate = new Gate
    var written = 0
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        val kept = step(in(row), gate)
        inline if (compact) then
          if (kept) then
            out(written) = gate.value
            written += 1
        else if (kept) then out(row) = gate.value
      row += 1
    }
    val length = inline if (compact) then written else live
    val validity =
      inline if (compact) then Validity.prefixValid(capacity, written) else column.validity
    val result = Float64Column.of(out, validity, length)
    new ColumnBatch(batch.schema, Vector(result), length)
