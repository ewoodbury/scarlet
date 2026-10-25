package com.ewoodbury.scarlet.columnar

import ColumnKernel.{Float64Map, Float64Predicate}

/**
 * One loop per double filter/project pattern of length 2–4.
 *
 * The scan is `[start, start + length)`. The output covers that range. A filter drops a null
 * without calling the predicate. A project skips a null and stores 0. When every row of the range
 * is present, the loop does not test the validity bit. A whole batch is
 * [[Morsel]]`(batch, 0, batch.length)`. That map reuses the input validity bitmap. The step
 * returns whether the row survives, so a missing keep decision does not compile.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object Float64Pipeline:

  def FF(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Predicate,
  ): ColumnBatch =
    fuse(morsel, true, (value, gate) => first(value) && second(value) && accept(gate, value))

  def PF(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && accept(gate, mapped0)
      },
    )

  def FP(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Map,
  ): ColumnBatch =
    fuse(morsel, true, (value, gate) => first(value) && accept(gate, second(value)))

  def PP(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Map,
  ): ColumnBatch =
    fuse(morsel, false, (value, gate) => accept(gate, second(first(value))))

  def FFF(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => first(value) && second(value) && third(value) && accept(gate, value),
    )

  def PFF(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && third(mapped0) && accept(gate, mapped0)
      },
    )

  def FPF(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) =>
        first(value) &&
          {
            val mapped1 = second(value)
            third(mapped1) && accept(gate, mapped1)
          },
    )

  def PPF(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Map,
      third: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => {
        val mapped1 = second(first(value))
        third(mapped1) && accept(gate, mapped1)
      },
    )

  def FFP(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Map,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => first(value) && second(value) && accept(gate, third(value)),
    )

  def PFP(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Map,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && accept(gate, third(mapped0))
      },
    )

  def FPP(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Map,
  ): ColumnBatch =
    fuse(morsel, true, (value, gate) => first(value) && accept(gate, third(second(value))))

  def PPP(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Map,
      third: Float64Map,
  ): ColumnBatch =
    fuse(morsel, false, (value, gate) => accept(gate, third(second(first(value)))))

  def FFFF(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) =>
        first(value) && second(value) && third(value) && fourth(value) && accept(gate, value),
    )

  def PFFF(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && third(mapped0) && fourth(mapped0) && accept(gate, mapped0)
      },
    )

  def FPFF(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) =>
        first(value) &&
          {
            val mapped1 = second(value)
            third(mapped1) && fourth(mapped1) && accept(gate, mapped1)
          },
    )

  def PPFF(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => {
        val mapped1 = second(first(value))
        third(mapped1) && fourth(mapped1) && accept(gate, mapped1)
      },
    )

  def FFPF(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
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
      morsel: Morsel,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
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
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) =>
        first(value) &&
          {
            val mapped2 = third(second(value))
            fourth(mapped2) && accept(gate, mapped2)
          },
    )

  def PPPF(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Predicate,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => {
        val mapped2 = third(second(first(value)))
        fourth(mapped2) && accept(gate, mapped2)
      },
    )

  def FFFP(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => first(value) && second(value) && third(value) && accept(gate, fourth(value)),
    )

  def PFFP(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && third(mapped0) && accept(gate, fourth(mapped0))
      },
    )

  def FPFP(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) =>
        first(value) &&
          {
            val mapped1 = second(value)
            third(mapped1) && accept(gate, fourth(mapped1))
          },
    )

  def PPFP(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => {
        val mapped1 = second(first(value))
        third(mapped1) && accept(gate, fourth(mapped1))
      },
    )

  def FFPP(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => first(value) && second(value) && accept(gate, fourth(third(value))),
    )

  def PFPP(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(
      morsel,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && accept(gate, fourth(third(mapped0)))
      },
    )

  def FPPP(
      morsel: Morsel,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(morsel, true, (value, gate) => first(value) && accept(gate, fourth(third(second(value)))))

  def PPPP(
      morsel: Morsel,
      first: Float64Map,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Map,
  ): ColumnBatch =
    fuse(morsel, false, (value, gate) => accept(gate, fourth(third(second(first(value))))))

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class Gate:
    var value: Double = 0.0

  /** Returns whether the row survives, so a pattern that forgets the decision does not compile. */
  private inline def accept(gate: Gate, value: Double): Boolean =
    gate.value = value
    true

  def sumFF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
  ): Double =
    sumFloat(batch, true, (value, gate) => first(value) && second(value) && accept(gate, value))

  def sumPF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && accept(gate, mapped0)
      },
    )

  def sumFP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
  ): Double =
    sumFloat(batch, true, (value, gate) => first(value) && accept(gate, second(value)))

  def sumPP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
  ): Double =
    sumFloat(batch, false, (value, gate) => accept(gate, second(first(value))))

  def sumFFF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Predicate,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => first(value) && second(value) && third(value) && accept(gate, value),
    )

  def sumPFF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Predicate,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && third(mapped0) && accept(gate, mapped0)
      },
    )

  def sumFPF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Predicate,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) =>
        first(value) &&
          {
            val mapped1 = second(value)
            third(mapped1) && accept(gate, mapped1)
          },
    )

  def sumPPF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Predicate,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => {
        val mapped1 = second(first(value))
        third(mapped1) && accept(gate, mapped1)
      },
    )

  def sumFFP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Map,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => first(value) && second(value) && accept(gate, third(value)),
    )

  def sumPFP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Map,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && accept(gate, third(mapped0))
      },
    )

  def sumFPP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Map,
  ): Double =
    sumFloat(batch, true, (value, gate) => first(value) && accept(gate, third(second(value))))

  def sumPPP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Map,
  ): Double =
    sumFloat(batch, false, (value, gate) => accept(gate, third(second(first(value)))))

  def sumFFFF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) =>
        first(value) && second(value) && third(value) && fourth(value) && accept(gate, value),
    )

  def sumPFFF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && third(mapped0) && fourth(mapped0) && accept(gate, mapped0)
      },
    )

  def sumFPFF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) =>
        first(value) &&
          {
            val mapped1 = second(value)
            third(mapped1) && fourth(mapped1) && accept(gate, mapped1)
          },
    )

  def sumPPFF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Predicate,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => {
        val mapped1 = second(first(value))
        third(mapped1) && fourth(mapped1) && accept(gate, mapped1)
      },
    )

  def sumFFPF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Predicate,
  ): Double =
    sumFloat(
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

  def sumPFPF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Predicate,
  ): Double =
    sumFloat(
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

  def sumFPPF(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Predicate,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) =>
        first(value) &&
          {
            val mapped2 = third(second(value))
            fourth(mapped2) && accept(gate, mapped2)
          },
    )

  def sumPPPF(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Predicate,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => {
        val mapped2 = third(second(first(value)))
        fourth(mapped2) && accept(gate, mapped2)
      },
    )

  def sumFFFP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Map,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => first(value) && second(value) && third(value) && accept(gate, fourth(value)),
    )

  def sumPFFP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Predicate,
      fourth: Float64Map,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && third(mapped0) && accept(gate, fourth(mapped0))
      },
    )

  def sumFPFP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Map,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) =>
        first(value) &&
          {
            val mapped1 = second(value)
            third(mapped1) && accept(gate, fourth(mapped1))
          },
    )

  def sumPPFP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Predicate,
      fourth: Float64Map,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => {
        val mapped1 = second(first(value))
        third(mapped1) && accept(gate, fourth(mapped1))
      },
    )

  def sumFFPP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Map,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => first(value) && second(value) && accept(gate, fourth(third(value))),
    )

  def sumPFPP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Predicate,
      third: Float64Map,
      fourth: Float64Map,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => {
        val mapped0 = first(value)
        second(mapped0) && accept(gate, fourth(third(mapped0)))
      },
    )

  def sumFPPP(
      batch: ColumnBatch,
      first: Float64Predicate,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Map,
  ): Double =
    sumFloat(
      batch,
      true,
      (value, gate) => first(value) && accept(gate, fourth(third(second(value)))),
    )

  def sumPPPP(
      batch: ColumnBatch,
      first: Float64Map,
      second: Float64Map,
      third: Float64Map,
      fourth: Float64Map,
  ): Double =
    sumFloat(batch, false, (value, gate) => accept(gate, fourth(third(second(first(value))))))

  def sumFiltered(batch: ColumnBatch, predicate: Float64Predicate): Double =
    sumFloat(batch, true, (value, gate) => predicate(value) && accept(gate, value))

  def sumProjected(batch: ColumnBatch, mapper: Float64Map): Double =
    sumFloat(batch, false, (value, gate) => accept(gate, mapper(value)))

  /** Add the fused values. Does not allocate an output column. */
  private inline def sumFloat(
      batch: ColumnBatch,
      inline compact: Boolean,
      inline step: (Double, Gate) => Boolean,
  ): Double =
    require(
      batch.columns.length == 1,
      s"float pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = ColumnKernel.asFloat64(ColumnKernel.columnAt(batch, 0), 0)
    val live = column.length
    val values = column.values
    val words = column.validity.words
    val gate = new Gate
    var total = 0.0
    var row = 0
    if (Validity.livePresent(column.validity, live)) then
      while (row < live) {
        val kept = step(values(row), gate)
        inline if (compact) then
          if (kept) then total += gate.value
        else if (kept) then total += gate.value
        row += 1
      }
    else
      while (row < live) {
        if (Validity.isSet(words, row)) then
          val kept = step(values(row), gate)
          inline if (compact) then
            if (kept) then total += gate.value
          else if (kept) then total += gate.value
        row += 1
      }
    total

  private inline def fuse(
      morsel: Morsel,
      inline compact: Boolean,
      inline step: (Double, Gate) => Boolean,
  ): ColumnBatch =
    val batch = morsel.batch
    require(
      batch.columns.length == 1,
      s"float pipeline expects a single-column batch, got ${batch.columns.length}",
    )
    val column = ColumnKernel.asFloat64(ColumnKernel.columnAt(batch, 0), 0)
    val start = morsel.start
    val length = morsel.length
    Ranges.check(start, length, column.length)
    val in = column.values
    val words = column.validity.words
    val end = start + length
    val gate = new Gate
    val dense = Validity.rangePresent(column.validity, start, length)
    inline if (compact) then
      val out = new Array[Double](length)
      var written = 0
      var row = start
      if (dense) then
        while (row < end) {
          val kept = step(in(row), gate)
          if (kept) then
            out(written) = gate.value
            written += 1
          row += 1
        }
      else
        while (row < end) {
          if (Validity.isSet(words, row)) then
            val kept = step(in(row), gate)
            if (kept) then
              out(written) = gate.value
              written += 1
          row += 1
        }
      publish(batch, out, Validity.prefixValid(length, written), written)
    else
      val wholeColumn = start == 0 && length == column.length
      val capacity = if (wholeColumn) then in.length else length
      val out = new Array[Double](capacity)
      if (wholeColumn) then
        var row = start
        if (dense) then
          while (row < end) {
            val kept = step(in(row), gate)
            if (kept) then out(row) = gate.value
            row += 1
          }
        else
          while (row < end) {
            if (Validity.isSet(words, row)) then
              val kept = step(in(row), gate)
              if (kept) then out(row) = gate.value
            row += 1
          }
        publish(batch, out, column.validity, length)
      else if (dense) then
        var offset = 0
        var row = start
        while (row < end) {
          val kept = step(in(row), gate)
          if (kept) then out(offset) = gate.value
          offset += 1
          row += 1
        }
        publish(batch, out, Validity.allValid(length), length)
      else
        val dstWords = Validity.allocate(length)
        var offset = 0
        var row = start
        while (row < end) {
          if (Validity.isSet(words, row)) then
            val kept = step(in(row), gate)
            if (kept) then
              out(offset) = gate.value
              Validity.setBit(dstWords, offset)
          offset += 1
          row += 1
        }
        publish(batch, out, Validity.fromWords(dstWords, length), length)

  private def publish(
      batch: ColumnBatch,
      out: Array[Double],
      validity: Validity,
      length: Int,
  ): ColumnBatch =
    val column = Float64Column.of(out, validity, length)
    new ColumnBatch(batch.schema, Vector(column), length)
