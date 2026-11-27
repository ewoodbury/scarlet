package com.ewoodbury.scarlet.columnar

/**
 * Filter, project, and sum of one `Float32` column.
 *
 * A null stays null. The predicate and the mapper are not called on it. A filter drops that row. A
 * project copies the clear bit and stores `0.0f`. `NaN` and `-0.0f` pass through with their bits.
 * The sum skips nulls and adds `Float`, starting at `0.0f`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object Float32Kernel:

  trait Float32Predicate:
    def apply(value: Float): Boolean

  trait Float32Map:
    def apply(value: Float): Float

  /** Scans `[start, start + length)`. Survivors stay in input order. Nulls are dropped. */
  def filter(
      batch: ColumnBatch,
      ordinal: Int,
      start: Int,
      length: Int,
      predicate: Float32Predicate,
  ): ColumnBatch =
    require(ordinal == 0, s"float32 filter reads column 0, got $ordinal")
    val column = float32(batch)
    Ranges.check(start, length, column.length)
    require(batch.columns.length == 1, s"float32 filter expects one column, got ${batch.schema}")
    val in = column.values
    val words = column.validity.words
    val out = new Array[Float](length)
    val end = start + length
    var written = 0
    var row = start
    while (row < end) {
      if (Validity.isSet(words, row)) then
        val value = in(row)
        if (predicate(value)) then
          out(written) = value
          written += 1
      row += 1
    }
    publish(batch, out, Validity.prefixValid(length, written), written)

  /**
   * Maps `[start, start + length)`. Output row 0 is source row `start`. The output length is
   * `length`, including nulls. A null stays null and the mapper is not called.
   */
  def project(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapper: Float32Map,
  ): ColumnBatch =
    val column = float32(batch)
    Ranges.check(start, length, column.length)
    require(batch.columns.length == 1, s"float32 project expects one column, got ${batch.schema}")
    val mapped =
      if (start == 0 && length == column.length) then mapColumn(column, mapper)
      else mapRange(column, start, length, mapper)
    new ColumnBatch(batch.schema, Vector(mapped), mapped.length)

  /** Skips nulls. An empty input is `0.0f`. */
  def sum(batches: Vector[ColumnBatch]): Float =
    batches.foldLeft(0.0f)((total, batch) => total + sumColumn(float32(batch)))

  private def mapColumn(column: Float32Column, mapper: Float32Map): Float32Column =
    val live = column.length
    val in = column.values
    val words = column.validity.words
    val out = new Array[Float](in.length)
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then out(row) = mapper(in(row))
      row += 1
    }
    Float32Column.of(out, column.validity, live)

  /** Writes the range at indexes `[0, length)`. Null slots stay `0.0f` and are left invalid. */
  private def mapRange(
      column: Float32Column,
      start: Int,
      length: Int,
      mapper: Float32Map,
  ): Float32Column =
    val in = column.values
    val words = column.validity.words
    val out = new Array[Float](length)
    val dstWords = Validity.allocate(length)
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (Validity.isSet(words, row)) then
        out(written) = mapper(in(row))
        Validity.setBit(dstWords, written)
      else out(written) = 0.0f
      written += 1
      row += 1
    }
    Float32Column.of(out, Validity.fromWords(dstWords, length), length)

  private def sumColumn(column: Float32Column): Float =
    val live = column.length
    val values = column.values
    var total = 0.0f
    var row = 0
    if (Validity.livePresent(column.validity, live)) then
      while (row < live) {
        total += values(row)
        row += 1
      }
    else
      val words = column.validity.words
      while (row < live) {
        if (Validity.isSet(words, row)) then total += values(row)
        row += 1
      }
    total

  private def publish(
      batch: ColumnBatch,
      values: Array[Float],
      validity: Validity,
      written: Int,
  ): ColumnBatch =
    val column = Float32Column.of(values, validity, written)
    new ColumnBatch(batch.schema, Vector(column), written)

  private def float32(batch: ColumnBatch): Float32Column =
    ColumnKernel.columnAt(batch, 0) match
      case column: Float32Column => column
      case other =>
        throw new IllegalArgumentException(
          s"column 0 is ${other.logicalType}, expected Float32",
        )
