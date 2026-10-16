package com.ewoodbury.scarlet.columnar

/**
 * Reduces finished batches to a scalar.
 *
 * `count` is the sum of batch lengths, so a null row still counts. `sumInt32` and `sumFloat64`
 * skip null slots. A fully present column adds values with no per-row bit test. An empty batch
 * contributes zero.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object ColumnReduce:

  def count(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L)((total, batch) => total + batch.length)

  def sumInt32(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L)((total, batch) => total + sumInt32(int32(batch)))

  def sumFloat64(batches: Vector[ColumnBatch]): Double =
    batches.foldLeft(0.0)((total, batch) => total + sumFloat64(float64(batch)))

  private def sumInt32(column: Int32Column): Long =
    val live = column.length
    val values = column.values
    var total = 0L
    var row = 0
    if (ColumnHash.fullyPresent(column)) then
      while (row < live) {
        total += values(row).toLong
        row += 1
      }
    else
      val words = column.validity.words
      while (row < live) {
        if (Validity.isSet(words, row)) then total += values(row).toLong
        row += 1
      }
    total

  private def sumFloat64(column: Float64Column): Double =
    val live = column.length
    val values = column.values
    var total = 0.0
    var row = 0
    if (ColumnHash.fullyPresent(column)) then
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

  private def int32(batch: ColumnBatch): Int32Column =
    require(batch.columns.length == 1, s"int sum expects one column, got ${batch.columns.length}")
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)

  private def float64(batch: ColumnBatch): Float64Column =
    require(
      batch.columns.length == 1,
      s"double sum expects one column, got ${batch.columns.length}",
    )
    ColumnKernel.asFloat64(ColumnKernel.columnAt(batch, 0), 0)
