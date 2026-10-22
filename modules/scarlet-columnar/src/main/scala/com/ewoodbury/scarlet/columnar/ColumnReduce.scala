package com.ewoodbury.scarlet.columnar

/**
 * Reduces finished batches to a scalar.
 *
 * `count` includes null slots. Sums skip them. When every live row is present the add does not
 * test the bitmap, including a filter result whose bitmap is longer than the survivor count.
 *
 * `sumInt32` and `sumFloat64` read column 0 of a one-column batch. `sumInt64` reads the `Int64`
 * measure of a wider batch, which is how a grouped sum folds its two-column result.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object ColumnReduce:

  def count(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L)((total, batch) => total + batch.length)

  def sumInt32(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L)((total, batch) => total + sumInt32(int32(batch)))

  /**
   * Sums the `Int64` column at `ordinal`. Other columns are left unread. Null slots are skipped.
   */
  def sumInt64(batches: Vector[ColumnBatch], ordinal: Int): Long =
    batches.foldLeft(0L)((total, batch) => total + sumInt64(int64(batch, ordinal)))

  def sumFloat64(batches: Vector[ColumnBatch]): Double =
    batches.foldLeft(0.0)((total, batch) => total + sumFloat64(float64(batch)))

  private def sumInt32(column: Int32Column): Long =
    val live = column.length
    val values = column.values
    var total = 0L
    var row = 0
    if (Validity.livePresent(column.validity, live)) then
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

  private def sumInt64(column: Int64Column): Long =
    val live = column.length
    val values = column.values
    var total = 0L
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

  private def sumFloat64(column: Float64Column): Double =
    val live = column.length
    val values = column.values
    var total = 0.0
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

  private def int32(batch: ColumnBatch): Int32Column =
    require(batch.columns.length == 1, s"int sum expects one column, got ${batch.columns.length}")
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    ColumnKernel.asInt64(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def float64(batch: ColumnBatch): Float64Column =
    require(
      batch.columns.length == 1,
      s"double sum expects one column, got ${batch.columns.length}",
    )
    ColumnKernel.asFloat64(ColumnKernel.columnAt(batch, 0), 0)
