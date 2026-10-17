package com.ewoodbury.scarlet.columnar

/**
 * Reduces finished batches to a scalar.
 *
 * `count` includes null slots. Sums skip them. When every live row is present the add does not
 * test the bitmap, including a filter result whose bitmap is longer than the survivor count.
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
    if (liveRowsPresent(column)) then
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
    if (liveRowsPresent(column)) then
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

  /** Spare capacity past `length` does not count. A hole in the live prefix does. */
  private def liveRowsPresent(column: Column): Boolean =
    val live = column.length
    if (live == 0) then true
    else
      val words = column.validity.words
      val fullWords = live >>> 6
      var index = 0
      var dense = true
      while (index < fullWords && dense) {
        if (words(index) != -1L) then dense = false
        index += 1
      }
      if (!dense) then false
      else
        val tail = live & 63
        tail == 0 || (words(fullWords) & ((1L << tail) - 1L)) == (1L << tail) - 1L

  private def int32(batch: ColumnBatch): Int32Column =
    require(batch.columns.length == 1, s"int sum expects one column, got ${batch.columns.length}")
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)

  private def float64(batch: ColumnBatch): Float64Column =
    require(
      batch.columns.length == 1,
      s"double sum expects one column, got ${batch.columns.length}",
    )
    ColumnKernel.asFloat64(ColumnKernel.columnAt(batch, 0), 0)
