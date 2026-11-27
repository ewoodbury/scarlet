package com.ewoodbury.scarlet.columnar

/**
 * Minimum and maximum of one column across finished batches.
 *
 * Null slots are skipped. An empty column, or a column of only nulls, is zero of that column's
 * type. The scan of each column is its own loop. A present value of zero is a real result.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object ColumnExtremum:

  def minInt32(batches: Vector[ColumnBatch], ordinal: Int): Int =
    acrossInt(batches, ordinal, least = true)

  def maxInt32(batches: Vector[ColumnBatch], ordinal: Int): Int =
    acrossInt(batches, ordinal, least = false)

  def minInt64(batches: Vector[ColumnBatch], ordinal: Int): Long =
    acrossLong(batches, ordinal, least = true)

  def maxInt64(batches: Vector[ColumnBatch], ordinal: Int): Long =
    acrossLong(batches, ordinal, least = false)

  def minFloat32(batches: Vector[ColumnBatch], ordinal: Int): Float =
    acrossFloat(batches, ordinal, least = true)

  def maxFloat32(batches: Vector[ColumnBatch], ordinal: Int): Float =
    acrossFloat(batches, ordinal, least = false)

  def minFloat64(batches: Vector[ColumnBatch], ordinal: Int): Double =
    acrossDouble(batches, ordinal, least = true)

  def maxFloat64(batches: Vector[ColumnBatch], ordinal: Int): Double =
    acrossDouble(batches, ordinal, least = false)

  private def acrossInt(batches: Vector[ColumnBatch], ordinal: Int, least: Boolean): Int =
    var best = 0
    var seen = false
    var index = 0
    while (index < batches.length) {
      val found =
        if (least) then leastInt32(int32(batches(index), ordinal))
        else greatestInt32(int32(batches(index), ordinal))
      found.foreach { value =>
        if (!seen || (least && value < best) || (!least && value > best)) then best = value
        seen = true
      }
      index += 1
    }
    best

  private def acrossLong(batches: Vector[ColumnBatch], ordinal: Int, least: Boolean): Long =
    var best = 0L
    var seen = false
    var index = 0
    while (index < batches.length) {
      val found =
        if (least) then leastInt64(int64(batches(index), ordinal))
        else greatestInt64(int64(batches(index), ordinal))
      found.foreach { value =>
        if (!seen || (least && value < best) || (!least && value > best)) then best = value
        seen = true
      }
      index += 1
    }
    best

  private def acrossFloat(batches: Vector[ColumnBatch], ordinal: Int, least: Boolean): Float =
    var best = 0.0f
    var seen = false
    var index = 0
    while (index < batches.length) {
      val found =
        if (least) then leastFloat32(float32(batches(index), ordinal))
        else greatestFloat32(float32(batches(index), ordinal))
      found.foreach { value =>
        if (!seen || (least && value < best) || (!least && value > best)) then best = value
        seen = true
      }
      index += 1
    }
    best

  private def acrossDouble(batches: Vector[ColumnBatch], ordinal: Int, least: Boolean): Double =
    var best = 0.0
    var seen = false
    var index = 0
    while (index < batches.length) {
      val found =
        if (least) then leastFloat64(float64(batches(index), ordinal))
        else greatestFloat64(float64(batches(index), ordinal))
      found.foreach { value =>
        if (!seen || (least && value < best) || (!least && value > best)) then best = value
        seen = true
      }
      index += 1
    }
    best

  private def leastInt32(column: Int32Column): Option[Int] =
    val live = column.length
    val values = column.values
    if (live == 0) then None
    else if (Validity.livePresent(column.validity, live)) then
      var best = values(0)
      var row = 1
      while (row < live) {
        val value = values(row)
        if (value < best) then best = value
        row += 1
      }
      Some(best)
    else scanInt32(values, column.validity.words, live, least = true)

  private def greatestInt32(column: Int32Column): Option[Int] =
    val live = column.length
    val values = column.values
    if (live == 0) then None
    else if (Validity.livePresent(column.validity, live)) then
      var best = values(0)
      var row = 1
      while (row < live) {
        val value = values(row)
        if (value > best) then best = value
        row += 1
      }
      Some(best)
    else scanInt32(values, column.validity.words, live, least = false)

  private def scanInt32(
      values: Array[Int],
      words: Array[Long],
      live: Int,
      least: Boolean,
  ): Option[Int] =
    var best = 0
    var seen = false
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        val value = values(row)
        if (!seen || (least && value < best) || (!least && value > best)) then best = value
        seen = true
      row += 1
    }
    if (seen) then Some(best) else None

  private def leastInt64(column: Int64Column): Option[Long] =
    val live = column.length
    val values = column.values
    if (live == 0) then None
    else if (Validity.livePresent(column.validity, live)) then
      var best = values(0)
      var row = 1
      while (row < live) {
        val value = values(row)
        if (value < best) then best = value
        row += 1
      }
      Some(best)
    else scanInt64(values, column.validity.words, live, least = true)

  private def greatestInt64(column: Int64Column): Option[Long] =
    val live = column.length
    val values = column.values
    if (live == 0) then None
    else if (Validity.livePresent(column.validity, live)) then
      var best = values(0)
      var row = 1
      while (row < live) {
        val value = values(row)
        if (value > best) then best = value
        row += 1
      }
      Some(best)
    else scanInt64(values, column.validity.words, live, least = false)

  private def scanInt64(
      values: Array[Long],
      words: Array[Long],
      live: Int,
      least: Boolean,
  ): Option[Long] =
    var best = 0L
    var seen = false
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        val value = values(row)
        if (!seen || (least && value < best) || (!least && value > best)) then best = value
        seen = true
      row += 1
    }
    if (seen) then Some(best) else None

  private def leastFloat32(column: Float32Column): Option[Float] =
    val live = column.length
    val values = column.values
    if (live == 0) then None
    else if (Validity.livePresent(column.validity, live)) then
      var best = values(0)
      var row = 1
      while (row < live) {
        val value = values(row)
        if (value < best) then best = value
        row += 1
      }
      Some(best)
    else scanFloat32(values, column.validity.words, live, least = true)

  private def greatestFloat32(column: Float32Column): Option[Float] =
    val live = column.length
    val values = column.values
    if (live == 0) then None
    else if (Validity.livePresent(column.validity, live)) then
      var best = values(0)
      var row = 1
      while (row < live) {
        val value = values(row)
        if (value > best) then best = value
        row += 1
      }
      Some(best)
    else scanFloat32(values, column.validity.words, live, least = false)

  private def scanFloat32(
      values: Array[Float],
      words: Array[Long],
      live: Int,
      least: Boolean,
  ): Option[Float] =
    var best = 0.0f
    var seen = false
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        val value = values(row)
        if (!seen || (least && value < best) || (!least && value > best)) then best = value
        seen = true
      row += 1
    }
    if (seen) then Some(best) else None

  private def leastFloat64(column: Float64Column): Option[Double] =
    val live = column.length
    val values = column.values
    if (live == 0) then None
    else if (Validity.livePresent(column.validity, live)) then
      var best = values(0)
      var row = 1
      while (row < live) {
        val value = values(row)
        if (value < best) then best = value
        row += 1
      }
      Some(best)
    else scanFloat64(values, column.validity.words, live, least = true)

  private def greatestFloat64(column: Float64Column): Option[Double] =
    val live = column.length
    val values = column.values
    if (live == 0) then None
    else if (Validity.livePresent(column.validity, live)) then
      var best = values(0)
      var row = 1
      while (row < live) {
        val value = values(row)
        if (value > best) then best = value
        row += 1
      }
      Some(best)
    else scanFloat64(values, column.validity.words, live, least = false)

  private def scanFloat64(
      values: Array[Double],
      words: Array[Long],
      live: Int,
      least: Boolean,
  ): Option[Double] =
    var best = 0.0
    var seen = false
    var row = 0
    while (row < live) {
      if (Validity.isSet(words, row)) then
        val value = values(row)
        if (!seen || (least && value < best) || (!least && value > best)) then best = value
        seen = true
      row += 1
    }
    if (seen) then Some(best) else None

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    ColumnKernel.asInt64(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def float64(batch: ColumnBatch, ordinal: Int): Float64Column =
    ColumnKernel.asFloat64(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def float32(batch: ColumnBatch, ordinal: Int): Float32Column =
    ColumnKernel.columnAt(batch, ordinal) match
      case column: Float32Column => column
      case other =>
        throw new IllegalArgumentException(
          s"column $ordinal is ${other.logicalType}, expected Float32",
        )
