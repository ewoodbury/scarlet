package com.ewoodbury.scarlet.columnar

/**
 * Mixing and sizing for the open-addressed columnar hash kernels.
 *
 * `tableSize` is a power of two, at least `max(16, rows * 2)`, and at most [[MaxCapacity]]. A
 * table that holds one slot per input row then stays at or under half full, so a single batch does
 * not rehash.
 */
private[columnar] object ColumnHash:

  val MaxCapacity: Int = 1 << 30

  def tableSize(rows: Int): Int =
    val need =
      if (rows > (MaxCapacity >>> 1)) MaxCapacity
      else math.max(16, rows << 1)
    if (need >= MaxCapacity) MaxCapacity
    else 1 << (32 - Integer.numberOfLeadingZeros(need - 1))

  def mix(key: Int): Int =
    val first = (key ^ (key >>> 16)) * 0x7feb352d
    val second = (first ^ (first >>> 15)) * 0x846ca68b
    second ^ (second >>> 16)

  /** Every live row is present, and the bitmap has no spare capacity past `length`. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def fullyPresent(column: Column): Boolean =
    val live = column.length
    if (live != column.validity.capacity) then false
    else
      val words = column.validity.words
      val fullWords = live >>> 6
      var index = 0
      var dense = true
      while (index < fullWords && dense) {
        if (words(index) != -1L) then dense = false
        index += 1
      }
      val tail = live & 63
      if (dense && tail != 0) then words(fullWords) == (1L << tail) - 1L else dense

  def int32At(batch: ColumnBatch, ordinal: Int): Int32Column =
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, ordinal), ordinal)

  def int64At(batch: ColumnBatch, ordinal: Int): Int64Column =
    ColumnKernel.asInt64(ColumnKernel.columnAt(batch, ordinal), ordinal)
