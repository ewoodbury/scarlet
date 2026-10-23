package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Presence bits for a column. Bit 0 of word 0 is row 0. A set bit means the value is present; a
 * clear bit means null. The payload array is not consulted for a null row.
 *
 * `capacity` is the number of rows the bitmap can describe. A column may use a shorter live
 * `length`.
 */
final class Validity private (val words: Array[Long], val capacity: Int):

  def isValid(row: Int): Boolean =
    require(row >= 0 && row < capacity, s"row $row outside validity capacity $capacity")
    Validity.isSet(words, row)

object Validity:

  /**
   * Every row below `live` is present. Spare bits past `live` are ignored, so a filter result
   * whose bitmap is longer than the survivor count still qualifies.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[columnar] def livePresent(validity: Validity, live: Int): Boolean =
    if (live == 0) then true
    else
      val words = validity.words
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

  /**
   * Every bit in `[start, start + length)` is set. An empty range is present. The caller keeps the
   * range inside the bitmap.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[columnar] def rangePresent(validity: Validity, start: Int, length: Int): Boolean =
    if (length <= 0) then true
    else
      val words = validity.words
      val end = start + length
      var row = start
      var dense = true
      while (dense && row < end && (row & 63) != 0) {
        dense = isSet(words, row)
        row += 1
      }
      while (dense && end - row >= 64) {
        if (words(row >>> 6) != -1L) then dense = false
        row += 64
      }
      while (dense && row < end) {
        dense = isSet(words, row)
        row += 1
      }
      dense

  /** Row is in range and `words` covers it. Kernels use this on the hot path. */
  private[columnar] inline def isSet(words: Array[Long], row: Int): Boolean =
    ((words(row >>> 6) >>> (row & 63)) & 1L) != 0L

  /** Sets bit `row`. The caller guarantees `row` is inside the bitmap. */
  private[columnar] inline def setBit(words: Array[Long], row: Int): Unit =
    val wordIndex = row >>> 6
    words(wordIndex) = words(wordIndex) | (1L << (row & 63))

  private[columnar] def allocate(capacity: Int): Array[Long] =
    require(capacity >= 0, s"capacity must be >= 0, got $capacity")
    new Array[Long](wordCount(capacity))

  private[columnar] def fromWords(words: Array[Long], capacity: Int): Validity =
    require(capacity >= 0, s"capacity must be >= 0, got $capacity")
    require(
      words.length == wordCount(capacity),
      s"validity words ${words.length} do not match capacity $capacity",
    )
    new Validity(words, capacity)

  /**
   * The first `live` rows are present. `capacity` may be larger. Bits at `live` and beyond are
   * clear.
   */
  private[columnar] def prefixValid(capacity: Int, live: Int): Validity =
    require(capacity >= 0, s"capacity must be >= 0, got $capacity")
    require(live >= 0 && live <= capacity, s"live $live outside capacity $capacity")
    val words = new Array[Long](wordCount(capacity))
    val fullWords = live >>> 6
    Arrays.fill(words, 0, fullWords, -1L)
    val tail = live & 63
    if (tail != 0) then words(fullWords) = (1L << tail) - 1L
    new Validity(words, capacity)

  def allValid(capacity: Int): Validity =
    require(capacity >= 0, s"capacity must be >= 0, got $capacity")
    val words = new Array[Long](wordCount(capacity))
    Arrays.fill(words, -1L)
    val tail = capacity & 63
    if (tail != 0) then words(words.length - 1) = (1L << tail) - 1L
    new Validity(words, capacity)

  /** `present(row) == true` means row `row` is not null. */
  def pack(capacity: Int, present: Int => Boolean): Validity =
    require(capacity >= 0, s"capacity must be >= 0, got $capacity")
    val words = new Array[Long](wordCount(capacity))
    (0 until capacity).foreach { row =>
      if (present(row)) then
        val wordIndex = row >>> 6
        words(wordIndex) = words(wordIndex) | (1L << (row & 63))
    }
    new Validity(words, capacity)

  private def wordCount(capacity: Int): Int =
    (capacity + 63) >>> 6
