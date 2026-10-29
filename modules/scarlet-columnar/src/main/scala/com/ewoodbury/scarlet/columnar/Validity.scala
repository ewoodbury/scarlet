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

  /** Word that holds bit `bit`. */
  private[columnar] inline def wordIndex(bit: Int): Int = bit >>> 6

  /** First bit index of word `word`. */
  private[columnar] inline def wordStart(word: Int): Int = word << 6

  /** Position of `bit` inside its word, from 0 to 63. */
  private[columnar] inline def bitOffset(bit: Int): Int = bit & 63

  /** Count of complete 64-bit words in a prefix of `live` rows. */
  private[columnar] inline def liveWords(live: Int): Int = live >>> 6

  /** Rows past the last full word of a prefix of `live` rows, from 0 to 63. */
  private[columnar] inline def tailBits(live: Int): Int = live & 63

  /**
   * The low `bits` bits set. `bits` is 0 to 63, and `bits == 0` yields `0L`. A count of 64 wraps
   * to a zero-bit shift: the JVM masks the shift distance, so `1L << 64` is `1L << 0`. Callers
   * that need a full word keep a `width == 64` guard and do not pass 64 here.
   */
  private[columnar] inline def tailMask(bits: Int): Long =
    (1L << bits) - 1L

  /**
   * Returns true when every row in `[0, live)` is present, and false when any of those rows is
   * null. `live == 0` returns true. Bits at index `live` and beyond are ignored, so a filter
   * result whose bitmap is longer than the survivor count still returns true. This is not
   * [[allValid]]: spare capacity past `live` may be clear, and a null inside the live prefix
   * returns false even when every other bit is set.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[columnar] def livePresent(validity: Validity, live: Int): Boolean =
    if (live == 0) then true
    else
      val words = validity.words
      val fullWords = liveWords(live)
      var index = 0
      var dense = true
      while (index < fullWords && dense) {
        if (words(index) != -1L) then dense = false
        index += 1
      }
      if (!dense) then false
      else
        val tail = tailBits(live)
        if (tail == 0) then true
        else
          val mask = tailMask(tail)
          (words(fullWords) & mask) == mask

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
      while (dense && row < end && bitOffset(row) != 0) {
        dense = isSet(words, row)
        row += 1
      }
      while (dense && end - row >= 64) {
        if (words(wordIndex(row)) != -1L) then dense = false
        row += 64
      }
      while (dense && row < end) {
        dense = isSet(words, row)
        row += 1
      }
      dense

  /** Row is in range and `words` covers it. Kernels use this on the hot path. */
  private[columnar] inline def isSet(words: Array[Long], row: Int): Boolean =
    ((words(wordIndex(row)) >>> bitOffset(row)) & 1L) != 0L

  /** Sets bit `row`. The caller guarantees `row` is inside the bitmap. */
  private[columnar] inline def setBit(words: Array[Long], row: Int): Unit =
    val index = wordIndex(row)
    words(index) = words(index) | (1L << bitOffset(row))

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
    val fullWords = liveWords(live)
    Arrays.fill(words, 0, fullWords, -1L)
    val tail = tailBits(live)
    if (tail != 0) then words(fullWords) = tailMask(tail)
    new Validity(words, capacity)

  def allValid(capacity: Int): Validity =
    require(capacity >= 0, s"capacity must be >= 0, got $capacity")
    val words = new Array[Long](wordCount(capacity))
    Arrays.fill(words, -1L)
    val tail = tailBits(capacity)
    if (tail != 0) then words(liveWords(capacity)) = tailMask(tail)
    new Validity(words, capacity)

  /** `present(row) == true` means row `row` is not null. */
  def pack(capacity: Int, present: Int => Boolean): Validity =
    require(capacity >= 0, s"capacity must be >= 0, got $capacity")
    val words = new Array[Long](wordCount(capacity))
    (0 until capacity).foreach { row =>
      if (present(row)) then setBit(words, row)
    }
    new Validity(words, capacity)

  private def wordCount(capacity: Int): Int =
    (capacity + 63) >>> 6
