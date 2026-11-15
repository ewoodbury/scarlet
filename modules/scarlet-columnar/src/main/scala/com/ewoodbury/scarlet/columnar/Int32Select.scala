package com.ewoodbury.scarlet.columnar

import ColumnKernel.Int32Predicate

/**
 * Int-slot survivor index for [[Selection]].
 *
 * An all-valid range tests the predicate once per row and does not test the validity bit. When
 * that scan keeps every row, the ids are `start + i`: a straight fill, not a store that moves only
 * on a hit. A range with a null stays on the bit test. A null does not call the predicate. Each
 * row is tested at most once.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object Int32Select:

  def seed(
      column: Int32Column,
      start: Int,
      length: Int,
      predicate: Int32Predicate,
      index: Array[Int],
  ): Int =
    val values = column.values
    val end = start + length
    val keep = captured(predicate)
    if (Validity.rangePresent(column.validity, start, length)) then
      var row = start
      while (row < end && keep(values(row))) {
        row += 1
      }
      if (row == end) then fillIdentity(index, start, length)
      else compactTail(values, keep, index, start, row, end)
    else seedChecked(values, column.validity.words, keep, index, start, end)

  def restrict(
      column: Int32Column,
      predicate: Int32Predicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val values = column.values
    val keep = captured(predicate)
    if (Validity.livePresent(column.validity, column.length)) then
      var read = 0
      while (read < count && keep(values(index(read)))) {
        read += 1
      }
      if (read == count) then count
      else
        var written = read
        read += 1
        while (read < count) {
          val row = index(read)
          if (keep(values(row))) then
            index(written) = row
            written += 1
          read += 1
        }
        written
    else restrictChecked(values, column.validity.words, keep, index, count)

  /** One function for the row loop. A bare trait is wrapped once here, not once per row. */
  private def captured(predicate: Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        value => fallback(value)

  /** `index(i) = start + i`. The address does not depend on the predicate. */
  private def fillIdentity(index: Array[Int], start: Int, length: Int): Int =
    var slot = 0
    while (slot < length) {
      index(slot) = start + slot
      slot += 1
    }
    length

  /**
   * Rows `[start, failed)` passed and `failed` did not. The prefix is the identity. The tail is
   * compacted. `failed` is not tested again.
   */
  private def compactTail(
      values: Array[Int],
      keep: Int => Boolean,
      index: Array[Int],
      start: Int,
      failed: Int,
      end: Int,
  ): Int =
    val passed = failed - start
    var slot = 0
    while (slot < passed) {
      index(slot) = start + slot
      slot += 1
    }
    var written = passed
    var row = failed + 1
    while (row < end) {
      if (keep(values(row))) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def seedChecked(
      values: Array[Int],
      words: Array[Long],
      keep: Int => Boolean,
      index: Array[Int],
      start: Int,
      end: Int,
  ): Int =
    var row = start
    var written = 0
    while (row < end) {
      if (Validity.isSet(words, row) && keep(values(row))) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  private def restrictChecked(
      values: Array[Int],
      words: Array[Long],
      keep: Int => Boolean,
      index: Array[Int],
      count: Int,
  ): Int =
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (Validity.isSet(words, row) && keep(values(row))) then
        index(written) = row
        written += 1
      read += 1
    }
    written
