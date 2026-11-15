package com.ewoodbury.scarlet.columnar

import ColumnKernel.Int32Predicate

/**
 * One-step count and sum for [[Selection]].
 *
 * A two-step chain stays in [[Selection]]. One int slot, or one int pair, is its own `while`. When
 * every ordinal the step reads is present on the range, the loop does not test a validity bit. A
 * null measure still tests its own bit: that null adds nothing, and the predicate is not how the
 * measure is dropped. A null predicate column stays on the bit test and does not call the
 * predicate. An empty range returns zero.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object SlotFold:

  def countOne(
      start: Int,
      length: Int,
      ordinal: Int,
      keep: Int32Predicate,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
      checkBits: Boolean,
  ): Long =
    require(ordinal == 0 || ordinal == 1, s"count ordinal $ordinal is not a pair column")
    val values = if (ordinal == 0) keyValues else valueValues
    val predicate = captured(keep)
    val end = start + length
    var row = start
    var total = 0L
    if (checkBits) then
      val words = if (ordinal == 0) keyWords else valueWords
      while (row < end) {
        if (Validity.isSet(words, row) && predicate(values(row))) then total += 1L
        row += 1
      }
    else
      while (row < end) {
        if (predicate(values(row))) then total += 1L
        row += 1
      }
    total

  def countPair(
      start: Int,
      length: Int,
      keep: PairKernel.IntPairPredicate,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
      checkBits: Boolean,
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    if (checkBits) then
      while (row < end) {
        if (
            Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row) &&
            keep(keyValues(row), valueValues(row))
          )
        then total += 1L
        row += 1
      }
    else
      while (row < end) {
        if (keep(keyValues(row), valueValues(row))) then total += 1L
        row += 1
      }
    total

  def sumOne(
      start: Int,
      length: Int,
      ordinal: Int,
      keep: Int32Predicate,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
      checkBits: Boolean,
      measureValues: Array[Int],
      measureWords: Array[Long],
      skipMeasureNull: Boolean,
  ): Long =
    require(ordinal == 0 || ordinal == 1, s"sum ordinal $ordinal is not a pair column")
    val values = if (ordinal == 0) keyValues else valueValues
    val predicate = captured(keep)
    val end = start + length
    var row = start
    var total = 0L
    if (!checkBits && skipMeasureNull) then
      while (row < end) {
        if (predicate(values(row))) then total += measureValues(row).toLong
        row += 1
      }
    else if (!checkBits) then
      while (row < end) {
        if (predicate(values(row)) && Validity.isSet(measureWords, row)) then
          total += measureValues(row).toLong
        row += 1
      }
    else if (skipMeasureNull) then
      val words = if (ordinal == 0) keyWords else valueWords
      while (row < end) {
        if (Validity.isSet(words, row) && predicate(values(row))) then
          total += measureValues(row).toLong
        row += 1
      }
    else
      val words = if (ordinal == 0) keyWords else valueWords
      while (row < end) {
        if (
            Validity.isSet(words, row) && predicate(values(row)) &&
            Validity.isSet(measureWords, row)
          )
        then total += measureValues(row).toLong
        row += 1
      }
    total

  def sumPair(
      start: Int,
      length: Int,
      keep: PairKernel.IntPairPredicate,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
      checkBits: Boolean,
      measureValues: Array[Int],
      measureWords: Array[Long],
      skipMeasureNull: Boolean,
  ): Long =
    val end = start + length
    var row = start
    var total = 0L
    if (!checkBits && skipMeasureNull) then
      while (row < end) {
        if (keep(keyValues(row), valueValues(row))) then total += measureValues(row).toLong
        row += 1
      }
    else if (!checkBits) then
      while (row < end) {
        if (keep(keyValues(row), valueValues(row)) && Validity.isSet(measureWords, row)) then
          total += measureValues(row).toLong
        row += 1
      }
    else if (skipMeasureNull) then
      while (row < end) {
        if (
            Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row) &&
            keep(keyValues(row), valueValues(row))
          )
        then total += measureValues(row).toLong
        row += 1
      }
    else
      while (row < end) {
        if (
            Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row) &&
            keep(keyValues(row), valueValues(row)) && Validity.isSet(measureWords, row)
          )
        then total += measureValues(row).toLong
        row += 1
      }
    total

  /** One function for the row loop. A bare trait is wrapped once here, not once per row. */
  private def captured(predicate: Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        value => fallback(value)
