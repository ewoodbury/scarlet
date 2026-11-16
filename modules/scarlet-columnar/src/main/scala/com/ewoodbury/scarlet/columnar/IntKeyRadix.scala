package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Stable least-significant-digit radix sort of signed int keys.
 *
 * Four 8-bit passes. The last pass flips the sign bit so a negative key sorts before a positive
 * one, including `Int.MinValue`. Each pass scatters left to right, so equal keys keep the order
 * they arrived in. [[RankSplit]] passes row ids in ascending order, and that arrival order is the
 * tie break.
 *
 * `keys` and `order` stay paired: the key at a slot moves with the id at that slot. Both arrays
 * are the same length. A length below 2 returns. The two scratch arrays are dropped when this
 * returns.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object IntKeyRadix:

  def sort(keys: Array[Int], order: Array[Int]): Unit =
    val rows = order.length
    require(keys.length == rows, s"radix key length ${keys.length} != order length $rows")
    if (rows > 1) then
      val keyBuf = new Array[Int](rows)
      val orderBuf = new Array[Int](rows)
      val hist = new Array[Int](256)
      unsigned(keys, order, keyBuf, orderBuf, hist, shift = 0)
      unsigned(keyBuf, orderBuf, keys, order, hist, shift = 8)
      unsigned(keys, order, keyBuf, orderBuf, hist, shift = 16)
      signed(keyBuf, orderBuf, keys, order, hist)

  private def unsigned(
      srcKeys: Array[Int],
      srcOrder: Array[Int],
      dstKeys: Array[Int],
      dstOrder: Array[Int],
      hist: Array[Int],
      shift: Int,
  ): Unit =
    Arrays.fill(hist, 0)
    val rows = srcOrder.length
    var index = 0
    while (index < rows) {
      hist((srcKeys(index) >>> shift) & 0xff) += 1
      index += 1
    }
    prefix(hist)
    index = 0
    while (index < rows) {
      val digit = (srcKeys(index) >>> shift) & 0xff
      val at = hist(digit)
      hist(digit) = at + 1
      dstKeys(at) = srcKeys(index)
      dstOrder(at) = srcOrder(index)
      index += 1
    }

  /** High byte, with the sign bit flipped so negatives sort first. */
  private def signed(
      srcKeys: Array[Int],
      srcOrder: Array[Int],
      dstKeys: Array[Int],
      dstOrder: Array[Int],
      hist: Array[Int],
  ): Unit =
    Arrays.fill(hist, 0)
    val rows = srcOrder.length
    var index = 0
    while (index < rows) {
      hist(((srcKeys(index) >>> 24) ^ 0x80) & 0xff) += 1
      index += 1
    }
    prefix(hist)
    index = 0
    while (index < rows) {
      val digit = ((srcKeys(index) >>> 24) ^ 0x80) & 0xff
      val at = hist(digit)
      hist(digit) = at + 1
      dstKeys(at) = srcKeys(index)
      dstOrder(at) = srcOrder(index)
      index += 1
    }

  private def prefix(hist: Array[Int]): Unit =
    var sum = 0
    var digit = 0
    while (digit < 256) {
      val count = hist(digit)
      hist(digit) = sum
      sum += count
      digit += 1
    }
