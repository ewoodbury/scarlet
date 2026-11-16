package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Stable sort of row indexes by effective int key.
 *
 * A null key orders as zero. The sort key is that effective key with the sign bit flipped, so an
 * unsigned radix matches `Integer.compare`. Least-significant digit order is stable, and the index
 * array starts in input order, so equal keys keep the smaller row index.
 *
 * Two 16-bit passes read and write the index and the packed key sequentially. The bottom-up merge
 * this replaces reread the key array through a permuted index on every pass, and the pass count
 * grew with the run.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object IntIndexSort:

  private val Radix = 65536
  private val Mask = 65535

  /** `1 << 31`, not the hex literal: that literal is a `Long` and would not flip an `Int`. */
  private val SignBit = 1 << 31

  def sort(keys: Array[Int], keyWords: Array[Long], order: Array[Int]): Unit =
    val rows = order.length
    if (rows > 1) then
      val packed = new Array[Int](rows)
      val present = Validity.livePresent(keyWords, keys.length)
      var index = 0
      while (index < rows) {
        val row = order(index)
        val key = if (present || Validity.isSet(keyWords, row)) keys(row) else 0
        packed(index) = key ^ SignBit
        index += 1
      }
      val bufOrder = new Array[Int](rows)
      val bufKey = new Array[Int](rows)
      val counts = new Array[Int](Radix)
      pass(packed, order, bufKey, bufOrder, rows, 0, counts)
      pass(bufKey, bufOrder, packed, order, rows, 16, counts)

  private def pass(
      srcKey: Array[Int],
      srcOrder: Array[Int],
      dstKey: Array[Int],
      dstOrder: Array[Int],
      rows: Int,
      shift: Int,
      counts: Array[Int],
  ): Unit =
    Arrays.fill(counts, 0)
    var index = 0
    while (index < rows) {
      val bucket = (srcKey(index) >>> shift) & Mask
      counts(bucket) += 1
      index += 1
    }
    var sum = 0
    var bucket = 0
    while (bucket < Radix) {
      val count = counts(bucket)
      counts(bucket) = sum
      sum += count
      bucket += 1
    }
    index = 0
    while (index < rows) {
      val bucket = (srcKey(index) >>> shift) & Mask
      val slot = counts(bucket)
      counts(bucket) = slot + 1
      dstKey(slot) = srcKey(index)
      dstOrder(slot) = srcOrder(index)
      index += 1
    }
