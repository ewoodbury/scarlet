package com.ewoodbury.scarlet.columnar

import java.util.Arrays
import java.util.concurrent.atomic.AtomicInteger

/**
 * Stable least-significant-digit radix of one present integer key.
 *
 * DataFusion's single-column sort is arrow's comparison `lexsort_to_indices`, and its multi-column
 * radix (arrow-rs #9683) sorts row-encoded bytes. A non-null signed integer is encoded there by
 * flipping the sign bit and laying the bytes out big-endian, so an unsigned radix matches signed
 * order. This pass uses that digit on the high byte and moves the value with the key. Each
 * counting pass is stable, so equal keys keep input order. A byte that is the same on every key is
 * not scattered. Eight-bit digits keep the histogram in cache. A null key does not reach this
 * pass: the caller keeps the index sort, which orders a null as zero and leaves the bit clear.
 *
 * `rows <= 1` is a no-op and does not count as a sort. The returned arrays are the sorted pair.
 * They may be the buffers the caller passed or the scratch buffers, and the caller publishes only
 * the returned ones.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object RadixPass:

  private val Digits = 256
  private val Mask = 255

  /** `1 << 31`, not the hex literal: that literal is a `Long` and would not flip an `Int`. */
  private val IntSign = 1 << 31

  /** Flips the sign bit of a long. `Long.MinValue` is that bit. */
  private val LongSign = java.lang.Long.MIN_VALUE

  private val intSorts = new AtomicInteger(0)
  private val longSorts = new AtomicInteger(0)

  /** Sorts of more than one int row since [[resetForTest]]. */
  private[scarlet] def intSortsForTest: Int = intSorts.get

  /** Sorts of more than one long row since [[resetForTest]]. */
  private[scarlet] def longSortsForTest: Int = longSorts.get

  /** Clears the sort counters. */
  private[scarlet] def resetForTest(): Unit =
    intSorts.set(0)
    longSorts.set(0)

  /** Sorted int pair. `passes` is the number of digits that were scattered. */
  final class Ints(val keys: Array[Int], val values: Array[Int], val passes: Int)

  /** Sorted long pair. `passes` is the number of digits that were scattered. */
  final class Longs(val keys: Array[Long], val values: Array[Long], val passes: Int)

  def sortInts(keys: Array[Int], values: Array[Int], rows: Int): Ints =
    if (rows <= 1) then new Ints(keys, values, 0)
    else
      val _ = intSorts.incrementAndGet()
      val counts = new Array[Int](Digits)
      var srcKey = keys
      var srcValue = values
      var dstKey = new Array[Int](rows)
      var dstValue = new Array[Int](rows)
      var passes = 0
      var shift = 0
      while (shift < 32) {
        if (scatterInt(srcKey, srcValue, dstKey, dstValue, rows, shift, counts)) then
          val heldKey = srcKey
          srcKey = dstKey
          dstKey = heldKey
          val heldValue = srcValue
          srcValue = dstValue
          dstValue = heldValue
          passes += 1
        shift += 8
      }
      new Ints(srcKey, srcValue, passes)

  def sortLongs(keys: Array[Long], values: Array[Long], rows: Int): Longs =
    if (rows <= 1) then new Longs(keys, values, 0)
    else
      val _ = longSorts.incrementAndGet()
      val counts = new Array[Int](Digits)
      var srcKey = keys
      var srcValue = values
      var dstKey = new Array[Long](rows)
      var dstValue = new Array[Long](rows)
      var passes = 0
      var shift = 0
      while (shift < 64) {
        if (scatterLong(srcKey, srcValue, dstKey, dstValue, rows, shift, counts)) then
          val heldKey = srcKey
          srcKey = dstKey
          dstKey = heldKey
          val heldValue = srcValue
          srcValue = dstValue
          dstValue = heldValue
          passes += 1
        shift += 8
      }
      new Longs(srcKey, srcValue, passes)

  private def scatterInt(
      srcKey: Array[Int],
      srcValue: Array[Int],
      dstKey: Array[Int],
      dstValue: Array[Int],
      rows: Int,
      shift: Int,
      counts: Array[Int],
  ): Boolean =
    Arrays.fill(counts, 0)
    var index = 0
    if (shift == 24) then
      while (index < rows) {
        val bucket = (srcKey(index) ^ IntSign) >>> 24
        counts(bucket) += 1
        index += 1
      }
    else
      while (index < rows) {
        val bucket = (srcKey(index) >>> shift) & Mask
        counts(bucket) += 1
        index += 1
      }
    if (uniform(counts, rows)) then false
    else
      prefix(counts)
      index = 0
      if (shift == 24) then
        while (index < rows) {
          val key = srcKey(index)
          val bucket = (key ^ IntSign) >>> 24
          val slot = counts(bucket)
          counts(bucket) = slot + 1
          dstKey(slot) = key
          dstValue(slot) = srcValue(index)
          index += 1
        }
      else
        while (index < rows) {
          val key = srcKey(index)
          val bucket = (key >>> shift) & Mask
          val slot = counts(bucket)
          counts(bucket) = slot + 1
          dstKey(slot) = key
          dstValue(slot) = srcValue(index)
          index += 1
        }
      true

  private def scatterLong(
      srcKey: Array[Long],
      srcValue: Array[Long],
      dstKey: Array[Long],
      dstValue: Array[Long],
      rows: Int,
      shift: Int,
      counts: Array[Int],
  ): Boolean =
    Arrays.fill(counts, 0)
    var index = 0
    if (shift == 56) then
      while (index < rows) {
        val bucket = ((srcKey(index) ^ LongSign) >>> 56).toInt
        counts(bucket) += 1
        index += 1
      }
    else
      while (index < rows) {
        val bucket = ((srcKey(index) >>> shift) & Mask.toLong).toInt
        counts(bucket) += 1
        index += 1
      }
    if (uniform(counts, rows)) then false
    else
      prefix(counts)
      index = 0
      if (shift == 56) then
        while (index < rows) {
          val key = srcKey(index)
          val bucket = ((key ^ LongSign) >>> 56).toInt
          val slot = counts(bucket)
          counts(bucket) = slot + 1
          dstKey(slot) = key
          dstValue(slot) = srcValue(index)
          index += 1
        }
      else
        while (index < rows) {
          val key = srcKey(index)
          val bucket = ((key >>> shift) & Mask.toLong).toInt
          val slot = counts(bucket)
          counts(bucket) = slot + 1
          dstKey(slot) = key
          dstValue(slot) = srcValue(index)
          index += 1
        }
      true

  /** True when one digit holds every row, so a stable scatter would not move anything. */
  private def uniform(counts: Array[Int], rows: Int): Boolean =
    var bucket = 0
    var found = false
    while (bucket < Digits && !found) {
      if (counts(bucket) == rows) then found = true
      bucket += 1
    }
    found

  /** Exclusive prefix sums. `counts(bucket)` becomes the first output slot of that digit. */
  private def prefix(counts: Array[Int]): Unit =
    var sum = 0
    var bucket = 0
    while (bucket < Digits) {
      val count = counts(bucket)
      counts(bucket) = sum
      sum += count
      bucket += 1
    }
