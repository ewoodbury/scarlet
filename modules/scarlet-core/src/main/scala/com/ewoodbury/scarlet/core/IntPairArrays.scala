package com.ewoodbury.scarlet.core

/**
 * Keys and values of one `(Int, Int)` partition, stored as primitive arrays.
 *
 * Iteration yields pairs, so the row path can read the partition. The arrays are this partition's
 * copy.
 */
final case class IntPairArrays(keys: Array[Int], values: Array[Int]) extends Iterable[(Int, Int)]:

  require(keys.length == values.length, "key and value arrays differ in length")

  override def knownSize: Int = keys.length

  override def size: Int = keys.length

  override def iterator: Iterator[(Int, Int)] =
    Iterator.tabulate(keys.length)(row => (keys(row), values(row)))
