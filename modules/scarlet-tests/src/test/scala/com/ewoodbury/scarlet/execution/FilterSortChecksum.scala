package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection

/**
 * Order-sensitive checksum for the two-filter int sort bench.
 *
 * Keys are a permutation of `0` until the row count. The filters drop a key divisible by 3 and a
 * negative value. The hash mixes `hash * 31 + key` then `hash * 31 + value` over the sorted kept
 * pairs. A local stable sort of those pairs produces the expected hash before timing.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object FilterSortChecksum:

  val shape: String = "SortByIntKey(FilterValue(FilterKey(Scan)))"

  def source(
      keys: Array[Int],
      values: Array[Int],
      partitions: Int,
  ): DistCollection[(Int, Int)] =
    DistCollection
      .ofIntPairs(keys, values, partitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .sortByIntKey

  def expected(keys: Array[Int], values: Array[Int]): Long =
    val order = keptRows(keys, values)
    scala.util.Sorting.stableSort(order, (left: Int, right: Int) => keys(left) < keys(right))
    var slot = 0
    var hash = 0L
    while (slot < order.length) {
      val row = order(slot)
      hash = hash * 31L + keys(row).toLong
      hash = hash * 31L + values(row).toLong
      slot += 1
    }
    hash

  def collected(rows: Iterable[(Int, Int)]): Long =
    val iterator = rows.iterator
    var hash = 0L
    while (iterator.hasNext) {
      val pair = iterator.next()
      hash = hash * 31L + pair._1.toLong
      hash = hash * 31L + pair._2.toLong
    }
    hash

  private def keptRows(keys: Array[Int], values: Array[Int]): Array[Int] =
    val count = keys.indices.count(row => kept(keys(row), values(row)))
    val order = new Array[Int](count)
    var index = 0
    var slot = 0
    while (index < keys.length) {
      if (kept(keys(index), values(index))) then
        order(slot) = index
        slot += 1
      index += 1
    }
    order

  private def kept(key: Int, value: Int): Boolean =
    key % 3 != 0 && value >= 0
