package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection

/**
 * Order-independent checksum for the two-filter inner join bench.
 *
 * Keys are the row index, so each key is unique. The build side drops a key divisible by 3 and a
 * negative value. The probe value is the key. The hash sums `(key * 31) xor the left value xor
 * the right value` over kept rows. Key order does not matter.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object FilterJoinChecksum:

  val shape: String =
    "HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(Scan))"

  def source(values: Array[Int], partitions: Int): DistCollection[(Int, (Int, Int))] =
    val keys = Array.tabulate(values.length)(identity)
    val left = DistCollection
      .ofIntPairs(keys, values, partitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val right = DistCollection.ofIntPairs(keys, keys, partitions)
    left.join(right)

  def expected(values: Array[Int]): Long =
    var index = 0
    var hash = 0L
    while (index < values.length) {
      val key = index
      val value = values(index)
      if (kept(key, value)) then hash += mix(key, value, key)
      index += 1
    }
    hash

  def collected(rows: Iterable[(Int, (Int, Int))]): Long =
    val iterator = rows.iterator
    var hash = 0L
    while (iterator.hasNext) {
      val row = iterator.next()
      val key = row._1
      val left = row._2._1
      val right = row._2._2
      require(key >= 0, s"filter-join key $key is negative")
      require(right == key, s"filter-join right value $right != key $key")
      hash += mix(key, left, right)
    }
    hash

  private def kept(key: Int, value: Int): Boolean =
    key % 3 != 0 && value >= 0

  private def mix(key: Int, left: Int, right: Int): Long =
    (key.toLong * 31L) ^ left.toLong ^ right.toLong
