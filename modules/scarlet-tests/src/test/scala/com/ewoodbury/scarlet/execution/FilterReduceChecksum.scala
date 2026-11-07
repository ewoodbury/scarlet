package com.ewoodbury.scarlet.execution

/**
 * Order-independent checksum for the two-filter `reduceByKey` bench.
 *
 * Keys are `row % groups`. The filters drop a key divisible by 3 and a negative value. The hash
 * sums `(key * 31) xor the wrapped value sum` over kept keys. `reduceByKey` wraps at `Int`, so the
 * local sum uses `Int` addition. Key order does not matter.
 * It also carries the two-filter count.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object FilterReduceChecksum:

  def expected(keys: Array[Int], values: Array[Int], groups: Int): Long =
    val sums = new Array[Int](groups)
    val seen = new Array[Boolean](groups)
    var index = 0
    while (index < keys.length) {
      val key = keys(index)
      val value = values(index)
      if (kept(key, value)) then
        sums(key) += value
        seen(key) = true
      index += 1
    }
    fold(sums, seen)

  def expectedCount(keys: Array[Int], values: Array[Int]): Long =
    var index = 0
    var total = 0L
    while (index < keys.length) {
      val key = keys(index)
      val value = values(index)
      if (kept(key, value)) then total += 1L
      index += 1
    }
    total

  def collected(rows: Iterable[(Int, Int)], groups: Int): Long =
    val sums = new Array[Int](groups)
    val seen = new Array[Boolean](groups)
    val iterator = rows.iterator
    while (iterator.hasNext) {
      val row = iterator.next()
      val key = row._1
      require(key >= 0 && key < groups, s"filter-reducebykey key $key outside 0 until $groups")
      require(!seen(key), s"filter-reducebykey saw key $key twice")
      sums(key) = row._2
      seen(key) = true
    }
    fold(sums, seen)

  private def kept(key: Int, value: Int): Boolean =
    key % 3 != 0 && value >= 0

  private def fold(sums: Array[Int], seen: Array[Boolean]): Long =
    var key = 0
    var hash = 0L
    while (key < sums.length) {
      if (seen(key)) then hash += (key.toLong * 31L) ^ sums(key).toLong
      key += 1
    }
    hash
