package com.ewoodbury.scarlet.execution

/**
 * Order-independent checksum for the two-filter int `groupByKey` bench.
 *
 * `(key * 31) ^ valueSum ^ valueCount`, summed over keys. Key order does not matter. Keys are
 * `row % groups`. The filters drop keys divisible by 3 and negative values.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object FilterGroupChecksum:

  def expected(keys: Array[Int], values: Array[Int], groups: Int): Long =
    val sums = new Array[Long](groups)
    val seen = new Array[Int](groups)
    var index = 0
    while (index < keys.length) {
      val key = keys(index)
      val value = values(index)
      if (key % 3 != 0 && value >= 0) then
        sums(key) += value.toLong
        seen(key) += 1
      index += 1
    }
    foldSeen(sums, seen)

  def collected(rows: Iterable[(Int, Iterable[Int])], groups: Int): Long =
    val sums = new Array[Long](groups)
    val seen = new Array[Int](groups)
    val iterator = rows.iterator
    while (iterator.hasNext) {
      val row = iterator.next()
      val key = row._1
      require(key >= 0 && key < groups, s"filter-groupbykey key $key outside 0 until $groups")
      val values = row._2.iterator
      while (values.hasNext) {
        sums(key) += values.next().toLong
        seen(key) += 1
      }
    }
    foldSeen(sums, seen)

  private def foldSeen(sums: Array[Long], seen: Array[Int]): Long =
    var key = 0
    var hash = 0L
    while (key < sums.length) {
      if (seen(key) > 0) then hash += (key.toLong * 31L) ^ sums(key) ^ seen(key).toLong
      key += 1
    }
    hash
