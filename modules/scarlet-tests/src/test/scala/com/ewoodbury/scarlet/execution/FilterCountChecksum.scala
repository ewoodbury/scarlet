package com.ewoodbury.scarlet.execution

/**
 * Order-independent checksum for the two-filter int `countByKey` bench.
 *
 * `(key * 31) ^ kept-row count`, summed over keys with a positive count. Key order does not
 * matter. Keys are `row % groups`. The filters drop keys divisible by 3 and negative values. The
 * count is an `Int` and wraps, matching the operator.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object FilterCountChecksum:

  def expected(keys: Array[Int], values: Array[Int], groups: Int): Long =
    val seen = new Array[Int](groups)
    var index = 0
    while (index < keys.length) {
      val key = keys(index)
      val value = values(index)
      if (key % 3 != 0 && value >= 0) then seen(key) += 1
      index += 1
    }
    fold(seen)

  def collected(rows: Iterable[(Int, Int)], groups: Int): Long =
    val seen = new Array[Int](groups)
    val iterator = rows.iterator
    while (iterator.hasNext) {
      val row = iterator.next()
      val key = row._1
      require(key >= 0 && key < groups, s"filter-countbykey key $key outside 0 until $groups")
      require(seen(key) == 0, s"filter-countbykey key $key was emitted twice")
      seen(key) = row._2
    }
    fold(seen)

  private def fold(seen: Array[Int]): Long =
    var key = 0
    var hash = 0L
    while (key < seen.length) {
      if (seen(key) > 0) then hash += (key.toLong * 31L) ^ seen(key).toLong
      key += 1
    }
    hash
