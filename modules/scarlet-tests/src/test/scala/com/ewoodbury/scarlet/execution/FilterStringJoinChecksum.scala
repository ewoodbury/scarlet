package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection

/**
 * Order-independent checksum for the two-filter string inner join bench.
 *
 * Build keys are `(row % 1024).toString`. The filters drop a key whose integer is divisible by 3
 * and a negative value. The probe is one row per key, and its value is that integer, so each kept
 * build row matches once. The hash sums `(key.hashCode * 31) xor the left value xor the right
 * value`. Key order does not matter. The source is built once, outside the timer.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object FilterStringJoinChecksum:

  val shape: String =
    "StringHashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(Scan))"

  private val groups = 1024

  def source(values: Array[Int], partitions: Int): DistCollection[(String, (Int, Int))] =
    val pool = Array.tabulate(groups)(_.toString)
    val keys = Array.tabulate(values.length)(row => pool(row % groups))
    val build = DistCollection(keys.zip(values).toSeq, partitions)
      .filterKeys((key: String) => key.toInt % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val probe = DistCollection((0 until groups).map(key => pool(key) -> key), partitions)
    build.join(probe)

  def expected(values: Array[Int]): Long =
    var index = 0
    var hash = 0L
    while (index < values.length) {
      val key = index % groups
      val value = values(index)
      if (kept(key, value)) then hash += mix(key, value)
      index += 1
    }
    hash

  def collected(rows: Iterable[(String, (Int, Int))]): Long =
    val iterator = rows.iterator
    var hash = 0L
    while (iterator.hasNext) {
      val row = iterator.next()
      val key = row._1.toInt
      val left = row._2._1
      val right = row._2._2
      require(
        key >= 0 && key < groups,
        s"filter-string-join key ${row._1} outside 0 until $groups",
      )
      require(right == key, s"filter-string-join right value $right != key $key")
      require(kept(key, left), s"filter-string-join kept key $key value $left")
      hash += mix(key, left)
    }
    hash

  private def kept(key: Int, value: Int): Boolean =
    key % 3 != 0 && value >= 0

  private def mix(key: Int, value: Int): Long =
    (key.toString.hashCode.toLong * 31L) ^ value.toLong ^ key.toLong
