package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection

/**
 * Order-independent checksum for the two-filter string `groupByKey` bench.
 *
 * The key is `(row % 1024).toString`. The filters drop a key whose integer is divisible by 3 and
 * a negative value. The hash sums `(key.hashCode * 31) xor the value sum xor the value count`
 * over kept keys. Key order does not matter. The source is built once, outside the timer.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object FilterStringGroupChecksum:

  val shape: String = "GroupByKey(Exchange(FilterValue(FilterKey(Scan))))"

  private val groups = 1024

  def source(values: Array[Int], partitions: Int): DistCollection[(String, Iterable[Int])] =
    val pool = Array.tabulate(groups)(_.toString)
    val keys = Array.tabulate(values.length)(row => pool(row % groups))
    DistCollection(keys.zip(values).toSeq, partitions)
      .filterKeys((key: String) => key.toInt % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .groupByKey

  def expected(values: Array[Int]): Long =
    val sums = new Array[Long](groups)
    val seen = new Array[Int](groups)
    var index = 0
    while (index < values.length) {
      val key = index % groups
      val value = values(index)
      if (key % 3 != 0 && value >= 0) then
        sums(key) += value.toLong
        seen(key) += 1
      index += 1
    }
    fold(sums, seen)

  def collected(rows: Iterable[(String, Iterable[Int])]): Long =
    val sums = new Array[Long](groups)
    val seen = new Array[Int](groups)
    val iterator = rows.iterator
    while (iterator.hasNext) {
      val row = iterator.next()
      val key = row._1.toInt
      require(
        key >= 0 && key < groups,
        s"filter-string-groupbykey key ${row._1} outside 0 until $groups",
      )
      require(seen(key) == 0, s"filter-string-groupbykey saw key ${row._1} twice")
      val grouped = row._2.iterator
      while (grouped.hasNext) {
        sums(key) += grouped.next().toLong
        seen(key) += 1
      }
    }
    fold(sums, seen)

  private def fold(sums: Array[Long], seen: Array[Int]): Long =
    var key = 0
    var hash = 0L
    while (key < groups) {
      if (seen(key) > 0) then
        hash += (key.toString.hashCode.toLong * 31L) ^ sums(key) ^ seen(key).toLong
      key += 1
    }
    hash
