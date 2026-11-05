package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection

/**
 * Order-independent checksum for the two-filter string `reduceByKey` bench.
 *
 * The key is `(row % 1024).toString`. The filters drop a key whose integer is divisible by 3 and
 * a negative value. The hash sums `(key.hashCode * 31) xor the wrapped value sum` over kept keys.
 * `reduceByKey` wraps at `Int`, so the local sum uses `Int` addition. Key order does not matter.
 * The source is built once, outside the timer.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object FilterStringReduceChecksum:

  val shape: String = "StringHashAggregate(Exchange(FilterValue(FilterKey(Scan))))"

  private val groups = 1024

  def source(values: Array[Int], partitions: Int): DistCollection[(String, Int)] =
    val pool = Array.tabulate(groups)(_.toString)
    val keys = Array.tabulate(values.length)(row => pool(row % groups))
    DistCollection(keys.zip(values).toSeq, partitions)
      .filterKeys((key: String) => key.toInt % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .reduceByKey[String, Int](_ + _)

  def expected(values: Array[Int]): Long =
    val sums = new Array[Int](groups)
    val seen = new Array[Boolean](groups)
    var index = 0
    while (index < values.length) {
      val key = index % groups
      val value = values(index)
      if (key % 3 != 0 && value >= 0) then
        sums(key) += value
        seen(key) = true
      index += 1
    }
    fold(sums, seen)

  def collected(rows: Iterable[(String, Int)]): Long =
    val sums = new Array[Int](groups)
    val seen = new Array[Boolean](groups)
    val iterator = rows.iterator
    while (iterator.hasNext) {
      val row = iterator.next()
      val key = row._1.toInt
      require(
        key >= 0 && key < groups,
        s"filter-string-reducebykey key ${row._1} outside 0 until $groups",
      )
      require(!seen(key), s"filter-string-reducebykey saw key ${row._1} twice")
      sums(key) = row._2
      seen(key) = true
    }
    fold(sums, seen)

  private def fold(sums: Array[Int], seen: Array[Boolean]): Long =
    var key = 0
    var hash = 0L
    while (key < groups) {
      if (seen(key)) then hash += (key.toString.hashCode.toLong * 31L) ^ sums(key).toLong
      key += 1
    }
    hash
