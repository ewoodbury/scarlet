package com.ewoodbury.scarlet.execution

/** Rolling mix of key then value. Shared by the sort queries in [[QueryBench]]. */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object PairChecksum:

  /** Local stable sort, then a rolling mix of key then value. Runs outside the timer. */
  def expectedSortedInts(keys: Array[Int], values: Array[Int]): Long =
    val order = Array.tabulate(keys.length)(identity)
    scala.util.Sorting.stableSort(order, (left: Int, right: Int) => keys(left) < keys(right))
    ordered(order, row => keys(row).toLong, row => values(row).toLong)

  /** Local stable sort, then a rolling mix of key then value. Runs outside the timer. */
  def expectedSortedLongs(keys: Array[Long], values: Array[Long]): Long =
    val order = Array.tabulate(keys.length)(identity)
    scala.util.Sorting.stableSort(order, (left: Int, right: Int) => keys(left) < keys(right))
    ordered(order, row => keys(row), row => values(row))

  def ordered(order: Array[Int], keyAt: Int => Long, valueAt: Int => Long): Long =
    var index = 0
    var hash = 0L
    while (index < order.length) {
      val row = order(index)
      hash = hash * 31L + keyAt(row)
      hash = hash * 31L + valueAt(row)
      index += 1
    }
    hash

  def intPairs(rows: Iterable[(Int, Int)]): Long =
    val iterator = rows.iterator
    var hash = 0L
    while (iterator.hasNext) {
      val pair = iterator.next()
      hash = hash * 31L + pair._1.toLong
      hash = hash * 31L + pair._2.toLong
    }
    hash

  def longPairs(rows: Iterable[(Long, Long)]): Long =
    val iterator = rows.iterator
    var hash = 0L
    while (iterator.hasNext) {
      val pair = iterator.next()
      hash = hash * 31L + pair._1
      hash = hash * 31L + pair._2
    }
    hash
