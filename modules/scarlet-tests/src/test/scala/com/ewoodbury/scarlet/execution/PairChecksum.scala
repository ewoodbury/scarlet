package com.ewoodbury.scarlet.execution

/** Rolling mix of key then value. Shared by the sort queries in [[QueryBench]]. */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object PairChecksum:

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
