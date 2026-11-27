package com.ewoodbury.scarlet.execution

import scala.collection.mutable.LinkedHashMap

/**
 * Row-path count and sum for one already key-hashed partition of `(Int, Int)` pairs.
 *
 * A null key is dropped. A null value increments the count and is not added. The count is an `Int`
 * and wraps. The sum is a `Long`. Key order is first-seen. The value is read through
 * `productElement`, so a null `Int` is not unboxed.
 */
private[execution] object CountSumRows:

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Var",
      "org.wartremover.warts.MutableDataStructures",
    ),
  )
  def of(rows: Iterable[(Any, Any)]): Seq[(Int, Int, Long)] =
    val acc = LinkedHashMap.empty[Int, (Int, Long)]
    val iterator = rows.iterator
    while (iterator.hasNext) do
      val product: Product = iterator.next()
      val key = product.productElement(0)
      if (key != null) then
        val value = product.productElement(1)
        val keyInt = key.asInstanceOf[Int]
        val (count, sum) = acc.getOrElse(keyInt, (0, 0L))
        val nextSum = if (value != null) then sum + value.asInstanceOf[Int].toLong else sum
        acc.update(keyInt, (count + 1, nextSum))
    acc.iterator.map { case (key, (count, sum)) => (key, count, sum) }.toSeq

  /** Same null rules as [[of]] for `(Long, Long)`. The count is an `Int` and wraps. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Var",
      "org.wartremover.warts.MutableDataStructures",
    ),
  )
  def longs(rows: Iterable[(Any, Any)]): Seq[(Long, Int, Long)] =
    val acc = LinkedHashMap.empty[Long, (Int, Long)]
    val iterator = rows.iterator
    while (iterator.hasNext) do
      val product: Product = iterator.next()
      val key = product.productElement(0)
      if (key != null) then
        val value = product.productElement(1)
        val keyLong = key.asInstanceOf[Long]
        val (count, sum) = acc.getOrElse(keyLong, (0, 0L))
        val nextSum = if (value != null) then sum + value.asInstanceOf[Long] else sum
        acc.update(keyLong, (count + 1, nextSum))
    acc.iterator.map { case (key, (count, sum)) => (key, count, sum) }.toSeq

  /**
   * Same null rules as [[of]] for `(String, Int)`. An empty string is a present key. The count is
   * an `Int` and wraps. The value is widened to `Long` before it is added.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Var",
      "org.wartremover.warts.MutableDataStructures",
    ),
  )
  def strings(rows: Iterable[(Any, Any)]): Seq[(String, Int, Long)] =
    val acc = LinkedHashMap.empty[String, (Int, Long)]
    val iterator = rows.iterator
    while (iterator.hasNext) do
      val product: Product = iterator.next()
      val key = product.productElement(0)
      if (key != null) then
        val value = product.productElement(1)
        val keyString = key.asInstanceOf[String]
        val (count, sum) = acc.getOrElse(keyString, (0, 0L))
        val nextSum = if (value != null) then sum + value.asInstanceOf[Int].toLong else sum
        acc.update(keyString, (count + 1, nextSum))
    acc.iterator.map { case (key, (count, sum)) => (key, count, sum) }.toSeq
