package com.ewoodbury.scarlet.execution

import scala.collection.mutable.LinkedHashMap

/**
 * Row-path min or max for one already key-hashed partition of `(Int, Int)` pairs.
 *
 * A null key is dropped. A null value is skipped. A key whose values are all null emits no row.
 * Key order is first-seen. The value is read through `productElement`, so a null `Int` is not
 * unboxed to `0`.
 */
private[execution] object ExtremumRows:

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.MutableDataStructures",
    ),
  )
  def of(rows: Iterable[(Any, Any)], minimum: Boolean): Seq[(Int, Int)] =
    val acc = LinkedHashMap.empty[Int, Int]
    val iterator = rows.iterator
    while (iterator.hasNext) do
      val product: Product = iterator.next()
      val key = product.productElement(0)
      if (key != null) then
        val value = product.productElement(1)
        if (value != null) then
          val keyInt = key.asInstanceOf[Int]
          val valueInt = value.asInstanceOf[Int]
          acc.get(keyInt) match
            case None =>
              val _ = acc.update(keyInt, valueInt)
            case Some(current) =>
              val next =
                if (minimum) then math.min(current, valueInt)
                else math.max(current, valueInt)
              if (next != current) then
                val _ = acc.update(keyInt, next)
    acc.iterator.map { case (key, value) => (key, value) }.toSeq
