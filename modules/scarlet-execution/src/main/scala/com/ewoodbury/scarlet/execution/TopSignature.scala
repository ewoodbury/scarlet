package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan

/**
 * Recognises a sort of int pairs by value descending, then key ascending.
 *
 * The key function has to swap `(sk, v)` to `(v, sk)`. The ordering has to rank a larger value
 * first and, for the same value, a smaller key first. Any other key or ordering declines. A throw
 * from the probe declines too. The probe runs at planning time, on fixed pairs, not on the input.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
  ),
)
private[execution] object TopSignature:

  def sourceOf(plan: Plan[_]): Option[Plan[_]] =
    plan match
      case sort: Plan.SortByOp[?, ?] if probe(sort) => Some(sort.source)
      case _ => None

  private def probe(sort: Plan.SortByOp[?, ?]): Boolean =
    try
      val key = sort.keyFunc.asInstanceOf[((Int, Int)) => Any]
      val ordering = sort.ordering.asInstanceOf[Ordering[(Int, Int)]]
      swaps(key) && orders(ordering)
    catch case scala.util.control.NonFatal(_) => false

  private def swaps(key: ((Int, Int)) => Any): Boolean =
    swapped(key((1, 2)), 2, 1) &&
      swapped(key((0, 0)), 0, 0) &&
      swapped(key((-3, 4)), 4, -3) &&
      swapped(key((Int.MinValue, Int.MaxValue)), Int.MaxValue, Int.MinValue) &&
      swapped(key((7, -8)), -8, 7)

  private def swapped(result: Any, value: Int, key: Int): Boolean =
    result match
      case product: Product if product.productArity == 2 =>
        (product.productElement(0), product.productElement(1)) match
          case (left: Integer, right: Integer) =>
            left.intValue == value && right.intValue == key
          case _ => false
      case _ => false

  private def orders(ordering: Ordering[(Int, Int)]): Boolean =
    ordering.compare((5, 1), (4, 9)) < 0 &&
      ordering.compare((5, 1), (5, 2)) < 0 &&
      ordering.compare((5, 2), (5, 1)) > 0 &&
      ordering.compare((3, 0), (4, 0)) > 0 &&
      ordering.compare((5, 1), (5, 1)) == 0 &&
      ordering.compare((0, Int.MinValue), (0, Int.MaxValue)) < 0 &&
      ordering.compare((Int.MinValue, 0), (Int.MaxValue, 0)) > 0
