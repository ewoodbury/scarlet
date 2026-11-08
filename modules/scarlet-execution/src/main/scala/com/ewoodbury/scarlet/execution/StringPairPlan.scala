package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

/**
 * Witness for a `(String, Int)` source.
 *
 * A present string key selects the kind. A null key is not a witness: classification keeps
 * looking, and an all-null-key source stays on the row path. A null int is still a value of this
 * pair: the witness reads `productElement`, so the null is not rejected as a non-`Int`. Grouped
 * sum does not use this kind.
 *
 * Named erasure boundary. The source element type is erased, so the witness matches `Any`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
private[execution] object StringPairPlan:

  /** Every remaining row is a string key, or a null key, with an int value. A null int counts. */
  def confirm(rest: Iterator[Any]): Option[PhysicalOp.Kind] =
    Option.when(rest.forall(stringIntPair))(PhysicalOp.Kind.StringPairs)

  /** Skip null keys until a present string key, then [[confirm]] the tail. */
  @tailrec
  def witness(rest: Iterator[Any]): Option[PhysicalOp.Kind] =
    if (!rest.hasNext) then None
    else
      val value = rest.next()
      if (nullKey(value)) then witness(rest)
      else if (presentKey(value)) then confirm(rest)
      else None

  /**
   * A null key beside an int value, including a null int. A present string is not this case.
   * `productElement` does not unbox, so a null int is not missed by `_: Int`.
   */
  def nullKey(value: Any): Boolean = pair(value) match
    case Some(product) =>
      absent(product.productElement(0)) && intCell(product.productElement(1))
    case None => false

  /** A present string key, including `""`, beside an int value or a null int. */
  def presentKey(value: Any): Boolean = pair(value) match
    case Some(product) =>
      stringCell(product.productElement(0)) && intCell(product.productElement(1))
    case None => false

  private def stringIntPair(value: Any): Boolean =
    nullKey(value) || presentKey(value)

  private def pair(value: Any): Option[Product] = value match
    case product: Product if product.productArity == 2 => Some(product)
    case _ => None

  private def absent(cell: Any): Boolean = cell match
    case null => true
    case _ => false

  private def stringCell(cell: Any): Boolean = cell match
    case _: String => true
    case _ => false

  /** A present `Int`, or a null int. Any other reference is not this column. */
  private def intCell(cell: Any): Boolean = cell match
    case null => true
    case _: Int => true
    case _ => false
