package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

/**
 * Witness for a `(String, Int)` source.
 *
 * A present string key selects the kind. A null key is not a witness: classification keeps
 * looking, and an all-null-key source stays on the row path. A null value is not a pair this path
 * can unbox, so the source stays on the row path. Grouped sum does not use this kind.
 *
 * Named erasure boundary. The source element type is erased, so the witness matches `Any`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
private[execution] object StringPairPlan:

  /** Every remaining row is a string key, or a null key, with an `Int` value. */
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

  /** A null key beside an `Int` value. A present string is not this case. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def nullKey(value: Any): Boolean = value match
    case (null, _: Int) => true
    case _ => false

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def stringIntPair(value: Any): Boolean = value match
    case (null, _: Int) => true
    case (_: String, _: Int) => true
    case _ => false

  private def presentKey(value: Any): Boolean = value match
    case (_: String, _: Int) => true
    case _ => false
