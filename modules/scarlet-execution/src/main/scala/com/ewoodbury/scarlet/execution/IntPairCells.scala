package com.ewoodbury.scarlet.execution

/**
 * Whether a grouped-sum row is a pair of `Int`-or-null cells.
 *
 * `(Int, Int)` erases its fields. `productElement` keeps a null component visible without unboxing
 * it to `0`. The check is allocation-free: it runs once per row while a source descriptor is still
 * unknown, and a per-row enum would cost more than the test saves.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
private[execution] object IntPairCells:

  /** True when both cells are an `Int` or null. Any other cell, or a non-pair, is false. */
  def isNullablePair(row: Any): Boolean = row match
    case pair: Tuple2[?, ?] =>
      nullableInt(pair.productElement(0)) && nullableInt(pair.productElement(1))
    case _ => false

  private def nullableInt(cell: Any): Boolean = cell match
    case null => true
    case _: Int => true
    case _ => false
