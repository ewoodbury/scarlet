package com.ewoodbury.scarlet.api

/**
 * Which int column of an inner join `(Int, (Int, Int))` a grouped sum reads.
 *
 * [[JoinSumProjection]] is the row-path map. The planner reads [[column]] and does not call the
 * function. `Left` is the build value. `Right` is the probe value.
 */
private[scarlet] enum JoinSumColumn:
  case Left, Right

/**
 * Maps one join match to `(key, measuredValue.toLong)`.
 *
 * [[apply]] is the row path. A null measured value fails when unboxed, the same as a hand-written
 * `toLong`. The planner recognizes this class and does not call [[apply]].
 */
private[scarlet] final class JoinSumProjection[A](
    val column: JoinSumColumn,
    ev: A =:= (Int, (Int, Int)),
) extends (A => (Int, Long)):

  def apply(row: A): (Int, Long) =
    val (key, pair) = ev(row)
    val (left, right) = pair
    column match
      case JoinSumColumn.Left => (key, left.toLong)
      case JoinSumColumn.Right => (key, right.toLong)

/**
 * Addition of two `Long` sums.
 *
 * This is a singleton so a plan can name addition without inspecting a lambda. Any other reducer,
 * including `_ + _`, stays on the row path.
 */
private[scarlet] object LongSum extends ((Long, Long) => Long):
  def apply(left: Long, right: Long): Long = left + right
