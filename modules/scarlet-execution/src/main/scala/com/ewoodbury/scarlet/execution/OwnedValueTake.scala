package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnBatch, IntTopN, Selection}
import com.ewoodbury.scarlet.core.{IntPairArrays, Plan}

/**
 * `take` of an int value-descending sort over owned int-pair arrays.
 *
 * The child is a dense scan or a stack of int key and value slot filters on that scan. Each
 * partition array is aliased into [[IntTopN.takeOwned]]. A boxed partition, a row predicate, a
 * map, and a long or string sort stay on the selecting heap: this returns `None` and does not
 * encode. `collect` does not call this.
 */
private[execution] object OwnedValueTake:

  def take(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
      n: Int,
  ): Option[ColumnBatch] =
    if (!bothColumns(ordinals)) then None
    else
      for
        parts <- owned(source, child)
        stack <- steps(source, child)
      yield IntTopN.takeOwned(parts, stack, n, ColumnarPlanner.parallelism)

  /** Sort reads the key and the value. A one-column demand stays on the encoded path. */
  private def bothColumns(ordinals: Set[Int]): Boolean =
    ordinals.contains(0) && ordinals.contains(1)

  /**
   * Fully present int pairs under the filter stack. One boxed partition returns `None`: that
   * partition can hold a null, and the selecting heap is what keeps it null.
   */
  private def owned(
      plan: Plan[_],
      op: PhysicalOp,
  ): Option[Vector[(Array[Int], Array[Int])]] =
    PredicatePlan.childOf(plan, op) match
      case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _)) =>
        val found = partitions.map { partition =>
          partition.data match
            case pairs: IntPairArrays => Some((pairs.keys, pairs.values))
            case _ => None
        }
        if (found.forall(_.isDefined)) then Some(found.flatten.toVector) else None
      case _ => None

  /**
   * Slot steps in source order, or `Nil` for a dense pair scan. A row step, an element step, and a
   * plan that is not a scan under filters are `None`.
   */
  private def steps(plan: Plan[_], op: PhysicalOp): Option[List[Selection.Step]] =
    PredicatePlan.compile(plan, op) match
      case None =>
        (plan, op) match
          case (_: Plan.Source[_], PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _)) => Some(Nil)
          case _ => None
      case Some(compiled) =>
        PredicatePlan.selectionSteps(compiled).flatMap { found =>
          if (slotStack(found)) then Some(found.toList) else None
        }

  /** Non-empty int key or value slots. An empty vector is not a filter stack. */
  private def slotStack(steps: Vector[Selection.Step]): Boolean =
    steps.nonEmpty && steps.forall(slot)

  private def slot(step: Selection.Step): Boolean = step match
    case Selection.Step.Int32(column, _) => column == 0 || column == 1
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
      false
