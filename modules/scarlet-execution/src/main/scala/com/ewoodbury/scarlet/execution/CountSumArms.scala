package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{IntPairArrays, Plan}

import ColumnarInterpreter.{Columns, interpretAt, pairsOf}

/**
 * Int `countSumByKey`.
 *
 * An owned int-pair scan, including a stack of int key and value slots and a scan with no filter,
 * is summed from those arrays. The predicates run inside the scan. The arrays are aliased and not
 * copied, and no survivor index is built. A boxed partition, a sort, a whole-row filter, and any
 * other pair child are interpreted, then aggregated. That path partial-aggregates survivor ids,
 * including a stack of one. A null key is dropped. A null value is counted and not added. The
 * exchange sees dense partials of `(key, count, sum)`.
 */
private[execution] object CountSumArms:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    child.kind match
      case PhysicalOp.Kind.Pairs =>
        val aggregated = folded(source, child, ordinals, width, workers, batchSize) match
          case Some(owned) => owned
          case None =>
            FilterChain.selected(
              source,
              child,
              selectedPartition,
              preserveAbsence,
              ordinals,
            ) match
              case Some(parts) => CountSum.byKeySelected(parts, width, workers, batchSize)
              case None =>
                CountSum.byKey(
                  pairsOf(
                    interpretAt(source, child, selectedPartition, preserveAbsence, ordinals),
                  ),
                  width,
                  workers,
                  batchSize,
                )
        Columns.CountSums(aggregated)
      case other =>
        throw new IllegalStateException(
          s"countSumByKey column is $other, expected int pairs",
        )

  /**
   * Owned arrays under an int slot stack, or under a scan with no filter. `None` leaves the
   * survivor index in place. A whole-row step, a non-pair atom, and a boxed partition are `None`.
   */
  private def folded(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    ownedPairs(source, child).flatMap { parts =>
      slotSteps(source, child, ordinals).map { steps =>
        CountSum.byKeyOwned(parts, steps, width, workers, batchSize)
      }
    }

  /** Fully present int pairs. Anything else, including one boxed partition, stays on encode. */
  private def ownedPairs(
      plan: Plan[_],
      op: PhysicalOp,
  ): Option[Vector[(Array[Int], Array[Int])]] =
    val (_, under) = PredicatePlan.childOf(plan, op)
    under match
      case PhysicalOp.Scan(PhysicalOp.Kind.Pairs, partitions) =>
        partitions.foldLeft(Option(Vector.empty[(Array[Int], Array[Int])])) { (acc, partition) =>
          (acc, partition.data) match
            case (Some(built), pairs: IntPairArrays) =>
              Some(built :+ ((pairs.keys, pairs.values)))
            case _ => None
        }
      case _ => None

  /**
   * Int key and value slots in source order. `Some(Nil)` is a scan with no filter. `None` when a
   * step is not an int slot, including a whole-row predicate.
   */
  private def slotSteps(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.compile(source, child) match
      case None => Some(Nil)
      case Some(compiled) =>
        PredicatePlan.selectionSteps(PredicatePlan.encoded(compiled, ordinals)) match
          case Some(steps) if steps.nonEmpty && admits(steps) => Some(steps.toList)
          case _ => None

  private def admits(steps: Vector[Selection.Step]): Boolean =
    steps.forall {
      case Selection.Step.Int32(column, _) => column == 0 || column == 1
      case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
          Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
          Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
        false
    }
