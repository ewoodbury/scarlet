package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.{Partition, Plan}

/**
 * One join side lowers and the other does not.
 *
 * [[ColumnarPlanner.tryRun]] decodes the side that lowers, so that side has already published its
 * batches. The other side, and the join, run on the row executor. Two decoded sources would lower
 * to a hash join, so the rewritten plan is not handed back to [[DefaultExecutionService]]. This is
 * not a kernel and it does not build a [[PhysicalOp]].
 */
private[execution] object MixedJoin:

  /**
   * Partitions of a join whose children disagree about lowering. `None` means the caller runs the
   * original plan: both sides lower, neither does, or the columnar side declined at runtime.
   */
  def partitions[A](
      plan: Plan[A],
      runRow: Plan[_] => Seq[Partition[_]],
  ): Option[Seq[Partition[A]]] =
    rewritten(plan, runRow).map(next => castPartitions[A](runRow(next)))

  private def rewritten(
      plan: Plan[_],
      runRow: Plan[_] => Seq[Partition[_]],
  ): Option[Plan[_]] = plan match
    case Plan.JoinOp(left, right, strategy) =>
      replaced(left, right, runRow).map { case (nextLeft, nextRight) =>
        Plan.JoinOp(nextLeft, nextRight, strategy)
      }
    case Plan.LeftOuterJoinOp(left, right, strategy) =>
      replaced(left, right, runRow).map { case (nextLeft, nextRight) =>
        Plan.LeftOuterJoinOp(nextLeft, nextRight, strategy)
      }
    case Plan.RightOuterJoinOp(left, right, strategy) =>
      replaced(left, right, runRow).map { case (nextLeft, nextRight) =>
        Plan.RightOuterJoinOp(nextLeft, nextRight, strategy)
      }
    case Plan.FullOuterJoinOp(left, right, strategy) =>
      replaced(left, right, runRow).map { case (nextLeft, nextRight) =>
        Plan.FullOuterJoinOp(nextLeft, nextRight, strategy)
      }
    case _ => None

  /**
   * Sources for the two sides when exactly one lowers. The lowered side is decoded. The other side
   * stays on `runRow`, including a `flatMap` whose own child would have lowered.
   */
  private def replaced[K, V, W](
      left: Plan[(K, V)],
      right: Plan[(K, W)],
      runRow: Plan[_] => Seq[Partition[_]],
  ): Option[(Plan[(K, V)], Plan[(K, W)])] =
    (ColumnarPlanner.lower(left), ColumnarPlanner.lower(right)) match
      case (Some(_), None) =>
        ColumnarPlanner.tryRun(left).map { decoded =>
          (freeze(decoded), freeze(rowPartitions[(K, W)](right, runRow)))
        }
      case (None, Some(_)) =>
        ColumnarPlanner.tryRun(right).map { decoded =>
          (freeze(rowPartitions[(K, V)](left, runRow)), freeze(decoded))
        }
      case _ => None

  /**
   * One traversal of a row-side [[scala.collection.immutable.LazyList]] must not empty the join.
   */
  private def freeze[A](parts: Seq[Partition[A]]): Plan.Source[A] =
    Plan.Source(parts.map(part => Partition(part.data.toVector)))

  private def rowPartitions[A](
      plan: Plan[A],
      runRow: Plan[_] => Seq[Partition[_]],
  ): Seq[Partition[A]] =
    castPartitions[A](runRow(plan))

  /**
   * Named erasure boundary. The row runner sees `Plan[_]`, and the partitions still have the plan
   * element type.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def castPartitions[A](parts: Seq[Partition[_]]): Seq[Partition[A]] =
    parts.asInstanceOf[Seq[Partition[A]]]
