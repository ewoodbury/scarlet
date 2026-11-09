package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{
  AbsentSide,
  ColumnBatch,
  ColumnPipeline,
  ColumnarFallback,
  StringOuterPipeline,
}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}

import ColumnarInterpreter.{Columns, interpretAt}

/**
 * Outer joins of `(String, Int)` pairs.
 *
 * Each arm forces `preserveAbsence` on and does not read the flag the caller passed. A null value
 * throws [[ColumnarFallback]] before the kernel, with the same message as the int outer arm. Hash
 * and broadcast right outer run the left-outer kernel on swapped inputs and swap the value columns
 * back once. Sort-merge right does not swap again: [[StringOuterPipeline.sortMergeRightOuterJoin]]
 * already restored `(key, left, right)`.
 */
private[execution] object StringOuterArms:

  def accepts(plan: Plan[_], op: PhysicalOp): Boolean =
    (plan, op) match
      case (
            Plan.LeftOuterJoinOp(_, _, Some(Plan.JoinStrategy.Broadcast)),
            PhysicalOp.StringBroadcastLeftOuter(_, _),
          ) =>
        true
      case (
            Plan.LeftOuterJoinOp(_, _, strategy),
            PhysicalOp.StringHashLeftOuter(PhysicalOp.Exchange(_), PhysicalOp.Exchange(_)),
          ) if JoinLowering.acceptsShuffleHash(strategy) =>
        true
      case (
            Plan.LeftOuterJoinOp(_, _, Some(Plan.JoinStrategy.SortMerge)),
            PhysicalOp.StringSortMergeLeftOuter(PhysicalOp.Exchange(_), PhysicalOp.Exchange(_)),
          ) =>
        true
      case (
            Plan.RightOuterJoinOp(_, _, Some(Plan.JoinStrategy.Broadcast)),
            PhysicalOp.StringBroadcastRightOuter(_, _),
          ) =>
        true
      case (
            Plan.RightOuterJoinOp(_, _, strategy),
            PhysicalOp.StringHashRightOuter(PhysicalOp.Exchange(_), PhysicalOp.Exchange(_)),
          ) if JoinLowering.acceptsShuffleHash(strategy) =>
        true
      case (
            Plan.RightOuterJoinOp(_, _, Some(Plan.JoinStrategy.SortMerge)),
            PhysicalOp.StringSortMergeRightOuter(PhysicalOp.Exchange(_), PhysicalOp.Exchange(_)),
          ) =>
        true
      case (
            Plan.FullOuterJoinOp(_, _, Some(Plan.JoinStrategy.Broadcast)),
            PhysicalOp.StringBroadcastFullOuter(_, _),
          ) =>
        true
      case (
            Plan.FullOuterJoinOp(_, _, strategy),
            PhysicalOp.StringHashFullOuter(PhysicalOp.Exchange(_), PhysicalOp.Exchange(_)),
          ) if JoinLowering.acceptsShuffleHash(strategy) =>
        true
      case (
            Plan.FullOuterJoinOp(_, _, Some(Plan.JoinStrategy.SortMerge)),
            PhysicalOp.StringSortMergeFullOuter(PhysicalOp.Exchange(_), PhysicalOp.Exchange(_)),
          ) =>
        true
      case _ => false

  def run(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      ordinals: Set[Int],
  ): Columns =
    (plan, op) match
      case (
            Plan.LeftOuterJoinOp(left, right, _),
            PhysicalOp.StringBroadcastLeftOuter(leftOp, rightOp),
          ) =>
        val (probe, build) = pairs(left, right, leftOp, rightOp, selectedPartition, ordinals)
        decline(probe, build, "a null value on a left outer join stays on the row path")
        Columns.Outer(
          StringOuterPipeline.broadcastLeftOuterJoin(
            probe,
            build,
            ColumnarPlanner.parallelism,
            ScarletConf.get.broadcastBuildBudgetBytes,
          ),
          AbsentSide.Right,
        )
      case (
            Plan.LeftOuterJoinOp(left, right, _),
            PhysicalOp.StringHashLeftOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        val (probe, build) = pairs(left, right, leftOp, rightOp, selectedPartition, ordinals)
        decline(probe, build, "a null value on a left outer join stays on the row path")
        Columns.Outer(
          StringOuterPipeline.hashLeftOuterJoin(
            probe,
            build,
            ColumnarPlanner.shuffleWidth,
            ColumnarPlanner.parallelism,
            ColumnarPlanner.batchSize,
            ScarletConf.get.broadcastBuildBudgetBytes,
          ),
          AbsentSide.Right,
        )
      case (
            Plan.LeftOuterJoinOp(left, right, _),
            PhysicalOp.StringSortMergeLeftOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        val (probe, build) = pairs(left, right, leftOp, rightOp, selectedPartition, ordinals)
        decline(probe, build, "a null value on a left outer join stays on the row path")
        Columns.Outer(
          StringOuterPipeline.sortMergeLeftOuterJoin(
            probe,
            build,
            ColumnarPlanner.shuffleWidth,
            ColumnarPlanner.parallelism,
            ColumnarPlanner.batchSize,
          ),
          AbsentSide.Right,
        )
      case (
            Plan.RightOuterJoinOp(left, right, _),
            PhysicalOp.StringBroadcastRightOuter(leftOp, rightOp),
          ) =>
        val (build, probe) = pairs(left, right, leftOp, rightOp, selectedPartition, ordinals)
        swappedRight(probe, build) { (probeBatches, buildBatches) =>
          StringOuterPipeline.broadcastLeftOuterJoin(
            probeBatches,
            buildBatches,
            ColumnarPlanner.parallelism,
            ScarletConf.get.broadcastBuildBudgetBytes,
          )
        }
      case (
            Plan.RightOuterJoinOp(left, right, _),
            PhysicalOp.StringHashRightOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        val (build, probe) = pairs(left, right, leftOp, rightOp, selectedPartition, ordinals)
        swappedRight(probe, build) { (probeBatches, buildBatches) =>
          StringOuterPipeline.hashLeftOuterJoin(
            probeBatches,
            buildBatches,
            ColumnarPlanner.shuffleWidth,
            ColumnarPlanner.parallelism,
            ColumnarPlanner.batchSize,
            ScarletConf.get.broadcastBuildBudgetBytes,
          )
        }
      case (
            Plan.RightOuterJoinOp(left, right, _),
            PhysicalOp.StringSortMergeRightOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        val (leftBatches, rightBatches) =
          pairs(left, right, leftOp, rightOp, selectedPartition, ordinals)
        decline(
          rightBatches,
          leftBatches,
          "a null value on a right outer join stays on the row path",
        )
        Columns.Outer(
          StringOuterPipeline.sortMergeRightOuterJoin(
            leftBatches,
            rightBatches,
            ColumnarPlanner.shuffleWidth,
            ColumnarPlanner.parallelism,
            ColumnarPlanner.batchSize,
          ),
          AbsentSide.Left,
        )
      case (
            Plan.FullOuterJoinOp(left, right, _),
            PhysicalOp.StringBroadcastFullOuter(leftOp, rightOp),
          ) =>
        val (probe, build) = pairs(left, right, leftOp, rightOp, selectedPartition, ordinals)
        full(probe, build) { (probeBatches, buildBatches) =>
          StringOuterPipeline.broadcastFullOuterJoin(
            probeBatches,
            buildBatches,
            ColumnarPlanner.parallelism,
            ScarletConf.get.broadcastBuildBudgetBytes,
          )
        }
      case (
            Plan.FullOuterJoinOp(left, right, _),
            PhysicalOp.StringHashFullOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        val (probe, build) = pairs(left, right, leftOp, rightOp, selectedPartition, ordinals)
        full(probe, build) { (probeBatches, buildBatches) =>
          StringOuterPipeline.hashFullOuterJoin(
            probeBatches,
            buildBatches,
            ColumnarPlanner.shuffleWidth,
            ColumnarPlanner.parallelism,
            ColumnarPlanner.batchSize,
            ScarletConf.get.broadcastBuildBudgetBytes,
          )
        }
      case (
            Plan.FullOuterJoinOp(left, right, _),
            PhysicalOp.StringSortMergeFullOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        val (probe, build) = pairs(left, right, leftOp, rightOp, selectedPartition, ordinals)
        full(probe, build) { (probeBatches, buildBatches) =>
          StringOuterPipeline.sortMergeFullOuterJoin(
            probeBatches,
            buildBatches,
            ColumnarPlanner.shuffleWidth,
            ColumnarPlanner.parallelism,
            ColumnarPlanner.batchSize,
          )
        }
      case _ =>
        throw new IllegalStateException(
          s"physical tree does not match ${plan.getClass.getSimpleName}",
        )

  private def pairs(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      ordinals: Set[Int],
  ): (Vector[ColumnBatch], Vector[ColumnBatch]) =
    val leftBatches = StringPairWalk.batches(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    val rightBatches = StringPairWalk.batches(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    (leftBatches, rightBatches)

  private def decline(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
      message: String,
  ): Unit =
    val present =
      ColumnPipeline.valuesFullyPresent(probe) && ColumnPipeline.valuesFullyPresent(build)
    if (!present) then throw new ColumnarFallback(message)

  /**
   * Run the left-outer kernel with the right input as the probe, then restore
   * `(key, left, right)`. Absence moves with the left value.
   */
  private def swappedRight(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
  )(join: (Vector[ColumnBatch], Vector[ColumnBatch]) => Vector[ColumnBatch]): Columns =
    decline(probe, build, "a null value on a right outer join stays on the row path")
    Columns.Outer(swapValueColumns(join(probe, build)), AbsentSide.Left)

  /** The probe is the left input, so the kernel batch is already `(key, left, right)`. */
  private def full(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
  )(join: (Vector[ColumnBatch], Vector[ColumnBatch]) => Vector[ColumnBatch]): Columns =
    decline(probe, build, "a null value on a full outer join stays on the row path")
    Columns.Outer(join(probe, build), AbsentSide.Both)

  /**
   * Kernel output is `(key, probe value, build value)`. Swap the values to `(key, left, right)`.
   */
  private def swapValueColumns(batches: Vector[ColumnBatch]): Vector[ColumnBatch] =
    batches.map { batch =>
      batch.columns match
        case Seq(key, probeValue, buildValue) =>
          new ColumnBatch(
            Vector(key.logicalType, buildValue.logicalType, probeValue.logicalType),
            Vector(key, buildValue, probeValue),
            batch.length,
          )
        case other =>
          throw new IllegalArgumentException(s"join output has ${other.size} columns")
    }
