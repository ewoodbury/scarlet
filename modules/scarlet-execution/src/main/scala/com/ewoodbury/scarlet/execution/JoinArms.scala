package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnBatch, ColumnPipeline, Selected}
import com.ewoodbury.scarlet.core.Plan

import ColumnPipeline.JoinInput

/**
 * Int inner shuffle-hash join.
 *
 * A slot-filter stack on an owned int-pair scan is copied by [[IntPairFilterEncode]]: one pass,
 * survivors only, no survivor index. The build reads that encoded key column. A boxed source, a
 * null, or any other stack still scatters survivor ids, including a stack of one. An int-pair row
 * filter does the same. The exchange reads those rows and does not slice them. A child with no
 * selection steps stays on the sliced dense exchange. [[FilterChain.selected]] interprets a stack
 * once. The `None` path is the only one that interprets that child.
 *
 * A null value is a clear bit in the output, including a selection. It is not an absence bit. A
 * null key is dropped by the exchange. A selected side skips the slice; both dense sides use the
 * caller's flag and slice. An owned filter copy is already dense and is not sliced. [[count]] uses
 * that same exchange and sums chain lengths.
 */
private[execution] object JoinArms:

  def inner(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    val build = arm(left, leftOp, selectedPartition, preserveAbsence, ordinals)
    val probe = arm(right, rightOp, selectedPartition, preserveAbsence, ordinals)
    if (build.slice && probe.slice) then
      ColumnPipeline.innerJoin(build.batches, probe.batches, width, workers, batchSize)
    else ColumnPipeline.innerJoin(build.input, probe.input, width, workers)

  /** Rows [[inner]] would publish. The match columns are not written. */
  def count(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Long =
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    val build = arm(left, leftOp, selectedPartition, preserveAbsence, ordinals)
    val probe = arm(right, rightOp, selectedPartition, preserveAbsence, ordinals)
    if (build.slice && probe.slice) then
      ColumnPipeline.countInner(build.batches, probe.batches, width, workers, batchSize)
    else ColumnPipeline.countInner(build.input, probe.input, width, workers)

  private final case class Armed(
      input: JoinInput,
      batches: Vector[ColumnBatch],
      slice: Boolean,
  )

  private def arm(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Armed =
    val copied =
      if (selectedPartition.nonEmpty) then None
      else IntPairFilterEncode.tryBatches(plan, op, ordinals)
    copied match
      case Some(batches) => Armed(JoinInput.Dense(batches), batches, false)
      case None =>
        selection(plan, op, selectedPartition, ordinals) match
          case Some(parts) =>
            Armed(JoinInput.Selected(parts), Vector.empty[ColumnBatch], false)
          case None =>
            val batches = pairs(plan, op, selectedPartition, preserveAbsence, ordinals)
            Armed(JoinInput.Dense(batches), batches, true)

  private def selection(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      ordinals: Set[Int],
  ): Option[Vector[Selected]] =
    FilterChain.selected(plan, op, selectedPartition, preserveAbsence = true, ordinals)

  private def pairs(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    ColumnarInterpreter.pairsOf(
      ColumnarInterpreter.interpretAt(plan, op, selectedPartition, preserveAbsence, ordinals),
    )
