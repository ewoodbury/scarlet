package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnBatch, ColumnPipeline, Selected}
import com.ewoodbury.scarlet.core.Plan

import ColumnPipeline.JoinInput

/**
 * Int inner shuffle-hash join.
 *
 * A slot-filter stack on either side scatters survivor ids, including a stack of one. An int-pair
 * row filter does the same. The exchange reads those rows and does not slice them. A child with no
 * selection steps stays on the sliced dense exchange. [[FilterChain.selected]] interprets a stack
 * once. The `None` path is the only one that interprets that child.
 *
 * A null value is a clear bit in the output, including a selection. It is not an absence bit.
 * A null key is dropped by the exchange. A selected side skips the slice; both dense sides use
 * the caller's flag and slice.
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
    val leftSelected = selection(left, leftOp, selectedPartition, ordinals)
    val rightSelected = selection(right, rightOp, selectedPartition, ordinals)
    (leftSelected, rightSelected) match
      case (None, None) =>
        ColumnPipeline.innerJoin(
          pairs(left, leftOp, selectedPartition, preserveAbsence, ordinals),
          pairs(right, rightOp, selectedPartition, preserveAbsence, ordinals),
          width,
          workers,
          batchSize,
        )
      case _ =>
        val build = input(left, leftOp, leftSelected, selectedPartition, ordinals)
        val probe = input(right, rightOp, rightSelected, selectedPartition, ordinals)
        ColumnPipeline.innerJoin(build, probe, width, workers)

  private def selection(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      ordinals: Set[Int],
  ): Option[Vector[Selected]] =
    FilterChain.selected(plan, op, selectedPartition, preserveAbsence = true, ordinals)

  private def input(
      plan: Plan[_],
      op: PhysicalOp,
      selected: Option[Vector[Selected]],
      selectedPartition: Option[Int],
      ordinals: Set[Int],
  ): JoinInput =
    selected match
      case Some(parts) => JoinInput.Selected(parts)
      case None =>
        JoinInput.Dense(pairs(plan, op, selectedPartition, preserveAbsence = true, ordinals))

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
