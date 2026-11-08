package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnBatch, Selected, StringHashJoin}
import com.ewoodbury.scarlet.core.Plan

import StringHashJoin.StringJoinInput

/**
 * String inner shuffle-hash join.
 *
 * A key or value stack on either side scatters survivor ids, including a stack of one. The
 * exchange reads those rows and does not slice them. Every other child stays on the dense
 * exchange. [[FilterChain.selected]] interprets a stack once. The `None` path is the only one that
 * interprets that child. Both dense sides keep the batch [[StringHashJoin.inner]] call.
 *
 * A null key is dropped by the exchange. `""` is a present key. A null value stays on the row
 * path, because classification never selects string pairs for it. An over-budget build bucket
 * spills inside [[StringHashJoin.inner]] when spill is enabled. A bucket at or under the budget,
 * and every bucket while spill is off, stays on that in-memory join.
 */
private[execution] object StringJoinArms:

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
    val leftSelected = selection(left, leftOp, selectedPartition, preserveAbsence, ordinals)
    val rightSelected = selection(right, rightOp, selectedPartition, preserveAbsence, ordinals)
    (leftSelected, rightSelected) match
      case (None, None) =>
        StringHashJoin.inner(
          batches(left, leftOp, selectedPartition, preserveAbsence, ordinals),
          batches(right, rightOp, selectedPartition, preserveAbsence, ordinals),
          width,
          workers,
        )
      case _ =>
        StringHashJoin.inner(
          side(left, leftOp, leftSelected, selectedPartition, preserveAbsence, ordinals),
          side(right, rightOp, rightSelected, selectedPartition, preserveAbsence, ordinals),
          width,
          workers,
        )

  private def selection(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Option[Vector[Selected]] =
    FilterChain.selected(plan, op, selectedPartition, preserveAbsence, ordinals)

  private def side(
      plan: Plan[_],
      op: PhysicalOp,
      selected: Option[Vector[Selected]],
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): StringJoinInput =
    selected match
      case Some(parts) => StringJoinInput.Selected(parts)
      case None =>
        StringJoinInput.Dense(batches(plan, op, selectedPartition, preserveAbsence, ordinals))

  private def batches(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    StringPairWalk.batches(
      ColumnarInterpreter.interpretAt(
        plan,
        op,
        selectedPartition,
        preserveAbsence,
        ordinals,
      ),
    )
