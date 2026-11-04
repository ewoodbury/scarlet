package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan

/**
 * Lowering and partition info for [[Plan.UnionOp]].
 *
 * A union concatenates two inputs of the same kind. It keeps no partitioner: the output is an
 * unknown-width concatenation of both sides, in an unsorted columnar layout. Two sides of
 * different kinds are refused, and the row path runs.
 */
private[execution] object UnionLowering:

  def lower(
      left: Plan[_],
      right: Plan[_],
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    (lowerChild(left), lowerChild(right)) match
      case (Some(leftOp), Some(rightOp)) if leftOp.kind == rightOp.kind =>
        Some(PhysicalOp.Union(leftOp, rightOp))
      case _ => None

  def info(
      op: PhysicalOp.Union,
      infoOf: PhysicalOp => PartitioningInfo,
      layout: Layout,
  ): PartitioningInfo =
    PartitioningInfo(
      Distribution.Unknown(infoOf(op.left).numPartitions + infoOf(op.right).numPartitions),
      OrderingTag.Unsorted,
      layout,
    )
