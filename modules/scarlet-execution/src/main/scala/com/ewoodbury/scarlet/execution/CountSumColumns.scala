package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{IntLongCountSum, LongCountSum, StringCountSum}
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, longPairsOf, pairsOf}

/**
 * Count-sum after the child is published.
 *
 * The int path stays in [[CountSumArms]], including the owned fold. Long, string, and int-long
 * read the batches the child already published. A null key is dropped. A null value is counted and
 * not added.
 */
private[execution] object CountSumColumns:

  def accepts(plan: Plan[_], op: PhysicalOp): Boolean = (plan, op) match
    case (Plan.CountSumByKeyOp(_), PhysicalOp.CountSum(PhysicalOp.Exchange(_))) => true
    case (Plan.CountSumByLongKeyOp(_), PhysicalOp.LongCountSum(PhysicalOp.Exchange(_))) => true
    case (Plan.CountSumByStringKeyOp(_), PhysicalOp.StringCountSum(PhysicalOp.Exchange(_))) =>
      true
    case (Plan.CountSumByIntLongKeyOp(_), PhysicalOp.IntLongCountSum(PhysicalOp.Exchange(_))) =>
      true
    case _ => false

  def dispatch(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns = (plan, op) match
    case (
          Plan.CountSumByKeyOp(source),
          PhysicalOp.CountSum(PhysicalOp.Exchange(child)),
        ) =>
      CountSumArms.columns(source, child, selectedPartition, preserveAbsence, ordinals)
    case (
          Plan.CountSumByLongKeyOp(source),
          PhysicalOp.LongCountSum(PhysicalOp.Exchange(child)),
        ) =>
      longs(source, child, selectedPartition, preserveAbsence, ordinals)
    case (
          Plan.CountSumByStringKeyOp(source),
          PhysicalOp.StringCountSum(PhysicalOp.Exchange(child)),
        ) =>
      strings(source, child, selectedPartition, preserveAbsence, ordinals)
    case (
          Plan.CountSumByIntLongKeyOp(source),
          PhysicalOp.IntLongCountSum(PhysicalOp.Exchange(child)),
        ) =>
      intLongs(source, child, selectedPartition, preserveAbsence, ordinals)
    case _ =>
      throw new IllegalStateException("count-sum dispatch expected a count-sum aggregate")

  def intLongs(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    Columns.CountSums(
      IntLongCountSum.byKey(
        pairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)),
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
      ),
    )

  def longs(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    Columns.LongCountSums(
      LongCountSum.byKey(
        longPairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)),
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
      ),
    )

  def strings(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    Columns.StringCountSums(
      StringCountSum.byKey(
        StringPairWalk.batches(
          interpretAt(source, child, selectedPartition, preserveAbsence, ordinals),
        ),
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
      ),
    )
