package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.IntLongSort
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns

/**
 * Runs the int-long key sort and the int-long value sort on published batches.
 *
 * The child is already filtered or mapped. A null orders as zero and stays null. There is no
 * spill.
 */
private[execution] object IntLongSortColumns:

  def accepts(plan: Plan[_], op: PhysicalOp): Boolean =
    (plan, op) match
      case (Plan.SortByIntLongKeyOp(_), _: PhysicalOp.SortByIntLongKey) => true
      case (Plan.SortByIntLongValueDescOp(_), _: PhysicalOp.SortByIntLongValueDesc) => true
      case _ => false

  def dispatch(plan: Plan[_], op: PhysicalOp, ordinals: Set[Int]): Columns =
    val (source, child) = (plan, op) match
      case (Plan.SortByIntLongKeyOp(source), PhysicalOp.SortByIntLongKey(child)) =>
        (source, child)
      case (
            Plan.SortByIntLongValueDescOp(source),
            PhysicalOp.SortByIntLongValueDesc(child),
          ) =>
        (source, child)
      case _ =>
        throw new IllegalStateException("int-long sort expected its own plan")
    val batches = ColumnarInterpreter.pairsOf(
      ColumnarInterpreter.interpretAt(
        source,
        child,
        selectedPartition = None,
        preserveAbsence = true,
        ordinals,
      ),
    )
    val sorted = op match
      case _: PhysicalOp.SortByIntLongKey =>
        IntLongSort.byKey(batches, ColumnarPlanner.shuffleWidth)
      case _ =>
        IntLongSort.byValueDesc(batches, ColumnarPlanner.shuffleWidth)
    Columns.Pairs(sorted)
