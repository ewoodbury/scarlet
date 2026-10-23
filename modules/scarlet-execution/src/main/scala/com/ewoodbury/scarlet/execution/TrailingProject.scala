package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.ColumnBatch
import com.ewoodbury.scarlet.core.Plan

/**
 * Fold an int filter/map chain into `sum` or `count` when `lower` has split it.
 *
 * Chains of two to four steps already sum inside [[ElementSum]] and do not write a column. A
 * longer chain is `Project(Pipeline4(...))` or another chunk on top of that pipeline. The physical
 * tree stays as `lower` built it. The action reads the int scan once, applies every step, and does
 * not write the pipeline column or the trailing projection. A single trailing int project is
 * counted the same way. `collect` still writes the column.
 */
private[execution] object TrailingProject:

  def sum(plan: Plan[_], op: PhysicalOp): Option[Long] =
    chain(plan, op).map { case (source, steps) =>
      foldScan(source)(batch => ElementSum.sumLonger(batch, steps))
    }

  def count(plan: Plan[_], op: PhysicalOp): Option[Long] =
    chain(plan, op) match
      case Some((source, steps)) =>
        Some(foldScan(source)(batch => ElementSum.countLonger(batch, steps)))
      case None =>
        countProject(plan, op)

  private def chain(
      plan: Plan[_],
      op: PhysicalOp,
  ): Option[(Plan.Source[_], List[ColumnarPlanner.ElementStep])] =
    if (!intScanChain(op)) then None
    else
      val (base, steps) = ColumnarPlanner.elementSteps(plan, Int.MaxValue)
      if (steps.length <= 4) then None
      else
        base match
          case source: Plan.Source[_] => Some((source, steps))
          case _ => None

  /** Count one trailing int project. Interpreting that project would write its column. */
  private def countProject(plan: Plan[_], op: PhysicalOp): Option[Long] =
    (plan, op) match
      case (Plan.MapOp(source, mapper), PhysicalOp.Project(child))
          if ColumnarPlanner.isInt(child) =>
        val counted = ColumnarPlanner.foldInterpreted(source, child) { batch =>
          ElementSum.countMapped(batch, mapper)
        }
        Some(counted)
      case _ => None

  private def foldScan(source: Plan.Source[_])(fn: ColumnBatch => Long): Long =
    val scan = PhysicalOp.Scan(PhysicalOp.Kind.Ints, source.partitions)
    ColumnarPlanner.foldInterpreted(source, scan)(fn)

  private def intScanChain(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Scan(PhysicalOp.Kind.Ints, _) => true
    case PhysicalOp.Project(child) => intScanChain(child)
    case PhysicalOp.Pipeline(child, _) => intScanChain(child)
    case PhysicalOp.Filter(child, PhysicalOp.Slot.Element) => intScanChain(child)
    case _ => false
