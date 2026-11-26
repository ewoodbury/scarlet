package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.IntJoinTriplePredicate
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, KernelChoice}

/**
 * `filter`, `filterKeys`, and `filterValues` ask [[ColumnBinding]] on the child layout.
 *
 * The tag that comes back is [[KernelChoice.Filter]], the class these parents already build. A
 * binder [[scala.None]] is [[scala.None]]. One key or value slot above a triple project whose
 * schema is `[Int32, Int64]` stays. That layout is
 * [[com.ewoodbury.scarlet.execution.schema.Layout.AssumedIntLong]], and the binder does not name a
 * filter for it. [[PhysicalSchema.isIntLongProject]] is that fact. The slot above one int key map,
 * one string-pair project, or one long-pair project is not this decision. [[ColumnarPlanner]]
 * keeps those paths before it asks here.
 *
 * A whole-row filter still uses the slot this child already used. The selection compiler is
 * unchanged. An element chain asks only when it contains a filter, and then keeps today's builder
 * only when the tag is [[KernelChoice.Filter]]. A pure project does not ask.
 */
private[execution] object FilterBinding:

  /** Key or value slot after the slot-above guards. */
  def slot(child: PhysicalOp, which: PhysicalOp.Slot): Option[PhysicalOp] =
    if (PhysicalSchema.isIntLongProject(child)) then Some(PhysicalOp.Filter(child, which))
    else build(child, parent(which), Some(which))

  /** Whole-row filter. The slot is the one this child already uses. */
  def row(child: PhysicalOp, predicate: Any): Option[PhysicalOp] =
    build(child, Operator.Filter, rowSlot(child, predicate))

  /** A filter whose slot is already chosen. A sort row filter and a string row filter use this. */
  def fixed(child: PhysicalOp, which: PhysicalOp.Slot): Option[PhysicalOp] =
    build(child, Operator.Filter, Some(which))

  /**
   * True when an element chain may keep today's builder.
   *
   * A chain with no filter does not ask. That project is a later section. A chain with a filter
   * asks [[Operator.Filter]] and keeps the builder only for [[KernelChoice.Filter]].
   */
  def element(child: PhysicalOp, steps: List[ColumnarPlanner.ElementStep]): Boolean =
    if (steps.exists(filterStep)) then admits(child, Operator.Filter) else true

  private def parent(which: PhysicalOp.Slot): Operator = which match
    case PhysicalOp.Slot.Key => Operator.FilterKeys
    case PhysicalOp.Slot.Value => Operator.FilterValues
    case PhysicalOp.Slot.Element | PhysicalOp.Slot.Row | PhysicalOp.Slot.TripleKey |
        PhysicalOp.Slot.TripleLeftValue | PhysicalOp.Slot.TripleRightValue |
        PhysicalOp.Slot.TripleRow =>
      Operator.Filter

  private def filterStep(step: ColumnarPlanner.ElementStep): Boolean = step match
    case ColumnarPlanner.ElementStep.Filter(_) => true
    case ColumnarPlanner.ElementStep.Project(_) => false

  private def build(
      child: PhysicalOp,
      operator: Operator,
      which: Option[PhysicalOp.Slot],
  ): Option[PhysicalOp] = which match
    case Some(chosen) if admits(child, operator) => Some(PhysicalOp.Filter(child, chosen))
    case _ => None

  private def admits(child: PhysicalOp, operator: Operator): Boolean =
    ColumnBinding.asked(child, operator).contains(KernelChoice.Filter)

  private def rowSlot(child: PhysicalOp, predicate: Any): Option[PhysicalOp.Slot] =
    child.kind match
      case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.LongPairs =>
        Some(PhysicalOp.Slot.Row)
      case PhysicalOp.Kind.Triples =>
        tripleSlot(predicate).orElse(innerRow(child))
      case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
          PhysicalOp.Kind.Bools | PhysicalOp.Kind.Strings | PhysicalOp.Kind.StringPairs |
          PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples |
          PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.GroupedInts |
          PhysicalOp.Kind.GroupedStrings | PhysicalOp.Kind.GroupedLongs |
          PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums |
          PhysicalOp.Kind.IntLongSums | PhysicalOp.Kind.GroupedIntLongs =>
        None

  private def innerRow(child: PhysicalOp): Option[PhysicalOp.Slot] =
    if (child.isInnerTriple) then Some(PhysicalOp.Slot.TripleRow) else None

  private def tripleSlot(predicate: Any): Option[PhysicalOp.Slot] = predicate match
    case recognized: IntJoinTriplePredicate[?] =>
      recognized.selectedSlot match
        case IntJoinTriplePredicate.Slot.Key => Some(PhysicalOp.Slot.TripleKey)
        case IntJoinTriplePredicate.Slot.LeftValue => Some(PhysicalOp.Slot.TripleLeftValue)
        case IntJoinTriplePredicate.Slot.RightValue => Some(PhysicalOp.Slot.TripleRightValue)
    case _ => None
