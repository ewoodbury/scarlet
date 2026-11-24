package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, KernelChoice}

/**
 * Lowers `distinct` of an int column or a string column.
 *
 * The child is an exchange, and the kernel performs the shuffle. [[PhysicalOp.Kind.Ints]] and
 * [[PhysicalOp.Kind.Strings]] lower to [[PhysicalOp.Dedupe]]. Every other kind stays on the row
 * path.
 *
 * [[apply]] asks [[ColumnBinding]] before it builds. [[scala.None]] from the binder stays
 * [[scala.None]]. The kind check stays, so a column type the binder does not tag is not the only
 * refusal. [[com.ewoodbury.scarlet.execution.schema.Describe.tag]] is not this decision.
 */
private[execution] object DistinctLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] =
    if tagged(child) && acceptedKind(child) then
      Some(PhysicalOp.Dedupe(PhysicalOp.Exchange(child)))
    else
      LoweringTrace.note("DistinctOp", child.kind, "DistinctLowering")
      None

  /** Binder tag for this distinct, or false when the layout is one distinct refuses. */
  private def tagged(child: PhysicalOp): Boolean =
    ColumnBinding.asked(child, Operator.Distinct) match
      case Some(KernelChoice.Dedupe) => true
      case _ => false

  /** An int column or a string column. A long column is not this shape. */
  private def acceptedKind(child: PhysicalOp): Boolean = child.kind match
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Strings =>
      true
    case PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats | PhysicalOp.Kind.Pairs |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.Bools | PhysicalOp.Kind.LongPairs |
        PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.StringPairs |
        PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.GroupedInts |
        PhysicalOp.Kind.GroupedStrings | PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter |
        PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums | PhysicalOp.Kind.IntLongSums =>
      false
