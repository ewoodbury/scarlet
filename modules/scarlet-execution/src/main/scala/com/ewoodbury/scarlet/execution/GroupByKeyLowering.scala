package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, KernelChoice}

/**
 * Lowers int, long, and string `groupByKey`.
 *
 * The child is an exchange, and the kernel performs the shuffle. A null key is dropped. A null
 * value is not dropped: a clear value bit is a planner bug. One grouped batch is published per
 * shuffle partition, including an empty bucket. There is no partial combine and no sort. Spill off
 * keeps the in-memory groups and writes nothing. Spill on slices one over-budget post-exchange
 * bucket in input order. A morsel whose group table does not fit throws
 * [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]] before that table. The appended groups
 * are not bounded by the budget. Key order inside a partition is first-seen. Value order inside a
 * key is input order. `values` and `take` stay on the row path. One map directly above the group
 * is [[GroupedDecode]]: the group runs once and the function sees the decoded rows. Int, long, and
 * string `keys` lower separately and do not build value lists. `count` is the number of groups.
 * [[PhysicalOp.Kind.Pairs]], [[PhysicalOp.Kind.LongPairs]], and [[PhysicalOp.Kind.StringPairs]]
 * lower. Every other kind stays on the row path.
 *
 * [[apply]] asks [[ColumnBinding]] before it builds. [[scala.None]] from the binder stays
 * [[scala.None]]. A grouped child reports the same two column types as a pair, so the kind check
 * still refuses it. [[com.ewoodbury.scarlet.execution.schema.Describe.tag]] is not this decision.
 */
private[execution] object GroupByKeyLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] =
    if tagged(child) && acceptedKind(child) then
      Some(PhysicalOp.GroupByKey(PhysicalOp.Exchange(child)))
    else
      LoweringTrace.note("GroupByKeyOp", child.kind, "GroupByKeyLowering")
      None

  /** Binder tag for this group, or false when the layout is one the group refuses. */
  private def tagged(child: PhysicalOp): Boolean =
    ColumnBinding.asked(child, Operator.GroupByKey) match
      case Some(KernelChoice.GroupByKey) => true
      case _ => false

  /**
   * Int, long, or string pairs. A grouped child has the same column types as a pair and is not
   * this shape.
   */
  private def acceptedKind(child: PhysicalOp): Boolean = child.kind match
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.StringPairs =>
      true
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.Strings |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.LongCounts |
        PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
        PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter |
        PhysicalOp.Kind.CountSums | PhysicalOp.Kind.IntLongSums =>
      false
