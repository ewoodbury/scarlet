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
 * lower. An int-to-long [[PhysicalOp.IntLongMapValue]] also lowers. A filter of that map does not:
 * its layout is still int-long pairs, and [[ColumnBinding.asked]] refuses it. Every other kind
 * stays on the row path. The int-long group keeps a null value in the list.
 *
 * [[apply]] asks [[ColumnBinding]] before it builds. [[scala.None]] from the binder stays
 * [[scala.None]]. A [[PhysicalOp.GroupByKey]] child is
 * [[com.ewoodbury.scarlet.execution.schema.Layout.Grouped]], so the binder refuses it. The kind
 * check still refuses a grouped kind. [[com.ewoodbury.scarlet.execution.schema.Describe.tag]] is
 * not this decision.
 */
private[execution] object GroupByKeyLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] =
    if intLongValue(child) then Some(PhysicalOp.GroupByKey(PhysicalOp.Exchange(child)))
    else if tagged(child) && acceptedKind(child) then
      Some(PhysicalOp.GroupByKey(PhysicalOp.Exchange(child)))
    else
      LoweringTrace.note("GroupByKeyOp", child.kind, "GroupByKeyLowering")
      None

  /** Int-to-long `mapValues`. A filter of that map is not this node. */
  private def intLongValue(child: PhysicalOp): Boolean = child match
    case _: PhysicalOp.IntLongMapValue => true
    case _ => false

  /** Binder tag for this group, or false when the layout is one the group refuses. */
  private def tagged(child: PhysicalOp): Boolean =
    ColumnBinding.asked(child, Operator.GroupByKey) match
      case Some(KernelChoice.GroupByKey) => true
      case _ => false

  /**
   * Int, long, or string pairs. A grouped kind is not this shape. The binder already refuses an
   * opaque [[PhysicalOp.GroupByKey]].
   */
  private def acceptedKind(child: PhysicalOp): Boolean = child.kind match
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.StringPairs =>
      true
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.Strings |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.LongCounts |
        PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
        PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter |
        PhysicalOp.Kind.CountSums | PhysicalOp.Kind.LongCountSums |
        PhysicalOp.Kind.StringCountSums | PhysicalOp.Kind.IntLongSums |
        PhysicalOp.Kind.GroupedIntLongs =>
      false
