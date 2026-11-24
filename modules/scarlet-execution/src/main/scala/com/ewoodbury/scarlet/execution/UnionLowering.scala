package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, KernelChoice}

/**
 * Lowering and partition info for [[Plan.UnionOp]].
 *
 * A union concatenates two inputs that share one columns layout. It keeps no partitioner: the
 * output is an unknown-width concatenation of both sides, in an unsorted columnar layout.
 * [[lower]] asks [[ColumnBinding]] before it builds. Different layouts stay [[scala.None]],
 * including when the kinds match. A grouped child reports the same columns as a pair, and
 * [[ColumnsConcat]] cannot join those batches, so that kind stays on the row path.
 * [[com.ewoodbury.scarlet.execution.schema.Describe.tag]] is not this decision.
 */
private[execution] object UnionLowering:

  def lower(
      left: Plan[_],
      right: Plan[_],
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    (lowerChild(left), lowerChild(right)) match
      case (Some(leftOp), Some(rightOp)) if binds(leftOp, rightOp) =>
        Some(PhysicalOp.Union(leftOp, rightOp))
      case (Some(leftOp), Some(rightOp)) =>
        LoweringTrace.note("UnionOp", leftOp.kind, rightOp.kind, "UnionLowering")
        None
      case _ =>
        LoweringTrace.noteIfEmpty("UnionOp", "UnionLowering")
        None

  /**
   * Same columns layout from the binder, and not a grouped batch.
   *
   * The binder runs first. A mixed layout is [[scala.None]] from that ask, not from comparing
   * [[PhysicalOp.Kind]]. Grouped ints, longs, and strings share their pair columns, so the kind
   * check is what keeps those batches off [[ColumnsConcat]].
   */
  private def binds(left: PhysicalOp, right: PhysicalOp): Boolean =
    ColumnBinding.askedUnion(left, right) match
      case Some(KernelChoice.Union) if !grouped(left) && !grouped(right) => true
      case _ => false

  /** A grouped batch is not a column batch. [[ColumnsConcat]] cannot join two of them. */
  private def grouped(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
        PhysicalOp.Kind.GroupedLongs =>
      true
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Pairs | PhysicalOp.Kind.Triples | PhysicalOp.Kind.Strings |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongCounts |
        PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.StringTriples |
        PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums |
        PhysicalOp.Kind.IntLongSums =>
      false

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
