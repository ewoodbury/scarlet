package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.*
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.{Operator, ReducerWitness}

/**
 * Lowering and partition info for [[Plan.UnionOp]].
 *
 * A union concatenates two inputs that report the same [[Output.Columns]] layout. It keeps no
 * partitioner: the output is an unknown-width concatenation of both sides, in an unsorted columnar
 * layout. [[ColumnBinding.choose]] is asked on each child before that comparison.
 * [[com.ewoodbury.scarlet.execution.schema.Describe.tag]] is not this decision.
 *
 * The kind check stays. [[PhysicalOp.Union.kind]] is the left child, and [[PhysicalSchema.of]]
 * reads that kind. [[PhysicalOp.JoinProbeSum]] is [[Layout.ProbeSum]], not
 * [[Layout.AssumedIntLong]], so a probe sum and a triple project do not share a layout. Two probe
 * sums still do. Binding them would publish [[PhysicalOp.Kind.IntLongSums]]. The union interpreter
 * does not run that node, and the failure is not a
 * [[com.ewoodbury.scarlet.columnar.ColumnarFallback]], so the row path would not run. A grouped
 * batch is not a column batch. Those shapes stay on the row path.
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
   * Same column layout, and a kind [[ColumnsConcat]] can append.
   *
   * [[agreed]] asks the binder even when the kinds differ, so a mixed pair is not refused before
   * [[ColumnBinding.choose]].
   */
  private def binds(left: PhysicalOp, right: PhysicalOp): Boolean =
    agreed(left, right) && executable(left, right)

  /**
   * Both children are [[Output.Columns]], the tags are [[KernelChoice.Union]], and the columns
   * match.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def agreed(left: PhysicalOp, right: PhysicalOp): Boolean =
    (columnsOf(left), columnsOf(right)) match
      case (Some(leftColumns), Some(rightColumns)) =>
        tagged(left, leftColumns) && tagged(right, rightColumns) && leftColumns == rightColumns
      case _ =>
        false

  /**
   * [[ColumnBinding.choose]] on `columns`. An opaque child does not reach this call. The reducer
   * witness is not a union input.
   */
  private def tagged(op: PhysicalOp, columns: Vector[ColumnLayout]): Boolean =
    val bound = Bound.of(op, Vector.empty, ReducerWitness.NotApplicable)
    ColumnBinding.choose(
      columns,
      Operator.Union,
      ReducerWitness.NotApplicable,
      Bound.nulls(bound),
      Bound.side(bound),
    ) match
      case Some(KernelChoice.Union) => true
      case _ => false

  private def columnsOf(op: PhysicalOp): Option[Vector[ColumnLayout]] = Layout.known(op) match
    case Output.Columns(columns) => Some(columns)
    case Output.IntLongPairs(_) | Output.Opaque(_) => None

  /**
   * Same kind, and a batch [[ColumnsConcat]] can append.
   *
   * Grouped kinds are not column batches. [[PhysicalOp.Kind.IntLongSums]] is only
   * [[PhysicalOp.JoinProbeSum]], which the union interpreter does not run.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def executable(left: PhysicalOp, right: PhysicalOp): Boolean =
    left.kind == right.kind && concatenable(left.kind)

  private def concatenable(kind: PhysicalOp.Kind): Boolean = kind match
    case PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
        PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.IntLongSums |
        PhysicalOp.Kind.GroupedIntLongs | PhysicalOp.Kind.IntLongTriples |
        PhysicalOp.Kind.IntLongOuter =>
      false
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.Strings | PhysicalOp.Kind.Pairs |
        PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.Triples |
        PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.LongCounts |
        PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums |
        PhysicalOp.Kind.LongCountSums | PhysicalOp.Kind.StringCountSums =>
      true

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
