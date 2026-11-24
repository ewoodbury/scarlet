package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.execution.schema.KernelChoice

/**
 * Kernel table for [[ColumnarInterpreter]].
 *
 * One arm per [[KernelChoice]]. [[KernelChoice.IntLongProjectSum]] is the only tag that runs
 * [[IntLongReduce]]. Every other tag keeps the path the interpreter already takes for that node.
 * This does not add a kernel.
 *
 * [[PhysicalSchema.isIntLongAggregate]] is not an arm. A node whose layout is `[Int32, Int64]`
 * still takes [[Arm.Existing]] when its tag is not [[KernelChoice.IntLongProjectSum]].
 * [[KernelChoice.JoinProbeSum]] is that shape: its layout is the assumed int-long columns and its
 * tag is not the project sum. [[KernelChoice.CountByKey]] still covers both the plain count and
 * the int-long count. Those two arms stay in the interpreter, because one tag does not choose
 * between them.
 */
private[execution] object KernelRoute:

  /** What [[ColumnarInterpreter]] runs after it has read the tag. */
  enum Arm:

    /** Decode with [[JoinProbeAggregate.decodeSums]] and run [[IntLongReduce]]. */
    case IntLongReduce

    /** The match the interpreter already has for this node. */
    case Existing

  /** One arm. A new [[KernelChoice]] does not compile until it has a case here. */
  def arm(kernel: KernelChoice): Arm = kernel match
    case KernelChoice.IntHashAggregate => Arm.Existing
    case KernelChoice.LongHashAggregate => Arm.Existing
    case KernelChoice.StringHashAggregate => Arm.Existing
    case KernelChoice.IntLongProjectSum => Arm.IntLongReduce
    case KernelChoice.CountByKey => Arm.Existing
    case KernelChoice.CountThroughProbe => Arm.Existing
    case KernelChoice.CountSum => Arm.Existing
    case KernelChoice.JoinProbeSum => Arm.Existing
    case KernelChoice.SortByIntKey => Arm.Existing
    case KernelChoice.SortByIntValueDesc => Arm.Existing
    case KernelChoice.SortByLongKey => Arm.Existing
    case KernelChoice.SortByStringKey => Arm.Existing
    case KernelChoice.GroupByKey => Arm.Existing
    case KernelChoice.GroupedDecode => Arm.Existing
    case KernelChoice.Dedupe => Arm.Existing
    case KernelChoice.HashJoin => Arm.Existing
    case KernelChoice.BroadcastJoin => Arm.Existing
    case KernelChoice.SortMergeJoin => Arm.Existing
    case KernelChoice.HashLeftOuter => Arm.Existing
    case KernelChoice.HashRightOuter => Arm.Existing
    case KernelChoice.HashFullOuter => Arm.Existing
    case KernelChoice.BroadcastLeftOuter => Arm.Existing
    case KernelChoice.BroadcastRightOuter => Arm.Existing
    case KernelChoice.BroadcastFullOuter => Arm.Existing
    case KernelChoice.SortMergeLeftOuter => Arm.Existing
    case KernelChoice.SortMergeRightOuter => Arm.Existing
    case KernelChoice.SortMergeFullOuter => Arm.Existing
    case KernelChoice.LongHashJoin => Arm.Existing
    case KernelChoice.LongBroadcastJoin => Arm.Existing
    case KernelChoice.LongSortMergeJoin => Arm.Existing
    case KernelChoice.LongHashLeftOuter => Arm.Existing
    case KernelChoice.LongHashRightOuter => Arm.Existing
    case KernelChoice.LongHashFullOuter => Arm.Existing
    case KernelChoice.LongBroadcastLeftOuter => Arm.Existing
    case KernelChoice.LongBroadcastRightOuter => Arm.Existing
    case KernelChoice.LongBroadcastFullOuter => Arm.Existing
    case KernelChoice.LongSortMergeLeftOuter => Arm.Existing
    case KernelChoice.LongSortMergeRightOuter => Arm.Existing
    case KernelChoice.LongSortMergeFullOuter => Arm.Existing
    case KernelChoice.StringHashJoin => Arm.Existing
    case KernelChoice.StringBroadcastJoin => Arm.Existing
    case KernelChoice.StringSortMergeJoin => Arm.Existing
    case KernelChoice.StringHashLeftOuter => Arm.Existing
    case KernelChoice.StringHashRightOuter => Arm.Existing
    case KernelChoice.StringHashFullOuter => Arm.Existing
    case KernelChoice.StringBroadcastLeftOuter => Arm.Existing
    case KernelChoice.StringBroadcastRightOuter => Arm.Existing
    case KernelChoice.StringBroadcastFullOuter => Arm.Existing
    case KernelChoice.StringSortMergeLeftOuter => Arm.Existing
    case KernelChoice.StringSortMergeRightOuter => Arm.Existing
    case KernelChoice.StringSortMergeFullOuter => Arm.Existing
    case KernelChoice.Scan => Arm.Existing
    case KernelChoice.Filter => Arm.Existing
    case KernelChoice.Project => Arm.Existing
    case KernelChoice.Column => Arm.Existing
    case KernelChoice.MapValue => Arm.Existing
    case KernelChoice.MapKey => Arm.Existing
    case KernelChoice.Pipeline => Arm.Existing
    case KernelChoice.Union => Arm.Existing
    case KernelChoice.Exchange => Arm.Existing
