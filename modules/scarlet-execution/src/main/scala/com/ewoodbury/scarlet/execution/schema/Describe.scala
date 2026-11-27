package com.ewoodbury.scarlet.execution.schema

import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.{ColumnarPlanner, PhysicalOp}

/**
 * Tags the root [[ColumnarPlanner.lower]] returns today.
 *
 * This is not the binder. It does not choose a kernel, and it does not change lowering.
 * [[scala.None]] means lowering refused the plan. Refuse is not a [[KernelChoice]].
 */
private[execution] object Describe:

  /** Lowered root and the tag for that root. */
  final case class Described(root: PhysicalOp, choice: KernelChoice)

  def describe(plan: Plan[_]): Option[Described] =
    ColumnarPlanner.lower(plan).map { root =>
      Described(root, choice(root))
    }

  /**
   * Tag an already lowered root.
   *
   * Same match as [[describe]]. Does not call [[ColumnarPlanner.lower]], so a parent can compare a
   * binder tag while it is itself inside that lower.
   */
  private[execution] def tag(root: PhysicalOp): KernelChoice = choice(root)

  /**
   * Tag `root`. A hash aggregate reports the tag its builder stored.
   * [[PhysicalOp.HashAggregateTag.IntHash]] is [[KernelChoice.IntHashAggregate]] even when the
   * child schema is `[Int32, Int64]`. [[PhysicalOp.HashAggregateTag.IntLongSum]] is
   * [[KernelChoice.IntLongProjectSum]]. A count whose child is an int hash join is
   * [[KernelChoice.CountThroughProbe]]. Every other root keeps its class name. The distinct class
   * is [[PhysicalOp.Dedupe]].
   */
  private def choice(root: PhysicalOp): KernelChoice = root match
    case PhysicalOp.HashAggregate(_, tag) =>
      tag match
        case PhysicalOp.HashAggregateTag.IntHash => KernelChoice.IntHashAggregate
        case PhysicalOp.HashAggregateTag.IntLongSum => KernelChoice.IntLongProjectSum
    case PhysicalOp.CountByKey(PhysicalOp.HashJoin(_, _)) =>
      KernelChoice.CountThroughProbe
    case _: PhysicalOp.LongHashAggregate => KernelChoice.LongHashAggregate
    case _: PhysicalOp.IntLongHashAggregate => KernelChoice.IntLongHashAggregate
    case _: PhysicalOp.StringHashAggregate => KernelChoice.StringHashAggregate
    case _: PhysicalOp.CountByKey => KernelChoice.CountByKey
    case _: PhysicalOp.CountSum | _: PhysicalOp.LongCountSum | _: PhysicalOp.StringCountSum |
        _: PhysicalOp.IntLongCountSum =>
      KernelChoice.CountSum
    case _: PhysicalOp.JoinProbeSum => KernelChoice.JoinProbeSum
    case _: PhysicalOp.SortByIntKey => KernelChoice.SortByIntKey
    case _: PhysicalOp.SortByIntValueDesc => KernelChoice.SortByIntValueDesc
    case _: PhysicalOp.SortByLongKey => KernelChoice.SortByLongKey
    case _: PhysicalOp.SortByStringKey => KernelChoice.SortByStringKey
    case _: PhysicalOp.SortByLongValueDesc => KernelChoice.SortByLongValueDesc
    case _: PhysicalOp.SortByStringValueDesc => KernelChoice.SortByStringValueDesc
    case _: PhysicalOp.SortByIntLongKey => KernelChoice.SortByIntLongKey
    case _: PhysicalOp.SortByIntLongValueDesc => KernelChoice.SortByIntLongValueDesc
    case _: PhysicalOp.GroupByKey => KernelChoice.GroupByKey
    case _: PhysicalOp.GroupedDecode => KernelChoice.GroupedDecode
    case _: PhysicalOp.Dedupe => KernelChoice.Dedupe
    case _: PhysicalOp.HashJoin => KernelChoice.HashJoin
    case _: PhysicalOp.BroadcastJoin => KernelChoice.BroadcastJoin
    case _: PhysicalOp.SortMergeJoin => KernelChoice.SortMergeJoin
    case _: PhysicalOp.HashLeftOuter => KernelChoice.HashLeftOuter
    case _: PhysicalOp.HashRightOuter => KernelChoice.HashRightOuter
    case _: PhysicalOp.HashFullOuter => KernelChoice.HashFullOuter
    case _: PhysicalOp.BroadcastLeftOuter => KernelChoice.BroadcastLeftOuter
    case _: PhysicalOp.BroadcastRightOuter => KernelChoice.BroadcastRightOuter
    case _: PhysicalOp.BroadcastFullOuter => KernelChoice.BroadcastFullOuter
    case _: PhysicalOp.SortMergeLeftOuter => KernelChoice.SortMergeLeftOuter
    case _: PhysicalOp.SortMergeRightOuter => KernelChoice.SortMergeRightOuter
    case _: PhysicalOp.SortMergeFullOuter => KernelChoice.SortMergeFullOuter
    case _: PhysicalOp.LongHashJoin => KernelChoice.LongHashJoin
    case _: PhysicalOp.IntLongHashJoin => KernelChoice.IntLongHashJoin
    case _: PhysicalOp.IntLongHashLeftOuter => KernelChoice.IntLongHashLeftOuter
    case _: PhysicalOp.IntLongHashRightOuter => KernelChoice.IntLongHashRightOuter
    case _: PhysicalOp.IntLongHashFullOuter => KernelChoice.IntLongHashFullOuter
    case _: PhysicalOp.IntLongBroadcastJoin => KernelChoice.IntLongBroadcastJoin
    case _: PhysicalOp.IntLongBroadcastLeftOuter => KernelChoice.IntLongBroadcastLeftOuter
    case _: PhysicalOp.IntLongBroadcastRightOuter => KernelChoice.IntLongBroadcastRightOuter
    case _: PhysicalOp.IntLongBroadcastFullOuter => KernelChoice.IntLongBroadcastFullOuter
    case _: PhysicalOp.IntLongSortMergeJoin => KernelChoice.IntLongSortMergeJoin
    case _: PhysicalOp.IntLongSortMergeLeftOuter => KernelChoice.IntLongSortMergeLeftOuter
    case _: PhysicalOp.IntLongSortMergeRightOuter => KernelChoice.IntLongSortMergeRightOuter
    case _: PhysicalOp.IntLongSortMergeFullOuter => KernelChoice.IntLongSortMergeFullOuter
    case _: PhysicalOp.LongBroadcastJoin => KernelChoice.LongBroadcastJoin
    case _: PhysicalOp.LongSortMergeJoin => KernelChoice.LongSortMergeJoin
    case _: PhysicalOp.LongHashLeftOuter => KernelChoice.LongHashLeftOuter
    case _: PhysicalOp.LongHashRightOuter => KernelChoice.LongHashRightOuter
    case _: PhysicalOp.LongHashFullOuter => KernelChoice.LongHashFullOuter
    case _: PhysicalOp.LongBroadcastLeftOuter => KernelChoice.LongBroadcastLeftOuter
    case _: PhysicalOp.LongBroadcastRightOuter => KernelChoice.LongBroadcastRightOuter
    case _: PhysicalOp.LongBroadcastFullOuter => KernelChoice.LongBroadcastFullOuter
    case _: PhysicalOp.LongSortMergeLeftOuter => KernelChoice.LongSortMergeLeftOuter
    case _: PhysicalOp.LongSortMergeRightOuter => KernelChoice.LongSortMergeRightOuter
    case _: PhysicalOp.LongSortMergeFullOuter => KernelChoice.LongSortMergeFullOuter
    case _: PhysicalOp.StringHashJoin => KernelChoice.StringHashJoin
    case _: PhysicalOp.StringBroadcastJoin => KernelChoice.StringBroadcastJoin
    case _: PhysicalOp.StringSortMergeJoin => KernelChoice.StringSortMergeJoin
    case _: PhysicalOp.StringHashLeftOuter => KernelChoice.StringHashLeftOuter
    case _: PhysicalOp.StringHashRightOuter => KernelChoice.StringHashRightOuter
    case _: PhysicalOp.StringHashFullOuter => KernelChoice.StringHashFullOuter
    case _: PhysicalOp.StringBroadcastLeftOuter => KernelChoice.StringBroadcastLeftOuter
    case _: PhysicalOp.StringBroadcastRightOuter => KernelChoice.StringBroadcastRightOuter
    case _: PhysicalOp.StringBroadcastFullOuter => KernelChoice.StringBroadcastFullOuter
    case _: PhysicalOp.StringSortMergeLeftOuter => KernelChoice.StringSortMergeLeftOuter
    case _: PhysicalOp.StringSortMergeRightOuter => KernelChoice.StringSortMergeRightOuter
    case _: PhysicalOp.StringSortMergeFullOuter => KernelChoice.StringSortMergeFullOuter
    case _: PhysicalOp.Scan => KernelChoice.Scan
    case _: PhysicalOp.Filter => KernelChoice.Filter
    case _: PhysicalOp.Project => KernelChoice.Project
    case _: PhysicalOp.Column => KernelChoice.Column
    case _: PhysicalOp.MapValue => KernelChoice.MapValue
    case _: PhysicalOp.IntLongMapValue => KernelChoice.IntLongMapValue
    case _: PhysicalOp.MapKey => KernelChoice.MapKey
    case _: PhysicalOp.Pipeline => KernelChoice.Pipeline
    case _: PhysicalOp.Union => KernelChoice.Union
    case _: PhysicalOp.Exchange => KernelChoice.Exchange
    case _: PhysicalOp.IntExtremum => KernelChoice.IntExtremum
    case _: PhysicalOp.WideFilter => KernelChoice.Filter
    case _: PhysicalOp.WideProject => KernelChoice.Project
