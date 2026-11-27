package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.{
  ColumnLayout,
  ColumnType,
  Layout as Columns,
  NullPolicy,
  Output,
}

/**
 * Lowers inner and outer joins.
 *
 * The class is the key [[ColumnType]] on the two children, plus the strategy hint those joins
 * already honor. Int, long, and string classes stay. [[PhysicalOp.Kind]] does not pick among them.
 * A pair kind whose key is [[ColumnType.Int64]] is the long class. An outer result and a source
 * [[ScanLowering]] refuses stay on the row path. Hash, broadcast, and sort-merge of an int-long
 * pair lower. An int-pair join stays in the int class.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Throw",
  ),
)
private[execution] object JoinLowering:

  private[execution] def isJoin(plan: Plan[_]): Boolean = plan match
    case _: Plan.JoinOp[_, _, _] | _: Plan.LeftOuterJoinOp[_, _, _] |
        _: Plan.RightOuterJoinOp[_, _, _] | _: Plan.FullOuterJoinOp[_, _, _] =>
      true
    case _ => false

  private[execution] def lower(
      plan: Plan[_],
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] = plan match
    case Plan.JoinOp(left, right, strategy) =>
      assemble(plan, left, right, strategy, JoinClass.Place.Inner, lowerChild)
    case Plan.LeftOuterJoinOp(left, right, strategy) =>
      assemble(plan, left, right, strategy, JoinClass.Place.Left, lowerChild)
    case Plan.RightOuterJoinOp(left, right, strategy) =>
      assemble(plan, left, right, strategy, JoinClass.Place.Right, lowerChild)
    case Plan.FullOuterJoinOp(left, right, strategy) =>
      assemble(plan, left, right, strategy, JoinClass.Place.Full, lowerChild)
    case _ => refusePlan(plan)

  /**
   * Both children lowered, and [[JoinClass.choose]] named a class. A missing child keeps that
   * child's note. Two lowered children with no class are a kind mismatch.
   */
  private def assemble(
      plan: Plan[_],
      left: Plan[_],
      right: Plan[_],
      strategy: Option[Plan.JoinStrategy],
      place: JoinClass.Place,
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    (lowerChild(left), lowerChild(right)) match
      case (Some(leftOp), Some(rightOp)) =>
        built(leftOp, rightOp, decided(strategy, place, rightOp), place) match
          case Some(op) => Some(op)
          case None => refuseKinds(plan, leftOp.kind, rightOp.kind)
      case _ => refuseChild(plan)

  /**
   * An unhinted inner join broadcasts when [[JoinCost]] knows the right side is small enough. An
   * outer join, a named hint, and an unknown count keep the strategy the caller passed.
   */
  private def decided(
      strategy: Option[Plan.JoinStrategy],
      place: JoinClass.Place,
      build: PhysicalOp,
  ): Option[Plan.JoinStrategy] = strategy match
    case None if place == JoinClass.Place.Inner && JoinCost.preferBroadcast(build) =>
      Some(Plan.JoinStrategy.Broadcast)
    case other => other

  /**
   * Class for two children that share a pair kind. The key type is read from each child's columns.
   * An [[NullPolicy.Absence]] column is an outer result and is not a key.
   */
  private def built(
      left: PhysicalOp,
      right: PhysicalOp,
      strategy: Option[Plan.JoinStrategy],
      place: JoinClass.Place,
  ): Option[PhysicalOp] =
    if (intLongPairs(left) && intLongPairs(right)) then
      JoinClass
        .intLongInner(strategy, place)
        .flatMap(choice => JoinClass.build(choice, left, right))
    else
      (keyOf(left), keyOf(right)) match
        case (Some(leftKey), Some(rightKey)) if samePair(left.kind, right.kind) =>
          JoinClass
            .choose(leftKey, rightKey, left.kind, strategy, place)
            .flatMap(choice => JoinClass.build(choice, left, right))
        case _ => None

  /** The widening, including a filter or a `Long => Long` map of it. Not a triple project. */
  private def intLongPairs(op: PhysicalOp): Boolean = Columns.known(op) match
    case Output.IntLongPairs(_) => true
    case Output.Columns(_) | Output.Opaque(_) => false

  /** First column, when every column is present or nullable. Absence is an outer partner. */
  private def keyOf(op: PhysicalOp): Option[ColumnType] = Columns.known(op) match
    case Output.Columns(key +: rest) if !absent(key) && !rest.exists(absent) =>
      Some(key.columnType)
    case Output.Columns(_) | Output.IntLongPairs(_) | Output.Opaque(_) => None

  private def absent(column: ColumnLayout): Boolean = column.nullPolicy match
    case NullPolicy.Absence => true
    case NullPolicy.Present | NullPolicy.Nullable => false

  /** Both children are the same pair kind the joins already run. */
  private def samePair(left: PhysicalOp.Kind, right: PhysicalOp.Kind): Boolean =
    (left, right) match
      case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) => true
      case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs) => true
      case (PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.StringPairs) => true
      case _ => false

  /** Kind mismatch. Both children lowered. */
  private def refuseKinds(
      plan: Plan[_],
      left: PhysicalOp.Kind,
      right: PhysicalOp.Kind,
  ): Option[PhysicalOp] =
    LoweringTrace.note(plan, left, right, "JoinLowering")
    None

  /** A child did not lower. Keeps that child's note when it recorded one. */
  private def refuseChild(plan: Plan[_]): Option[PhysicalOp] =
    LoweringTrace.noteIfEmpty(plan, "JoinLowering")
    None

  /** This join shape has no columnar kernel. */
  private def refusePlan(plan: Plan[_]): Option[PhysicalOp] =
    LoweringTrace.note(plan, "JoinLowering")
    None

  /** Shuffle-hash, including a join with no hint. Broadcast is a different physical node. */
  private[execution] def acceptsShuffleHash(strategy: Option[Plan.JoinStrategy]): Boolean =
    strategy match
      case None | Some(Plan.JoinStrategy.ShuffleHash) => true
      case Some(Plan.JoinStrategy.Broadcast) | Some(Plan.JoinStrategy.SortMerge) => false

  /** Explicit broadcast hint. No hint stays on [[acceptsShuffleHash]]. */
  private[execution] def acceptsBroadcast(strategy: Option[Plan.JoinStrategy]): Boolean =
    strategy.contains(Plan.JoinStrategy.Broadcast)

  /** Explicit sort-merge hint. No hint stays on [[acceptsShuffleHash]]. */
  private[execution] def acceptsSortMerge(strategy: Option[Plan.JoinStrategy]): Boolean =
    strategy.contains(Plan.JoinStrategy.SortMerge)

  /**
   * Sort-merge or broadcast on a [[PhysicalOp.HashJoin]] of two exchanges.
   *
   * Not [[acceptsShuffleHash]], and not its complement: no hint and shuffle-hash stay on that
   * predicate. Collect of these hints still lowers to [[PhysicalOp.SortMergeJoin]] and
   * [[PhysicalOp.BroadcastJoin]]. [[ReduceByKeyLowering]] and [[CountByKeyLowering]] build the
   * hash-join node under a triple project. [[JoinAggregateBinding]] builds that same hash join for
   * an unmapped count, with no project.
   */
  private[execution] def hintedInnerHashed(strategy: Option[Plan.JoinStrategy]): Boolean =
    strategy match
      case Some(Plan.JoinStrategy.SortMerge) | Some(Plan.JoinStrategy.Broadcast) => true
      case None | Some(Plan.JoinStrategy.ShuffleHash) => false

  /**
   * Layout of a join. Aggregates, scans, and exchanges stay in [[ColumnarPlanner.infoOf]]. A
   * broadcast join keeps the probe side's distribution and replaces the layout. Moved here so
   * [[ColumnarPlanner]] stays inside its line cap.
   */
  private[execution] def info(
      op: PhysicalOp,
      child: PhysicalOp => PartitioningInfo,
      layout: PhysicalOp.Kind => Layout,
      hashed: PhysicalOp.Kind => PartitioningInfo,
  ): PartitioningInfo = op match
    case _: PhysicalOp.HashJoin | _: PhysicalOp.SortMergeJoin | _: PhysicalOp.SortMergeLeftOuter |
        _: PhysicalOp.SortMergeRightOuter | _: PhysicalOp.SortMergeFullOuter |
        _: PhysicalOp.HashLeftOuter | _: PhysicalOp.HashRightOuter | _: PhysicalOp.HashFullOuter =>
      hashed(PhysicalOp.Kind.Pairs)
    case _: PhysicalOp.LongHashJoin | _: PhysicalOp.LongSortMergeJoin =>
      hashed(PhysicalOp.Kind.LongPairs)
    case _: PhysicalOp.StringHashJoin | _: PhysicalOp.StringSortMergeJoin =>
      hashed(PhysicalOp.Kind.StringTriples)
    case _: PhysicalOp.IntLongHashJoin | _: PhysicalOp.IntLongSortMergeJoin =>
      hashed(PhysicalOp.Kind.IntLongTriples)
    case _: PhysicalOp.IntLongHashLeftOuter | _: PhysicalOp.IntLongHashRightOuter |
        _: PhysicalOp.IntLongHashFullOuter | _: PhysicalOp.IntLongSortMergeLeftOuter |
        _: PhysicalOp.IntLongSortMergeRightOuter | _: PhysicalOp.IntLongSortMergeFullOuter =>
      hashed(PhysicalOp.Kind.IntLongOuter)
    case PhysicalOp.IntLongBroadcastJoin(left, right, buildSide) =>
      broadcastProbe(left, right, buildSide, PhysicalOp.Kind.IntLongTriples, child, layout)
    case PhysicalOp.IntLongBroadcastLeftOuter(left, _) =>
      child(left).copy(layout = layout(PhysicalOp.Kind.IntLongOuter))
    case PhysicalOp.IntLongBroadcastRightOuter(_, right) =>
      child(right).copy(layout = layout(PhysicalOp.Kind.IntLongOuter))
    case PhysicalOp.IntLongBroadcastFullOuter(left, _) =>
      child(left).copy(layout = layout(PhysicalOp.Kind.IntLongOuter))
    case _: PhysicalOp.LongHashLeftOuter | _: PhysicalOp.LongHashRightOuter |
        _: PhysicalOp.LongHashFullOuter | _: PhysicalOp.LongSortMergeLeftOuter |
        _: PhysicalOp.LongSortMergeRightOuter | _: PhysicalOp.LongSortMergeFullOuter =>
      hashed(PhysicalOp.Kind.LongOuter)
    case PhysicalOp.LongBroadcastLeftOuter(left, _) =>
      child(left).copy(layout = layout(PhysicalOp.Kind.LongOuter))
    case PhysicalOp.LongBroadcastRightOuter(_, right) =>
      child(right).copy(layout = layout(PhysicalOp.Kind.LongOuter))
    case PhysicalOp.LongBroadcastFullOuter(left, _) =>
      child(left).copy(layout = layout(PhysicalOp.Kind.LongOuter))
    case _: PhysicalOp.StringHashLeftOuter | _: PhysicalOp.StringHashRightOuter |
        _: PhysicalOp.StringHashFullOuter | _: PhysicalOp.StringSortMergeLeftOuter |
        _: PhysicalOp.StringSortMergeRightOuter | _: PhysicalOp.StringSortMergeFullOuter =>
      hashed(PhysicalOp.Kind.StringOuter)
    case PhysicalOp.StringBroadcastLeftOuter(left, _) =>
      child(left).copy(layout = layout(PhysicalOp.Kind.StringOuter))
    case PhysicalOp.StringBroadcastRightOuter(_, right) =>
      child(right).copy(layout = layout(PhysicalOp.Kind.StringOuter))
    case PhysicalOp.StringBroadcastFullOuter(left, _) =>
      child(left).copy(layout = layout(PhysicalOp.Kind.StringOuter))
    case PhysicalOp.BroadcastJoin(left, right, buildSide) =>
      broadcastProbe(left, right, buildSide, PhysicalOp.Kind.Triples, child, layout)
    case PhysicalOp.LongBroadcastJoin(left, right, buildSide) =>
      broadcastProbe(left, right, buildSide, PhysicalOp.Kind.LongTriples, child, layout)
    case PhysicalOp.StringBroadcastJoin(left, right, buildSide) =>
      broadcastProbe(left, right, buildSide, PhysicalOp.Kind.StringTriples, child, layout)
    case PhysicalOp.BroadcastLeftOuter(left, _) =>
      child(left).copy(layout = layout(PhysicalOp.Kind.Triples))
    case PhysicalOp.BroadcastRightOuter(_, right) =>
      child(right).copy(layout = layout(PhysicalOp.Kind.Triples))
    case PhysicalOp.BroadcastFullOuter(left, _) =>
      child(left).copy(layout = layout(PhysicalOp.Kind.Triples))
    case other =>
      throw new IllegalStateException(s"${other.shape} is not a join layout")

  private def broadcastProbe(
      left: PhysicalOp,
      right: PhysicalOp,
      buildSide: PhysicalOp.BuildSide,
      kind: PhysicalOp.Kind,
      child: PhysicalOp => PartitioningInfo,
      layout: PhysicalOp.Kind => Layout,
  ): PartitioningInfo =
    val probe = buildSide match
      case PhysicalOp.BuildSide.Right => left
      case PhysicalOp.BuildSide.Left => right
    child(probe).copy(layout = layout(kind))
