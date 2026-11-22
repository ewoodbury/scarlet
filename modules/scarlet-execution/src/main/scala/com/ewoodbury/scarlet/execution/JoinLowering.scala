package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan

/**
 * Lowers inner and outer joins for the pair kinds the columnar kernels accept.
 *
 * An int inner join accepts no hint, shuffle-hash, broadcast, and sort-merge. A long inner join
 * accepts no hint, shuffle-hash, broadcast, and sort-merge. A string inner join accepts no hint,
 * shuffle-hash, broadcast, and sort-merge. Outer joins on int pairs, on `(Long, Long)` pairs, and
 * on `(String, Int)` pairs accept broadcast, shuffle-hash, an absent hint, and sort-merge. Any
 * other join stays on the row path.
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
    case Plan.JoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) =>
      sortMergeJoinOf(plan, left, right, PhysicalOp.SortMergeJoin.apply, lowerChild)
    case Plan.JoinOp(left, right, strategy) if acceptsHashJoin(strategy) =>
      (lowerChild(left), lowerChild(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(joinOf(strategy, leftOp, rightOp))
            case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs) =>
              strategy match
                case Some(Plan.JoinStrategy.Broadcast) =>
                  Some(
                    PhysicalOp.LongBroadcastJoin(
                      leftOp,
                      rightOp,
                      PhysicalOp.BuildSide.Right,
                    ),
                  )
                case None | Some(Plan.JoinStrategy.ShuffleHash) =>
                  Some(
                    PhysicalOp.LongHashJoin(
                      PhysicalOp.Exchange(leftOp),
                      PhysicalOp.Exchange(rightOp),
                    ),
                  )
                case Some(Plan.JoinStrategy.SortMerge) =>
                  throw new IllegalStateException("sort-merge is lowered by sortMergeJoinOf")
            case (PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.StringPairs) =>
              strategy match
                case Some(Plan.JoinStrategy.Broadcast) =>
                  Some(
                    PhysicalOp.StringBroadcastJoin(
                      leftOp,
                      rightOp,
                      PhysicalOp.BuildSide.Right,
                    ),
                  )
                case None | Some(Plan.JoinStrategy.ShuffleHash) =>
                  Some(
                    PhysicalOp.StringHashJoin(
                      PhysicalOp.Exchange(leftOp),
                      PhysicalOp.Exchange(rightOp),
                    ),
                  )
                case Some(Plan.JoinStrategy.SortMerge) =>
                  throw new IllegalStateException("sort-merge is lowered by sortMergeJoinOf")
            case (leftKind, rightKind) => refuseKinds(plan, leftKind, rightKind)
        case _ => refuseChild(plan)

    case Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)) =>
      (lowerChild(left), lowerChild(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(PhysicalOp.BroadcastLeftOuter(left = leftOp, right = rightOp))
            case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs) =>
              Some(PhysicalOp.LongBroadcastLeftOuter(left = leftOp, right = rightOp))
            case (PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.StringPairs) =>
              Some(PhysicalOp.StringBroadcastLeftOuter(left = leftOp, right = rightOp))
            case (leftKind, rightKind) => refuseKinds(plan, leftKind, rightKind)
        case _ => refuseChild(plan)
    // An absent hint is shuffle-hash, the same rule as an inner join. The row executor
    // shuffle-hashes an unhinted outer join, so that plan is this op.
    case Plan.LeftOuterJoinOp(left, right, strategy) if acceptsShuffleHash(strategy) =>
      (lowerChild(left), lowerChild(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(
                PhysicalOp.HashLeftOuter(
                  PhysicalOp.Exchange(leftOp),
                  PhysicalOp.Exchange(rightOp),
                ),
              )
            case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs) =>
              Some(
                PhysicalOp.LongHashLeftOuter(
                  PhysicalOp.Exchange(leftOp),
                  PhysicalOp.Exchange(rightOp),
                ),
              )
            case (PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.StringPairs) =>
              Some(
                PhysicalOp.StringHashLeftOuter(
                  PhysicalOp.Exchange(leftOp),
                  PhysicalOp.Exchange(rightOp),
                ),
              )
            case (leftKind, rightKind) => refuseKinds(plan, leftKind, rightKind)
        case _ => refuseChild(plan)
    case Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)) =>
      (lowerChild(left), lowerChild(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(PhysicalOp.BroadcastRightOuter(left = leftOp, right = rightOp))
            case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs) =>
              Some(PhysicalOp.LongBroadcastRightOuter(left = leftOp, right = rightOp))
            case (PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.StringPairs) =>
              Some(PhysicalOp.StringBroadcastRightOuter(left = leftOp, right = rightOp))
            case (leftKind, rightKind) => refuseKinds(plan, leftKind, rightKind)
        case _ => refuseChild(plan)
    // An absent hint is shuffle-hash, the same rule as an inner join and a left outer join.
    // The row executor shuffle-hashes an unhinted outer join, so that plan is this op.
    case Plan.RightOuterJoinOp(left, right, strategy) if acceptsShuffleHash(strategy) =>
      (lowerChild(left), lowerChild(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(
                PhysicalOp.HashRightOuter(
                  PhysicalOp.Exchange(leftOp),
                  PhysicalOp.Exchange(rightOp),
                ),
              )
            case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs) =>
              Some(
                PhysicalOp.LongHashRightOuter(
                  PhysicalOp.Exchange(leftOp),
                  PhysicalOp.Exchange(rightOp),
                ),
              )
            case (PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.StringPairs) =>
              Some(
                PhysicalOp.StringHashRightOuter(
                  PhysicalOp.Exchange(leftOp),
                  PhysicalOp.Exchange(rightOp),
                ),
              )
            case (leftKind, rightKind) => refuseKinds(plan, leftKind, rightKind)
        case _ => refuseChild(plan)
    case Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)) =>
      (lowerChild(left), lowerChild(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(PhysicalOp.BroadcastFullOuter(left = leftOp, right = rightOp))
            case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs) =>
              Some(PhysicalOp.LongBroadcastFullOuter(left = leftOp, right = rightOp))
            case (PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.StringPairs) =>
              Some(PhysicalOp.StringBroadcastFullOuter(left = leftOp, right = rightOp))
            case (leftKind, rightKind) => refuseKinds(plan, leftKind, rightKind)
        case _ => refuseChild(plan)
    // An absent hint is shuffle-hash, the same rule as an inner join and the other outer joins.
    // The row executor shuffle-hashes an unhinted outer join, so that plan is this op.
    case Plan.FullOuterJoinOp(left, right, strategy) if acceptsShuffleHash(strategy) =>
      (lowerChild(left), lowerChild(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(
                PhysicalOp.HashFullOuter(
                  PhysicalOp.Exchange(leftOp),
                  PhysicalOp.Exchange(rightOp),
                ),
              )
            case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs) =>
              Some(
                PhysicalOp.LongHashFullOuter(
                  PhysicalOp.Exchange(leftOp),
                  PhysicalOp.Exchange(rightOp),
                ),
              )
            case (PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.StringPairs) =>
              Some(
                PhysicalOp.StringHashFullOuter(
                  PhysicalOp.Exchange(leftOp),
                  PhysicalOp.Exchange(rightOp),
                ),
              )
            case (leftKind, rightKind) => refuseKinds(plan, leftKind, rightKind)
        case _ => refuseChild(plan)
    case Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) =>
      outerSortMerge(
        plan,
        left,
        right,
        PhysicalOp.SortMergeLeftOuter.apply,
        PhysicalOp.LongSortMergeLeftOuter.apply,
        lowerChild,
      ).orElse(
        stringOuterSortMerge(
          plan,
          left,
          right,
          PhysicalOp.StringSortMergeLeftOuter.apply,
          lowerChild,
        ),
      )
    case Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) =>
      outerSortMerge(
        plan,
        left,
        right,
        PhysicalOp.SortMergeRightOuter.apply,
        PhysicalOp.LongSortMergeRightOuter.apply,
        lowerChild,
      ).orElse(
        stringOuterSortMerge(
          plan,
          left,
          right,
          PhysicalOp.StringSortMergeRightOuter.apply,
          lowerChild,
        ),
      )
    case Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) =>
      outerSortMerge(
        plan,
        left,
        right,
        PhysicalOp.SortMergeFullOuter.apply,
        PhysicalOp.LongSortMergeFullOuter.apply,
        lowerChild,
      ).orElse(
        stringOuterSortMerge(
          plan,
          left,
          right,
          PhysicalOp.StringSortMergeFullOuter.apply,
          lowerChild,
        ),
      )
    // A strategy the cases above do not accept stays on the row path.
    case _: Plan.LeftOuterJoinOp[_, _, _] | _: Plan.RightOuterJoinOp[_, _, _] |
        _: Plan.FullOuterJoinOp[_, _, _] =>
      refusePlan(plan)
    case _ => refusePlan(plan)

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

  /**
   * Columnar hash joins. No hint, shuffle-hash, and broadcast all qualify. Sort-merge is
   * [[PhysicalOp.SortMergeJoin]], not this predicate. Broadcast records the right input as the
   * build side; see [[joinOf]].
   */
  private[execution] def acceptsHashJoin(strategy: Option[Plan.JoinStrategy]): Boolean =
    strategy match
      case None | Some(Plan.JoinStrategy.ShuffleHash) | Some(Plan.JoinStrategy.Broadcast) => true
      case Some(Plan.JoinStrategy.SortMerge) => false

  /** Shuffle-hash, including a join with no hint. Broadcast is a different physical node. */
  private[execution] def acceptsShuffleHash(strategy: Option[Plan.JoinStrategy]): Boolean =
    strategy match
      case None | Some(Plan.JoinStrategy.ShuffleHash) => true
      case Some(Plan.JoinStrategy.Broadcast) | Some(Plan.JoinStrategy.SortMerge) => false

  /**
   * Sort-merge or broadcast on a [[PhysicalOp.HashJoin]] of two exchanges.
   *
   * Not [[acceptsShuffleHash]], and not its complement: no hint and shuffle-hash stay on that
   * predicate. Collect of these hints still lowers to [[PhysicalOp.SortMergeJoin]] and
   * [[PhysicalOp.BroadcastJoin]]. [[ReduceByKeyLowering]] and [[CountByKeyLowering]] build the
   * hash-join node under a triple project.
   */
  private[execution] def hintedInnerHashed(strategy: Option[Plan.JoinStrategy]): Boolean =
    strategy match
      case Some(Plan.JoinStrategy.SortMerge) | Some(Plan.JoinStrategy.Broadcast) => true
      case None | Some(Plan.JoinStrategy.ShuffleHash) => false

  /**
   * Shuffle-hash exchanges both children. Broadcast keeps both children and records
   * [[PhysicalOp.BuildSide.Right]]. A later cost-based pass may choose the left side.
   */
  private def joinOf(
      strategy: Option[Plan.JoinStrategy],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
  ): PhysicalOp = strategy match
    case Some(Plan.JoinStrategy.Broadcast) =>
      PhysicalOp.BroadcastJoin(
        left = leftOp,
        right = rightOp,
        buildSide = PhysicalOp.BuildSide.Right,
      )
    case None | Some(Plan.JoinStrategy.ShuffleHash) =>
      PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp))
    // Exhaustive only. Sort-merge is lowered by sortMergeJoinOf and never reaches this helper.
    case Some(Plan.JoinStrategy.SortMerge) =>
      throw new IllegalStateException("sort-merge is lowered by sortMergeJoinOf")

  /**
   * Sort-merge of two outer-join sides. Int pairs use the int node. Long pairs use the long node.
   * Both children are exchanged. Any other kind stays on the row path. The inner sort-merge stays
   * on [[sortMergeJoinOf]] and accepts int pairs, long pairs, and string pairs.
   */
  private def outerSortMerge(
      plan: Plan[_],
      left: Plan[_],
      right: Plan[_],
      intNode: (PhysicalOp, PhysicalOp) => PhysicalOp,
      longNode: (PhysicalOp, PhysicalOp) => PhysicalOp,
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    (lowerChild(left), lowerChild(right)) match
      case (Some(leftOp), Some(rightOp)) =>
        (leftOp.kind, rightOp.kind) match
          case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
            Some(intNode(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)))
          case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs) =>
            Some(longNode(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)))
          case (leftKind, rightKind) => refuseKinds(plan, leftKind, rightKind)
      case _ => refuseChild(plan)

  /**
   * Sort-merge of two string-pair sides. Both children are exchanged. Any other kind stays on the
   * row path. Int and long outer joins use [[outerSortMerge]]. The inner sort-merge stays on
   * [[sortMergeJoinOf]] and accepts int pairs, long pairs, and string pairs.
   */
  private def stringOuterSortMerge(
      plan: Plan[_],
      left: Plan[_],
      right: Plan[_],
      node: (PhysicalOp, PhysicalOp) => PhysicalOp,
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    (lowerChild(left), lowerChild(right)) match
      case (Some(leftOp), Some(rightOp)) =>
        (leftOp.kind, rightOp.kind) match
          case (PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.StringPairs) =>
            Some(node(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)))
          case (leftKind, rightKind) => refuseKinds(plan, leftKind, rightKind)
      case _ => refuseChild(plan)

  /**
   * Sort-merge of two inner-join sides. Int pairs use [[PhysicalOp.SortMergeJoin]]. Long pairs use
   * [[PhysicalOp.LongSortMergeJoin]]. String pairs use [[PhysicalOp.StringSortMergeJoin]]. Both
   * children are exchanged. Any other kind stays on the row path. Outer joins use
   * [[outerSortMerge]].
   */
  private def sortMergeJoinOf(
      plan: Plan[_],
      left: Plan[_],
      right: Plan[_],
      node: (PhysicalOp, PhysicalOp) => PhysicalOp,
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    (lowerChild(left), lowerChild(right)) match
      case (Some(leftOp), Some(rightOp)) =>
        (leftOp.kind, rightOp.kind) match
          case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
            Some(node(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)))
          case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs) =>
            Some(
              PhysicalOp.LongSortMergeJoin(
                PhysicalOp.Exchange(leftOp),
                PhysicalOp.Exchange(rightOp),
              ),
            )
          case (PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.StringPairs) =>
            Some(
              PhysicalOp.StringSortMergeJoin(
                PhysicalOp.Exchange(leftOp),
                PhysicalOp.Exchange(rightOp),
              ),
            )
          case (leftKind, rightKind) => refuseKinds(plan, leftKind, rightKind)
      case _ => refuseChild(plan)

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
