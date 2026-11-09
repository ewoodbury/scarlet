package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan

/**
 * Lowers inner and outer joins for the pair kinds the columnar kernels accept.
 *
 * An int inner join accepts no hint, shuffle-hash, broadcast, and sort-merge. A long inner join
 * and a string inner join accept no hint and shuffle-hash. Outer joins on int pairs, on
 * `(Long, Long)` pairs, and on `(String, Int)` pairs accept broadcast, shuffle-hash, an absent
 * hint, and sort-merge. Any other join stays on the row path.
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
      sortMergeJoinOf(left, right, PhysicalOp.SortMergeJoin.apply, lowerChild)
    case Plan.JoinOp(left, right, strategy) if acceptsHashJoin(strategy) =>
      (lowerChild(left), lowerChild(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(joinOf(strategy, leftOp, rightOp))
            case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs)
                if acceptsShuffleHash(strategy) =>
              Some(
                PhysicalOp.LongHashJoin(
                  PhysicalOp.Exchange(leftOp),
                  PhysicalOp.Exchange(rightOp),
                ),
              )
            case (PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.StringPairs)
                if acceptsShuffleHash(strategy) =>
              Some(
                PhysicalOp.StringHashJoin(
                  PhysicalOp.Exchange(leftOp),
                  PhysicalOp.Exchange(rightOp),
                ),
              )
            case _ => None
        case _ => None

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
            case _ => None
        case _ => None
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
            case _ => None
        case _ => None
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
            case _ => None
        case _ => None
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
            case _ => None
        case _ => None
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
            case _ => None
        case _ => None
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
            case _ => None
        case _ => None
    case Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) =>
      outerSortMerge(
        left,
        right,
        PhysicalOp.SortMergeLeftOuter.apply,
        PhysicalOp.LongSortMergeLeftOuter.apply,
        lowerChild,
      ).orElse(
        stringOuterSortMerge(
          left,
          right,
          PhysicalOp.StringSortMergeLeftOuter.apply,
          lowerChild,
        ),
      )
    case Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) =>
      outerSortMerge(
        left,
        right,
        PhysicalOp.SortMergeRightOuter.apply,
        PhysicalOp.LongSortMergeRightOuter.apply,
        lowerChild,
      ).orElse(
        stringOuterSortMerge(
          left,
          right,
          PhysicalOp.StringSortMergeRightOuter.apply,
          lowerChild,
        ),
      )
    case Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) =>
      outerSortMerge(
        left,
        right,
        PhysicalOp.SortMergeFullOuter.apply,
        PhysicalOp.LongSortMergeFullOuter.apply,
        lowerChild,
      ).orElse(
        stringOuterSortMerge(
          left,
          right,
          PhysicalOp.StringSortMergeFullOuter.apply,
          lowerChild,
        ),
      )
    // A strategy the cases above do not accept stays on the row path.
    case _: Plan.LeftOuterJoinOp[_, _, _] | _: Plan.RightOuterJoinOp[_, _, _] |
        _: Plan.FullOuterJoinOp[_, _, _] =>
      None
    case _ => None

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
   * on [[sortMergeJoinOf]] and accepts int pairs only.
   */
  private def outerSortMerge(
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
          case _ => None
      case _ => None

  /**
   * Sort-merge of two string-pair sides. Both children are exchanged. Any other kind stays on the
   * row path. Int and long outer joins use [[outerSortMerge]]. The inner sort-merge stays on
   * [[sortMergeJoinOf]] and accepts int pairs only.
   */
  private def stringOuterSortMerge(
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
          case _ => None
      case _ => None

  /**
   * Sort-merge of two int-pair sides. Both children are exchanged. A non-pair side stays on the
   * row path. The inner join uses this gate. Outer joins use [[outerSortMerge]].
   */
  private def sortMergeJoinOf(
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
          case _ => None
      case _ => None
