package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.{ColumnType, KernelChoice}

/**
 * Which existing join class two children use.
 *
 * The family is the key [[ColumnType]] on both children. [[PhysicalOp.Kind]] does not pick it.
 * [[ColumnType.Int64]] with [[PhysicalOp.Kind.Pairs]] is the long class. The strategy hint selects
 * hash, broadcast, or sort-merge inside that family. Inner, left, right, and full each keep the
 * class they already have. A non-pair kind, including an outer result, disagreeing keys, and a key
 * with no join class are [[scala.None]]. No join class is deleted.
 * [[com.ewoodbury.scarlet.execution.schema.Describe.tag]] is not read.
 */
private[execution] object JoinClass:

  /** Inner join, or the outer side the kernels already build. */
  private[execution] enum Place:
    case Inner, Left, Right, Full

  private enum Family:
    case IntKey, LongKey, StringKey

  private enum Hint:
    case Hash, Broadcast, SortMerge

  /**
   * Join class for `leftKey` and `rightKey`.
   *
   * `kind` admits a pair the kernels already run. It does not select int, long, or string.
   * `strategy` is the hint those joins already honor. `place` is inner or an outer side.
   */
  private[execution] def choose(
      leftKey: ColumnType,
      rightKey: ColumnType,
      kind: PhysicalOp.Kind,
      strategy: Option[Plan.JoinStrategy],
      place: Place,
  ): Option[KernelChoice] =
    if !pairKind(kind) || !agreed(leftKey, rightKey) then None
    else family(leftKey).map(keyFamily => node(keyFamily, hint(strategy), place))

  /** Physical node for a class [[choose]] returned. A non-join tag is [[scala.None]]. */
  private[execution] def build(
      choice: KernelChoice,
      left: PhysicalOp,
      right: PhysicalOp,
  ): Option[PhysicalOp] =
    val leftExchange = PhysicalOp.Exchange(left)
    val rightExchange = PhysicalOp.Exchange(right)
    choice match
      case KernelChoice.HashJoin =>
        Some(PhysicalOp.HashJoin(leftExchange, rightExchange))
      case KernelChoice.BroadcastJoin =>
        Some(PhysicalOp.BroadcastJoin(left, right, PhysicalOp.BuildSide.Right))
      case KernelChoice.SortMergeJoin =>
        Some(PhysicalOp.SortMergeJoin(leftExchange, rightExchange))
      case KernelChoice.HashLeftOuter =>
        Some(PhysicalOp.HashLeftOuter(leftExchange, rightExchange))
      case KernelChoice.HashRightOuter =>
        Some(PhysicalOp.HashRightOuter(leftExchange, rightExchange))
      case KernelChoice.HashFullOuter =>
        Some(PhysicalOp.HashFullOuter(leftExchange, rightExchange))
      case KernelChoice.BroadcastLeftOuter =>
        Some(PhysicalOp.BroadcastLeftOuter(left, right))
      case KernelChoice.BroadcastRightOuter =>
        Some(PhysicalOp.BroadcastRightOuter(left, right))
      case KernelChoice.BroadcastFullOuter =>
        Some(PhysicalOp.BroadcastFullOuter(left, right))
      case KernelChoice.SortMergeLeftOuter =>
        Some(PhysicalOp.SortMergeLeftOuter(leftExchange, rightExchange))
      case KernelChoice.SortMergeRightOuter =>
        Some(PhysicalOp.SortMergeRightOuter(leftExchange, rightExchange))
      case KernelChoice.SortMergeFullOuter =>
        Some(PhysicalOp.SortMergeFullOuter(leftExchange, rightExchange))
      case KernelChoice.LongHashJoin =>
        Some(PhysicalOp.LongHashJoin(leftExchange, rightExchange))
      case KernelChoice.LongBroadcastJoin =>
        Some(PhysicalOp.LongBroadcastJoin(left, right, PhysicalOp.BuildSide.Right))
      case KernelChoice.LongSortMergeJoin =>
        Some(PhysicalOp.LongSortMergeJoin(leftExchange, rightExchange))
      case KernelChoice.LongHashLeftOuter =>
        Some(PhysicalOp.LongHashLeftOuter(leftExchange, rightExchange))
      case KernelChoice.LongHashRightOuter =>
        Some(PhysicalOp.LongHashRightOuter(leftExchange, rightExchange))
      case KernelChoice.LongHashFullOuter =>
        Some(PhysicalOp.LongHashFullOuter(leftExchange, rightExchange))
      case KernelChoice.LongBroadcastLeftOuter =>
        Some(PhysicalOp.LongBroadcastLeftOuter(left, right))
      case KernelChoice.LongBroadcastRightOuter =>
        Some(PhysicalOp.LongBroadcastRightOuter(left, right))
      case KernelChoice.LongBroadcastFullOuter =>
        Some(PhysicalOp.LongBroadcastFullOuter(left, right))
      case KernelChoice.LongSortMergeLeftOuter =>
        Some(PhysicalOp.LongSortMergeLeftOuter(leftExchange, rightExchange))
      case KernelChoice.LongSortMergeRightOuter =>
        Some(PhysicalOp.LongSortMergeRightOuter(leftExchange, rightExchange))
      case KernelChoice.LongSortMergeFullOuter =>
        Some(PhysicalOp.LongSortMergeFullOuter(leftExchange, rightExchange))
      case KernelChoice.StringHashJoin =>
        Some(PhysicalOp.StringHashJoin(leftExchange, rightExchange))
      case KernelChoice.StringBroadcastJoin =>
        Some(PhysicalOp.StringBroadcastJoin(left, right, PhysicalOp.BuildSide.Right))
      case KernelChoice.StringSortMergeJoin =>
        Some(PhysicalOp.StringSortMergeJoin(leftExchange, rightExchange))
      case KernelChoice.StringHashLeftOuter =>
        Some(PhysicalOp.StringHashLeftOuter(leftExchange, rightExchange))
      case KernelChoice.StringHashRightOuter =>
        Some(PhysicalOp.StringHashRightOuter(leftExchange, rightExchange))
      case KernelChoice.StringHashFullOuter =>
        Some(PhysicalOp.StringHashFullOuter(leftExchange, rightExchange))
      case KernelChoice.StringBroadcastLeftOuter =>
        Some(PhysicalOp.StringBroadcastLeftOuter(left, right))
      case KernelChoice.StringBroadcastRightOuter =>
        Some(PhysicalOp.StringBroadcastRightOuter(left, right))
      case KernelChoice.StringBroadcastFullOuter =>
        Some(PhysicalOp.StringBroadcastFullOuter(left, right))
      case KernelChoice.StringSortMergeLeftOuter =>
        Some(PhysicalOp.StringSortMergeLeftOuter(leftExchange, rightExchange))
      case KernelChoice.StringSortMergeRightOuter =>
        Some(PhysicalOp.StringSortMergeRightOuter(leftExchange, rightExchange))
      case KernelChoice.StringSortMergeFullOuter =>
        Some(PhysicalOp.StringSortMergeFullOuter(leftExchange, rightExchange))
      case KernelChoice.IntLongHashJoin =>
        Some(PhysicalOp.IntLongHashJoin(leftExchange, rightExchange))
      case KernelChoice.IntLongHashLeftOuter =>
        Some(PhysicalOp.IntLongHashLeftOuter(leftExchange, rightExchange))
      case KernelChoice.IntLongHashRightOuter =>
        Some(PhysicalOp.IntLongHashRightOuter(leftExchange, rightExchange))
      case KernelChoice.IntLongHashFullOuter =>
        Some(PhysicalOp.IntLongHashFullOuter(leftExchange, rightExchange))
      case KernelChoice.IntLongBroadcastJoin =>
        Some(PhysicalOp.IntLongBroadcastJoin(left, right, PhysicalOp.BuildSide.Right))
      case KernelChoice.IntLongBroadcastLeftOuter =>
        Some(PhysicalOp.IntLongBroadcastLeftOuter(left, right))
      case KernelChoice.IntLongBroadcastRightOuter =>
        Some(PhysicalOp.IntLongBroadcastRightOuter(left, right))
      case KernelChoice.IntLongBroadcastFullOuter =>
        Some(PhysicalOp.IntLongBroadcastFullOuter(left, right))
      case KernelChoice.IntLongSortMergeJoin =>
        Some(PhysicalOp.IntLongSortMergeJoin(leftExchange, rightExchange))
      case KernelChoice.IntLongSortMergeLeftOuter =>
        Some(PhysicalOp.IntLongSortMergeLeftOuter(leftExchange, rightExchange))
      case KernelChoice.IntLongSortMergeRightOuter =>
        Some(PhysicalOp.IntLongSortMergeRightOuter(leftExchange, rightExchange))
      case KernelChoice.IntLongSortMergeFullOuter =>
        Some(PhysicalOp.IntLongSortMergeFullOuter(leftExchange, rightExchange))
      case KernelChoice.IntHashAggregate | KernelChoice.LongHashAggregate |
          KernelChoice.StringHashAggregate | KernelChoice.IntLongProjectSum |
          KernelChoice.IntLongHashAggregate | KernelChoice.CountByKey |
          KernelChoice.CountThroughProbe | KernelChoice.CountSum | KernelChoice.JoinProbeSum |
          KernelChoice.SortByIntKey | KernelChoice.SortByIntValueDesc |
          KernelChoice.SortByLongKey | KernelChoice.SortByStringKey |
          KernelChoice.SortByLongValueDesc | KernelChoice.SortByStringValueDesc |
          KernelChoice.SortByIntLongKey | KernelChoice.SortByIntLongValueDesc |
          KernelChoice.GroupByKey | KernelChoice.GroupedDecode | KernelChoice.Dedupe |
          KernelChoice.Scan | KernelChoice.Filter | KernelChoice.Project | KernelChoice.Column |
          KernelChoice.MapValue | KernelChoice.IntLongMapValue | KernelChoice.MapKey |
          KernelChoice.Pipeline | KernelChoice.Union | KernelChoice.Exchange =>
        None

  /**
   * Join of two int-long pairs. The caller already checked both children.
   *
   * No hint and shuffle-hash use the hash nodes. Broadcast and sort-merge use their own nodes.
   * This is not [[choose]]: an `Int32` key on [[PhysicalOp.Kind.Pairs]] would build the int hash
   * join and read the long as an int.
   */
  private[execution] def intLongInner(
      strategy: Option[Plan.JoinStrategy],
      place: Place,
  ): Option[KernelChoice] =
    (strategy, place) match
      case (None | Some(Plan.JoinStrategy.ShuffleHash), Place.Inner) =>
        Some(KernelChoice.IntLongHashJoin)
      case (None | Some(Plan.JoinStrategy.ShuffleHash), Place.Left) =>
        Some(KernelChoice.IntLongHashLeftOuter)
      case (None | Some(Plan.JoinStrategy.ShuffleHash), Place.Right) =>
        Some(KernelChoice.IntLongHashRightOuter)
      case (None | Some(Plan.JoinStrategy.ShuffleHash), Place.Full) =>
        Some(KernelChoice.IntLongHashFullOuter)
      case (Some(Plan.JoinStrategy.Broadcast), Place.Inner) =>
        Some(KernelChoice.IntLongBroadcastJoin)
      case (Some(Plan.JoinStrategy.Broadcast), Place.Left) =>
        Some(KernelChoice.IntLongBroadcastLeftOuter)
      case (Some(Plan.JoinStrategy.Broadcast), Place.Right) =>
        Some(KernelChoice.IntLongBroadcastRightOuter)
      case (Some(Plan.JoinStrategy.Broadcast), Place.Full) =>
        Some(KernelChoice.IntLongBroadcastFullOuter)
      case (Some(Plan.JoinStrategy.SortMerge), Place.Inner) =>
        Some(KernelChoice.IntLongSortMergeJoin)
      case (Some(Plan.JoinStrategy.SortMerge), Place.Left) =>
        Some(KernelChoice.IntLongSortMergeLeftOuter)
      case (Some(Plan.JoinStrategy.SortMerge), Place.Right) =>
        Some(KernelChoice.IntLongSortMergeRightOuter)
      case (Some(Plan.JoinStrategy.SortMerge), Place.Full) =>
        Some(KernelChoice.IntLongSortMergeFullOuter)

  /** Pair kinds the int, long, and string joins already run. Outer kinds are not pairs. */
  private def pairKind(kind: PhysicalOp.Kind): Boolean = kind match
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.StringPairs =>
      true
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.Strings | PhysicalOp.Kind.Triples |
        PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.GroupedInts |
        PhysicalOp.Kind.GroupedStrings | PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter |
        PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.CountSums |
        PhysicalOp.Kind.LongCountSums | PhysicalOp.Kind.StringCountSums |
        PhysicalOp.Kind.IntLongSums | PhysicalOp.Kind.GroupedIntLongs |
        PhysicalOp.Kind.IntLongTriples | PhysicalOp.Kind.IntLongOuter =>
      false

  private def agreed(left: ColumnType, right: ColumnType): Boolean = (left, right) match
    case (ColumnType.Int32, ColumnType.Int32) | (ColumnType.Int64, ColumnType.Int64) |
        (ColumnType.Float64, ColumnType.Float64) | (ColumnType.Bool, ColumnType.Bool) |
        (ColumnType.Utf8Dict, ColumnType.Utf8Dict) =>
      true
    case _ => false

  /** Key families that already have a join class. Float and bool do not. */
  private def family(key: ColumnType): Option[Family] = key match
    case ColumnType.Int32 => Some(Family.IntKey)
    case ColumnType.Int64 => Some(Family.LongKey)
    case ColumnType.Utf8Dict => Some(Family.StringKey)
    case ColumnType.Float64 | ColumnType.Bool => None

  /** No hint and shuffle-hash are the hash class. Broadcast and sort-merge stay separate. */
  private def hint(strategy: Option[Plan.JoinStrategy]): Hint = strategy match
    case None | Some(Plan.JoinStrategy.ShuffleHash) => Hint.Hash
    case Some(Plan.JoinStrategy.Broadcast) => Hint.Broadcast
    case Some(Plan.JoinStrategy.SortMerge) => Hint.SortMerge

  private def node(keyFamily: Family, hint: Hint, place: Place): KernelChoice =
    (keyFamily, hint, place) match
      case (Family.IntKey, Hint.Hash, Place.Inner) => KernelChoice.HashJoin
      case (Family.IntKey, Hint.Broadcast, Place.Inner) => KernelChoice.BroadcastJoin
      case (Family.IntKey, Hint.SortMerge, Place.Inner) => KernelChoice.SortMergeJoin
      case (Family.IntKey, Hint.Hash, Place.Left) => KernelChoice.HashLeftOuter
      case (Family.IntKey, Hint.Hash, Place.Right) => KernelChoice.HashRightOuter
      case (Family.IntKey, Hint.Hash, Place.Full) => KernelChoice.HashFullOuter
      case (Family.IntKey, Hint.Broadcast, Place.Left) => KernelChoice.BroadcastLeftOuter
      case (Family.IntKey, Hint.Broadcast, Place.Right) => KernelChoice.BroadcastRightOuter
      case (Family.IntKey, Hint.Broadcast, Place.Full) => KernelChoice.BroadcastFullOuter
      case (Family.IntKey, Hint.SortMerge, Place.Left) => KernelChoice.SortMergeLeftOuter
      case (Family.IntKey, Hint.SortMerge, Place.Right) => KernelChoice.SortMergeRightOuter
      case (Family.IntKey, Hint.SortMerge, Place.Full) => KernelChoice.SortMergeFullOuter
      case (Family.LongKey, Hint.Hash, Place.Inner) => KernelChoice.LongHashJoin
      case (Family.LongKey, Hint.Broadcast, Place.Inner) => KernelChoice.LongBroadcastJoin
      case (Family.LongKey, Hint.SortMerge, Place.Inner) => KernelChoice.LongSortMergeJoin
      case (Family.LongKey, Hint.Hash, Place.Left) => KernelChoice.LongHashLeftOuter
      case (Family.LongKey, Hint.Hash, Place.Right) => KernelChoice.LongHashRightOuter
      case (Family.LongKey, Hint.Hash, Place.Full) => KernelChoice.LongHashFullOuter
      case (Family.LongKey, Hint.Broadcast, Place.Left) => KernelChoice.LongBroadcastLeftOuter
      case (Family.LongKey, Hint.Broadcast, Place.Right) => KernelChoice.LongBroadcastRightOuter
      case (Family.LongKey, Hint.Broadcast, Place.Full) => KernelChoice.LongBroadcastFullOuter
      case (Family.LongKey, Hint.SortMerge, Place.Left) => KernelChoice.LongSortMergeLeftOuter
      case (Family.LongKey, Hint.SortMerge, Place.Right) => KernelChoice.LongSortMergeRightOuter
      case (Family.LongKey, Hint.SortMerge, Place.Full) => KernelChoice.LongSortMergeFullOuter
      case (Family.StringKey, Hint.Hash, Place.Inner) => KernelChoice.StringHashJoin
      case (Family.StringKey, Hint.Broadcast, Place.Inner) => KernelChoice.StringBroadcastJoin
      case (Family.StringKey, Hint.SortMerge, Place.Inner) => KernelChoice.StringSortMergeJoin
      case (Family.StringKey, Hint.Hash, Place.Left) => KernelChoice.StringHashLeftOuter
      case (Family.StringKey, Hint.Hash, Place.Right) => KernelChoice.StringHashRightOuter
      case (Family.StringKey, Hint.Hash, Place.Full) => KernelChoice.StringHashFullOuter
      case (Family.StringKey, Hint.Broadcast, Place.Left) =>
        KernelChoice.StringBroadcastLeftOuter
      case (Family.StringKey, Hint.Broadcast, Place.Right) =>
        KernelChoice.StringBroadcastRightOuter
      case (Family.StringKey, Hint.Broadcast, Place.Full) =>
        KernelChoice.StringBroadcastFullOuter
      case (Family.StringKey, Hint.SortMerge, Place.Left) =>
        KernelChoice.StringSortMergeLeftOuter
      case (Family.StringKey, Hint.SortMerge, Place.Right) =>
        KernelChoice.StringSortMergeRightOuter
      case (Family.StringKey, Hint.SortMerge, Place.Full) =>
        KernelChoice.StringSortMergeFullOuter
