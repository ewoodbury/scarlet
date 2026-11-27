package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.{ColumnCompare, Plan, WideRow}

/**
 * Planner IR for the steps applied inside a stage. [[Stage]] is what runs. [[StageBuilder]] builds
 * [[Stage.ChainedStage]] from a chain of these operations in `chainOperationUnsafe`.
 *
 * @tparam A
 *   Input type of the operation
 * @tparam B
 *   Output type of the operation
 */
sealed trait Operation[A, B]

/**
 * Type-safe metadata for wide operations using sealed trait hierarchy.
 */
sealed trait WideOpMeta {
  def kind: WideOpKind
  def numPartitions: Int
  def sides: Seq[StageBuilder.Side]
}

/**
 * Metadata for simple wide operations that don't require functions.
 */
final case class SimpleWideOpMeta(
    kind: WideOpKind,
    numPartitions: Int,
    joinStrategy: Option[com.ewoodbury.scarlet.core.Plan.JoinStrategy] = None,
    sides: Seq[StageBuilder.Side] = Seq.empty,
) extends WideOpMeta

/**
 * Metadata for sort operations with typed key function and ordering.
 */
final case class SortWideOpMeta[A, B](
    kind: WideOpKind = WideOpKind.SortBy,
    numPartitions: Int,
    keyFunc: A => B,
    ordering: Ordering[B],
    sides: Seq[StageBuilder.Side] = Seq.empty,
) extends WideOpMeta

/**
 * Metadata for reduce operations with typed reduce function.
 */
final case class ReduceWideOpMeta[V](
    kind: WideOpKind = WideOpKind.ReduceByKey,
    numPartitions: Int,
    reduceFunc: (V, V) => V,
    sides: Seq[StageBuilder.Side] = Seq.empty,
) extends WideOpMeta

/**
 * Enumeration of wide operation types that require shuffle boundaries.
 */
enum WideOpKind:
  case GroupByKey, ReduceByKey, SortBy, PartitionBy, Repartition, Coalesce, Distinct, Join,
    LeftOuterJoin, RightOuterJoin, FullOuterJoin, CoGroup

/**
 * Structured description of a wide operation attached to a shuffle stage. This is the execution
 * dispatch record: StageExecutor executes a shuffle stage by pattern-matching on it.
 */
sealed trait WideOp

// Concrete wide operation implementations
final case class GroupByKeyWideOp(meta: SimpleWideOpMeta) extends WideOp

/**
 * Keyed count. The metadata reuses [[WideOpKind.GroupByKey]] so the shuffle writer hashes by key.
 * This is not [[GroupByKeyWideOp]]: that operator returns `(K, Iterable[V])`.
 */
final case class CountByKeyWideOp(meta: SimpleWideOpMeta) extends WideOp

/**
 * Keyed count and sum of `(Int, Int)`. The metadata reuses [[WideOpKind.GroupByKey]] so the
 * shuffle writer hashes by key. This is not [[CountByKeyWideOp]]: that operator returns
 * `(K, Int)`.
 */
final case class CountSumByKeyWideOp(meta: SimpleWideOpMeta) extends WideOp

/** Keyed count and sum of `(Long, Long)`. The writer hashes by the long key. */
final case class CountSumByLongKeyWideOp(meta: SimpleWideOpMeta) extends WideOp

/** Keyed count and sum of `(String, Int)`. The writer hashes by the string key. */
final case class CountSumByStringKeyWideOp(meta: SimpleWideOpMeta) extends WideOp

/** Keyed count and sum of `(Int, Long)`. The writer hashes by the int key. */
final case class CountSumByIntLongKeyWideOp(meta: SimpleWideOpMeta) extends WideOp

/**
 * Keyed min or max of `(Int, Int)`. The metadata reuses [[WideOpKind.GroupByKey]] so the writer
 * hashes by key. `minimum` selects the min.
 */
final case class ExtremumByKeyWideOp(meta: SimpleWideOpMeta, minimum: Boolean) extends WideOp
final case class ReduceByKeyWideOp[V](meta: ReduceWideOpMeta[V]) extends WideOp
final case class SortByWideOp[A, B](meta: SortWideOpMeta[A, B]) extends WideOp
final case class PartitionByWideOp(meta: SimpleWideOpMeta) extends WideOp
final case class RepartitionWideOp(meta: SimpleWideOpMeta) extends WideOp
final case class CoalesceWideOp(meta: SimpleWideOpMeta) extends WideOp

/**
 * Element distinct. The writer hashes the element, not a pair key, then the stage keeps one copy
 * in each shuffle partition. The output distribution is unknown: this is not pair-key hash, so a
 * later `groupByKey` still shuffles.
 */
final case class DistinctWideOp(meta: SimpleWideOpMeta) extends WideOp
final case class JoinWideOp(meta: SimpleWideOpMeta) extends WideOp
final case class LeftOuterJoinWideOp(meta: SimpleWideOpMeta) extends WideOp
final case class RightOuterJoinWideOp(meta: SimpleWideOpMeta) extends WideOp
final case class FullOuterJoinWideOp(meta: SimpleWideOpMeta) extends WideOp
final case class CoGroupWideOp(meta: SimpleWideOpMeta) extends WideOp

// Narrow transformations
final case class MapOp[A, B](f: A => B) extends Operation[A, B]
final case class FilterOp[A](p: A => Boolean) extends Operation[A, A]
final case class FlatMapOp[A, B](f: A => IterableOnce[B]) extends Operation[A, B]
final case class DistinctOp[A]() extends Operation[A, A]
final case class KeysOp[K, V]() extends Operation[(K, V), K]
final case class ValuesOp[K, V]() extends Operation[(K, V), V]
final case class MapValuesOp[K, V, V2](f: V => V2) extends Operation[(K, V), (K, V2)]

/** Int-key map. A null key stays null and `f` is not called. The output is still a pair. */
final case class MapIntKeyOp(f: Int => Int) extends Operation[(Int, Int), (Int, Int)]
final case class FilterKeysOp[K, V](p: K => Boolean) extends Operation[(K, V), (K, V)]
final case class FilterValuesOp[K, V](p: V => Boolean) extends Operation[(K, V), (K, V)]
final case class FlatMapValuesOp[K, V, V2](f: V => IterableOnce[V2])
    extends Operation[(K, V), (K, V2)]
final case class MapPartitionsOp[A, B](f: Iterator[A] => Iterator[B]) extends Operation[A, B]

// Bypassed wide operations, appended as narrow no-op stages when upstream data is already
// partitioned the way the operation wants it
final case class GroupByKeyLocalOp[K, V]() extends Operation[(K, V), (K, Iterable[V])]

/** Local count of an already key-hashed partition. The value is not read. */
final case class CountByKeyLocalOp[K, V]() extends Operation[(K, V), (K, Int)]

/**
 * Local count and sum of an already key-hashed `(Int, Int)` partition. A null key is dropped. A
 * null value is counted and not added.
 */
final case class CountSumByKeyLocalOp() extends Operation[(Int, Int), (Int, Int, Long)]

/** Local count and sum of an already key-hashed `(Long, Long)` partition. */
final case class CountSumByLongKeyLocalOp() extends Operation[(Long, Long), (Long, Int, Long)]

/** Local count and sum of an already key-hashed `(String, Int)` partition. */
final case class CountSumByStringKeyLocalOp() extends Operation[(String, Int), (String, Int, Long)]

/** Local count and sum of an already key-hashed `(Int, Long)` partition. */
final case class CountSumByIntLongKeyLocalOp() extends Operation[(Int, Long), (Int, Int, Long)]

/**
 * Local min or max of an already key-hashed `(Int, Int)` partition. A null key is dropped. A null
 * value is skipped. A key whose values are all null emits no row.
 */
final case class ExtremumByKeyLocalOp(minimum: Boolean) extends Operation[(Int, Int), (Int, Int)]
final case class ReduceByKeyLocalOp[K, V](reduceFunc: (V, V) => V)
    extends Operation[(K, V), (K, V)]
final case class PartitionByLocalOp[K, V](numPartitions: Int) extends Operation[(K, V), (K, V)]
final case class RepartitionOp[A](numPartitions: Int) extends Operation[A, A]
final case class CoalesceOp[A](numPartitions: Int) extends Operation[A, A]

/** Filter one wide-row column. A null in that column is dropped. */
final case class WideFilterOp(ordinal: Int, compare: ColumnCompare)
    extends Operation[WideRow, WideRow]

/** Drop, reorder, or rename wide-row columns. Does not compute. */
final case class WideProjectOp(spec: Vector[(Int, String)]) extends Operation[WideRow, WideRow]

object Operation {

  /**
   * The single named erasure boundary for Plan → Operation conversion. Stage building works with
   * `Operation[Any, Any]` because element types are erased at the stage level; the returned
   * operation retains its real types, so execution is type-correct even though the compiler cannot
   * verify it here.
   */
  @SuppressWarnings(
    Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Any"),
  )
  private[execution] def fromPlan(plan: Plan[_]): Operation[Any, Any] = plan match {
    case Plan.MapOp(_, f) => MapOp(f.asInstanceOf[Any => Any])
    case Plan.FilterOp(_, p) => FilterOp(p.asInstanceOf[Any => Boolean])
    case Plan.FlatMapOp(_, f) => FlatMapOp(f.asInstanceOf[Any => IterableOnce[Any]])
    case Plan.DistinctOp(_) => DistinctOp[Any]()
    case Plan.KeysOp(_) => KeysOp[Any, Any]().asInstanceOf[Operation[Any, Any]]
    case Plan.ValuesOp(_) => ValuesOp[Any, Any]().asInstanceOf[Operation[Any, Any]]
    case Plan.MapValuesOp(_, f) =>
      MapValuesOp[Any, Any, Any](f.asInstanceOf[Any => Any]).asInstanceOf[Operation[Any, Any]]
    case Plan.MapIntKeyOp(_, f) =>
      MapIntKeyOp(f).asInstanceOf[Operation[Any, Any]]
    case Plan.FilterKeysOp(_, p) =>
      FilterKeysOp[Any, Any](p.asInstanceOf[Any => Boolean]).asInstanceOf[Operation[Any, Any]]
    case Plan.FilterValuesOp(_, p) =>
      FilterValuesOp[Any, Any](p.asInstanceOf[Any => Boolean]).asInstanceOf[Operation[Any, Any]]
    case Plan.FlatMapValuesOp(_, f) =>
      FlatMapValuesOp[Any, Any, Any](f.asInstanceOf[Any => IterableOnce[Any]])
        .asInstanceOf[Operation[Any, Any]]
    case Plan.MapPartitionsOp(_, f) =>
      MapPartitionsOp(f.asInstanceOf[Iterator[Any] => Iterator[Any]])
    case _ =>
      throw new IllegalArgumentException(
        s"Plan node $plan is not a narrow operation and cannot become a stage Operation",
      )
  }

  /**
   * Determines if a shuffle operation can be bypassed based on upstream partitioning metadata.
   */
  private[execution] def canBypassShuffle(
      plan: Plan[_],
      upstreamPartitioning: Option[PartitioningInfo],
      conf: com.ewoodbury.scarlet.core.ScarletConf,
  ): Boolean = {
    plan match {
      case _: Plan.GroupByKeyOp[_, _] | _: Plan.CountByKeyOp[_, _] | _: Plan.ReduceByKeyOp[_, _] |
          _: Plan.CountSumByKeyOp | _: Plan.CountSumByLongKeyOp | _: Plan.CountSumByStringKeyOp |
          _: Plan.CountSumByIntLongKeyOp | _: Plan.ExtremumByKeyOp =>
        // Already hash-partitioned at the default shuffle width.
        upstreamPartitioning.exists(_.isHashPartitioned(conf.defaultShufflePartitions))

      case pby: Plan.PartitionByOp[_, _] =>
        upstreamPartitioning.exists(_.isHashPartitioned(pby.numPartitions))

      case rep: Plan.RepartitionOp[_] =>
        // Any non-hash width: unknown, range, round-robin, or singleton.
        upstreamPartitioning.exists(_.matchesNonHashWidth(rep.numPartitions))

      case _ =>
        // Coalesce and other wide operations cannot bypass shuffle
        false
    }
  }
}
