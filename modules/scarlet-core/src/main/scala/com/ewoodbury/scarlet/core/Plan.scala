package com.ewoodbury.scarlet.core

/**
 * Represents the logical plan for computing a DistCollection. This is a DAG where nodes are
 * operations.
 *
 * @tparam A
 *   The type of elements in the collection represented by this plan node.
 */
sealed trait Plan[A]

object Plan {

  /**
   * Type information carried by a source for empty partitions and checked against non-empty data.
   */
  enum SourceDescriptor:
    case Unknown, Ints, Longs, Doubles, Pairs, Strings, Bools, LongPairs

  /**
   * Applies a user function over each input partition using iterators. The function receives an
   * iterator view of the partition elements and returns an iterator of output elements. This is a
   * narrow transformation that preserves partitioning metadata where applicable.
   *
   * @param source
   *   The preceding plan node (producing elements of type A).
   * @param f
   *   The partition-level mapping function operating on iterators.
   * @tparam A
   *   The input element type from the source plan.
   * @tparam B
   *   The output element type.
   */
  final case class MapPartitionsOp[A, B](source: Plan[A], f: Iterator[A] => Iterator[B])
      extends Plan[B]

  /**
   * Repartitions the dataset to exactly `numPartitions` partitions. Implemented as a shuffle in
   * the current engine. This transformation does not imply key-based partitioning.
   *
   * @param source
   *   The preceding plan node.
   * @param numPartitions
   *   Target number of partitions; must be positive.
   * @tparam A
   *   The element type.
   */
  final case class RepartitionOp[A](source: Plan[A], numPartitions: Int) extends Plan[A]

  /**
   * Coalesces the dataset into `numPartitions` partitions. In this initial implementation, this is
   * performed via a shuffle for simplicity and determinism. Future implementations may avoid
   * shuffles when safe.
   *
   * @param source
   *   The preceding plan node.
   * @param numPartitions
   *   Target number of partitions; must be positive and less than or equal to current partitions.
   * @tparam A
   *   The element type.
   */
  final case class CoalesceOp[A](source: Plan[A], numPartitions: Int) extends Plan[A]

  /**
   * Partitions a key-value dataset by key using the runtime's configured partitioner and the
   * specified number of partitions.
   *
   * @param source
   *   The preceding plan node producing `(K, V)` pairs.
   * @param numPartitions
   *   Target number of partitions; must be positive.
   */
  final case class PartitionByOp[K, V](source: Plan[(K, V)], numPartitions: Int)
      extends Plan[(K, V)]

  /**
   * The initial data source. It stores the `Seq[Partition[A]]` it is given and does not copy it.
   * The `DistCollection` factories copy record data (`slice` / `fromOwned`) before those
   * partitions are built.
   *
   * @param partitions
   *   The partitions this source stores.
   * @param descriptor
   *   Type information for empty partitions and checks against non-empty data.
   */
  case class Source[A](
      partitions: Seq[Partition[A]],
      descriptor: SourceDescriptor = SourceDescriptor.Unknown,
  ) extends Plan[A]

  /**
   * Represents a map transformation.
   * @param source
   *   The preceding plan node (producing elements of type I).
   * @param f
   *   The mapping function from I to A.
   * @tparam I
   *   The input element type from the source plan.
   */
  case class MapOp[I, A](source: Plan[I], f: I => A) extends Plan[A]

  /**
   * Represents a filter transformation.
   * @param source
   *   The preceding plan node (producing elements of type A).
   * @param p
   *   The predicate function.
   */
  case class FilterOp[A](source: Plan[A], p: A => Boolean) extends Plan[A]

  /**
   * Represents a flatMap transformation.
   * @param source
   *   The preceding plan node (producing elements of type A).
   * @param f
   *   The flatMap function from A to IterableOnce[B].
   * @tparam B
   *   The type of elements in the resulting DistCollection.
   */
  case class FlatMapOp[A, B](source: Plan[A], f: A => IterableOnce[B]) extends Plan[B]

  /**
   * Represents a distinct transformation.
   * @param source
   *   The preceding plan node (producing elements of type A).
   */
  case class DistinctOp[A](source: Plan[A]) extends Plan[A]

  /**
   * Represents a union transformation.
   * @param left
   *   The left preceding plan node (producing elements of type A).
   * @param right
   *   The right preceding plan node (producing elements of type A).
   */
  case class UnionOp[A](left: Plan[A], right: Plan[A]) extends Plan[A]

  /**
   * Represents a keys transformation.
   * @param source
   *   The preceding plan node (producing elements of type (K, V)).
   */
  case class KeysOp[K, V](source: Plan[(K, V)]) extends Plan[K]

  /**
   * Represents a values transformation.
   * @param source
   *   The preceding plan node (producing elements of type (K, V)).
   */
  case class ValuesOp[K, V](source: Plan[(K, V)]) extends Plan[V]

  /**
   * Represents a mapValues transformation.
   * @param source
   *   The preceding plan node (producing elements of type (K, A)).
   * @param mapFunction
   *   The mapping function from A to B.
   * @tparam K
   *   The key type.
   * @tparam V
   *   The value type of the source plan.
   * @tparam B
   *   The type of elements in the resulting DistCollection.
   */
  case class MapValuesOp[K, V, B](source: Plan[(K, V)], mapFunction: V => B) extends Plan[(K, B)]

  /**
   * Represents a filterKeys transformation.
   * @param source
   *   The preceding plan node (producing elements of type (K, A)).
   * @param predicateFunction
   *   The predicate function from K to Boolean.
   * @tparam K
   *   The key type.
   * @tparam V
   *   The value type of the source plan.
   */
  case class FilterKeysOp[K, V](source: Plan[(K, V)], predicateFunction: K => Boolean)
      extends Plan[(K, V)]

  /**
   * Represents a filterValues transformation.
   * @param source
   *   The preceding plan node (producing elements of type (K, V)).
   * @param predicateFunction
   *   The predicate function from V to Boolean.
   * @tparam K
   *   The key type.
   * @tparam V
   *   The value type of the source plan.
   */
  case class FilterValuesOp[K, V](source: Plan[(K, V)], predicateFunction: V => Boolean)
      extends Plan[(K, V)]

  /**
   * Represents a flatMapValues transformation.
   * @param source
   *   The preceding plan node (producing elements of type (K, V)).
   * @param flatMapFunction
   *   The flatMap function from V to IterableOnce[B].
   * @tparam K
   *   The key type.
   * @tparam V
   *   The value type of the source plan.
   * @tparam B
   *   The type of elements in the resulting DistCollection.
   */
  case class FlatMapValuesOp[K, V, B](source: Plan[(K, V)], flatMapFunction: V => IterableOnce[B])
      extends Plan[(K, B)]

  // --- Wide Transformations (require shuffles) ---

  /**
   * Represents a groupByKey transformation that requires shuffling data by key.
   * @param source
   *   The preceding plan node (producing elements of type (K, V)).
   * @tparam K
   *   The key type.
   * @tparam V
   *   The value type.
   */
  case class GroupByKeyOp[K, V](source: Plan[(K, V)]) extends Plan[(K, Iterable[V])]

  /**
   * Represents a reduceByKey transformation that requires shuffling data by key.
   * @param source
   *   The preceding plan node (producing elements of type (K, V)).
   * @param reduceFunc
   *   The reduction function to combine values with the same key.
   * @tparam K
   *   The key type.
   * @tparam V
   *   The value type.
   */
  case class ReduceByKeyOp[K, V](source: Plan[(K, V)], reduceFunc: (V, V) => V)
      extends Plan[(K, V)]

  /**
   * Represents a sortBy transformation that requires shuffling data for global ordering.
   * @param source
   *   The preceding plan node (producing elements of type A).
   * @param keyFunc
   *   The function to extract the sort key from each element.
   * @param ordering
   *   The ordering to use for sorting.
   * @tparam A
   *   The element type.
   * @tparam B
   *   The sort key type.
   */
  case class SortByOp[A, B](source: Plan[A], keyFunc: A => B, ordering: Ordering[B])
      extends Plan[A]

  /**
   * Sorts `(Int, Int)` pairs ascending by column 0. This node is planned like [[SortByOp]] with
   * the key taken from column 0 and [[Ordering.Int]]. Generic [[SortByOp]] stays opaque. A scan of
   * int pairs lowers to a columnar rank split. Any other child stays on the row path. A null `Int`
   * key orders as zero and the row is retained. This matches `sortBy(_._1)` and is not a null-safe
   * ordering.
   *
   * @param source
   *   The preceding plan node.
   */
  case class SortByIntKeyOp(source: Plan[(Int, Int)]) extends Plan[(Int, Int)]

  /**
   * Sorts `(Long, Long)` pairs ascending by column 0. This node is planned like [[SortByOp]] with
   * the key taken from column 0 and [[Ordering.Long]]. Generic [[SortByOp]] stays opaque. A scan
   * of long pairs lowers to a columnar rank split. Any other child stays on the row path. A null
   * `Long` key orders as zero and the row is retained. This matches `sortBy(_._1)` and is not a
   * null-safe ordering. There is no spill. An input over the budget stays in memory.
   *
   * @param source
   *   The preceding plan node.
   */
  case class SortByLongKeyOp(source: Plan[(Long, Long)]) extends Plan[(Long, Long)]

  /**
   * Join execution strategies for physical planning.
   */
  enum JoinStrategy {
    case ShuffleHash, SortMerge, Broadcast
  }

  /**
   * Represents a join transformation that requires shuffling both datasets by key.
   * @param left
   *   The left plan node (producing elements of type (K, V)).
   * @param right
   *   The right plan node (producing elements of type (K, W)).
   * @param joinStrategy
   *   Hint for join strategy selection (None means auto-select). A null key is dropped on both
   *   sides for every strategy and does not match.
   * @tparam K
   *   The key type.
   * @tparam V
   *   The left value type.
   * @tparam W
   *   The right value type.
   */
  case class JoinOp[K, V, W](
      left: Plan[(K, V)],
      right: Plan[(K, W)],
      joinStrategy: Option[JoinStrategy],
  ) extends Plan[(K, (V, W))]

  /**
   * Represents a left outer join. Matching keys emit the cartesian product. A left row whose key
   * is absent on the right is emitted once with `None`. A null key never matches.
   *
   * @param left
   *   The left plan node (producing elements of type (K, V)).
   * @param right
   *   The right plan node (producing elements of type (K, W)).
   * @param joinStrategy
   *   Stored with the plan. The row executor ignores it and always runs shuffle-hash. The columnar
   *   planner accepts a broadcast hint, a shuffle-hash hint, or an absent hint for int pairs. The
   *   left input is the probe side. An absent hint uses the shuffle-hash op, matching an inner
   *   join. A null value stays on the row path. An explicit sort-merge hint on int pairs lowers to
   *   `SortMergeLeftOuter`.
   * @tparam K
   *   The key type.
   * @tparam V
   *   The left value type.
   * @tparam W
   *   The right value type.
   */
  case class LeftOuterJoinOp[K, V, W](
      left: Plan[(K, V)],
      right: Plan[(K, W)],
      joinStrategy: Option[JoinStrategy],
  ) extends Plan[(K, (V, Option[W]))]

  /**
   * Represents a right outer join. Matching keys emit the cartesian product. A right row whose key
   * is absent on the left is emitted once with `None`. A null key never matches.
   *
   * @param left
   *   The left plan node (producing elements of type (K, V)).
   * @param right
   *   The right plan node (producing elements of type (K, W)).
   * @param joinStrategy
   *   Stored with the plan. The row executor ignores it and always runs shuffle-hash. The columnar
   *   planner accepts a broadcast hint, a shuffle-hash hint, or an absent hint for int pairs. The
   *   right input is the probe side. An absent hint uses the shuffle-hash op, matching an inner
   *   join. A null value stays on the row path. An explicit sort-merge hint on int pairs lowers to
   *   `SortMergeRightOuter`.
   * @tparam K
   *   The key type.
   * @tparam V
   *   The left value type.
   * @tparam W
   *   The right value type.
   */
  case class RightOuterJoinOp[K, V, W](
      left: Plan[(K, V)],
      right: Plan[(K, W)],
      joinStrategy: Option[JoinStrategy],
  ) extends Plan[(K, (Option[V], W))]

  /**
   * Represents a full outer join. Matching keys emit the cartesian product with both values
   * present. A key only on the left emits each left row with the right value absent. A key only on
   * the right emits each right row with the left value absent. A null key never matches: each left
   * null row is emitted once with the right value absent, and each right null row is emitted once
   * with the left value absent.
   *
   * @param left
   *   The left plan node (producing elements of type (K, V)).
   * @param right
   *   The right plan node (producing elements of type (K, W)).
   * @param joinStrategy
   *   Stored with the plan. The row executor ignores it and always runs shuffle-hash. The columnar
   *   planner accepts a broadcast hint, a shuffle-hash hint, or an absent hint for int pairs. The
   *   left input is the probe side. An absent hint uses the shuffle-hash op. A null value stays on
   *   the row path. An explicit sort-merge hint on int pairs lowers to `SortMergeFullOuter`.
   * @tparam K
   *   The key type.
   * @tparam V
   *   The left value type.
   * @tparam W
   *   The right value type.
   */
  case class FullOuterJoinOp[K, V, W](
      left: Plan[(K, V)],
      right: Plan[(K, W)],
      joinStrategy: Option[JoinStrategy],
  ) extends Plan[(K, (Option[V], Option[W]))]

  /**
   * Represents a cogroup transformation that groups both datasets by key. A null key is dropped on
   * either side and does not form a group.
   * @param left
   *   The left plan node (producing elements of type (K, V)).
   * @param right
   *   The right plan node (producing elements of type (K, W)).
   * @tparam K
   *   The key type.
   * @tparam V
   *   The left value type.
   * @tparam W
   *   The right value type.
   */
  case class CoGroupOp[K, V, W](left: Plan[(K, V)], right: Plan[(K, W)])
      extends Plan[(K, (Iterable[V], Iterable[W]))]
}
