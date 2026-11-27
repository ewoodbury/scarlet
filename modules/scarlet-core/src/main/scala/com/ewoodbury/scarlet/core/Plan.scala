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
    case Unknown, Ints, Longs, Doubles, Pairs, Strings, Bools, LongPairs, Float32s, Columns

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
   * Maps an int key and keeps the value.
   *
   * A null key stays null and `mapFunction` is not called. The node is the witness that the
   * function is `Int => Int`. The planner does not inspect the function.
   */
  case class MapIntKeyOp(source: Plan[(Int, Int)], mapFunction: Int => Int)
      extends Plan[(Int, Int)]

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
   * Counts rows per key. A null key is dropped. A null value still counts; the operator does not
   * read it. The result is one `(key, count)` pair per distinct key. The count is an `Int` and
   * wraps on overflow, the same two's-complement wrap `HashReduce` uses.
   *
   * @param source
   *   The preceding plan node (producing elements of type (K, V)).
   * @tparam K
   *   The key type.
   * @tparam V
   *   The value type. It is not read.
   */
  case class CountByKeyOp[K, V](source: Plan[(K, V)]) extends Plan[(K, Int)]

  /**
   * Counts and sums `(Int, Int)` pairs by key.
   *
   * A null key is dropped. A null value increments the count and is not added to the sum. The
   * count is an `Int` and wraps. The sum is a `Long` and does not wrap at `Int`. One
   * `(key, count, sum)` row is published per distinct key. This is a typed witness, the way
   * [[MapIntKeyOp]] is. A generic `groupByKey` followed by `map` stays [[GroupByKeyOp]] and does
   * not become this node.
   *
   * @param source
   *   The preceding plan node, producing `(Int, Int)` pairs.
   */
  case class CountSumByKeyOp(source: Plan[(Int, Int)]) extends Plan[(Int, Int, Long)]

  /**
   * Counts and sums `(Long, Long)` pairs by key.
   *
   * A null key is dropped. A null value increments the count and is not added to the sum. The
   * count is an `Int` and wraps. The sum is a `Long`. One `(key, count, sum)` row is published per
   * distinct key. This is a typed witness. A generic `groupByKey` followed by `map` stays
   * [[GroupByKeyOp]] and does not become this node.
   *
   * @param source
   *   The preceding plan node, producing `(Long, Long)` pairs.
   */
  case class CountSumByLongKeyOp(source: Plan[(Long, Long)]) extends Plan[(Long, Int, Long)]

  /**
   * Counts and sums `(String, Int)` pairs by key.
   *
   * A null key is dropped. An empty string is a present key. A null value increments the count and
   * is not added to the sum. The count is an `Int` and wraps. The sum is a `Long`. One
   * `(key, count, sum)` row is published per distinct key. This is a typed witness. A generic
   * `groupByKey` followed by `map` stays [[GroupByKeyOp]] and does not become this node.
   *
   * @param source
   *   The preceding plan node, producing `(String, Int)` pairs.
   */
  case class CountSumByStringKeyOp(source: Plan[(String, Int)]) extends Plan[(String, Int, Long)]

  /**
   * Counts and sums `(Int, Long)` pairs by key.
   *
   * A null key is dropped. A null value increments the count and is not added to the sum. The
   * count is an `Int` and wraps. The sum is a `Long` of the long values. One `(key, count, sum)`
   * row is published per distinct key. This is a typed witness. A generic `groupByKey` followed by
   * `map` stays [[GroupByKeyOp]] and does not become this node.
   *
   * @param source
   *   The preceding plan node, producing `(Int, Long)` pairs.
   */
  case class CountSumByIntLongKeyOp(source: Plan[(Int, Long)]) extends Plan[(Int, Int, Long)]

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
   * int pairs, or a filter chain over one, lowers to a columnar rank split. A filter, including a
   * stack of one, sorts the survivor index and does not gather first. Over the budget, with spill
   * on, that index is spilled and dropped rows are not written. At or under the budget, and
   * whenever spill is off, the in-memory split runs and writes nothing. One int-pair project, one
   * key map, or one key or value filter immediately above that map, on that scan or filter stack
   * lowers too. Neither function is read. A null input key stays null, is not passed to the key
   * function or a key predicate, and orders as zero. A second slot, a whole-row filter above that
   * map, a second map, `mapValues`, and any other child stay on the row path. A null `Int` key
   * orders as zero and the row is retained. This matches `sortBy(_._1)` and is not a null-safe
   * ordering. A key or value filter drops a null in that column before the sort, while the row
   * path unboxes the null to `0`.
   *
   * @param source
   *   The preceding plan node.
   */
  case class SortByIntKeyOp(source: Plan[(Int, Int)]) extends Plan[(Int, Int)]

  /**
   * Sorts `(Int, Int)` pairs by column 1 descending, then column 0 ascending. This node is planned
   * like [[SortByOp]] with the key `(value, key)` and
   * `Ordering.Tuple2(Ordering.Int.reverse, Ordering.Int)`. Generic [[SortByOp]] stays opaque. A
   * scan of int pairs, or a filter chain over one, lowers to a columnar top-N when `take` asks for
   * a prefix, and to a full two-key sort when the action materializes every row. One int-pair
   * project on that scan or filter stack lowers too. One key map on that scan or filter stack
   * lowers too. One key or value filter immediately above that key map lowers too. One int
   * `mapValues` on that scan or filter stack lowers too. Neither function is read. A null input
   * key stays null, is not passed to the key function, and orders as zero. A null value stays null
   * and is not passed to `mapValues`. A second map, a slot above that project or above
   * `mapValues`, a second slot, a whole-row filter, a second slot or a whole-row filter above a
   * key map under a key sort, an aggregate, or a join under the sort stays on the row path. A
   * filter, join, aggregate, or projection above it stays on the row path. A null value or key
   * orders as zero and stays null. Ties keep input order.
   *
   * @param source
   *   The preceding plan node.
   */
  case class SortByIntValueDescOp(source: Plan[(Int, Int)]) extends Plan[(Int, Int)]

  /**
   * Sorts `(Long, Long)` pairs ascending by column 0. This node is planned like [[SortByOp]] with
   * the key taken from column 0 and [[Ordering.Long]]. Generic [[SortByOp]] stays opaque. A scan
   * of long pairs, or key and value filters over that scan, lowers to a columnar rank split. Those
   * filters sort the survivor index. A long-pair row filter does the same, including a stack of
   * one. One long-pair project on that scan or filter stack lowers too. One key or value filter
   * immediately above that project lowers too. Neither function is read. `mapValues`, a second
   * map, a second slot, a whole-row filter above that project, and any other map, aggregate, or
   * join under the sort stay on the row path, as does a filter, join, aggregate, or projection
   * above the sort. A null `Long` key orders as zero and the row is retained. This matches
   * `sortBy(_._1)` and is not a null-safe ordering. Over the budget, with spill on, a survivor
   * index is spilled and dropped rows are not written. A dense input still writes one sorted run
   * per partition and merges `spillFanIn` at a time. One input partition is still sorted in
   * memory. At or under the budget, and whenever spill is off, the in-memory split runs and writes
   * nothing.
   *
   * @param source
   *   The preceding plan node.
   */
  case class SortByLongKeyOp(source: Plan[(Long, Long)]) extends Plan[(Long, Long)]

  /**
   * Sorts `(String, Int)` pairs ascending by the effective key. A present key orders by
   * `String.compareTo` (UTF-16 code units). A null key orders as `""` and stays null. Ties keep
   * input order. This matches `sortBy(pair => if (pair._1 == null) "" else pair._1)`. Generic
   * [[SortByOp]] stays opaque. A scan of string pairs, or key and value filters over that scan,
   * lowers to a columnar rank split. Those filters sort the survivor index. A whole-row string
   * filter does the same, including a stack of one. One string-pair project on that scan or filter
   * stack lowers too. One key or value filter immediately above that project lowers too. Neither
   * function is read. `mapValues`, a second map, a second slot, a whole-row filter above that
   * project, and any other map, aggregate, or join under the sort stay on the row path, as does a
   * filter, join, aggregate, or projection above the sort. Over the budget, with spill on, a
   * survivor index is spilled and dropped rows are not written. A dense input still writes one
   * sorted run per partition and merges `spillFanIn` at a time. One input partition is still
   * sorted in memory. At or under the budget, and whenever spill is off, the in-memory split runs
   * and writes nothing.
   *
   * @param source
   *   The preceding plan node.
   */
  case class SortByStringKeyOp(source: Plan[(String, Int)]) extends Plan[(String, Int)]

  /**
   * Sorts `(Long, Long)` pairs by column 1 descending, then column 0 ascending. This node is
   * planned like [[SortByOp]] with a null-safe key `(value, key)` and
   * `Ordering.Tuple2(Ordering.Long.reverse, Ordering.Long)`. A null orders as zero and stays null.
   * A scan, filters over that scan, one long-pair project, one key or value filter above that
   * project, or one `mapValues` on that scan or filter stack lowers. Neither function is read. A
   * second map, a slot above `mapValues`, a second slot, a whole-row filter above the project, and
   * a parent above the sort stay on the row path. Ties keep input order. There is no spill.
   *
   * @param source
   *   The preceding plan node.
   */
  case class SortByLongValueDescOp(source: Plan[(Long, Long)]) extends Plan[(Long, Long)]

  /**
   * Sorts `(String, Int)` pairs by the int value descending, then the string key ascending. A
   * present key orders by `String.compareTo`. A null key orders as `""` and stays null. A null
   * value orders as zero and stays null. A scan, filters over that scan, one string-pair project,
   * one key or value filter above that project, or one `mapValues` on that scan or filter stack
   * lowers. Neither function is read. A second map, a slot above `mapValues`, a second slot, a
   * whole-row filter above the project, and a parent above the sort stay on the row path. Ties
   * keep input order. There is no spill.
   *
   * @param source
   *   The preceding plan node.
   */
  case class SortByStringValueDescOp(source: Plan[(String, Int)]) extends Plan[(String, Int)]

  /**
   * Sorts `(Int, Long)` ascending by the int key. A null key orders as zero and stays null. A null
   * long stays null. The widening, or one filter stack on it, lowers. A second map stays on the
   * row path.
   *
   * @param source
   *   The preceding plan node.
   */
  case class SortByIntLongKeyOp(source: Plan[(Int, Long)]) extends Plan[(Int, Long)]

  /**
   * Sorts `(Int, Long)` by the long value descending, then the int key ascending. A null orders as
   * zero and stays null. The widening, a filter stack on it, or one `Long => Long` map of that
   * child lowers. A filter above that map stays on the row path. Ties keep input order.
   *
   * @param source
   *   The preceding plan node.
   */
  case class SortByIntLongValueDescOp(source: Plan[(Int, Long)]) extends Plan[(Int, Long)]

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
   *   join. A null value on an int shuffle-hash, broadcast, or sort-merge stays columnar. A long
   *   or string left outer join still declines a null value. An explicit sort-merge hint on int
   *   pairs lowers to `SortMergeLeftOuter`. `(Long, Long)` pairs lower to `LongHashLeftOuter`,
   *   `LongBroadcastLeftOuter`, or `LongSortMergeLeftOuter` for the same hints. `(String, Int)`
   *   pairs lower to `StringHashLeftOuter`, `StringBroadcastLeftOuter`, or
   *   `StringSortMergeLeftOuter` for the same hints.
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
   *   join. A null value on an int shuffle-hash, broadcast, or sort-merge stays columnar. A long
   *   or string right outer join still declines a null value. An explicit sort-merge hint on int
   *   pairs lowers to `SortMergeRightOuter`. `(Long, Long)` pairs lower to `LongHashRightOuter`,
   *   `LongBroadcastRightOuter`, or `LongSortMergeRightOuter` for the same hints. `(String, Int)`
   *   pairs lower to `StringHashRightOuter`, `StringBroadcastRightOuter`, or
   *   `StringSortMergeRightOuter` for the same hints.
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
   *   left input is the probe side. An absent hint uses the shuffle-hash op. A null value on an
   *   int full outer join stays a clear validity bit. A long or string full outer join still
   *   declines a null value. An explicit sort-merge hint on int pairs lowers to
   *   `SortMergeFullOuter`. `(Long, Long)` pairs lower to `LongHashFullOuter`,
   *   `LongBroadcastFullOuter`, or `LongSortMergeFullOuter` for the same hints. `(String, Int)`
   *   pairs lower to `StringHashFullOuter`, `StringBroadcastFullOuter`, or
   *   `StringSortMergeFullOuter` for the same hints.
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

  /**
   * Min or max of `(Int, Int)` pairs by key.
   *
   * A null key is dropped. A null value is skipped. A key whose values are all null emits no row.
   * `minimum` selects the min. One `(key, value)` row is published per distinct key that has a
   * present value. This is a typed witness. A generic `groupByKey` followed by `map` stays
   * [[GroupByKeyOp]] and does not become this node.
   *
   * @param source
   *   The preceding plan node, producing `(Int, Int)` pairs.
   * @param minimum
   *   True for the minimum value, false for the maximum.
   */
  case class ExtremumByKeyOp(source: Plan[(Int, Int)], minimum: Boolean) extends Plan[(Int, Int)]

  /**
   * Filter one column of a wide row.
   *
   * A null in that column is dropped and is not compared. A null in another column stays. The
   * compare has to be one the closed set allows for that column. A compare it does not name stays
   * on the row path.
   *
   * @param source
   *   The preceding wide-row plan.
   * @param ordinal
   *   Column to test.
   * @param compare
   *   Comparison against a constant.
   */
  case class WideFilterOp(source: Plan[WideRow], ordinal: Int, compare: ColumnCompare)
      extends Plan[WideRow]

  /**
   * Drop, reorder, or rename columns of a wide row. This does not compute a new column.
   *
   * @param source
   *   The preceding wide-row plan.
   * @param spec
   *   Output order. Each pair is a source ordinal and the output name.
   */
  case class WideProjectOp(source: Plan[WideRow], spec: Vector[(Int, String)])
      extends Plan[WideRow]

  /**
   * Appends one computed column. Arithmetic uses the wider numeric input. A compare yields bool.
   * Null in an input yields null. Divide and strings are not in this set.
   *
   * @param source
   *   The preceding wide-row plan.
   * @param expr
   *   Closed expression over source ordinals.
   * @param name
   *   Name of the appended column.
   */
  case class WideComputeOp(source: Plan[WideRow], expr: ColumnExpr, name: String)
      extends Plan[WideRow]
}
