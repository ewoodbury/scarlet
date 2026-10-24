package com.ewoodbury.scarlet.api

import com.ewoodbury.scarlet.core.{ExecutionService, Partition, Plan}

/**
 * A lazy, immutable representation of a "distributed" collection. Operations build up a Plan,
 * execution is deferred until an action is called.
 *
 * This is the primary user-facing API for Scarlet, providing familiar transformation and action
 * operations for distributed data processing.
 *
 * @param plan
 *   The logical plan defining how to compute this collection.
 * @tparam A
 *   The type of elements in the collection.
 */
final case class DistCollection[A](plan: Plan[A]):

  // Initialize execution service on first use
  DistCollection.ensureExecutionServiceInitialized()

  // --- Helper to view the plan ---
  override def toString: String = s"DistCollection(plan = $plan)"

  /**
   * The single erasure point for key-value plan refinement. `Plan` is invariant, so the
   * `A =:= (K, V)` evidence cannot be applied substitutionally and the cast is required; it is
   * safe because every caller provides that evidence.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def kvPlan[K, V](using ev: A =:= (K, V)): Plan[(K, V)] =
    this.plan.asInstanceOf[Plan[(K, V)]]

  // --- Basic Transformations ---

  /**
   * Transformation: Applies a mapping function lazily. Returns a new DistCollection representing
   * the result of the map. Does not trigger computation.
   */
  def map[B](f: A => B): DistCollection[B] =
    DistCollection(Plan.MapOp(this.plan, f))

  /**
   * Transformation: Applies a filter predicate lazily. Returns a new DistCollection representing
   * the filtered result. Does not trigger computation.
   */
  def filter(p: A => Boolean): DistCollection[A] =
    DistCollection(Plan.FilterOp(this.plan, p))

  /**
   * Filters strings equal to `value`. This explicit form is recognized by the columnar planner,
   * which compares dictionary codes. A null `value` is not recognized and stays on the scalar
   * predicate. A regular `filter(_ == value)` remains a scalar predicate.
   */
  def filterStringEquals(value: String)(using ev: A =:= String): DistCollection[A] =
    filterStringLiterals(Vector(value))

  /**
   * Filters strings contained in a literal list. Lists of one to eight non-null strings are
   * recognized by the columnar planner and compared as dictionary codes. Other list sizes and
   * lists containing null use the scalar predicate path.
   */
  def filterStringIn(values: String*)(using ev: A =:= String): DistCollection[A] =
    filterStringLiterals(values.toVector)

  private def filterStringLiterals(values: Vector[String])(using
      ev: A =:= String,
  ): DistCollection[A] =
    val predicate = new DictionaryStringPredicate[A](values, ev)
    DistCollection(Plan.FilterOp(this.plan, predicate))

  /**
   * Transformation: Applies a flatMap function lazily. Returns a new DistCollection representing
   * the result of the flatMap. Does not trigger computation.
   */
  def flatMap[B](f: A => IterableOnce[B]): DistCollection[B] =
    DistCollection(Plan.FlatMapOp(this.plan, f))

  /**
   * Transformation: Applies a function to each partition as an iterator-in/iterator-out
   * transformation. Returns a new DistCollection representing the result. Does not trigger
   * computation.
   */
  def mapPartitions[B](f: Iterator[A] => Iterator[B]): DistCollection[B] =
    DistCollection(Plan.MapPartitionsOp(this.plan, f))

  /**
   * Transformation: Returns a new DistCollection with distinct elements. Does not trigger
   * computation.
   */
  def distinct(): DistCollection[A] =
    DistCollection(Plan.DistinctOp(this.plan))

  /**
   * Transformation: Returns a new DistCollection with the elements of two collections. Does not
   * trigger computation.
   */
  def union(other: DistCollection[A]): DistCollection[A] =
    DistCollection(Plan.UnionOp(this.plan, other.plan))

  /**
   * Transformation: Rebalances data across exactly `numPartitions` partitions (shuffle-based).
   * Returns a new DistCollection. Does not trigger computation.
   */
  def repartition(numPartitions: Int): DistCollection[A] =
    DistCollection(Plan.RepartitionOp(this.plan, numPartitions))

  /**
   * Transformation: Coalesces data into `numPartitions` partitions. Currently implemented via a
   * shuffle for simplicity. Returns a new DistCollection. Does not trigger computation.
   */
  def coalesce(numPartitions: Int): DistCollection[A] =
    DistCollection(Plan.CoalesceOp(this.plan, numPartitions))

  /**
   * Transformation: Explicitly partitions a key-value dataset by key using the runtime's
   * configured partitioner into `numPartitions` partitions.
   */
  def partitionBy[K, V](numPartitions: Int)(using ev: A =:= (K, V)): DistCollection[(K, V)] =
    DistCollection(Plan.PartitionByOp(kvPlan, numPartitions))

  // --- Key-Value Transformations ---

  /**
   * Transformation: Extracts the keys from the elements in the collection. Returns a new
   * DistCollection representing the keys. Does not trigger computation.
   */
  def keys[K, V](using ev: A =:= (K, V)): DistCollection[K] =
    DistCollection(Plan.KeysOp(kvPlan))

  /**
   * Transformation: Extracts the values from the elements in the collection. Returns a new
   * DistCollection representing the values. Does not trigger computation.
   */
  def values[K, V](using ev: A =:= (K, V)): DistCollection[V] =
    DistCollection(Plan.ValuesOp(kvPlan))

  /**
   * Transformation: Applies a function to the values of the elements in the collection. Returns a
   * new DistCollection representing the result of the map. Does not trigger computation.
   */
  def mapValues[K, V, B](f: V => B)(using ev: A =:= (K, V)): DistCollection[(K, B)] =
    DistCollection(Plan.MapValuesOp(kvPlan, f))

  /**
   * Transformation: Filters the elements of the collection by the keys. Returns a new
   * DistCollection representing the filtered result. Does not trigger computation.
   */
  def filterKeys[K, V](p: K => Boolean)(using ev: A =:= (K, V)): DistCollection[(K, V)] =
    DistCollection(Plan.FilterKeysOp(kvPlan, p))

  /**
   * Transformation: Filters the elements of the collection by the values. Returns a new
   * DistCollection representing the filtered result. Does not trigger computation.
   */
  def filterValues[K, V](p: V => Boolean)(using ev: A =:= (K, V)): DistCollection[(K, V)] =
    DistCollection(Plan.FilterValuesOp(kvPlan, p))

  /**
   * Filters an int join result by its key. This explicit form can filter the three join columns
   * without constructing a `(Int, (Int, Int))` tuple for each row. A general `filter` remains on
   * the row path because its predicate accepts that tuple.
   */
  def filterJoinKey(p: Int => Boolean)(using
      ev: A =:= (Int, (Int, Int)),
  ): DistCollection[A] =
    filterJoinColumn(p, IntJoinTriplePredicate.Slot.Key)

  /** Filters an int join result by the left value without constructing a row tuple. */
  def filterJoinLeft(p: Int => Boolean)(using
      ev: A =:= (Int, (Int, Int)),
  ): DistCollection[A] =
    filterJoinColumn(p, IntJoinTriplePredicate.Slot.LeftValue)

  /** Filters an int join result by the right value without constructing a row tuple. */
  def filterJoinRight(p: Int => Boolean)(using
      ev: A =:= (Int, (Int, Int)),
  ): DistCollection[A] =
    filterJoinColumn(p, IntJoinTriplePredicate.Slot.RightValue)

  private def filterJoinColumn(
      predicate: Int => Boolean,
      slot: IntJoinTriplePredicate.Slot,
  )(using ev: A =:= (Int, (Int, Int))): DistCollection[A] =
    val triplePredicate = new IntJoinTriplePredicate[A]:
      val selectedSlot: IntJoinTriplePredicate.Slot = slot
      def keep(value: Int): Boolean = predicate(value)
      def apply(value: A): Boolean =
        val (key, (left, right)) = ev(value)
        selectedSlot match
          case IntJoinTriplePredicate.Slot.Key => keep(key)
          case IntJoinTriplePredicate.Slot.LeftValue => keep(left)
          case IntJoinTriplePredicate.Slot.RightValue => keep(right)
    DistCollection(Plan.FilterOp(plan, triplePredicate))

  /**
   * Transformation: Applies a function to the values of the elements in the collection. Returns a
   * new DistCollection representing the result of the flatMap. Does not trigger computation.
   */
  def flatMapValues[K, V, B](f: V => IterableOnce[B])(using
      ev: A =:= (K, V),
  ): DistCollection[(K, B)] =
    DistCollection(Plan.FlatMapValuesOp(kvPlan, f))

  // --- Wide Transformations (require shuffles) ---

  /**
   * Transformation: Groups values by key, requiring a shuffle operation. Returns a new
   * DistCollection representing the grouped data. Does not trigger computation.
   */
  def groupByKey[K, V](using ev: A =:= (K, V)): DistCollection[(K, Iterable[V])] =
    DistCollection(Plan.GroupByKeyOp(kvPlan))

  /**
   * Transformation: Reduces values by key using the provided function, requiring a shuffle
   * operation. Returns a new DistCollection representing the reduced data. Does not trigger
   * computation.
   *
   * `op` must be associative and commutative. Scarlet may combine values within each mapper
   * partition before the shuffle and again after; applying `op` in a different grouping must not
   * change the result.
   */
  def reduceByKey[K, V](op: (V, V) => V)(using ev: A =:= (K, V)): DistCollection[(K, V)] =
    DistCollection(Plan.ReduceByKeyOp(kvPlan, op))

  /**
   * Transformation: Sorts the collection by the specified key function, requiring a shuffle
   * operation. Returns a new DistCollection representing the sorted data. Does not trigger
   * computation.
   */
  def sortBy[B](keyFunc: A => B)(using ordering: Ordering[B]): DistCollection[A] =
    DistCollection(Plan.SortByOp(this.plan, keyFunc, ordering))

  /**
   * Transformation: Joins this collection with another collection by key, requiring a shuffle
   * operation. Returns a new DistCollection representing the joined data. Does not trigger
   * computation.
   */
  def join[K, V, W](other: DistCollection[(K, W)])(using
      ev: A =:= (K, V),
  ): DistCollection[(K, (V, W))] =
    DistCollection(Plan.JoinOp(kvPlan, other.plan, None))

  /**
   * Transformation: Joins this collection with another collection by key using shuffle-hash join.
   * Returns a new DistCollection representing the joined data. Does not trigger computation.
   */
  def shuffleHashJoin[K, V, W](other: DistCollection[(K, W)])(using
      ev: A =:= (K, V),
  ): DistCollection[(K, (V, W))] =
    DistCollection(
      Plan.JoinOp(
        kvPlan,
        other.plan,
        Some(Plan.JoinStrategy.ShuffleHash),
      ),
    )

  /**
   * Transformation: Joins this collection with another collection by key using sort-merge join.
   * Returns a new DistCollection representing the joined data. Does not trigger computation.
   */
  def sortMergeJoin[K, V, W](other: DistCollection[(K, W)])(using
      ev: A =:= (K, V),
  ): DistCollection[(K, (V, W))] =
    DistCollection(
      Plan.JoinOp(
        kvPlan,
        other.plan,
        Some(Plan.JoinStrategy.SortMerge),
      ),
    )

  /**
   * Transformation: Joins this collection with another collection by key using broadcast-hash
   * join. Returns a new DistCollection representing the joined data. Does not trigger computation.
   */
  def broadcastJoin[K, V, W](other: DistCollection[(K, W)])(using
      ev: A =:= (K, V),
  ): DistCollection[(K, (V, W))] =
    DistCollection(
      Plan.JoinOp(
        kvPlan,
        other.plan,
        Some(Plan.JoinStrategy.Broadcast),
      ),
    )

  /**
   * Transformation: Co-groups this collection with another collection by key, requiring a shuffle
   * operation. Returns a new DistCollection representing the co-grouped data. Does not trigger
   * computation.
   */
  def cogroup[K, V, W](other: DistCollection[(K, W)])(using
      ev: A =:= (K, V),
  ): DistCollection[(K, (Iterable[V], Iterable[W]))] =
    DistCollection(Plan.CoGroupOp(kvPlan, other.plan))

  // --- Actions ---

  /**
   * Action: Executes the plan and returns the results as a single local Iterable. This triggers
   * the computation using the registered ExecutionService.
   */
  def collect(): Iterable[A] =
    ExecutionService.get.execute(this.plan)

  /**
   * Action: Executes the plan and returns the count of elements. This triggers computation. A
   * columnar plan counts column lengths and does not build a `Seq`.
   */
  def count(): Long =
    ExecutionService.get.count(this.plan)

  /**
   * Action: Sums an `Int` collection as a `Long`, a `Long` collection as a `Long`, or a `Double`
   * collection as a `Double`. Empty input returns zero. An `Int` sum does not wrap. A `Long` sum
   * wraps. A columnar plan adds the finished column and does not build a `Seq`.
   */
  def sum[R](using result: DistCollection.SumOf[A, R]): R =
    result(this.plan)

  /**
   * Action: Sums `(Int, Int)` values by key as a `Long` and folds those group sums into one
   * `Long`. Empty input returns zero. A null component drops the row before a preceding filter or
   * pair map. Any other cell type fails. The total does not wrap at `Int`. A source of int pairs
   * runs the wide kernel and does not build a `Seq`. A preceding `filter`, `filterKeys`,
   * `filterValues`, or pair `map` runs inside that scan and does not build the filtered or
   * projected pairs. Primitive source descriptors provide the type for empty sources. Any other
   * shape sums through the row engine. `reduceByKey` still applies its `(Int, Int) => Int`
   * operator.
   */
  def groupedSum(using ev: A =:= (Int, Int)): Long =
    ExecutionService.get.groupedSum(kvPlan)

  /**
   * Action: Executes the plan and returns the first n elements. This triggers computation.
   */
  def take(n: Int): List[A] =
    ExecutionService.get.take(this.plan, n).toList

  /**
   * Action: Executes the plan and returns the first element. This triggers computation.
   */
  def first(): A =
    take(1).headOption.getOrElse(throw new NoSuchElementException("Collection is empty"))

  /**
   * Action: Executes the plan and reduces all elements using the given function. This triggers
   * computation.
   */
  def reduce(op: (A, A) => A): A =
    collect().reduceOption(op).getOrElse(throw new NoSuchElementException("Collection is empty"))

  /**
   * Action: Executes the plan and folds all elements using the given initial value and function.
   * This triggers computation.
   */
  def fold(initial: A)(op: (A, A) => A): A = collect().fold(initial)(op)

  /**
   * Action: Executes the plan and aggregates the elements using the given functions. This triggers
   * computation. `seqOp` is applied per partition starting from `zero`; `combOp` combines the
   * per-partition accumulators. Empty input returns `zero` without invoking either function.
   */
  def aggregate[B](zero: B)(seqOp: (B, A) => B, combOp: (B, B) => B): B = {
    val partitions = ExecutionService.get.executePartitions(this.plan)
    if (partitions.forall(_.data.isEmpty)) zero
    else
      partitions
        .map(partition => partition.data.foldLeft(zero)(seqOp))
        .reduceOption(combOp)
        .getOrElse(zero)
  }

  /**
   * Action: Executes the plan and applies the given function to each element. This triggers
   * computation.
   */
  def foreach(f: A => Unit): Unit = collect().foreach(f)

end DistCollection

/**
 * String literal predicate. [[literals]] is the membership source: [[apply]] is final and tests an
 * immutable set built from that vector, so the planner and the row path cannot disagree. The
 * planner reads [[literals]] and does not call [[apply]].
 */
private[scarlet] final class DictionaryStringPredicate[A](
    val literals: Vector[String],
    ev: A =:= String,
) extends (A => Boolean):

  private val membership: Set[String] = literals.toSet

  final def apply(value: A): Boolean = membership.contains(ev(value))

/** Internal marker for a tuple-free predicate over one int join column. */
private[scarlet] trait IntJoinTriplePredicate[A] extends (A => Boolean):
  def selectedSlot: IntJoinTriplePredicate.Slot
  def keep(value: Int): Boolean

private[scarlet] object IntJoinTriplePredicate:
  enum Slot:
    case Key, LeftValue, RightValue

// Companion object for creating a DistCollection from a source
object DistCollection:

  /** Sum of an `Int` or `Long` column as a `Long`, or a `Double` column as a `Double`. */
  trait SumOf[A, R]:
    def apply(plan: Plan[A]): R

  object SumOf:
    given SumOf[Int, Long] with
      def apply(plan: Plan[Int]): Long = ExecutionService.get.sumInt(plan)

    given SumOf[Long, Long] with
      def apply(plan: Plan[Long]): Long = ExecutionService.get.sumLong(plan)

    given SumOf[Double, Double] with
      def apply(plan: Plan[Double]): Double = ExecutionService.get.sumDouble(plan)

  // Lazy initialization to ensure execution service is available
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var initialized = false

  private def ensureExecutionServiceInitialized(): Unit = {
    if (!initialized) {
      synchronized {
        if (!initialized) {
          // Try to initialize the execution module if available on classpath
          try {
            val clazz = Class.forName("com.ewoodbury.scarlet.execution.ExecutionModuleInit")
            val method = clazz.getMethod("initialize")
            method.invoke(null)
          } catch {
            case _: ClassNotFoundException =>
              // Execution module not on classpath - user must provide their own ExecutionService
              ()
            case _: Exception =>
              // Other initialization errors - log but continue
              ()
          }
          initialized = true
        }
      }
    }
  }

  /**
   * Creates a DistCollection from an existing Iterable data source, splitting it into exactly
   * `numPartitions` partitions. Records are distributed deterministically: input order is
   * preserved within each partition, and earlier records land in earlier partitions. Empty inputs
   * produce exactly `numPartitions` empty partitions.
   *
   * @param data
   *   The source data.
   * @param numPartitions
   *   The desired number of partitions; must be positive.
   * @return
   *   A new DistCollection.
   */
  def apply[A](data: Iterable[A], numPartitions: Int): DistCollection[A] =
    require(numPartitions > 0, "Number of partitions must be positive.")
    val elements = data.toVector
    splitSource(elements.length, numPartitions, Plan.SourceDescriptor.Unknown)((start, end) =>
      elements.slice(start, end),
    )

  /**
   * Partitions a primitive `Int` array. Each partition is an
   * [[com.ewoodbury.scarlet.core.IntArrays]] copy of its range. The caller's array is not
   * retained, and the partition array is not publicly mutable.
   */
  def ofInts(values: Array[Int], numPartitions: Int): DistCollection[Int] =
    import com.ewoodbury.scarlet.core.IntArrays

    splitSource(values.length, numPartitions, Plan.SourceDescriptor.Ints) { (start, end) =>
      IntArrays.fromOwned(values.slice(start, end))
    }

  /**
   * Partitions a primitive `Long` array. Each partition is a
   * [[com.ewoodbury.scarlet.core.LongArrays]] copy of its range. The caller's array is not
   * retained, and the partition array is not publicly mutable.
   */
  def ofLongs(values: Array[Long], numPartitions: Int): DistCollection[Long] =
    import com.ewoodbury.scarlet.core.LongArrays

    splitSource(values.length, numPartitions, Plan.SourceDescriptor.Longs) { (start, end) =>
      LongArrays.fromOwned(values.slice(start, end))
    }

  /**
   * Partitions a primitive `Boolean` array. Each partition is a
   * [[com.ewoodbury.scarlet.core.BoolArrays]] copy of its range. The caller's array is not
   * retained, and the partition array is not publicly mutable.
   */
  def ofBools(values: Array[Boolean], numPartitions: Int): DistCollection[Boolean] =
    import com.ewoodbury.scarlet.core.BoolArrays

    splitSource(values.length, numPartitions, Plan.SourceDescriptor.Bools) { (start, end) =>
      BoolArrays.fromOwned(values.slice(start, end))
    }

  /**
   * Partitions a `String` array. Each partition is a [[com.ewoodbury.scarlet.core.StringArrays]]
   * copy of its range. The caller's array is not retained, and the partition array is not publicly
   * mutable. The source descriptor is
   * [[com.ewoodbury.scarlet.core.Plan.SourceDescriptor.Strings]], including when the array is
   * empty. [[apply]] on an `Iterable[String]` stays
   * [[com.ewoodbury.scarlet.core.Plan.SourceDescriptor.Unknown]].
   */
  def ofStrings(values: Array[String], numPartitions: Int): DistCollection[String] =
    import com.ewoodbury.scarlet.core.StringArrays

    splitSource(values.length, numPartitions, Plan.SourceDescriptor.Strings) { (start, end) =>
      StringArrays.fromOwned(values.slice(start, end))
    }

  /**
   * Partitions a primitive `Double` array. Each partition is a
   * [[com.ewoodbury.scarlet.core.DoubleArrays]] copy of its range. The caller's array is not
   * retained, and the partition array is not publicly mutable.
   */
  def ofDoubles(values: Array[Double], numPartitions: Int): DistCollection[Double] =
    import com.ewoodbury.scarlet.core.DoubleArrays

    splitSource(values.length, numPartitions, Plan.SourceDescriptor.Doubles) { (start, end) =>
      DoubleArrays.fromOwned(values.slice(start, end))
    }

  /**
   * Partitions primitive `(Int, Int)` keys and values. Each partition is an
   * [[com.ewoodbury.scarlet.core.IntPairArrays]] copy of its range and does not expose those
   * arrays.
   */
  def ofIntPairs(
      keys: Array[Int],
      values: Array[Int],
      numPartitions: Int,
  ): DistCollection[(Int, Int)] =
    import com.ewoodbury.scarlet.core.IntPairArrays

    IntPairArrays.requireSameLength(keys, values)
    splitSource(keys.length, numPartitions, Plan.SourceDescriptor.Pairs) { (start, end) =>
      IntPairArrays.fromOwned(keys.slice(start, end), values.slice(start, end))
    }

  /**
   * Splits `length` into exactly `numPartitions` contiguous ranges, preserving order. The first
   * `length % numPartitions` ranges receive one extra index. `numPartitions` must be positive.
   */
  private def splitSource[A](
      length: Int,
      numPartitions: Int,
      descriptor: Plan.SourceDescriptor,
  )(
      slice: (Int, Int) => Iterable[A],
  ): DistCollection[A] =
    require(numPartitions > 0, "Number of partitions must be positive.")
    val baseSize = length / numPartitions
    val remainder = length % numPartitions
    val boundaries =
      Seq.tabulate(numPartitions + 1)(index => index * baseSize + math.min(index, remainder))
    val partitions = boundaries.zip(boundaries.drop(1)).map { case (start, end) =>
      Partition(slice(start, end))
    }
    DistCollection(Plan.Source(partitions, descriptor))
end DistCollection
