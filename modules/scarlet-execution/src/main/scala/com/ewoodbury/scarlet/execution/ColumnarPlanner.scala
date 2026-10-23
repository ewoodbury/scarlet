package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.api.{DictionaryStringPredicate, IntJoinTriplePredicate}
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}

/**
 * Lowers `Int`, `Double`, `(Int, Int)`, and `String` plans to [[PhysicalOp]] and runs that tree on
 * the columnar kernels.
 *
 * Named erasure boundary. `Plan` is invariant and its element type is erased, so non-empty sources
 * are classified by their values and checked against any descriptor, while empty sources use their
 * descriptor. An element map lowers only when it declares the column's signature and every other
 * `apply` is an erasure bridge: `apply` or `apply$mcII$sp` for `Int => Int`, `apply` or
 * `apply$mcDD$sp` for `Double => Double`, and `apply` for `String => String`. The check reads
 * method descriptors and does not call the function. One failing project refuses the whole element
 * chain, so a type-changing map stays on the row path and is not run before that fallback.
 * `getDeclaredMethods` misses an inherited `apply`, so that function stays on the row path too.
 * Other user functions are cast to the selected operation. A result that is not that type throws
 * `ClassCastException`. [[tryRun]] catches it and returns `None`, and the row path runs the whole
 * plan. Any unsupported node (union, flatMap, distinct, sort, a map that is not fed by `Int`,
 * `Double`, or `String`, and the rest) makes the whole plan ineligible, so a supported prefix is
 * not executed on its own.
 *
 * [[lower]] and interpretation walk the same logical plan. The physical node records the operator
 * and not the function, so each logical node is matched to the node [[lower]] built for it. A
 * mismatch is a planner bug: interpretation throws `IllegalStateException`, and [[tryRun]] does
 * not turn that into a row-path fallback. A `ClassCastException` from a filter, or from a string
 * map that declares `String` and returns another value, is still a fallback, and that function may
 * already have run.
 *
 * [[lower]] does not call user functions. A straight `Int`, `Double`, or `String` filter/project
 * run of two to four steps is one [[PhysicalOp.Pipeline]]. A longer run is cut into those chunks,
 * source order first. One step stays a [[PhysicalOp.Filter]] or [[PhysicalOp.Project]]. Pair
 * filters, aggregates, and joins stay one node per operator. Narrow map and filter keep input
 * order and the input partition count. `Double` and `String` select filter and map only. A filter
 * over an int join result is columnar only for explicit key, left-value, and right-value filters;
 * general tuple predicates stay on the row path. `keys` and `values` of an int pair are that
 * column. The arrays are shared: a published column is not mutated, so a view is cheaper than a
 * copy and still correct. `mapValues` lowers only when the function declares an unboxed `Int`
 * result and every other `apply` is an erasure bridge. A wider primitive, a reference result, or
 * an overloaded `apply` stays on the row path. That check reads the method descriptor and does not
 * call the function. `getDeclaredMethods` does not see an inherited `apply`, so that function
 * stays on the row path too. `reduceByKey` applies the user's function and wraps at `Int`. A
 * `join` with no hint, or with an explicit shuffle-hash hint, hash-partitions both sides and
 * probes one bucket at a time. Sort-merge and broadcast are not implemented: those hints make
 * `lower` return `None`, so the plan stays on the row path. Output width for an accepted join or
 * aggregate is `defaultShufflePartitions`. [[tryCount]] and [[trySumInt]] fold an int filter/map
 * chain that starts at a scan or at `keys` or `values`, including an int `mapValues`, and do not
 * write that chain. Sum folds one trailing project and chains longer than four. Count folds those
 * and chains of two to four steps. [[trySumDouble]] folds at most four steps. Sum of `keys`,
 * `values`, or an int `mapValues` with no further element chain reads the pair column in that
 * morsel loop and does not build the projection. [[tryGroupedSum]] runs
 * [[ColumnPipeline.sumByKey]] on a typed pair source and folds the `Int64` group sums. A preceding
 * `FilterOp`, `filterKeys`, `filterValues`, or pair `MapOp` is applied inside that partial scan,
 * so the action does not build those pairs. [[groupedSumPeel]] labels those walked steps. It is
 * not an action shape and not a physical node. A source descriptor supplies the type when all
 * partitions are empty. It does not decode. `collect` still does. A pair `MapOp` stays on the row
 * path for `collect` and `reduceByKey`, and [[partitioning]] reports no layout for it.
 *
 * An [[com.ewoodbury.scarlet.core.IntArrays]], [[com.ewoodbury.scarlet.core.DoubleArrays]], or
 * [[com.ewoodbury.scarlet.core.IntPairArrays]] source is copied into the column on the calling
 * thread. The partition stays readable, so the column does not adopt its array. A boxed `Vector`
 * or `List` is still read through its iterator. An `ArraySeq` passed straight to the codec is
 * copied the same way.
 *
 * A null string is a value. Classification skips a leading null and keeps looking for a `String`.
 * An all-null source stays on the row path, because there is no witness. An accepted string plan
 * records [[Layout.Utf8Dict]]: that dictionary plus codes, one dictionary per batch. The string
 * lambda stays scalar over the decoded string. Int, double, int-pair, and int-triple plans stay
 * [[Layout.Columnar]].
 *
 * `take` has a separate bounded path for accepted partition-preserving shapes: it interprets one
 * source partition at a time in order and decodes only the remaining requested values. Plans with
 * an exchange or aggregate stay on the row path because their output cannot be produced with this
 * narrow short-circuit.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw",
  ),
)
object ColumnarPlanner:

  /**
   * Layout of an accepted plan, without running it. A plan is accepted when [[lower]] returns a
   * physical tree and that tree's layout is valid. `None` means the plan is not accepted, which is
   * the row path. There is no second layout source. A string filter or map reports
   * [[Layout.Utf8Dict]]. An int, double, or int-pair plan reports [[Layout.Columnar]]. A pair map,
   * including one that [[tryGroupedSum]] fuses and one that changes `(Int, Int)` to another pair
   * type, is not accepted, so this is `None`.
   */
  def partitioning[A](plan: Plan[A]): Option[PartitioningInfo] =
    accepted(plan).map(infoOf)

  /**
   * The physical tree for `plan`, without running it. `None` means the row path, including an
   * element chain whose project does not declare the column's type. A join lowers when its hint is
   * absent or shuffle-hash. Sort-merge and broadcast return `None`. [[partitioning]] and
   * [[tryRun]] follow that refusal. They also drop a lowered tree whose layout is invalid.
   */
  def lower(plan: Plan[_]): Option[PhysicalOp] =
    if (!ScarletConf.get.columnarExecution) then None
    else lowerPlan(plan)

  /**
   * Run `plan` on the columnar path. `None` means the caller should use the row path. The single
   * result cast is safe when selection succeeded: the decoded partitions have the plan's element
   * type.
   */
  def tryRun[A](plan: Plan[A]): Option[Seq[Partition[A]]] =
    accepted(plan) match
      case None => None
      case Some(op) =>
        try Some(decode(interpret(plan, op)).asInstanceOf[Seq[Partition[A]]])
        catch case _: ClassCastException => None

  /**
   * Run a partition-preserving accepted plan only until its first `n` results are available. Plans
   * containing an exchange or aggregate are refused because producing their first output requires
   * the full upstream input.
   */
  def tryTake[A](plan: Plan[A], n: Int): Option[Seq[A]] =
    if (n <= 0) then Some(Seq.empty[A])
    else
      accepted(plan).flatMap(op => preservedPartitions(op).map(count => (op, count))) match
        case None => None
        case Some((op, count)) =>
          try Some(prefixOf(plan, op, count, n).asInstanceOf[Seq[A]])
          catch case _: ClassCastException => None

  /**
   * First `n` values in partition order. Decoding stops once `n` values are in hand, and every
   * batch of a partition contributes before the next partition is read. Each returned chunk is
   * appended once, in order.
   */
  private def prefixOf(plan: Plan[_], op: PhysicalOp, count: Int, n: Int): Seq[Any] =
    takeChunks(
      n,
      Iterator.range(0, count).map { index => (remaining: Int) =>
        decodePrefix(interpret(plan, op, Some(index)), remaining)
      },
    )

  /** Append chunks in order and stop once `limit` elements are stored. */
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  private def takeChunks[A](limit: Int, producers: Iterator[Int => Seq[A]]): Seq[A] =
    val acc = List.newBuilder[A]

    @tailrec
    def read(taken: Int): Unit =
      if (taken < limit && producers.hasNext) then
        val produce = producers.next()
        val chunk = produce(limit - taken)
        acc ++= chunk
        read(taken + chunk.length)

    read(0)
    acc.result()

  /**
   * Row count of a columnar plan. `None` means the row path. This reads batch lengths and does not
   * decode.
   */
  def tryCount(plan: Plan[_]): Option[Long] =
    accepted(plan) match
      case None => None
      case Some(op) =>
        try TrailingProject.count(plan, op).orElse(finishedCount(plan, op))
        catch case _: ClassCastException => None

  private def finishedCount(plan: Plan[_], op: PhysicalOp): Option[Long] =
    Some(ColumnReduce.count(batchesOf(interpret(plan, op))))

  /**
   * Wide sum of a finished int column. `None` means the row path, including a plan whose result is
   * not an int column. Null slots are skipped. This does not decode.
   */
  def trySumInt(plan: Plan[_]): Option[Long] =
    accepted(plan) match
      case Some(op) if isInt(op) =>
        try Some(sumInt(plan, op))
        catch case _: ClassCastException => None
      case _ => None

  /**
   * Sum of a double plan. `None` means the row path, including a plan whose result is not a double
   * column. A filter/project chain adds in the fused loop and does not write that chain's output
   * column. Null slots are skipped.
   */
  def trySumDouble(plan: Plan[_]): Option[Double] =
    accepted(plan) match
      case Some(op) if isFloat(op) =>
        try Some(sumDouble(plan, op))
        catch case _: ClassCastException => None
      case _ => None

  /**
   * Folded wide sum of int pairs. `None` means the row path. A typed pair source runs
   * [[ColumnPipeline.sumByKey]]. A preceding row, key, or value filter, or a map that stays a
   * pair, is compiled into `keep`/`map` or [[com.ewoodbury.scarlet.columnar.PairStep]]s and
   * applied inside the partial `sumInt32`. A source descriptor supplies the type for empty
   * partitions. A null component drops the row before those steps. A 2-tuple of `Int` or null
   * still witnesses a pair, so a leading null does not refuse the encode. Any other cell type
   * fails. This does not decode. A mistyped function throws `ClassCastException` and is refused
   * after any rows it already touched. Any other shape is refused. A pair map that stays
   * `(Int, Int)` is the case [[lower]] refuses while this still succeeds. There is no layout for
   * that map. [[groupedSumPeel]] labels the walked steps and is not this action. A map to another
   * pair type, such as `(Long, Int)`, throws `ClassCastException` and returns `None`.
   */
  def tryGroupedSum(plan: Plan[_]): Option[Long] =
    if (!ScarletConf.get.columnarExecution) then None
    else
      val (base, steps) = peelPairOps(plan)
      base match
        case source: Plan.Source[_] =>
          try runGroupedSum(source, steps)
          catch case _: ClassCastException => None
        case _ => None

  private def sumInt(plan: Plan[_], op: PhysicalOp): Long =
    TrailingProject.sum(plan, op).getOrElse(matchedSumInt(plan, op))

  private def matchedSumInt(plan: Plan[_], op: PhysicalOp): Long = (plan, op) match
    case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Ints, _)) =>
      ColumnReduce.sumInt32(encodeInts(partitions))
    case (Plan.FilterOp(source, predicate), PhysicalOp.Filter(child, PhysicalOp.Slot.Element))
        if isInt(child) =>
      val keep = predicate.asInstanceOf[Int => Boolean]
      foldLong(batchesOf(interpret(source, child))) { batch =>
        Int32Pipeline.sumFiltered(batch, value => keep(value))
      }
    case (logical, PhysicalOp.Pipeline(child, length)) if isInt(child) =>
      val (source, steps) = elementSteps(logical, length)
      if (steps.length < length) then
        throw new IllegalStateException("pipeline does not match logical element steps")
      foldLong(batchesOf(interpret(source, child))) { batch =>
        ElementSum.sumInts(batch, steps)
      }
    case (
          Plan.ValuesOp(Plan.MapValuesOp(source, mapper)),
          PhysicalOp.Column(PhysicalOp.MapValue(child), 1),
        ) =>
      // Named erasure boundary. The descriptor check verified the declared `Int => Int` shape
      // before lowering, so this cast is safe.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val fn = mapper.asInstanceOf[Int => Int]
      foldLong(pairsOf(interpret(source, child))) { batch =>
        ColumnReduce.sumMappedInt32(batch, ordinal = 1, value => fn(value))
      }
    case (Plan.KeysOp(source), PhysicalOp.Column(child, 0)) =>
      sumPairColumn(source, child, ordinal = 0)
    case (Plan.ValuesOp(source), PhysicalOp.Column(child, 1)) =>
      sumPairColumn(source, child, ordinal = 1)
    case _ =>
      ColumnReduce.sumInt32(batchesOf(interpret(plan, op)))

  private def sumDouble(plan: Plan[_], op: PhysicalOp): Double = (plan, op) match
    case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Floats, _)) =>
      ColumnReduce.sumFloat64(encodeFloats(partitions))
    case (
          Plan.FilterOp(source, predicate),
          PhysicalOp.Filter(child, PhysicalOp.Slot.Element),
        ) if isFloat(child) =>
      val keep = predicate.asInstanceOf[Double => Boolean]
      foldDouble(batchesOf(interpret(source, child))) { batch =>
        Float64Pipeline.sumFiltered(batch, value => keep(value))
      }
    case (Plan.MapOp(source, mapper), PhysicalOp.Project(child)) if isFloat(child) =>
      val fn = mapper.asInstanceOf[Double => Double]
      foldDouble(batchesOf(interpret(source, child))) { batch =>
        Float64Pipeline.sumProjected(batch, value => fn(value))
      }
    case (logical, PhysicalOp.Pipeline(child, length)) if isFloat(child) =>
      val (source, steps) = elementSteps(logical, length)
      if (steps.length < length) then
        throw new IllegalStateException("pipeline does not match logical element steps")
      foldDouble(batchesOf(interpret(source, child))) { batch =>
        ElementSum.sumFloats(batch, steps)
      }
    case _ =>
      ColumnReduce.sumFloat64(batchesOf(interpret(plan, op)))

  /**
   * Pair filters and pair maps above `base`, in source order. The functions are cast here and
   * called later, during the aggregate scan. A function that is not the pair type throws
   * `ClassCastException` then.
   */
  private[execution] def peelPairOps(plan: Plan[_]): (Plan[_], List[PairStep]) =
    @tailrec
    def collect(current: Plan[_], steps: List[PairStep]): (Plan[_], List[PairStep]) =
      current match
        case Plan.FilterOp(source, predicate) =>
          val keep = predicate.asInstanceOf[((Int, Int)) => Boolean]
          val step = PairStep.KeepRow((key, value) => keep((key, value)))
          collect(source, step :: steps)
        case Plan.FilterKeysOp(source, predicate) =>
          val keep = predicate.asInstanceOf[Int => Boolean]
          collect(source, PairStep.KeepKey(keep) :: steps)
        case Plan.FilterValuesOp(source, predicate) =>
          val keep = predicate.asInstanceOf[Int => Boolean]
          collect(source, PairStep.KeepValue(keep) :: steps)
        case Plan.MapOp(source, mapper) =>
          val project = mapper.asInstanceOf[((Int, Int)) => (Int, Int)]
          val step = PairStep.MapPair((key, value) => project((key, value)))
          collect(source, step :: steps)
        case _ => (current, steps)

    collect(plan, Nil)

  private def runGroupedSum(source: Plan.Source[_], steps: List[PairStep]): Option[Long] =
    if (!groupedPairSource(source)) then None
    else
      val parts = encodeGroupedPairs(source.partitions)
      val grouped = ColumnPipeline.sumByKey(
        parts,
        keyOrdinal = 0,
        valueOrdinal = 1,
        numPartitions = shuffleWidth,
        parallelism = parallelism,
        batchSize = batchSize,
        steps = steps.toVector,
      )
      Some(ColumnReduce.sumInt64(grouped, ordinal = 1))

  /**
   * Structural peel label of the pair steps above a grouped int-pair source. Not an action shape,
   * not a [[com.ewoodbury.scarlet.core.Plan]] node, and not a [[PhysicalOp]]. `None` means the
   * plan is not that peel: columnar execution is off, or the base is not a grouped pair source. An
   * int-pair source is `GroupedSum(Scan)`. Pair filters and a pair map wrap it, in source order,
   * as `FilterRow`, `FilterKey`, `FilterValue`, or `MapPair`. [[lower]] refuses the map, so
   * [[partitioning]] is `None`. [[tryGroupedSum]] still runs a map that stays `(Int, Int)`. A
   * type-changing pair map keeps this peel label, and its run returns `None`.
   */
  def groupedSumPeel(plan: Plan[_]): Option[String] =
    GroupedSumPeel.of(plan)

  /**
   * True when grouped sum can read `source` as int pairs. [[classify]] requires two `Int` cells,
   * so a leading null is not a pair and the encode never runs. A 2-tuple whose cells are `Int` or
   * null is still a pair: the null stays absent in the grouped-sum encode. Other actions keep the
   * strict witness and do not unbox that null.
   */
  private[execution] def groupedPairSource(source: Plan.Source[_]): Boolean =
    classify(source).contains(PhysicalOp.Kind.Pairs) || nullablePairWitness(source)

  private def nullablePairWitness(source: Plan.Source[_]): Boolean =
    val values = source.partitions.iterator.flatMap(_.data.iterator)
    source.descriptor match
      case Plan.SourceDescriptor.Unknown =>
        values.nextOption().exists(isNullableIntPair)
      case Plan.SourceDescriptor.Pairs =>
        values.nextOption() match
          case None => false
          case Some(first) => isNullableIntPair(first) && values.forall(isNullableIntPair)
      case Plan.SourceDescriptor.Ints | Plan.SourceDescriptor.Doubles |
          Plan.SourceDescriptor.Strings =>
        false

  /**
   * Named erasure boundary. A grouped-sum pair is a 2-tuple. `productElement` does not unbox, so a
   * null component still witnesses a pair. A non-null cell is a reference, so the class test is
   * safe.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def isNullableIntPair(value: Any): Boolean =
    def isIntOrNull(cell: Any): Boolean = cell match
      case null => true
      case _: Int => true
      case _ => false

    value match
      case pair: Tuple2[?, ?] =>
        isIntOrNull(pair.productElement(0)) && isIntOrNull(pair.productElement(1))
      case _ => false

  private[execution] def isInt(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Ints => true
    case _ => false

  private def isFloat(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Floats => true
    case _ => false

  /** Read one int column of a pair plan. Does not allocate a projection batch. */
  private def sumPairColumn(source: Plan[_], child: PhysicalOp, ordinal: Int): Long =
    foldLong(pairsOf(interpret(source, child))) { batch =>
      ColumnReduce.sumInt32At(batch, ordinal)
    }

  private def pairProjection(
      source: Plan[_],
      child: PhysicalOp,
      ordinal: Int,
      selectedPartition: Option[Int] = None,
  ): Columns =
    val columns = pairsOf(interpret(source, child, selectedPartition)).map { batch =>
      ColumnBatches.takeColumn(batch, ordinal)
    }
    Columns.Ints(columns)

  private[execution] def foldInterpreted(plan: Plan[_], op: PhysicalOp)(
      fn: ColumnBatch => Long,
  ): Long =
    foldLong(batchesOf(interpret(plan, op)))(fn)

  private def foldLong(parts: Vector[ColumnBatch])(fn: ColumnBatch => Long): Long =
    if (parts.isEmpty) then 0L
    else
      val morsels = parts.flatMap(part => ColumnBatches.slice(part, batchSize))
      val partials = MorselScheduler.map(morsels, parallelism)((_, morsel) => fn(morsel))
      partials.foldLeft(0L)(_ + _)

  private def foldDouble(parts: Vector[ColumnBatch])(fn: ColumnBatch => Double): Double =
    if (parts.isEmpty) then 0.0
    else
      // One left fold per partition. Double addition is not associative, so morsel totals
      // would not match a scan of the whole column.
      val partials = MorselScheduler.map(parts, parallelism)((_, part) => fn(part))
      partials.foldLeft(0.0)(_ + _)

  private def batchesOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Ints(parts) => parts
    case Columns.Floats(parts) => parts
    case Columns.Pairs(parts) => parts
    case Columns.Triples(parts) => parts
    case Columns.Strings(parts) => parts

  private enum Columns:
    case Ints(parts: Vector[ColumnBatch])
    case Floats(parts: Vector[ColumnBatch])
    case Pairs(parts: Vector[ColumnBatch])
    case Triples(parts: Vector[ColumnBatch])
    case Strings(parts: Vector[ColumnBatch])

  private[execution] enum ElementStep:
    case Filter(predicate: Any)
    case Project(mapper: Any)

  private def accepted(plan: Plan[_]): Option[PhysicalOp] =
    lower(plan).filter(op => infoOf(op).invalidReason.isEmpty)

  /**
   * Partition count when `op` keeps input order and the input partition count. `None` is an
   * exchange, aggregate, or join: `take` must not read one partition and present it as the result.
   */
  private def preservedPartitions(op: PhysicalOp): Option[Int] = op match
    case PhysicalOp.Scan(_, partitions) => Some(partitions.length)
    case PhysicalOp.Filter(child, _) => preservedPartitions(child)
    case PhysicalOp.Project(child) => preservedPartitions(child)
    case PhysicalOp.Pipeline(child, _) => preservedPartitions(child)
    case PhysicalOp.Column(child, _) => preservedPartitions(child)
    case PhysicalOp.MapValue(child) => preservedPartitions(child)
    case _: PhysicalOp.HashAggregate | _: PhysicalOp.HashJoin | _: PhysicalOp.Exchange => None

  private def selectPartition[A](
      partitions: Seq[Partition[A]],
      selectedPartition: Option[Int],
  ): Seq[Partition[A]] = selectedPartition match
    case None => partitions
    case Some(index) => partitions.slice(index, index + 1)

  private def decodePrefix(columns: Columns, limit: Int): Seq[Any] =
    if (limit <= 0) then Seq.empty
    else
      columns match
        case Columns.Ints(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeInts(batch, rows))
        case Columns.Floats(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeFloat64s(batch, rows))
        case Columns.Pairs(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeIntPairs(batch, rows))
        case Columns.Triples(parts) =>
          // Joins are not partition-preserving, so `take` does not reach this arm.
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeIntTriples(batch, rows))
        case Columns.Strings(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeStrings(batch, rows))

  private def prefixRows[A](
      parts: Vector[ColumnBatch],
      limit: Int,
      decode: (ColumnBatch, Int) => Seq[A],
  ): Seq[Any] =
    takeChunks(
      limit,
      parts.iterator.map { batch => (remaining: Int) =>
        decode(batch, remaining)
      },
    )

  private def lowerPlan(plan: Plan[_]): Option[PhysicalOp] =
    val (base, steps) = elementSteps(plan, Int.MaxValue)
    if (steps.isEmpty) then lowerPlanNode(base)
    else lowerPlan(base).flatMap(child => lowerElementSteps(child, steps))

  /** Columnar joins are hash joins. No hint and an explicit shuffle-hash hint both qualify. */
  private def acceptsHashJoin(strategy: Option[Plan.JoinStrategy]): Boolean =
    strategy match
      case None | Some(Plan.JoinStrategy.ShuffleHash) => true
      case Some(_) => false

  private def lowerPlanNode(plan: Plan[_]): Option[PhysicalOp] = plan match
    case source: Plan.Source[_] =>
      classify(source).map(kind => PhysicalOp.Scan(kind, source.partitions))
    case Plan.FilterKeysOp(source, _) =>
      columnFilter(source, PhysicalOp.Slot.Key)
    case Plan.FilterValuesOp(source, _) =>
      columnFilter(source, PhysicalOp.Slot.Value)
    case Plan.KeysOp(source) =>
      pairColumn(source, ordinal = 0)
    case Plan.ValuesOp(source) =>
      pairColumn(source, ordinal = 1)
    case Plan.MapValuesOp(source, mapper) =>
      if (returnsUnboxedInt(mapper)) then mapValue(source) else None
    case Plan.ReduceByKeyOp(source, _) =>
      lowerPlan(source).flatMap { child =>
        child.kind match
          case PhysicalOp.Kind.Pairs =>
            Some(PhysicalOp.HashAggregate(PhysicalOp.Exchange(child)))
          case _ => None
      }
    case Plan.JoinOp(left, right, strategy) if acceptsHashJoin(strategy) =>
      (lowerPlan(left), lowerPlan(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)))
            case _ => None
        case _ => None
    case _ => None

  private[execution] def elementSteps(
      plan: Plan[_],
      limit: Int,
  ): (Plan[_], List[ElementStep]) =
    @tailrec
    def collect(
        current: Plan[_],
        remaining: Int,
        steps: List[ElementStep],
    ): (Plan[_], List[ElementStep]) =
      if (remaining == 0) then (current, steps)
      else
        current match
          case Plan.FilterOp(source, predicate) =>
            collect(source, remaining - 1, ElementStep.Filter(predicate) :: steps)
          case Plan.MapOp(source, mapper) =>
            collect(source, remaining - 1, ElementStep.Project(mapper) :: steps)
          case _ => (current, steps)

    collect(plan, limit, Nil)

  private def lowerElementSteps(
      child: PhysicalOp,
      steps: List[ElementStep],
  ): Option[PhysicalOp] = child.kind match
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Floats | PhysicalOp.Kind.Strings =>
      if (elementProjectsMatch(child.kind, steps)) then Some(lowerElementPipeline(child, steps))
      else None
    case _ =>
      steps.foldLeft(Option(child)) { (current, step) =>
        current.flatMap(op => lowerElementStep(op, step))
      }

  private def lowerElementStep(child: PhysicalOp, step: ElementStep): Option[PhysicalOp] =
    step match
      case ElementStep.Filter(predicate) =>
        child.kind match
          case PhysicalOp.Kind.Pairs => Some(PhysicalOp.Filter(child, PhysicalOp.Slot.Row))
          case PhysicalOp.Kind.Triples =>
            tripleFilterSlot(predicate).map { slot =>
              PhysicalOp.Filter(child, slot)
            }
          case _ => None
      case ElementStep.Project(_) => None

  private def tripleFilterSlot(predicate: Any): Option[PhysicalOp.Slot] = predicate match
    case recognized: IntJoinTriplePredicate[?] =>
      recognized.selectedSlot match
        case IntJoinTriplePredicate.Slot.Key => Some(PhysicalOp.Slot.TripleKey)
        case IntJoinTriplePredicate.Slot.LeftValue => Some(PhysicalOp.Slot.TripleLeftValue)
        case IntJoinTriplePredicate.Slot.RightValue => Some(PhysicalOp.Slot.TripleRightValue)
    case _ => None

  private def lowerElementPipeline(child: PhysicalOp, steps: List[ElementStep]): PhysicalOp =
    def lower(current: PhysicalOp, remaining: List[ElementStep]): PhysicalOp =
      val (ordinary, rest) = remaining.span {
        case ElementStep.Filter(predicate) =>
          child.kind match
            case PhysicalOp.Kind.Strings => dictionaryStringLiterals(predicate).isEmpty
            case _ => true
        case ElementStep.Project(_) => true
      }
      val chunked = ordinary.grouped(4).foldLeft(current) { (parent, group) =>
        group match
          case List(ElementStep.Filter(_)) => PhysicalOp.Filter(parent, PhysicalOp.Slot.Element)
          case List(ElementStep.Project(_)) => PhysicalOp.Project(parent)
          case _ => PhysicalOp.Pipeline(parent, group.length)
      }
      rest match
        case ElementStep.Filter(_) :: tail =>
          lower(PhysicalOp.Filter(chunked, PhysicalOp.Slot.Element), tail)
        case Nil => chunked
        case _ =>
          throw new IllegalStateException("only a dictionary string filter splits a pipeline")

    lower(child, steps)

  private val MaxDictionaryStringLiterals = 8

  private def dictionaryStringLiterals(predicate: Any): Option[Vector[String]] = predicate match
    case recognized: DictionaryStringPredicate[?] =>
      val literals = recognized.literals
      Option.when(
        literals.nonEmpty &&
          literals.length <= MaxDictionaryStringLiterals &&
          literals.forall(presentLiteral),
      )(literals)
    case _ => None

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def presentLiteral(value: String): Boolean = value match
    case null => false
    case _ => true

  private def pairColumn(source: Plan[_], ordinal: Int): Option[PhysicalOp] =
    lowerPlan(source).flatMap { child =>
      child.kind match
        case PhysicalOp.Kind.Pairs =>
          Some(PhysicalOp.Column(child, ordinal))
        case _ => None
    }

  private def mapValue(source: Plan[_]): Option[PhysicalOp] =
    lowerPlan(source).flatMap { child =>
      child.kind match
        case PhysicalOp.Kind.Pairs => Some(PhysicalOp.MapValue(child))
        case _ => None
    }

  /** Unboxed `Int => Int`, the same witness as an int element project. */
  private def returnsUnboxedInt(function: Any): Boolean =
    declaredElement(PhysicalOp.Kind.Ints, function)

  /**
   * True when every project in `steps` declares the signature of `kind`. A filter does not change
   * the element type. One failing project refuses the chain. This does not call the functions.
   */
  private def elementProjectsMatch(kind: PhysicalOp.Kind, steps: List[ElementStep]): Boolean =
    steps.forall {
      case ElementStep.Filter(_) => true
      case ElementStep.Project(mapper) => declaredElement(kind, mapper)
    }

  private def declaredElement(kind: PhysicalOp.Kind, function: Any): Boolean = kind match
    case PhysicalOp.Kind.Ints =>
      declaredSignature(function, "apply$mcII$sp", classOf[Int], classOf[Int], allowBridge = false)
    case PhysicalOp.Kind.Floats =>
      declaredSignature(
        function,
        "apply$mcDD$sp",
        classOf[Double],
        classOf[Double],
        allowBridge = false,
      )
    case PhysicalOp.Kind.Strings =>
      declaredSignature(function, "apply", classOf[String], classOf[String], allowBridge = true)
    case _ => false

  /**
   * True when `function` declares `apply` or `specialized` from `argument` to `result`, and every
   * other one-argument `apply` is an erasure bridge. A hand-written `apply(Any): Any` beside that
   * method is not a bridge, so the plan stays on the row path. `getDeclaredMethods` ignores an
   * inherited `apply`, which also stays on the row path. This does not call `function`.
   *
   * `allowBridge` is for `String => String`. A LambdaMetafactory spin is a hidden class whose
   * `apply(String): String` and `apply(Object): Object` are both ordinary methods, not bridges.
   * The erased method counts only as an erasure bridge. A bridge with the declared string
   * signature still counts. A primitive specialized apply is the implementation, so a bridge does
   * not satisfy it.
   */
  private def declaredSignature(
      function: Any,
      specialized: String,
      argument: Class[_],
      result: Class[_],
      allowBridge: Boolean,
  ): Boolean =
    val methods = function.getClass.getDeclaredMethods
    def matches(method: java.lang.reflect.Method): Boolean =
      isDeclaredApply(method, specialized, argument, result, allowBridge)

    methods.exists(matches) &&
    !methods.exists(method =>
      isDisqualifyingApply(method, specialized, argument, result, allowBridge),
    )

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def isDeclaredApply(
      method: java.lang.reflect.Method,
      specialized: String,
      argument: Class[_],
      result: Class[_],
      allowBridge: Boolean,
  ): Boolean =
    val named = method.getName == "apply" || method.getName == specialized
    val implemented = allowBridge || !method.isBridge
    named &&
    implemented &&
    method.getParameterCount == 1 &&
    method.getReturnType.eq(result) &&
    method.getParameterTypes()(0).eq(argument)

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def isDisqualifyingApply(
      method: java.lang.reflect.Method,
      specialized: String,
      argument: Class[_],
      result: Class[_],
      allowBridge: Boolean,
  ): Boolean =
    method.getName == "apply" &&
      method.getParameterCount == 1 &&
      !isDeclaredApply(method, specialized, argument, result, allowBridge) &&
      !isErasureBridge(method)

  /**
   * Compiler bridge from a specialized apply to `Function1.apply`. A Scala 3 lambda's erased
   * `apply(Object): Object` is not marked as a bridge; that class is hidden, and the method is the
   * same erasure. A hand-written overload is neither.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def isErasureBridge(method: java.lang.reflect.Method): Boolean =
    val parameters = method.getParameterTypes
    val erased =
      method.getParameterCount == 1 &&
        method.getReturnType.eq(classOf[AnyRef]) &&
        parameters(0).eq(classOf[Object])
    val lambdaErasure = method.getDeclaringClass.isHidden
    erased && (method.isBridge || method.isSynthetic || lambdaErasure)

  private def columnFilter(source: Plan[_], slot: PhysicalOp.Slot): Option[PhysicalOp] =
    lowerPlan(source).flatMap { child =>
      child.kind match
        case PhysicalOp.Kind.Pairs => Some(PhysicalOp.Filter(child, slot))
        case _ => None
    }

  private def classify(source: Plan.Source[_]): Option[PhysicalOp.Kind] =
    val inferred = classifyData(source.partitions)
    source.descriptor match
      case Plan.SourceDescriptor.Unknown => inferred
      case descriptor =>
        val describedKind = kindOf(descriptor)
        inferred match
          case Some(kind)
              if describedKind.contains(kind) && dataMatchesDescriptor(source, descriptor) =>
            Some(kind)
          case None if source.partitions.forall(_.data.isEmpty) => describedKind
          case _ => None

  private def kindOf(descriptor: Plan.SourceDescriptor): Option[PhysicalOp.Kind] = descriptor match
    // Boundary guard: classify returns on Unknown before this match.
    case Plan.SourceDescriptor.Unknown => None
    case Plan.SourceDescriptor.Ints => Some(PhysicalOp.Kind.Ints)
    case Plan.SourceDescriptor.Doubles => Some(PhysicalOp.Kind.Floats)
    case Plan.SourceDescriptor.Pairs => Some(PhysicalOp.Kind.Pairs)
    case Plan.SourceDescriptor.Strings => Some(PhysicalOp.Kind.Strings)

  private def dataMatchesDescriptor(
      source: Plan.Source[_],
      descriptor: Plan.SourceDescriptor,
  ): Boolean =
    if (ownedMatches(source, descriptor)) then true
    else
      val rows = source.partitions.iterator.flatMap(_.data.iterator)
      descriptor match
        case Plan.SourceDescriptor.Ints => rows.forall(isIntValue)
        case Plan.SourceDescriptor.Doubles => rows.forall(isDoubleValue)
        case Plan.SourceDescriptor.Pairs => rows.forall(isIntPairValue)
        case Plan.SourceDescriptor.Strings => rows.forall(isStringValue)
        // Boundary guard: classify does not call this with Unknown.
        case Plan.SourceDescriptor.Unknown => false

  /**
   * Engine-built primitive holders are type-safe by construction. Skipping them avoids a second
   * scan that would box every element before encode.
   */
  private def ownedMatches(source: Plan.Source[_], descriptor: Plan.SourceDescriptor): Boolean =
    import com.ewoodbury.scarlet.core.{DoubleArrays, IntArrays, IntPairArrays}

    descriptor match
      case Plan.SourceDescriptor.Ints =>
        source.partitions.forall(partition => partition.data.isInstanceOf[IntArrays])
      case Plan.SourceDescriptor.Doubles =>
        source.partitions.forall(partition => partition.data.isInstanceOf[DoubleArrays])
      case Plan.SourceDescriptor.Pairs =>
        source.partitions.forall(partition => partition.data.isInstanceOf[IntPairArrays])
      case Plan.SourceDescriptor.Strings | Plan.SourceDescriptor.Unknown => false

  private def isIntValue(value: Any): Boolean = value match
    case _: Int => true
    case _ => false

  private def isDoubleValue(value: Any): Boolean = value match
    case _: Double => true
    case _ => false

  private def isIntPairValue(value: Any): Boolean = value match
    case (_: Int, _: Int) => true
    case _ => false

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def isStringValue(value: Any): Boolean = value match
    case null => true
    case _: String => true
    case _ => false

  private def classifyData(partitions: Seq[Partition[_]]): Option[PhysicalOp.Kind] =
    if (partitions.isEmpty) then None
    else
      val values = partitions.iterator.flatMap(_.data.iterator)
      values.nextOption().flatMap(value => classifyValue(value, values))

  /**
   * The first present value selects the kind. A leading empty cell can only witness a string. A
   * pair witness here is two `Int` cells, so other actions do not unbox a null. Grouped sum also
   * accepts a null component; see [[groupedPairSource]].
   */
  private def classifyValue(value: Any, rest: Iterator[Any]): Option[PhysicalOp.Kind] =
    Option(value) match
      case Some(_: Int) => Some(PhysicalOp.Kind.Ints)
      case Some(_: Double) => Some(PhysicalOp.Kind.Floats)
      case Some((_: Int, _: Int)) => Some(PhysicalOp.Kind.Pairs)
      case Some(_: String) => Some(PhysicalOp.Kind.Strings)
      case Some(_) => None
      case None => stringWitness(rest)

  @tailrec
  private def stringWitness(values: Iterator[Any]): Option[PhysicalOp.Kind] =
    values.nextOption() match
      case None => None
      case Some(value) =>
        Option(value) match
          case None => stringWitness(values)
          case Some(_: String) => Some(PhysicalOp.Kind.Strings)
          case Some(_) => None

  private def infoOf(op: PhysicalOp): PartitioningInfo = op match
    case PhysicalOp.Filter(child, _) => infoOf(child)
    case PhysicalOp.Project(child) => infoOf(child)
    case PhysicalOp.Pipeline(child, _) => infoOf(child)
    case PhysicalOp.Column(child, _) => infoOf(child).withoutPairDistribution
    case PhysicalOp.MapValue(child) => infoOf(child)
    case _: PhysicalOp.HashAggregate | _: PhysicalOp.HashJoin => hashed
    case PhysicalOp.Exchange(_) =>
      throw new IllegalStateException("exchange is only a child of aggregate or join")
    case PhysicalOp.Scan(kind, partitions) =>
      narrow(Distribution.Unknown(partitions.length), layoutOf(kind))

  private def interpret(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int] = None,
  ): Columns = (plan, op) match
    case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Ints, _)) =>
      Columns.Ints(encodeInts(selectPartition(partitions, selectedPartition)))
    case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Floats, _)) =>
      Columns.Floats(encodeFloats(selectPartition(partitions, selectedPartition)))
    case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _)) =>
      Columns.Pairs(encodePairs(selectPartition(partitions, selectedPartition)))
    case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Strings, _)) =>
      Columns.Strings(encodeStrings(selectPartition(partitions, selectedPartition)))
    case (Plan.FilterOp(source, predicate), PhysicalOp.Filter(child, slot)) =>
      applyFilter(interpret(source, child, selectedPartition), predicate, slot)
    case (Plan.MapOp(source, mapper), PhysicalOp.Project(child)) =>
      project(interpret(source, child, selectedPartition), mapper)
    case (logical, PhysicalOp.Pipeline(child, length)) =>
      interpretPipeline(logical, child, length, selectedPartition)
    case (Plan.FilterKeysOp(source, predicate), PhysicalOp.Filter(child, PhysicalOp.Slot.Key)) =>
      applyFilter(interpret(source, child, selectedPartition), predicate, PhysicalOp.Slot.Key)
    case (
          Plan.FilterValuesOp(source, predicate),
          PhysicalOp.Filter(child, PhysicalOp.Slot.Value),
        ) =>
      applyFilter(interpret(source, child, selectedPartition), predicate, PhysicalOp.Slot.Value)
    case (Plan.KeysOp(source), PhysicalOp.Column(child, 0)) =>
      pairProjection(source, child, ordinal = 0, selectedPartition = selectedPartition)
    case (Plan.ValuesOp(source), PhysicalOp.Column(child, 1)) =>
      pairProjection(source, child, ordinal = 1, selectedPartition = selectedPartition)
    case (Plan.MapValuesOp(source, mapper), PhysicalOp.MapValue(child)) =>
      // Named erasure boundary. The descriptor check verified the declared `Int => Int` shape
      // before lowering, so this cast is safe.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val fn = mapper.asInstanceOf[Int => Int]
      Columns.Pairs(onEach(pairsOf(interpret(source, child, selectedPartition))) { batch =>
        ColumnKernel.projectInt32(batch, 1, value => fn(value))
      })
    case (
          Plan.ReduceByKeyOp(source, reduceFunc),
          PhysicalOp.HashAggregate(PhysicalOp.Exchange(child)),
        ) =>
      // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
      val op = reduceFunc.asInstanceOf[(Int, Int) => Int]
      Columns.Pairs(
        HashReduce.byKey(
          pairsOf(interpret(source, child, selectedPartition)),
          shuffleWidth,
          parallelism,
          batchSize,
          (left, right) => op(left, right),
        ),
      )
    case (
          Plan.JoinOp(left, right, strategy),
          PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
        ) if acceptsHashJoin(strategy) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      Columns.Triples(
        ColumnPipeline.innerJoin(
          pairsOf(interpret(left, leftOp, selectedPartition)),
          pairsOf(interpret(right, rightOp, selectedPartition)),
          shuffleWidth,
          parallelism,
          batchSize,
        ),
      )
    case _ =>
      throw new IllegalStateException(
        s"physical tree does not match ${plan.getClass.getSimpleName}",
      )

  private def interpretPipeline(
      plan: Plan[_],
      child: PhysicalOp,
      length: Int,
      selectedPartition: Option[Int],
  ): Columns =
    val (source, steps) = elementSteps(plan, length)
    if (steps.length < length) then
      // Lowering uses this same collector, so this only guards a malformed physical tree.
      throw new IllegalStateException("pipeline does not match logical element steps")
    pipeline(interpret(source, child, selectedPartition), steps)

  private def pipeline(columns: Columns, steps: List[ElementStep]): Columns = columns match
    case Columns.Ints(parts) =>
      Columns.Ints(onEach(parts)(batch => ElementPipeline.ints(batch, steps)))
    case Columns.Floats(parts) =>
      Columns.Floats(onEach(parts)(batch => ElementPipeline.floats(batch, steps)))
    case Columns.Strings(parts) =>
      Columns.Strings(onEach(parts)(batch => ElementPipeline.strings(batch, steps)))
    case _ => throw new IllegalStateException("pipeline expects an int, float, or string column")

  private def triplePredicate(
      slot: PhysicalOp.Slot,
      keep: IntJoinTriplePredicate[?],
  ): PairKernel.IntTriplePredicate = slot match
    case PhysicalOp.Slot.TripleKey => (key, _, _) => keep.keep(key)
    case PhysicalOp.Slot.TripleLeftValue => (_, left, _) => keep.keep(left)
    case PhysicalOp.Slot.TripleRightValue => (_, _, right) => keep.keep(right)
    case _ => throw new IllegalStateException("triple filter has a non-triple slot")

  private def applyFilter(columns: Columns, predicate: Any, slot: PhysicalOp.Slot): Columns =
    slot match
      case PhysicalOp.Slot.Element =>
        columns match
          case Columns.Ints(parts) =>
            val keep = predicate.asInstanceOf[Int => Boolean]
            Columns.Ints(onEach(parts) { batch =>
              ColumnKernel.filterInt32(batch, 0, value => keep(value))
            })
          case Columns.Floats(parts) =>
            val keep = predicate.asInstanceOf[Double => Boolean]
            Columns.Floats(onEach(parts) { batch =>
              ColumnKernel.filterFloat64(batch, 0, value => keep(value))
            })
          case Columns.Strings(parts) =>
            dictionaryStringLiterals(predicate) match
              case Some(literals) =>
                Columns.Strings(onEach(parts) { batch =>
                  ColumnKernel.filterUtf8ByValues(batch, 0, literals)
                })
              case None =>
                val keep = predicate.asInstanceOf[String => Boolean]
                Columns.Strings(onEach(parts) { batch =>
                  ColumnKernel.filterUtf8(batch, 0, value => keep(value))
                })
          case _ =>
            throw new IllegalStateException(
              "columnar element filter expects ints, floats, or strings",
            )
      case PhysicalOp.Slot.Row =>
        val keep = predicate.asInstanceOf[((Int, Int)) => Boolean]
        // `keep` is `(Int, Int) => Boolean`, so the pair is boxed for the call.
        Columns.Pairs(onEach(pairsOf(columns)) { batch =>
          PairKernel.filter(batch, (key, value) => keep((key, value)))
        })
      case PhysicalOp.Slot.Key =>
        val keep = predicate.asInstanceOf[Int => Boolean]
        Columns.Pairs(onEach(pairsOf(columns)) { batch =>
          ColumnKernel.filterInt32(batch, 0, value => keep(value))
        })
      case PhysicalOp.Slot.Value =>
        val keep = predicate.asInstanceOf[Int => Boolean]
        Columns.Pairs(onEach(pairsOf(columns)) { batch =>
          ColumnKernel.filterInt32(batch, 1, value => keep(value))
        })
      case PhysicalOp.Slot.TripleKey | PhysicalOp.Slot.TripleLeftValue |
          PhysicalOp.Slot.TripleRightValue =>
        val keep = predicate match
          case recognized: IntJoinTriplePredicate[?] => recognized
          case _ => throw new IllegalStateException("triple filter lost its slot predicate")
        val select = triplePredicate(slot, keep)
        Columns.Triples(onEach(triplesOf(columns)) { batch =>
          PairKernel.filterTriple(batch, select)
        })

  private def project(columns: Columns, mapper: Any): Columns = columns match
    case Columns.Ints(parts) =>
      val fn = mapper.asInstanceOf[Int => Int]
      Columns.Ints(onEach(parts) { batch =>
        ColumnKernel.projectInt32(batch, 0, value => fn(value))
      })
    case Columns.Floats(parts) =>
      val fn = mapper.asInstanceOf[Double => Double]
      Columns.Floats(onEach(parts) { batch =>
        ColumnKernel.projectFloat64(batch, 0, value => fn(value))
      })
    case Columns.Strings(parts) =>
      val fn = mapper.asInstanceOf[String => Any]
      Columns.Strings(onEach(parts) { batch =>
        ColumnKernel.projectUtf8(batch, 0, value => stringResult(fn(value)))
      })
    case _ =>
      throw new IllegalStateException("columnar map expects ints, floats, or strings")

  private def pairsOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Pairs(parts) if parts.nonEmpty => parts
    case Columns.Pairs(_) =>
      throw new IllegalStateException("columnar pairs have no partitions")
    case _ =>
      throw new IllegalStateException("columnar op expected int pairs")

  private def triplesOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Triples(parts) if parts.nonEmpty => parts
    case Columns.Triples(_) =>
      throw new IllegalStateException("columnar triples have no partitions")
    case _ =>
      throw new IllegalStateException("columnar op expected int triples")

  private def onEach(
      parts: Vector[ColumnBatch],
  )(fn: ColumnBatch => ColumnBatch): Vector[ColumnBatch] =
    if (parts.isEmpty) then parts
    else
      val sliced = parts.map(part => ColumnBatches.slice(part, batchSize))
      val flat = sliced.flatten
      val mapped = MorselScheduler.map(flat, parallelism)((_, morsel) => fn(morsel))
      regroup(sliced.map(_.length), mapped)

  private def regroup(counts: Vector[Int], flat: Vector[ColumnBatch]): Vector[ColumnBatch] =
    val seed: (Vector[ColumnBatch], Vector[ColumnBatch]) = (flat, Vector.empty[ColumnBatch])
    val (_, grouped) = counts.foldLeft(seed) { (state, count) =>
      val (rest, acc) = state
      val (group, tail) = rest.splitAt(count)
      (tail, acc :+ ColumnBatches.concat(group))
    }
    grouped

  private def encodeFloats(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.DoubleArrays

    partitions.map { partition =>
      partition.data match
        case owned: DoubleArrays => BatchCodec.copiedFloat64s(owned.values)
        case other =>
          // Named erasure boundary. Classification already selected double elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Double]]
          BatchCodec.float64s(rows)
    }.toVector

  private def encodeInts(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.IntArrays

    partitions.map { partition =>
      partition.data match
        case owned: IntArrays => BatchCodec.copiedInts(owned.values)
        case other =>
          // Named erasure boundary. Classification already selected int elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Int]]
          BatchCodec.ints(rows)
    }.toVector

  private def encodePairs(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    encodePairPartitions(partitions, rows => BatchCodec.intPairs(rows))

  /**
   * Pair encode for grouped sum. [[BatchCodec.intPairsPreservingAbsence]] keeps a null component
   * absent and rejects any other non-int cell. An owned pair array has no nulls and is copied like
   * [[encodePairs]].
   */
  private def encodeGroupedPairs(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    encodePairPartitions(partitions, rows => BatchCodec.intPairsPreservingAbsence(rows))

  private def encodePairPartitions(
      partitions: Seq[Partition[_]],
      encodeBoxed: Iterable[(Int, Int)] => ColumnBatch,
  ): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.IntPairArrays

    partitions.map { partition =>
      partition.data match
        case pairs: IntPairArrays => BatchCodec.copiedIntPairs(pairs.keys, pairs.values)
        case other =>
          // Named erasure boundary. Classification already selected `(Int, Int)` elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[(Int, Int)]]
          encodeBoxed(rows)
    }.toVector

  private def encodeStrings(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    partitions.map { partition =>
      BatchCodec.strings(partition.data.asInstanceOf[Iterable[String]])
    }.toVector

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private[execution] def stringResult(value: Any): String = value match
    case null => null
    case text: String => text
    case other =>
      throw new ClassCastException(
        s"columnar string map expected a String, found ${other.getClass.getName}",
      )

  private def decode(columns: Columns): Seq[Partition[_]] = columns match
    case Columns.Ints(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeInts(batch)))
    case Columns.Floats(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeFloat64s(batch)))
    case Columns.Pairs(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeIntPairs(batch)))
    case Columns.Triples(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeIntTriples(batch)))
    case Columns.Strings(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeStrings(batch)))

  private def narrow(distribution: Distribution, chosen: Layout): PartitioningInfo =
    PartitioningInfo(distribution, OrderingTag.Unsorted, chosen)

  private def hashed: PartitioningInfo =
    PartitioningInfo(
      Distribution.Hash(shuffleWidth, PartitioningInfo.RowKeyOrdinals),
      OrderingTag.Unsorted,
      layoutOf(PhysicalOp.Kind.Pairs),
    )

  /** String plans name the dictionary layout. Every other classified kind stays columnar. */
  private def layoutOf(kind: PhysicalOp.Kind): Layout = kind match
    case PhysicalOp.Kind.Strings => Layout.Utf8Dict(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Floats | PhysicalOp.Kind.Pairs |
        PhysicalOp.Kind.Triples =>
      Layout.Columnar(ScarletConf.get.columnarBatchSize)

  private def batchSize: Int =
    val size = ScarletConf.get.columnarBatchSize
    require(size > 0, s"columnar batch size must be positive, got $size")
    size

  private def parallelism: Int =
    val workers = ScarletConf.get.threadPoolSize
    require(workers > 0, s"thread pool size must be positive, got $workers")
    workers

  private def shuffleWidth: Int =
    val width = ScarletConf.get.defaultShufflePartitions
    require(width > 0, s"shuffle partitions must be positive, got $width")
    width
