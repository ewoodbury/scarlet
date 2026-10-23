package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.api.{DictionaryStringPredicate, IntJoinTriplePredicate}
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.*

/**
 * Lowers `Int`, `Double`, `(Int, Int)`, and `String` plans to [[PhysicalOp]] and runs that tree on
 * the columnar kernels.
 *
 * Named erasure boundary. `Plan` is invariant and its element type is erased, so non-empty sources
 * are classified by their values and checked against any descriptor, while empty sources use their
 * descriptor. User functions are then cast to that operation. A function that does not return the
 * selected type throws `ClassCastException`. [[tryRun]] catches that and returns `None`, and the
 * row path runs the whole plan. Any unsupported node (union, flatMap, distinct, sort, a map that
 * is not fed by `Int`, `Double`, or `String`, and the rest) makes the whole plan ineligible, so a
 * supported prefix is not executed on its own.
 *
 * [[lower]] and interpretation walk the same logical plan. The physical node records the operator
 * and not the function, so each logical node is matched to the node [[lower]] built for it. A
 * mismatch is a planner bug: interpretation throws `IllegalStateException`, and [[tryRun]] does
 * not turn that into a row-path fallback. A `ClassCastException` from a function that is not the
 * selected type still is a fallback, and user side effects in that function may already have run.
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
 * result. A wider primitive or a reference result stays on the row path. That check reads the
 * method descriptor and does not call the function. `reduceByKey` applies the user's function and
 * wraps at `Int`. `join` hash-partitions both sides and probes one bucket at a time. Output width
 * for those two is `defaultShufflePartitions`. [[tryCount]] reads finished column lengths.
 * [[trySumInt]] and [[trySumDouble]] add a filter/project chain in the fused loop and do not write
 * that chain's output column. Sum of `keys`, `values`, or an int `mapValues` reads the pair column
 * in that morsel loop and does not build the projection. [[tryGroupedSum]] runs
 * [[ColumnPipeline.sumByKey]] on a typed pair source and folds the `Int64` group sums. A preceding
 * `FilterOp`, `filterKeys`, `filterValues`, or pair `MapOp` is applied inside that partial scan,
 * so the action does not build those pairs. A source descriptor supplies the type when all
 * partitions are empty. It does not decode. `collect` still does. A pair `MapOp` stays on the row
 * path for `collect` and `reduceByKey`.
 *
 * An [[IntArrays]], [[DoubleArrays]], or [[IntPairArrays]] source is copied into the column on the
 * calling thread. The partition stays readable, so the column does not adopt its array. A boxed
 * `Vector` or `List` is still read through its iterator. An `ArraySeq` passed straight to the
 * codec is copied the same way.
 *
 * A null string is a value. Classification skips a leading null and keeps looking for a `String`.
 * An all-null source stays on the row path, because there is no witness. An accepted string plan
 * records [[Layout.Utf8Dict]]: that dictionary plus codes, one dictionary per batch. The string
 * lambda stays scalar over the decoded string. Int, double, and int-pair plans stay
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
   * Layout the planner would use, without running the plan. `None` means the row path. A string
   * filter or map reports [[Layout.Utf8Dict]]. An int, double, or int-pair plan reports
   * [[Layout.Columnar]]. A pair map that [[tryGroupedSum]] can fuse reports columnar here and
   * still lowers to `None`, because `collect` and `reduceByKey` do not run that map on the
   * columnar path.
   */
  def partitioning[A](plan: Plan[A]): Option[PartitioningInfo] =
    accepted(plan).map(infoOf).orElse(fusedPairLayout(plan))

  /**
   * The physical tree for `plan`, without running it. `None` means the row path. An invalid layout
   * still lowers; [[partitioning]] and [[tryRun]] refuse it.
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
      accepted(plan).filter(isPartitionPreserving) match
        case None => None
        case Some(op) =>
          try
            val values = (0 until partitionCount(op)).foldLeft(Vector.empty[Any]) {
              (result, index) =>
                if (result.length >= n) then result
                else result ++ decodePrefix(interpret(plan, op, Some(index)), n - result.length)
            }
            Some(values.asInstanceOf[Seq[A]])
          catch case _: ClassCastException => None

  /**
   * Row count of a columnar plan. `None` means the row path. This reads batch lengths and does not
   * decode.
   */
  def tryCount(plan: Plan[_]): Option[Long] =
    consume(plan) { case columns =>
      ColumnReduce.count(batchesOf(columns))
    }

  /**
   * Wide sum of a finished int column. `None` means the row path, including a plan whose result is
   * not an int column. Null slots are skipped. This does not decode.
   */
  def trySumInt(plan: Plan[_]): Option[Long] =
    accepted(plan) match
      case None => None
      case Some(op) =>
        try Some(sumInt(plan, op))
        catch case _: ClassCastException => None

  /**
   * Sum of a double plan. `None` means the row path. A filter/project chain adds in the fused loop
   * and does not write that chain's output column. Null slots are skipped.
   */
  def trySumDouble(plan: Plan[_]): Option[Double] =
    accepted(plan) match
      case None => None
      case Some(op) =>
        try Some(sumDouble(plan, op))
        catch case _: ClassCastException => None

  /**
   * Folded wide sum of int pairs. `None` means the row path. A typed pair source runs
   * [[ColumnPipeline.sumByKey]]. A preceding row, key, or value filter, or a map that stays a
   * pair, is compiled into `keep`/`map` or [[com.ewoodbury.scarlet.columnar.PairStep]]s and
   * applied inside the partial `sumInt32`. A source descriptor supplies the type for empty
   * partitions. Null keys stay dropped. This does not decode. A mistyped function throws
   * `ClassCastException` and is refused after any rows it already touched. Any other shape is
   * refused.
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

  private def sumInt(plan: Plan[_], op: PhysicalOp): Long = (plan, op) match
    case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Ints, _)) =>
      ColumnReduce.sumInt32(encodeInts(partitions))
    case (Plan.FilterOp(source, predicate), PhysicalOp.Filter(child, PhysicalOp.Slot.Element))
        if isInt(child) =>
      val keep = predicate.asInstanceOf[Int => Boolean]
      foldLong(batchesOf(interpret(source, child))) { batch =>
        Int32Pipeline.sumFiltered(batch, value => keep(value))
      }
    case (Plan.MapOp(source, mapper), PhysicalOp.Project(child)) if isInt(child) =>
      val fn = mapper.asInstanceOf[Int => Int]
      foldLong(batchesOf(interpret(source, child))) { batch =>
        Int32Pipeline.sumProjected(batch, value => fn(value))
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
  private def peelPairOps(plan: Plan[_]): (Plan[_], List[PairStep]) =
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
    classify(source) match
      case Some(PhysicalOp.Kind.Pairs) =>
        val parts = encodePairs(source.partitions)
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
      case _ => None

  /**
   * Columnar layout for a pair map (or a map mixed with pair filters) that [[tryGroupedSum]]
   * fuses. [[lower]] still refuses the map, so `collect` and `reduceByKey` stay on the row path.
   */
  private def fusedPairLayout(plan: Plan[_]): Option[PartitioningInfo] =
    if (!ScarletConf.get.columnarExecution) then None
    else
      val (base, steps) = peelPairOps(plan)
      base match
        case source: Plan.Source[_] if steps.exists(isPairMap) =>
          classify(source) match
            case Some(PhysicalOp.Kind.Pairs) =>
              Some(
                narrow(
                  Distribution.Unknown(source.partitions.length),
                  layoutOf(PhysicalOp.Kind.Pairs),
                ),
              )
            case _ => None
        case _ => None

  private def isPairMap(step: PairStep): Boolean = step match
    case PairStep.MapPair(_) => true
    case _ => false

  private def isInt(op: PhysicalOp): Boolean = op.kind match
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

  private def consume[A](plan: Plan[_])(read: PartialFunction[Columns, A]): Option[A] =
    accepted(plan) match
      case None => None
      case Some(op) =>
        try
          val columns = interpret(plan, op)
          if (read.isDefinedAt(columns)) Some(read(columns)) else None
        catch case _: ClassCastException => None

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

  private def isPartitionPreserving(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Scan(_, _) => true
    case PhysicalOp.Filter(child, _) => isPartitionPreserving(child)
    case PhysicalOp.Project(child) => isPartitionPreserving(child)
    case PhysicalOp.Pipeline(child, _) => isPartitionPreserving(child)
    case PhysicalOp.Column(child, _) => isPartitionPreserving(child)
    case PhysicalOp.MapValue(child) => isPartitionPreserving(child)
    case _: PhysicalOp.HashAggregate | _: PhysicalOp.HashJoin | _: PhysicalOp.Exchange => false

  private def partitionCount(op: PhysicalOp): Int = op match
    case PhysicalOp.Scan(_, partitions) => partitions.length
    case PhysicalOp.Filter(child, _) => partitionCount(child)
    case PhysicalOp.Project(child) => partitionCount(child)
    case PhysicalOp.Pipeline(child, _) => partitionCount(child)
    case PhysicalOp.Column(child, _) => partitionCount(child)
    case PhysicalOp.MapValue(child) => partitionCount(child)
    case _: PhysicalOp.HashAggregate | _: PhysicalOp.HashJoin | _: PhysicalOp.Exchange => 0

  private def selectPartition[A](
      partitions: Seq[Partition[A]],
      selectedPartition: Option[Int],
  ): Seq[Partition[A]] = selectedPartition match
    case None => partitions
    case Some(index) => partitions.slice(index, index + 1)

  private def decodePrefix(columns: Columns, limit: Int): Seq[Any] = columns match
    case Columns.Ints(parts) =>
      parts.headOption.toSeq.flatMap(batch => BatchCodec.decodeInts(batch, limit))
    case Columns.Floats(parts) =>
      parts.headOption.toSeq.flatMap(batch => BatchCodec.decodeFloat64s(batch, limit))
    case Columns.Pairs(parts) =>
      parts.headOption.toSeq.flatMap(batch => BatchCodec.decodeIntPairs(batch, limit))
    case Columns.Triples(parts) =>
      parts.headOption.toSeq.flatMap(batch => BatchCodec.decodeIntTriples(batch, limit))
    case Columns.Strings(parts) =>
      parts.headOption.toSeq.flatMap(batch => BatchCodec.decodeStrings(batch, limit))

  private def lowerPlan(plan: Plan[_]): Option[PhysicalOp] =
    val (base, steps) = elementSteps(plan, Int.MaxValue)
    if (steps.isEmpty) then lowerPlanNode(base)
    else lowerPlan(base).flatMap(child => lowerElementSteps(child, steps))

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
    case Plan.JoinOp(left, right, _) =>
      (lowerPlan(left), lowerPlan(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)))
            case _ => None
        case _ => None
    case _ => None

  private def elementSteps(
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
      Some(lowerElementPipeline(child, steps))
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
          literals.forall(value => Option(value).isDefined),
      )(literals)
    case _ => None

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

  /**
   * True when `function` declares an unboxed `Int => Int` apply. A declared `apply` that returns
   * some other reference type is a change of type, including the bridge on an anonymous class, so
   * the plan stays on the row path. This does not call `function`.
   */
  private def returnsUnboxedInt(function: Any): Boolean =
    val methods = function.getClass.getDeclaredMethods
    methods.exists(isUnboxedIntApply) && !methods.exists(isOtherReferenceApply)

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def isUnboxedIntApply(method: java.lang.reflect.Method): Boolean =
    val named = method.getName match
      case "apply" | "apply$mcII$sp" => true
      case _ => false
    named &&
    method.getReturnType.eq(classOf[Int]) &&
    method.getParameterCount == 1 &&
    method.getParameterTypes()(0).eq(classOf[Int])

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def isOtherReferenceApply(method: java.lang.reflect.Method): Boolean =
    val named = method.getName match
      case "apply" => true
      case _ => false
    named &&
    !method.getReturnType.eq(classOf[Int]) &&
    !method.getReturnType.eq(classOf[AnyRef])

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
    case Plan.SourceDescriptor.Unknown => None
    case Plan.SourceDescriptor.Ints => Some(PhysicalOp.Kind.Ints)
    case Plan.SourceDescriptor.Doubles => Some(PhysicalOp.Kind.Floats)
    case Plan.SourceDescriptor.Pairs => Some(PhysicalOp.Kind.Pairs)
    case Plan.SourceDescriptor.Strings => Some(PhysicalOp.Kind.Strings)

  private def dataMatchesDescriptor(
      source: Plan.Source[_],
      descriptor: Plan.SourceDescriptor,
  ): Boolean =
    source.partitions.iterator
      .flatMap(_.data.iterator)
      .forall { value =>
        (descriptor, Option(value)) match
          case (Plan.SourceDescriptor.Ints, Some(_: Int)) => true
          case (Plan.SourceDescriptor.Doubles, Some(_: Double)) => true
          case (Plan.SourceDescriptor.Pairs, Some((_: Int, _: Int))) => true
          case (Plan.SourceDescriptor.Strings, Some(_: String)) => true
          case (Plan.SourceDescriptor.Strings, None) => true
          case _ => false
      }

  private def classifyData(partitions: Seq[Partition[_]]): Option[PhysicalOp.Kind] =
    if (partitions.isEmpty) then None
    else
      val values = partitions.iterator.flatMap(_.data.iterator)
      values.nextOption().flatMap(value => classifyValue(value, values))

  /** The first present value selects the kind. A leading empty cell can only witness a string. */
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
      val fn = mapper.asInstanceOf[Int => Int]
      Columns.Pairs(onEach(pairsOf(interpret(source, child, selectedPartition))) { batch =>
        ColumnKernel.projectInt32(batch, 1, value => fn(value))
      })
    case (
          Plan.ReduceByKeyOp(source, reduceFunc),
          PhysicalOp.HashAggregate(PhysicalOp.Exchange(child)),
        ) =>
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
          Plan.JoinOp(left, right, _),
          PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
        ) =>
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
        Columns.Triples(onEach(triplesOf(columns)) { batch =>
          PairKernel.filterTriple(
            batch,
            (key, left, right) =>
              slot match
                case PhysicalOp.Slot.TripleKey => keep.keep(key)
                case PhysicalOp.Slot.TripleLeftValue => keep.keep(left)
                case PhysicalOp.Slot.TripleRightValue => keep.keep(right)
                case _ => throw new IllegalStateException("triple filter has a non-triple slot"),
          )
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
    partitions.map { partition =>
      partition.data match
        case owned: DoubleArrays => BatchCodec.copiedFloat64s(owned.values)
        case other => BatchCodec.float64s(other.asInstanceOf[Iterable[Double]])
    }.toVector

  private def encodeInts(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    partitions.map { partition =>
      partition.data match
        case owned: IntArrays => BatchCodec.copiedInts(owned.values)
        case other => BatchCodec.ints(other.asInstanceOf[Iterable[Int]])
    }.toVector

  private def encodePairs(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    partitions.map { partition =>
      partition.data match
        case pairs: IntPairArrays => BatchCodec.copiedIntPairs(pairs.keys, pairs.values)
        case other => BatchCodec.intPairs(other.asInstanceOf[Iterable[(Int, Int)]])
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

  /** String plans name the dictionary layout. Int, double, and pair plans stay columnar. */
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
