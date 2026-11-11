package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.api.{DictionaryStringPredicate, IntJoinTriplePredicate}
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}

/**
 * Lowers typed plans to [[PhysicalOp]] and runs that tree on the columnar kernels.
 *
 * Selection, fusion, which joins lower, and the fallback contract are documented in
 * `docs/ARCHITECTURE.md`.
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
   * [[Layout.Utf8Dict]]. An int, long, boolean, double, or int-pair plan reports
   * [[Layout.Columnar]]. A long-pair plan reports that layout too. A pair map, including one that
   * [[tryGroupedSum]] fuses and one that changes `(Int, Int)` to another pair type, is not
   * accepted, so this is `None`.
   */
  def partitioning[A](plan: Plan[A]): Option[PartitioningInfo] =
    accepted(plan).map(infoOf)

  /**
   * The physical tree for `plan`, without running it. `None` means the row path, including an
   * element chain whose project does not declare the column's type. An int join lowers when its
   * hint is absent, shuffle-hash, or broadcast. A `(Long, Long)` inner join lowers only for an
   * absent hint or a shuffle-hash hint, to [[PhysicalOp.LongHashJoin]]. A null value on that join
   * stays a clear validity bit and [[tryRun]] keeps the matched row. It is not an absence bit. A
   * long outer join lowers for an absent hint, shuffle-hash, broadcast, or sort-merge. Long inner
   * broadcast and long inner sort-merge stay on the row path. A `(String, Int)` inner join lowers
   * only for an absent hint or a shuffle-hash hint, to [[PhysicalOp.StringHashJoin]]. A null key
   * is dropped. A null value stays a clear validity bit and [[tryRun]] keeps the matched row. It
   * is not an absence bit. A key or value filter on either side, including a stack of one,
   * scatters that side's survivor index and does not compact. An over-budget build bucket spills
   * when `spillEnabled` is on; the estimate includes the dictionary. At or under the budget, and
   * whenever spill is off, the join stays in memory. A string outer join lowers for an absent
   * hint, shuffle-hash, broadcast, or sort-merge, to the `String*Outer` ops. Hash and broadcast do
   * not spill. Sort-merge runs in memory and throws instead of spilling. String inner broadcast
   * and string inner sort-merge stay on the row path. The layout is [[Layout.StringTriple]]. An
   * explicit sort-merge hint on int pairs lowers to [[PhysicalOp.SortMergeJoin]]. A left, right,
   * or full outer sort-merge hint on int pairs lowers to the matching node. Any other sort-merge
   * returns `None`. A left outer join on int pairs lowers for a broadcast hint, a shuffle-hash
   * hint, an absent hint, or a sort-merge hint. An absent hint uses the shuffle-hash op, matching
   * an inner join. A right outer join on int pairs lowers for a broadcast hint, a shuffle-hash
   * hint, an absent hint, or a sort-merge hint. An absent hint uses the shuffle-hash op. The left
   * input is the build side and the right input is the probe side. A full outer join on int pairs
   * lowers for a broadcast hint, a shuffle-hash hint, an absent hint, or a sort-merge hint. An
   * absent hint uses the shuffle-hash op. A null value does not change that shape: [[tryRun]]
   * reads the value columns and returns `None`, and the row path runs. [[SortLowering]] accepts an
   * int, long, or string key sort of a scan, or of filters over that scan. A map, aggregate, or
   * join under the sort stays on the row path. One filter above an int sort lowers to
   * [[PhysicalOp.Filter]] on [[PhysicalOp.Slot.Row]] around [[PhysicalOp.SortByIntKey]], and only
   * for a fully present key column: the pair-row filter atom drops a null key while the row path
   * keeps it. The sort is [[SortLowering.lowerInt]]. [[SortLowering.acceptsAsChild]] stays false,
   * so a null key, a second filter, a long or string sort, and every other parent stay on the row
   * path. A slot filter sorts the survivor index. Over the budget, with spill on, that index is
   * spilled. A long-pair row filter and a whole-row string filter sort that index too. A long sort
   * spills when `spillEnabled` is on and the retained estimate exceeds `spillBudgetBytes`. The
   * physical op stays `SortByLongKey`. A string sort spills on that same budget. The physical op
   * stays `SortByStringKey`. At or under the budget, and whenever spill is off, the split stays in
   * memory and writes nothing. [[partitioning]] and [[tryRun]] follow a refusal. They also drop a
   * lowered tree whose layout is invalid.
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
      case Some(op) => runLowered(plan, op)

  /**
   * Decode a tree [[lower]] would hand to [[tryRun]]. The catch is the row-path fallback.
   * [[ColumnarFallback]] is its named form. Any other failure, including a boolean column in the
   * element pipeline, propagates.
   *
   * A pair, long-pair, triple, or string-pair stack whose selection steps are defined decodes
   * [[FilterChain.selected]], including a stack of one, and does not gather. That call interprets
   * the source under the stack, so this does not interpret the source again. Every other plan
   * decodes [[ColumnarInterpreter.interpret]]. The result cast is the same erasure boundary as
   * [[tryRun]]: the decoded partitions have the plan's element type.
   */
  private[execution] def runLowered[A](plan: Plan[A], op: PhysicalOp): Option[Seq[Partition[A]]] =
    try
      val decoded = FilterChain.selected(
        plan,
        op,
        selectedPartition = None,
        preserveAbsence = false,
        ordinals = ColumnUse.ordinals(plan, op),
      ) match
        case Some(parts) =>
          parts.map { selected =>
            Partition(decodeSelected(selected, selected.count, op))
          }
        case None =>
          ColumnarInterpreter.decodeFor(ColumnarInterpreter.interpret(plan, op), op)
      Some(decoded.asInstanceOf[Seq[Partition[A]]])
    // ColumnarFallback extends ClassCastException, so the catch above still sees a decline.
    catch case _: ClassCastException => None

  /**
   * Run an accepted plan until its first `n` results are available. Partition-preserving plans
   * interpret one source partition at a time. An int-key, long-key, or string-key sort is not
   * partition-preserving: the whole sort runs, then only the requested prefix is decoded across
   * the output partitions in order. An exchange, aggregate, or join is refused. `n <= 0` returns
   * empty without running. The catch is the row-path fallback. [[ColumnarFallback]] is its named
   * form.
   */
  def tryTake[A](plan: Plan[A], n: Int): Option[Seq[A]] =
    if (n <= 0) then Some(Seq.empty[A])
    else
      accepted(plan) match
        case None => None
        case Some(op) =>
          try takeAccepted(plan, op, n).map(_.asInstanceOf[Seq[A]])
          // ColumnarFallback extends ClassCastException, so the catch above still sees a decline.
          catch case _: ClassCastException => None

  private def takeAccepted(plan: Plan[_], op: PhysicalOp, n: Int): Option[Seq[Any]] =
    op match
      case _: PhysicalOp.SortByIntKey | _: PhysicalOp.SortByLongKey |
          _: PhysicalOp.SortByStringKey =>
        Some(
          ColumnarInterpreter.decodePrefixFor(ColumnarInterpreter.interpret(plan, op), n, op),
        )
      case other =>
        PartitionPreserving.of(other).map(count => prefixOf(plan, other, count, n))

  /**
   * First `n` values in partition order. Decoding stops once `n` values are in hand, and every
   * batch of a partition contributes before the next partition is read. Each returned chunk is
   * appended once, in order.
   *
   * A partition-preserving pair, long-pair, triple, or string-pair filter stack, including a stack
   * of one, decodes a prefix of [[FilterChain.selected]]. The decoder sees `min(remaining, count)`
   * rows, so the rest of the index is not read and the stack is not gathered. Any other plan uses
   * [[ColumnarInterpreter.decodePrefixFor]].
   */
  private def prefixOf(plan: Plan[_], op: PhysicalOp, count: Int, n: Int): Seq[Any] =
    val ordinals = ColumnUse.ordinals(plan, op)
    takeChunks(
      n,
      Iterator.range(0, count).map { index => (remaining: Int) =>
        FilterChain.selected(
          plan,
          op,
          Some(index),
          preserveAbsence = false,
          ordinals,
        ) match
          case Some(parts) => prefixSelected(parts, op, remaining)
          case None =>
            ColumnarInterpreter.decodePrefixFor(
              ColumnarInterpreter.interpret(plan, op, Some(index)),
              remaining,
              op,
            )
      },
    )

  /** Prefix of one partition's selection. Later batches are not decoded once `limit` is filled. */
  private def prefixSelected(parts: Vector[Selected], op: PhysicalOp, limit: Int): Seq[Any] =
    takeChunks(
      limit,
      parts.iterator.map { selected => (remaining: Int) =>
        decodeSelected(selected, math.min(remaining, selected.count), op)
      },
    )

  /** `count` rows of `selected`, in index order. */
  private def decodeSelected(selected: Selected, count: Int, op: PhysicalOp): Seq[Any] =
    op.kind match
      case PhysicalOp.Kind.Pairs =>
        BatchCodec.decodeIntPairs(selected.batch, selected.rows, count)
      case PhysicalOp.Kind.LongPairs =>
        BatchCodec.decodeLongPairs(selected.batch, selected.rows, count)
      case PhysicalOp.Kind.Triples =>
        BatchCodec.decodeIntTriples(selected.batch, selected.rows, count)
      case PhysicalOp.Kind.StringPairs =>
        BatchCodec.decodeStringPairs(selected.batch, selected.rows, count)
      case other =>
        throw new ColumnarFallback(
          s"selection decode expected pairs, long pairs, triples, or string pairs, got $other",
        )

  /** Append chunks in order and stop once `limit` elements are stored. */
  @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
  private[execution] def takeChunks[A](limit: Int, producers: Iterator[Int => Seq[A]]): Seq[A] =
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
   * Row count of a columnar plan. `None` means the row path. This does not decode.
   * [[TrailingProject.count]] folds an int, long, or dictionary-literal string chain without
   * writing it. A filter stack over an int-pair scan counts in [[Selection.count]] and does not
   * build a survivor index or a gathered batch. Any other accepted plan reads finished batch
   * lengths. A string shape that is not the literal chain is that second strategy. The catch is
   * the row-path fallback. [[ColumnarFallback]] is its named form.
   */
  def tryCount(plan: Plan[_]): Option[Long] =
    accepted(plan) match
      case None => None
      case Some(op) =>
        try
          TrailingProject
            .count(plan, op)
            .orElse(Option.when(countsFilteredPairs(plan, op))(countFilteredPairs(plan, op)))
            .orElse(finishedCount(plan, op))
        // ColumnarFallback extends ClassCastException, so the catch above still sees a decline.
        catch case _: ClassCastException => None

  /**
   * True when `plan` is a pair-slot filter stack whose source is an int-pair scan.
   * [[PredicatePlan.selectionSteps]] is defined for key, value, and row atoms, including a stack
   * of one. An empty stack, a long or string pair, a triple, and a stack reached through a map are
   * false, and [[finishedCount]] runs them.
   */
  private def countsFilteredPairs(plan: Plan[_], op: PhysicalOp): Boolean =
    PredicatePlan.childOf(plan, op) match
      case (_, PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _)) =>
        PredicatePlan.compile(plan, op).flatMap(PredicatePlan.selectionSteps) match
          case Some(steps) => steps.forall(pairCountStep)
          case None => false
      case _ => false

  private def pairCountStep(step: Selection.Step): Boolean = step match
    case Selection.Step.Int32(_, _) | Selection.Step.IntPair(_) => true
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntTriple(_, _) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) =>
      false

  /** Counts the admitted stack in place. The source scan is encoded once and not filtered. */
  private def countFilteredPairs(plan: Plan[_], op: PhysicalOp): Long =
    val compiled = PredicatePlan.compile(plan, op).getOrElse(missingPairCount)
    val ordinals = ColumnUse.ordinals(plan, op)
    val steps = PredicatePlan
      .selectionSteps(PredicatePlan.encoded(compiled, ordinals))
      .getOrElse(missingPairCount)
      .toList
    val batches = PredicatePlan.childOf(plan, op) match
      case (_, PhysicalOp.Scan(PhysicalOp.Kind.Pairs, partitions)) =>
        SourceEncode.encodePairs(partitions, ordinals)
      case _ => missingPairCount
    foldMorsels(batches) { morsel =>
      Selection.count(morsel.batch, morsel.start, morsel.length, steps)
    }

  private def missingPairCount: Nothing =
    throw new IllegalStateException("pair filter count lost its scan")

  private def finishedCount(plan: Plan[_], op: PhysicalOp): Option[Long] =
    op match
      case _: PhysicalOp.GroupByKey =>
        Some(ColumnarInterpreter.groupCount(ColumnarInterpreter.interpret(plan, op)))
      case _ =>
        Some(
          ColumnReduce.count(
            ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op)),
          ),
        )

  /**
   * Wide sum of a finished int column. `None` means the row path, including a plan whose result is
   * not an int column. Null slots are skipped. This does not decode. The catch is the row-path
   * fallback. [[ColumnarFallback]] is its named form.
   */
  def trySumInt(plan: Plan[_]): Option[Long] =
    accepted(plan) match
      case Some(op) if isInt(op) =>
        try Some(sumInt(plan, op))
        // ColumnarFallback extends ClassCastException, so the catch above still sees a decline.
        catch case _: ClassCastException => None
      case _ => None

  /**
   * Sum of a double plan. `None` means the row path, including a plan whose result is not a double
   * column. A filter/project chain adds in the fused loop and does not write that chain's output
   * column. Null slots are skipped. The catch is the row-path fallback. [[ColumnarFallback]] is
   * its named form.
   */
  def trySumDouble(plan: Plan[_]): Option[Double] =
    accepted(plan) match
      case Some(op) if isFloat(op) =>
        try Some(sumDouble(plan, op))
        // ColumnarFallback extends ClassCastException, so the catch above still sees a decline.
        catch case _: ClassCastException => None
      case _ => None

  /**
   * Wide sum of a finished long column. `None` means the row path, including a plan whose result
   * is not a long column. The sum wraps. Null slots are skipped. A filter/project chain adds in
   * the fused walk and does not write that chain's output column. The catch is the row-path
   * fallback. [[ColumnarFallback]] is its named form.
   */
  def trySumLong(plan: Plan[_]): Option[Long] =
    accepted(plan) match
      case Some(op) if isLong(op) =>
        try Some(sumLong(plan, op))
        // ColumnarFallback extends ClassCastException, so the catch above still sees a decline.
        catch case _: ClassCastException => None
      case _ => None

  /**
   * Folded wide sum of int pairs. `None` means the row path. A typed pair source runs
   * [[ColumnPipeline.sumByKey]]. A preceding row, key, or value filter is
   * [[PredicatePlan.pairSteps]] from one [[PredicatePlan.compile]]. A map that stays a pair is
   * still a [[com.ewoodbury.scarlet.columnar.PairStep.MapPair]]. Both run inside the partial
   * `sumInt32`. The child source is encoded with no predicate, so those steps run once. A source
   * descriptor supplies the type for empty partitions. A null component drops the row before those
   * steps. A 2-tuple of `Int` or null still witnesses a pair, so a leading null does not refuse
   * the encode. Any other cell type fails. This does not decode. A mistyped function throws
   * `ClassCastException` and is refused after any rows it already touched. Any other shape is
   * refused. A pair map that stays `(Int, Int)` is the case [[lower]] refuses while this still
   * succeeds. There is no layout for that map. [[groupedSumPeel]] labels the walked steps and is
   * not this action. A map to another pair type, such as `(Long, Int)`, throws
   * [[ColumnarFallback]] from the pair unbox and this catch returns `None`.
   */
  def tryGroupedSum(plan: Plan[_]): Option[Long] =
    if (!ScarletConf.get.columnarExecution) then None
    else
      val (base, steps) = peelPairOps(plan)
      base match
        case source: Plan.Source[_] =>
          try runGroupedSum(source, steps)
          // ColumnarFallback extends ClassCastException, so the catch above still sees a decline.
          catch case _: ClassCastException => None
        case _ => None

  private def sumInt(plan: Plan[_], op: PhysicalOp): Long =
    TrailingProject.sum(plan, op).getOrElse(matchedSumInt(plan, op))

  private def matchedSumInt(plan: Plan[_], op: PhysicalOp): Long = (plan, op) match
    case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Ints, _)) =>
      ColumnReduce.sumInt32(SourceEncode.encodeInts(partitions))
    case (Plan.FilterOp(source, _), PhysicalOp.Filter(child, PhysicalOp.Slot.Element))
        if isInt(child) =>
      val keep = PredicatePlan.int32Filter(plan, op)
      foldMorsels(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(source, child))) {
        morsel =>
          val column = Int32Pipeline.singleInt(morsel.batch)
          Int32Pipeline.sumFiltered(column, morsel.start, morsel.length, keep)
      }
    case (logical, PhysicalOp.Pipeline(child, length)) if isInt(child) =>
      val (source, steps) = elementSteps(logical, length)
      if (steps.length < length) then
        throw new IllegalStateException("pipeline does not match logical element steps")
      foldMorsels(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(source, child))) {
        morsel =>
          ElementSum.sumInts(morsel, steps)
      }
    case (
          Plan.ValuesOp(Plan.MapValuesOp(source, mapper)),
          PhysicalOp.Column(PhysicalOp.MapValue(child), 1),
        ) if child.kind eq PhysicalOp.Kind.Pairs =>
      // Named erasure boundary. The descriptor check verified the declared `Int => Int` shape
      // before lowering, so this cast is safe.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val fn = mapper.asInstanceOf[Int => Int]
      foldSliced(ColumnarInterpreter.pairsOf(ColumnarInterpreter.interpret(source, child))) {
        batch =>
          ColumnReduce.sumMappedInt32(batch, ordinal = 1, value => fn(value))
      }
    case (Plan.KeysOp(source), PhysicalOp.Column(child, 0)) =>
      sumPairColumn(source, child, ordinal = 0)
    case (Plan.ValuesOp(source), PhysicalOp.Column(child, 1))
        if child.kind ne PhysicalOp.Kind.StringPairs =>
      sumPairColumn(source, child, ordinal = 1)
    case _ =>
      ColumnReduce.sumInt32(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op)))

  private def sumLong(plan: Plan[_], op: PhysicalOp): Long =
    TrailingProject.sumLong(plan, op).getOrElse(matchedSumLong(plan, op))

  private def matchedSumLong(plan: Plan[_], op: PhysicalOp): Long = (plan, op) match
    case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Longs, _)) =>
      ColumnReduce.sumInt64(SourceEncode.encodeLongs(partitions), 0)
    case (Plan.FilterOp(source, _), PhysicalOp.Filter(child, PhysicalOp.Slot.Element))
        if isLong(child) =>
      val keep = PredicatePlan.int64Filter(plan, op)
      foldMorsels(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(source, child))) {
        morsel =>
          val column = Int64Pipeline.single(morsel.batch)
          Int64Pipeline.sum(
            column,
            morsel.start,
            morsel.length,
            List(Int64Pipeline.Step.Filter(keep)),
          )
      }
    case (logical, PhysicalOp.Pipeline(child, length)) if isLong(child) =>
      val (source, steps) = elementSteps(logical, length)
      if (steps.length < length) then
        throw new IllegalStateException("pipeline does not match logical element steps")
      foldMorsels(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(source, child))) {
        morsel =>
          ElementSum.sumLongs(morsel, steps)
      }
    case _ =>
      ColumnReduce.sumInt64(
        ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op)),
        0,
      )

  private def sumDouble(plan: Plan[_], op: PhysicalOp): Double = (plan, op) match
    case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Floats, _)) =>
      ColumnReduce.sumFloat64(SourceEncode.encodeFloats(partitions))
    case (
          Plan.FilterOp(source, _),
          PhysicalOp.Filter(child, PhysicalOp.Slot.Element),
        ) if isFloat(child) =>
      val keep = PredicatePlan.float64Filter(plan, op)
      foldDouble(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(source, child))) {
        batch =>
          Float64Pipeline.sumFiltered(batch, keep)
      }
    case (Plan.MapOp(source, mapper), PhysicalOp.Project(child)) if isFloat(child) =>
      val fn = mapper.asInstanceOf[Double => Double]
      foldDouble(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(source, child))) {
        batch =>
          Float64Pipeline.sumProjected(batch, value => fn(value))
      }
    case (logical, PhysicalOp.Pipeline(child, length)) if isFloat(child) =>
      val (source, steps) = elementSteps(logical, length)
      if (steps.length < length) then
        throw new IllegalStateException("pipeline does not match logical element steps")
      foldDouble(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(source, child))) {
        batch =>
          ElementSum.sumFloats(batch, steps)
      }
    case _ =>
      ColumnReduce.sumFloat64(
        ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op)),
      )

  /**
   * Pair filters and pair maps above `base`, in source order. A filter prefix is
   * [[PredicatePlan.pairKeeps]]: one [[PredicatePlan.compile]], then [[PredicatePlan.pairSteps]].
   * The map arm is cast here and called later, during the aggregate scan. [[lower]] refuses that
   * map. A function that is not `(Int, Int)` throws [[ColumnarFallback]] when the scan unboxes it.
   */
  private[execution] def peelPairOps(plan: Plan[_]): (Plan[_], List[PairStep]) =
    @tailrec
    def collect(current: Plan[_], steps: List[PairStep]): (Plan[_], List[PairStep]) =
      current match
        case Plan.FilterOp(_, _) | Plan.FilterKeysOp(_, _) | Plan.FilterValuesOp(_, _) =>
          val (child, kept) = PredicatePlan.pairKeeps(current)
          collect(child, kept.toList ++ steps)
        case Plan.MapOp(source, mapper) =>
          val project = mapper.asInstanceOf[((Int, Int)) => (Int, Int)]
          val step = PairStep.MapPair((key, value) => project((key, value)))
          collect(source, step :: steps)
        case _ => (current, steps)

    collect(plan, Nil)

  private def runGroupedSum(source: Plan.Source[_], steps: List[PairStep]): Option[Long] =
    if (!groupedPairSource(source)) then None
    else
      // `steps` already names every filter. This copy runs none of them.
      val parts = SourceEncode.encodeGroupedPairs(source.partitions)
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
   * True when grouped sum can read `source` as int pairs. [[ScanLowering.classify]] requires two
   * `Int` cells, so a leading null is not a pair and the encode never runs. A 2-tuple whose cells
   * are `Int` or null is still a pair. The boxed scan keeps that null absent for grouped sum and
   * for the other actions that encode the same source.
   */
  private[execution] def groupedPairSource(source: Plan.Source[_]): Boolean =
    ScanLowering.classify(source).contains(PhysicalOp.Kind.Pairs) || nullablePairWitness(source)

  private def nullablePairWitness(source: Plan.Source[_]): Boolean =
    val values = source.partitions.iterator.flatMap(_.data.iterator)
    source.descriptor match
      case Plan.SourceDescriptor.Unknown =>
        values.nextOption().exists(IntPairCells.isNullablePair)
      case Plan.SourceDescriptor.Pairs =>
        values.nextOption() match
          case None => false
          case Some(first) =>
            IntPairCells.isNullablePair(first) && values.forall(IntPairCells.isNullablePair)
      case Plan.SourceDescriptor.Ints | Plan.SourceDescriptor.Longs |
          Plan.SourceDescriptor.Doubles | Plan.SourceDescriptor.Bools |
          Plan.SourceDescriptor.Strings | Plan.SourceDescriptor.LongPairs =>
        false

  private[execution] def isInt(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Ints => true
    case _ => false

  private def isFloat(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Floats => true
    case _ => false

  private[execution] def isLong(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Longs => true
    case _ => false

  /**
   * Sum one int column of a pair plan. `ordinal` is 0 for keys and 1 for values. A filter stack
   * over an int-pair scan adds in [[Selection.sumInt32]] and does not build a survivor index or a
   * gathered batch. The stack is the one [[countsFilteredPairs]] admits, and only when the encoded
   * columns include that ordinal. Any other shape still interprets and sums the projected column.
   */
  private def sumPairColumn(source: Plan[_], child: PhysicalOp, ordinal: Int): Long =
    sumFilteredPairs(source, child, ordinal).getOrElse(
      foldSliced(ColumnarInterpreter.pairsOf(ColumnarInterpreter.interpret(source, child))) {
        batch =>
          ColumnReduce.sumInt32At(batch, ordinal)
      },
    )

  /**
   * True when [[countsFilteredPairs]] admits `plan` and `ordinal` is the projected pair column.
   * Keys are ordinal 0 and values are ordinal 1.
   */
  private def sumsFilteredPairs(plan: Plan[_], op: PhysicalOp, ordinal: Int): Boolean =
    (ordinal == 0 || ordinal == 1) &&
      countsFilteredPairs(plan, op) &&
      ColumnUse.ordinals(plan, op).contains(ordinal)

  /**
   * Sums the admitted stack in place. The source scan is encoded once, both columns, and not
   * filtered. A null component stays absent, so a null measure adds nothing and a null filter slot
   * still fails the row. Steps keep the logical ordinals of that two-column batch. A shape that
   * admission rejects returns `None`, so the caller falls back rather than throwing.
   */
  private def sumFilteredPairs(plan: Plan[_], op: PhysicalOp, ordinal: Int): Option[Long] =
    if (!sumsFilteredPairs(plan, op, ordinal)) then None
    else
      for
        compiled <- PredicatePlan.compile(plan, op)
        steps <- PredicatePlan.selectionSteps(compiled).map(_.toList)
        partitions <- PredicatePlan.childOf(plan, op) match
          case (_, PhysicalOp.Scan(PhysicalOp.Kind.Pairs, scanned)) => Some(scanned)
          case _ => None
      yield
        val batches = SourceEncode.encodeGroupedPairs(partitions)
        foldMorsels(batches) { morsel =>
          Selection.sumInt32(morsel.batch, morsel.start, morsel.length, steps, ordinal)
        }

  /**
   * Fold `fn` over morsels of the batches `op` produces. Each batch is cut with [[Morsels.of]] and
   * those morsels run on the scheduler, one visit each. An empty batch is one zero-length morsel.
   * A kernel that must see the whole batch uses [[foldWholeBatches]].
   */
  private[execution] def foldInterpreted(
      plan: Plan[_],
      op: PhysicalOp,
  )(fn: Morsel => Long): Long =
    foldMorsels(batchesFor(plan, op))(fn)

  /**
   * Fold `fn` over the batches `op` produces, on the caller. The kernel sees each whole batch and
   * must handle any length, including zero. Nothing is cut into morsels. `fn` runs once per batch,
   * including an empty batch.
   */
  private[execution] def foldWholeBatches(
      plan: Plan[_],
      op: PhysicalOp,
  )(fn: ColumnBatch => Long): Long =
    batchesFor(plan, op).foldLeft(0L)((total, batch) => total + fn(batch))

  /**
   * The batches `op` produces. Both fold entry points interpret through here, so they cannot
   * drift.
   */
  private def batchesFor(plan: Plan[_], op: PhysicalOp): Vector[ColumnBatch] =
    ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op))

  /** Int and long element folds. Each morsel shares its batch. */
  private def foldMorsels(parts: Vector[ColumnBatch])(fn: Morsel => Long): Long =
    if (parts.isEmpty) then 0L
    else
      val morsels = parts.flatMap(part => Morsels.of(part, batchSize))
      val partials = MorselScheduler.map(morsels, parallelism)((_, morsel) => fn(morsel))
      partials.foldLeft(0L)(_ + _)

  /**
   * Copied slices. Pair-column sums still use this, including [[ColumnReduce.sumMappedInt32]]. Int
   * and long element folds use [[foldMorsels]].
   */
  private def foldSliced(parts: Vector[ColumnBatch])(fn: ColumnBatch => Long): Long =
    if (parts.isEmpty) then 0L
    else
      val slices = parts.flatMap(part => ColumnBatches.slice(part, batchSize))
      val partials = MorselScheduler.map(slices, parallelism)((_, slice) => fn(slice))
      partials.foldLeft(0L)(_ + _)

  private def foldDouble(parts: Vector[ColumnBatch])(fn: ColumnBatch => Double): Double =
    if (parts.isEmpty) then 0.0
    else
      // One left fold per partition. Double addition is not associative, so morsel totals
      // would not match a scan of the whole column.
      val partials = MorselScheduler.map(parts, parallelism)((_, part) => fn(part))
      partials.foldLeft(0.0)(_ + _)

  private[execution] enum ElementStep:
    case Filter(predicate: Any)
    case Project(mapper: Any)

  private def accepted(plan: Plan[_]): Option[PhysicalOp] =
    lower(plan).filter(op => infoOf(op).invalidReason.isEmpty)

  /**
   * Partition count when `op` keeps input order and the input partition count. `None` is an
   * exchange, aggregate, join, or key sort. A sort's output width is the shuffle width, so take
   * runs the whole sort instead of reading one input partition. A `(Long, Long)` scan keeps its
   * partitions, and `take` decodes that prefix. `take` must not read one partition of a wide op
   * and present it as the result.
   */
  private def lowerPlan(plan: Plan[_]): Option[PhysicalOp] =
    val (base, steps) = elementSteps(plan, Int.MaxValue)
    steps match
      case Nil => lowerPlanNode(base)
      case List(ElementStep.Filter(_)) =>
        filterOverIntSort(base).orElse(lowerSteps(base, steps))
      case _ => lowerSteps(base, steps)

  /**
   * One row filter whose base is an int sort. The shape is admitted only for a fully present key
   * column, because the pair-row filter atom drops a null key while the row path keeps it. The
   * sort is [[SortLowering.lowerInt]], not [[lowerChild]]. [[presentIntKeyColumn]] is
   * [[groupedPairSource]]'s pair of checks, [[ScanLowering.classify]] and the
   * [[nullablePairWitness]] walk, and it accepts only the strict side. A null key, a stack of two,
   * and every non-sort base return `None` or use [[lowerSteps]].
   */
  private def filterOverIntSort(base: Plan[_]): Option[PhysicalOp] = base match
    case Plan.SortByIntKeyOp(source) if presentIntKeyColumn(source) =>
      SortLowering.lowerInt(source, lowerPlan).map { sorted =>
        PhysicalOp.Filter(sorted, PhysicalOp.Slot.Row)
      }
    case _ => None

  /**
   * True when every pair under `plan` has a present int key. A filter does not invent a key, so
   * the scan is the witness. [[groupedPairSource]] is [[ScanLowering.classify]] or
   * [[nullablePairWitness]]. This shape needs the strict side: classify of a
   * [[Plan.SourceDescriptor.Pairs]] scan already requires every cell to be an `Int`, and an owned
   * array has no null. An unknown scan classifies from the first row, so the same walk as
   * [[nullablePairWitness]] checks every key.
   */
  private def presentIntKeyColumn(plan: Plan[_]): Boolean = plan match
    case source: Plan.Source[_] =>
      if (!ScanLowering.classify(source).contains(PhysicalOp.Kind.Pairs)) then false
      else
        source.descriptor match
          case Plan.SourceDescriptor.Pairs => true
          case Plan.SourceDescriptor.Unknown =>
            source.partitions.iterator.flatMap(_.data.iterator).forall(IntPairCells.presentKey)
          case Plan.SourceDescriptor.Ints | Plan.SourceDescriptor.Longs |
              Plan.SourceDescriptor.Doubles | Plan.SourceDescriptor.Bools |
              Plan.SourceDescriptor.Strings | Plan.SourceDescriptor.LongPairs =>
            false
    case Plan.FilterOp(child, _) => presentIntKeyColumn(child)
    case Plan.FilterKeysOp(child, _) => presentIntKeyColumn(child)
    case Plan.FilterValuesOp(child, _) => presentIntKeyColumn(child)
    case _ => false

  private def lowerSteps(base: Plan[_], steps: List[ElementStep]): Option[PhysicalOp] =
    lowerChild(base).flatMap(child => lowerElementSteps(child, steps))

  /**
   * Lower `plan` when a parent can wrap it. [[SortLowering.acceptsAsChild]] refuses a sort, so
   * this does not wrap one. [[filterOverIntSort]] is the exception for one filter over an int
   * sort, and only when that key column is fully present. A join, aggregate, or projection above a
   * sort stays on the row path.
   */
  private def lowerChild(plan: Plan[_]): Option[PhysicalOp] =
    lowerPlan(plan).filter(SortLowering.acceptsAsChild)

  private def lowerPlanNode(plan: Plan[_]): Option[PhysicalOp] = plan match
    case Plan.UnionOp(left, right) => UnionLowering.lower(left, right, lowerChild)
    case Plan.DistinctOp(source) =>
      lowerChild(source) match
        case Some(child) =>
          child.kind match
            case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Strings =>
              Some(PhysicalOp.Dedupe(PhysicalOp.Exchange(child)))
            case _ => None
        case None => None
    case source: Plan.Source[_] =>
      ScanLowering.classify(source).map(kind => PhysicalOp.Scan(kind, source.partitions))
    case Plan.FilterKeysOp(source, _) =>
      columnFilter(source, PhysicalOp.Slot.Key)
    case Plan.FilterValuesOp(source, _) =>
      columnFilter(source, PhysicalOp.Slot.Value)
    case Plan.KeysOp(source) =>
      pairColumn(source, ordinal = 0)
    case Plan.ValuesOp(source) =>
      pairColumn(source, ordinal = 1)
    case Plan.MapValuesOp(source, mapper) =>
      if (returnsUnboxedInt(mapper)) then mapValue(source, PhysicalOp.Kind.Pairs)
      else if (returnsUnboxedLong(mapper)) then mapValue(source, PhysicalOp.Kind.LongPairs)
      else None
    case Plan.ReduceByKeyOp(source, _) =>
      lowerChild(source).flatMap(ReduceByKeyLowering.apply)
    case Plan.GroupByKeyOp(source) =>
      lowerChild(source).flatMap(GroupByKeyLowering.apply)
    case Plan.CountByKeyOp(source) =>
      lowerChild(source).flatMap(CountByKeyLowering.apply)
    case plan if JoinLowering.isJoin(plan) => JoinLowering.lower(plan, lowerChild)
    case Plan.SortByIntKeyOp(source) =>
      SortLowering.lowerInt(source, lowerPlan)
    case Plan.SortByLongKeyOp(source) =>
      SortLowering.lowerLong(source, lowerPlan)
    case Plan.SortByStringKeyOp(source) =>
      SortLowering.lowerString(source, lowerPlan)
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
    case PhysicalOp.Kind.Bools =>
      BoolPipeline.lower(child, steps)
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Strings =>
      if (elementProjectsMatch(child.kind, steps)) then Some(lowerElementPipeline(child, steps))
      else None
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongPairs =>
      steps.foldLeft(Option(child)) { (current, step) =>
        current.flatMap(op => lowerElementStep(op, step))
      }
    case PhysicalOp.Kind.StringPairs =>
      steps.foldLeft(Option(child)) { (current, step) =>
        current.flatMap(op => lowerStringRow(op, step))
      }
    case PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.StringTriples |
        PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
        PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter =>
      None

  /** A whole-row string filter. A project still refuses the chain. */
  private def lowerStringRow(child: PhysicalOp, step: ElementStep): Option[PhysicalOp] =
    step match
      case ElementStep.Filter(_) => Some(PhysicalOp.Filter(child, PhysicalOp.Slot.Row))
      case ElementStep.Project(_) => None

  private def lowerElementStep(child: PhysicalOp, step: ElementStep): Option[PhysicalOp] =
    step match
      case ElementStep.Filter(predicate) =>
        child.kind match
          case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.LongPairs =>
            Some(PhysicalOp.Filter(child, PhysicalOp.Slot.Row))
          case PhysicalOp.Kind.Triples =>
            tripleFilterSlot(predicate) match
              case Some(slot) => Some(PhysicalOp.Filter(child, slot))
              case None if child.isInnerTriple =>
                Some(PhysicalOp.Filter(child, PhysicalOp.Slot.TripleRow))
              case None => None
          case _ => None
      case ElementStep.Project(_) =>
        Option.when(child.kind == PhysicalOp.Kind.Triples && child.isInnerTriple)(
          PhysicalOp.Project(child),
        )

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

  private[execution] def dictionaryStringLiterals(predicate: Any): Option[Vector[String]] =
    predicate match
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
    lowerChild(source).flatMap { child =>
      child.kind match
        case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.StringPairs =>
          Some(PhysicalOp.Column(child, ordinal))
        case _ => None
    }

  private def mapValue(source: Plan[_], kind: PhysicalOp.Kind): Option[PhysicalOp] =
    lowerChild(source).flatMap { child =>
      (child.kind, kind) match
        case (PhysicalOp.Kind.Pairs | PhysicalOp.Kind.StringPairs, PhysicalOp.Kind.Pairs) =>
          Some(PhysicalOp.MapValue(child))
        case (PhysicalOp.Kind.LongPairs, PhysicalOp.Kind.LongPairs) =>
          Some(PhysicalOp.MapValue(child))
        case _ => None
    }

  /** Unboxed `Int => Int` via [[FunctionWitness]], same as an int element project. */
  private def returnsUnboxedInt(function: Any): Boolean =
    FunctionWitness.declaredElement(PhysicalOp.Kind.Ints, function)

  /** Unboxed `Long => Long` via [[FunctionWitness]], same as a long element project. */
  private def returnsUnboxedLong(function: Any): Boolean =
    FunctionWitness.declaredElement(PhysicalOp.Kind.Longs, function)

  /**
   * True when every project in `steps` declares the signature of `kind`. A filter does not change
   * the element type. One failing project refuses the chain. This does not call the functions.
   */
  private def elementProjectsMatch(kind: PhysicalOp.Kind, steps: List[ElementStep]): Boolean =
    steps.forall {
      case ElementStep.Filter(_) => true
      case ElementStep.Project(mapper) => FunctionWitness.declaredElement(kind, mapper)
    }

  private def columnFilter(source: Plan[_], slot: PhysicalOp.Slot): Option[PhysicalOp] =
    lowerChild(source).flatMap { child =>
      child.kind match
        case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.LongPairs =>
          Some(PhysicalOp.Filter(child, slot))
        case _ => None
    }

  private[execution] def infoOf(op: PhysicalOp): PartitioningInfo = op match
    case PhysicalOp.Filter(child, _) => infoOf(child)
    case PhysicalOp.Project(child) => infoOf(child)
    case PhysicalOp.Pipeline(child, _) => infoOf(child)
    case union: PhysicalOp.Union =>
      UnionLowering.info(union, infoOf, layoutOf(op.kind))
    case PhysicalOp.Dedupe(_) => hashed(op.kind)
    case column @ PhysicalOp.Column(child, _) =>
      infoOf(child).withoutPairDistribution.copy(layout = layoutOf(column.kind))
    case PhysicalOp.MapValue(child) => infoOf(child)
    case _: PhysicalOp.SortByIntKey | _: PhysicalOp.SortByLongKey =>
      SortLowering.info(shuffleWidth, layoutOf(PhysicalOp.Kind.Pairs))
    case _: PhysicalOp.SortByStringKey =>
      SortLowering.info(shuffleWidth, layoutOf(PhysicalOp.Kind.StringPairs))
    case _: PhysicalOp.HashAggregate | _: PhysicalOp.LongHashAggregate | _: PhysicalOp.HashJoin |
        _: PhysicalOp.SortMergeJoin | _: PhysicalOp.SortMergeLeftOuter |
        _: PhysicalOp.SortMergeRightOuter | _: PhysicalOp.SortMergeFullOuter |
        _: PhysicalOp.HashLeftOuter | _: PhysicalOp.HashRightOuter | _: PhysicalOp.HashFullOuter =>
      hashed(PhysicalOp.Kind.Pairs)
    case _: PhysicalOp.StringHashAggregate =>
      hashed(PhysicalOp.Kind.StringPairs)
    case group: PhysicalOp.GroupByKey =>
      hashed(group.kind)
    case count: PhysicalOp.CountByKey =>
      hashed(count.kind)
    case _: PhysicalOp.LongHashJoin =>
      hashed(PhysicalOp.Kind.LongPairs)
    case _: PhysicalOp.StringHashJoin =>
      hashed(PhysicalOp.Kind.StringTriples)
    case _: PhysicalOp.LongHashLeftOuter | _: PhysicalOp.LongHashRightOuter |
        _: PhysicalOp.LongHashFullOuter | _: PhysicalOp.LongSortMergeLeftOuter |
        _: PhysicalOp.LongSortMergeRightOuter | _: PhysicalOp.LongSortMergeFullOuter =>
      hashed(PhysicalOp.Kind.LongOuter)
    case PhysicalOp.LongBroadcastLeftOuter(left, _) =>
      infoOf(left).copy(layout = layoutOf(PhysicalOp.Kind.LongOuter))
    case PhysicalOp.LongBroadcastRightOuter(_, right) =>
      infoOf(right).copy(layout = layoutOf(PhysicalOp.Kind.LongOuter))
    case PhysicalOp.LongBroadcastFullOuter(left, _) =>
      infoOf(left).copy(layout = layoutOf(PhysicalOp.Kind.LongOuter))
    case _: PhysicalOp.StringHashLeftOuter | _: PhysicalOp.StringHashRightOuter |
        _: PhysicalOp.StringHashFullOuter | _: PhysicalOp.StringSortMergeLeftOuter |
        _: PhysicalOp.StringSortMergeRightOuter | _: PhysicalOp.StringSortMergeFullOuter =>
      hashed(PhysicalOp.Kind.StringOuter)
    case PhysicalOp.StringBroadcastLeftOuter(left, _) =>
      infoOf(left).copy(layout = layoutOf(PhysicalOp.Kind.StringOuter))
    case PhysicalOp.StringBroadcastRightOuter(_, right) =>
      infoOf(right).copy(layout = layoutOf(PhysicalOp.Kind.StringOuter))
    case PhysicalOp.StringBroadcastFullOuter(left, _) =>
      infoOf(left).copy(layout = layoutOf(PhysicalOp.Kind.StringOuter))
    case PhysicalOp.BroadcastJoin(left, right, buildSide) =>
      broadcastInfo(left, right, buildSide)
    case PhysicalOp.BroadcastLeftOuter(left, _) =>
      infoOf(left).copy(layout = layoutOf(PhysicalOp.Kind.Triples))
    case PhysicalOp.BroadcastRightOuter(_, right) =>
      infoOf(right).copy(layout = layoutOf(PhysicalOp.Kind.Triples))
    case PhysicalOp.BroadcastFullOuter(left, _) =>
      infoOf(left).copy(layout = layoutOf(PhysicalOp.Kind.Triples))
    case PhysicalOp.Exchange(_) =>
      throw new IllegalStateException("exchange is only a child of aggregate or join")
    case PhysicalOp.Scan(kind, partitions) =>
      narrow(Distribution.Unknown(partitions.length), layoutOf(kind))

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private[execution] def stringResult(value: Any): String = value match
    case null => null
    case text: String => text
    case other =>
      throw new ColumnarFallback(
        s"columnar string map expected a String, found ${other.getClass.getName}",
      )

  private def broadcastInfo(
      left: PhysicalOp,
      right: PhysicalOp,
      buildSide: PhysicalOp.BuildSide,
  ): PartitioningInfo =
    val probe = buildSide match
      case PhysicalOp.BuildSide.Right => left
      case PhysicalOp.BuildSide.Left => right
    infoOf(probe).copy(layout = layoutOf(PhysicalOp.Kind.Triples))

  private def narrow(distribution: Distribution, chosen: Layout): PartitioningInfo =
    PartitioningInfo(distribution, OrderingTag.Unsorted, chosen)

  private def hashed(kind: PhysicalOp.Kind): PartitioningInfo =
    PartitioningInfo(
      Distribution.Hash(shuffleWidth, PartitioningInfo.RowKeyOrdinals),
      OrderingTag.Unsorted,
      layoutOf(kind),
    )

  /** String plans name the dictionary layout. String pairs and string joins name their columns. */
  private def layoutOf(kind: PhysicalOp.Kind): Layout = kind match
    case PhysicalOp.Kind.Strings => Layout.Utf8Dict(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.StringPairs => Layout.StringPair(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.StringOuter =>
      Layout.StringTriple(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.GroupedInts => Layout.GroupedInt(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.GroupedStrings =>
      Layout.GroupedString(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.GroupedLongs =>
      Layout.GroupedLong(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.Pairs | PhysicalOp.Kind.Triples |
        PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples |
        PhysicalOp.Kind.LongOuter =>
      Layout.Columnar(ScarletConf.get.columnarBatchSize)

  private[execution] def batchSize: Int =
    val size = ScarletConf.get.columnarBatchSize
    require(size > 0, s"columnar batch size must be positive, got $size")
    size

  private[execution] def parallelism: Int =
    val workers = ScarletConf.get.threadPoolSize
    require(workers > 0, s"thread pool size must be positive, got $workers")
    workers

  private[execution] def shuffleWidth: Int =
    val width = ScarletConf.get.defaultShufflePartitions
    require(width > 0, s"shuffle partitions must be positive, got $width")
    width
