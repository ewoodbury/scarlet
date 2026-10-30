package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.api.{DictionaryStringPredicate, IntJoinTriplePredicate}
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}

/**
 * Lowers `Int`, `Long`, `Double`, `Boolean`, `(Int, Int)`, `(Long, Long)`, and `String` plans to
 * [[PhysicalOp]] and runs that tree on the columnar kernels.
 *
 * Named erasure boundary. `Plan` is invariant and its element type is erased, so non-empty sources
 * are classified by their values and checked against any descriptor, while empty sources use their
 * descriptor. An element map lowers only when it declares the column's signature and every other
 * `apply` is an erasure bridge: `apply` or `apply$mcII$sp` for `Int => Int`, `apply` or
 * `apply$mcJJ$sp` for `Long => Long`, `apply` or `apply$mcDD$sp` for `Double => Double`, and
 * `apply` for `String => String`. The check reads method descriptors and does not call the
 * function. One failing project refuses the whole element chain, so a type-changing map stays on
 * the row path and is not run before that fallback. `getDeclaredMethods` misses an inherited
 * `apply`, so that function stays on the row path too. Other user functions are cast to the
 * selected operation. A result that is not that type throws `ClassCastException`. [[tryRun]]
 * catches it and returns `None`, and the row path runs the whole plan. Any unsupported node
 * (union, flatMap, distinct, generic sortBy, a map that is not fed by `Int`, `Long`, `Double`, or
 * `String`, any `Boolean` map, and the rest) makes the whole plan ineligible, so a supported
 * prefix is not executed on its own. `sortByIntKey` of an int-pair scan is the exception: it
 * lowers to a rank split. A null key orders as zero and stays null.
 *
 * [[lower]] and interpretation walk the same logical plan. The physical node records the operator
 * and not the function, so each logical node is matched to the node [[lower]] built for it. A
 * mismatch is a planner bug: interpretation throws `IllegalStateException`, and [[tryRun]] does
 * not turn that into a row-path fallback. A partition selector on an aggregate, a join, or a sort
 * fails a `require` guard with `IllegalArgumentException`, and that is not a fallback either. A
 * `ClassCastException` from a filter, or from a string map that declares `String` and returns
 * another value, is still a fallback, and that function may already have run.
 *
 * [[lower]] does not call user functions. A straight `Int`, `Long`, `Double`, or `String`
 * filter/project run of two to four steps is one [[PhysicalOp.Pipeline]]. A longer run is cut into
 * those chunks, source order first. One step stays a [[PhysicalOp.Filter]] or
 * [[PhysicalOp.Project]]. A `Boolean` plan lowers filters only, one [[PhysicalOp.Filter]] per
 * step, through [[BoolPipeline]]. That chain does not enter [[lowerElementPipeline]]. A boolean
 * column that reaches the element pipeline throws `IllegalStateException`, and [[tryRun]] catches
 * only `ClassCastException`. A `Boolean` map is refused without being called. `Function1` has no
 * `apply$mcZZ$sp`, so `Boolean => Boolean` cannot be distinguished from `Boolean => Int`. Boolean
 * has no sum. Count reads batch lengths. A string scan under only dictionary literal filters
 * counts codes from that mask and does not write the filtered column. Any other string step still
 * writes its column. Take decodes a prefix. Pair filters, aggregates, and joins stay one node per
 * operator. Narrow map and filter keep input order and the input partition count. `Double` and
 * `String` select filter and map only. A filter over an int join result is columnar only for
 * explicit key, left-value, and right-value filters; general tuple predicates stay on the row
 * path. `keys` and `values` of an int pair are that column. The arrays are shared: a published
 * column is not mutated, so a view is cheaper than a copy and still correct. `mapValues` lowers
 * only when the function declares an unboxed `Int` result and every other `apply` is an erasure
 * bridge. A wider primitive, a reference result, or an overloaded `apply` stays on the row path.
 * That check reads the method descriptor and does not call the function. `getDeclaredMethods` does
 * not see an inherited `apply`, so that function stays on the row path too. `reduceByKey` on
 * `(Int, Int)` applies the user's function and wraps at `Int`. A `(Long, Long)` source lowers to
 * [[PhysicalOp.LongHashAggregate]] and wraps at `Long`. Any other pair type stays on the row path.
 * A `(Long, Long)` inner join with no hint, or with an explicit shuffle-hash hint, lowers to
 * [[PhysicalOp.LongHashJoin]]. Both sides are exchanged on `Long.hashCode`. There is no spill and
 * no build budget. A null value is not a decoded long: interpretation reads the value columns and
 * leaves the plan on the row path. A broadcast hint, a sort-merge hint, and every outer join on
 * long keys stay on the row path. An int `join` with no hint, or with an explicit shuffle-hash
 * hint, hash-partitions both sides and probes one bucket at a time. A broadcast hint records both
 * children and builds the right input. A later cost-based pass may build the left input instead.
 * The other child is probed in place, with no exchange on either side. Output columns stay
 * `(key, left, right)`. Multiplicity, values, and null semantics match the shuffle-hash join.
 * Order is unspecified. The build table is estimated before it is allocated. That estimate covers
 * the table only, not the rest of the query. An estimate above `broadcastBuildBudgetBytes` throws
 * `IllegalStateException`. Both inputs are already evaluated, so [[tryRun]] does not turn that
 * failure into a row-path fallback. An inner sort-merge on int pairs lowers to
 * [[PhysicalOp.SortMergeJoin]]. A left, right, or full outer sort-merge on int pairs lowers to the
 * matching sort-merge node. Sort-merge on a non-int key, including a long key, makes `lower`
 * return `None`. A `(Long, Long)` inner shuffle-hash join still lowers. Output width for a
 * shuffle-hash join or an aggregate is `defaultShufflePartitions`. A broadcast join's output
 * follows the probe side, whichever child that is; `take` still refuses a join, so the prefix path
 * does not use that count. A left outer join on int pairs lowers for a broadcast hint, a
 * shuffle-hash hint, or an absent hint. Broadcast is [[PhysicalOp.BroadcastLeftOuter]]: the right
 * input is built and the left input is the probe side, with no exchange. Shuffle-hash and an
 * absent hint are [[PhysicalOp.HashLeftOuter]]. An absent hint is the same choice an inner join
 * makes. The row executor shuffle-hashes an unhinted outer join, so that plan is the shuffle op.
 * The left exchange carries a null key to partition 0. The right exchange drops a null key. A
 * probe-side row with no partner keeps the left value and clears the right value's validity bit.
 * That bit is absence. A null value is not absence and is not representable here: interpretation
 * reads the value columns' validity words and leaves the plan on the row path. An explicit
 * sort-merge hint is [[PhysicalOp.SortMergeLeftOuter]]. A right outer join on int pairs lowers for
 * the same three hints. Broadcast is [[PhysicalOp.BroadcastRightOuter]]: the left input is built
 * and the right input is the probe side, with no exchange. Shuffle-hash and an absent hint are
 * [[PhysicalOp.HashRightOuter]]. The right exchange carries a null key to partition 0. The left
 * exchange drops a null key. The left-outer kernel runs with those roles, then the value columns
 * are swapped back to `(key, left, right)`. Absence is the left value's clear bit. A null value
 * stays on the row path the same way. An explicit sort-merge hint is
 * [[PhysicalOp.SortMergeRightOuter]]. A full outer join on int pairs lowers for a broadcast hint,
 * a shuffle-hash hint, or an absent hint. Broadcast is [[PhysicalOp.BroadcastFullOuter]]: the
 * right input is built and the left input is the probe side, with no exchange. Shuffle-hash and an
 * absent hint are [[PhysicalOp.HashFullOuter]]. Both exchanges carry a null key to partition 0.
 * Each broadcast probe thread keeps its own matched-row bitmap, and the bitmaps are OR'd after the
 * probes join. Each shuffle partition keeps one bitmap and emits its own unmatched build rows. A
 * null build key is carried outside the table and emitted once. An empty probe still emits every
 * build row. Either value's clear bit is absence. A null value stays on the row path. The
 * matched-row bitmaps are not part of `broadcastBuildBudgetBytes`. An explicit sort-merge hint is
 * [[PhysicalOp.SortMergeFullOuter]]. Both sides are hash-partitioned, and a null key on either
 * side is carried to partition 0. [[tryCount]] and [[trySumInt]] fold an int filter/map chain that
 * starts at a scan or at `keys` or `values`, including an int `mapValues`, and do not write that
 * chain. Sum folds one trailing project and chains longer than four. Count folds those, chains of
 * two to four steps, and a string scan under only dictionary literal filters. That string fold
 * reads the scan `lower` built. A string shape that is not that chain reads the finished batches.
 * [[trySumLong]] folds a long filter/map chain the same way, and the sum wraps. [[trySumDouble]]
 * folds at most four steps. Sum of `keys`, `values`, or an int `mapValues` with no further element
 * chain reads the pair column in that morsel loop and does not build the projection.
 * [[tryGroupedSum]] runs [[ColumnPipeline.sumByKey]] on a typed pair source and folds the `Int64`
 * group sums. A preceding `FilterOp`, `filterKeys`, `filterValues`, or pair `MapOp` is applied
 * inside that partial scan, so the action does not build those pairs. [[groupedSumPeel]] labels
 * those walked steps. It is not an action shape and not a physical node. A source descriptor
 * supplies the type when all partitions are empty. It does not decode. `collect` still does. A
 * pair `MapOp` stays on the row path for `collect` and `reduceByKey`, and [[partitioning]] reports
 * no layout for it.
 *
 * An [[com.ewoodbury.scarlet.core.IntArrays]], [[com.ewoodbury.scarlet.core.LongArrays]],
 * [[com.ewoodbury.scarlet.core.BoolArrays]], [[com.ewoodbury.scarlet.core.DoubleArrays]],
 * [[com.ewoodbury.scarlet.core.StringArrays]], [[com.ewoodbury.scarlet.core.IntPairArrays]], or
 * [[com.ewoodbury.scarlet.core.LongPairArrays]] source is copied into the column on the calling
 * thread. The partition stays readable, so the column does not adopt its array. A boxed `Vector`
 * or `List` is still read through its iterator. An `ArraySeq` passed straight to the codec is
 * copied the same way.
 *
 * A null string is a value. Classification skips a leading null and keeps looking for a `String`.
 * An all-null source stays on the row path, because there is no witness. An accepted string plan
 * records [[Layout.Utf8Dict]]: that dictionary plus codes, one dictionary per batch. The string
 * lambda stays scalar over the decoded string. Int, long, boolean, double, int-pair, long-pair,
 * int-triple, and long-triple plans stay [[Layout.Columnar]].
 *
 * `take` has a separate bounded path for accepted partition-preserving shapes: it interprets one
 * source partition at a time in order and decodes only the remaining requested values. Plans with
 * an exchange or aggregate stay on the row path because their output cannot be produced with this
 * narrow short-circuit. An int-key sort of a pair scan is not partition-preserving. Take runs the
 * whole sort, then decodes the requested prefix across the sorted output partitions in order.
 * `take(0)` returns empty without running.
 *
 * [[ColumnarInterpreter]] runs that lowered tree and encodes the source partitions.
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
   * does not change the lowered shape: [[tryRun]] reads the value columns and returns `None`.
   * Broadcast, sort-merge, and every outer join on long keys stay on the row path. An explicit
   * sort-merge hint on int pairs lowers to [[PhysicalOp.SortMergeJoin]]. A left, right, or full
   * outer sort-merge hint on int pairs lowers to the matching node. Any other sort-merge returns
   * `None`. A left outer join on int pairs lowers for a broadcast hint, a shuffle-hash hint, an
   * absent hint, or a sort-merge hint. An absent hint uses the shuffle-hash op, matching an inner
   * join. A right outer join on int pairs lowers for a broadcast hint, a shuffle-hash hint, an
   * absent hint, or a sort-merge hint. An absent hint uses the shuffle-hash op. The left input is
   * the build side and the right input is the probe side. A full outer join on int pairs lowers
   * for a broadcast hint, a shuffle-hash hint, an absent hint, or a sort-merge hint. An absent
   * hint uses the shuffle-hash op. A null value does not change that shape: [[tryRun]] reads the
   * value columns and returns `None`, and the row path runs. [[Plan.SortByIntKeyOp]] lowers only
   * when its child is a scan of int pairs, to [[PhysicalOp.SortByIntKey]]. Any other child, and
   * any parent of that sort, stays on the row path. [[partitioning]] and [[tryRun]] follow a
   * refusal. They also drop a lowered tree whose layout is invalid.
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
   * Decode a tree [[lower]] would hand to [[tryRun]]. `ClassCastException` is the row-path
   * fallback. Any other failure, including a boolean column in the element pipeline, propagates.
   */
  private[execution] def runLowered[A](plan: Plan[A], op: PhysicalOp): Option[Seq[Partition[A]]] =
    try
      Some(
        ColumnarInterpreter
          .decodeFor(ColumnarInterpreter.interpret(plan, op), op)
          .asInstanceOf[Seq[Partition[A]]],
      )
    catch case _: ClassCastException => None

  /**
   * Run an accepted plan until its first `n` results are available. Partition-preserving plans
   * interpret one source partition at a time. An int-key sort is not partition-preserving: the
   * whole sort runs, then only the requested prefix is decoded across the output partitions in
   * order. An exchange, aggregate, or join is refused. `n <= 0` returns empty without running.
   * `ClassCastException` falls back to `None`.
   */
  def tryTake[A](plan: Plan[A], n: Int): Option[Seq[A]] =
    if (n <= 0) then Some(Seq.empty[A])
    else
      accepted(plan) match
        case None => None
        case Some(op) =>
          try takeAccepted(plan, op, n).map(_.asInstanceOf[Seq[A]])
          catch case _: ClassCastException => None

  private def takeAccepted(plan: Plan[_], op: PhysicalOp, n: Int): Option[Seq[Any]] =
    op match
      case _: PhysicalOp.SortByIntKey =>
        Some(
          ColumnarInterpreter.decodePrefixFor(ColumnarInterpreter.interpret(plan, op), n, op),
        )
      case other =>
        preservedPartitions(other).map(count => prefixOf(plan, other, count, n))

  /**
   * First `n` values in partition order. Decoding stops once `n` values are in hand, and every
   * batch of a partition contributes before the next partition is read. Each returned chunk is
   * appended once, in order.
   */
  private def prefixOf(plan: Plan[_], op: PhysicalOp, count: Int, n: Int): Seq[Any] =
    takeChunks(
      n,
      Iterator.range(0, count).map { index => (remaining: Int) =>
        ColumnarInterpreter
          .decodePrefixFor(ColumnarInterpreter.interpret(plan, op, Some(index)), remaining, op)
      },
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
   * writing it. Any other accepted plan reads finished batch lengths. A string shape that is not
   * the literal chain is that second strategy.
   */
  def tryCount(plan: Plan[_]): Option[Long] =
    accepted(plan) match
      case None => None
      case Some(op) =>
        try TrailingProject.count(plan, op).orElse(finishedCount(plan, op))
        catch case _: ClassCastException => None

  private def finishedCount(plan: Plan[_], op: PhysicalOp): Option[Long] =
    Some(
      ColumnReduce.count(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op))),
    )

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
   * Wide sum of a finished long column. `None` means the row path, including a plan whose result
   * is not a long column. The sum wraps. Null slots are skipped. A filter/project chain adds in
   * the fused walk and does not write that chain's output column.
   */
  def trySumLong(plan: Plan[_]): Option[Long] =
    accepted(plan) match
      case Some(op) if isLong(op) =>
        try Some(sumLong(plan, op))
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
      ColumnReduce.sumInt32(ColumnarInterpreter.encodeInts(partitions))
    case (Plan.FilterOp(source, predicate), PhysicalOp.Filter(child, PhysicalOp.Slot.Element))
        if isInt(child) =>
      val keep = predicate.asInstanceOf[Int => Boolean]
      foldMorsels(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(source, child))) {
        morsel =>
          val column = Int32Pipeline.singleInt(morsel.batch)
          Int32Pipeline.sumFiltered(column, morsel.start, morsel.length, value => keep(value))
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
        ) =>
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
    case (Plan.ValuesOp(source), PhysicalOp.Column(child, 1)) =>
      sumPairColumn(source, child, ordinal = 1)
    case _ =>
      ColumnReduce.sumInt32(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op)))

  private def sumLong(plan: Plan[_], op: PhysicalOp): Long =
    TrailingProject.sumLong(plan, op).getOrElse(matchedSumLong(plan, op))

  private def matchedSumLong(plan: Plan[_], op: PhysicalOp): Long = (plan, op) match
    case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Longs, _)) =>
      ColumnReduce.sumInt64(ColumnarInterpreter.encodeLongs(partitions), 0)
    case (Plan.FilterOp(source, predicate), PhysicalOp.Filter(child, PhysicalOp.Slot.Element))
        if isLong(child) =>
      foldMorsels(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(source, child))) {
        morsel =>
          ElementSum.sumLongs(morsel, List(ElementStep.Filter(predicate)))
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
      ColumnReduce.sumFloat64(ColumnarInterpreter.encodeFloats(partitions))
    case (
          Plan.FilterOp(source, predicate),
          PhysicalOp.Filter(child, PhysicalOp.Slot.Element),
        ) if isFloat(child) =>
      val keep = predicate.asInstanceOf[Double => Boolean]
      foldDouble(ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(source, child))) {
        batch =>
          Float64Pipeline.sumFiltered(batch, value => keep(value))
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
      val parts = ColumnarInterpreter.encodeGroupedPairs(source.partitions)
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
      case Plan.SourceDescriptor.Ints | Plan.SourceDescriptor.Longs |
          Plan.SourceDescriptor.Doubles | Plan.SourceDescriptor.Bools |
          Plan.SourceDescriptor.Strings | Plan.SourceDescriptor.LongPairs =>
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

  private[execution] def isLong(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Longs => true
    case _ => false

  /** Read one int column of a pair plan. Does not allocate a projection batch. */
  private def sumPairColumn(source: Plan[_], child: PhysicalOp, ordinal: Int): Long =
    foldSliced(ColumnarInterpreter.pairsOf(ColumnarInterpreter.interpret(source, child))) {
      batch =>
        ColumnReduce.sumInt32At(batch, ordinal)
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
   * exchange, aggregate, join, or int-key sort. A sort's output width is the shuffle width, so
   * take runs the whole sort instead of reading one input partition. A `(Long, Long)` scan keeps
   * its partitions, and `take` decodes that prefix. `take` must not read one partition of a wide
   * op and present it as the result.
   */
  private def preservedPartitions(op: PhysicalOp): Option[Int] = op match
    case PhysicalOp.Scan(_, partitions) => Some(partitions.length)
    case PhysicalOp.Filter(child, _) => preservedPartitions(child)
    case PhysicalOp.Project(child) => preservedPartitions(child)
    case PhysicalOp.Pipeline(child, _) => preservedPartitions(child)
    case PhysicalOp.Column(child, _) => preservedPartitions(child)
    case PhysicalOp.MapValue(child) => preservedPartitions(child)
    case _: PhysicalOp.HashAggregate | _: PhysicalOp.LongHashAggregate | _: PhysicalOp.HashJoin |
        _: PhysicalOp.LongHashJoin | _: PhysicalOp.SortMergeJoin |
        _: PhysicalOp.SortMergeLeftOuter | _: PhysicalOp.SortMergeRightOuter |
        _: PhysicalOp.SortMergeFullOuter | _: PhysicalOp.HashLeftOuter |
        _: PhysicalOp.HashRightOuter | _: PhysicalOp.HashFullOuter | _: PhysicalOp.BroadcastJoin |
        _: PhysicalOp.BroadcastLeftOuter | _: PhysicalOp.BroadcastRightOuter |
        _: PhysicalOp.BroadcastFullOuter | _: PhysicalOp.Exchange | _: PhysicalOp.SortByIntKey =>
      None

  private def lowerPlan(plan: Plan[_]): Option[PhysicalOp] =
    val (base, steps) = elementSteps(plan, Int.MaxValue)
    if (steps.isEmpty) then lowerPlanNode(base)
    else lowerChild(base).flatMap(child => lowerElementSteps(child, steps))

  /**
   * Lower `plan` when a parent can wrap it. A [[PhysicalOp.SortByIntKey]] child is refused, so a
   * filter, join, aggregate, or projection above a sort stays on the row path. The sort itself is
   * lowered by [[lowerPlan]].
   */
  private def lowerChild(plan: Plan[_]): Option[PhysicalOp] =
    lowerPlan(plan).filter {
      case _: PhysicalOp.SortByIntKey => false
      case _ => true
    }

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
      lowerChild(source).flatMap(ReduceByKeyLowering.apply)
    case Plan.JoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) =>
      sortMergeJoinOf(left, right, PhysicalOp.SortMergeJoin.apply)
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
            case _ => None
        case _ => None

    case Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)) =>
      (lowerChild(left), lowerChild(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(PhysicalOp.BroadcastLeftOuter(left = leftOp, right = rightOp))
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
            case _ => None
        case _ => None
    case Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)) =>
      (lowerChild(left), lowerChild(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(PhysicalOp.BroadcastRightOuter(left = leftOp, right = rightOp))
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
            case _ => None
        case _ => None
    case Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)) =>
      (lowerChild(left), lowerChild(right)) match
        case (Some(leftOp), Some(rightOp)) =>
          (leftOp.kind, rightOp.kind) match
            case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
              Some(PhysicalOp.BroadcastFullOuter(left = leftOp, right = rightOp))
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
            case _ => None
        case _ => None
    case Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) =>
      sortMergeJoinOf(left, right, PhysicalOp.SortMergeLeftOuter.apply)
    case Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) =>
      sortMergeJoinOf(left, right, PhysicalOp.SortMergeRightOuter.apply)
    case Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) =>
      sortMergeJoinOf(left, right, PhysicalOp.SortMergeFullOuter.apply)
    case Plan.SortByIntKeyOp(source) =>
      lowerPlan(source) match
        case Some(scan @ PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _)) =>
          Some(PhysicalOp.SortByIntKey(scan))
        case _ => None
    // A strategy the cases above do not accept stays on the row path.
    case _: Plan.LeftOuterJoinOp[_, _, _] | _: Plan.RightOuterJoinOp[_, _, _] |
        _: Plan.FullOuterJoinOp[_, _, _] =>
      None
    case _ => None

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
   * Sort-merge of two int-pair sides. Both children are exchanged. A non-pair side stays on the
   * row path. The inner join and the three outer joins share this gate.
   */
  private def sortMergeJoinOf(
      left: Plan[_],
      right: Plan[_],
      node: (PhysicalOp, PhysicalOp) => PhysicalOp,
  ): Option[PhysicalOp] =
    (lowerChild(left), lowerChild(right)) match
      case (Some(leftOp), Some(rightOp)) =>
        (leftOp.kind, rightOp.kind) match
          case (PhysicalOp.Kind.Pairs, PhysicalOp.Kind.Pairs) =>
            Some(node(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)))
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
    case PhysicalOp.Kind.Bools =>
      BoolPipeline.lower(child, steps)
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Strings =>
      if (elementProjectsMatch(child.kind, steps)) then Some(lowerElementPipeline(child, steps))
      else None
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.Triples =>
      steps.foldLeft(Option(child)) { (current, step) =>
        current.flatMap(op => lowerElementStep(op, step))
      }
    case PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongTriples => None

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
        case PhysicalOp.Kind.Pairs =>
          Some(PhysicalOp.Column(child, ordinal))
        case _ => None
    }

  private def mapValue(source: Plan[_]): Option[PhysicalOp] =
    lowerChild(source).flatMap { child =>
      child.kind match
        case PhysicalOp.Kind.Pairs => Some(PhysicalOp.MapValue(child))
        case _ => None
    }

  /** Unboxed `Int => Int` via [[FunctionWitness]], same as an int element project. */
  private def returnsUnboxedInt(function: Any): Boolean =
    FunctionWitness.declaredElement(PhysicalOp.Kind.Ints, function)

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
    case Plan.SourceDescriptor.Longs => Some(PhysicalOp.Kind.Longs)
    case Plan.SourceDescriptor.Doubles => Some(PhysicalOp.Kind.Floats)
    case Plan.SourceDescriptor.Pairs => Some(PhysicalOp.Kind.Pairs)
    case Plan.SourceDescriptor.Strings => Some(PhysicalOp.Kind.Strings)
    case Plan.SourceDescriptor.Bools => Some(PhysicalOp.Kind.Bools)
    case Plan.SourceDescriptor.LongPairs => Some(PhysicalOp.Kind.LongPairs)

  private def dataMatchesDescriptor(
      source: Plan.Source[_],
      descriptor: Plan.SourceDescriptor,
  ): Boolean =
    if (ownedMatches(source, descriptor)) then true
    else
      val rows = source.partitions.iterator.flatMap(_.data.iterator)
      descriptor match
        case Plan.SourceDescriptor.Ints => rows.forall(isIntValue)
        case Plan.SourceDescriptor.Longs => rows.forall(isLongValue)
        case Plan.SourceDescriptor.Doubles => rows.forall(isDoubleValue)
        case Plan.SourceDescriptor.Pairs => rows.forall(isIntPairValue)
        case Plan.SourceDescriptor.Strings => rows.forall(isStringValue)
        case Plan.SourceDescriptor.Bools => rows.forall(isBoolValue)
        case Plan.SourceDescriptor.LongPairs => rows.forall(isLongPairValue)
        // Boundary guard: classify does not call this with Unknown.
        case Plan.SourceDescriptor.Unknown => false

  /**
   * Engine-built array holders are type-safe by construction. Skipping them avoids a second scan
   * that would box every element before encode.
   */
  private def ownedMatches(source: Plan.Source[_], descriptor: Plan.SourceDescriptor): Boolean =
    import com.ewoodbury.scarlet.core.{
      BoolArrays,
      DoubleArrays,
      IntArrays,
      IntPairArrays,
      LongArrays,
      LongPairArrays,
      StringArrays,
    }

    descriptor match
      case Plan.SourceDescriptor.Ints =>
        source.partitions.forall(partition => partition.data.isInstanceOf[IntArrays])
      case Plan.SourceDescriptor.Longs =>
        source.partitions.forall(partition => partition.data.isInstanceOf[LongArrays])
      case Plan.SourceDescriptor.Doubles =>
        source.partitions.forall(partition => partition.data.isInstanceOf[DoubleArrays])
      case Plan.SourceDescriptor.Pairs =>
        source.partitions.forall(partition => partition.data.isInstanceOf[IntPairArrays])
      case Plan.SourceDescriptor.Bools =>
        source.partitions.forall(partition => partition.data.isInstanceOf[BoolArrays])
      case Plan.SourceDescriptor.Strings =>
        source.partitions.forall(partition => partition.data.isInstanceOf[StringArrays])
      case Plan.SourceDescriptor.LongPairs =>
        source.partitions.forall(partition => partition.data.isInstanceOf[LongPairArrays])
      // Boundary guard: classify does not call this with Unknown.
      case Plan.SourceDescriptor.Unknown => false

  private def isIntValue(value: Any): Boolean = value match
    case _: Int => true
    case _ => false

  private def isLongValue(value: Any): Boolean = value match
    case _: Long => true
    case _ => false

  private def isDoubleValue(value: Any): Boolean = value match
    case _: Double => true
    case _ => false

  private def isBoolValue(value: Any): Boolean = value match
    case _: Boolean => true
    case _ => false

  private def isIntPairValue(value: Any): Boolean = value match
    case (_: Int, _: Int) => true
    case _ => false

  private def isLongPairValue(value: Any): Boolean = value match
    case (_: Long, _: Long) => true
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
   * pair witness is two `Int` cells or two `Long` cells. A leading null component does not witness
   * [[PhysicalOp.Kind.LongPairs]]. Other actions do not unbox a null. Grouped sum also accepts a
   * null component; see [[groupedPairSource]].
   */
  private def classifyValue(value: Any, rest: Iterator[Any]): Option[PhysicalOp.Kind] =
    Option(value) match
      case Some(_: Int) => Some(PhysicalOp.Kind.Ints)
      case Some(_: Long) => Some(PhysicalOp.Kind.Longs)
      case Some(_: Double) => Some(PhysicalOp.Kind.Floats)
      case Some((_: Int, _: Int)) => Some(PhysicalOp.Kind.Pairs)
      case Some((_: Long, _: Long)) => Some(PhysicalOp.Kind.LongPairs)
      case Some(_: Boolean) => Some(PhysicalOp.Kind.Bools)
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

  private[execution] def infoOf(op: PhysicalOp): PartitioningInfo = op match
    case PhysicalOp.Filter(child, _) => infoOf(child)
    case PhysicalOp.Project(child) => infoOf(child)
    case PhysicalOp.Pipeline(child, _) => infoOf(child)
    case PhysicalOp.Column(child, _) => infoOf(child).withoutPairDistribution
    case PhysicalOp.MapValue(child) => infoOf(child)
    case _: PhysicalOp.SortByIntKey =>
      PartitioningInfo(
        Distribution.Range(shuffleWidth, PartitioningInfo.RowKeyOrdinals),
        OrderingTag.Sorted(PartitioningInfo.RowKeyOrdinals, Seq(true)),
        layoutOf(PhysicalOp.Kind.Pairs),
      )
    case _: PhysicalOp.HashAggregate | _: PhysicalOp.LongHashAggregate | _: PhysicalOp.HashJoin |
        _: PhysicalOp.SortMergeJoin | _: PhysicalOp.SortMergeLeftOuter |
        _: PhysicalOp.SortMergeRightOuter | _: PhysicalOp.SortMergeFullOuter |
        _: PhysicalOp.HashLeftOuter | _: PhysicalOp.HashRightOuter | _: PhysicalOp.HashFullOuter =>
      hashed(PhysicalOp.Kind.Pairs)
    case _: PhysicalOp.LongHashJoin =>
      hashed(PhysicalOp.Kind.LongPairs)
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
      throw new ClassCastException(
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

  /** String plans name the dictionary layout. Every other classified kind stays columnar. */
  private def layoutOf(kind: PhysicalOp.Kind): Layout = kind match
    case PhysicalOp.Kind.Strings => Layout.Utf8Dict(ScarletConf.get.columnarBatchSize)
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.Pairs | PhysicalOp.Kind.Triples |
        PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongTriples =>
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
