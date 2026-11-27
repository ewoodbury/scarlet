package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.BatchCodec
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/** Oracle helpers, sources, and per-family assertions for the columnar contract suites. */
trait ColumnarContractFixture extends Matchers:

  protected val originalConf = ScarletConf.get

  protected def installContractConf(): Unit =
    ScarletConf.set(originalConf.copy(columnarExecution = true))

  protected def restoreContractConf(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)

  protected final case class AcceptedCase(
      label: String,
      plan: Plan[_],
      info: PartitioningInfo,
      physicalShape: Option[String],
      groupedSum: Option[Long] = None,
      longProbe: Option[() => Option[String]] = None,
  )

  protected final case class ExcludedCase(
      label: String,
      plan: Plan[_],
      observe: () => Unit = () => (),
  )

  protected enum PlanNode:
    case MapPartitions, Repartition, Coalesce, PartitionBy, Source, Map, Filter, FlatMap, Distinct
    case Union, Keys, Values, MapValues, FilterKeys, FilterValues, FlatMapValues, GroupByKey
    case CountByKey, CountSumByKey, ReduceByKey, SortBy, Join, CoGroup

  protected val intSource: Plan[Int] = Plan.Source(
    Seq(Partition(Seq(1, 2)), Partition(Seq(3, 4))),
  )
  protected val longSource: Plan[Long] = Plan.Source(
    Seq(Partition(Seq(1L, 2L)), Partition(Seq(3L, 4L))),
  )
  protected val doubleSource: Plan[Double] = Plan.Source(
    Seq(Partition(Seq(1.5, 2.5)), Partition(Seq(3.5))),
  )
  protected val stringSource: Plan[String] = Plan.Source(
    Seq(Partition(Seq("red", "blue")), Partition(Seq("green"))),
  )
  protected val pairSource: Plan[(Int, Int)] = Plan.Source(
    Seq(Partition(Seq(1 -> 10)), Partition(Seq(2 -> 20, 3 -> 30))),
  )
  protected val rightPairSource: Plan[(Int, Int)] = Plan.Source(
    Seq(Partition(Seq(1 -> 100, 2 -> 200)), Partition(Seq(3 -> 300))),
  )
  protected val stringKeySource: Plan[(String, Int)] = Plan.Source(
    Seq(Partition(Seq("red" -> 1))),
  )
  protected val doubleKeySource: Plan[(Double, Int)] = Plan.Source(
    Seq(Partition(Seq(1.5 -> 1))),
  )
  protected val emptyPairSource: Plan[(Int, Int)] =
    DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 2).plan
  protected val longPairSource: Plan[(Long, Long)] =
    DistCollection.ofLongPairs(Array(1L, 2L, 1L), Array(10L, 20L, 5L), 2).plan
  protected val emptyLongPairSource: Plan[(Long, Long)] =
    DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], 2).plan
  protected val emptyIntSource: Plan[Int] = Plan.Source(Seq(Partition(Seq.empty[Int])))
  protected val emptyLongSource: Plan[Long] = DistCollection.ofLongs(Array.empty[Long], 2).plan
  protected val boolSource: Plan[Boolean] = Plan.Source(
    Seq(Partition(Seq(true, false)), Partition(Seq(false, true))),
  )
  protected val emptyBoolSource: Plan[Boolean] =
    DistCollection.ofBools(Array.empty[Boolean], 2).plan
  protected val nullText: String = BatchCodec.stringElement(None)
  protected val allNullStringSource: Plan[String] = Plan.Source(
    Seq(Partition(Seq(nullText, nullText))),
  )
  protected val floatSource: Plan[Float] = Plan.Source(Seq(Partition(Seq(1.0f, 2.0f))))
  protected val tripleSource: Plan[(Int, (Int, Int))] = DistCollection(pairSource)
    .join(DistCollection(rightPairSource))
    .plan

  protected def groupedDecodeCases: Seq[AcceptedCase] =
    val width = ScarletConf.get.columnarBatchSize
    GroupedDecodeCases.fixtures(pairSource, stringKeySource, longPairSource).map { fixture =>
      AcceptedCase(
        fixture.label,
        fixture.plan,
        hashedInfo(GroupedDecodeCases.layout(fixture.family, width)),
        Some(GroupedDecodeCases.shape),
      )
    }

  /**
   * Slot stacks the aggregate parents admit. Two key or value slots sit on one `mapIntKey`, or on
   * one same-type `mapValues` of a long or string project. One slot above `mapIntKey` already
   * lowers and is not in this list.
   */
  protected def aggregateAdmissionCases: Seq[AcceptedCase] =
    val layout = columnarLayout
    val strings = Layout.StringPair(ScarletConf.get.columnarBatchSize)
    val mapped = Plan.MapIntKeyOp(pairSource, (key: Int) => key + 1)
    val intStack = Plan.FilterValuesOp(
      Plan.FilterKeysOp(mapped, (key: Int) => key > 0),
      (value: Int) => value > 0,
    )
    val longProjected = Plan.MapOp(
      longPairSource,
      (pair: (Long, Long)) => (pair._1 + 1L, pair._2),
    )
    val longMapped = Plan.MapValuesOp(longProjected, (value: Long) => value + 1L)
    val longStack = Plan.FilterValuesOp(
      Plan.FilterKeysOp(longMapped, (key: Long) => key > 0L),
      (value: Long) => value > 0L,
    )
    val stringProjected = Plan.MapOp(
      stringKeySource,
      (pair: (String, Int)) => (pair._1, pair._2 + 1),
    )
    val stringMapped = Plan.MapValuesOp(stringProjected, (value: Int) => value + 1)
    val stringStack = Plan.FilterValuesOp(
      Plan.FilterKeysOp(stringMapped, (key: String) => key.nonEmpty),
      (value: Int) => value > 0,
    )
    val intChild = "FilterValue(FilterKey(MapKey(Scan)))"
    val mappedChild = "FilterValue(FilterKey(MapValue(Project(Scan))))"
    Seq(
      AcceptedCase(
        "countByKey of a slot stack above mapIntKey",
        Plan.CountByKeyOp(intStack),
        hashedInfo(layout),
        Some(s"CountByKey(Exchange($intChild))"),
      ),
      AcceptedCase(
        "reduceByKey of a slot stack above mapIntKey",
        Plan.ReduceByKeyOp(intStack, (left: Int, right: Int) => left + right),
        hashedInfo(layout),
        Some(s"HashAggregate(Exchange($intChild))"),
      ),
      AcceptedCase(
        "countSumByKey of a slot stack above mapIntKey",
        Plan.CountSumByKeyOp(intStack),
        hashedInfo(layout),
        Some(s"CountSum(Exchange($intChild))"),
      ),
      AcceptedCase(
        "countByKey of a slot stack above long mapValues",
        Plan.CountByKeyOp(longStack),
        hashedInfo(layout),
        Some(s"CountByKey(Exchange($mappedChild))"),
      ),
      AcceptedCase(
        "reduceByKey of a slot stack above long mapValues",
        Plan.ReduceByKeyOp(longStack, (left: Long, right: Long) => left + right),
        hashedInfo(layout),
        Some(s"LongHashAggregate(Exchange($mappedChild))"),
      ),
      AcceptedCase(
        "countByKey of a slot stack above string mapValues",
        Plan.CountByKeyOp(stringStack),
        hashedInfo(strings),
        Some(s"CountByKey(Exchange($mappedChild))"),
      ),
      AcceptedCase(
        "reduceByKey of a slot stack above string mapValues",
        Plan.ReduceByKeyOp(stringStack, (left: Int, right: Int) => left + right),
        hashedInfo(strings),
        Some(s"StringHashAggregate(Exchange($mappedChild))"),
      ),
    )

  /**
   * Join-shaped aggregates. `countByKey` of an int hash, broadcast, or sort-merge join is
   * count-through-probe: `CountByKey` of a hash join of two exchanges. `reduceByKey` that adds
   * longs reads the project above that join. The three hints rewrite to the same hash join.
   */
  protected def joinAdmissionCases: Seq[AcceptedCase] =
    val layout = columnarLayout
    val hash = Plan.JoinOp(pairSource, rightPairSource, None)
    val broadcast = Plan.JoinOp(
      pairSource,
      rightPairSource,
      Some(Plan.JoinStrategy.Broadcast),
    )
    val merged = Plan.JoinOp(
      pairSource,
      rightPairSource,
      Some(Plan.JoinStrategy.SortMerge),
    )
    val counted = "CountByKey(HashJoin(Exchange(Scan), Exchange(Scan)))"
    val summed =
      "HashAggregate(Exchange(Project(HashJoin(Exchange(Scan), Exchange(Scan)))))"
    val add = (left: Long, right: Long) => left + right
    def project(join: Plan[(Int, (Int, Int))]): Plan[(Int, Long)] =
      Plan.MapOp(join, (row: (Int, (Int, Int))) => (row._1, row._2._1.toLong))
    Seq(
      AcceptedCase(
        "countByKey of an int hash join",
        Plan.CountByKeyOp(hash),
        hashedInfo(layout),
        Some(counted),
      ),
      AcceptedCase(
        "countByKey of an int broadcast join",
        Plan.CountByKeyOp(broadcast),
        hashedInfo(layout),
        Some(counted),
      ),
      AcceptedCase(
        "countByKey of an int sort-merge join",
        Plan.CountByKeyOp(merged),
        hashedInfo(layout),
        Some(counted),
      ),
      AcceptedCase(
        "reduceByKey of a project over an int hash join",
        Plan.ReduceByKeyOp(project(hash), add),
        hashedInfo(layout),
        Some(summed),
      ),
      AcceptedCase(
        "reduceByKey of a project over an int broadcast join",
        Plan.ReduceByKeyOp(project(broadcast), add),
        hashedInfo(layout),
        Some(summed),
      ),
      AcceptedCase(
        "reduceByKey of a project over an int sort-merge join",
        Plan.ReduceByKeyOp(project(merged), add),
        hashedInfo(layout),
        Some(summed),
      ),
    )

  /**
   * A boolean filter, then a project. The project is the second element step. `lower` refuses the
   * chain, so `partitioning` and `tryRun` are empty and do not call the mapper. The row path runs
   * it once for each element the filter keeps.
   */
  protected def booleanFilterThenMap: ExcludedCase =
    val keep = (value: Boolean) => value
    val hits = new AtomicInteger
    val mapped = Plan.MapOp(
      Plan.FilterOp(boolSource, keep),
      (value: Boolean) => {
        hits.incrementAndGet()
        !value
      },
    )
    val elements = boolSource match
      case Plan.Source(partitions, _) => partitions.flatMap(_.data)
      case other => fail(s"expected a boolean source, got $other")
    val expected = elements.filter(keep).map(value => !value)
    ExcludedCase(
      "boolean filter then map",
      mapped,
      () => {
        ColumnarPlanner.partitioning(mapped) shouldBe Option.empty[PartitioningInfo]
        ColumnarPlanner.tryRun(mapped) shouldBe Option.empty[Seq[Partition[Boolean]]]
        hits.get() shouldBe 0
        DistCollection(mapped).collect() shouldBe expected
        hits.get() shouldBe expected.length
      },
    )

  /**
   * `lower` and the action gates share one decision. A refused plan, including a double
   * sort-merge, makes every gate return `None`. An accepted plan runs `tryRun` and `tryCount`.
   * `tryTake` runs when the physical tree keeps input partitions, and for an int-key, long-key, or
   * string-key sort, which runs the whole sort and then decodes the prefix. `trySumInt`,
   * `trySumLong`, and `trySumDouble` run only for that numeric kind. `tryGroupedSum` equals
   * `groupedSum` for an accepted grouped pair source, an accepted `(Int, Int)` pair map, and an
   * accepted row, key, or value filter of one. It is `None` for every other accepted plan. The
   * peel label follows [[expectedGroupedPeel]]. A pair map whose result is not `(Int, Int)` still
   * lowers, and [[ColumnarPlanner.tryRun]] returns `None`.
   */
  protected def assertGatesAgree(
      plan: Plan[_],
      accepted: Boolean,
      groupedSum: Option[Long],
  ): Unit =
    val lowered = ColumnarPlanner.lower(plan)
    lowered.isDefined shouldBe accepted
    ColumnarPlanner.partitioning(plan).isDefined shouldBe accepted
    ColumnarPlanner.tryRun(plan).isDefined shouldBe accepted
    ColumnarPlanner.tryCount(plan).isDefined shouldBe accepted
    ColumnarPlanner.trySumInt(plan).isDefined shouldBe lowered.exists(sumsInts)
    ColumnarPlanner.trySumLong(plan).isDefined shouldBe lowered.exists(sumsLongs)
    ColumnarPlanner.trySumDouble(plan).isDefined shouldBe lowered.exists(sumsFloats)
    ColumnarPlanner.tryTake(plan, 1).isDefined shouldBe lowered.exists(acceptsTake)
    if (accepted) then
      val peel = expectedGroupedPeel(plan)
      ColumnarPlanner.tryGroupedSum(plan) shouldBe groupedSum
      ColumnarPlanner.groupedSumPeel(plan) shouldBe peel
      groupedSum.isDefined shouldBe peel.isDefined
    else
      groupedSum shouldBe None
      ColumnarPlanner.tryGroupedSum(plan) shouldBe None

  protected def sumsInts(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Ints => true
    case PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats | PhysicalOp.Kind.Pairs |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.Strings | PhysicalOp.Kind.Bools |
        PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples |
        PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.GroupedInts |
        PhysicalOp.Kind.GroupedStrings | PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter |
        PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums | PhysicalOp.Kind.LongCountSums |
        PhysicalOp.Kind.StringCountSums | PhysicalOp.Kind.IntLongSums |
        PhysicalOp.Kind.GroupedIntLongs | PhysicalOp.Kind.IntLongTriples |
        PhysicalOp.Kind.IntLongOuter | PhysicalOp.Kind.Float32s | PhysicalOp.Kind.Columns =>
      false

  protected def sumsLongs(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Longs => true
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Floats | PhysicalOp.Kind.Pairs |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.Strings | PhysicalOp.Kind.Bools |
        PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples |
        PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.GroupedInts |
        PhysicalOp.Kind.GroupedStrings | PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter |
        PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums | PhysicalOp.Kind.LongCountSums |
        PhysicalOp.Kind.StringCountSums | PhysicalOp.Kind.IntLongSums |
        PhysicalOp.Kind.GroupedIntLongs | PhysicalOp.Kind.IntLongTriples |
        PhysicalOp.Kind.IntLongOuter | PhysicalOp.Kind.Float32s | PhysicalOp.Kind.Columns =>
      false

  protected def sumsFloats(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Floats => true
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Pairs |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.Strings | PhysicalOp.Kind.Bools |
        PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples |
        PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.GroupedInts |
        PhysicalOp.Kind.GroupedStrings | PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter |
        PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums | PhysicalOp.Kind.LongCountSums |
        PhysicalOp.Kind.StringCountSums | PhysicalOp.Kind.IntLongSums |
        PhysicalOp.Kind.GroupedIntLongs | PhysicalOp.Kind.IntLongTriples |
        PhysicalOp.Kind.IntLongOuter | PhysicalOp.Kind.Float32s | PhysicalOp.Kind.Columns =>
      false

  protected def preservesPartitions(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Scan(_, _) => true
    case PhysicalOp.Filter(child, _) => preservesPartitions(child)
    case PhysicalOp.Project(child) => preservesPartitions(child)
    case PhysicalOp.WideFilter(child, _, _) => preservesPartitions(child)
    case PhysicalOp.WideProject(child, _) => preservesPartitions(child)
    case PhysicalOp.WideCompute(child, _, _) => preservesPartitions(child)
    case PhysicalOp.Pipeline(child, _) => preservesPartitions(child)
    case PhysicalOp.Column(child, _) => preservesPartitions(child)
    case PhysicalOp.MapValue(child) => preservesPartitions(child)
    case PhysicalOp.IntLongMapValue(child) => preservesPartitions(child)
    case PhysicalOp.MapKey(child) => preservesPartitions(child)
    case PhysicalOp.Exchange(_) | PhysicalOp.HashAggregate(_, _) |
        PhysicalOp.LongHashAggregate(_) | PhysicalOp.StringHashAggregate(_) |
        PhysicalOp.GroupByKey(_) | PhysicalOp.GroupedDecode(_) | PhysicalOp.CountByKey(_) |
        PhysicalOp.CountSum(_) | PhysicalOp.LongCountSum(_) | PhysicalOp.StringCountSum(_) |
        PhysicalOp.IntExtremum(_, _) | PhysicalOp.IntLongHashAggregate(_) |
        PhysicalOp.IntLongCountSum(_) | PhysicalOp.JoinProbeSum(_, _, _) |
        PhysicalOp.HashJoin(_, _) | PhysicalOp.LongHashJoin(_, _) |
        PhysicalOp.IntLongHashJoin(_, _) | PhysicalOp.IntLongHashLeftOuter(_, _) |
        PhysicalOp.IntLongHashRightOuter(_, _) | PhysicalOp.IntLongHashFullOuter(_, _) |
        PhysicalOp.IntLongBroadcastJoin(_, _, _) | PhysicalOp.IntLongBroadcastLeftOuter(_, _) |
        PhysicalOp.IntLongBroadcastRightOuter(_, _) | PhysicalOp.IntLongBroadcastFullOuter(_, _) |
        PhysicalOp.IntLongSortMergeJoin(_, _) | PhysicalOp.IntLongSortMergeLeftOuter(_, _) |
        PhysicalOp.IntLongSortMergeRightOuter(_, _) | PhysicalOp.IntLongSortMergeFullOuter(_, _) |
        PhysicalOp.LongSortMergeJoin(_, _) | PhysicalOp.StringHashJoin(_, _) |
        PhysicalOp.StringSortMergeJoin(_, _) | PhysicalOp.SortMergeJoin(_, _) |
        PhysicalOp.SortMergeLeftOuter(_, _) | PhysicalOp.SortMergeRightOuter(_, _) |
        PhysicalOp.SortMergeFullOuter(_, _) | PhysicalOp.HashLeftOuter(_, _) |
        PhysicalOp.HashRightOuter(_, _) | PhysicalOp.HashFullOuter(_, _) |
        PhysicalOp.BroadcastJoin(_, _, _) | PhysicalOp.LongBroadcastJoin(_, _, _) |
        PhysicalOp.StringBroadcastJoin(_, _, _) | PhysicalOp.BroadcastLeftOuter(_, _) |
        PhysicalOp.BroadcastRightOuter(_, _) | PhysicalOp.BroadcastFullOuter(_, _) |
        PhysicalOp.SortByIntKey(_) | PhysicalOp.SortByIntValueDesc(_) |
        PhysicalOp.SortByLongKey(_) | PhysicalOp.SortByStringKey(_) |
        PhysicalOp.SortByLongValueDesc(_) | PhysicalOp.SortByStringValueDesc(_) |
        PhysicalOp.SortByIntLongKey(_) | PhysicalOp.SortByIntLongValueDesc(_) |
        PhysicalOp.Union(_, _) | PhysicalOp.Dedupe(_) | PhysicalOp.LongHashLeftOuter(_, _) |
        PhysicalOp.LongHashRightOuter(_, _) | PhysicalOp.LongHashFullOuter(_, _) |
        PhysicalOp.LongBroadcastLeftOuter(_, _) | PhysicalOp.LongBroadcastRightOuter(_, _) |
        PhysicalOp.LongBroadcastFullOuter(_, _) | PhysicalOp.LongSortMergeLeftOuter(_, _) |
        PhysicalOp.LongSortMergeRightOuter(_, _) | PhysicalOp.LongSortMergeFullOuter(_, _) |
        PhysicalOp.StringHashLeftOuter(_, _) | PhysicalOp.StringHashRightOuter(_, _) |
        PhysicalOp.StringHashFullOuter(_, _) | PhysicalOp.StringBroadcastLeftOuter(_, _) |
        PhysicalOp.StringBroadcastRightOuter(_, _) | PhysicalOp.StringBroadcastFullOuter(_, _) |
        PhysicalOp.StringSortMergeLeftOuter(_, _) | PhysicalOp.StringSortMergeRightOuter(_, _) |
        PhysicalOp.StringSortMergeFullOuter(_, _) =>
      false

  /** A key sort is not partition-preserving, and `tryTake` still runs it. */
  protected def acceptsTake(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.SortByIntKey | _: PhysicalOp.SortByIntValueDesc |
        _: PhysicalOp.SortByLongKey | _: PhysicalOp.SortByStringKey |
        _: PhysicalOp.SortByLongValueDesc | _: PhysicalOp.SortByStringValueDesc =>
      true
    case other => preservesPartitions(other)

  protected def assertRowPath[A](plan: Plan[A]): Unit =
    ColumnarPlanner.partitioning(plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.lower(plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.tryRun(plan) shouldBe None
    ColumnarPlanner.tryCount(plan) shouldBe None
    ColumnarPlanner.trySumInt(plan) shouldBe None
    ColumnarPlanner.trySumLong(plan) shouldBe None
    ColumnarPlanner.trySumDouble(plan) shouldBe None
    ColumnarPlanner.tryTake(plan, 1) shouldBe None
    ColumnarPlanner.tryGroupedSum(plan) shouldBe None

  protected def collected[A](plan: Plan[A]): Seq[A] =
    ScarletRuntime.get.shuffle.clear()
    DistCollection(plan).collect().toSeq

  protected def rowResult[A](plan: Plan[A]): Seq[A] =
    rowAction(collected(plan))

  /** Columnar collect, sum, and count against the row path. `None` means they agree. */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  protected def longDisagreement(plan: Plan[Long]): Option[String] =
    val collectedRows = collected(plan)
    val rowRows = rowResult(plan)
    val columnarSum = DistCollection(plan).sum
    val rowSum = rowAction(DistCollection(plan).sum)
    val columnarCount = DistCollection(plan).count()
    val rowCount = rowAction(DistCollection(plan).count())
    val details = List(
      Option.when(collectedRows != rowRows)(s"collect $collectedRows != $rowRows"),
      Option.when(columnarSum != rowSum)(s"sum $columnarSum != $rowSum"),
      Option.when(columnarCount != rowCount)(s"count $columnarCount != $rowCount"),
    ).flatten
    if (details.isEmpty) then None else Some(details.mkString(", "))

  protected def rowAction[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  protected def acceptedLong(
      label: String,
      plan: Plan[Long],
      info: PartitioningInfo,
      physicalShape: Option[String],
  ): AcceptedCase =
    AcceptedCase(
      label,
      plan,
      info,
      physicalShape,
      longProbe = Some(() => longDisagreement(plan)),
    )

  protected def elementPipelineCases[A](
      kind: String,
      source: Plan[A],
      filterStep: Plan[A] => Plan[A],
      mapStep: Plan[A] => Plan[A],
      layout: Layout,
      probe: Plan[A] => Option[() => Option[String]] = (_: Plan[A]) => None,
  ): Seq[AcceptedCase] =
    for
      length <- 2 to 4
      pattern <- 0 until (1 << length)
    yield
      val plan = (0 until length).foldLeft(source) { (current, index) =>
        if (((pattern >>> index) & 1) == 0) then filterStep(current) else mapStep(current)
      }
      AcceptedCase(
        s"$kind element pipeline length=$length pattern=$pattern",
        plan,
        narrowInfo(2, layout),
        Some(s"Pipeline$length(Scan)"),
        longProbe = probe(plan),
      )

  protected def longPipelineCases[A](
      kind: String,
      source: Plan[A],
      filterStep: Plan[A] => Plan[A],
      mapStep: Plan[A] => Plan[A],
      layout: Layout,
      probe: Plan[A] => Option[() => Option[String]] = (_: Plan[A]) => None,
  ): Seq[AcceptedCase] =
    val fiveSteps = (0 until 5).foldLeft(source)((current, _) => mapStep(current))
    val sixSteps = filterStep(fiveSteps)
    Seq(
      AcceptedCase(
        s"$kind five-step element pipeline",
        fiveSteps,
        narrowInfo(2, layout),
        Some("Project(Pipeline4(Scan))"),
        longProbe = probe(fiveSteps),
      ),
      AcceptedCase(
        s"$kind six-step element pipeline",
        sixSteps,
        narrowInfo(2, layout),
        Some("Pipeline2(Pipeline4(Scan))"),
        longProbe = probe(sixSteps),
      ),
    )

  protected def planNode(plan: Plan[_]): PlanNode = plan match
    case Plan.MapPartitionsOp(_, _) => PlanNode.MapPartitions
    case Plan.RepartitionOp(_, _) => PlanNode.Repartition
    case Plan.CoalesceOp(_, _) => PlanNode.Coalesce
    case Plan.PartitionByOp(_, _) => PlanNode.PartitionBy
    case Plan.Source(_, _) => PlanNode.Source
    case Plan.MapOp(_, _) => PlanNode.Map
    // The typed key map shares the generic map label. The physical shape is MapKey.
    case Plan.MapIntKeyOp(_, _) => PlanNode.Map
    case Plan.FilterOp(_, _) => PlanNode.Filter
    case Plan.WideFilterOp(_, _, _) => PlanNode.Filter
    case Plan.WideProjectOp(_, _) => PlanNode.Map
    case Plan.WideComputeOp(_, _, _) => PlanNode.Map
    case Plan.FlatMapOp(_, _) => PlanNode.FlatMap
    case Plan.DistinctOp(_) => PlanNode.Distinct
    case Plan.UnionOp(_, _) => PlanNode.Union
    case Plan.KeysOp(_) => PlanNode.Keys
    case Plan.ValuesOp(_) => PlanNode.Values
    case Plan.MapValuesOp(_, _) => PlanNode.MapValues
    case Plan.FilterKeysOp(_, _) => PlanNode.FilterKeys
    case Plan.FilterValuesOp(_, _) => PlanNode.FilterValues
    case Plan.FlatMapValuesOp(_, _) => PlanNode.FlatMapValues
    case Plan.GroupByKeyOp(_) => PlanNode.GroupByKey
    case Plan.CountByKeyOp(_) => PlanNode.CountByKey
    case Plan.CountSumByKeyOp(_) | Plan.CountSumByLongKeyOp(_) | Plan.CountSumByStringKeyOp(_) |
        Plan.CountSumByIntLongKeyOp(_) | Plan.ExtremumByKeyOp(_, _) =>
      PlanNode.CountSumByKey
    case Plan.ReduceByKeyOp(_, _) => PlanNode.ReduceByKey
    case Plan.SortByOp(_, _, _) => PlanNode.SortBy
    // Typed key nodes deliberately share the generic PlanNode.SortBy label.
    case Plan.SortByIntKeyOp(_) => PlanNode.SortBy
    case Plan.SortByIntValueDescOp(_) => PlanNode.SortBy
    case Plan.SortByLongKeyOp(_) => PlanNode.SortBy
    case Plan.SortByStringKeyOp(_) => PlanNode.SortBy
    case Plan.SortByLongValueDescOp(_) => PlanNode.SortBy
    case Plan.SortByStringValueDescOp(_) => PlanNode.SortBy
    case Plan.SortByIntLongKeyOp(_) | Plan.SortByIntLongValueDescOp(_) => PlanNode.SortBy
    case Plan.JoinOp(_, _, _) => PlanNode.Join
    case Plan.LeftOuterJoinOp(_, _, _) => PlanNode.Join
    case Plan.RightOuterJoinOp(_, _, _) => PlanNode.Join
    case Plan.FullOuterJoinOp(_, _, _) => PlanNode.Join
    case Plan.CoGroupOp(_, _) => PlanNode.CoGroup

  /**
   * Structural peel label of an accepted grouped sum. A grouped int-pair source, including an
   * empty typed source, is `GroupedSum(Scan)`. A row, key, or value filter wraps that scan. A pair
   * map wraps that peel as `MapPair` only when the child peels. Any other accepted plan is `None`.
   * Eligibility is [[ColumnarPlanner.groupedPairSource]], so any grouped int-pair source is
   * included, not one particular source value.
   */
  protected def expectedGroupedPeel(plan: Plan[_]): Option[String] =
    def body(current: Plan[_]): Option[String] = current match
      case Plan.FilterOp(source, _) => body(source).map(inner => s"FilterRow($inner)")
      case Plan.FilterKeysOp(source, _) => body(source).map(inner => s"FilterKey($inner)")
      case Plan.FilterValuesOp(source, _) => body(source).map(inner => s"FilterValue($inner)")
      case Plan.MapOp(source, _) => body(source).map(inner => s"MapPair($inner)")
      case source: Plan.Source[_] if ColumnarPlanner.groupedPairSource(source) => Some("Scan")
      case _ => None

    body(plan).map(inner => s"GroupedSum($inner)")

  protected def narrowInfo(width: Int, layout: Layout): PartitioningInfo =
    PartitioningInfo(Distribution.Unknown(width), OrderingTag.Unsorted, layout)

  protected def rangeSortedInfo(layout: Layout): PartitioningInfo =
    PartitioningInfo(
      Distribution
        .Range(ScarletConf.get.defaultShufflePartitions, PartitioningInfo.RowKeyOrdinals),
      OrderingTag.Sorted(PartitioningInfo.RowKeyOrdinals, Seq(true)),
      layout,
    )

  protected def hashedInfo(layout: Layout): PartitioningInfo =
    PartitioningInfo(
      Distribution.Hash(ScarletConf.get.defaultShufflePartitions, PartitioningInfo.RowKeyOrdinals),
      OrderingTag.Unsorted,
      layout,
    )

  protected def columnarLayout: Layout = Layout.Columnar(ScarletConf.get.columnarBatchSize)

  protected def utf8Layout: Layout = Layout.Utf8Dict(ScarletConf.get.columnarBatchSize)
