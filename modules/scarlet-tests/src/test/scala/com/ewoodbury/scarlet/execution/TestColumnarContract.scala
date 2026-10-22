package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.BatchCodec
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/** Executable inventory of the logical shapes selected by the columnar planner. */
class TestColumnarContract extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "the columnar contract" should "cover every Plan case and pin accepted layouts and shapes" in {
    val accepted = acceptedCases
    val excluded = excludedCases
    val mismatches = plannerMismatchCases
    val allPlans = accepted.map(_.plan) ++ excluded.map(_.plan) ++ mismatches.map(_.plan)

    accepted.size shouldBe 121
    excluded.size shouldBe 37
    mismatches.size shouldBe 6
    allPlans.map(planNode).toSet shouldBe PlanNode.values.toSet
    accepted.foreach { expected =>
      withClue(s"${expected.label} (${planNode(expected.plan)}): ") {
        ColumnarPlanner.partitioning(expected.plan) shouldBe Some(expected.info)
        ColumnarPlanner.lower(expected.plan).map(_.shape) shouldBe expected.physicalShape
      }
    }
    excluded.foreach { expected =>
      withClue(s"${expected.label} (${planNode(expected.plan)}): ") {
        ColumnarPlanner.partitioning(expected.plan) shouldBe Option.empty[PartitioningInfo]
      }
    }
    mismatches.foreach { expected =>
      withClue(s"${expected.label} (${planNode(expected.plan)}): ") {
        ColumnarPlanner.partitioning(expected.plan) shouldBe Some(expected.info)
        ColumnarPlanner.lower(expected.plan).map(_.shape) shouldBe expected.physicalShape
        ColumnarPlanner.tryRun(expected.plan).isEmpty shouldBe expected.rowFallback
      }
    }
  }

  it should "keep fused grouped sum action-level and bounded take on its accepted physical shape" in {
    val mappedPairs = Plan.MapOp(pairSource, (pair: (Int, Int)) => (pair._1 + 1, pair._2 + pair._1))
    ColumnarPlanner.partitioning(mappedPairs).map(_.layout) shouldBe Some(columnarLayout)
    ColumnarPlanner.lower(mappedPairs) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.tryGroupedSum(mappedPairs) shouldBe Some(66L)
    ColumnarPlanner.tryGroupedSum(pairSource) shouldBe Some(60L)
    ColumnarPlanner.tryGroupedSum(Plan.FilterKeysOp(pairSource, (key: Int) => key > 1)) shouldBe
      Some(50L)
    ColumnarPlanner.tryGroupedSum(Plan.FilterValuesOp(pairSource, (value: Int) => value > 10)) shouldBe
      Some(50L)
    ColumnarPlanner.tryGroupedSum(Plan.FilterOp(pairSource, (pair: (Int, Int)) => pair._1 > 1)) shouldBe
      Some(50L)

    val intPipeline = Plan.MapOp(
      Plan.FilterOp(intSource, (value: Int) => value % 2 == 0),
      (value: Int) => value * 10,
    )
    ColumnarPlanner.lower(intPipeline).map(_.shape) shouldBe Some("Pipeline2(Scan)")
    ColumnarPlanner.tryTake(intPipeline, 2) shouldBe Some(Seq(20, 40))

    val pairFilter = Plan.FilterValuesOp(pairSource, (value: Int) => value > 10)
    ColumnarPlanner.lower(pairFilter).map(_.shape) shouldBe Some("FilterValue(Scan)")
    ColumnarPlanner.tryTake(pairFilter, 1) shouldBe Some(Seq(2 -> 20))

    val stringPipeline = DistCollection(stringSource)
      .filterStringEquals("red")
      .map(_.toUpperCase(java.util.Locale.ENGLISH))
      .plan
    ColumnarPlanner.lower(stringPipeline).map(_.shape) shouldBe Some("Project(Filter(Scan))")
    ColumnarPlanner.tryTake(stringPipeline, 1) shouldBe Some(Seq("RED"))
  }

  private final case class AcceptedCase(
      label: String,
      plan: Plan[_],
      info: PartitioningInfo,
      physicalShape: Option[String],
  )

  private final case class ExcludedCase(label: String, plan: Plan[_])

  private final case class PlannerMismatchCase(
      label: String,
      plan: Plan[_],
      info: PartitioningInfo,
      physicalShape: Option[String],
      rowFallback: Boolean,
  )

  private enum PlanNode:
    case MapPartitions, Repartition, Coalesce, PartitionBy, Source, Map, Filter, FlatMap, Distinct
    case Union, Keys, Values, MapValues, FilterKeys, FilterValues, FlatMapValues, GroupByKey
    case ReduceByKey, SortBy, Join, CoGroup

  private val intSource: Plan[Int] = Plan.Source(
    Seq(Partition(Seq(1, 2)), Partition(Seq(3, 4))),
  )
  private val doubleSource: Plan[Double] = Plan.Source(
    Seq(Partition(Seq(1.5, 2.5)), Partition(Seq(3.5))),
  )
  private val stringSource: Plan[String] = Plan.Source(
    Seq(Partition(Seq("red", "blue")), Partition(Seq("green"))),
  )
  private val pairSource: Plan[(Int, Int)] = Plan.Source(
    Seq(Partition(Seq(1 -> 10)), Partition(Seq(2 -> 20, 3 -> 30))),
  )
  private val rightPairSource: Plan[(Int, Int)] = Plan.Source(
    Seq(Partition(Seq(1 -> 100, 2 -> 200)), Partition(Seq(3 -> 300))),
  )
  private val stringKeySource: Plan[(String, Int)] = Plan.Source(
    Seq(Partition(Seq("red" -> 1))),
  )
  private val doubleKeySource: Plan[(Double, Int)] = Plan.Source(
    Seq(Partition(Seq(1.5 -> 1))),
  )
  private val emptyPairSource: Plan[(Int, Int)] = Plan.Source(
    Seq(Partition(Seq.empty[(Int, Int)])),
  )
  private val emptyIntSource: Plan[Int] = Plan.Source(Seq(Partition(Seq.empty[Int])))
  private val nullText: String = BatchCodec.stringElement(None)
  private val allNullStringSource: Plan[String] = Plan.Source(
    Seq(Partition(Seq(nullText, nullText))),
  )
  private val floatSource: Plan[Float] = Plan.Source(Seq(Partition(Seq(1.0f, 2.0f))))
  private val tripleSource: Plan[(Int, (Int, Int))] = DistCollection(pairSource)
    .join(DistCollection(rightPairSource))
    .plan

  private def acceptedCases: Seq[AcceptedCase] =
    val intFilter = Plan.FilterOp(intSource, (value: Int) => value > 1)
    val intMap = Plan.MapOp(intSource, (value: Int) => value + 1)
    val doubleFilter = Plan.FilterOp(doubleSource, (value: Double) => value > 0.0)
    val doubleMap = Plan.MapOp(doubleSource, (value: Double) => value * 2.0)
    val stringFilter = Plan.FilterOp(stringSource, (value: String) => value.nonEmpty)
    val stringMap = Plan.MapOp(stringSource, (value: String) => value.toUpperCase(java.util.Locale.ENGLISH))
    val dictionaryMap = DistCollection(stringSource)
      .filterStringEquals("red")
      .map(_.toUpperCase(java.util.Locale.ENGLISH))
      .plan
    val dictionaryFilter = DistCollection(stringSource).filterStringEquals("red").plan
    val pairRowFilter = Plan.FilterOp(pairSource, (pair: (Int, Int)) => pair._2 > 10)
    val pairKeyFilter = Plan.FilterKeysOp(pairSource, (key: Int) => key > 1)
    val pairValueFilter = Plan.FilterValuesOp(pairSource, (value: Int) => value > 10)
    val keys = Plan.KeysOp(pairSource)
    val values = Plan.ValuesOp(pairSource)
    val mapValues = Plan.MapValuesOp(pairSource, (value: Int) => value + 1)
    val mappedKeys = Plan.MapOp(keys, (key: Int) => key + 1)
    val mappedValues = Plan.MapOp(values, (value: Int) => value + 1)
    val filteredMapValues = Plan.FilterValuesOp(mapValues, (value: Int) => value > 0)
    val reduced = Plan.ReduceByKeyOp(pairSource, (left: Int, right: Int) => left + right)
    val reducedAfterFilter = Plan.ReduceByKeyOp(
      pairValueFilter,
      (left: Int, right: Int) => left + right,
    )
    val reducedAfterMapValues = Plan.ReduceByKeyOp(
      mapValues,
      (left: Int, right: Int) => left + right,
    )
    val reducedTwice = Plan.ReduceByKeyOp(
      reduced,
      (left: Int, right: Int) => left + right,
    )
    val joined = DistCollection(pairSource).join(DistCollection(rightPairSource)).plan
    val shuffleHashJoin = DistCollection(pairSource)
      .shuffleHashJoin(DistCollection(rightPairSource))
      .plan
    val joinAfterAggregate = DistCollection(reduced).join(DistCollection(rightPairSource)).plan
    val joinedCollection = DistCollection(tripleSource)
    val joinKeyFilter = joinedCollection.filterJoinKey(_ > 0).plan
    val joinLeftFilter = joinedCollection.filterJoinLeft(_ > 0).plan
    val joinRightFilter = joinedCollection.filterJoinRight(_ > 0).plan
    val layout = columnarLayout
    val stringLayout = utf8Layout

    Seq(
      AcceptedCase("int source", intSource, narrowInfo(2, layout), Some("Scan")),
      AcceptedCase("int filter", intFilter, narrowInfo(2, layout), Some("Filter(Scan)")),
      AcceptedCase("int map", intMap, narrowInfo(2, layout), Some("Project(Scan)")),
      AcceptedCase("double source", doubleSource, narrowInfo(2, layout), Some("Scan")),
      AcceptedCase("double filter", doubleFilter, narrowInfo(2, layout), Some("Filter(Scan)")),
      AcceptedCase("double map", doubleMap, narrowInfo(2, layout), Some("Project(Scan)")),
      AcceptedCase("string source", stringSource, narrowInfo(2, stringLayout), Some("Scan")),
      AcceptedCase("string filter", stringFilter, narrowInfo(2, stringLayout), Some("Filter(Scan)")),
      AcceptedCase("string map", stringMap, narrowInfo(2, stringLayout), Some("Project(Scan)")),
      AcceptedCase("dictionary string equality", dictionaryFilter, narrowInfo(2, stringLayout), Some("Filter(Scan)")),
      AcceptedCase("dictionary equality then string map", dictionaryMap, narrowInfo(2, stringLayout), Some("Project(Filter(Scan))")),
      AcceptedCase("int pair source", pairSource, narrowInfo(2, layout), Some("Scan")),
      AcceptedCase("int pair row filter", pairRowFilter, narrowInfo(2, layout), Some("FilterRow(Scan)")),
      AcceptedCase("int pair key filter", pairKeyFilter, narrowInfo(2, layout), Some("FilterKey(Scan)")),
      AcceptedCase("int pair value filter", pairValueFilter, narrowInfo(2, layout), Some("FilterValue(Scan)")),
      AcceptedCase("int pair keys", keys, narrowInfo(2, layout), Some("Key(Scan)")),
      AcceptedCase("int pair values", values, narrowInfo(2, layout), Some("Value(Scan)")),
      AcceptedCase("int mapValues", mapValues, narrowInfo(2, layout), Some("MapValue(Scan)")),
      AcceptedCase("mapped int keys", mappedKeys, narrowInfo(2, layout), Some("Project(Key(Scan))")),
      AcceptedCase("mapped int values", mappedValues, narrowInfo(2, layout), Some("Project(Value(Scan))")),
      AcceptedCase("filtered int mapValues", filteredMapValues, narrowInfo(2, layout), Some("FilterValue(MapValue(Scan))")),
      AcceptedCase("int reduceByKey", reduced, hashedInfo(layout), Some("HashAggregate(Exchange(Scan))")),
      AcceptedCase("filtered int reduceByKey", reducedAfterFilter, hashedInfo(layout), Some("HashAggregate(Exchange(FilterValue(Scan)))")),
      AcceptedCase("mapValues int reduceByKey", reducedAfterMapValues, hashedInfo(layout), Some("HashAggregate(Exchange(MapValue(Scan)))")),
      AcceptedCase("nested int reduceByKey", reducedTwice, hashedInfo(layout), Some("HashAggregate(Exchange(HashAggregate(Exchange(Scan))))")),
      AcceptedCase("int hash join", joined, hashedInfo(layout), Some("HashJoin(Exchange(Scan), Exchange(Scan))")),
      AcceptedCase("explicit shuffle-hash join", shuffleHashJoin, hashedInfo(layout), Some("HashJoin(Exchange(Scan), Exchange(Scan))")),
      AcceptedCase("join after int aggregate", joinAfterAggregate, hashedInfo(layout), Some("HashJoin(Exchange(HashAggregate(Exchange(Scan))), Exchange(Scan))")),
      AcceptedCase("join-key filter", joinKeyFilter, hashedInfo(layout), Some("FilterTripleKey(HashJoin(Exchange(Scan), Exchange(Scan)))")),
      AcceptedCase("join-left-value filter", joinLeftFilter, hashedInfo(layout), Some("FilterTripleLeftValue(HashJoin(Exchange(Scan), Exchange(Scan)))")),
      AcceptedCase("join-right-value filter", joinRightFilter, hashedInfo(layout), Some("FilterTripleRightValue(HashJoin(Exchange(Scan), Exchange(Scan)))")),
    ) ++
      elementPipelineCases(
        "int",
        intSource,
        plan => Plan.FilterOp(plan, (value: Int) => value > 1),
        plan => Plan.MapOp(plan, (value: Int) => value + 1),
        layout,
      ) ++
      elementPipelineCases(
        "double",
        doubleSource,
        plan => Plan.FilterOp(plan, (value: Double) => value > 0.0),
        plan => Plan.MapOp(plan, (value: Double) => value + 1.0),
        layout,
      ) ++
      elementPipelineCases(
        "string",
        stringSource,
        plan => Plan.FilterOp(plan, (value: String) => value.nonEmpty),
        plan => Plan.MapOp(plan, (value: String) => value + "!"),
        stringLayout,
      ) ++
      longPipelineCases(
        "int",
        intSource,
        plan => Plan.FilterOp(plan, (value: Int) => value > 0),
        plan => Plan.MapOp(plan, (value: Int) => value + 1),
        layout,
      ) ++
      longPipelineCases(
        "double",
        doubleSource,
        plan => Plan.FilterOp(plan, (value: Double) => value > 0.0),
        plan => Plan.MapOp(plan, (value: Double) => value + 1.0),
        layout,
      ) ++
      longPipelineCases(
        "string",
        stringSource,
        plan => Plan.FilterOp(plan, (value: String) => value.nonEmpty),
        plan => Plan.MapOp(plan, (value: String) => value + "!"),
        stringLayout,
      )

  private def excludedCases: Seq[ExcludedCase] =
    val stringKeys = Plan.KeysOp(stringKeySource)
    val stringValues = Plan.ValuesOp(stringKeySource)
    val doubleKeys = Plan.KeysOp(doubleKeySource)
    val doubleValues = Plan.ValuesOp(doubleKeySource)
    val floatMap = Plan.MapOp(floatSource, (value: Float) => value + 1.0f)
    val tripleMapped = Plan.MapOp(
      tripleSource,
      (row: (Int, (Int, Int))) => (row._1, (row._2._1, row._2._2)),
    )
    val genericTripleFilter = Plan.FilterOp(
      tripleSource,
      (row: (Int, (Int, Int))) => row._2._1 > 0,
    )
    val reduceStringKeys = Plan.ReduceByKeyOp(
      stringKeySource,
      (left: Int, right: Int) => left + right,
    )
    val reduceDoubleKeys = Plan.ReduceByKeyOp(
      doubleKeySource,
      (left: Int, right: Int) => left + right,
    )
    val groupStringKeys = Plan.GroupByKeyOp(stringKeySource)
    val groupDoubleKeys = Plan.GroupByKeyOp(doubleKeySource)
    val stringKeyJoin = Plan.JoinOp(stringKeySource, stringKeySource, None)
    val doubleKeyJoin = Plan.JoinOp(doubleKeySource, doubleKeySource, None)
    val stringMapValues = Plan.MapValuesOp(pairSource, (value: Int) => value.toString)
    val doubleMapValues = Plan.MapValuesOp(pairSource, (value: Int) => value.toDouble)
    val stringPairMapValues = Plan.MapValuesOp(stringKeySource, (value: Int) => value + 1)
    val doublePairMapValues = Plan.MapValuesOp(doubleKeySource, (value: Int) => value + 1)

    Seq(
      ExcludedCase("mapPartitions", Plan.MapPartitionsOp(intSource, (values: Iterator[Int]) => values)),
      ExcludedCase("repartition", Plan.RepartitionOp(intSource, 2)),
      ExcludedCase("coalesce", Plan.CoalesceOp(intSource, 1)),
      ExcludedCase("partitionBy", Plan.PartitionByOp(pairSource, 2)),
      ExcludedCase("flatMap", Plan.FlatMapOp(intSource, (value: Int) => List(value, value + 1))),
      ExcludedCase("distinct", Plan.DistinctOp(intSource)),
      ExcludedCase("union", Plan.UnionOp(intSource, intSource)),
      ExcludedCase("flatMapValues", Plan.FlatMapValuesOp(pairSource, (value: Int) => List(value))),
      ExcludedCase("groupByKey int", Plan.GroupByKeyOp(pairSource)),
      ExcludedCase("cogroup", Plan.CoGroupOp(pairSource, rightPairSource)),
      ExcludedCase("sortBy", Plan.SortByOp(intSource, (value: Int) => value, Ordering.Int)),
      ExcludedCase("map on triples", tripleMapped),
      ExcludedCase("generic triple filter", genericTripleFilter),
      ExcludedCase("mapValues to string", stringMapValues),
      ExcludedCase("mapValues to double", doubleMapValues),
      ExcludedCase("keys of string pairs", stringKeys),
      ExcludedCase("values of string pairs", stringValues),
      ExcludedCase("keys of double pairs", doubleKeys),
      ExcludedCase("values of double pairs", doubleValues),
      ExcludedCase("mapValues on string-key pairs", stringPairMapValues),
      ExcludedCase("mapValues on double-key pairs", doublePairMapValues),
      ExcludedCase("string filterKeys", Plan.FilterKeysOp(stringKeySource, (key: String) => key.nonEmpty)),
      ExcludedCase("double filterKeys", Plan.FilterKeysOp(doubleKeySource, (key: Double) => key > 0.0)),
      ExcludedCase("string filterValues", Plan.FilterValuesOp(stringKeySource, (value: Int) => value > 0)),
      ExcludedCase("double filterValues", Plan.FilterValuesOp(doubleKeySource, (value: Int) => value > 0)),
      ExcludedCase("reduceByKey with string keys", reduceStringKeys),
      ExcludedCase("reduceByKey with double keys", reduceDoubleKeys),
      ExcludedCase("groupByKey with string keys", groupStringKeys),
      ExcludedCase("groupByKey with double keys", groupDoubleKeys),
      ExcludedCase("join with string keys", stringKeyJoin),
      ExcludedCase("join with double keys", doubleKeyJoin),
      ExcludedCase("single-precision Float source", floatSource),
      ExcludedCase("single-precision Float map", floatMap),
      ExcludedCase("all-null string source", allNullStringSource),
      ExcludedCase("empty int source without a witness", emptyIntSource),
      ExcludedCase("empty pair source without a witness", emptyPairSource),
      ExcludedCase("unsupported Boolean source", Plan.Source(Seq(Partition(Seq(true, false))))),
    )

  private def plannerMismatchCases: Seq[PlannerMismatchCase] =
    Seq(
      PlannerMismatchCase(
        "int map changes type but planner still reports a columnar projection",
        Plan.MapOp(intSource, (value: Int) => value.toString),
        narrowInfo(2, columnarLayout),
        Some("Project(Scan)"),
        true,
      ),
      PlannerMismatchCase(
        "double map changes type but planner still reports a columnar projection",
        Plan.MapOp(doubleSource, (value: Double) => value.toInt),
        narrowInfo(2, columnarLayout),
        Some("Project(Scan)"),
        true,
      ),
      PlannerMismatchCase(
        "string map changes type but planner still reports a dictionary projection",
        Plan.MapOp(stringSource, (value: String) => value.length),
        narrowInfo(2, utf8Layout),
        Some("Project(Scan)"),
        true,
      ),
      PlannerMismatchCase(
        "explicit sort-merge join currently lowers as a hash join",
        DistCollection(pairSource).sortMergeJoin(DistCollection(rightPairSource)).plan,
        hashedInfo(columnarLayout),
        Some("HashJoin(Exchange(Scan), Exchange(Scan))"),
        false,
      ),
      PlannerMismatchCase(
        "explicit broadcast join currently lowers as a hash join",
        DistCollection(pairSource).broadcastJoin(DistCollection(rightPairSource)).plan,
        hashedInfo(columnarLayout),
        Some("HashJoin(Exchange(Scan), Exchange(Scan))"),
        false,
      ),
      PlannerMismatchCase(
        "pair map has a fused groupedSum layout but no physical shape",
        Plan.MapOp(pairSource, (pair: (Int, Int)) => (pair._1 + 1, pair._2 + pair._1)),
        narrowInfo(2, columnarLayout),
        Option.empty[String],
        true,
      ),
    )

  private def elementPipelineCases[A](
      kind: String,
      source: Plan[A],
      filterStep: Plan[A] => Plan[A],
      mapStep: Plan[A] => Plan[A],
      layout: Layout,
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
      )

  private def longPipelineCases[A](
      kind: String,
      source: Plan[A],
      filterStep: Plan[A] => Plan[A],
      mapStep: Plan[A] => Plan[A],
      layout: Layout,
  ): Seq[AcceptedCase] =
    val fiveSteps = (0 until 5).foldLeft(source)((current, _) => mapStep(current))
    val sixSteps = filterStep(fiveSteps)
    Seq(
      AcceptedCase(
        s"$kind five-step element pipeline",
        fiveSteps,
        narrowInfo(2, layout),
        Some("Project(Pipeline4(Scan))"),
      ),
      AcceptedCase(
        s"$kind six-step element pipeline",
        sixSteps,
        narrowInfo(2, layout),
        Some("Pipeline2(Pipeline4(Scan))"),
      ),
    )

  private def planNode(plan: Plan[_]): PlanNode = plan match
    case Plan.MapPartitionsOp(_, _) => PlanNode.MapPartitions
    case Plan.RepartitionOp(_, _) => PlanNode.Repartition
    case Plan.CoalesceOp(_, _) => PlanNode.Coalesce
    case Plan.PartitionByOp(_, _) => PlanNode.PartitionBy
    case Plan.Source(_) => PlanNode.Source
    case Plan.MapOp(_, _) => PlanNode.Map
    case Plan.FilterOp(_, _) => PlanNode.Filter
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
    case Plan.ReduceByKeyOp(_, _) => PlanNode.ReduceByKey
    case Plan.SortByOp(_, _, _) => PlanNode.SortBy
    case Plan.JoinOp(_, _, _) => PlanNode.Join
    case Plan.CoGroupOp(_, _) => PlanNode.CoGroup

  private def narrowInfo(width: Int, layout: Layout): PartitioningInfo =
    PartitioningInfo(Distribution.Unknown(width), OrderingTag.Unsorted, layout)

  private def hashedInfo(layout: Layout): PartitioningInfo =
    PartitioningInfo(
      Distribution.Hash(ScarletConf.get.defaultShufflePartitions, PartitioningInfo.RowKeyOrdinals),
      OrderingTag.Unsorted,
      layout,
    )

  private def columnarLayout: Layout = Layout.Columnar(ScarletConf.get.columnarBatchSize)

  private def utf8Layout: Layout = Layout.Utf8Dict(ScarletConf.get.columnarBatchSize)
