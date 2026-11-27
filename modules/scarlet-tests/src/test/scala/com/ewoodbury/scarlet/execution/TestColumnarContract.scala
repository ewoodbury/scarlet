package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}

/** Executable inventory of the logical shapes selected by the columnar planner. */
class TestColumnarContract
    extends AnyFlatSpec
    with BeforeAndAfterEach
    with ColumnarContractFixture:

  override def beforeEach(): Unit =
    super.beforeEach()
    installContractConf()

  override def afterEach(): Unit =
    restoreContractConf()
    ()

  "the columnar contract" should "cover every Plan case and pin accepted layouts and shapes" in {
    val accepted = acceptedCases
    val excluded = excludedCases
    val covered = accepted.map(_.plan) ++ excluded.map(_.plan)

    accepted.size shouldBe 251
    // A null key refuses the shape rather than excluding it. Outer joins are covered by execution
    // tests, not contract fixtures, because the contract does not track the int refusals either.
    // A type-changing pair map lowers and declines at tryRun, so it is in neither list.
    excluded.size shouldBe 32
    (accepted.size + excluded.size) shouldBe 283
    val mismatches = accepted.flatMap { expected =>
      expected.longProbe match
        case None => Nil
        case Some(probe) => probe().toList.map(detail => s"${expected.label}: $detail")
    }
    accepted.count(_.longProbe.isDefined) shouldBe accepted.count { expected =>
      ColumnarPlanner.lower(expected.plan).exists(sumsLongs)
    }
    mismatches shouldBe empty
    covered.map(planNode).toSet shouldBe PlanNode.values.toSet
    // A refused plan has no layout. Acceptance is lower plus a valid layout.
    covered.count { plan =>
      ColumnarPlanner.lower(plan).isEmpty && ColumnarPlanner.partitioning(plan).isDefined
    } shouldBe 0
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
  }

  it should "agree on lower and every action gate for every contract fixture" in {
    acceptedCases.foreach { expected =>
      withClue(s"${expected.label}: ") {
        assertGatesAgree(expected.plan, accepted = true, groupedSum = expected.groupedSum)
      }
    }
    excludedCases.foreach { expected =>
      withClue(s"${expected.label}: ") {
        assertGatesAgree(expected.plan, accepted = false, groupedSum = None)
      }
    }
  }

  private[execution] def acceptedCases: Seq[AcceptedCase] =
    val intFilter = Plan.FilterOp(intSource, (value: Int) => value > 1)
    val intMap = Plan.MapOp(intSource, (value: Int) => value + 1)
    val longFilter = Plan.FilterOp(longSource, (value: Long) => value > 1L)
    val longMap = Plan.MapOp(longSource, (value: Long) => value + 1L)
    val boolFilter = Plan.FilterOp(boolSource, (value: Boolean) => value)
    val boolFilters = Plan.FilterOp(boolFilter, (value: Boolean) => !value)
    val doubleFilter = Plan.FilterOp(doubleSource, (value: Double) => value > 0.0)
    val doubleMap = Plan.MapOp(doubleSource, (value: Double) => value * 2.0)
    val stringFilter = Plan.FilterOp(stringSource, (value: String) => value.nonEmpty)
    val stringMap =
      Plan.MapOp(stringSource, (value: String) => value.toUpperCase(java.util.Locale.ENGLISH))
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
    val mapIntKey = Plan.MapIntKeyOp(pairSource, (key: Int) => key % 100)
    val pairMap = Plan.MapOp(pairSource, (pair: (Int, Int)) => (pair._1 + 1, pair._2 + pair._1))
    val mappedKeys = Plan.MapOp(keys, (key: Int) => key + 1)
    val mappedValues = Plan.MapOp(values, (value: Int) => value + 1)
    val filteredMapValues = Plan.FilterValuesOp(mapValues, (value: Int) => value > 0)
    val longKeys = Plan.KeysOp(longPairSource)
    val longValues = Plan.ValuesOp(longPairSource)
    val longMapValues = Plan.MapValuesOp(longPairSource, (value: Long) => value + 1L)
    val longPairFilter =
      Plan.FilterOp(longPairSource, (pair: (Long, Long)) => pair._1 > 0L)
    val longKeyFilter = Plan.FilterKeysOp(longPairSource, (key: Long) => key > 0L)
    val longValueFilter = Plan.FilterValuesOp(longPairSource, (value: Long) => value > 0L)
    val longPairMap =
      Plan.MapOp(longPairSource, (pair: (Long, Long)) => (pair._1 + 1L, pair._2))
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
    val broadcastJoin = DistCollection(pairSource)
      .broadcastJoin(DistCollection(rightPairSource))
      .plan
    val sortMergeJoin = DistCollection(pairSource)
      .sortMergeJoin(DistCollection(rightPairSource))
      .plan
    val joinAfterAggregate = DistCollection(reduced).join(DistCollection(rightPairSource)).plan
    val joinedCollection = DistCollection(tripleSource)
    val joinKeyFilter = joinedCollection.filterJoinKey(_ > 0).plan
    val joinLeftFilter = joinedCollection.filterJoinLeft(_ > 0).plan
    val joinRightFilter = joinedCollection.filterJoinRight(_ > 0).plan
    val tripleMapped = Plan.MapOp(
      tripleSource,
      (row: (Int, (Int, Int))) => (row._1, (row._2._1, row._2._2)),
    )
    val genericTripleFilter = Plan.FilterOp(
      tripleSource,
      (row: (Int, (Int, Int))) => row._2._1 > 0,
    )
    val layout = columnarLayout
    val stringLayout = utf8Layout
    val stringPairLayout = Layout.StringPair(ScarletConf.get.columnarBatchSize)
    val stringTripleLayout = Layout.StringTriple(ScarletConf.get.columnarBatchSize)
    val stringKeys = Plan.KeysOp(stringKeySource)
    val stringValues = Plan.ValuesOp(stringKeySource)
    val stringPairMapValues = Plan.MapValuesOp(stringKeySource, (value: Int) => value + 1)
    val stringPairMap =
      Plan.MapOp(stringKeySource, (pair: (String, Int)) => (pair._1 + "!", pair._2))

    Seq(
      AcceptedCase("int source", intSource, narrowInfo(2, layout), Some("Scan")),
      AcceptedCase("int filter", intFilter, narrowInfo(2, layout), Some("Filter(Scan)")),
      AcceptedCase("int map", intMap, narrowInfo(2, layout), Some("Project(Scan)")),
      acceptedLong("long source", longSource, narrowInfo(2, layout), Some("Scan")),
      acceptedLong("empty long source", emptyLongSource, narrowInfo(2, layout), Some("Scan")),
      acceptedLong("long filter", longFilter, narrowInfo(2, layout), Some("Filter(Scan)")),
      acceptedLong("long map", longMap, narrowInfo(2, layout), Some("Project(Scan)")),
      AcceptedCase("boolean source", boolSource, narrowInfo(2, layout), Some("Scan")),
      AcceptedCase("empty boolean source", emptyBoolSource, narrowInfo(2, layout), Some("Scan")),
      AcceptedCase("boolean filter", boolFilter, narrowInfo(2, layout), Some("Filter(Scan)")),
      AcceptedCase(
        "boolean filter chain",
        boolFilters,
        narrowInfo(2, layout),
        Some("Filter(Filter(Scan))"),
      ),
      AcceptedCase("double source", doubleSource, narrowInfo(2, layout), Some("Scan")),
      AcceptedCase("double filter", doubleFilter, narrowInfo(2, layout), Some("Filter(Scan)")),
      AcceptedCase("double map", doubleMap, narrowInfo(2, layout), Some("Project(Scan)")),
      AcceptedCase(
        "single-precision Float source",
        floatSource,
        narrowInfo(1, layout),
        Some("Scan"),
      ),
      AcceptedCase(
        "single-precision Float map",
        Plan.MapOp(floatSource, (value: Float) => value + 1.0f),
        narrowInfo(1, layout),
        Some("Project(Scan)"),
      ),
      AcceptedCase("string source", stringSource, narrowInfo(2, stringLayout), Some("Scan")),
      AcceptedCase(
        "string filter",
        stringFilter,
        narrowInfo(2, stringLayout),
        Some("Filter(Scan)"),
      ),
      AcceptedCase("string map", stringMap, narrowInfo(2, stringLayout), Some("Project(Scan)")),
      AcceptedCase(
        "dictionary string equality",
        dictionaryFilter,
        narrowInfo(2, stringLayout),
        Some("Filter(Scan)"),
      ),
      AcceptedCase(
        "dictionary equality then string map",
        dictionaryMap,
        narrowInfo(2, stringLayout),
        Some("Project(Filter(Scan))"),
      ),
      AcceptedCase("int pair source", pairSource, narrowInfo(2, layout), Some("Scan"), Some(60L)),
      AcceptedCase(
        "empty int pair source",
        emptyPairSource,
        narrowInfo(2, layout),
        Some("Scan"),
        Some(0L),
      ),
      AcceptedCase("long pair source", longPairSource, narrowInfo(2, layout), Some("Scan")),
      AcceptedCase(
        "empty long pair source",
        emptyLongPairSource,
        narrowInfo(2, layout),
        Some("Scan"),
      ),
      AcceptedCase(
        "int pair row filter",
        pairRowFilter,
        narrowInfo(2, layout),
        Some("FilterRow(Scan)"),
        Some(50L),
      ),
      AcceptedCase(
        "int pair key filter",
        pairKeyFilter,
        narrowInfo(2, layout),
        Some("FilterKey(Scan)"),
        Some(50L),
      ),
      AcceptedCase(
        "int pair value filter",
        pairValueFilter,
        narrowInfo(2, layout),
        Some("FilterValue(Scan)"),
        Some(50L),
      ),
      AcceptedCase(
        "int sortByIntKey",
        Plan.SortByIntKeyOp(pairSource),
        rangeSortedInfo(layout),
        Some("SortByIntKey(Scan)"),
      ),
      AcceptedCase(
        "filter over sortByIntKey",
        Plan.FilterOp(Plan.SortByIntKeyOp(pairSource), (pair: (Int, Int)) => pair._2 > 0),
        rangeSortedInfo(layout),
        Some("FilterRow(SortByIntKey(Scan))"),
      ),
      AcceptedCase(
        "long sortByLongKey",
        Plan.SortByLongKeyOp(longPairSource),
        rangeSortedInfo(layout),
        Some("SortByLongKey(Scan)"),
      ),
      AcceptedCase(
        "filter over sortByLongKey",
        Plan.FilterOp(
          Plan.SortByLongKeyOp(longPairSource),
          (pair: (Long, Long)) => pair._2 > 0L,
        ),
        rangeSortedInfo(layout),
        Some("FilterRow(SortByLongKey(Scan))"),
      ),
      AcceptedCase(
        "sortByStringKey of a string-pair scan",
        Plan.SortByStringKeyOp(stringKeySource),
        rangeSortedInfo(stringPairLayout),
        Some("SortByStringKey(Scan)"),
      ),
      AcceptedCase(
        "filter over sortByStringKey",
        Plan.FilterOp(
          Plan.SortByStringKeyOp(stringKeySource),
          (pair: (String, Int)) => pair._2 > 0,
        ),
        rangeSortedInfo(stringPairLayout),
        Some("FilterRow(SortByStringKey(Scan))"),
      ),
      AcceptedCase("int pair keys", keys, narrowInfo(2, layout), Some("Key(Scan)")),
      AcceptedCase("int pair values", values, narrowInfo(2, layout), Some("Value(Scan)")),
      AcceptedCase("int mapValues", mapValues, narrowInfo(2, layout), Some("MapValue(Scan)")),
      AcceptedCase(
        "int mapValues to long",
        Plan.MapValuesOp(pairSource, (value: Int) => value.toLong),
        narrowInfo(2, layout),
        Some("IntLongMapValue(Scan)"),
      ),
      AcceptedCase("int mapIntKey", mapIntKey, narrowInfo(2, layout), Some("MapKey(Scan)")),
      AcceptedCase(
        "keys of mapIntKey",
        Plan.KeysOp(mapIntKey),
        narrowInfo(2, layout),
        Some("Key(MapKey(Scan))"),
      ),
      AcceptedCase(
        "values of mapIntKey",
        Plan.ValuesOp(mapIntKey),
        narrowInfo(2, layout),
        Some("Value(MapKey(Scan))"),
      ),
      AcceptedCase(
        "filterKeys above mapIntKey",
        Plan.FilterKeysOp(mapIntKey, (key: Int) => key > 0),
        narrowInfo(2, layout),
        Some("FilterKey(MapKey(Scan))"),
      ),
      AcceptedCase(
        "filterValues above mapIntKey",
        Plan.FilterValuesOp(mapIntKey, (value: Int) => value > 0),
        narrowInfo(2, layout),
        Some("FilterValue(MapKey(Scan))"),
      ),
      AcceptedCase(
        "keys of string pairs",
        stringKeys,
        narrowInfo(1, stringLayout),
        Some("Key(Scan)"),
      ),
      AcceptedCase(
        "values of string pairs",
        stringValues,
        narrowInfo(1, layout),
        Some("Value(Scan)"),
      ),
      AcceptedCase(
        "mapValues on string-key pairs",
        stringPairMapValues,
        narrowInfo(1, stringPairLayout),
        Some("MapValue(Scan)"),
      ),
      AcceptedCase(
        "string filterKeys",
        Plan.FilterKeysOp(stringKeySource, (key: String) => key.nonEmpty),
        narrowInfo(1, stringPairLayout),
        Some("FilterKey(Scan)"),
      ),
      AcceptedCase(
        "string filterValues",
        Plan.FilterValuesOp(stringKeySource, (value: Int) => value > 0),
        narrowInfo(1, stringPairLayout),
        Some("FilterValue(Scan)"),
      ),
      AcceptedCase(
        "mapped int keys",
        mappedKeys,
        narrowInfo(2, layout),
        Some("Project(Key(Scan))"),
      ),
      AcceptedCase(
        "mapped int values",
        mappedValues,
        narrowInfo(2, layout),
        Some("Project(Value(Scan))"),
      ),
      AcceptedCase(
        "filtered int mapValues",
        filteredMapValues,
        narrowInfo(2, layout),
        Some("FilterValue(MapValue(Scan))"),
      ),
      acceptedLong("keys of long pairs", longKeys, narrowInfo(2, layout), Some("Key(Scan)")),
      acceptedLong("values of long pairs", longValues, narrowInfo(2, layout), Some("Value(Scan)")),
      AcceptedCase(
        "mapValues on long pairs",
        longMapValues,
        narrowInfo(2, layout),
        Some("MapValue(Scan)"),
      ),
      AcceptedCase(
        "filter of long pairs",
        longPairFilter,
        narrowInfo(2, layout),
        Some("FilterRow(Scan)"),
      ),
      AcceptedCase(
        "filterKeys of long pairs",
        longKeyFilter,
        narrowInfo(2, layout),
        Some("FilterKey(Scan)"),
      ),
      AcceptedCase(
        "filterValues of long pairs",
        longValueFilter,
        narrowInfo(2, layout),
        Some("FilterValue(Scan)"),
      ),
      AcceptedCase(
        "int reduceByKey",
        reduced,
        hashedInfo(layout),
        Some("HashAggregate(Exchange(Scan))"),
      ),
      AcceptedCase(
        "groupByKey int",
        Plan.GroupByKeyOp(pairSource),
        hashedInfo(Layout.GroupedInt(ScarletConf.get.columnarBatchSize)),
        Some("GroupByKey(Exchange(Scan))"),
      ),
      AcceptedCase(
        "keys of int groupByKey",
        Plan.KeysOp(Plan.GroupByKeyOp(pairSource)),
        narrowInfo(ScarletConf.get.defaultShufflePartitions, layout),
        Some("Key(CountByKey(Exchange(Scan)))"),
      ),
      acceptedLong(
        "keys of long groupByKey",
        Plan.KeysOp(Plan.GroupByKeyOp(longPairSource)),
        narrowInfo(ScarletConf.get.defaultShufflePartitions, layout),
        Some("Key(CountByKey(Exchange(Scan)))"),
      ),
      AcceptedCase(
        "keys of string groupByKey",
        Plan.KeysOp(Plan.GroupByKeyOp(stringKeySource)),
        narrowInfo(ScarletConf.get.defaultShufflePartitions, stringLayout),
        Some("Key(CountByKey(Exchange(Scan)))"),
      ),
      AcceptedCase(
        "countByKey int",
        Plan.CountByKeyOp(pairSource),
        hashedInfo(layout),
        Some("CountByKey(Exchange(Scan))"),
      ),
      AcceptedCase(
        "countSumByKey int",
        Plan.CountSumByKeyOp(pairSource),
        hashedInfo(layout),
        Some("CountSum(Exchange(Scan))"),
      ),
      AcceptedCase(
        "countByKey of mapIntKey",
        Plan.CountByKeyOp(mapIntKey),
        hashedInfo(layout),
        Some("CountByKey(Exchange(MapKey(Scan)))"),
      ),
      AcceptedCase(
        "countByKey of filterKeys above mapIntKey",
        Plan.CountByKeyOp(Plan.FilterKeysOp(mapIntKey, (key: Int) => key > 0)),
        hashedInfo(layout),
        Some("CountByKey(Exchange(FilterKey(MapKey(Scan))))"),
      ),
      AcceptedCase(
        "countByKey of filterValues above mapIntKey",
        Plan.CountByKeyOp(Plan.FilterValuesOp(mapIntKey, (value: Int) => value > 0)),
        hashedInfo(layout),
        Some("CountByKey(Exchange(FilterValue(MapKey(Scan))))"),
      ),
      AcceptedCase(
        "reduceByKey of mapIntKey",
        Plan.ReduceByKeyOp(mapIntKey, (left: Int, right: Int) => left + right),
        hashedInfo(layout),
        Some("HashAggregate(Exchange(MapKey(Scan)))"),
      ),
      AcceptedCase(
        "int pair map",
        pairMap,
        narrowInfo(2, layout),
        Some("Project(Scan)"),
        Some(66L),
      ),
      AcceptedCase(
        "countByKey of int pair map",
        Plan.CountByKeyOp(pairMap),
        hashedInfo(layout),
        Some("CountByKey(Exchange(Project(Scan)))"),
      ),
      AcceptedCase(
        "countByKey long",
        Plan.CountByKeyOp(longPairSource),
        hashedInfo(layout),
        Some("CountByKey(Exchange(Scan))"),
      ),
      AcceptedCase(
        "long pair map",
        longPairMap,
        narrowInfo(2, layout),
        Some("Project(Scan)"),
      ),
      AcceptedCase(
        "countByKey of long pair map",
        Plan.CountByKeyOp(longPairMap),
        hashedInfo(layout),
        Some("CountByKey(Exchange(Project(Scan)))"),
      ),
      AcceptedCase(
        "countByKey string",
        Plan.CountByKeyOp(stringKeySource),
        hashedInfo(stringPairLayout),
        Some("CountByKey(Exchange(Scan))"),
      ),
      AcceptedCase(
        "string pair map",
        stringPairMap,
        narrowInfo(1, stringPairLayout),
        Some("Project(Scan)"),
      ),
      AcceptedCase(
        "countByKey of string pair map",
        Plan.CountByKeyOp(stringPairMap),
        hashedInfo(stringPairLayout),
        Some("CountByKey(Exchange(Project(Scan)))"),
      ),
      AcceptedCase(
        "groupByKey with string keys",
        Plan.GroupByKeyOp(stringKeySource),
        hashedInfo(Layout.GroupedString(ScarletConf.get.columnarBatchSize)),
        Some("GroupByKey(Exchange(Scan))"),
      ),
      AcceptedCase(
        "groupByKey with long keys",
        Plan.GroupByKeyOp(longPairSource),
        hashedInfo(Layout.GroupedLong(ScarletConf.get.columnarBatchSize)),
        Some("GroupByKey(Exchange(Scan))"),
      ),
      AcceptedCase(
        "long reduceByKey",
        Plan.ReduceByKeyOp(longPairSource, (left: Long, right: Long) => left + right),
        hashedInfo(layout),
        Some("LongHashAggregate(Exchange(Scan))"),
      ),
      AcceptedCase(
        "reduceByKey with string keys",
        Plan.ReduceByKeyOp(stringKeySource, (left: Int, right: Int) => left + right),
        hashedInfo(stringPairLayout),
        Some("StringHashAggregate(Exchange(Scan))"),
      ),
      AcceptedCase(
        "long inner join",
        DistCollection(longPairSource).join(DistCollection(longPairSource)).plan,
        hashedInfo(layout),
        Some("LongHashJoin(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "explicit shuffle-hash long join",
        DistCollection(longPairSource).shuffleHashJoin(DistCollection(longPairSource)).plan,
        hashedInfo(layout),
        Some("LongHashJoin(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "explicit broadcast long join",
        DistCollection(longPairSource).broadcastJoin(DistCollection(longPairSource)).plan,
        narrowInfo(2, layout),
        Some("LongBroadcastJoin(Scan, Scan)"),
      ),
      AcceptedCase(
        "explicit sort-merge long join",
        DistCollection(longPairSource).sortMergeJoin(DistCollection(longPairSource)).plan,
        hashedInfo(layout),
        Some("LongSortMergeJoin(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "join with string keys",
        DistCollection(stringKeySource).join(DistCollection(stringKeySource)).plan,
        hashedInfo(stringTripleLayout),
        Some("StringHashJoin(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "explicit broadcast string join",
        DistCollection(stringKeySource).broadcastJoin(DistCollection(stringKeySource)).plan,
        narrowInfo(1, stringTripleLayout),
        Some("StringBroadcastJoin(Scan, Scan)"),
      ),
      AcceptedCase(
        "explicit sort-merge string join",
        DistCollection(stringKeySource).sortMergeJoin(DistCollection(stringKeySource)).plan,
        hashedInfo(stringTripleLayout),
        Some("StringSortMergeJoin(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "filtered int reduceByKey",
        reducedAfterFilter,
        hashedInfo(layout),
        Some("HashAggregate(Exchange(FilterValue(Scan)))"),
      ),
      AcceptedCase(
        "mapValues int reduceByKey",
        reducedAfterMapValues,
        hashedInfo(layout),
        Some("HashAggregate(Exchange(MapValue(Scan)))"),
      ),
      AcceptedCase(
        "int mapValues to long reduceByKey",
        Plan.ReduceByKeyOp(
          Plan.MapValuesOp(pairSource, (value: Int) => value.toLong),
          (left: Long, right: Long) => left + right,
        ),
        hashedInfo(layout),
        Some("HashAggregate(Exchange(IntLongMapValue(Scan)))"),
      ),
      AcceptedCase(
        "nested int reduceByKey",
        reducedTwice,
        hashedInfo(layout),
        Some("HashAggregate(Exchange(HashAggregate(Exchange(Scan))))"),
      ),
      AcceptedCase(
        "int hash join",
        joined,
        hashedInfo(layout),
        Some("HashJoin(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "explicit shuffle-hash join",
        shuffleHashJoin,
        hashedInfo(layout),
        Some("HashJoin(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "explicit broadcast join",
        broadcastJoin,
        narrowInfo(2, layout),
        Some("BroadcastJoin(Scan, Scan)"),
      ),
      AcceptedCase(
        "explicit sort-merge join",
        sortMergeJoin,
        hashedInfo(layout),
        Some("SortMergeJoin(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "broadcast left outer join",
        Plan.LeftOuterJoinOp(pairSource, rightPairSource, Some(Plan.JoinStrategy.Broadcast)),
        narrowInfo(2, layout),
        Some("BroadcastLeftOuter(Scan, Scan)"),
      ),
      AcceptedCase(
        "shuffle-hash left outer join",
        Plan.LeftOuterJoinOp(pairSource, rightPairSource, Some(Plan.JoinStrategy.ShuffleHash)),
        hashedInfo(layout),
        Some("HashLeftOuter(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "broadcast right outer join",
        Plan.RightOuterJoinOp(pairSource, rightPairSource, Some(Plan.JoinStrategy.Broadcast)),
        narrowInfo(2, layout),
        Some("BroadcastRightOuter(Scan, Scan)"),
      ),
      AcceptedCase(
        "shuffle-hash right outer join",
        Plan.RightOuterJoinOp(pairSource, rightPairSource, Some(Plan.JoinStrategy.ShuffleHash)),
        hashedInfo(layout),
        Some("HashRightOuter(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "right outer join",
        DistCollection(pairSource).rightOuterJoin(DistCollection(rightPairSource)).plan,
        hashedInfo(layout),
        Some("HashRightOuter(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "broadcast full outer join",
        Plan.FullOuterJoinOp(pairSource, rightPairSource, Some(Plan.JoinStrategy.Broadcast)),
        narrowInfo(2, layout),
        Some("BroadcastFullOuter(Scan, Scan)"),
      ),
      AcceptedCase(
        "shuffle-hash full outer join",
        Plan.FullOuterJoinOp(pairSource, rightPairSource, Some(Plan.JoinStrategy.ShuffleHash)),
        hashedInfo(layout),
        Some("HashFullOuter(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "full outer join",
        DistCollection(pairSource).fullOuterJoin(DistCollection(rightPairSource)).plan,
        hashedInfo(layout),
        Some("HashFullOuter(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "sort-merge left outer join",
        Plan.LeftOuterJoinOp(pairSource, rightPairSource, Some(Plan.JoinStrategy.SortMerge)),
        hashedInfo(layout),
        Some("SortMergeLeftOuter(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "sort-merge right outer join",
        Plan.RightOuterJoinOp(pairSource, rightPairSource, Some(Plan.JoinStrategy.SortMerge)),
        hashedInfo(layout),
        Some("SortMergeRightOuter(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "sort-merge full outer join",
        Plan.FullOuterJoinOp(pairSource, rightPairSource, Some(Plan.JoinStrategy.SortMerge)),
        hashedInfo(layout),
        Some("SortMergeFullOuter(Exchange(Scan), Exchange(Scan))"),
      ),
      AcceptedCase(
        "join after int aggregate",
        joinAfterAggregate,
        hashedInfo(layout),
        Some("HashJoin(Exchange(HashAggregate(Exchange(Scan))), Exchange(Scan))"),
      ),
      AcceptedCase(
        "union",
        Plan.UnionOp(intSource, intSource),
        narrowInfo(4, layout),
        Some("Union(Scan, Scan)"),
      ),
      AcceptedCase(
        "distinct ints",
        Plan.DistinctOp(intSource),
        PartitioningInfo(
          Distribution.Hash(ScarletConf.get.defaultShufflePartitions, List(0)),
          OrderingTag.Unsorted,
          layout,
        ),
        Some("Dedupe(Exchange(Scan))"),
      ),
      AcceptedCase(
        "distinct strings",
        Plan.DistinctOp(stringSource),
        hashedInfo(stringLayout),
        Some("Dedupe(Exchange(Scan))"),
      ),
      AcceptedCase(
        "join-key filter",
        joinKeyFilter,
        hashedInfo(layout),
        Some("FilterTripleKey(HashJoin(Exchange(Scan), Exchange(Scan)))"),
      ),
      AcceptedCase(
        "join-left-value filter",
        joinLeftFilter,
        hashedInfo(layout),
        Some("FilterTripleLeftValue(HashJoin(Exchange(Scan), Exchange(Scan)))"),
      ),
      AcceptedCase(
        "join-right-value filter",
        joinRightFilter,
        hashedInfo(layout),
        Some("FilterTripleRightValue(HashJoin(Exchange(Scan), Exchange(Scan)))"),
      ),
      AcceptedCase(
        "map on triples",
        tripleMapped,
        hashedInfo(layout),
        Some("Project(HashJoin(Exchange(Scan), Exchange(Scan)))"),
      ),
      AcceptedCase(
        "generic triple filter",
        genericTripleFilter,
        hashedInfo(layout),
        Some("FilterTripleRow(HashJoin(Exchange(Scan), Exchange(Scan)))"),
      ),
    ) ++
      elementPipelineCases(
        "int",
        intSource,
        plan => Plan.FilterOp(plan, (value: Int) => value > 1),
        plan => Plan.MapOp(plan, (value: Int) => value + 1),
        layout,
      ) ++
      elementPipelineCases(
        "long",
        longSource,
        plan => Plan.FilterOp(plan, (value: Long) => value > 1L),
        plan => Plan.MapOp(plan, (value: Long) => value + 1L),
        layout,
        probe = plan => Some(() => longDisagreement(plan)),
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
        "long",
        longSource,
        plan => Plan.FilterOp(plan, (value: Long) => value > 0L),
        plan => Plan.MapOp(plan, (value: Long) => value + 1L),
        layout,
        probe = plan => Some(() => longDisagreement(plan)),
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
      ) ++
      groupedDecodeCases ++
      aggregateAdmissionCases ++
      joinAdmissionCases

  private def excludedCases: Seq[ExcludedCase] =
    val doubleKeys = Plan.KeysOp(doubleKeySource)
    val doubleValues = Plan.ValuesOp(doubleKeySource)
    val reduceDoubleKeys = Plan.ReduceByKeyOp(
      doubleKeySource,
      (left: Int, right: Int) => left + right,
    )
    val groupDoubleKeys = Plan.GroupByKeyOp(doubleKeySource)
    val doubleKeyJoin = Plan.JoinOp(doubleKeySource, doubleKeySource, None)
    val stringMapValues = Plan.MapValuesOp(pairSource, (value: Int) => value.toString)
    val doubleMapValues = Plan.MapValuesOp(pairSource, (value: Int) => value.toDouble)
    val doublePairMapValues = Plan.MapValuesOp(doubleKeySource, (value: Int) => value + 1)

    Seq(
      ExcludedCase(
        "mapPartitions",
        Plan.MapPartitionsOp(intSource, (values: Iterator[Int]) => values),
      ),
      ExcludedCase("repartition", Plan.RepartitionOp(intSource, 2)),
      ExcludedCase("coalesce", Plan.CoalesceOp(intSource, 1)),
      ExcludedCase("partitionBy", Plan.PartitionByOp(pairSource, 2)),
      ExcludedCase("flatMap", Plan.FlatMapOp(intSource, (value: Int) => List(value, value + 1))),
      ExcludedCase("flatMapValues", Plan.FlatMapValuesOp(pairSource, (value: Int) => List(value))),
      ExcludedCase("cogroup", Plan.CoGroupOp(pairSource, rightPairSource)),
      ExcludedCase("sortBy", Plan.SortByOp(intSource, (value: Int) => value, Ordering.Int)),
      ExcludedCase("mapValues to string", stringMapValues),
      ExcludedCase("mapValues to double", doubleMapValues),
      ExcludedCase("keys of double pairs", doubleKeys),
      ExcludedCase("values of double pairs", doubleValues),
      ExcludedCase("mapValues on double-key pairs", doublePairMapValues),
      ExcludedCase(
        "double filterKeys",
        Plan.FilterKeysOp(doubleKeySource, (key: Double) => key > 0.0),
      ),
      ExcludedCase(
        "double filterValues",
        Plan.FilterValuesOp(doubleKeySource, (value: Int) => value > 0),
      ),
      ExcludedCase("reduceByKey with double keys", reduceDoubleKeys),
      ExcludedCase("groupByKey with double keys", groupDoubleKeys),
      ExcludedCase("join with double keys", doubleKeyJoin),
      ExcludedCase("all-null string source", allNullStringSource),
      ExcludedCase("empty int source without a witness", emptyIntSource),
      ExcludedCase("boolean map to boolean", Plan.MapOp(boolSource, (value: Boolean) => value)),
      ExcludedCase(
        "boolean map to int",
        Plan.MapOp(boolSource, (value: Boolean) => if (value) 1 else 0),
      ),
      booleanFilterThenMap,
      ExcludedCase("int map to long", Plan.MapOp(intSource, (value: Int) => value.toLong)),
      ExcludedCase("long map to int", Plan.MapOp(longSource, (value: Long) => value.toInt)),
      ExcludedCase("long map to string", Plan.MapOp(longSource, (value: Long) => value.toString)),
      ExcludedCase("int map to pair", Plan.MapOp(intSource, (value: Int) => (value, value))),
      ExcludedCase(
        "int filter then map to string",
        Plan.MapOp(
          Plan.FilterOp(intSource, (value: Int) => value > 0),
          (value: Int) => value.toString,
        ),
      ),
      ExcludedCase("double map to int", Plan.MapOp(doubleSource, (value: Double) => value.toInt)),
      ExcludedCase(
        "string map via length",
        Plan.MapOp(stringSource, (value: String) => value.length),
      ),
      ExcludedCase("int map to string", Plan.MapOp(intSource, (value: Int) => value.toString)),
      ExcludedCase(
        "type-changing map inside a three-step chain",
        Plan.FilterOp(
          Plan.MapOp(
            Plan.MapOp(intSource, (value: Int) => value + 1),
            (value: Int) => value.toString,
          ),
          (value: String) => value.nonEmpty,
        ),
      ),
    )
