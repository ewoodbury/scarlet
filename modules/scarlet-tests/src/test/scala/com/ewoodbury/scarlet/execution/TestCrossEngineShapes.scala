package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnarFallback, LogicalType}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Known answers for the added cross-engine shapes, on one small int table.
 *
 * The filter is `k % 3 != 0` and `v >= 0`, except the top-N shape, which filters `sk`, and
 * `filter-joinagg`, which also keeps right values below 50 and then sums by the join key. The
 * generic value sort still declines; `sortByIntValueDesc` lowers.
 */
class TestCrossEngineShapes extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val partitions = 2

  // Kept rows are (2, 5), (4, 0), (5, 8), (1, 3). Keys divisible by 3 drop.
  private val groupKeys = Array(0, 1, 2, 3, 4, 5, 1, 3, 6, 9)
  private val groupValues = Array(4, -2, 5, 1, 0, 8, 3, 7, -4, 2)

  // Each source partition has four rows. The largest values are the last two of partition 1.
  private val topKeys = Array(4, 7, 8, 3, 5, 6, 1, 2)
  private val topValues = Array(10, 1, -1, 8, 9, 7, 100, 99)

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8192,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
        spillEnabled = true,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "filter-distinct" should "count the surviving keys" in {
    val data = filtered(groupKeys, groupValues)
    val distinct = CrossEngineShapes.distinct(data)
    ColumnarPlanner.lower(distinct.plan).map(_.shape) shouldBe Some(
      "Dedupe(Exchange(Key(FilterValue(FilterKey(Scan)))))",
    )
    distinct.count() shouldBe 4L
  }

  "filter-twogroup" should "return the count and the sum of each surviving key" in {
    val data = filtered(groupKeys, groupValues)
    val grouped = CrossEngineShapes.twoGroup(data)
    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(FilterValue(FilterKey(Scan))))",
    )
    val rows = grouped.collect()
    rows.map { case (key, count, sum) => key -> (count, sum) }.toMap shouldBe Map(
      1 -> (1, 3L),
      2 -> (1, 5L),
      4 -> (1, 0L),
      5 -> (1, 8L),
    )
    CrossEngineShapes.tripleChecksum(rows) shouldBe 11672L
  }

  "filter-keymod" should "count rows by key modulo 100" in {
    val data = filtered(groupKeys, groupValues)
    val counted = CrossEngineShapes.keyMod(data)
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(MapKey(FilterValue(FilterKey(Scan)))))",
    )
    counted.collect().toMap shouldBe Map(1 -> 1, 2 -> 1, 4 -> 1, 5 -> 1)
    CrossEngineShapes.pairChecksum(counted.collect()) shouldBe 376L
  }

  "filter-topn" should "keep the first rows in value order" in {
    val data = filtered(topKeys, topValues)
    val sorted = CrossEngineShapes.sorted(data)
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some(
      "SortByIntValueDesc(FilterValue(FilterKey(Scan)))",
    )
    // The winning rows sit past index 2 in their source partition. A limit applied before
    // the sort would drop them.
    sorted.collect().take(3) shouldBe List(1 -> 100, 2 -> 99, 4 -> 10)
    CrossEngineShapes.pairChecksum(sorted.collect().take(3)) shouldBe 426L
    sorted.take(3) shouldBe List(1 -> 100, 2 -> 99, 4 -> 10)
  }

  "filter-joinagg" should "sum the left value once per filtered match" in {
    // Left keeps (1, 10), (2, 20), (5, 7). Right keeps (1, 10), (1, 20), (2, 10), (4, 0).
    // Key 1 matches twice, so its left value is added twice.
    val left = DistCollection.ofIntPairs(
      Array(1, 2, 3, 4, 5),
      Array(10, 20, 5, -1, 7),
      partitions,
    )
    val right = DistCollection.ofIntPairs(
      Array(1, 1, 2, 3, 4, 5),
      Array(10, 20, 10, 5, 0, 60),
      partitions,
    )
    val aggregated = CrossEngineShapes.joinAgg(left, right, filters = 2)
    val joined = left
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .join(
        right
          .filterKeys((key: Int) => key % 3 != 0)
          .filterValues((value: Int) => value < 50),
      )

    ColumnarPlanner.lower(aggregated.plan).map(_.shape) shouldBe Some(
      "JoinProbeSumLeft(Exchange(FilterValue(FilterKey(Scan))), Exchange(FilterValue(FilterKey(Scan))))",
    )
    ColumnarPlanner.partitioning(aggregated.plan).isDefined shouldBe true
    ColumnarPlanner.tryRun(aggregated.plan).isDefined shouldBe true
    ColumnarPlanner.lower(joined.plan).map(_.shape) shouldBe Some(
      "HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(FilterValue(FilterKey(Scan))))",
    )
    val projected = joined.map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
    val projectedOp = ColumnarPlanner.lower(projected.plan).get
    projectedOp.shape shouldBe (
      "Project(" +
        "HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(FilterValue(FilterKey(Scan))))" +
        ")"
    )
    projectedOp.kind shouldBe PhysicalOp.Kind.Triples
    projectedOp.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int64)
    Task.resetSortMergeJoinRunsForTest()
    ColumnarPlanner.tryRun(projected.plan).isDefined shouldBe true
    Task.sortMergeJoinRunsForTest shouldBe 0
    ScarletRuntime.get.shuffle.clear()
    Task.resetSortMergeJoinRunsForTest()
    val projectedRows = projected.collect()
    Task.sortMergeJoinRunsForTest shouldBe 0
    projectedRows should contain theSameElementsAs Seq((1, 10L), (1, 10L), (2, 20L))
    CrossEngineShapes.sumChecksum(projectedRows) shouldBe 164L
    val keptProject = ScarletConf.get
    ScarletConf.set(keptProject.copy(columnarExecution = false))
    val rowProjected =
      try
        ScarletRuntime.get.shuffle.clear()
        projected.collect()
      finally ScarletConf.set(keptProject)
    projectedRows should contain theSameElementsAs rowProjected
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      Task.resetSortMergeJoinRunsForTest()
      projected.collect() should contain theSameElementsAs projectedRows
      Task.sortMergeJoinRunsForTest shouldBe 0
    finally ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 8192))
    val generic = projected.reduceByKey((left: Long, right: Long) => left + right)
    val genericOp = ColumnarPlanner.lower(generic.plan).get
    genericOp.shape should startWith("HashAggregate(Exchange(Project(")
    genericOp.shape should not include "SortMergeJoin"
    genericOp.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int64)
    ColumnarPlanner.tryRun(generic.plan).isDefined shouldBe true
    val summed = generic.collect().toMap
    summed shouldBe Map(1 -> 20L, 2 -> 20L)
    CrossEngineShapes.sumChecksum(summed) shouldBe 133L
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      generic.collect().toMap shouldBe summed
    finally ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 8192))
    val counted = projected.countByKey
    val countedOp = ColumnarPlanner.lower(counted.plan).get
    countedOp.shape should include("CountByKey(Exchange(Project(HashJoin")
    countedOp.shape should not include "SortMergeJoin"
    countedOp.shape should not include "JoinProbeSum"
    ColumnarPlanner.tryRun(counted.plan).isDefined shouldBe true
    val counts = counted.collect().toMap
    counts shouldBe Map(1 -> 2, 2 -> 1)
    CrossEngineShapes.pairChecksum(counts) shouldBe 96L
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      counted.collect().toMap shouldBe counts
    finally ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 8192))
    val kept = ScarletConf.get
    ScarletConf.set(kept.copy(columnarExecution = false))
    val rowCounts = counted.collect().toMap
    ScarletConf.set(kept)
    counts shouldBe rowCounts
    val countedChanged = joined
      .map { case (key, (leftValue, _)) => (leftValue, key.toLong) }
      .countByKey
    ColumnarPlanner.lower(countedChanged.plan).map(_.shape).get should include(
      "CountByKey(Exchange(Project(",
    )
    countedChanged.collect().toMap shouldBe Map(10 -> 2, 20 -> 1)
    val changed = joined
      .map { case (key, (leftValue, _)) => (leftValue, key.toLong) }
      .reduceByKey((left: Long, right: Long) => left + right)
    val changedShape = ColumnarPlanner.lower(changed.plan).map(_.shape).get
    changedShape should include("HashAggregate(Exchange(Project(")
    changed.collect().toMap shouldBe Map(10 -> 2L, 20 -> 2L)
    val nested = joined
      .map { case (key, pair) => (key, pair) }
      .reduceByKey((left: (Int, Int), right: (Int, Int)) =>
        (left._1 + right._1, left._2 + right._2),
      )
    ColumnarPlanner.lower(nested.plan).isDefined shouldBe true
    ColumnarPlanner.tryRun(nested.plan).isEmpty shouldBe true
    val declined = intercept[ColumnarFallback] {
      ColumnarInterpreter.interpret(nested.plan, ColumnarPlanner.lower(nested.plan).get)
    }
    declined.getMessage should include("(Int, Long)")
    val nestedCount = joined.map { case (key, pair) => (key, pair) }.countByKey
    ColumnarPlanner.lower(nestedCount.plan).isDefined shouldBe true
    ColumnarPlanner.tryRun(nestedCount.plan).isEmpty shouldBe true
    val countDeclined = intercept[ColumnarFallback] {
      ColumnarInterpreter.interpret(nestedCount.plan, ColumnarPlanner.lower(nestedCount.plan).get)
    }
    countDeclined.getMessage should include("(Int, Long)")
    val columnarOn = ScarletConf.get
    ScarletConf.set(columnarOn.copy(columnarExecution = false))
    val rowPairs = nested.collect().toMap
    ScarletConf.set(columnarOn)
    nested.collect().toMap shouldBe rowPairs
    rowPairs shouldBe Map(1 -> (20, 30), 2 -> (20, 10))
    aggregated.collect().toMap shouldBe Map(1 -> 20L, 2 -> 20L)
    CrossEngineShapes.sumChecksum(aggregated.collect()) shouldBe 133L
  }

  "a generic value sort" should "decline tryRun and stay off the columnar arm" in {
    val data = filtered(topKeys, topValues)
    val plan = data
      .sortBy(pair => (pair._2, pair._1))(using
        Ordering.Tuple2(Ordering.Int.reverse, Ordering.Int),
      )
      .plan
    ColumnarPlanner.lower(plan).isEmpty shouldBe true
    ColumnarPlanner.partitioning(plan).isEmpty shouldBe true
    ColumnarPlanner.tryRun(plan).isEmpty shouldBe true
  }

  private def filtered(keys: Array[Int], values: Array[Int]): DistCollection[(Int, Int)] =
    DistCollection
      .ofIntPairs(keys, values, partitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
