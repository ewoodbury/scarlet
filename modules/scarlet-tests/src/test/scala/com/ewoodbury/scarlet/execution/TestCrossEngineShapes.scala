package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
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
    ColumnarPlanner.tryCount(distinct.plan) shouldBe Some(4L)
    DistinctKernel.resetIntsForTest()
    distinct.count() shouldBe 4L
    ColumnarPlanner.tryCount(distinct.plan) shouldBe Some(4L)
    DistinctKernel.intsCallsForTest shouldBe 0
    distinct.collect().toSeq.sorted shouldBe Seq(1, 2, 4, 5)
    DistinctKernel.intsCallsForTest should be > 0
    data.countByKey.collect().toMap shouldBe Map(1 -> 1, 2 -> 1, 4 -> 1, 5 -> 1)
    val saved = ScarletConf.get
    ScarletConf.set(saved.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      DistinctKernel.resetIntsForTest()
      distinct.count() shouldBe 4L
      ColumnarPlanner.tryCount(distinct.plan) shouldBe Some(4L)
      DistinctKernel.intsCallsForTest shouldBe 0
    finally ScarletConf.set(saved)
  }

  it should "count one null key and keep zero distinct from it" in {
    // Partition split puts one null in each half. Value filter keeps both. Key filter drops them.
    val rows = Seq(
      erased(Some(0), Some(5)),
      erased(None, Some(8)),
      erased(Some(2), Some(3)),
      erased(Some(1), Some(-3)),
      erased(None, Some(2)),
      erased(Some(4), Some(9)),
      erased(Some(2), Some(7)),
    )
    val data = DistCollection(rows, partitions)
    val byValue = data.filterValues((value: Int) => value >= 0).keys.distinct()
    ColumnarPlanner.lower(byValue.plan).map(_.shape) shouldBe Some(
      "Dedupe(Exchange(Key(FilterValue(Scan))))",
    )
    byValue.count() shouldBe 4L
    ColumnarPlanner.tryCount(byValue.plan) shouldBe Some(4L)
    DistinctKernel.resetIntsForTest()
    ColumnarPlanner.tryCount(byValue.plan) shouldBe Some(4L)
    DistinctKernel.intsCallsForTest shouldBe 0
    val saved = ScarletConf.get
    ScarletConf.set(saved.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      byValue.count() shouldBe 4L
      ColumnarPlanner.tryCount(byValue.plan) shouldBe Some(4L)
    finally ScarletConf.set(saved)
    val both = data
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .keys
      .distinct()
    ColumnarPlanner.lower(both.plan).map(_.shape) shouldBe Some(
      "Dedupe(Exchange(Key(FilterValue(FilterKey(Scan)))))",
    )
    both.count() shouldBe 2L
    ColumnarPlanner.tryCount(both.plan) shouldBe Some(2L)
    ScarletConf.set(saved.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      both.count() shouldBe 2L
    finally ScarletConf.set(saved)
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
    val winners = List(1 -> 100, 2 -> 99, 4 -> 10)
    sorted.collect().take(3) shouldBe winners
    CrossEngineShapes.pairChecksum(sorted.collect().take(3)) shouldBe 426L
    val sink = new CountingSink
    val taken = ColumnMetrics.withSink(sink)(sorted.take(3))
    taken shouldBe winners
    CrossEngineShapes.pairChecksum(taken) shouldBe 426L
    sink.primitiveEncodeCount shouldBe 0L
    sink.fusedCount should be > 0L
    val saved = ScarletConf.get
    ScarletConf.set(saved.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      sorted.take(3) shouldBe winners
    finally ScarletConf.set(saved)
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
    JoinWriteRuns.resetRunsForTest()
    joined.collect().iterator.hasNext shouldBe true
    JoinWriteRuns.runsForTest should be > 0L
    val projected = joined.map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
    val projectedOp = ColumnarPlanner.lower(projected.plan).get
    projectedOp.shape shouldBe (
      "Project(" +
        "HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(FilterValue(FilterKey(Scan))))" +
        ")"
    )
    projectedOp.kind shouldBe PhysicalOp.Kind.Triples
    projectedOp.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int64)
    ColumnarPlanner.tryRun(projected.plan).isEmpty shouldBe true
    val generic = projected.reduceByKey((left: Long, right: Long) => left + right)
    val genericOp = ColumnarPlanner.lower(generic.plan).get
    genericOp.shape should startWith("HashAggregate(Exchange(Project(")
    genericOp.shape should not include "SortMergeJoin"
    genericOp.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int64)
    ColumnarPlanner.tryRun(generic.plan).isDefined shouldBe true
    JoinWriteRuns.resetRunsForTest()
    val summed = generic.collect().toMap
    JoinWriteRuns.runsForTest shouldBe 0L
    summed shouldBe Map(1 -> 20L, 2 -> 20L)
    CrossEngineShapes.sumChecksum(summed) shouldBe 133L
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      JoinWriteRuns.resetRunsForTest()
      generic.collect().toMap shouldBe summed
      JoinWriteRuns.runsForTest shouldBe 0L
    finally ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 8192))
    val counted = projected.countByKey
    val countedOp = ColumnarPlanner.lower(counted.plan).get
    countedOp.shape should include("CountByKey(Exchange(Project(HashJoin")
    countedOp.shape should not include "SortMergeJoin"
    countedOp.shape should not include "JoinProbeSum"
    ColumnarPlanner.tryRun(counted.plan).isDefined shouldBe true
    JoinWriteRuns.resetRunsForTest()
    HashCount.resetByKeyForTest()
    val counts = counted.collect().toMap
    JoinWriteRuns.runsForTest shouldBe 0L
    HashCount.byKeyCallsForTest shouldBe 0
    counts shouldBe Map(1 -> 2, 2 -> 1)
    CrossEngineShapes.pairChecksum(counts) shouldBe 96L
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      JoinWriteRuns.resetRunsForTest()
      HashCount.resetByKeyForTest()
      counted.collect().toMap shouldBe counts
      JoinWriteRuns.runsForTest shouldBe 0L
      HashCount.byKeyCallsForTest shouldBe 0
    finally ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 8192))
    val kept = ScarletConf.get
    ScarletConf.set(kept.copy(columnarExecution = false))
    val rowCounts = counted.collect().toMap
    ScarletConf.set(kept)
    counts shouldBe rowCounts
    val countedChanged = joined.map { case (key, (leftValue, _)) =>
      (leftValue, key.toLong)
    }.countByKey
    ColumnarPlanner.lower(countedChanged.plan).map(_.shape).get should include(
      "CountByKey(Exchange(Project(",
    )
    JoinWriteRuns.resetRunsForTest()
    HashCount.resetByKeyForTest()
    countedChanged.collect().toMap shouldBe Map(10 -> 2, 20 -> 1)
    JoinWriteRuns.runsForTest shouldBe 0L
    HashCount.byKeyCallsForTest shouldBe 0
    val changed = joined
      .map { case (key, (leftValue, _)) => (leftValue, key.toLong) }
      .reduceByKey((left: Long, right: Long) => left + right)
    val changedShape = ColumnarPlanner.lower(changed.plan).map(_.shape).get
    changedShape should include("HashAggregate(Exchange(Project(")
    JoinWriteRuns.resetRunsForTest()
    changed.collect().toMap shouldBe Map(10 -> 2L, 20 -> 2L)
    JoinWriteRuns.runsForTest shouldBe 0L
    val nested = joined
      .map { case (key, pair) => (key, pair) }
      .reduceByKey((left: (Int, Int), right: (Int, Int)) =>
        (left._1 + right._1, left._2 + right._2),
      )
    ColumnarPlanner.lower(nested.plan) shouldBe None
    ColumnarPlanner.tryRun(nested.plan).isEmpty shouldBe true
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

  "a hinted triple project" should "reduce by hashing both sides" in {
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
    val filteredLeft = left
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val filteredRight = right
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value < 50)
    val sortJoined = filteredLeft.sortMergeJoin(filteredRight)
    ColumnarPlanner.lower(sortJoined.plan).map(_.shape).get should include("SortMergeJoin")
    val sortSum = sortJoined
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .reduceByKey((left: Long, right: Long) => left + right)
    val sortOp = ColumnarPlanner.lower(sortSum.plan).get
    sortOp.shape should startWith("HashAggregate(Exchange(Project(")
    sortOp.shape should include("HashJoin")
    sortOp.shape should not include "SortMergeJoin"
    ColumnarPlanner.tryRun(sortSum.plan).isDefined shouldBe true
    Task.resetSortMergeJoinRunsForTest()
    JoinWriteRuns.resetRunsForTest()
    val summed = sortSum.collect().toMap
    Task.sortMergeJoinRunsForTest shouldBe 0
    JoinWriteRuns.runsForTest shouldBe 0L
    summed shouldBe Map(1 -> 20L, 2 -> 20L)
    CrossEngineShapes.sumChecksum(summed) shouldBe 133L
    val savedBatch = ScarletConf.get
    ScarletConf.set(savedBatch.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      JoinWriteRuns.resetRunsForTest()
      sortSum.collect().toMap shouldBe summed
      JoinWriteRuns.runsForTest shouldBe 0L
    finally ScarletConf.set(savedBatch)
    val savedRow = ScarletConf.get
    ScarletConf.set(savedRow.copy(columnarExecution = false))
    val rowSum = sortSum.collect().toMap
    ScarletConf.set(savedRow)
    summed shouldBe rowSum
    val sortCount = sortJoined.map { case (key, (leftValue, _)) =>
      (key, leftValue.toLong)
    }.countByKey
    val sortCountOp = ColumnarPlanner.lower(sortCount.plan).get
    sortCountOp.shape should include("CountByKey(Exchange(Project(HashJoin")
    sortCountOp.shape should not include "SortMergeJoin"
    sortCountOp.shape should not include "JoinProbeSum"
    ColumnarPlanner.tryRun(sortCount.plan).isDefined shouldBe true
    Task.resetSortMergeJoinRunsForTest()
    JoinWriteRuns.resetRunsForTest()
    HashCount.resetByKeyForTest()
    val counts = sortCount.collect().toMap
    Task.sortMergeJoinRunsForTest shouldBe 0
    JoinWriteRuns.runsForTest shouldBe 0L
    HashCount.byKeyCallsForTest shouldBe 0
    counts shouldBe Map(1 -> 2, 2 -> 1)
    CrossEngineShapes.pairChecksum(counts) shouldBe 96L
    ScarletConf.set(savedBatch.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      JoinWriteRuns.resetRunsForTest()
      HashCount.resetByKeyForTest()
      sortCount.collect().toMap shouldBe counts
      JoinWriteRuns.runsForTest shouldBe 0L
      HashCount.byKeyCallsForTest shouldBe 0
    finally ScarletConf.set(savedBatch)
    val changed = sortJoined
      .map { case (key, (leftValue, _)) => (leftValue, key.toLong) }
      .reduceByKey((left: Long, right: Long) => left + right)
    ColumnarPlanner.lower(changed.plan).map(_.shape).get should include(
      "HashAggregate(Exchange(Project(",
    )
    JoinWriteRuns.resetRunsForTest()
    changed.collect().toMap shouldBe Map(10 -> 2L, 20 -> 2L)
    JoinWriteRuns.runsForTest shouldBe 0L
    val broadcastJoined = filteredLeft.broadcastJoin(filteredRight)
    ColumnarPlanner.lower(broadcastJoined.plan).map(_.shape).get should include("BroadcastJoin")
    val broadcastSum = broadcastJoined
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .reduceByKey((left: Long, right: Long) => left + right)
    val broadcastShape = ColumnarPlanner.lower(broadcastSum.plan).map(_.shape).get
    broadcastShape should startWith("HashAggregate(Exchange(Project(")
    broadcastShape should not include "BroadcastJoin"
    JoinWriteRuns.resetRunsForTest()
    broadcastSum.collect().toMap shouldBe summed
    JoinWriteRuns.runsForTest shouldBe 0L
    val broadcastCount = broadcastJoined.map { case (key, (leftValue, _)) =>
      (key, leftValue.toLong)
    }.countByKey
    val broadcastCountShape = ColumnarPlanner.lower(broadcastCount.plan).map(_.shape).get
    broadcastCountShape should include("CountByKey(Exchange(Project(HashJoin")
    broadcastCountShape should not include "BroadcastJoin"
    broadcastCountShape should not include "SortMergeJoin"
    ColumnarPlanner.tryRun(broadcastCount.plan).isDefined shouldBe true
    JoinWriteRuns.resetRunsForTest()
    HashCount.resetByKeyForTest()
    val broadcastCounts = broadcastCount.collect().toMap
    JoinWriteRuns.runsForTest shouldBe 0L
    HashCount.byKeyCallsForTest shouldBe 0
    broadcastCounts shouldBe Map(1 -> 2, 2 -> 1)
    CrossEngineShapes.pairChecksum(broadcastCounts) shouldBe 96L
    ScarletConf.set(savedBatch.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      JoinWriteRuns.resetRunsForTest()
      HashCount.resetByKeyForTest()
      broadcastCount.collect().toMap shouldBe broadcastCounts
      JoinWriteRuns.runsForTest shouldBe 0L
      HashCount.byKeyCallsForTest shouldBe 0
    finally ScarletConf.set(savedBatch)
    val maximized = sortJoined
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .reduceByKey((left: Long, right: Long) => math.max(left, right))
    ColumnarPlanner.tryRun(maximized.plan).isEmpty shouldBe true
    val nested = sortJoined
      .map { case (key, pair) => (key, pair) }
      .reduceByKey((left: (Int, Int), right: (Int, Int)) =>
        (left._1 + right._1, left._2 + right._2),
      )
    ColumnarPlanner.lower(nested.plan) shouldBe None
    ColumnarPlanner.tryRun(nested.plan).isEmpty shouldBe true
    val savedNested = ScarletConf.get
    ScarletConf.set(savedNested.copy(columnarExecution = false))
    val rowPairs = nested.collect().toMap
    ScarletConf.set(savedNested)
    nested.collect().toMap shouldBe rowPairs
    rowPairs shouldBe Map(1 -> (20, 30), 2 -> (20, 10))
    val scan = PhysicalOp.Scan(PhysicalOp.Kind.Pairs, Seq.empty)
    val bare = PhysicalOp.Project(PhysicalOp.SortMergeJoin(scan, scan))
    ReduceByKeyLowering.apply(bare).isEmpty shouldBe true
    val oneSide = PhysicalOp.Project(
      PhysicalOp.SortMergeJoin(PhysicalOp.Exchange(scan), scan),
    )
    ReduceByKeyLowering.apply(oneSide).isEmpty shouldBe true
    val joined = PhysicalOp.SortMergeJoin(
      PhysicalOp.Exchange(scan),
      PhysicalOp.Exchange(scan),
    )
    val slots = Seq(
      PhysicalOp.Slot.TripleRow,
      PhysicalOp.Slot.TripleKey,
      PhysicalOp.Slot.TripleLeftValue,
      PhysicalOp.Slot.TripleRightValue,
    )
    slots.foreach { slot =>
      val between = PhysicalOp.Project(PhysicalOp.Filter(joined, slot))
      val counted = CountByKeyLowering.apply(between).get.shape
      counted should include(s"CountByKey(Exchange(Project(${slot.label}(HashJoin")
      counted should not include "SortMergeJoin"
      val reduced = ReduceByKeyLowering.apply(between).get.shape
      reduced should include(s"HashAggregate(Exchange(Project(${slot.label}(HashJoin")
      reduced should not include "SortMergeJoin"
    }
    val broadcast = PhysicalOp.Project(
      PhysicalOp.Filter(
        PhysicalOp.BroadcastJoin(scan, scan, PhysicalOp.BuildSide.Right),
        PhysicalOp.Slot.TripleRow,
      ),
    )
    val broadcastCounted = CountByKeyLowering.apply(broadcast).get.shape
    broadcastCounted should include("FilterTripleRow(HashJoin")
    broadcastCounted should not include "BroadcastJoin"
    broadcastCounted should not include "SortMergeJoin"
    val stacked = PhysicalOp.Project(
      PhysicalOp.Filter(
        PhysicalOp.Filter(joined, PhysicalOp.Slot.TripleLeftValue),
        PhysicalOp.Slot.TripleRow,
      ),
    )
    val element = PhysicalOp.Project(PhysicalOp.Filter(joined, PhysicalOp.Slot.Row))
    ReduceByKeyLowering.apply(stacked).isEmpty shouldBe true
    ReduceByKeyLowering.apply(element).isEmpty shouldBe true
    CountByKeyLowering.apply(bare).isEmpty shouldBe true
    CountByKeyLowering.apply(oneSide).isEmpty shouldBe true
    CountByKeyLowering.apply(stacked).isEmpty shouldBe true
    CountByKeyLowering.apply(element).isEmpty shouldBe true
  }

  "a filtered hinted count" should "hash the survivors from the probe" in {
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
    val filteredLeft = left
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val filteredRight = right
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value < 50)
    val sortCounted = filteredLeft
      .sortMergeJoin(filteredRight)
      .filter { case (_, (leftValue, _)) => leftValue > 10 }
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .countByKey
    // Key 1's left value is 10, so both of its matches drop. Key 2's left value is 20.
    assertProbedCount(sortCounted, Map(2 -> 1))
    CrossEngineShapes.pairChecksum(sortCounted.collect()) shouldBe 63L
    val broadcastCounted = filteredLeft
      .broadcastJoin(filteredRight)
      .filter { case (_, (leftValue, _)) => leftValue > 10 }
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .countByKey
    assertProbedCount(broadcastCounted, Map(2 -> 1))
    val mixedLeft = DistCollection.ofIntPairs(Array(1, 1, 2), Array(10, 30, 20), partitions)
    val mixedRight = DistCollection.ofIntPairs(Array(1, 1, 2), Array(7, 8, 9), partitions)
    val mixed = mixedLeft
      .sortMergeJoin(mixedRight)
      .filter { case (_, (leftValue, _)) => leftValue > 10 }
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .countByKey
    // Key 1 keeps the two matches whose left value is 30 and drops the two whose left value is 10.
    assertProbedCount(mixed, Map(1 -> 2, 2 -> 1))
    val byLeft = mixedLeft
      .sortMergeJoin(mixedRight)
      .filterJoinLeft(_ > 10)
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .countByKey
    assertProbedCount(byLeft, Map(1 -> 2, 2 -> 1))
    val byKey = mixedLeft
      .sortMergeJoin(mixedRight)
      .filterJoinKey(_ == 1)
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .countByKey
    assertProbedCount(byKey, Map(1 -> 4))
    val byRight = mixedLeft
      .sortMergeJoin(mixedRight)
      .filterJoinRight(_ == 7)
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .countByKey
    assertProbedCount(byRight, Map(1 -> 2))
    val twice = mixedLeft
      .sortMergeJoin(mixedRight)
      .filter { case (_, (leftValue, _)) => leftValue > 10 }
      .filter { case (key, _) => key > 0 }
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .countByKey
    ColumnarPlanner.lower(twice.plan).isEmpty shouldBe true
    val saved = ScarletConf.get
    ScarletConf.set(saved.copy(columnarExecution = false))
    val rowTwice = twice.collect().toMap
    ScarletConf.set(saved)
    twice.collect().toMap shouldBe rowTwice
    val reduced = mixedLeft
      .sortMergeJoin(mixedRight)
      .filter { case (_, (leftValue, _)) => leftValue > 10 }
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .reduceByKey((left: Long, right: Long) => left + right)
    val reduceShape = ColumnarPlanner.lower(reduced.plan).get.shape
    reduceShape should include("HashAggregate(Exchange(Project(Filter")
    reduceShape should include("HashJoin")
    reduceShape should not include "SortMergeJoin"
    ColumnarPlanner.tryRun(reduced.plan).isDefined shouldBe true
    Task.resetSortMergeJoinRunsForTest()
    val sums = reduced.collect().toMap
    Task.sortMergeJoinRunsForTest shouldBe 0
    sums shouldBe Map(1 -> 60L, 2 -> 20L)
    ScarletConf.set(saved.copy(columnarExecution = false))
    val rowSums = reduced.collect().toMap
    ScarletConf.set(saved)
    sums shouldBe rowSums
  }

  "an unmapped filtered join" should "count keys from the probe tables" in {
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
    val filteredLeft = left
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val filteredRight = right
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value < 50)
    // Key 3 drops on the key filter. Key 4 stays on the right only. Key 5 stays on the left only.
    assertUnmappedCount(filteredLeft.join(filteredRight).countByKey)
    assertUnmappedCount(filteredLeft.sortMergeJoin(filteredRight).countByKey)
    assertUnmappedCount(filteredLeft.broadcastJoin(filteredRight).countByKey)
    val above = filteredLeft
      .join(filteredRight)
      .filter { case (key, _) => key > 0 }
      .countByKey
    ColumnarPlanner.lower(above.plan).isEmpty shouldBe true
    val longLeft = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), partitions)
    val longRight = DistCollection.ofLongPairs(Array(1L), Array(3L), partitions)
    ColumnarPlanner.lower(longLeft.join(longRight).countByKey.plan).isEmpty shouldBe true
    ColumnarPlanner
      .lower(filteredLeft.leftOuterJoin(filteredRight).countByKey.plan)
      .isEmpty shouldBe true
    val scan = PhysicalOp.Scan(PhysicalOp.Kind.Pairs, Seq.empty)
    val hashed = PhysicalOp.HashJoin(PhysicalOp.Exchange(scan), PhysicalOp.Exchange(scan))
    CountByKeyLowering.apply(hashed).get.shape shouldBe
      "CountByKey(HashJoin(Exchange(Scan), Exchange(Scan)))"
    CountByKeyLowering
      .apply(PhysicalOp.Filter(hashed, PhysicalOp.Slot.TripleRow))
      .isEmpty shouldBe true
    val oneSide = PhysicalOp.SortMergeJoin(PhysicalOp.Exchange(scan), scan)
    CountByKeyLowering.apply(oneSide).isEmpty shouldBe true
    val longScan = PhysicalOp.Scan(PhysicalOp.Kind.LongPairs, Seq.empty)
    val longJoin = PhysicalOp.LongHashJoin(
      PhysicalOp.Exchange(longScan),
      PhysicalOp.Exchange(longScan),
    )
    CountByKeyLowering.apply(longJoin).isEmpty shouldBe true
    val boxed = DistCollection(Seq(1 -> 10, 2 -> 20, 4 -> 1), partitions)
      .join(DistCollection(Seq(1 -> 1, 1 -> 2, 3 -> 9), partitions))
      .countByKey
    val boxedShape = ColumnarPlanner.lower(boxed.plan).get.shape
    boxedShape should include("CountByKey(HashJoin(")
    boxedShape should not include "Project"
    JoinWriteRuns.resetRunsForTest()
    HashCount.resetByKeyForTest()
    val boxedCounts = boxed.collect().toMap
    JoinWriteRuns.runsForTest shouldBe 0L
    HashCount.byKeyCallsForTest shouldBe 0
    boxedCounts shouldBe Map(1 -> 2)
    val boxedSaved = ScarletConf.get
    ScarletConf.set(boxedSaved.copy(columnarExecution = false))
    val boxedRow = boxed.collect().toMap
    ScarletConf.set(boxedSaved)
    boxedCounts shouldBe boxedRow
  }

  "a slot above a hinted triple project" should "count from the probe" in {
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
    val filteredLeft = left
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val filteredRight = right
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value < 50)
    val sortJoined = filteredLeft.sortMergeJoin(filteredRight)
    val projected = sortJoined.map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
    val keyed = projected.filterKeys((key: Int) => key == 1).countByKey
    val keyedOp = ColumnarPlanner.lower(keyed.plan).get
    keyedOp.shape should include("CountByKey(Exchange(FilterKey(Project(HashJoin")
    keyedOp.shape should not include "SortMergeJoin"
    keyedOp match
      case PhysicalOp.CountByKey(PhysicalOp.Exchange(child)) =>
        child.schema shouldBe PhysicalSchema.IntLong
      case _ => fail(keyedOp.shape)
    ColumnarPlanner.tryRun(keyed.plan).isDefined shouldBe true
    Task.resetSortMergeJoinRunsForTest()
    JoinWriteRuns.resetRunsForTest()
    HashCount.resetByKeyForTest()
    val counts = keyed.collect().toMap
    Task.sortMergeJoinRunsForTest shouldBe 0
    JoinWriteRuns.runsForTest shouldBe 0L
    HashCount.byKeyCallsForTest shouldBe 0
    counts shouldBe Map(1 -> 2)
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      JoinWriteRuns.resetRunsForTest()
      HashCount.resetByKeyForTest()
      keyed.collect().toMap shouldBe counts
      JoinWriteRuns.runsForTest shouldBe 0L
      HashCount.byKeyCallsForTest shouldBe 0
    finally ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 8192))
    val savedRow = ScarletConf.get
    ScarletConf.set(savedRow.copy(columnarExecution = false))
    val rowCounts = keyed.collect().toMap
    ScarletConf.set(savedRow)
    counts shouldBe rowCounts
    val valued = projected.filterValues((value: Long) => value > 10L).countByKey
    val valuedOp = ColumnarPlanner.lower(valued.plan).get
    valuedOp.shape should include("CountByKey(Exchange(FilterValue(Project(HashJoin")
    valuedOp.shape should not include "SortMergeJoin"
    JoinWriteRuns.resetRunsForTest()
    HashCount.resetByKeyForTest()
    val valueCounts = valued.collect().toMap
    JoinWriteRuns.runsForTest shouldBe 0L
    HashCount.byKeyCallsForTest shouldBe 0
    valueCounts shouldBe Map(2 -> 1)
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      valued.collect().toMap shouldBe valueCounts
    finally ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 8192))
    ScarletConf.set(savedRow.copy(columnarExecution = false))
    val rowValues = valued.collect().toMap
    ScarletConf.set(savedRow)
    valueCounts shouldBe rowValues
    val broadcast = filteredLeft.broadcastJoin(filteredRight)
    val broadcastKeyed = broadcast
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .filterKeys((key: Int) => key == 1)
      .countByKey
    val broadcastShape = ColumnarPlanner.lower(broadcastKeyed.plan).map(_.shape).get
    broadcastShape should include("CountByKey(Exchange(FilterKey(Project(HashJoin")
    broadcastShape should not include "BroadcastJoin"
    broadcastShape should not include "SortMergeJoin"
    JoinWriteRuns.resetRunsForTest()
    broadcastKeyed.collect().toMap shouldBe Map(1 -> 2)
    JoinWriteRuns.runsForTest shouldBe 0L
    val reduced = projected
      .filterKeys((key: Int) => key == 1)
      .reduceByKey((left: Long, right: Long) => left + right)
    ColumnarPlanner.lower(reduced.plan).isEmpty shouldBe true
    reduced.collect().toMap shouldBe Map(1 -> 20L)
    val twice = projected
      .filterKeys((key: Int) => key == 1)
      .filterValues((value: Long) => value > 0L)
      .countByKey
    ColumnarPlanner.lower(twice.plan).isEmpty shouldBe true
    val bare = sortJoined.filterKeys((key: Int) => key == 1).countByKey
    ColumnarPlanner.lower(bare.plan).isEmpty shouldBe true
    val scan = PhysicalOp.Scan(PhysicalOp.Kind.Pairs, Seq.empty)
    val hashed = PhysicalOp.HashJoin(PhysicalOp.Exchange(scan), PhysicalOp.Exchange(scan))
    val slotted = PhysicalOp.Filter(PhysicalOp.Project(hashed), PhysicalOp.Slot.Key)
    CountByKeyLowering.apply(slotted).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(FilterKey(Project(HashJoin(Exchange(Scan), Exchange(Scan))))))",
    )
    ReduceByKeyLowering.apply(slotted).isEmpty shouldBe true
    val second = PhysicalOp.Filter(slotted, PhysicalOp.Slot.Value)
    CountByKeyLowering.apply(second).isEmpty shouldBe true
    val oneSide = PhysicalOp.Filter(
      PhysicalOp.Project(PhysicalOp.SortMergeJoin(PhysicalOp.Exchange(scan), scan)),
      PhysicalOp.Slot.Key,
    )
    CountByKeyLowering.apply(oneSide).isEmpty shouldBe true
    val calls = new AtomicInteger
    val preds = new AtomicInteger
    val dropped = sortJoined
      .map(dropProjectedKey(calls))
      .filterKeys { (key: Int) =>
        val _ = preds.incrementAndGet()
        key == 2
      }
      .countByKey
    calls.set(0)
    preds.set(0)
    JoinWriteRuns.resetRunsForTest()
    dropped.collect().toMap shouldBe Map(2 -> 1)
    calls.get shouldBe 3
    preds.get shouldBe 1
    JoinWriteRuns.runsForTest shouldBe 0L
    calls.set(0)
    preds.set(0)
    ScarletConf.set(savedRow.copy(columnarExecution = false))
    val rowDropped = dropped.collect().toMap
    ScarletConf.set(savedRow)
    rowDropped shouldBe Map(2 -> 1)
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

  private def assertUnmappedCount(counted: DistCollection[(Int, Int)]): Unit =
    val shape = ColumnarPlanner.lower(counted.plan).get.shape
    shape should include("CountByKey(HashJoin(")
    shape should not include "Project"
    shape should not include "SortMergeJoin"
    shape should not include "BroadcastJoin"
    shape should not include "Exchange(HashJoin"
    ColumnarPlanner.tryRun(counted.plan).isDefined shouldBe true
    Task.resetSortMergeJoinRunsForTest()
    JoinWriteRuns.resetRunsForTest()
    HashCount.resetByKeyForTest()
    val counts = counted.collect().toMap
    Task.sortMergeJoinRunsForTest shouldBe 0
    JoinWriteRuns.runsForTest shouldBe 0L
    HashCount.byKeyCallsForTest shouldBe 0
    counts shouldBe Map(1 -> 2, 2 -> 1)
    CrossEngineShapes.pairChecksum(counts) shouldBe 96L
    val saved = ScarletConf.get
    ScarletConf.set(saved.copy(columnarExecution = false))
    val rowCounts = counted.collect().toMap
    ScarletConf.set(saved)
    counts shouldBe rowCounts
    ScarletConf.set(saved.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      JoinWriteRuns.resetRunsForTest()
      HashCount.resetByKeyForTest()
      Task.resetSortMergeJoinRunsForTest()
      counted.collect().toMap shouldBe counts
      JoinWriteRuns.runsForTest shouldBe 0L
      HashCount.byKeyCallsForTest shouldBe 0
      Task.sortMergeJoinRunsForTest shouldBe 0
    finally ScarletConf.set(saved)

  private def assertProbedCount(
      counted: DistCollection[(Int, Int)],
      expected: Map[Int, Int],
  ): Unit =
    val shape = ColumnarPlanner.lower(counted.plan).get.shape
    shape should include("CountByKey(Exchange(Project(Filter")
    shape should include("HashJoin")
    shape should not include "SortMergeJoin"
    shape should not include "BroadcastJoin"
    shape should not include "JoinProbeSum"
    ColumnarPlanner.tryRun(counted.plan).isDefined shouldBe true
    Task.resetSortMergeJoinRunsForTest()
    JoinWriteRuns.resetRunsForTest()
    HashCount.resetByKeyForTest()
    val counts = counted.collect().toMap
    Task.sortMergeJoinRunsForTest shouldBe 0
    JoinWriteRuns.runsForTest shouldBe 0L
    HashCount.byKeyCallsForTest shouldBe 0
    counts shouldBe expected
    val saved = ScarletConf.get
    ScarletConf.set(saved.copy(columnarExecution = false))
    val rowCounts = counted.collect().toMap
    ScarletConf.set(saved)
    counts shouldBe rowCounts
    ScarletConf.set(saved.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      JoinWriteRuns.resetRunsForTest()
      HashCount.resetByKeyForTest()
      Task.resetSortMergeJoinRunsForTest()
      counted.collect().toMap shouldBe counts
      JoinWriteRuns.runsForTest shouldBe 0L
      HashCount.byKeyCallsForTest shouldBe 0
      Task.sortMergeJoinRunsForTest shouldBe 0
    finally ScarletConf.set(saved)

  private def filtered(keys: Array[Int], values: Array[Int]): DistCollection[(Int, Int)] =
    DistCollection
      .ofIntPairs(keys, values, partitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)

  /** Key `1` becomes null. The value stays the left int as a long. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def dropProjectedKey(
      calls: AtomicInteger,
  ): ((Int, (Int, Int))) => (Int, Long) = { case (key, (leftValue, _)) =>
    val _ = calls.incrementAndGet()
    val projected: Any = if (key == 1) null else Int.box(key)
    Tuple2(projected, Long.box(leftValue.toLong)).asInstanceOf[(Int, Long)]
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erased(key: Option[Int], value: Option[Int]): (Int, Int) =
    val left: AnyRef = key match
      case Some(presentKey) => Int.box(presentKey)
      case None => null
    val right: AnyRef = value match
      case Some(presentValue) => Int.box(presentValue)
      case None => null
    Tuple2(left, right).asInstanceOf[(Int, Int)]
