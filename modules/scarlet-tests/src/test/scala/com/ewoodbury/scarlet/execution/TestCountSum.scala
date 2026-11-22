package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{
  ColumnMetrics,
  CountSum,
  CountingSink,
  GroupByKey,
  IntPairProject,
}
import com.ewoodbury.scarlet.core.{IntPairArrays, Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `countSumByKey` against the row path.
 *
 * Non-null rows match `groupByKey.map` with the count-and-sum fold. A null key is dropped. A null
 * value is counted and not added. Generic `groupByKey.map` stays `GroupedDecode` and still builds
 * value lists. This operator does not.
 */
class TestCountSum extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val partitions = 2

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
    GroupByKey.resetValueListsBuilt()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "countSumByKey" should "match the row fold on the filter-twogroup fixture" in {
    val data = twogroup()
    GroupByKey.resetValueListsBuilt()
    val rows = ColumnMetrics.withSink(new CountingSink) {
      val found = CrossEngineShapes.twoGroup(data).collect()
      ColumnMetrics.primitiveEncodeCount shouldBe 0L
      found
    }
    val columnar = pairs(rows)
    GroupByKey.valueListsBuilt shouldBe 0L
    val folded = row {
      pairs {
        data.groupByKey
          .map { case (key, grouped) =>
            val (count, sum) = grouped.iterator.foldLeft((0, 0L)) { case ((count, sum), value) =>
              (count + 1, sum + value.toLong)
            }
            (key, count, sum)
          }
          .collect()
      }
    }
    columnar shouldBe folded
    columnar shouldBe expectedGroups
    CrossEngineShapes.tripleChecksum(rows) shouldBe 11672L
  }

  it should "match the same groups at batch size 1" in {
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarBatchSize = 1, threadPoolSize = 4))
      ScarletRuntime.get.shuffle.clear()
      GroupByKey.resetValueListsBuilt()
      val rows = ColumnMetrics.withSink(new CountingSink) {
        val found = CrossEngineShapes.twoGroup(twogroup()).collect()
        ColumnMetrics.primitiveEncodeCount shouldBe 0L
        found
      }
      pairs(rows) shouldBe expectedGroups
      GroupByKey.valueListsBuilt shouldBe 0L
      CrossEngineShapes.tripleChecksum(rows) shouldBe 11672L
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
  }

  it should "fold a dense owned scan and a single slot without encoding" in {
    val dense = DistCollection.ofIntPairs(Array(1, 2, 1), Array(4, -3, 5), partitions)
    ColumnarPlanner.lower(dense.countSumByKey.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(Scan))",
    )
    owned(dense.countSumByKey) shouldBe Map(1 -> (2, 9L), 2 -> (1, -3L))
    val keyed = dense.filterKeys((key: Int) => key % 2 != 0)
    ColumnarPlanner.lower(keyed.countSumByKey.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(FilterKey(Scan)))",
    )
    owned(keyed.countSumByKey) shouldBe Map(1 -> (2, 9L))
    val valued = dense.filterValues((value: Int) => value >= 0)
    ColumnarPlanner.lower(valued.countSumByKey.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(FilterValue(Scan)))",
    )
    owned(valued.countSumByKey) shouldBe Map(1 -> (2, 9L))
  }

  it should "cross an empty owned partition" in {
    val parts = Seq(
      Partition(IntPairArrays.fromOwned(Array(1, 1), Array(4, 5))),
      Partition(IntPairArrays.fromOwned(Array.empty[Int], Array.empty[Int])),
      Partition(IntPairArrays.fromOwned(Array(2), Array(7))),
    )
    val data = DistCollection(Plan.Source(parts, Plan.SourceDescriptor.Pairs))
    ColumnarPlanner.lower(data.countSumByKey.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(Scan))",
    )
    owned(data.countSumByKey) shouldBe Map(1 -> (2, 9L), 2 -> (1, 7L))
  }

  it should "keep a whole-row filter on the interpreted path" in {
    val data = DistCollection
      .ofIntPairs(fixtureKeys, fixtureValues, partitions)
      .filter((pair: (Int, Int)) => pair._1 % 3 != 0 && pair._2 >= 0)
    ColumnarPlanner.lower(data.countSumByKey.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(FilterRow(Scan)))",
    )
    val rows = ColumnMetrics.withSink(new CountingSink) {
      val found = data.countSumByKey.collect()
      ColumnMetrics.primitiveEncodeCount should be > 0L
      found
    }
    pairs(rows) shouldBe expectedGroups
    GroupByKey.valueListsBuilt shouldBe 0L
  }

  it should "leave a generic groupByKey map on GroupedDecode" in {
    val data = DistCollection
      .ofIntPairs(Array(1, 2, 1), Array(4, 5, 6), partitions)
      .filterKeys((key: Int) => key % 3 != 0)
    val mapped = data.groupByKey.map { case (key, grouped) => (key, grouped.size) }
    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some(
      "GroupedDecode(GroupByKey(Exchange(FilterKey(Scan))))",
    )
  }

  it should "fold mapIntKey without publishing the pair batch" in {
    val data = DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), partitions)
    val counted = data.mapIntKey((key: Int) => key + 1).countSumByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(MapKey(Scan)))",
    )
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      MapIntKey.resetPublishedForTest()
      CountSum.resetByKeyForTest()
      val rows = ColumnMetrics.withSink(new CountingSink) {
        val found = counted.collect()
        ColumnMetrics.primitiveEncodeCount shouldBe 0L
        found
      }
      MapIntKey.publishedForTest shouldBe 0
      CountSum.byKeyCallsForTest shouldBe 0
      pairs(rows)
    }
    val summed = atBatch(8192)
    summed shouldBe Map(2 -> (2, 40L), 3 -> (1, 20L), 4 -> (1, 40L))
    atBatch(1) shouldBe summed
    summed shouldBe row(pairs(counted.collect()))

    MapIntKey.resetPublishedForTest()
    val projected = data.mapIntKey((key: Int) => key + 1).collect().toList.sorted
    MapIntKey.publishedForTest should be > 0
    projected shouldBe List((2, 10), (2, 30), (3, 20), (4, 40))
  }

  it should "fold a pair project without publishing the pair batch" in {
    val data = DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), partitions)
    val counted = data.map { case (key, value) => (key + 1, value) }.countSumByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(Project(Scan)))",
    )
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      IntPairProject.resetPublishedForTest()
      CountSum.resetByKeyForTest()
      val found = ColumnMetrics.withSink(new CountingSink) {
        val rows = pairs(counted.collect())
        ColumnMetrics.primitiveEncodeCount shouldBe 0L
        rows
      }
      IntPairProject.publishedForTest shouldBe 0
      CountSum.byKeyCallsForTest shouldBe 0
      found
    }
    val summed = atBatch(8192)
    summed shouldBe Map(2 -> (2, 40L), 3 -> (1, 20L), 4 -> (1, 40L))
    atBatch(1) shouldBe summed
    summed shouldBe row(pairs(counted.collect()))

    IntPairProject.resetPublishedForTest()
    val projected = data.map { case (key, value) => (key + 1, value) }.collect().toList.sorted
    IntPairProject.publishedForTest should be > 0
    projected shouldBe List((2, 10), (2, 30), (3, 20), (4, 40))
  }

  it should "fold a filtered pair project from the source morsels" in {
    val data = DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), partitions)
    val counted = data
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .map { case (key, value) => (key % 100, value) }
      .countSumByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(Project(FilterValue(FilterKey(Scan)))))",
    )
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      IntPairProject.resetPublishedForTest()
      CountSum.resetByKeyForTest()
      val found = ColumnMetrics.withSink(new CountingSink) {
        val rows = pairs(counted.collect())
        ColumnMetrics.primitiveEncodeCount shouldBe 0L
        rows
      }
      IntPairProject.publishedForTest shouldBe 0
      CountSum.byKeyCallsForTest shouldBe 0
      found
    }
    val summed = atBatch(8192)
    summed shouldBe Map(1 -> (2, 40L), 2 -> (1, 20L))
    atBatch(1) shouldBe summed
    summed shouldBe row(pairs(counted.collect()))
  }

  it should "keep a second pair map and a whole-row filter on the interpreted path" in {
    val data = DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), partitions)
    val twice = data
      .map { case (key, value) => (key + 1, value) }
      .map { case (key, value) => (key * 2, value) }
      .countSumByKey
    ColumnarPlanner.lower(twice.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(Project(Project(Scan))))",
    )
    IntPairProject.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val doubled = pairs(twice.collect())
    IntPairProject.publishedForTest should be > 0
    CountSum.byKeyCallsForTest should be > 0
    doubled shouldBe Map(4 -> (2, 40L), 6 -> (1, 20L), 8 -> (1, 40L))
    doubled shouldBe row(pairs(twice.collect()))

    val whole = data
      .filter((pair: (Int, Int)) => pair._1 % 2 != 0 && pair._2 > 0)
      .map { case (key, value) => (key + 1, value) }
      .countSumByKey
    ColumnarPlanner.lower(whole.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(Project(FilterRow(Scan))))",
    )
    IntPairProject.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val filtered = pairs(whole.collect())
    IntPairProject.publishedForTest should be > 0
    CountSum.byKeyCallsForTest should be > 0
    filtered shouldBe Map(2 -> (2, 40L), 4 -> (1, 40L))
    filtered shouldBe row(pairs(whole.collect()))
  }

  it should "drop a null projected key and count a null projected value" in {
    val calls = new AtomicInteger
    val counted = nullPairs().map(shiftNullPair(calls)).countSumByKey
    val expected = Map(
      2 -> (2, 9L),
      6 -> (2, 4L),
      3 -> (1, 0L),
      8 -> (2, 4L),
    )
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      IntPairProject.resetPublishedForTest()
      CountSum.resetByKeyForTest()
      val found = pairs(counted.collect())
      IntPairProject.publishedForTest shouldBe 0
      CountSum.byKeyCallsForTest shouldBe 0
      (found, calls.get())
    }
    val (summed, invoked) = atBatch(8192)
    summed shouldBe expected
    invoked shouldBe 9
    atBatch(1) shouldBe (summed, invoked)
    calls.set(0)
    row(pairs(counted.collect())) shouldBe expected
    calls.get() shouldBe invoked

    val valueCalls = new AtomicInteger
    val values = nullValue().map(shiftNullPair(valueCalls, _ * 2)).countSumByKey
    valueCalls.set(0)
    IntPairProject.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val columnarValues = pairs(values.collect())
    valueCalls.get() shouldBe 2
    IntPairProject.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    columnarValues shouldBe Map(2 -> (1, 0L), 8 -> (1, 4L))
    valueCalls.set(0)
    row(pairs(values.collect())) shouldBe columnarValues
    valueCalls.get() shouldBe 2
  }

  it should "fold a filtered mapIntKey from the source columns" in {
    val calls = new AtomicInteger
    val data = DistCollection.ofIntPairs(Array(1, 2, 3, 4, 6), Array(10, -1, 20, 0, 5), partitions)
    val counted = data
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .mapIntKey { key =>
        calls.incrementAndGet()
        key % 100
      }
      .countSumByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(MapKey(FilterValue(FilterKey(Scan)))))",
    )
    calls.set(0)
    MapIntKey.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val summed = pairs(counted.collect())
    calls.get() shouldBe 2
    MapIntKey.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    summed shouldBe Map(1 -> (1, 10L), 4 -> (1, 0L))
    calls.set(0)
    summed shouldBe row(pairs(counted.collect()))
    calls.get() shouldBe 2

    val valueFirst = data
      .filterValues((value: Int) => value >= 0)
      .filterKeys((key: Int) => key % 3 != 0)
      .mapIntKey((key: Int) => key + 1)
      .countSumByKey
    ColumnarPlanner.lower(valueFirst.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(MapKey(FilterKey(FilterValue(Scan)))))",
    )
    pairs(valueFirst.collect()) shouldBe Map(2 -> (1, 10L), 5 -> (1, 0L))
    pairs(valueFirst.collect()) shouldBe row(pairs(valueFirst.collect()))
  }

  it should "skip a null key and count a null value without publishing the map" in {
    val calls = new AtomicInteger
    val counted = nullPairs().mapIntKey { key =>
      calls.incrementAndGet()
      key + 1
    }.countSumByKey
    val expected = Map(
      2 -> (2, 9L),
      6 -> (2, 4L),
      3 -> (1, 0L),
      8 -> (2, 4L),
    )
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      MapIntKey.resetPublishedForTest()
      CountSum.resetByKeyForTest()
      val found = pairs(counted.collect())
      MapIntKey.publishedForTest shouldBe 0
      CountSum.byKeyCallsForTest shouldBe 0
      (found, calls.get())
    }
    val (summed, invoked) = atBatch(8192)
    summed shouldBe expected
    invoked shouldBe 7
    atBatch(1) shouldBe (summed, invoked)
    calls.set(0)
    row(pairs(counted.collect())) shouldBe expected
    calls.get() shouldBe invoked

    val valueCalls = new AtomicInteger
    val values = nullValue().mapIntKey { key =>
      valueCalls.incrementAndGet()
      key * 2
    }.countSumByKey
    valueCalls.set(0)
    MapIntKey.resetPublishedForTest()
    val columnarValues = pairs(values.collect())
    valueCalls.get() shouldBe 2
    MapIntKey.publishedForTest shouldBe 0
    columnarValues shouldBe Map(2 -> (1, 0L), 8 -> (1, 4L))
    valueCalls.set(0)
    row(pairs(values.collect())) shouldBe columnarValues
    valueCalls.get() shouldBe 2
  }

  it should "fold one slot above mapIntKey without publishing the pair batch" in {
    val data = DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), partitions)
    val counted =
      data.mapIntKey((key: Int) => key + 1).filterKeys((key: Int) => key > 2).countSumByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(FilterKey(MapKey(Scan))))",
    )
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      MapIntKey.resetPublishedForTest()
      CountSum.resetByKeyForTest()
      val rows = ColumnMetrics.withSink(new CountingSink) {
        val found = counted.collect()
        ColumnMetrics.primitiveEncodeCount shouldBe 0L
        found
      }
      MapIntKey.publishedForTest shouldBe 0
      CountSum.byKeyCallsForTest shouldBe 0
      pairs(rows)
    }
    val summed = atBatch(8192)
    summed shouldBe Map(3 -> (1, 20L), 4 -> (1, 40L))
    atBatch(1) shouldBe summed
    summed shouldBe row(pairs(counted.collect()))

    MapIntKey.resetPublishedForTest()
    val projected = data
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 2)
      .collect()
      .toList
      .sorted
    MapIntKey.publishedForTest should be > 0
    projected shouldBe List((3, 20), (4, 40))

    val byValue =
      data.mapIntKey((key: Int) => key + 1).filterValues((value: Int) => value >= 20).countSumByKey
    ColumnarPlanner.lower(byValue.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(FilterValue(MapKey(Scan))))",
    )
    MapIntKey.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val values = ColumnMetrics.withSink(new CountingSink) {
      val found = pairs(byValue.collect())
      ColumnMetrics.primitiveEncodeCount shouldBe 0L
      found
    }
    MapIntKey.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    // 10 fails `>= 20`, so the mapped key 2 keeps the value 30.
    values shouldBe Map(2 -> (1, 30L), 3 -> (1, 20L), 4 -> (1, 40L))
    values shouldBe row(pairs(byValue.collect()))

    val calls = new AtomicInteger
    val stacked = DistCollection
      .ofIntPairs(Array(1, 2, 3, 4, 6), Array(10, -1, 20, 0, 5), partitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .mapIntKey { key =>
        calls.incrementAndGet()
        key % 100
      }
      .filterKeys((key: Int) => key < 50)
      .countSumByKey
    ColumnarPlanner.lower(stacked.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(FilterKey(MapKey(FilterValue(FilterKey(Scan))))))",
    )
    calls.set(0)
    MapIntKey.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val filtered = pairs(stacked.collect())
    calls.get() shouldBe 2
    MapIntKey.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    filtered shouldBe Map(1 -> (1, 10L), 4 -> (1, 0L))
    calls.set(0)
    filtered shouldBe row(pairs(stacked.collect()))
    calls.get() shouldBe 2
  }

  it should "skip a null key above the map and count a null value" in {
    val mapCalls = new AtomicInteger
    val keyCalls = new AtomicInteger
    val counted = nullPairs().mapIntKey { key =>
      mapCalls.incrementAndGet()
      key + 1
    }.filterKeys { (key: Int) =>
      keyCalls.incrementAndGet()
      key > 2
    }.countSumByKey
    val expected = Map(
      6 -> (2, 4L),
      3 -> (1, 0L),
      8 -> (2, 4L),
    )
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      mapCalls.set(0)
      keyCalls.set(0)
      MapIntKey.resetPublishedForTest()
      CountSum.resetByKeyForTest()
      val found = pairs(counted.collect())
      MapIntKey.publishedForTest shouldBe 0
      CountSum.byKeyCallsForTest shouldBe 0
      (found, mapCalls.get(), keyCalls.get())
    }
    val (summed, mapped, keyed) = atBatch(8192)
    summed shouldBe expected
    mapped shouldBe 7
    keyed shouldBe 7
    atBatch(1) shouldBe (summed, mapped, keyed)
    mapCalls.set(0)
    row(pairs(counted.collect())) shouldBe expected
    mapCalls.get() shouldBe mapped

    val valueCalls = new AtomicInteger
    val values = nullPairs().mapIntKey { key =>
      mapCalls.incrementAndGet()
      key + 1
    }.filterValues { (value: Int) =>
      valueCalls.incrementAndGet()
      value > 0
    }.countSumByKey
    mapCalls.set(0)
    valueCalls.set(0)
    MapIntKey.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val columnarValues = pairs(values.collect())
    mapCalls.get() shouldBe 7
    valueCalls.get() shouldBe 7
    MapIntKey.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    columnarValues shouldBe Map(2 -> (2, 9L), 6 -> (2, 4L), 8 -> (1, 4L))
    mapCalls.set(0)
    row(pairs(values.collect())) shouldBe columnarValues
    mapCalls.get() shouldBe 7

    val nullValues = nullValue().mapIntKey { key =>
      mapCalls.incrementAndGet()
      key * 2
    }.filterKeys((key: Int) => key > 0).countSumByKey
    mapCalls.set(0)
    MapIntKey.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val countedNulls = pairs(nullValues.collect())
    mapCalls.get() shouldBe 2
    MapIntKey.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    countedNulls shouldBe Map(2 -> (1, 0L), 8 -> (1, 4L))
    mapCalls.set(0)
    row(pairs(nullValues.collect())) shouldBe countedNulls
    mapCalls.get() shouldBe 2
  }

  it should "leave a second slot and a whole-row filter above mapIntKey on the row path" in {
    val data = DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), partitions)
    val twice = data
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 2)
      .filterValues((value: Int) => value >= 20)
      .countSumByKey
    ColumnarPlanner.lower(twice.plan) shouldBe None
    val stacked = pairs(twice.collect())
    stacked shouldBe Map(3 -> (1, 20L), 4 -> (1, 40L))
    stacked shouldBe row(pairs(twice.collect()))

    val whole = data
      .mapIntKey((key: Int) => key + 1)
      .filter((pair: (Int, Int)) => pair._1 > 2 && pair._2 >= 20)
      .countSumByKey
    ColumnarPlanner.lower(whole.plan) shouldBe None
    val rows = pairs(whole.collect())
    rows shouldBe Map(3 -> (1, 20L), 4 -> (1, 40L))
    rows shouldBe row(pairs(whole.collect()))

    val nested = data
      .mapIntKey((key: Int) => key + 1)
      .mapIntKey((key: Int) => key * 2)
      .filterKeys((key: Int) => key > 2)
      .countSumByKey
    ColumnarPlanner.lower(nested.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(FilterKey(MapKey(MapKey(Scan)))))",
    )
    MapIntKey.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val doubled = pairs(nested.collect())
    MapIntKey.publishedForTest should be > 0
    CountSum.byKeyCallsForTest should be > 0
    doubled shouldBe Map(4 -> (2, 40L), 6 -> (1, 20L), 8 -> (1, 40L))
    doubled shouldBe row(pairs(nested.collect()))
  }

  it should "keep a second map and a whole-row filter on the interpreted path" in {
    val data = DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), partitions)
    val twice =
      data.mapIntKey((key: Int) => key + 1).mapIntKey((key: Int) => key * 2).countSumByKey
    ColumnarPlanner.lower(twice.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(MapKey(MapKey(Scan))))",
    )
    MapIntKey.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val doubled = pairs(twice.collect())
    MapIntKey.publishedForTest should be > 0
    CountSum.byKeyCallsForTest should be > 0
    doubled shouldBe Map(4 -> (2, 40L), 6 -> (1, 20L), 8 -> (1, 40L))
    doubled shouldBe row(pairs(twice.collect()))

    val whole = data
      .filter((pair: (Int, Int)) => pair._1 % 2 != 0 && pair._2 > 0)
      .mapIntKey((key: Int) => key + 1)
      .countSumByKey
    ColumnarPlanner.lower(whole.plan).map(_.shape) shouldBe Some(
      "CountSum(Exchange(MapKey(FilterRow(Scan))))",
    )
    MapIntKey.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val filtered = pairs(whole.collect())
    MapIntKey.publishedForTest should be > 0
    CountSum.byKeyCallsForTest should be > 0
    filtered shouldBe Map(2 -> (2, 40L), 4 -> (1, 40L))
    filtered shouldBe row(pairs(whole.collect()))
  }

  it should "count a null value and drop a null key, on both paths" in {
    val data = nullPairs()
    val counted = data.countSumByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some("CountSum(Exchange(Scan))")
    val columnar = pairs(counted.collect())
    val onRows = row(pairs(data.countSumByKey.collect()))
    val expected = Map(
      1 -> (2, 9L),
      5 -> (2, 4L),
      2 -> (1, 0L),
      7 -> (2, 4L),
    )
    columnar shouldBe expected
    onRows shouldBe expected
    CrossEngineShapes.tripleChecksum(data.countSumByKey.collect()) shouldBe 14649L
  }

  private val fixtureKeys = Array(0, 1, 2, 3, 4, 5, 1, 3, 6, 9)
  private val fixtureValues = Array(4, -2, 5, 1, 0, 8, 3, 7, -4, 2)

  private val expectedGroups = Map(1 -> (1, 3L), 2 -> (1, 5L), 4 -> (1, 0L), 5 -> (1, 8L))

  private def twogroup(): DistCollection[(Int, Int)] =
    DistCollection
      .ofIntPairs(fixtureKeys, fixtureValues, partitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)

  /** Columnar groups. An owned scan must not copy the pair arrays. */
  private def owned(counted: DistCollection[(Int, Int, Long)]): Map[Int, (Int, Long)] =
    val rows = ColumnMetrics.withSink(new CountingSink) {
      val found = counted.collect()
      ColumnMetrics.primitiveEncodeCount shouldBe 0L
      found
    }
    val columnar = pairs(rows)
    columnar shouldBe row(pairs(counted.collect()))
    columnar

  private def pairs(rows: Iterable[(Int, Int, Long)]): Map[Int, (Int, Long)] =
    rows.map { case (key, count, sum) => key -> (count, sum) }.toMap

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullPairs(): DistCollection[(Int, Int)] =
    def cell(key: Any, value: Any): (Any, Any) = (key, value)
    val source = Plan.Source(
      Seq(
        Partition(Seq(cell(1, 4), cell(null, 9), cell(5, 1), cell(1, 5), cell(7, null))),
        Partition(Seq(cell(5, 3), cell(2, null), cell(null, 8), cell(7, 4))),
      ),
    )
    DistCollection(source.asInstanceOf[Plan[(Int, Int)]])

  /**
   * A null key stays null, so count-sum drops it. A present key is `bump`. `productElement` keeps
   * a null value, so both paths count it and do not add it. The row path and the pair project call
   * this once per row, including a null key.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def shiftNullPair(
      calls: AtomicInteger,
      bump: Int => Int = _ + 1,
  ): ((Int, Int)) => (Int, Int) =
    pair => {
      val _ = calls.incrementAndGet()
      val product = pair.asInstanceOf[Product]
      val key = product.productElement(0) match
        case null => null
        case present: Int => Int.box(bump(present))
        case other => throw new IllegalArgumentException(s"pair map saw $other")
      Tuple2(key, product.productElement(1)).asInstanceOf[(Int, Int)]
    }

  /** One null value. The key stays present, so the key function still runs. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullValue(): DistCollection[(Int, Int)] =
    def cell(key: Any, value: Any): (Any, Any) = (key, value)
    val source = Plan.Source(Seq(Partition(Seq(cell(1, null), cell(4, 4)))))
    DistCollection(source.asInstanceOf[Plan[(Int, Int)]])

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
