package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
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
      "GroupedDecode(GroupByKey(Exchange(FilterValue(FilterKey(Scan)))))",
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

    ColumnarPlanner.lower(aggregated.plan).isEmpty shouldBe true
    ColumnarPlanner.partitioning(aggregated.plan).isEmpty shouldBe true
    ColumnarPlanner.lower(joined.plan).map(_.shape) shouldBe Some(
      "HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(FilterValue(FilterKey(Scan))))",
    )
    val projected = joined.map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
    ColumnarPlanner.lower(projected.plan).map(_.shape) shouldBe Some(
      "Project(" +
        "HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(FilterValue(FilterKey(Scan))))" +
        ")",
    )
    ColumnarPlanner.tryRun(projected.plan).isEmpty shouldBe true
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
