package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Known answers for the four added cross-engine shapes, on one small int table.
 *
 * The filter is `k % 3 != 0` and `v >= 0`, except the top-N shape, which filters `sk`. The
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
    ColumnarPlanner.lower(counted.plan).isEmpty shouldBe true
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

  "a generic value sort" should "decline tryRun and stay off the columnar arm" in {
    val data = filtered(topKeys, topValues)
    val plan = data
      .sortBy(pair => (pair._2, pair._1))(using Ordering.Tuple2(Ordering.Int.reverse, Ordering.Int))
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
