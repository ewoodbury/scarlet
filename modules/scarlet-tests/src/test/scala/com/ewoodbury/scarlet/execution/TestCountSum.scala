package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, GroupByKey}
import com.ewoodbury.scarlet.core.{IntPairArrays, Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `countSumByKey` against the row path.
 *
 * Non-null rows match `groupByKey.map` with the count-and-sum fold. A null key is dropped. A
 * null value is counted and not added. Generic `groupByKey.map` stays `GroupedDecode` and still
 * builds value lists. This operator does not.
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
        data.groupByKey.map { case (key, grouped) =>
          val (count, sum) = grouped.iterator.foldLeft((0, 0L)) { case ((count, sum), value) =>
            (count + 1, sum + value.toLong)
          }
          (key, count, sum)
        }.collect()
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

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
