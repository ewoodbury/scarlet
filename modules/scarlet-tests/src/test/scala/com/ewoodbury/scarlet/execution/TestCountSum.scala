package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.GroupByKey
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
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
    val keys = Array(0, 1, 2, 3, 4, 5, 1, 3, 6, 9)
    val values = Array(4, -2, 5, 1, 0, 8, 3, 7, -4, 2)
    val data = DistCollection
      .ofIntPairs(keys, values, partitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    GroupByKey.resetValueListsBuilt()
    val columnar = pairs(CrossEngineShapes.twoGroup(data).collect())
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
    columnar shouldBe Map(1 -> (1, 3L), 2 -> (1, 5L), 4 -> (1, 0L), 5 -> (1, 8L))
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
