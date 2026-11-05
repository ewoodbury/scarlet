package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{ExecutionService, Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/** Int and string `groupByKey` on the columnar path, compared with the row engine per key. */
class TestGroupByKeyColumnar extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val keys = Array(1, 2, -1, 4, Int.MinValue, 1, 2, 1, 4)
  private val values = Array(10, 20, 30, 40, 50, 11, 21, 12, 41)

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 2,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "groupByKey" should "match the row path value order and count groups" in {
    val source = DistCollection.ofIntPairs(keys, values, 4)
    val grouped = source.groupByKey
    val found = valuesByKey(grouped.collect())

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    source.count() shouldBe 9L
    grouped.count() shouldBe 5L
    grouped.count() shouldBe row(grouped.count())
    found shouldBe valuesByKey(row(grouped.collect()))
    found shouldBe Map(
      1 -> Seq(10, 11, 12),
      2 -> Seq(20, 21),
      -1 -> Seq(30),
      4 -> Seq(40, 41),
      Int.MinValue -> Seq(50),
    )
    ExecutionService.get.executePartitions(grouped.plan).size shouldBe
      ScarletConf.get.defaultShufflePartitions
  }

  it should "keep duplicate values and several input partitions" in {
    val source = DistCollection.ofIntPairs(Array(3, 3, 5, 3), Array(1, 2, 3, 4), 4)
    val grouped = source.groupByKey

    valuesByKey(grouped.collect()) shouldBe valuesByKey(row(grouped.collect()))
    valuesByKey(grouped.collect()) shouldBe Map(3 -> Seq(1, 2, 4), 5 -> Seq(3))
    grouped.count() shouldBe 2L
  }

  it should "publish an empty partition for every shuffle slot of an empty source" in {
    val grouped = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 2).groupByKey
    val parts = ExecutionService.get.executePartitions(grouped.plan)

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    parts.size shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(_.data shouldBe empty)
    grouped.collect() shouldBe empty
    grouped.count() shouldBe 0L
  }

  it should "keep empty shuffle partitions beside the populated ones" in {
    val grouped = DistCollection.ofIntPairs(Array(4, 8, 0, 4), Array(1, 2, 3, 4), 3).groupByKey
    val parts = ExecutionService.get.executePartitions(grouped.plan)

    parts.size shouldBe 4
    parts.count(_.data.nonEmpty) shouldBe 1
    valuesByKey(grouped.collect()) shouldBe Map(4 -> Seq(1, 4), 8 -> Seq(2), 0 -> Seq(3))
    grouped.count() shouldBe 3L
  }

  it should "lower a filterValues child and refuse projections above the group" in {
    val source = DistCollection.ofIntPairs(keys, values, 4)
    val filtered = source.filterValues((value: Int) => value > 10).groupByKey
    val grouped = source.groupByKey

    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some(
      "GroupByKey(Exchange(FilterValue(Scan)))",
    )
    valuesByKey(filtered.collect()) shouldBe valuesByKey(row(filtered.collect()))
    valuesByKey(filtered.collect())(1) shouldBe Seq(11, 12)
    ColumnarPlanner.lower(grouped.keys.plan) shouldBe None
    ColumnarPlanner.lower(grouped.values.plan) shouldBe None
    ColumnarPlanner.lower(grouped.map((row: (Int, Iterable[Int])) => row._1).plan) shouldBe None
    ColumnarPlanner.tryTake(grouped.plan, 1) shouldBe None
    grouped.take(2) shouldBe row(grouped.take(2))
  }

  it should "refuse a union of two grouped results" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 2).groupByKey
    val right = DistCollection.ofIntPairs(Array(1, 3), Array(11, 30), 2).groupByKey
    val union = left.union(right)

    ColumnarPlanner.lower(union.plan) shouldBe None
    valuesByKey(union.collect()) shouldBe valuesByKey(row(union.collect()))
  }

  "string groupByKey" should "match the row path value order and count groups" in {
    val source = stringSource
    val grouped = source.groupByKey
    val found = stringValues(grouped.collect())

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    ColumnarPlanner.partitioning(grouped.plan).map(_.layout) shouldBe Some(
      Layout.GroupedString(ScarletConf.get.columnarBatchSize),
    )
    source.count() shouldBe 12L
    grouped.count() shouldBe 5L
    grouped.count() shouldBe row(grouped.count())
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(5L)
    found shouldBe stringValues(row(grouped.collect()))
    found shouldBe Map(
      "" -> Seq(1, 8),
      accent -> Seq(2, 9, 9),
      decomposed -> Seq(3, 10),
      emoji -> Seq(4, 7),
      "red" -> Seq(5, 6),
    )
    accent should not be decomposed
    ExecutionService.get.executePartitions(grouped.plan).size shouldBe
      ScarletConf.get.defaultShufflePartitions
  }

  it should "keep duplicate values and several input partitions" in {
    val source = DistCollection(Seq("red" -> 1, "red" -> 1, "blue" -> 2, "red" -> 3), 4)
    val grouped = source.groupByKey

    stringValues(grouped.collect()) shouldBe stringValues(row(grouped.collect()))
    stringValues(grouped.collect()) shouldBe Map("red" -> Seq(1, 1, 3), "blue" -> Seq(2))
    grouped.count() shouldBe 2L
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(2L)
  }

  it should "leave an empty iterable on the row path" in {
    val grouped = DistCollection(Seq.empty[(String, Int)], 2).groupByKey
    val parts = ExecutionService.get.executePartitions(grouped.plan)

    ColumnarPlanner.lower(grouped.plan) shouldBe None
    parts.size shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(_.data shouldBe empty)
    grouped.collect() shouldBe empty
    grouped.count() shouldBe 0L
    grouped.count() shouldBe row(grouped.count())
  }

  it should "publish an empty grouped batch when a filter keeps nothing" in {
    val grouped = stringSource.filterValues((value: Int) => value < 0).groupByKey
    val parts = ExecutionService.get.executePartitions(grouped.plan)

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some(
      "GroupByKey(Exchange(FilterValue(Scan)))",
    )
    parts.size shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(_.data shouldBe empty)
    grouped.collect() shouldBe empty
    grouped.count() shouldBe 0L
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(0L)
  }

  it should "lower a filterValues child and refuse projections above the group" in {
    val filtered = stringSource.filterValues((value: Int) => value > 4).groupByKey
    val grouped = stringSource.groupByKey

    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some(
      "GroupByKey(Exchange(FilterValue(Scan)))",
    )
    stringValues(filtered.collect()) shouldBe stringValues(row(filtered.collect()))
    stringValues(filtered.collect()) shouldBe Map(
      "" -> Seq(8),
      accent -> Seq(9, 9),
      decomposed -> Seq(10),
      emoji -> Seq(7),
      "red" -> Seq(5, 6),
    )
    ColumnarPlanner.lower(grouped.keys.plan) shouldBe None
    ColumnarPlanner.lower(grouped.values.plan) shouldBe None
    ColumnarPlanner.lower(grouped.map((row: (String, Iterable[Int])) => row._1).plan) shouldBe None
    ColumnarPlanner.tryTake(grouped.plan, 1) shouldBe None
    grouped.take(2) shouldBe row(grouped.take(2))
  }

  it should "refuse a union of two grouped string results" in {
    val left = DistCollection(Seq("a" -> 1, "b" -> 2), 2).groupByKey
    val right = DistCollection(Seq("a" -> 3, "c" -> 4), 2).groupByKey
    val union = left.union(right)

    ColumnarPlanner.lower(union.plan) shouldBe None
    stringValues(union.collect()) shouldBe stringValues(row(union.collect()))
  }

  private val accent = "é"
  private val decomposed = "e\u0301"
  private val emoji = "😀"

  private def stringSource: DistCollection[(String, Int)] =
    DistCollection(Plan.Source(Seq(Partition(leftRows), Partition(rightRows))))

  private def leftRows: Seq[(String, Int)] = Seq(
    "" -> 1,
    accent -> 2,
    decomposed -> 3,
    emoji -> 4,
    "red" -> 5,
    nullText(9),
    "red" -> 6,
  )

  private def rightRows: Seq[(String, Int)] = Seq(
    emoji -> 7,
    "" -> 8,
    accent -> 9,
    decomposed -> 10,
    accent -> 9,
  )

  private def stringValues(rows: Iterable[(String, Iterable[Int])]): Map[String, Seq[Int]] =
    rows.map { case (key, values) => key -> values.toSeq }.toMap

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullText(value: Int): (String, Int) = (null: String) -> value

  private def valuesByKey(rows: Iterable[(Int, Iterable[Int])]): Map[Int, Seq[Int]] =
    rows.map { case (key, values) => key -> values.toSeq }.toMap

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
