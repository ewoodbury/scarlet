package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Present-value plans in the closed kernel.
 *
 * Each case lowers to a fixed physical shape, and [[ColumnarPlanner.tryRun]] matches the row
 * executor. Nulls and inputs over the in-memory budget are outside this suite.
 */
class TestClosedKernel extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "the closed kernel" should "filter ints and count the survivors" in {
    val filtered = DistCollection
      .ofInts(Array(1, 2, 3, 4), 2)
      .filter((value: Int) => value % 2 == 0)
    val columnar = lowered(filtered.plan, "Filter(Scan)")
    columnar shouldBe rowValues(filtered.plan)
    columnar.length.toLong shouldBe rowAction(filtered.count())
  }

  it should "filter longs and sum the survivors" in {
    val filtered = DistCollection
      .ofLongs(Array(1L, 5L, 2L, 8L), 2)
      .filter((value: Long) => value > 2L)
    val columnar = lowered(filtered.plan, "Filter(Scan)")
    columnar shouldBe rowValues(filtered.plan)
    columnar.sum shouldBe rowAction(filtered.sum)
  }

  it should "filter doubles and sum the survivors" in {
    val filtered = DistCollection
      .ofDoubles(Array(1.5, -2.0, 3.5, 0.5), 2)
      .filter((value: Double) => value > 1.0)
    val columnar = lowered(filtered.plan, "Filter(Scan)")
    columnar shouldBe rowValues(filtered.plan)
    columnar.sum shouldBe rowAction(filtered.sum)
  }

  it should "filter strings and count the survivors" in {
    val filtered = DistCollection
      .ofStrings(Array("red", "", "blue", "green"), 2)
      .filter((value: String) => value.nonEmpty)
    val columnar = lowered(filtered.plan, "Filter(Scan)")
    columnar shouldBe rowValues(filtered.plan)
    columnar.length.toLong shouldBe rowAction(filtered.count())
  }

  it should "filter booleans and count the survivors" in {
    val filtered = DistCollection
      .ofBools(Array(true, false, true, false), 2)
      .filter((value: Boolean) => value)
    val columnar = lowered(filtered.plan, "Filter(Scan)")
    columnar shouldBe rowValues(filtered.plan)
    columnar.length.toLong shouldBe rowAction(filtered.count())
  }

  it should "filter int-pair keys and values and count the survivors" in {
    val filtered = DistCollection
      .ofIntPairs(Array(1, 2, 3, 2), Array(10, -5, 30, 7), 2)
      .filterKeys((key: Int) => key > 1)
      .filterValues((value: Int) => value > 0)
    val columnar = lowered(filtered.plan, "FilterValue(FilterKey(Scan))")
    columnar shouldBe rowValues(filtered.plan)
    columnar.length.toLong shouldBe rowAction(filtered.count())
  }

  it should "sum int pairs with reduceByKey and collect a map" in {
    val reduced = DistCollection
      .ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 5, 7), 2)
      .reduceByKey((left: Int, right: Int) => left + right)
    assertMap(reduced.plan, "HashAggregate(Exchange(Scan))")
  }

  it should "sum long pairs with reduceByKey and collect a map" in {
    val reduced = DistCollection
      .ofLongPairs(Array(1L, 2L, 1L, 3L), Array(10L, 20L, 5L, 7L), 2)
      .reduceByKey((left: Long, right: Long) => left + right)
    assertMap(reduced.plan, "LongHashAggregate(Exchange(Scan))")
  }

  it should "sum string pairs with reduceByKey and collect a map" in {
    val reduced = stringPairScan(Seq("red" -> 1, "blue" -> 2), Seq("red" -> 4, "green" -> 3))
      .reduceByKey((left: Int, right: Int) => left + right)
    assertMap(reduced.plan, "StringHashAggregate(Exchange(Scan))")
  }

  it should "count int pairs by key and collect a map" in {
    val counted = DistCollection
      .ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 5, 7), 2)
      .countByKey
    assertMap(counted.plan, "CountByKey(Exchange(Scan))")
  }

  it should "count long pairs by key and collect a map" in {
    val counted = DistCollection
      .ofLongPairs(Array(1L, 2L, 1L, 3L), Array(10L, 20L, 5L, 7L), 2)
      .countByKey
    assertMap(counted.plan, "CountByKey(Exchange(Scan))")
  }

  it should "count string pairs by key and collect a map" in {
    val counted = stringPairScan(Seq("red" -> 1, "blue" -> 2), Seq("red" -> 4, "green" -> 3))
      .countByKey
    assertMap(counted.plan, "CountByKey(Exchange(Scan))")
  }

  it should "count an int inner join" in {
    val left = DistCollection.ofIntPairs(Array(1, 2, 1), Array(10, 20, 11), 2)
    val right = DistCollection.ofIntPairs(Array(1, 3, 2), Array(100, 300, 200), 2)
    assertCount(left.join(right).plan, "HashJoin(Exchange(Scan), Exchange(Scan))")
  }

  it should "count a long inner join" in {
    val left = DistCollection.ofLongPairs(Array(1L, 2L, 1L), Array(10L, 20L, 11L), 2)
    val right = DistCollection.ofLongPairs(Array(1L, 3L, 2L), Array(100L, 300L, 200L), 2)
    assertCount(left.join(right).plan, "LongHashJoin(Exchange(Scan), Exchange(Scan))")
  }

  it should "count a string inner join" in {
    val left = stringPairScan(Seq("red" -> 1, "blue" -> 2), Seq("red" -> 4))
    val right = stringPairScan(Seq("red" -> 10, "green" -> 30), Seq("blue" -> 20))
    assertCount(left.join(right).plan, "StringHashJoin(Exchange(Scan), Exchange(Scan))")
  }

  it should "sort int pairs by key and keep row order" in {
    val sorted = DistCollection
      .ofIntPairs(Array(3, 1, 2, 1), Array(30, 10, 20, 11), 2)
      .sortByIntKey
    assertSequence(sorted.plan, "SortByIntKey(Scan)")
  }

  it should "sort long pairs by key and keep row order" in {
    val sorted = DistCollection
      .ofLongPairs(Array(3L, 1L, 2L, 1L), Array(30L, 10L, 20L, 11L), 2)
      .sortByLongKey
    assertSequence(sorted.plan, "SortByLongKey(Scan)")
  }

  it should "sort string pairs by key and keep row order" in {
    val sorted = stringPairScan(Seq("b" -> 2, "a" -> 1), Seq("c" -> 3, "a" -> 4)).sortByStringKey
    assertSequence(sorted.plan, "SortByStringKey(Scan)")
  }

  /**
   * `(String, Int)` scan. There is no pair factory for strings; other suites build the same
   * `Plan.Source` of partitions.
   */
  private def stringPairScan(
      left: Seq[(String, Int)],
      right: Seq[(String, Int)],
  ): DistCollection[(String, Int)] =
    DistCollection(Plan.Source(Seq(Partition(left), Partition(right))))

  private def lowered[A](plan: Plan[A], expectedShape: String): Seq[A] =
    ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(expectedShape)
    decoded(plan)

  private def decoded[A](plan: Plan[A]): Seq[A] =
    val ran = ColumnarPlanner.tryRun(plan)
    ran shouldBe defined
    ran.getOrElse(fail("tryRun returned None")).flatMap(_.data).toList

  private def assertSequence[A](plan: Plan[A], expectedShape: String): Unit =
    lowered(plan, expectedShape) shouldBe rowValues(plan)

  private def assertMap[K, V](plan: Plan[(K, V)], expectedShape: String): Unit =
    val columnar = lowered(plan, expectedShape)
    val rows = rowValues(plan)
    columnar.toMap shouldBe rows.toMap
    columnar.length shouldBe rows.length

  private def assertCount[A](plan: Plan[A], expectedShape: String): Unit =
    val columnar = lowered(plan, expectedShape)
    val rows = rowValues(plan)
    columnar.length shouldBe rows.length
    columnar should contain theSameElementsAs rows

  /** Row executor. Columnar execution is off, and shuffle state from `tryRun` is cleared first. */
  private def rowValues[A](plan: Plan[A]): Seq[A] =
    rowAction(DistCollection(plan).collect().toList)

  private def rowAction[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
