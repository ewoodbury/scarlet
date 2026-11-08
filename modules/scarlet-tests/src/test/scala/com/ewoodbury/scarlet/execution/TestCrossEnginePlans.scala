package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}

/**
 * Plan shapes for the six cross-engine queries. A few dozen rows, no actions, and no timing.
 *
 * Present pairs are checked with one filter and with two. Null values are checked for the
 * two-filter int sort, the two-filter int join, and the long reduce. The null value is not on the
 * first row: a leading null is not a pair witness. There is no null key. Spill is on, then
 * restored.
 */
class TestCrossEnginePlans extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val rows = 48
  private val partitions = 2
  private val nullAt = 5

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8192,
        threadPoolSize = 4,
        defaultShufflePartitions = 4,
        spillEnabled = true,
      ),
    )

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ()

  "present pairs" should "lower every query for one filter and for two" in {
    expect("filter-count/1", present("filter-count", 1), "FilterKey(Scan)")
    expect("filter-count/2", present("filter-count", 2), "FilterValue(FilterKey(Scan))")
    expect("filter-sum/1", present("filter-sum", 1), "Value(FilterKey(Scan))")
    expect("filter-sum/2", present("filter-sum", 2), "Value(FilterValue(FilterKey(Scan)))")
    expect(
      "filter-groupbykey/1",
      present("filter-groupbykey", 1),
      "GroupByKey(Exchange(FilterKey(Scan)))",
    )
    expect(
      "filter-groupbykey/2",
      present("filter-groupbykey", 2),
      "GroupByKey(Exchange(FilterValue(FilterKey(Scan))))",
    )
    expect(
      "filter-reducebykey/1",
      present("filter-reducebykey", 1),
      "LongHashAggregate(Exchange(FilterKey(Scan)))",
    )
    expect(
      "filter-reducebykey/2",
      present("filter-reducebykey", 2),
      "LongHashAggregate(Exchange(FilterValue(FilterKey(Scan))))",
    )
    expect(
      "filter-join/1",
      present("filter-join", 1),
      "HashJoin(Exchange(FilterKey(Scan)), Exchange(Scan))",
    )
    expect(
      "filter-join/2",
      present("filter-join", 2),
      "HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(Scan))",
    )
    expect("filter-sort/1", present("filter-sort", 1), "SortByIntKey(FilterKey(Scan))")
    expect(
      "filter-sort/2",
      present("filter-sort", 2),
      "SortByIntKey(FilterValue(FilterKey(Scan)))",
    )
  }

  "null values" should "lower the confirmed sort, join, and long reduce" in {
    expect(
      "null-sort/2",
      filterInts(nullInts).sortByIntKey.plan,
      "SortByIntKey(FilterValue(FilterKey(Scan)))",
    )
    expect(
      "null-join/2",
      filterInts(nullInts).join(joinSide).plan,
      "HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(Scan))",
    )
    expect(
      "null-reduce/1",
      filterLongs(nullLongs, 1).reduceByKey[Long, Long](_ + _).plan,
      "LongHashAggregate(Exchange(FilterKey(Scan)))",
    )
    expect(
      "null-reduce/2",
      filterLongs(nullLongs, 2).reduceByKey[Long, Long](_ + _).plan,
      "LongHashAggregate(Exchange(FilterValue(FilterKey(Scan))))",
    )
  }

  private def expect(label: String, plan: Plan[_], expected: String): Unit =
    withClue(label) {
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(expected)
    }

  private def present(name: String, filters: Int): Plan[_] = name match
    case "filter-count" => filterInts(intSource, filters).plan
    case "filter-sum" => filterInts(intSource, filters).values.plan
    case "filter-groupbykey" => filterInts(intSource, filters).groupByKey.plan
    case "filter-reducebykey" =>
      filterLongs(longSource, filters).reduceByKey[Long, Long](_ + _).plan
    case "filter-join" => filterInts(intSource, filters).join(joinSide).plan
    case "filter-sort" => filterInts(intSource, filters).sortByIntKey.plan
    case other => fail(s"unknown query $other")

  private def intSource: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(
      Array.tabulate(rows)(index => index % 1024),
      Array.tabulate(rows)(index => index - 24),
      partitions,
    )

  private def longSource: DistCollection[(Long, Long)] =
    DistCollection.ofLongPairs(
      Array.tabulate(rows)(index => (index % 1024).toLong),
      Array.tabulate(rows)(index => (index - 24).toLong),
      partitions,
    )

  private def joinSide: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(0, 1, 2, 3), Array(1, 2, 3, 4), partitions)

  private def nullInts: DistCollection[(Int, Int)] =
    DistCollection(Seq.tabulate(rows)(nullInt), partitions)

  private def nullLongs: DistCollection[(Long, Long)] =
    DistCollection(Seq.tabulate(rows)(nullLong), partitions)

  private def nullInt(index: Int): (Int, Int) =
    val key = index % 1024
    if (index == nullAt) then intPair(boxedInt(key), absentInt) else key -> (index + 1)

  private def nullLong(index: Int): (Long, Long) =
    val key = (index % 1024).toLong
    if (index == nullAt) then longPair(boxedLong(key), absentLong) else key -> (index + 1L)

  private def filterInts(
      data: DistCollection[(Int, Int)],
      filters: Int = 2,
  ): DistCollection[(Int, Int)] =
    val keyed = data.filterKeys((key: Int) => key % 3 != 0)
    if (filters == 1) then keyed
    else keyed.filterValues((value: Int) => value >= 0)

  private def filterLongs(
      data: DistCollection[(Long, Long)],
      filters: Int,
  ): DistCollection[(Long, Long)] =
    val keyed = data.filterKeys((key: Long) => key % 3 != 0)
    if (filters == 1) then keyed
    else keyed.filterValues((value: Long) => value >= 0)

  /** Named erasure boundary. `Tuple2` keeps a null `Integer`; `(Int, Int)` would unbox it. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  /** Named erasure boundary. `Tuple2` keeps a null `Long`; `(Long, Long)` would unbox it. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def longPair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentInt: java.lang.Integer = null

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentLong: java.lang.Long = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)

  private def boxedLong(value: Long): java.lang.Long = java.lang.Long.valueOf(value)
