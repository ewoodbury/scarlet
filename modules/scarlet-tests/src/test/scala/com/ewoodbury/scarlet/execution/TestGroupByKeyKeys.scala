package com.ewoodbury.scarlet.execution
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.GroupByKey
import com.ewoodbury.scarlet.core.{ExecutionService, Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.support.Check.shouldBeEmpty

/**
 * Int, long, and string `keys` of `groupByKey` against the row path.
 *
 * Equality is the sequence, per shuffle partition and concatenated. Duplicate keys, one and four
 * partitions, a null key, a null value, and one key on two input partitions are covered. Order is
 * the row path's `groupMap` order on that key type. `""` is kept. `"é"` and `"e\u0301"` stay
 * distinct. `values` and `take` stay unlowered. A map above the group decodes once.
 */
class TestGroupByKeyKeys extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

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
    GroupByKey.resetValueListsBuilt()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    GroupByKey.resetValueListsBuilt()
    ()

  "keys of int groupByKey" should "match the row path for duplicates on one partition" in {
    val source = DistCollection.ofIntPairs(Array(5, 5, 1, 5, 6), Array(1, 2, 3, 4, 5), 1)
    val keys = source.groupByKey.keys

    ColumnarPlanner.lower(keys.plan).map(_.shape) shouldBe Some(
      "Key(CountByKey(Exchange(Scan)))",
    )
    assertSameOrder(keys)
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = 1))
    assertSameOrder(keys)
  }

  it should "match the row path across four input partitions" in {
    val source = DistCollection.ofIntPairs(
      Array(5, 1, 5, 2, 1, 6, 9, Int.MinValue),
      Array(1, 2, 3, 4, 5, 6, 7, 8),
      4,
    )
    val keys = source.groupByKey.keys

    assertSameOrder(keys)
    keys.count() shouldBe row(keys.count())
    keys.sum shouldBe row(keys.sum)
  }

  it should "keep one key that lands on two input partitions" in {
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = 2))
    val source = DistCollection.ofIntPairs(Array(5, 6, 5, 7), Array(1, 2, 3, 4), 2)
    val keys = source.groupByKey.keys

    assertSameOrder(keys)
    partitionsOf(keys.plan).count(_.nonEmpty) shouldBe 2
  }

  it should "drop a null key, keep a null value's key, and share a key across partitions" in {
    assertNullKey()
  }

  /**
   * Named erasure boundary. The source is boxed so a null key and a null value can sit beside
   * present ints. Collected keys are `Int` once the null key is dropped.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def assertNullKey(): Unit =
    val source = nullKeySource()
    val keys = source.groupByKey.keys

    ColumnarPlanner.lower(keys.plan).map(_.shape) shouldBe Some(
      "Key(CountByKey(Exchange(Scan)))",
    )
    assertSameOrder(keys)
    keys.collect().toList.map(_.asInstanceOf[Int]) shouldBe List(1, 5, 2)

  it should "lower keys of a value filter without building value lists" in {
    val source = DistCollection.ofIntPairs(Array(5, 1, 5, 2), Array(1, 30, 3, 4), 2)
    val grouped = source.filterValues((value: Int) => value > 2).groupByKey
    val keys = grouped.keys

    ColumnarPlanner.lower(keys.plan).map(_.shape) shouldBe Some(
      "Key(CountByKey(Exchange(FilterValue(Scan))))",
    )
    GroupByKey.resetValueListsBuilt()
    val _ = keys.collect()
    GroupByKey.valueListsBuilt shouldBe 0L
    assertSameOrder(keys)
  }

  it should "leave values and take on the row path and decode a map" in {
    val grouped = DistCollection.ofIntPairs(Array(5, 1, 5), Array(1, 2, 3), 2).groupByKey
    val mapped = grouped.map((row: (Int, Iterable[Int])) => row._1)

    ColumnarPlanner.partitioning(grouped.values.plan).shouldBeEmpty
    ColumnarPlanner.lower(grouped.values.plan).shouldBeEmpty
    ColumnarPlanner.tryTake(grouped.plan, 1).shouldBeEmpty
    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some(
      "GroupedDecode(GroupByKey(Exchange(Scan)))",
    )
    ColumnarPlanner.partitioning(mapped.plan).map(_.layout) shouldBe Some(
      Layout.GroupedInt(ScarletConf.get.columnarBatchSize),
    )
  }

  it should "build value lists for the group and none for keys" in {
    val grouped = DistCollection.ofIntPairs(Array(5, 1, 5, 2), Array(1, 2, 3, 4), 2)

    GroupByKey.resetValueListsBuilt()
    val _ = grouped.groupByKey.collect()
    GroupByKey.valueListsBuilt should be > 0L

    GroupByKey.resetValueListsBuilt()
    val _ = grouped.groupByKey.keys.collect()
    GroupByKey.valueListsBuilt shouldBe 0L

    GroupByKey.resetValueListsBuilt()
    val _ = row(grouped.groupByKey.keys.collect())
    GroupByKey.valueListsBuilt should be > 0L
  }

  "keys of long groupByKey" should "match the row path for duplicates on one partition" in {
    val source =
      DistCollection.ofLongPairs(Array(5L, 5L, 1L, 5L, 6L), Array(1L, 2L, 3L, 4L, 5L), 1)
    val keys = source.groupByKey.keys

    ColumnarPlanner.lower(keys.plan).map(_.shape) shouldBe Some(
      "Key(CountByKey(Exchange(Scan)))",
    )
    assertSameOrder(keys)
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = 1))
    assertSameOrder(keys)
  }

  it should "match the row path across four input partitions" in {
    val source = DistCollection.ofLongPairs(
      Array(5L, 1L, 5L, 2L, 1L, 6L, 9L, Long.MinValue),
      Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L),
      4,
    )
    val keys = source.groupByKey.keys

    assertSameOrder(keys)
    keys.count() shouldBe row(keys.count())
    keys.sum shouldBe row(keys.sum)
  }

  it should "keep hash-colliding longs distinct" in {
    val collided = 1L << 32
    val source = DistCollection.ofLongPairs(
      Array(1L, collided, Long.MinValue, 1L, collided),
      Array(1L, 2L, 3L, 4L, 5L),
      4,
    )
    val keys = source.groupByKey.keys

    assertSameOrder(keys)
    keys.collect().toSet shouldBe Set(1L, collided, Long.MinValue)
  }

  it should "drop a null key and keep a null value's key" in {
    assertNullLongKey()
  }

  it should "lower keys of a value filter without building value lists" in {
    val source = DistCollection.ofLongPairs(Array(5L, 1L, 5L, 2L), Array(1L, 30L, 3L, 4L), 2)
    val keys = source.filterValues((value: Long) => value > 2L).groupByKey.keys

    ColumnarPlanner.lower(keys.plan).map(_.shape) shouldBe Some(
      "Key(CountByKey(Exchange(FilterValue(Scan))))",
    )
    GroupByKey.resetValueListsBuilt()
    val _ = keys.collect()
    GroupByKey.valueListsBuilt shouldBe 0L
    assertSameOrder(keys)
  }

  it should "build value lists for the group and none for keys" in {
    val grouped = DistCollection.ofLongPairs(Array(5L, 1L, 5L, 2L), Array(1L, 2L, 3L, 4L), 2)

    GroupByKey.resetValueListsBuilt()
    val _ = grouped.groupByKey.collect()
    GroupByKey.valueListsBuilt should be > 0L

    GroupByKey.resetValueListsBuilt()
    val _ = grouped.groupByKey.keys.collect()
    GroupByKey.valueListsBuilt shouldBe 0L

    GroupByKey.resetValueListsBuilt()
    val _ = row(grouped.groupByKey.keys.collect())
    GroupByKey.valueListsBuilt should be > 0L
  }

  "keys of string groupByKey" should "keep an empty string and distinct accents" in {
    val accent = "é"
    val decomposed = "e\u0301"
    val source = DistCollection(
      Seq("" -> 1, accent -> 2, decomposed -> 3, "red" -> 4, "" -> 5, accent -> 6),
      4,
    )
    val keys = source.groupByKey.keys

    ColumnarPlanner.lower(keys.plan).map(_.shape) shouldBe Some(
      "Key(CountByKey(Exchange(Scan)))",
    )
    assertSameOrder(keys)
    val found = keys.collect().toList
    found should contain(accent)
    found should contain(decomposed)
    found should contain("")
    accent should not be decomposed
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = 1))
    assertSameOrder(keys)
  }

  it should "drop a null key and keep a null value's key" in {
    assertNullStringKey()
  }

  it should "lower keys of a value filter without building value lists" in {
    val source = DistCollection(Seq("red" -> 1, "blue" -> 4, "red" -> 2, "" -> 9), 2)
    val keys = source.filterValues((value: Int) => value > 2).groupByKey.keys

    ColumnarPlanner.lower(keys.plan).map(_.shape) shouldBe Some(
      "Key(CountByKey(Exchange(FilterValue(Scan))))",
    )
    GroupByKey.resetValueListsBuilt()
    val _ = keys.collect()
    GroupByKey.valueListsBuilt shouldBe 0L
    assertSameOrder(keys)
  }

  it should "build value lists for the group and none for keys" in {
    val grouped = DistCollection(Seq("red" -> 1, "blue" -> 2, "red" -> 3), 2)

    GroupByKey.resetValueListsBuilt()
    val _ = grouped.groupByKey.collect()
    GroupByKey.valueListsBuilt should be > 0L

    GroupByKey.resetValueListsBuilt()
    val _ = grouped.groupByKey.keys.collect()
    GroupByKey.valueListsBuilt shouldBe 0L

    GroupByKey.resetValueListsBuilt()
    val _ = row(grouped.groupByKey.keys.collect())
    GroupByKey.valueListsBuilt should be > 0L
  }

  private def assertSameOrder[A](keys: DistCollection[A]): Unit =
    val found = partitionsOf(keys.plan)
    found shouldBe row(partitionsOf(keys.plan))
    keys.collect().toList shouldBe row(keys.collect().toList)

  private def partitionsOf[A](plan: Plan[A]): Seq[Seq[A]] =
    ExecutionService.get.executePartitions(plan).map(_.data.toSeq)

  /**
   * A present pair is first, so the scan classifies as long pairs. The null key is later and is
   * dropped. Key 2 has a null value and must still appear.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def assertNullLongKey(): Unit =
    val source = DistCollection(
      Seq(
        (1L, 4L),
        erasedLong(None, Some(9L)),
        (5L, 1L),
        (1L, 5L),
        (5L, 3L),
        erasedLong(Some(2L), None),
        erasedLong(None, Some(8L)),
      ),
      2,
    )
    val keys = source.groupByKey.keys

    ColumnarPlanner.lower(keys.plan).map(_.shape) shouldBe Some(
      "Key(CountByKey(Exchange(Scan)))",
    )
    assertSameOrder(keys)
    keys.collect().toSet shouldBe Set(1L, 5L, 2L)

  /** Named erasure boundary. `Tuple2` keeps a null `Long`; `(Long, Long)` would unbox it. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erasedLong(key: Option[Long], value: Option[Long]): (Long, Long) =
    val left: AnyRef = key match
      case Some(present) => Long.box(present)
      case None => null
    val right: AnyRef = value match
      case Some(present) => Long.box(present)
      case None => null
    Tuple2(left, right).asInstanceOf[(Long, Long)]

  /**
   * A present pair is first, so the scan classifies as string pairs. The null key is dropped. Key
   * `c` has a null value and must still appear. `""` is kept.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def assertNullStringKey(): Unit =
    val source = DistCollection(
      Seq(
        "a" -> 1,
        erasedString(None, Some(9)),
        "b" -> 2,
        erasedString(Some("c"), None),
        "" -> 4,
        "a" -> 5,
      ),
      2,
    )
    val keys = source.groupByKey.keys

    ColumnarPlanner.lower(keys.plan).map(_.shape) shouldBe Some(
      "Key(CountByKey(Exchange(Scan)))",
    )
    assertSameOrder(keys)
    keys.collect().toSet shouldBe Set("a", "b", "c", "")

  /** Named erasure boundary. `Tuple2` keeps a null `String` or `Integer`. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erasedString(key: Option[String], value: Option[Int]): (String, Int) =
    val left: AnyRef = key match
      case Some(present) => present
      case None => null
    val right: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(left, right).asInstanceOf[(String, Int)]

  /**
   * A present pair is first, so the scan classifies as int pairs. The null key is later and is
   * dropped. Key 5 is on both partitions. Key 2 has a null value and must still appear.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullKeySource(): DistCollection[(Any, Any)] =
    def cell(key: Any, value: Any): (Any, Any) = (key, value)
    DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq(cell(1, 4), cell(null, 9), cell(5, 1), cell(1, 5))),
          Partition(Seq(cell(5, 3), cell(2, null), cell(null, 8))),
        ),
      ),
    )

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
