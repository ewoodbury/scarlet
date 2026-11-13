package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * One map directly above `groupByKey` decodes the grouped batch once.
 *
 * The function still sees `(K, Iterable[V])`. `keys`, `values`, and `take` stay on the row path.
 * A map above a sort stays on the row path. A map above an int inner join stays the triple
 * project it already was.
 */
class TestGroupedDecode extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 4,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "a map above int groupByKey" should "match the row path, including a null key and a key seen once" in {
    val source = DistCollection(
      Seq(
        (1, 10),
        erasedInt(None, Some(99)),
        (1, 11),
        (2, 20),
        (1, 12),
      ),
      2,
    )
    val grouped = source.groupByKey
    val mapped = grouped.map { case (key, values) => (key, values.toSeq) }

    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some(
      "GroupedDecode(GroupByKey(Exchange(Scan)))",
    )
    ColumnarPlanner.partitioning(mapped.plan).map(_.layout) shouldBe Some(
      Layout.GroupedInt(ScarletConf.get.columnarBatchSize),
    )
    ColumnarPlanner.lower(grouped.keys.plan) shouldBe None
    ColumnarPlanner.lower(grouped.values.plan) shouldBe None
    ColumnarPlanner.tryTake(grouped.plan, 1) shouldBe None
    ColumnarPlanner.tryTake(mapped.plan, 1) shouldBe None
    val found = mapped.collect().toMap
    found shouldBe row(mapped.collect().toMap)
    found shouldBe Map(1 -> Seq(10, 11, 12), 2 -> Seq(20))
    found(2) should have size 1
    found.keySet shouldBe Set(1, 2)
  }

  it should "read both the key and the values" in {
    val source = DistCollection.ofIntPairs(Array(4, 4, 7), Array(1, 2, 3), 2)
    val mapped = source.groupByKey.map { case (key, values) => (key, values.sum) }

    ColumnarPlanner.tryRun(mapped.plan) shouldBe defined
    mapped.collect().toMap shouldBe row(mapped.collect().toMap)
    mapped.collect().toMap shouldBe Map(4 -> 3, 7 -> 3)
  }

  it should "scan the group once" in {
    val calls = new AtomicInteger
    val source = DistCollection.ofIntPairs(Array(1, 1, 2, 3), Array(10, 11, 20, 30), 2)
    val mapped = source
      .filterKeys { (_: Int) =>
        val _ = calls.incrementAndGet()
        true
      }
      .groupByKey
      .map { case (key, values) => (key, values.toSeq) }

    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some(
      "GroupedDecode(GroupByKey(Exchange(FilterKey(Scan))))",
    )
    calls.set(0)
    val found = mapped.collect().toMap
    calls.get() shouldBe 4
    found shouldBe Map(1 -> Seq(10, 11), 2 -> Seq(20), 3 -> Seq(30))
  }

  "a map above string groupByKey" should "drop a null key and keep a key seen once" in {
    val source = DistCollection(Seq("a" -> 1, nullKey(9), "a" -> 2, "b" -> 3), 2)
    val mapped = source.groupByKey.map { case (key, values) => (key, values.toSeq) }

    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some(
      "GroupedDecode(GroupByKey(Exchange(Scan)))",
    )
    val found = mapped.collect().toMap
    found shouldBe row(mapped.collect().toMap)
    found shouldBe Map("a" -> Seq(1, 2), "b" -> Seq(3))
    found("b") should have size 1
  }

  "a map above long groupByKey" should "drop a null key and read the key and the values" in {
    val source = DistCollection(
      Seq(
        (1L, 10L),
        erasedLong(None, Some(99L)),
        (1L, 11L),
        (8L, 40L),
      ),
      2,
    )
    val mapped = source.groupByKey.map { case (key, values) => (key, values.sum) }

    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some(
      "GroupedDecode(GroupByKey(Exchange(Scan)))",
    )
    val found = mapped.collect().toMap
    found shouldBe row(mapped.collect().toMap)
    found shouldBe Map(1L -> 21L, 8L -> 40L)
  }

  "a second map" should "stay on the row path" in {
    val twice = DistCollection
      .ofIntPairs(Array(1, 1), Array(4, 5), 2)
      .groupByKey
      .map { case (key, values) => (key, values.sum) }
      .map { case (key, total) => key + total }

    ColumnarPlanner.lower(twice.plan) shouldBe None
    twice.collect().toSeq should contain theSameElementsAs row(twice.collect().toSeq)
  }

  "a map above a sort" should "stay on the row path" in {
    val mapped = DistCollection
      .ofIntPairs(Array(2, 1), Array(20, 10), 2)
      .sortByIntKey
      .map { case (key, value) => (key, value + 1) }

    ColumnarPlanner.lower(mapped.plan) shouldBe None
    mapped.collect().toSeq shouldBe row(mapped.collect().toSeq)
  }

  "a map above a join" should "stay the triple project" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 2)
    val right = DistCollection.ofIntPairs(Array(1, 2), Array(100, 200), 2)
    val mapped = left.join(right).map { case (key, pair) => (key, (pair._1, pair._2)) }

    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some(
      "Project(HashJoin(Exchange(Scan), Exchange(Scan)))",
    )
    mapped.collect().toMap shouldBe row(mapped.collect().toMap)
  }

  /** Named erasure boundary. `Tuple2` keeps a null `Integer`; `(Int, Int)` would unbox it. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erasedInt(key: Option[Int], value: Option[Int]): (Int, Int) =
    val left: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val right: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(left, right).asInstanceOf[(Int, Int)]

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

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
