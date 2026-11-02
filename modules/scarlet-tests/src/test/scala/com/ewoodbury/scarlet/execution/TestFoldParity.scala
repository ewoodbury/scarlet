package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{BatchCodec, ColumnBatch, ColumnReduce}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Fold results against the row action and, when `lower` accepts the plan, against `interpret`.
 *
 * Grouped sum is compared only to the row action. A later null still lowers, and `interpret`
 * unboxes it. A null on the other ordinal drops the row before the key predicate. That is not the
 * survivor count `Selection` would publish for the same key filter.
 */
class TestFoldParity extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(running)

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "numeric folds" should "match the row action and interpret" in {
    val ints = DistCollection(Seq(1, -2, 3, 0, 4), 2)
    val longs = DistCollection(Seq(1L, -2L, 3L, 0L, 8L), 2)
    val doubles = DistCollection(Seq(1.0, -2.0, 3.0, 0.0, 8.0), 2)

    agreeInt("int scan", ints, "Scan")
    agreeLong("long scan", longs, "Scan")
    agreeDouble("double scan", doubles, "Scan")

    agreeInt("int filter", ints.filter(_ > 0), "Filter(Scan)")
    agreeLong("long filter", longs.filter(_ > 0L), "Filter(Scan)")
    agreeDouble("double filter", doubles.filter(_ > 0.0), "Filter(Scan)")

    agreeInt("int pipeline 2", ints.filter(_ > 0).map(_ + 1), "Pipeline2(Scan)")
    agreeLong("long pipeline 2", longs.filter(_ > 0L).map(_ + 1L), "Pipeline2(Scan)")
    agreeDouble("double pipeline 2", doubles.filter(_ > 0.0).map(_ + 1.0), "Pipeline2(Scan)")

    val intPipe3 = ints.filter(_ > 0).map(_ + 1).filter(_ % 2 == 0)
    val longPipe3 = longs.filter(_ > 0L).map(_ + 1L).filter(_ % 2L == 0L)
    val doublePipe3 = doubles.filter(_ > 0.0).map(_ + 1.0).filter(_ % 2.0 == 0.0)
    agreeInt("int pipeline 3", intPipe3, "Pipeline3(Scan)")
    agreeLong("long pipeline 3", longPipe3, "Pipeline3(Scan)")
    agreeDouble("double pipeline 3", doublePipe3, "Pipeline3(Scan)")

    agreeInt("int pipeline 4", intPipe3.map(_ * 10), "Pipeline4(Scan)")
    agreeLong("long pipeline 4", longPipe3.map(_ * 10L), "Pipeline4(Scan)")
    agreeDouble("double pipeline 4", doublePipe3.map(_ * 10.0), "Pipeline4(Scan)")

    agreeInt("five int maps", plus(ints, 5), "Project(Pipeline4(Scan))")
  }

  "string folds" should "agree on an empty string, a null cell, and contradicted literals" in {
    val rows = texts()
    val source = DistCollection(rows, 1)

    agreeTexts("empty string", source.filterStringEquals(""), Seq("", ""))
    val calls = new AtomicInteger
    val absent = new AtomicInteger
    val general = source.filter { (value: String) =>
      calls.incrementAndGet()
      if (BatchCodec.isAbsent(value)) then absent.incrementAndGet()
      BatchCodec.isAbsent(value) || value.isEmpty
    }
    val expectedCount = row(general.count())
    val expectedCells = row(general.collect().toList).map(present)
    calls.set(0)
    absent.set(0)
    ColumnarPlanner.lower(general.plan).map(_.shape) shouldBe Some("Filter(Scan)")
    ColumnarPlanner.tryCount(general.plan) shouldBe Some(expectedCount)
    calls.get() shouldBe rows.length
    absent.get() shouldBe 1
    counted(general.plan) shouldBe expectedCount
    decodedTexts(general.plan) shouldBe expectedCells

    agreeCount(
      "contradicted dictionary groups",
      source.filterStringEquals("US").filterStringEquals("CA"),
      "Filter(Filter(Scan))",
    )
    ColumnarPlanner.tryCount(source.filterStringEquals("US").filterStringEquals("CA").plan) shouldBe
      Some(0L)
  }

  "a boolean chain" should "match the row count and interpret" in {
    val flags = DistCollection(Seq(true, false, true, false), 1)
      .filter((value: Boolean) => value || !value)
      .filter((value: Boolean) => value || !value)
      .filter((value: Boolean) => value)
    agreeCount("bool chain", flags, "Filter(Filter(Filter(Scan)))")
  }

  "grouped sum" should "match the row action for filters, a pair map, and null components" in {
    val rows = Seq(1 -> 4, 1 -> 5, 2 -> 6, 1 -> 1)
    val source = DistCollection(rows, 2)

    agreeGrouped("duplicate keys", source)
    agreeGrouped("filter", source.filter { case (key, value) => key == 1 && value > 1 })
    agreeGrouped("filterKeys", source.filterKeys[Int, Int](_ > 1))
    agreeGrouped("filterValues", source.filterValues[Int, Int](_ > 4))

    val mapped = source.map { case (key, value) => (key + 1, value + key) }
    ColumnarPlanner.lower(mapped.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.tryRun(mapped.plan) shouldBe None
    agreeGrouped("pair map", mapped)

    val projected = DistCollection.ofIntPairs(Array(1, 2, 3, 4), Array(10, 5, 30, 40), 2)
    agreeInt(
      "two value filters",
      projected.filterValues[Int, Int](_ > 0).filterValues[Int, Int](_ % 2 == 0).values,
      "Value(FilterValue(FilterValue(Scan)))",
    )
    agreeInt(
      "two key filters",
      projected.filterKeys[Int, Int](_ % 2 == 1).filterKeys[Int, Int](_ > 0).keys,
      "Key(FilterKey(FilterKey(Scan)))",
    )
  }

  it should "skip the key predicate when the other ordinal is null" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val nullKey = DistCollection(
      Seq(erased(None, Some(9)), erased(Some(1), Some(2)), erased(None, None)),
      1,
    ).filterKeys { (key: Int) =>
      keyCalls.incrementAndGet()
      key > 0
    }
    val nullValue = DistCollection(
      Seq(erased(Some(4), None), erased(Some(2), Some(5))),
      1,
    ).filterValues { (value: Int) =>
      valueCalls.incrementAndGet()
      value > 0
    }
    val otherOrdinal = DistCollection(
      Seq(erased(Some(4), None), erased(Some(2), Some(5))),
      1,
    ).filterKeys { (key: Int) =>
      keyCalls.incrementAndGet()
      key > 0
    }

    // Each action is one pass. `row` runs first, then the counter is cleared for the fold.
    callsOnce("null key", nullKey, keyCalls, 1)
    callsOnce("null value", nullValue, valueCalls, 1)
    // Selection would still call this predicate on the null-value row. Grouped sum drops it.
    callsOnce("null on the other ordinal", otherOrdinal, keyCalls, 1)
    ColumnarPlanner.tryGroupedSum(otherOrdinal.plan) shouldBe Some(5L)
  }

  private def plus(source: DistCollection[Int], steps: Int): DistCollection[Int] =
    (1 to steps).foldLeft(source)((current, _) => current.map(_ + 1))

  private def agreeInt(label: String, collection: DistCollection[Int], shape: String): Unit =
    withClue(s"$label: ") {
      ColumnarPlanner.lower(collection.plan).map(_.shape) shouldBe Some(shape)
      val expectedSum = row(collection.sum)
      val expectedCount = row(collection.count())
      ColumnarPlanner.trySumInt(collection.plan) shouldBe Some(expectedSum)
      ColumnarPlanner.tryCount(collection.plan) shouldBe Some(expectedCount)
      summedInts(collection.plan) shouldBe expectedSum
      counted(collection.plan) shouldBe expectedCount
    }

  private def agreeLong(label: String, collection: DistCollection[Long], shape: String): Unit =
    withClue(s"$label: ") {
      ColumnarPlanner.lower(collection.plan).map(_.shape) shouldBe Some(shape)
      val expectedSum = row(collection.sum)
      val expectedCount = row(collection.count())
      ColumnarPlanner.trySumLong(collection.plan) shouldBe Some(expectedSum)
      ColumnarPlanner.tryCount(collection.plan) shouldBe Some(expectedCount)
      summedLongs(collection.plan) shouldBe expectedSum
      counted(collection.plan) shouldBe expectedCount
    }

  private def agreeDouble(
      label: String,
      collection: DistCollection[Double],
      shape: String,
  ): Unit =
    withClue(s"$label: ") {
      ColumnarPlanner.lower(collection.plan).map(_.shape) shouldBe Some(shape)
      val expectedSum = row(collection.sum)
      val expectedCount = row(collection.count())
      ColumnarPlanner.trySumDouble(collection.plan) shouldBe Some(expectedSum)
      ColumnarPlanner.tryCount(collection.plan) shouldBe Some(expectedCount)
      summedDoubles(collection.plan) shouldBe expectedSum
      counted(collection.plan) shouldBe expectedCount
    }

  private def agreeCount(label: String, collection: DistCollection[_], shape: String): Unit =
    withClue(s"$label: ") {
      ColumnarPlanner.lower(collection.plan).map(_.shape) shouldBe Some(shape)
      val expected = row(collection.count())
      ColumnarPlanner.tryCount(collection.plan) shouldBe Some(expected)
      counted(collection.plan) shouldBe expected
    }

  private def agreeTexts(
      label: String,
      collection: DistCollection[String],
      cells: Seq[String],
  ): Unit =
    withClue(s"$label: ") {
      val expected = row(collection.collect().toList).map(present)
      expected shouldBe cells.map(present)
      ColumnarPlanner.tryCount(collection.plan) shouldBe Some(expected.length.toLong)
      counted(collection.plan) shouldBe expected.length.toLong
      decodedTexts(collection.plan) shouldBe expected
    }

  private def callsOnce(
      label: String,
      collection: DistCollection[(Int, Int)],
      calls: AtomicInteger,
      expectedCalls: Int,
  ): Unit =
    val expected = row(collection.groupedSum)
    calls.set(0)
    withClue(s"$label: ") {
      ColumnarPlanner.tryGroupedSum(collection.plan) shouldBe Some(expected)
      calls.get() shouldBe expectedCalls
    }

  private def agreeGrouped(label: String, collection: DistCollection[(Int, Int)]): Unit =
    withClue(s"$label: ") {
      val expected = row(collection.groupedSum)
      ColumnarPlanner.tryGroupedSum(collection.plan) shouldBe Some(expected)
    }

  private def summedInts(plan: Plan[Int]): Long =
    ColumnReduce.sumInt32(batches(plan))

  private def summedLongs(plan: Plan[Long]): Long =
    ColumnReduce.sumInt64(batches(plan), 0)

  private def summedDoubles(plan: Plan[Double]): Double =
    ColumnReduce.sumFloat64(batches(plan))

  private def counted(plan: Plan[_]): Long =
    ColumnReduce.count(batches(plan))

  private def batches(plan: Plan[_]): Vector[ColumnBatch] =
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("expected a columnar plan"))
    ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op))

  private def decodedTexts(plan: Plan[_]): Seq[Option[String]] =
    batches(plan).flatMap(BatchCodec.decodeNullableStrings)

  private def present(value: String): Option[String] =
    if (BatchCodec.isAbsent(value)) then None else Some(value)

  private def running: ScarletConf =
    original.copy(columnarExecution = true, columnarBatchSize = 1024, threadPoolSize = 1)

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletRuntime.get.shuffle.clear()
      ScarletConf.set(saved.copy(columnarExecution = false))
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def texts(): Seq[String] =
    Seq("US", "", null, "CA", "")

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erased(key: Option[Int], value: Option[Int]): (Int, Int) =
    val left: AnyRef = key match
      case Some(presentKey) => Int.box(presentKey)
      case None => null
    val right: AnyRef = value match
      case Some(presentValue) => Int.box(presentValue)
      case None => null
    Tuple2(left, right).asInstanceOf[(Int, Int)]
