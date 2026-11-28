package com.ewoodbury.scarlet.execution
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.{Describe, KernelChoice}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.support.Check.shouldBeEmpty

/**
 * `flatMap` keeps a columnar child when the decoded partitions match the row executor.
 *
 * An int scan publishes a batch, and `collect` equals the row `flatMap`. A checksum with no batch
 * is not this test. A null key or null value on a lowered filter does not match: the filter drops
 * the null, and the row predicate keeps the unboxed null. That `flatMap` is [[scala.None]], and
 * `collect` equals the row plan. Unboxing the null to `0` and calling that a match is not this
 * test. `flatMapValues` uses that same decode. An int-pair scan publishes a batch. A null value on
 * a lowered filter is [[scala.None]]. Untyped `sortBy` publishes the child and matches the row
 * order. A per-partition sort is not this test. `cogroup` publishes each scan. One null mismatch
 * is [[scala.None]] for the whole cogroup. A hash join is not this test. A refused `mapValues` to
 * string publishes the int-pair scan and matches the row map. A same-type `mapValues` still
 * lowers. A null key under that refused map is [[scala.None]]. Unboxing that null to `0` and
 * calling it a match is not this test.
 */
class TestRowBoundary extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    ()

  "an int scan under flatMap" should "publish the scan and match the row flatMap" in {
    val scanned = DistCollection.ofInts(Array(1, 2, 3, 4), 2)
    val mapped = scanned.flatMap((n: Int) => Seq(n, n + 10))
    Describe.describe(mapped.plan) shouldBe Option.empty[Describe.Described]
    ColumnarPlanner.lower(scanned.plan).isDefined shouldBe true
    mapped.plan match
      case Plan.FlatMapOp(source, f) =>
        RowBoundary.flatMap(source, f).isDefined shouldBe true
      case other => fail(s"expected flatMap, got $other")

    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      mapped.collect().toSeq
    }
    sink.batchCount should be > 0L
    found shouldBe row(mapped.collect().toSeq)
  }

  "a null key on filterKeys under flatMap" should "stay on the row plan" in {
    val scanned = DistCollection(Seq((1, 10), erased(None, Some(20)), (2, 30)), 1)
    val mapped = scanned.filterKeys((key: Int) => key >= 0).flatMap(keep)
    refuse(mapped)
    val found = show(collect(mapped))
    found shouldBe row(show(collect(mapped)))
    found.exists(_.contains("null")) shouldBe true
  }

  "a null value on filterValues under flatMap" should "stay on the row plan" in {
    val scanned = DistCollection(Seq((1, 10), erased(Some(2), None), (3, 30)), 1)
    val mapped = scanned.filterValues((value: Int) => value >= 0).flatMap(keep)
    refuse(mapped)
    val found = show(collect(mapped))
    found shouldBe row(show(collect(mapped)))
    found.exists(_.contains("null")) shouldBe true
  }

  "an int-pair scan under flatMapValues" should "publish the scan and match the row function" in {
    val scanned = DistCollection.ofIntPairs(Array(1, 2, 3, 4), Array(10, 20, 30, 40), 2)
    val mapped = scanned.flatMapValues((value: Int) => Seq(value, value + 1))
    Describe.describe(mapped.plan) shouldBe Option.empty[Describe.Described]
    ColumnarPlanner.lower(scanned.plan).isDefined shouldBe true
    mapped.plan match
      case Plan.FlatMapValuesOp(source, f) =>
        RowBoundary.flatMapValues(source, f).isDefined shouldBe true
      case other => fail(s"expected flatMapValues, got $other")

    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      mapped.collect().toSeq
    }
    sink.batchCount should be > 0L
    found shouldBe row(mapped.collect().toSeq)
  }

  "a null value on filterValues under flatMapValues" should "stay on the row plan" in {
    val scanned = DistCollection(Seq((1, 10), erased(Some(2), None), (3, 30)), 1)
    val mapped =
      scanned.filterValues((value: Int) => value >= 0).flatMapValues((value: Int) => Seq(value))
    Describe.describe(mapped.plan) shouldBe Option.empty[Describe.Described]
    mapped.plan match
      case Plan.FlatMapValuesOp(source, f) =>
        RowBoundary.flatMapValues(source, f).shouldBeEmpty
      case other => fail(s"expected flatMapValues, got $other")
    collect(mapped) shouldBe row(collect(mapped))
  }

  "an int scan under sortBy" should "publish the scan and match the row order" in {
    val scanned = DistCollection.ofInts(Array(3, 1, 4, 2), 2)
    val sorted = scanned.sortBy((n: Int) => n)
    Describe.describe(sorted.plan) shouldBe Option.empty[Describe.Described]
    ColumnarPlanner.lower(scanned.plan).isDefined shouldBe true
    sorted.plan match
      case Plan.SortByOp(source, keyFunc, ordering) =>
        RowBoundary.sortBy(source, keyFunc, ordering).isDefined shouldBe true
      case other => fail(s"expected sortBy, got $other")

    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      sorted.collect().toSeq
    }
    sink.batchCount should be > 0L
    found shouldBe Seq(1, 2, 3, 4)
    found shouldBe row(sorted.collect().toSeq)
  }

  "a null key on filterKeys under sortBy" should "stay on the row plan" in {
    val scanned = DistCollection(Seq((1, 10), erased(None, Some(20)), (2, 30)), 1)
    val sorted =
      scanned.filterKeys((key: Int) => key >= 0).sortBy((pair: (Int, Int)) => pair._1)
    Describe.describe(sorted.plan) shouldBe Option.empty[Describe.Described]
    sorted.plan match
      case Plan.SortByOp(source, keyFunc, ordering) =>
        RowBoundary.sortBy(source, keyFunc, ordering).shouldBeEmpty
      case other => fail(s"expected sortBy, got $other")
    val found = show(collect(sorted))
    found shouldBe row(show(collect(sorted)))
    found.exists(_.contains("null")) shouldBe true
  }

  "two int-pair scans under cogroup" should "publish both scans and match the row groups" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 1)
    val right = DistCollection.ofIntPairs(Array(2, 3), Array(30, 40), 1)
    val grouped = left.cogroup(right)
    Describe.describe(grouped.plan) shouldBe Option.empty[Describe.Described]
    ColumnarPlanner.lower(left.plan).isDefined shouldBe true
    ColumnarPlanner.lower(right.plan).isDefined shouldBe true
    grouped.plan match
      case Plan.CoGroupOp(leftPlan, rightPlan) =>
        RowBoundary.cogroup(leftPlan, rightPlan).isDefined shouldBe true
      case other => fail(s"expected cogroup, got $other")

    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      grouped.collect().toSeq
    }
    sink.batchCount should be >= 2L
    intGroups(found) shouldBe Map(
      1 -> (Seq(10), Seq.empty[Int]),
      2 -> (Seq(20), Seq(30)),
      3 -> (Seq.empty[Int], Seq(40)),
    )
    found shouldBe row(grouped.collect().toSeq)
  }

  "a null key on one cogroup side" should "refuse the whole cogroup" in {
    val left = DistCollection(Seq((1, 10), erased(None, Some(20)), (2, 30)), 1)
      .filterKeys((key: Int) => key >= 0)
    val right = DistCollection.ofIntPairs(Array(1), Array(40), 1)
    val grouped = left.cogroup(right)
    Describe.describe(grouped.plan) shouldBe Option.empty[Describe.Described]
    grouped.plan match
      case Plan.CoGroupOp(leftPlan, rightPlan) =>
        RowBoundary.cogroup(leftPlan, rightPlan).shouldBeEmpty
      case other => fail(s"expected cogroup, got $other")
    grouped.collect().toSeq shouldBe row(grouped.collect().toSeq)
  }

  "a scan beside a row map under cogroup" should "publish the scan and match the row groups" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 1)
    val right =
      DistCollection.ofIntPairs(Array(2), Array(7), 1).mapValues((value: Int) => value.toString)
    val grouped = left.cogroup(right)
    ColumnarPlanner.lower(left.plan).isDefined shouldBe true
    ColumnarPlanner.lower(right.plan).isDefined shouldBe false
    grouped.plan match
      case Plan.CoGroupOp(leftPlan, rightPlan) =>
        RowBoundary.cogroup(leftPlan, rightPlan).isDefined shouldBe true
      case other => fail(s"expected cogroup, got $other")

    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      grouped.collect().toSeq
    }
    sink.batchCount should be > 0L
    stringGroups(found) shouldBe Map(
      1 -> (Seq(10), Seq.empty[String]),
      2 -> (Seq(20), Seq("7")),
    )
    found shouldBe row(grouped.collect().toSeq)
  }

  "an int-pair scan under mapValues to string" should "publish the scan and match the row map" in {
    val scanned = DistCollection.ofIntPairs(Array(1, 2, 3, 4), Array(10, 20, 30, 40), 2)
    val mapped = scanned.mapValues((value: Int) => value.toString)
    Describe.describe(mapped.plan) shouldBe Option.empty[Describe.Described]
    ColumnarPlanner.lower(mapped.plan).isDefined shouldBe false
    ColumnarPlanner.lower(scanned.plan).isDefined shouldBe true
    mapped.plan match
      case Plan.MapValuesOp(source, f) =>
        RowBoundary.mapValues(source, f).isDefined shouldBe true
      case other => fail(s"expected mapValues, got $other")

    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      mapped.collect().toSeq
    }
    sink.batchCount should be > 0L
    found shouldBe Seq((1, "10"), (2, "20"), (3, "30"), (4, "40"))
    found shouldBe row(mapped.collect().toSeq)
  }

  "an int scan under map to string" should "publish the scan and match the row map" in {
    val scanned = DistCollection.ofInts(Array(1, 2, 3, 4), 2)
    val mapped = scanned.map((n: Int) => n.toString)
    Describe.describe(mapped.plan) shouldBe Option.empty[Describe.Described]
    ColumnarPlanner.lower(mapped.plan).isDefined shouldBe false
    ColumnarPlanner.lower(scanned.plan).isDefined shouldBe true
    mapped.plan match
      case Plan.MapOp(source, f) =>
        RowBoundary.map(source, f).isDefined shouldBe true
      case other => fail(s"expected map, got $other")

    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      mapped.collect().toSeq
    }
    sink.batchCount should be > 0L
    found shouldBe Seq("1", "2", "3", "4")
    found shouldBe row(mapped.collect().toSeq)
  }

  "an int mapValues" should "still lower as MapValue" in {
    val scanned = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 1)
    val mapped = scanned.mapValues((value: Int) => value + 1)
    ColumnarPlanner.lower(mapped.plan).isDefined shouldBe true
    Describe.describe(mapped.plan).map(_.choice) shouldBe Some(KernelChoice.MapValue)
  }

  "a null key on filterKeys under mapValues to string" should "stay on the row plan" in {
    val scanned = DistCollection(Seq((1, 10), erased(None, Some(20)), (2, 30)), 1)
    val mapped =
      scanned.filterKeys((key: Int) => key >= 0).mapValues((value: Int) => value.toString)
    Describe.describe(mapped.plan) shouldBe Option.empty[Describe.Described]
    mapped.plan match
      case Plan.MapValuesOp(source, f) =>
        RowBoundary.mapValues(source, f).shouldBeEmpty
      case other => fail(s"expected mapValues, got $other")
    val found = collect(mapped).map(showCell)
    found shouldBe row(collect(mapped).map(showCell))
    found.exists(_.contains("null")) shouldBe true
  }

  private def intGroups(
      rows: Seq[(Int, (Iterable[Int], Iterable[Int]))],
  ): Map[Int, (Seq[Int], Seq[Int])] =
    rows.map { case (key, (leftVals, rightVals)) =>
      key -> (leftVals.toSeq, rightVals.toSeq)
    }.toMap

  private def stringGroups(
      rows: Seq[(Int, (Iterable[Int], Iterable[String]))],
  ): Map[Int, (Seq[Int], Seq[String])] =
    rows.map { case (key, (leftVals, rightVals)) =>
      key -> (leftVals.toSeq, rightVals.toSeq)
    }.toMap

  private def refuse(mapped: DistCollection[(Int, Int)]): Unit =
    Describe.describe(mapped.plan) shouldBe Option.empty[Describe.Described]
    mapped.plan match
      case Plan.FlatMapOp(source, f) =>
        RowBoundary.flatMap(source, f).shouldBeEmpty
      case other => fail(s"expected flatMap, got $other")

  private def keep(row: (Int, Int)): Seq[(Int, Int)] = Seq(row)

  private def collect[A](mapped: DistCollection[A]): Seq[A] =
    ScarletRuntime.get.shuffle.clear()
    mapped.collect().toSeq

  private def show(rows: Seq[(Int, Int)]): Seq[String] = rows.map(showCell)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def showCell(row: Product): String =
    val cells = (0 until row.productArity).map { index =>
      row.productElement(index) match
        case null => "null"
        case other => String.valueOf(other)
    }
    cells.mkString("[", ",", "]")

  /** Named erasure boundary. `Tuple2` keeps a null `Integer`; `(Int, Int)` would unbox it. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erased(key: Option[Int], value: Option[Int]): (Int, Int) =
    val left: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val right: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(left, right).asInstanceOf[(Int, Int)]

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()
