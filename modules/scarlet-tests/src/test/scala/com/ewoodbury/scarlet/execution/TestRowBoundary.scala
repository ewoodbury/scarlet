package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.Describe
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `flatMap` keeps a columnar child when the decoded partitions match the row executor.
 *
 * An int scan publishes a batch, and `collect` equals the row `flatMap`. A checksum with no batch
 * is not this test. A null key or null value on a lowered filter does not match: the filter drops
 * the null, and the row predicate keeps the unboxed null. That `flatMap` is [[scala.None]], and
 * `collect` equals the row plan. Unboxing the null to `0` and calling that a match is not this
 * test. `flatMapValues` uses that same decode. An int-pair scan publishes a batch. A null value on
 * a lowered filter is [[scala.None]]. Untyped `sortBy` publishes the child and matches the row
 * order. A per-partition sort is not this test.
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
        RowBoundary.flatMapValues(source, f) shouldBe None
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
        RowBoundary.sortBy(source, keyFunc, ordering) shouldBe None
      case other => fail(s"expected sortBy, got $other")
    val found = show(collect(sorted))
    found shouldBe row(show(collect(sorted)))
    found.exists(_.contains("null")) shouldBe true
  }

  private def refuse(mapped: DistCollection[(Int, Int)]): Unit =
    Describe.describe(mapped.plan) shouldBe Option.empty[Describe.Described]
    mapped.plan match
      case Plan.FlatMapOp(source, f) =>
        RowBoundary.flatMap(source, f) shouldBe None
      case other => fail(s"expected flatMap, got $other")

  private def keep(row: (Int, Int)): Seq[(Int, Int)] = Seq(row)

  private def collect(mapped: DistCollection[(Int, Int)]): Seq[(Int, Int)] =
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
  private def showCell(row: (Int, Int)): String =
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
