package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, HashJoin}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `take` and `count` split a mixed plan before any `MapPartitionsOp` wrapper.
 *
 * A fully bound sort stays on [[ColumnarPlanner.tryTake]]. A fully bound count stays on
 * [[ColumnarPlanner.tryCount]]. A `mapPartitions` tail over `countByKey` publishes that child's
 * batches, counts the tail, and `take` returns a short prefix in row order. `flatMap` and untyped
 * `sortBy` publish nothing. A lowered `filterKeys` or `filterValues` drops a null the row
 * predicate unboxes to `0`. [[ColumnarPlanner.tryRun]] of an int `mapValues` over a null key
 * throws. Either child under the tail still returns the row prefix. A one-sided join whose
 * lowered side disagrees makes [[MixedJoin.partitions]] return `None`. [[Describe]] is not the
 * route. A checksum is not the prefix.
 */
class TestMixedTake extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()
    HashJoin.resetKernelCallsForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    HashJoin.resetKernelCallsForTest()
    ()

  "sortByIntKey" should "take the tryTake prefix" in {
    val sorted = pairs.sortByIntKey
    val full = ColumnarPlanner.tryTake(sorted.plan, 4).getOrElse(fail("sort did not take"))
    full.length shouldBe 4
    val small = decoded(sorted, 2)
    val wide = decoded(sorted, 4)
    small.rows shouldBe full.take(2).toList
    wide.rows shouldBe full.toList
    small.decodedRows should be < wide.decodedRows
    small.batches should be > 0L
  }

  "countByKey" should "count through tryCount" in {
    val counted = pairs.countByKey
    val expected = ColumnarPlanner.tryCount(counted.plan).getOrElse(fail("count did not bind"))
    val seen = observeCount(counted)
    seen.count shouldBe expected
    seen.batches should be > 0L
  }

  "mapPartitions over countByKey" should "count the tail and take a short prefix" in {
    val counted = pairs.countByKey
    val doubled = twice(counted)
    ColumnarPlanner.lower(doubled.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.tryCount(doubled.plan) shouldBe Option.empty[Long]
    ColumnarPlanner.tryCount(counted.plan) shouldBe Some(3L)
    ColumnarPlanner.lower(counted.plan).isDefined shouldBe true

    val child = new CountingSink
    val childRan = ColumnMetrics.withSink(child) {
      ScarletRuntime.get.shuffle.clear()
      ColumnarPlanner.tryRun(counted.plan).isDefined
    }
    childRan shouldBe true
    child.batchCount should be > 0L

    val countedTail = observeCount(doubled)
    countedTail.count shouldBe 6L
    countedTail.count shouldBe row(doubled.count())
    countedTail.batches shouldBe child.batchCount

    val full = row(doubled.collect().toList)
    full.length shouldBe 6
    val prefix = doubled.take(2)
    prefix.length shouldBe 2
    prefix shouldBe full.take(2)
    prefix match
      case first :: second :: Nil =>
        second shouldBe (first._1, first._2 + 100)
      case _ => fail("take(2) was not a two-row prefix")

    val seen = observe(doubled.take(2))
    seen.rows shouldBe show(full.take(2))
    seen.batches shouldBe child.batchCount
  }

  "flatMap over countByKey" should "publish no column batch" in {
    val flat = pairs.countByKey.flatMap((row: (Int, Int)) => Seq(row))
    ColumnarPlanner.lower(flat.plan) shouldBe Option.empty[PhysicalOp]
    val counted = observeCount(flat)
    counted.count shouldBe row(flat.count())
    counted.batches shouldBe 0L
    val seen = observe(flat.take(2))
    seen.rows shouldBe row(show(flat.take(2)))
    seen.batches shouldBe 0L
    seen.rows.length shouldBe 2
  }

  "untyped sortBy over countByKey" should "publish no column batch" in {
    val sorted = pairs.countByKey.sortBy((row: (Int, Int)) => row._1)
    ColumnarPlanner.lower(sorted.plan) shouldBe Option.empty[PhysicalOp]
    val counted = observeCount(sorted)
    counted.count shouldBe row(sorted.count())
    counted.batches shouldBe 0L
    val seen = observe(sorted.take(2))
    seen.rows shouldBe row(show(sorted.take(2)))
    seen.batches shouldBe 0L
    seen.rows.length shouldBe 2
  }

  "mapPartitions over filterKeys of a null key" should "keep the unboxed row" in {
    val filtered = nullKey.filterKeys((key: Int) => key >= 0)
    disagree(filtered, pass(filtered)) shouldBe "short"
  }

  "mapPartitions over filterValues of a null value" should "keep the unboxed row" in {
    val filtered = nullValue.filterValues((value: Int) => value >= 0)
    disagree(filtered, pass(filtered)) shouldBe "short"
  }

  "mapPartitions over filterKeys of a null value" should "keep the row the decoder sees" in {
    val filtered = nullValue.filterKeys((key: Int) => key >= 0)
    disagree(filtered, pass(filtered)) shouldBe "rejected"
  }

  "mapPartitions over filterValues of a null key" should "keep the row the decoder sees" in {
    val filtered = nullKey.filterValues((value: Int) => value >= 0)
    disagree(filtered, pass(filtered)) shouldBe "rejected"
  }

  "mapPartitions over mapValues of a null key" should "keep the null key" in {
    val mapped = nullKey.mapValues((value: Int) => value + 1)
    disagree(mapped, pass(mapped)) shouldBe "rejected"
  }

  "mapPartitions over a null source" should "keep the stored null without decoding" in {
    val thrown = the[IllegalArgumentException] thrownBy ColumnarPlanner.tryRun(nullKey.plan)
    thrown.getMessage should include("null Int32 pair")
    val tailed = pass(nullKey)
    val counted = observeCount(tailed)
    counted.count shouldBe 3L
    counted.count shouldBe row(tailed.count())
    counted.batches shouldBe 0L
    val seen = observe(tailed.take(2))
    seen.rows shouldBe row(show(tailed.take(2)))
    seen.batches shouldBe 0L
    seen.rows.exists(_.contains("null")) shouldBe true
  }

  "a present one-sided join" should "publish the lowered side" in {
    val joined = left.join(right.flatMap(keep))
    ColumnarPlanner.lower(joined.plan) shouldBe Option.empty[PhysicalOp]
    MixedJoin.partitions(joined.plan).isDefined shouldBe true
    HashJoin.resetKernelCallsForTest()
    val counted = observeCount(joined)
    HashJoin.kernelCallsForTest shouldBe 0
    counted.count shouldBe row(joined.count())
    counted.count should be > 2L
    counted.batches should be > 0L
    HashJoin.resetKernelCallsForTest()
    val seen = observe(joined.take(2))
    HashJoin.kernelCallsForTest shouldBe 0
    seen.rows.length shouldBe 2
    seen.rows shouldBe row(show(joined.take(2)))
    seen.batches should be > 0L
  }

  "a null one-sided join" should "follow MixedJoin returning None" in {
    val partner = right.flatMap(keep)
    val joined = partner.fullOuterJoin(nullKey)
    MixedJoin.partitions(joined.plan).isDefined shouldBe false
    HashJoin.resetKernelCallsForTest()
    val counted = observeCount(joined)
    HashJoin.kernelCallsForTest shouldBe 0
    counted.count shouldBe row(joined.count())
    val found = row(show(joined.collect()))
    found.exists(_.contains("null")) shouldBe true
    found.length should be > 2
    show(joined.collect()).sorted shouldBe found.sorted
    val prefix = show(joined.take(2))
    prefix.length shouldBe 2
    prefix shouldBe row(show(joined.take(2)))
    prefix shouldBe show(joined.collect()).take(2)
  }

  /**
   * `"short"` when the lowered child drops a null. `"rejected"` when the decoder throws. The tail
   * still returns the row prefix, including that null.
   */
  private def disagree(
      child: DistCollection[(Int, Int)],
      tailed: DistCollection[(Int, Int)],
  ): String =
    val rowCount = row(tailed.count())
    rowCount shouldBe 3L
    val outcome =
      try
        ColumnarPlanner.tryRun(child.plan) match
          case Some(parts) =>
            val decoded = parts.iterator.map(_.data.size.toLong).sum
            if decoded < rowCount then "short" else "same"
          case None => "refused"
      catch
        case e: IllegalArgumentException if e.getMessage.contains("null Int32 pair") =>
          "rejected"
    val counted = observeCount(tailed)
    counted.count shouldBe rowCount
    val seen = observe(tailed.take(2))
    val expected = row(show(tailed.take(2)))
    seen.rows shouldBe expected
    seen.rows.length shouldBe 2
    seen.rows.exists(_.contains("null")) shouldBe true
    outcome

  private def pairs: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 2)

  private def left: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 2)

  private def right: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2, 1), Array(100, 200, 300), 2)

  /** A present pair first, so the scan lowers, then a null key. */
  private def nullKey: DistCollection[(Int, Int)] =
    DistCollection(Seq((1, 10), erased(None, Some(20)), (2, 30)), 1)

  /** A present pair first, so the scan lowers, then a null value on key 2. */
  private def nullValue: DistCollection[(Int, Int)] =
    DistCollection(Seq((1, 10), erased(Some(2), None), (3, 30)), 1)

  private def pass(values: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    values.mapPartitions((rows: Iterator[(Int, Int)]) => rows)

  private def twice(values: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    values.mapPartitions { (rows: Iterator[(Int, Int)]) =>
      rows.flatMap { case (key, count) => Iterator((key, count), (key, count + 100)) }
    }

  private def keep(row: (Int, Int)): Seq[(Int, Int)] = Seq(row)

  private def decoded(sorted: DistCollection[(Int, Int)], n: Int): Taken =
    val sink = new CountingSink
    val rows = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      sorted.take(n)
    }
    Taken(rows, sink.batchCount, sink.decodedValueCount)

  private def observeCount(values: DistCollection[_]): Counted =
    val sink = new CountingSink
    val count = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      values.count()
    }
    Counted(count, sink.batchCount)

  private def observe[A](body: => Iterable[A]): Observed =
    val sink = new CountingSink
    val rows = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      show(body)
    }
    Observed(rows, sink.batchCount)

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def show(rows: Iterable[Any]): Seq[String] = rows.toSeq.map(showCell)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def showCell(cell: Any): String = cell match
    case null => "null"
    case None => "None"
    case Some(value) => "Some(" + showCell(value) + ")"
    case product: Product =>
      val cells = (0 until product.productArity).map { index =>
        showCell(product.productElement(index))
      }
      cells.mkString("[", ",", "]")
    case other => String.valueOf(other)

  /** Named erasure boundary. `Tuple2` keeps a null `Integer`; `(Int, Int)` would unbox it. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erased(key: Option[Int], value: Option[Int]): (Int, Int) =
    val leftCell: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val rightCell: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(leftCell, rightCell).asInstanceOf[(Int, Int)]

  private final case class Taken(rows: Seq[(Int, Int)], batches: Long, decodedRows: Long)
  private final case class Counted(count: Long, batches: Long)
  private final case class Observed(rows: Seq[String], batches: Long)
