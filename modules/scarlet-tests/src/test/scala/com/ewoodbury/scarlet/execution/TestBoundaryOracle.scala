package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.{Describe, KernelChoice}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * A numeric array child is kept without a second run. A boxed null still falls back.
 *
 * The fake this fails re-runs `ofIntPairs` under `mapValues` to string: the oracle counter moves.
 * A silent row collect of the element map publishes no batch. Skipping the counter on a boxed null
 * filter would make [[RowBoundary.mapValues]] return a decode, and the null test still requires
 * [[scala.None]].
 */
class TestBoundaryOracle extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()
    RowBoundary.resetChildOracleRunsForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    ()

  "an int-pair scan under mapValues to string" should "skip the row child" in {
    val scanned = DistCollection.ofIntPairs(Array(1, 2, 3, 4), Array(10, 20, 30, 40), 2)
    val mapped = scanned.mapValues[Int, Int, String]((value: Int) => value.toString)
    ColumnarPlanner.lower(mapped.plan).isDefined shouldBe false
    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      mapped.collect().toSeq
    }
    RowBoundary.childOracleRunsForTest shouldBe 0L
    sink.batchCount should be > 0L
    matchesRow(mapped, found)
  }

  "an int scan under flatMap" should "skip the row child" in {
    val mapped = DistCollection.ofInts(Array(1, 2, 3, 4), 2).flatMap((n: Int) => Seq(n, n + 10))
    ColumnarPlanner.lower(mapped.plan).isDefined shouldBe false
    kept(mapped, 1L)
  }

  "an int-pair scan under flatMapValues" should "skip the row child" in {
    val mapped = DistCollection
      .ofIntPairs(Array(1, 2, 3, 4), Array(10, 20, 30, 40), 2)
      .flatMapValues((value: Int) => Seq(value, value + 1))
    ColumnarPlanner.lower(mapped.plan).isDefined shouldBe false
    kept(mapped, 1L)
  }

  "an int scan under sortBy" should "skip the row child" in {
    val sorted = DistCollection.ofInts(Array(3, 1, 4, 2), 2).sortBy((n: Int) => n)
    ColumnarPlanner.lower(sorted.plan).isDefined shouldBe false
    kept(sorted, 1L) shouldBe Seq(1, 2, 3, 4)
  }

  "two int-pair scans under cogroup" should "skip both row children" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 1)
    val right = DistCollection.ofIntPairs(Array(2, 3), Array(30, 40), 1)
    val grouped = left.cogroup(right)
    ColumnarPlanner.lower(grouped.plan).isDefined shouldBe false
    kept(grouped, 2L)
  }

  "an int scan under map to long" should "skip the row child and stay unlowered" in {
    val mapped = DistCollection.ofInts(Array(1, 2, 3, 4), 2).map((value: Int) => value.toLong)
    ColumnarPlanner.lower(mapped.plan).isDefined shouldBe false
    kept(mapped, 1L)
  }

  "an int mapValues to long" should "stay on its kernel and off the boundary" in {
    val mapped = DistCollection
      .ofIntPairs(Array(1, 2), Array(10, 20), 1)
      .mapValues[Int, Int, Long]((value: Int) => value.toLong)
    ColumnarPlanner.lower(mapped.plan).isDefined shouldBe true
    Describe.describe(mapped.plan).map(_.choice) shouldBe Some(KernelChoice.IntLongMapValue)
    val found = mapped.collect().toSeq
    RowBoundary.childOracleRunsForTest shouldBe 0L
    matchesRow(mapped, found)
  }

  "a null key under mapValues to string" should "still compare the child and fall back" in {
    val scanned = DistCollection(Seq((1, 10), erased(None, Some(20)), (2, 30)), 1)
    val mapped =
      scanned
        .filterKeys((key: Int) => key >= 0)
        .mapValues[Int, Int, String](value => value.toString)
    mapped.plan match
      case Plan.MapValuesOp(source, f) =>
        RowBoundary.mapValues(source, f) shouldBe None
      case other => fail(s"expected mapValues, got $other")
    RowBoundary.childOracleRunsForTest should be > 0L
    val found = collectShown(mapped)
    found shouldBe row(collectShown(mapped))
    found.exists(_.contains("null")) shouldBe true
  }

  private def kept[A](data: DistCollection[A], batches: Long): Seq[A] =
    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      data.collect().toSeq
    }
    RowBoundary.childOracleRunsForTest shouldBe 0L
    sink.batchCount should be >= batches
    matchesRow(data, found)

  /** Columnar rows match the row plan, and that row plan publishes no batch. */
  private def matchesRow[A](data: DistCollection[A], found: Seq[A]): Seq[A] =
    val rowSink = new CountingSink
    val again = ColumnMetrics.withSink(rowSink)(row(data.collect().toSeq))
    rowSink.batchCount shouldBe 0L
    found shouldBe again
    found

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Product"))
  private def collectShown(data: DistCollection[(Int, String)]): Seq[String] =
    data.collect().toSeq.map { pair =>
      val cells = (0 until pair.productArity).map { index =>
        pair.productElement(index) match
          case null => "null"
          case other => String.valueOf(other)
      }
      cells.mkString("[", ",", "]")
    }

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()

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
