package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, Int32Column}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * A boxed int-pair scan keeps a later null absent.
 *
 * The first row is a present pair, so classification runs the encode. The null value is on key 2,
 * which the key filter keeps. `collect` of that filtered pair plan is not the oracle:
 * [[com.ewoodbury.scarlet.columnar.BatchCodec.decodeIntPairs]] rejects a clear bit. The bit is
 * read from the interpreted columns. A row-path filter unboxes a null to `0`, so a value filter is
 * not compared to that path.
 */
class TestNullPairScan extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "count" should "include a null value the key filter keeps and publish a column batch" in {
    val kept = source.filterKeys((key: Int) => key > 0)
    val (counted, batches, encodes) = measured(kept.count())

    counted shouldBe 5L
    ColumnarPlanner.tryCount(kept.plan) shouldBe Some(5L)
    batches should be > 0L
    encodes shouldBe 0L
    nullValueKept(kept) shouldBe true
  }

  "filterValues" should "drop a null value without calling the predicate" in {
    val calls = new AtomicInteger
    val filtered = source.filterValues { (value: Int) =>
      calls.incrementAndGet()
      value >= 0
    }

    ColumnarPlanner.tryCount(filtered.plan) shouldBe Some(4L)
    calls.get() shouldBe 5
    calls.set(0)
    filtered.count() shouldBe 4L
    calls.get() shouldBe 5
  }

  "sum" should "skip a null value on the one-sided encode" in {
    val values = source.values
    val (summed, batches, encodes) = measured(values.sum)

    summed shouldBe 25L
    ColumnarPlanner.trySumInt(values.plan) shouldBe Some(25L)
    batches should be > 0L
    encodes shouldBe 0L
    clearBit(values.plan) shouldBe true
  }

  "reduceByKey" should "drop a null value and keep the other value for that key" in {
    val reduced = source.filterKeys((key: Int) => key > 0).reduceByKey[Int, Int] { (left, right) =>
      left + right + 1
    }
    val (rows, batches, _) = measured(reduced.collect().toMap)

    ColumnarPlanner.tryRun(reduced.plan) shouldBe defined
    batches should be > 0L
    rows shouldBe Map(1 -> 10, 2 -> 7, 4 -> -1, 5 -> 0)
  }

  "countByKey" should "count a null value" in {
    val filtered = source.filterKeys((key: Int) => key > 0)
    nullValueKept(filtered) shouldBe true
    val counted = filtered.countByKey
    val (rows, batches, _) = measured(counted.collect().toMap)

    ColumnarPlanner.tryRun(counted.plan) shouldBe defined
    batches should be > 0L
    rows shouldBe Map(1 -> 1, 2 -> 2, 4 -> 1, 5 -> 1)
  }

  "sortByIntKey" should "retain a null value" in {
    val sorted = source.filterKeys((key: Int) => key > 0).sortByIntKey
    val (rows, batches, _) = measured(shown(sorted.collect()))

    ColumnarPlanner.tryRun(sorted.plan) shouldBe defined
    batches should be > 0L
    rows shouldBe Seq("1,10", "2,null", "2,7", "4,-1", "5,0")
  }

  "join" should "keep a null value when neither side is filtered" in {
    val right = DistCollection(Seq((2, 200), (5, 50)), 1)
    val joined = source.join(right)
    val (rows, batches, _) = measured(shownJoin(joined.collect()).sorted)

    ColumnarPlanner.tryRun(joined.plan) shouldBe defined
    batches should be > 0L
    rows shouldBe Seq("2,7,200", "2,null,200", "5,0,50")
  }

  "groupByKey" should "refuse the null value the scan kept" in {
    val thrown = the[IllegalArgumentException] thrownBy {
      source.groupByKey.collect()
    }
    thrown.getMessage should include("clear value bit")
  }

  "an owned array" should "still note a primitive encode" in {
    val owned = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 1)
    val (counted, batches, encodes) = measured(owned.filterKeys((key: Int) => key > 0).count())

    counted shouldBe 2L
    batches should be > 0L
    encodes should be > 0L
  }

  private def source: DistCollection[(Int, Int)] =
    DistCollection(
      Seq(
        (1, 10),
        erased(Some(2), None),
        (2, 7),
        (5, 0),
        (4, -1),
        (0, 9),
      ),
      1,
    )

  private def measured[A](body: => A): (A, Long, Long) =
    ColumnMetrics.withSink(new CountingSink) {
      val value = body
      (value, ColumnMetrics.batchCount, ColumnMetrics.primitiveEncodeCount)
    }

  private def nullValueKept(data: DistCollection[(Int, Int)]): Boolean =
    val op = ColumnarPlanner.lower(data.plan).getOrElse(fail("expected a pair plan"))
    ColumnarInterpreter.pairsOf(ColumnarInterpreter.interpret(data.plan, op)).exists { batch =>
      val keys = int32(batch, 0)
      val values = int32(batch, 1)
      (0 until batch.length).exists { row =>
        keys.isValid(row) && !values.isValid(row) && values.values(row) == 0
      }
    }

  private def clearBit(plan: Plan[_]): Boolean =
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("expected a columnar plan"))
    ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op)).exists { batch =>
      batch.columns.exists(column => (0 until column.length).exists(row => !column.isValid(row)))
    }

  private def int32(
      batch: com.ewoodbury.scarlet.columnar.ColumnBatch,
      ordinal: Int,
  ): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  /** Named erasure boundary. `productElement` reads a null int without unboxing it. */
  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def shown(rows: Iterable[Product]): Seq[String] =
    rows.toSeq.map { row =>
      Seq(row.productElement(0), row.productElement(1))
        .map(cellText)
        .mkString(",")
    }

  /**
   * Named erasure boundary. A join value may be null. The token `null` is a clear validity bit.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def shownJoin(rows: Iterable[Product]): Seq[String] =
    rows.toSeq.map { row =>
      val values = row.productElement(1).asInstanceOf[Product]
      Seq(row.productElement(0), values.productElement(0), values.productElement(1))
        .map(cellText)
        .mkString(",")
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def cellText(cell: Any): String = cell match
    case null => "null"
    case value: Int => String.valueOf(value)
    case _ => "other"

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
