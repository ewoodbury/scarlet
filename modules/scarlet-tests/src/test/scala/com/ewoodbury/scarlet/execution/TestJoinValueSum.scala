package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * The cross-engine join sum reads the columns [[MatchCount]] does not write.
 *
 * Same keys as `filter-join`: `row % 1024` on the build, ten probe copies, two filters. The
 * checksum is `left * 31 + right` over each match.
 */
class TestJoinValueSum extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 64,
        defaultShufflePartitions = 4,
        threadPoolSize = 2,
        spillEnabled = false,
      ),
    )

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ScarletRuntime.get.shuffle.clear()
    ()

  "filter-joinsum" should "sum the written value pair and match the row path" in {
    val joined = crossJoin(4096)
    ColumnarPlanner.lower(joined.plan).map(_.shape) shouldBe Some(
      "HashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(Scan))",
    )

    val summed = JoinValueSum.of(joined)
    summed shouldBe fold(joined.collect())
    ScarletRuntime.get.shuffle.clear()
    summed shouldBe fold(row(joined.collect()))

    val parts = interpreted(joined)
    parts.iterator.map(_.length.toLong).sum shouldBe joined.count()
  }

  it should "write match columns where the count writes none" in {
    val joined = crossJoin(512)
    val counted = new CountingSink
    val written = new CountingSink
    ColumnMetrics.withSink(counted) {
      val _ = joined.count()
    }
    ScarletRuntime.get.shuffle.clear()
    ColumnMetrics.withSink(written) {
      val _ = JoinValueSum.of(joined)
    }

    counted.batchCount shouldBe 0L
    written.batchCount should be > 0L
  }

  it should "skip a null value and keep a present partner" in {
    val left = DistCollection(
      Seq(intPair(boxedInt(1), boxedInt(10)), intPair(boxedInt(1), absentInt)),
      1,
    )
    val right = DistCollection.ofIntPairs(Array(1), Array(7), 1)
    val joined = left.join(right)

    // A null left value is a match. SUM skips it. The row path would unbox that null to 0.
    JoinValueSum.of(joined) shouldBe 10L * 31L + 7L
    joined.count() shouldBe 2L
  }

  it should "sum duplicate build keys" in {
    val left = DistCollection.ofIntPairs(Array(1, 1, 2), Array(10, 11, 20), 2)
    val right = DistCollection.ofIntPairs(Array(1, 1), Array(100, 101), 1)

    JoinValueSum.of(left.join(right)) shouldBe 1704L
    val empty = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 1)
    JoinValueSum.of(left.join(empty)) shouldBe 0L
  }

  /** Same generator as [[CrossEngineBench]]: keys `row % 1024`, ten probe copies, two filters. */
  private def crossJoin(rows: Int): DistCollection[(Int, (Int, Int))] =
    val groups = 1024
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = lcg(rows)
    val left = DistCollection
      .ofIntPairs(keys, values, 2)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val width = 10 * groups
    val right = DistCollection.ofIntPairs(
      Array.tabulate(width)(row => row % groups),
      Array.tabulate(width)(row => row % 100),
      2,
    )
    left.join(right)

  private def interpreted(joined: DistCollection[(Int, (Int, Int))]) =
    val op = ColumnarPlanner.lower(joined.plan).getOrElse(fail("join did not lower"))
    ColumnarInterpreter.interpret(joined.plan, op) match
      case ColumnarInterpreter.Columns.Triples(parts) => parts
      case other => fail(s"join published $other")

  private def fold(rows: Iterable[(Int, (Int, Int))]): Long =
    rows.iterator.foldLeft(0L) { (total, row) =>
      val pair = row._2
      total + pair._1.toLong * 31L + pair._2.toLong
    }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def lcg(count: Int): Array[Int] =
    val generated = new Array[Int](count)
    var index = 0
    var state = 0x12345678
    while (index < count) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      generated(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    generated

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  /** Named erasure boundary. `Tuple2` keeps a null `Integer`; `(Int, Int)` would unbox it. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentInt: java.lang.Integer = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)
