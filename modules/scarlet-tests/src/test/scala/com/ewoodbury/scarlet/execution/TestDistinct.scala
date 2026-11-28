package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{BatchCodec, DistinctKernel}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Distinct keeps one copy of each value across partitions.
 *
 * Int, long, double, float, and string columns lower to `Dedupe(Exchange(Scan))`. A bool, a pair,
 * and an int column with columnar execution off hash-shuffle on the row path and then dedupe
 * inside each shuffle partition. A per-partition `iterator.distinct` with no shuffle keeps a
 * duplicate that landed in two source partitions. `-0.0` collapses with `0.0`. Every `NaN` stays,
 * matching `==` on a primitive `Double`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class TestDistinct extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  private def columnar(values: Seq[Int], partitions: Int): Seq[Int] =
    DistCollection(values, partitions).distinct().collect().toSeq.sorted

  "a columnar distinct" should "keep one of each value across partitions" in {
    // Values 1 and 4 sit in different source partitions, so a per-partition dedupe would keep
    // both copies. Global distinct must return each once.
    val values = Seq(1, 2, 2, 3, 1, 4, 4, 4, 0, -1)
    columnar(values, 3) shouldBe Seq(-1, 0, 1, 2, 3, 4)
    ColumnarPlanner.lower(DistCollection(values, 3).distinct().plan).map(_.shape) shouldBe
      Some("Dedupe(Exchange(Scan))")
  }

  it should "dedupe with one partition and with a prime count" in {
    val values = (1 to 100).map(_ % 7) ++ Seq(0, 0, 0)
    columnar(values, 1) shouldBe Seq(0, 1, 2, 3, 4, 5, 6)
    columnar(values, 5) shouldBe Seq(0, 1, 2, 3, 4, 5, 6)
    columnar(values, 7) shouldBe Seq(0, 1, 2, 3, 4, 5, 6)
  }

  it should "return empty for empty input" in {
    columnar(Seq.empty, 3) shouldBe empty
  }

  it should "keep every value when all are distinct" in {
    val values = (0 until 500)
    columnar(values, 4).length shouldBe 500
  }

  it should "refuse take as not partition-preserving" in {
    val lowered = ColumnarPlanner.lower(DistCollection(Seq(1, 2, 2), 2).distinct().plan).get
    lowered.shape shouldBe "Dedupe(Exchange(Scan))"
    PartitionPreserving.of(lowered) shouldBe Option.empty[Int]
  }

  it should "lower a long column and keep one null beside zero" in {
    val absent: java.lang.Long = null
    val rows = Seq[java.lang.Long](
      java.lang.Long.valueOf(1L),
      absent,
      java.lang.Long.valueOf(0L),
      java.lang.Long.valueOf(1L),
      absent,
      java.lang.Long.valueOf(0L),
      java.lang.Long.valueOf(Long.MinValue),
      java.lang.Long.valueOf(Long.MinValue),
    )
    val distinct = DistCollection(rows, 3).distinct()
    ColumnarPlanner.lower(distinct.plan).map(_.shape) shouldBe Some("Dedupe(Exchange(Scan))")
    val columnar = distinct.collect()
    distinct.count() shouldBe 4L
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val row = DistCollection(rows, 3).distinct().collect()
    longProfile(columnar) shouldBe longProfile(row)
    longProfile(columnar) shouldBe (1, Seq(Long.MinValue, 0L, 1L))

    val batch = BatchCodec.nullableLongs(Seq(Some(0L), None, Some(0L), None))
    val decoded = DistinctKernel.longs(Vector(batch), 4).flatMap(BatchCodec.decodeNullableLongs)
    decoded.count(_.isEmpty) shouldBe 1
    decoded.flatten shouldBe Seq(0L)
  }

  it should "lower a double column, collapse negative zero, and keep every NaN" in {
    val payload = java.lang.Double.longBitsToDouble(0x7ff8000000000001L)
    val values = Array(
      1.0,
      -0.0,
      0.0,
      Double.NaN,
      Double.NaN,
      payload,
      1.0,
      Double.PositiveInfinity,
      Double.NegativeInfinity,
    )
    val distinct = DistCollection.ofDoubles(values, 3).distinct()
    ColumnarPlanner.lower(distinct.plan).map(_.shape) shouldBe Some("Dedupe(Exchange(Scan))")
    val columnar = distinct.collect()
    distinct.count() shouldBe 7L
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val row = DistCollection.ofDoubles(values, 3).distinct().collect()
    doubleProfile(columnar) shouldBe doubleProfile(row)
    doubleProfile(columnar) shouldBe (
      3,
      Seq(Double.NegativeInfinity, 0.0, 1.0, Double.PositiveInfinity),
    )

    val absent: java.lang.Double = null
    val boxed = Seq[java.lang.Double](
      java.lang.Double.valueOf(1.0),
      absent,
      java.lang.Double.valueOf(1.0),
      absent,
    )
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val kept = DistCollection(boxed, 2).distinct()
    val keptRows = kept.collect()
    kept.count() shouldBe 2L
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    nullsOf(keptRows) shouldBe nullsOf(DistCollection(boxed, 2).distinct().collect())
    nullsOf(keptRows) shouldBe 1
  }

  /**
   * Fails if each source partition dedupes itself and never shuffles. The bool `true` is in both
   * source partitions. Global distinct still returns two values, and the row graph hash-shuffles.
   */
  it should "drop a duplicate that lands in another partition" in {
    val bools = DistCollection.ofBools(Array(true, false, true, false), 2)
    assert(ColumnarPlanner.lower(bools.distinct().plan).isEmpty)
    bools.distinct().count() shouldBe 2L
    val graph = StageBuilder.buildStageGraph(bools.distinct().plan)
    val shuffle = graph.stages(graph.finalStageId)
    shuffle.isShuffleStage shouldBe true
    shuffle.wideOp shouldBe Some(
      DistinctWideOp(
        SimpleWideOpMeta(
          kind = WideOpKind.Distinct,
          numPartitions = ScarletConf.get.defaultShufflePartitions,
        ),
      ),
    )
    shuffle.outputPartitioning.map(_.distribution) shouldBe Some(
      Distribution.Unknown(ScarletConf.get.defaultShufflePartitions),
    )

    val pairs = DistCollection.ofIntPairs(Array(1, 2, 1, 2), Array(10, 20, 10, 20), 2)
    pairs.distinct().count() shouldBe 2L
    pairs.distinct().collect().toSet shouldBe Set((1, 10), (2, 20))
    val floats = DistCollection(Seq(1.0f, 2.0f, 1.0f, 2.0f), 2)
    floats.distinct().count() shouldBe 2L
    // Each source partition is already unique, so only a shuffle drops the second copy.
    val nullKey = DistCollection(
      Seq[(String, Int)]((null, 1), ("k", 2), (null, 1), ("k", 2)),
      2,
    )
    nullKey.distinct().count() shouldBe 2L
    nullKey.distinct().collect().toSet shouldBe Set((null, 1), ("k", 2))

    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val values = Seq(1, 2, 2, 3, 1, 4, 4, 4, 0, -1)
    val ints = DistCollection(values, 3).distinct()
    ints.count() shouldBe 6L
    ints.collect().toSeq.sorted shouldBe Seq(-1, 0, 1, 2, 3, 4)
    DistCollection(Seq.empty[Int], 3).distinct().collect() shouldBe empty
    DistCollection(Seq(7, 7, 7), 1).distinct().collect().toSeq shouldBe Seq(7)
    DistCollection.ofLongs(Array(1L, 1L, 2L), 2).distinct().collect().toSet shouldBe Set(1L, 2L)
    val words = DistCollection(Seq[String]("a", null, "b", null, "a"), 2).distinct()
    words.count() shouldBe 3L
    words.collect().toSet shouldBe Set[String]("a", "b", null)
  }

  /** One null, then the present longs in order. A `scala.Long` read would unbox the null. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def longProfile(values: Iterable[java.lang.Long]): (Int, Seq[Long]) =
    val cells = values.iterator.toList
    val nulls = cells.count(_ == null)
    val present = cells.collect { case value: java.lang.Long => value.longValue() }.sorted
    (nulls, present)

  /** `NaN` count, then the other doubles with `-0.0` written as `0.0`. */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def doubleProfile(values: Iterable[Double]): (Int, Seq[Double]) =
    val cells = values.iterator.toList
    val nans = cells.count(_.isNaN)
    val present = cells.filter(!_.isNaN).map(value => if (value == 0.0) 0.0 else value).sorted
    (nans, present)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullsOf(values: Iterable[java.lang.Double]): Int =
    values.iterator.count(_ == null)
