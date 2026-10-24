package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, NoOpSink}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

import MaterialisationStudySupport.{batchSize, elementRows, sourcePartitions}

/**
 * Long columnar plans against the row path. Five steps lower to `Project(Pipeline4(Scan))` for
 * five maps, and to `Filter(Pipeline4(Scan))` when the fifth step is a filter. Six steps lower to
 * `Pipeline2(Pipeline4(Scan))`. Sum and count walk that chain and do not write it.
 */
class TestLongColumnar extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "long plans" should "match the row path for a million rows, a short array, and an empty array" in {
    check(million, 4)
    check(Array(4L, -2L, 0L, 9L, Long.MinValue, 7L), 3)
    check(Array.empty[Long], 2)
  }

  it should "match the row path for chain lengths 1 to 5" in {
    val values = Array(1L, -4L, 8L, 0L, 3L, 11L)
    val source = DistCollection.ofLongs(values, 3)

    val filter = source.filter(_ > 0L)
    val mapped = source.map(_ + 2L)
    shape(filter) shouldBe "Filter(Scan)"
    shape(mapped) shouldBe "Project(Scan)"
    agree(filter.collect().toVector)
    agree(mapped.sum)
    agree(mapped.count())
    agree(mapped.take(2))

    val two = source.filter(_ % 2L == 0L).map(_ * 3L)
    val three = two.filter(_ > 0L)
    val four = three.map(_ + 1L)
    shape(two) shouldBe "Pipeline2(Scan)"
    shape(three) shouldBe "Pipeline3(Scan)"
    shape(four) shouldBe "Pipeline4(Scan)"
    agree(two.collect().toVector)
    agree(three.sum)
    agree(four.count())
    agree(four.take(3))

    // Five maps are one project over a four-step pipeline. The action still folds every step.
    val five = plus(source, 5)
    shape(five) shouldBe "Project(Pipeline4(Scan))"
    agree(five.collect().toVector)
    agree(five.sum)
    agree(five.count())
    agree(five.take(1))

    val fiveFilter = plus(source, 4).filter(_ > 0L)
    shape(fiveFilter) shouldBe "Filter(Pipeline4(Scan))"
    agree(fiveFilter.sum)
    agree(fiveFilter.count())

    val six = plus(source, 5).filter(_ % 2L == 0L)
    shape(six) shouldBe "Pipeline2(Pipeline4(Scan))"
    agree(six.collect().toVector)
    agree(six.sum)
  }

  it should "keep a single element and an empty partition" in {
    val single = DistCollection.ofLongs(Array(Long.MinValue), 1)
    shape(single) shouldBe "Scan"
    agree(single.collect().toVector) shouldBe Vector(Long.MinValue)
    agree(single.sum) shouldBe Long.MinValue
    agree(single.count()) shouldBe 1L
    agree(single.take(1)) shouldBe List(Long.MinValue)

    val sparse = DistCollection.ofLongs(Array(4L), 3).map(_ + 1L)
    agree(sparse.collect().toVector) shouldBe Vector(5L)
    agree(sparse.sum) shouldBe 5L
    agree(sparse.count()) shouldBe 1L
    agree(sparse.take(2)) shouldBe List(5L)
  }

  it should "classify an empty source from its descriptor" in {
    val empty = DistCollection.ofLongs(Array.empty[Long], 2)
    ColumnarPlanner.lower(empty.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Longs)
    agree(empty.sum) shouldBe 0L
    agree(empty.count()) shouldBe 0L
    agree(empty.take(4)) shouldBe Nil
    agree(empty.collect().toVector) shouldBe Vector.empty[Long]
  }

  it should "wrap Long.MaxValue + 1 and sum an empty collection to 0" in {
    val wrapped = DistCollection.ofLongs(Array(Long.MaxValue, 1L), 2)
    agree(wrapped.sum) shouldBe Long.MinValue
    val bothEnds = DistCollection.ofLongs(Array(Long.MaxValue, Long.MaxValue), 1)
    agree(bothEnds.sum) shouldBe -2L
    val empty = DistCollection.ofLongs(Array.empty[Long], 1)
    agree(empty.sum) shouldBe 0L
  }

  it should "ignore later writes to the caller's array" in {
    val raw = Array(1L, 2L, 3L, 4L)
    val collection = DistCollection.ofLongs(raw, 2)
    raw(0) = 99L
    raw(3) = -5L
    collection.collect() shouldBe Seq(1L, 2L, 3L, 4L)
    collection.sum shouldBe 10L
  }

  it should "classify a boxed java.lang.Long as longs" in {
    val first = java.lang.Long.valueOf(8L)
    val second = java.lang.Long.valueOf(-1L)
    first.getClass shouldBe classOf[java.lang.Long]
    val boxed = DistCollection(Seq(first, second), 1)
    ColumnarPlanner.lower(boxed.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Longs)
    agree(boxed.collect().toVector) shouldBe Vector(first, second)
    agree(boxed.count()) shouldBe 2L

    val longs = DistCollection(Seq(8L, -1L), 1)
    ColumnarPlanner.lower(longs.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Longs)
    agree(longs.filter(_ > 0L).map(_ + 1L).sum) shouldBe 9L
  }

  it should "refuse a Long to Int map and run it once per element" in {
    val hits = new AtomicInteger
    val mapped = DistCollection.ofLongs(Array(1L, 2L, 4L), 2).map { value =>
      hits.incrementAndGet()
      value.toInt
    }

    ColumnarPlanner.partitioning(mapped.plan) shouldBe None
    ColumnarPlanner.tryRun(mapped.plan) shouldBe None
    ColumnarPlanner.tryCount(mapped.plan) shouldBe None
    ColumnarPlanner.trySumLong(mapped.plan) shouldBe None
    ColumnarPlanner.tryTake(mapped.plan, 1) shouldBe None
    hits.get() shouldBe 0

    mapped.collect() shouldBe Seq(1, 2, 4)
    hits.get() shouldBe 3
    hits.set(0)
    mapped.sum shouldBe 7
    hits.get() shouldBe 3
  }

  "long sum and count" should "fold four and five maps without an output column" in {
    val generated = MaterialisationStudySupport.generate(elementRows)
    val values = generated.map(value => value.toLong)
    val source = DistCollection.ofLongs(values, sourcePartitions)
    val four = plus(source, 4)
    val five = plus(source, 5)
    val partials = morselCount(elementRows, sourcePartitions, batchSize)
    val encodes = sourcePartitions.toLong

    armStudy()
    shape(four) shouldBe "Pipeline4(Scan)"
    shape(five) shouldBe "Project(Pipeline4(Scan))"

    val fourSum = measure(four.sum)
    val fiveSum = measure(five.sum)
    val fiveCount = measure(five.count())

    fourSum.result shouldBe expectPlus(generated, 4)
    fiveSum.result shouldBe expectPlus(generated, 5)
    fourSum.result shouldBe row(four.sum)
    fiveSum.result shouldBe row(five.sum)
    fiveCount.result shouldBe elementRows.toLong
    fiveCount.result shouldBe row(five.count())

    partials shouldBe 124L
    fourSum.batches shouldBe encodes + partials
    fiveSum.batches shouldBe 126L
    fiveSum.batches shouldBe fourSum.batches
    fiveCount.batches shouldBe 126L
    fourSum.visits shouldBe partials
    fiveSum.visits shouldBe 124L
    fiveCount.visits shouldBe 124L
    fourSum.encodes shouldBe encodes
    fiveSum.encodes shouldBe encodes
    fiveCount.encodes shouldBe encodes
  }

  private def check(values: Array[Long], parts: Int): Unit =
    val source = DistCollection.ofLongs(values, parts)
    val filtered = source.filter(value => value > 0L)
    val mapped = source.map(value => value + 1L)
    val both = source.filter(value => value % 2L == 0L).map(value => value * 3L)
    agree(filtered.collect().toVector)
    agree(mapped.collect().toVector)
    agree(both.collect().toVector)
    agree(filtered.count())
    agree(mapped.count())
    agree(both.count())
    agree(filtered.sum)
    agree(mapped.sum)
    agree(both.sum)
    agree(filtered.take(3))
    agree(mapped.take(1))
    agree(both.take(5))
    ()

  private def plus(source: DistCollection[Long], steps: Int): DistCollection[Long] =
    (0 until steps).foldLeft(source)((current, _) => current.map(_ + 1L))

  private def shape(collection: DistCollection[?]): String =
    ColumnarPlanner.lower(collection.plan).map(_.shape).getOrElse(fail("long plan was refused"))

  private def million: Array[Long] =
    Array.tabulate(1000000)(index => index.toLong - 500000L)

  private def expectPlus(values: Array[Int], steps: Int): Long =
    val extra = steps.toLong
    values.foldLeft(0L)((sum, value) => sum + value.toLong + extra)

  private def morselCount(rows: Int, parts: Int, size: Int): Long =
    val perPartition = rows / parts
    parts.toLong * ((perPartition + size - 1) / size).toLong

  private def armStudy(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        threadPoolSize = 4,
      ),
    )

  private final case class Footprint[A](result: A, batches: Long, visits: Long, encodes: Long)

  private def measure[A](body: => A): Footprint[A] =
    ScarletRuntime.get.shuffle.clear()
    ColumnMetrics.withSink(new CountingSink) {
      val result = body
      Footprint(
        result,
        ColumnMetrics.batchCount,
        ColumnMetrics.visitCount,
        ColumnMetrics.primitiveEncodeCount,
      )
    }

  private def agree[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = true))
      val columnar = body
      ScarletRuntime.get.shuffle.clear()
      ScarletConf.set(saved.copy(columnarExecution = false))
      val rows = body
      columnar shouldBe rows
      columnar
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletRuntime.get.shuffle.clear()
      ScarletConf.set(saved.copy(columnarExecution = false))
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
