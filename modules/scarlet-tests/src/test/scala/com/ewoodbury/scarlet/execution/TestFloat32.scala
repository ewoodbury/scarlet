package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Float32 scan, one filter, one `Float => Float` map, sum, and distinct.
 *
 * Two steps, and a map to another type, stay on the row path. A null is kept by distinct of a
 * boxed `java.lang.Float` and is not passed to a filter or a map above that distinct. Sum skips
 * that null. `-0.0f` shares `0.0f`. Every `NaN` stays.
 */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class TestFloat32 extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "a float32 scan" should "classify an owned array and agree with the row count" in {
    val values = Array(1.0f, 2.0f, 3.0f)
    val owned = DistCollection.ofFloats(values, 2)
    owned.plan match
      case Plan.Source(_, descriptor) =>
        descriptor shouldBe Plan.SourceDescriptor.Float32s
      case other => fail(s"expected a source, got $other")
    ColumnarPlanner.lower(owned.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Float32s)
    owned.count() shouldBe 3L
    owned.count() shouldBe row(owned.count())
    owned.collect().toVector shouldBe Vector(1.0f, 2.0f, 3.0f)
  }

  it should "classify an unknown descriptor from the first float" in {
    val plain = DistCollection(Seq(1.0f, 2.0f), 2)
    plain.plan match
      case Plan.Source(_, descriptor) =>
        descriptor shouldBe Plan.SourceDescriptor.Unknown
      case other => fail(s"expected a source, got $other")
    ColumnarPlanner.lower(plain.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Float32s)
    plain.collect().toVector shouldBe Vector(1.0f, 2.0f)
  }

  it should "leave a leading null on the row path" in {
    val absent: java.lang.Float = null
    val leading = DistCollection(
      Seq[java.lang.Float](absent, java.lang.Float.valueOf(1.0f)),
      1,
    )
    ColumnarPlanner.lower(leading.plan) shouldBe None
  }

  "one float32 filter" should "keep the same values as the row path" in {
    val source = DistCollection.ofFloats(Array(1.0f, 2.0f, 3.0f, 4.0f), 2)
    val filtered = source.filter(value => value > 2.0f)
    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some("Filter(Scan)")
    filtered.collect().toVector shouldBe Vector(3.0f, 4.0f)
    filtered.collect().toVector shouldBe row(filtered.collect().toVector)
  }

  "one float32 map" should "keep the same values as the row path" in {
    val source = DistCollection.ofFloats(Array(1.0f, 2.0f, 3.0f, 4.0f), 2)
    val mapped = source.map(value => value + 1.0f)
    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some("Project(Scan)")
    mapped.collect().toVector shouldBe Vector(2.0f, 3.0f, 4.0f, 5.0f)
    mapped.collect().toVector shouldBe row(mapped.collect().toVector)
  }

  it should "run the mapper once, on count, and not while partitioning" in {
    val calls = new AtomicInteger
    val source = DistCollection.ofFloats(Array(1.0f, 2.0f, 3.0f, 4.0f), 2)
    val mapped = source.map { (value: Float) =>
      val _ = calls.incrementAndGet()
      value + 1.0f
    }
    ColumnarPlanner.partitioning(mapped.plan).isDefined shouldBe true
    calls.get() shouldBe 0
    mapped.count() shouldBe 4L
    calls.get() shouldBe 4
  }

  "a float32 map to another type" should "stay on the row path" in {
    val source = DistCollection.ofFloats(Array(1.0f, 2.0f), 2)
    ColumnarPlanner.tryRun(source.map(value => value.toInt).plan).isDefined shouldBe false
    ColumnarPlanner.tryRun(source.map(value => value.toLong).plan).isDefined shouldBe false
    ColumnarPlanner.tryRun(source.map(value => value.toDouble).plan).isDefined shouldBe false
    ColumnarPlanner.tryRun(source.map(value => value.toString).plan).isDefined shouldBe false
  }

  "two float32 steps" should "stay on the row path" in {
    val source = DistCollection.ofFloats(Array(1.0f, 2.0f, 3.0f), 2)
    val twice = source.filter(value => value > 0.0f).filter(value => value < 3.0f)
    val stacked = source.filter(value => value > 1.0f).map(value => value + 1.0f)
    ColumnarPlanner.tryRun(twice.plan).isDefined shouldBe false
    ColumnarPlanner.tryRun(stacked.plan).isDefined shouldBe false
  }

  "float32 sum" should "add present values and return zero for an empty source" in {
    val source = DistCollection.ofFloats(Array(1.0f, 2.0f, 3.0f, 4.0f), 2)
    source.sum shouldBe 10.0f
    source.sum shouldBe row(source.sum)
    ColumnarPlanner.trySumFloat(source.plan) shouldBe Some(10.0f)
    val empty = DistCollection.ofFloats(Array.emptyFloatArray, 2)
    empty.sum shouldBe 0.0f
    ColumnarPlanner.trySumFloat(empty.plan) shouldBe Some(0.0f)
  }

  "float32 distinct" should "collapse negative zero and keep every NaN" in {
    val payload = java.lang.Float.intBitsToFloat(0x7fc00001)
    val values = Array(
      1.0f,
      -0.0f,
      0.0f,
      Float.NaN,
      Float.NaN,
      payload,
      1.0f,
      Float.PositiveInfinity,
      Float.NegativeInfinity,
    )
    val distinct = DistCollection.ofFloats(values, 3).distinct()
    ColumnarPlanner.lower(distinct.plan).map(_.shape) shouldBe Some("Dedupe(Exchange(Scan))")
    val columnar = distinct.collect()
    distinct.count() shouldBe 7L
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val onRows = DistCollection.ofFloats(values, 3).distinct().collect()
    floatProfile(columnar) shouldBe floatProfile(onRows)
    floatProfile(columnar) shouldBe (
      3,
      Seq(Float.NegativeInfinity, 0.0f, 1.0f, Float.PositiveInfinity),
    )
  }

  it should "keep one null from a boxed scan" in {
    val absent: java.lang.Float = null
    val boxed = Seq[java.lang.Float](
      java.lang.Float.valueOf(1.0f),
      absent,
      java.lang.Float.valueOf(1.0f),
      absent,
    )
    val kept = DistCollection(boxed, 2).distinct()
    ColumnarPlanner.lower(kept.plan).map(_.shape) shouldBe Some("Dedupe(Exchange(Scan))")
    val keptRows = kept.collect()
    kept.count() shouldBe 2L
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    nullsOf(keptRows) shouldBe nullsOf(DistCollection(boxed, 2).distinct().collect())
    nullsOf(keptRows) shouldBe 1
  }

  it should "skip a null in a filter and a map above that distinct" in {
    val absent: java.lang.Float = null
    val boxed = Seq[java.lang.Float](
      java.lang.Float.valueOf(1.0f),
      absent,
      java.lang.Float.valueOf(4.0f),
      absent,
    )
    val distinct = asFloats(DistCollection(boxed, 2).distinct())
    val filterCalls = new AtomicInteger
    val filtered = distinct.filter { (value: Float) =>
      val _ = filterCalls.incrementAndGet()
      value > 2.0f
    }
    filtered.count() shouldBe 1L
    filterCalls.get() shouldBe 2

    val mapCalls = new AtomicInteger
    val mapped = distinct.map { (value: Float) =>
      val _ = mapCalls.incrementAndGet()
      value + 1.0f
    }
    mapped.sum shouldBe 7.0f
    mapCalls.get() shouldBe 2
    ColumnarPlanner.trySumFloat(mapped.plan) shouldBe Some(7.0f)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rowSum = asFloats(DistCollection(boxed, 2).distinct()).map(value => value + 1.0f).sum
    rowSum shouldBe 8.0f
  }

  /**
   * `DistCollection[java.lang.Float]` does not declare `apply$mcFF$sp`. The plan already holds
   * `Float` values, including one null. The cast selects the specialized lambda on the next step.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def asFloats(values: DistCollection[java.lang.Float]): DistCollection[Float] =
    values.asInstanceOf[DistCollection[Float]]

  /** `NaN` count, then the other floats with `-0.0f` written as `0.0f`. */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def floatProfile(values: Iterable[Float]): (Int, Seq[Float]) =
    val cells = values.iterator.toList
    val nans = cells.count(_.isNaN)
    val present = cells.filter(!_.isNaN).map(value => if (value == 0.0f) 0.0f else value).sorted
    (nans, present)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullsOf(values: Iterable[java.lang.Float]): Int =
    values.iterator.count(_ == null)

  private def row[A](read: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      read
    finally ScarletConf.set(saved)
