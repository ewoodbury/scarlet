package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Int and long action folds scan morsels in place. An empty batch is one zero-length morsel. A
 * tail shorter than the batch size is its own morsel. Null slots stay in a map-only count and stay
 * out of a filtered count.
 */
class TestMorselScans extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val poison = 999

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "morsel cuts" should "match slice cuts and share the batch" in {
    val rows = 0 until 10
    val batch = BatchCodec.ints(rows)
    val morsels = Morsels.of(batch, batchSize = 4)
    val slices = ColumnBatches.slice(batch, batchSize = 4)

    morsels.map(_.length) shouldBe slices.map(_.length)
    morsels.map(morsel => (morsel.start, morsel.length)) shouldBe Seq((0, 4), (4, 4), (8, 2))
    morsels.foreach(_.batch should be theSameInstanceAs batch)
  }

  it should "keep one zero-length morsel for an empty batch" in {
    val batch = BatchCodec.ints(Seq.empty[Int])
    val morsels = Morsels.of(batch, batchSize = 8)

    morsels shouldBe Vector(Morsel(batch, 0, 0))
    ColumnBatches.slice(batch, batchSize = 8) should have size 1
  }

  it should "keep one morsel when the batch is shorter than the batch size" in {
    val batch = BatchCodec.ints(Seq(4, 5, 6))
    val morsels = Morsels.of(batch, batchSize = 32)

    morsels shouldBe Vector(Morsel(batch, 0, 3))
  }

  "an empty partition" should "visit once and return an empty sum and count" in {
    val empty = DistCollection.ofInts(Array.empty[Int], 1).map(_ + 1)

    arm(batchSize = 4)
    ColumnMetrics.withSink(new CountingSink) {
      empty.sum shouldBe 0L
      ColumnMetrics.visitCount shouldBe 1L
      ColumnMetrics.batchCount shouldBe 1L
    }
    ColumnMetrics.withSink(new CountingSink) {
      empty.count() shouldBe 0L
      ColumnMetrics.visitCount shouldBe 1L
    }
  }

  "a tail morsel" should "be shorter than the batch size and still be summed" in {
    val values = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val ints = DistCollection.ofInts(values, 1).map(_ + 1)
    val longs = DistCollection.ofLongs(values.map(_.toLong), 1).map(_ + 1L)
    val expected = values.foldLeft(0L)((sum, value) => sum + value.toLong + 1L)

    arm(batchSize = 4)
    val summed = measure(ints.sum)
    val longSum = measure(longs.sum)
    summed.result shouldBe expected
    longSum.result shouldBe expected
    summed.visits shouldBe 3L
    longSum.visits shouldBe 3L
    summed.batches shouldBe 1L
    longSum.batches shouldBe 1L
    measure(ints.count()).result shouldBe values.length.toLong
  }

  "a batch shorter than one morsel" should "be one visit" in {
    val values = Array(4, 5, 6)
    val ints = DistCollection.ofInts(values, 1).map(_ + 1)
    val longs = DistCollection.ofLongs(values.map(_.toLong), 1).map(_ + 1L)

    arm(batchSize = 32)
    val summed = measure(ints.sum)
    summed.result shouldBe 18L
    summed.visits shouldBe 1L
    summed.batches shouldBe 1L
    measure(longs.sum).result shouldBe 18L
    measure(longs.count()).result shouldBe 3L
  }

  "null slots" should "stay in a map-only count and out of a filtered count" in {
    val morsel = ranged(intColumn)
    val presentSum = 300L

    val mappedCalls = new AtomicInteger
    val projectInt = projectInts(mappedCalls)
    ElementSum.countMapped(morsel, projectInt) shouldBe 4L
    mappedCalls.get() shouldBe 3
    mappedCalls.set(0)
    ElementSum.sumMapped(morsel, projectInt) shouldBe presentSum
    mappedCalls.get() shouldBe 3

    val filteredCalls = new AtomicInteger
    val keep = ColumnarPlanner.ElementStep.Filter(keepInts(filteredCalls))
    ElementSum.countLonger(morsel, List(keep)) shouldBe 3L
    filteredCalls.get() shouldBe 3
    filteredCalls.set(0)
    ElementSum.sumLonger(morsel, List(keep)) shouldBe presentSum
    filteredCalls.get() shouldBe 3

    val projectedCalls = new AtomicInteger
    val hotCalls = new AtomicInteger
    ElementSum.sumInts(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Filter(keepInts(hotCalls)),
        ColumnarPlanner.ElementStep.Project(projectInts(projectedCalls)),
      ),
    ) shouldBe presentSum
    hotCalls.get() shouldBe 3
    projectedCalls.get() shouldBe 3

    val longMorsel = ranged(longColumn)
    val longCalls = new AtomicInteger
    val project = ColumnarPlanner.ElementStep.Project(projectLongs(longCalls))
    ElementSum.countLongs(longMorsel, List(project)) shouldBe 4L
    longCalls.get() shouldBe 3
    longCalls.set(0)
    ElementSum.sumLongs(longMorsel, List(project)) shouldBe presentSum
    longCalls.get() shouldBe 3

    val predicateCalls = new AtomicInteger
    val predicate = ColumnarPlanner.ElementStep.Filter(keepLongs(predicateCalls))
    ElementSum.countLongs(longMorsel, List(predicate)) shouldBe 3L
    predicateCalls.get() shouldBe 3
    predicateCalls.set(0)
    ElementSum.sumLongs(longMorsel, List(predicate)) shouldBe presentSum
    predicateCalls.get() shouldBe 3
  }

  // Rows [0, 4) are present, so a prefix check of that width looks dense. The morsel starts at 6.
  // Row 4 is a null before the morsel and row 7 is a null inside it. Both store `poison`.
  private def ranged(build: (Array[Long], Array[Boolean]) => ColumnBatch): Morsel =
    val values = Array[Long](1, 1, 1, 1, poison.toLong, 50, 100, poison.toLong, 100, 100)
    val present = Array(true, true, true, true, false, true, true, false, true, true)
    val batch = build(values, present)
    Morsel(batch, start = 6, length = 4)

  private def intColumn(values: Array[Long], present: Array[Boolean]): ColumnBatch =
    val stored = values.map(_.toInt)
    val column =
      Int32Column.of(stored, Validity.pack(stored.length, row => present(row)), stored.length)
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  private def longColumn(values: Array[Long], present: Array[Boolean]): ColumnBatch =
    val column =
      Int64Column.of(values, Validity.pack(values.length, row => present(row)), values.length)
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  private def projectInts(calls: AtomicInteger): Int => Int =
    value =>
      calls.incrementAndGet()
      if (value == poison) then throw new IllegalStateException("int kernel saw a null slot")
      value

  private def keepInts(calls: AtomicInteger): Int => Boolean =
    value =>
      calls.incrementAndGet()
      if (value == poison) then throw new IllegalStateException("int kernel saw a null slot")
      value > 0

  private def projectLongs(calls: AtomicInteger): Long => Long =
    value =>
      calls.incrementAndGet()
      if (value == poison.toLong) then
        throw new IllegalStateException("long kernel saw a null slot")
      value

  private def keepLongs(calls: AtomicInteger): Long => Boolean =
    value =>
      calls.incrementAndGet()
      if (value == poison.toLong) then
        throw new IllegalStateException("long kernel saw a null slot")
      value > 0L

  private def arm(batchSize: Int): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        threadPoolSize = 1,
      ),
    )

  private final case class Footprint(result: Long, batches: Long, visits: Long)

  private def measure(body: => Long): Footprint =
    ScarletRuntime.get.shuffle.clear()
    ColumnMetrics.withSink(new CountingSink) {
      Footprint(body, ColumnMetrics.batchCount, ColumnMetrics.visitCount)
    }
