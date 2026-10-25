package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Int and long action folds scan morsels in place. An empty batch is one zero-length morsel. A
 * tail shorter than the batch size is its own morsel. Null slots stay in a map-only count and stay
 * out of a filtered count. An int or long element filter, and an int or long element map, collect
 * those morsels without copying the input batch. A float filter or map still copies slices, so its
 * batch count is the comparison.
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

  "an element filter" should "visit each morsel and publish no slice batch" in {
    val values = Array(1, -2, 3, 4, -5, 6, 7, 8, 9, -10)
    val expected = values.filter(_ > 0).toSeq
    val ints = DistCollection.ofInts(values, 1).filter(_ > 0)
    val longs = DistCollection.ofLongs(values.map(_.toLong), 1).filter(_ > 0L)
    val floats = DistCollection.ofDoubles(values.map(_.toDouble), 1).filter(_ > 0.0)

    arm(batchSize = 4)
    val collected = measure(ints.collect().toSeq)
    val counted = measure(ints.count())
    val longCollected = measure(longs.collect().toSeq)
    val floatCollected = measure(floats.collect().iterator.map(_.toInt).toSeq)

    collected.result shouldBe expected
    counted.result shouldBe expected.length.toLong
    counted.visits shouldBe collected.visits
    counted.batches shouldBe collected.batches
    longCollected.result shouldBe expected.map(_.toLong)
    longCollected.visits shouldBe collected.visits
    longCollected.batches shouldBe collected.batches
    floatCollected.result shouldBe expected
    floatCollected.visits shouldBe collected.visits
    collected.visits shouldBe 3L
    // Each sliced visit publishes one copied input batch. The range scan does not.
    collected.batches shouldBe floatCollected.batches - floatCollected.visits
  }

  it should "visit one zero-length morsel when the partition is empty" in {
    val ints = DistCollection.ofInts(Array.empty[Int], 1).filter(_ > 0)
    val longs = DistCollection.ofLongs(Array.empty[Long], 1).filter(_ > 0L)
    val floats = DistCollection.ofDoubles(Array.empty[Double], 1).filter(_ > 0.0)

    arm(batchSize = 4)
    val collected = measure(ints.collect().toSeq)
    val counted = measure(ints.count())
    val longCollected = measure(longs.collect().toSeq)
    val floatCollected = measure(floats.count())

    collected.result shouldBe Seq.empty[Int]
    counted.result shouldBe 0L
    counted.visits shouldBe 1L
    collected.visits shouldBe 1L
    longCollected.result shouldBe Seq.empty[Long]
    longCollected.visits shouldBe 1L
    floatCollected.visits shouldBe 1L
    collected.batches shouldBe floatCollected.batches
    longCollected.batches shouldBe collected.batches
  }

  it should "keep partition order when morsels split an int or long filter" in {
    val values = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val ints = DistCollection.ofInts(values, 2).filter(_ % 2 == 0)
    val longs = DistCollection.ofLongs(values.map(_.toLong), 2).filter(_ % 2L == 0L)

    arm(batchSize = 4, threads = 4)
    val collected = measure(ints.collect().toSeq)
    val longCollected = measure(longs.collect().toSeq)

    collected.result shouldBe Seq(2, 4, 6, 8, 10)
    longCollected.result shouldBe Seq(2L, 4L, 6L, 8L, 10L)
    collected.visits shouldBe 4L
    longCollected.visits shouldBe 4L
    measure(ints.count()).result shouldBe 5L
    measure(longs.count()).result shouldBe 5L
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

  "an int element map" should "project a range that starts past zero" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.ints(Morsel(sparseBatch, 3, 3), List(projectStep(calls), projectStep(calls)))

    BatchCodec.decodeNullableInts(mapped) shouldBe Seq(Some(32), None, Some(52))
    mapped.length shouldBe 3
    // Two present rows in the range, two maps each. Six present rows sit in the batch.
    calls.get() shouldBe 4
  }

  it should "filter then map a range with nulls before, inside, and after it" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val mapped = ElementPipeline.ints(
      Morsel(sparseBatch, 3, 3),
      List(filterStep(predicates), projectStep(mappers, value => value * 2)),
    )

    BatchCodec.decodeNullableInts(mapped) shouldBe Seq(Some(60), Some(100))
    predicates.get() shouldBe 2
    mappers.get() shouldBe 2
  }

  it should "scan a tail shorter than the source batch" in {
    val calls = new AtomicInteger
    val batch = sparseBatch
    val mapped = ElementPipeline.ints(
      Morsel(batch, 7, 2),
      List(projectStep(calls), projectStep(calls)),
    )

    batch.length should be > 2
    BatchCodec.decodeNullableInts(mapped) shouldBe Seq(None, Some(82))
    mapped.length shouldBe 2
    calls.get() shouldBe 2
  }

  it should "return an empty batch for an empty range and call nothing" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.ints(
      Morsel(sparseBatch, 4, 0),
      List(filterStep(calls), projectStep(calls)),
    )

    mapped.length shouldBe 0
    BatchCodec.decodeNullableInts(mapped) shouldBe Seq.empty[Option[Int]]
    calls.get() shouldBe 0
  }

  it should "match a full-range scan to the whole-batch kernels" in {
    val batch = sparseBatch
    val morsel = Morsel(batch, 0, batch.length)
    val fusedProjects = ElementPipeline.ints(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Project((value: Int) => value + 1),
        ColumnarPlanner.ElementStep.Project((value: Int) => value * 2),
      ),
    )
    val stepwiseProjects = ColumnKernel.projectInt32(
      ColumnKernel.projectInt32(batch, 0, _ + 1),
      0,
      _ * 2,
    )
    sameIntColumn(fusedProjects, stepwiseProjects)

    val fusedFilter = ElementPipeline.ints(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Filter((value: Int) => value > 20),
        ColumnarPlanner.ElementStep.Project((value: Int) => value * 2),
      ),
    )
    val stepwiseFilter = ColumnKernel.projectInt32(
      ColumnKernel.filterInt32(batch, 0, _ > 20),
      0,
      _ * 2,
    )
    sameIntColumn(fusedFilter, stepwiseFilter)
  }

  it should "leave rows outside the range uncalled" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val seen = new AtomicInteger
    val keep: Int => Boolean = value =>
      predicates.incrementAndGet()
      if (value == poison || value < 30 || value > 50) then
        seen.incrementAndGet()
        ()
      value > 0
    val project: Int => Int = value =>
      mappers.incrementAndGet()
      if (value == poison || value < 30 || value > 50) then
        seen.incrementAndGet()
        ()
      value
    val mapped = ElementPipeline.ints(
      Morsel(sparseBatch, 3, 3),
      List(
        ColumnarPlanner.ElementStep.Filter(keep),
        ColumnarPlanner.ElementStep.Project(project),
      ),
    )

    BatchCodec.decodeNullableInts(mapped) shouldBe Seq(Some(30), Some(50))
    predicates.get() shouldBe 2
    mappers.get() shouldBe 2
    seen.get() shouldBe 0
  }

  it should "visit each morsel of a map chain and publish no slice batch" in {
    val values = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val ints = DistCollection.ofInts(values, 1).map(_ + 1).map(_ + 1)
    val floats = DistCollection.ofDoubles(values.map(_.toDouble), 1).map(_ + 1.0).map(_ + 1.0)

    arm(batchSize = 4)
    val collected = measure(ints.collect().toSeq)
    val floatCollected = measure(floats.collect().iterator.map(_.toInt).toSeq)

    collected.result shouldBe values.map(_ + 2).toSeq
    collected.visits shouldBe 3L
    floatCollected.visits shouldBe collected.visits
    // Each sliced visit publishes one copied input batch. The range scan does not.
    collected.batches shouldBe floatCollected.batches - floatCollected.visits
  }

  it should "visit one zero-length morsel when a mapped partition is empty" in {
    val ints = DistCollection.ofInts(Array.empty[Int], 1).map(_ + 1).map(_ + 1)
    val floats = DistCollection.ofDoubles(Array.empty[Double], 1).map(_ + 1.0).map(_ + 1.0)

    arm(batchSize = 4)
    val collected = measure(ints.collect().toSeq)
    val floatCollected = measure(floats.count())

    collected.result shouldBe Seq.empty[Int]
    collected.visits shouldBe 1L
    floatCollected.visits shouldBe 1L
    collected.batches shouldBe floatCollected.batches
  }

  it should "keep partition order when morsels split an int map" in {
    val values = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val ints = DistCollection.ofInts(values, 2).map(_ + 1).filter(_ % 2 == 0)

    arm(batchSize = 4, threads = 4)
    val collected = measure(ints.collect().toSeq)

    collected.result shouldBe Seq(2, 4, 6, 8, 10)
    collected.visits shouldBe 4L
  }

  "a long element map" should "project a range that starts past zero" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.longs(
      Morsel(sparseLongBatch, 3, 3),
      List(projectLongStep(calls), projectLongStep(calls)),
    )

    BatchCodec.decodeNullableLongs(mapped) shouldBe Seq(Some(32L), None, Some(52L))
    mapped.length shouldBe 3
    // Two present rows in the range, two maps each. Six present rows sit in the batch.
    calls.get() shouldBe 4
  }

  it should "filter then map a range with nulls before, inside, and after it" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val mapped = ElementPipeline.longs(
      Morsel(sparseLongBatch, 3, 3),
      List(filterLongStep(predicates), projectLongStep(mappers, value => value * 2L)),
    )

    BatchCodec.decodeNullableLongs(mapped) shouldBe Seq(Some(60L), Some(100L))
    predicates.get() shouldBe 2
    mappers.get() shouldBe 2
  }

  it should "scan a tail shorter than the source batch" in {
    val calls = new AtomicInteger
    val batch = sparseLongBatch
    val mapped = ElementPipeline.longs(
      Morsel(batch, 7, 2),
      List(projectLongStep(calls), projectLongStep(calls)),
    )

    batch.length should be > 2
    BatchCodec.decodeNullableLongs(mapped) shouldBe Seq(None, Some(82L))
    mapped.length shouldBe 2
    calls.get() shouldBe 2
  }

  it should "return an empty batch for an empty range and call nothing" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.longs(
      Morsel(sparseLongBatch, 4, 0),
      List(filterLongStep(calls), projectLongStep(calls)),
    )

    mapped.length shouldBe 0
    BatchCodec.decodeNullableLongs(mapped) shouldBe Seq.empty[Option[Long]]
    calls.get() shouldBe 0
  }

  it should "match a full-range scan to the whole-batch kernels" in {
    val batch = sparseLongBatch
    val morsel = Morsel(batch, 0, batch.length)
    val fusedProjects = ElementPipeline.longs(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Project((value: Long) => value + 1L),
        ColumnarPlanner.ElementStep.Project((value: Long) => value * 2L),
      ),
    )
    val stepwiseProjects = ColumnKernel.projectInt64(
      ColumnKernel.projectInt64(batch, 0, _ + 1L),
      0,
      _ * 2L,
    )
    sameLongColumn(fusedProjects, stepwiseProjects)

    val fusedFilter = ElementPipeline.longs(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Filter((value: Long) => value > 20L),
        ColumnarPlanner.ElementStep.Project((value: Long) => value * 2L),
      ),
    )
    val stepwiseFilter = ColumnKernel.projectInt64(
      ColumnKernel.filterInt64(batch, 0, _ > 20L),
      0,
      _ * 2L,
    )
    sameLongColumn(fusedFilter, stepwiseFilter)
  }

  it should "leave rows outside the range uncalled" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val seen = new AtomicInteger
    val keep: Long => Boolean = value =>
      predicates.incrementAndGet()
      if (value == poison.toLong || value < 30L || value > 50L) then
        seen.incrementAndGet()
        ()
      value > 0L
    val project: Long => Long = value =>
      mappers.incrementAndGet()
      if (value == poison.toLong || value < 30L || value > 50L) then
        seen.incrementAndGet()
        ()
      value
    val mapped = ElementPipeline.longs(
      Morsel(sparseLongBatch, 3, 3),
      List(
        ColumnarPlanner.ElementStep.Filter(keep),
        ColumnarPlanner.ElementStep.Project(project),
      ),
    )

    BatchCodec.decodeNullableLongs(mapped) shouldBe Seq(Some(30L), Some(50L))
    predicates.get() shouldBe 2
    mappers.get() shouldBe 2
    seen.get() shouldBe 0
  }

  it should "visit each morsel of a map chain and publish no slice batch" in {
    val values = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val ints = DistCollection.ofInts(values, 1).map(_ + 1).map(_ + 1)
    val longs = DistCollection.ofLongs(values.map(_.toLong), 1).map(_ + 1L).map(_ + 1L)
    val floats = DistCollection.ofDoubles(values.map(_.toDouble), 1).map(_ + 1.0).map(_ + 1.0)

    arm(batchSize = 4)
    val collected = measure(ints.collect().toSeq)
    val longCollected = measure(longs.collect().toSeq)
    val floatCollected = measure(floats.collect().iterator.map(_.toInt).toSeq)

    collected.result shouldBe values.map(_ + 2).toSeq
    longCollected.result shouldBe values.map(value => (value + 2).toLong).toSeq
    collected.visits shouldBe 3L
    longCollected.visits shouldBe collected.visits
    floatCollected.visits shouldBe collected.visits
    // The int chain fuses to one output column per morsel. The long chain publishes one column
    // per step, so two maps add one batch per visit, and neither publishes a copied input slice.
    longCollected.batches shouldBe collected.batches + collected.visits
    collected.batches shouldBe floatCollected.batches - floatCollected.visits
  }

  it should "visit one zero-length morsel when a mapped partition is empty" in {
    val ints = DistCollection.ofInts(Array.empty[Int], 1).map(_ + 1).map(_ + 1)
    val longs = DistCollection.ofLongs(Array.empty[Long], 1).map(_ + 1L).map(_ + 1L)

    arm(batchSize = 4)
    val collected = measure(ints.collect().toSeq)
    val longCollected = measure(longs.collect().toSeq)

    collected.result shouldBe Seq.empty[Int]
    longCollected.result shouldBe Seq.empty[Long]
    collected.visits shouldBe 1L
    longCollected.visits shouldBe 1L
    // A batch no longer than the batch size was never copied. Two long maps still publish twice.
    longCollected.batches shouldBe collected.batches + longCollected.visits
  }

  it should "keep partition order when morsels split a long map" in {
    val values = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val longs = DistCollection.ofLongs(values.map(_.toLong), 2).map(_ + 1L).filter(_ % 2L == 0L)

    arm(batchSize = 4, threads = 4)
    val collected = measure(longs.collect().toSeq)

    collected.result shouldBe Seq(2L, 4L, 6L, 8L, 10L)
    collected.visits shouldBe 4L
  }

  "a six-step collect" should "keep order after regroup and the next morsel cut" in {
    // Batch size 4. Partition 0 has three morsels and the middle one is entirely rejected.
    // Partition 1 is entirely rejected and is longer than one morsel. Partition 2 keeps rows on
    // both sides of a morsel cut. The filter sits in the first four-step chunk, so the second
    // chunk morsels the survivors.
    val width = 4
    val first = Seq(1, 2, 3, 4, -1, -2, -3, -4, 5, 6)
    val rejected = Seq(-8, -7, -6, -5, -4)
    val third = Seq(7, 8, 9, -5, 10, 11)
    val partitions = Seq(first, rejected, third)
    val expected = partitions.map(rows => rows.filter(_ > 0).map(value => value * 10 + 4))
    val sourceVisits = partitions.map(rows => morselCuts(rows.length, width)).sum
    val survivorVisits = expected.map(rows => morselCuts(rows.length, width)).sum

    first.slice(4, 8).forall(_ < 0) shouldBe true
    rejected.forall(_ < 0) shouldBe true
    rejected.length should be > width
    third.length should be > width

    arm(batchSize = width, threads = 4)
    val visits = sourceVisits + survivorVisits
    visits shouldBe 12
    checkRegroup(sixInts(intSource(partitions)), expected, visits)
    checkRegroup(sixLongs(longSource(partitions)), expected.map(_.map(_.toLong)), visits)
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

  private def projectStep(
      calls: AtomicInteger,
      fn: Int => Int = value => value + 1,
  ): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Project(countMap(calls, fn))

  private def filterStep(calls: AtomicInteger): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Filter(keepInts(calls))

  private def countMap(calls: AtomicInteger, fn: Int => Int): Int => Int =
    value =>
      calls.incrementAndGet()
      if (value == poison) then throw new IllegalStateException("int map saw a null slot")
      fn(value)

  // Row 2 is null before the sample range. Row 4 is null inside it. Rows 6 and 8 are present
  // after it, and row 7 is null after it. Null slots store `poison`.
  private def sparseBatch: ColumnBatch =
    val values = Array(10, 11, poison, 30, poison, 50, 60, poison, 80)
    val present = Array(true, true, false, true, false, true, true, false, true)
    intColumn(values.map(_.toLong), present)

  private def sameIntColumn(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    (left.columns.headOption, right.columns.headOption) match
      case (Some(leftColumn: Int32Column), Some(rightColumn: Int32Column)) =>
        leftColumn.length shouldBe rightColumn.length
        leftColumn.values.toSeq shouldBe rightColumn.values.toSeq
        leftColumn.validity.capacity shouldBe rightColumn.validity.capacity
        leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq
      case _ => fail("expected Int32 columns")

  private def sparseLongBatch: ColumnBatch =
    val values = Array(10L, 11L, poison.toLong, 30L, poison.toLong, 50L, 60L, poison.toLong, 80L)
    val present = Array(true, true, false, true, false, true, true, false, true)
    longColumn(values, present)

  private def sameLongColumn(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    (left.columns.headOption, right.columns.headOption) match
      case (Some(leftColumn: Int64Column), Some(rightColumn: Int64Column)) =>
        leftColumn.length shouldBe rightColumn.length
        leftColumn.values.toSeq shouldBe rightColumn.values.toSeq
        leftColumn.validity.capacity shouldBe rightColumn.validity.capacity
        leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq
      case _ => fail("expected Int64 columns")

  private def projectLongStep(
      calls: AtomicInteger,
      fn: Long => Long = value => value + 1L,
  ): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Project(countLongMap(calls, fn))

  private def filterLongStep(calls: AtomicInteger): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Filter(keepLongs(calls))

  private def countLongMap(calls: AtomicInteger, fn: Long => Long): Long => Long =
    value =>
      calls.incrementAndGet()
      if (value == poison.toLong) then
        throw new IllegalStateException("long map saw a null slot")
      fn(value)

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

  private def checkRegroup[A](
      collection: DistCollection[A],
      expected: Seq[Seq[A]],
      visits: Int,
  ): Unit =
    ColumnarPlanner.lower(collection.plan).map(_.shape) shouldBe Some(
      "Pipeline2(Pipeline4(Scan))",
    )
    val ran = ColumnarPlanner.tryRun(collection.plan).getOrElse(fail("chain was refused"))
    ran.map(_.data.toSeq) shouldBe expected
    ran.lift(1).getOrElse(fail("missing regrouped partition")).data shouldBe empty
    val collected = measure(collection.collect().toSeq)
    collected.result shouldBe expected.flatten
    collected.visits shouldBe visits.toLong
    collected.result shouldBe rowCollect(collection)

  private def rowCollect[A](collection: DistCollection[A]): Seq[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      collection.collect().toSeq
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  "the production cut" should "keep its golden shapes" in {
    ColumnBatches.cuts(0, 4) shouldBe Vector((0, 0))
    ColumnBatches.cuts(4, 4) shouldBe Vector((0, 4))
    ColumnBatches.cuts(5, 2) shouldBe Vector((0, 2), (2, 2), (4, 1))
    ColumnBatches.cuts(10, 3) shouldBe Vector((0, 3), (3, 3), (6, 3), (9, 1))
  }

  private def morselCuts(rows: Int, batchSize: Int): Int =
    ColumnBatches.cuts(rows, batchSize).length

  private def intSource(partitions: Seq[Seq[Int]]): DistCollection[Int] =
    DistCollection(
      Plan.Source(partitions.map(rows => Partition(rows)), Plan.SourceDescriptor.Ints),
    )

  private def longSource(partitions: Seq[Seq[Int]]): DistCollection[Long] =
    DistCollection(
      Plan.Source(
        partitions.map(rows => Partition(rows.map(_.toLong))),
        Plan.SourceDescriptor.Longs,
      ),
    )

  private def sixInts(source: DistCollection[Int]): DistCollection[Int] =
    source.filter(_ > 0).map(_ * 10).map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1)

  private def sixLongs(source: DistCollection[Long]): DistCollection[Long] =
    source.filter(_ > 0L).map(_ * 10L).map(_ + 1L).map(_ + 1L).map(_ + 1L).map(_ + 1L)

  private def arm(batchSize: Int, threads: Int = 1): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        threadPoolSize = threads,
      ),
    )

  private final case class Footprint[A](result: A, batches: Long, visits: Long)

  private def measure[A](body: => A): Footprint[A] =
    ScarletRuntime.get.shuffle.clear()
    ColumnMetrics.withSink(new CountingSink) {
      Footprint(body, ColumnMetrics.batchCount, ColumnMetrics.visitCount)
    }
