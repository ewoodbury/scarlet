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
 * out of a filtered count. Element filters for ints, longs, floats, strings, and bools collect
 * morsels without copying the input batch. A single-step element map publishes one output column
 * per morsel and does not copy the input. A float or string chain of 2–4 steps scans each morsel
 * once. The published batch count is that pass.
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
    val texts = values.map(value => if (value > 0) "k" else "x")
    val strings = DistCollection.ofStrings(texts, 1).filter(_.startsWith("k"))
    val dictionary = DistCollection.ofStrings(texts, 1).filterStringEquals("k")
    val bools = DistCollection.ofBools(values.map(_ > 0), 1).filter(identity)
    val floatMaps = DistCollection.ofDoubles(values.map(_.toDouble), 1).map(value => value)

    arm(batchSize = 4)
    val collected = measure(ints.collect().toSeq)
    val counted = measure(ints.count())
    val longCollected = measure(longs.collect().toSeq)
    val floatCollected = measure(floats.collect().iterator.map(_.toInt).toSeq)
    val stringCollected = measure(strings.collect().toSeq)
    val dictionaryCollected = measure(dictionary.collect().toSeq)
    val boolCollected = measure(bools.collect().toSeq)
    val floatMapped = measure(floatMaps.collect().iterator.map(_.toInt).toSeq)

    collected.result shouldBe expected
    counted.result shouldBe expected.length.toLong
    counted.visits shouldBe collected.visits
    counted.batches shouldBe collected.batches
    longCollected.result shouldBe expected.map(_.toLong)
    longCollected.visits shouldBe collected.visits
    longCollected.batches shouldBe collected.batches
    floatCollected.result shouldBe expected
    floatCollected.visits shouldBe collected.visits
    floatCollected.batches shouldBe collected.batches
    stringCollected.result shouldBe Seq.fill(expected.length)("k")
    stringCollected.visits shouldBe collected.visits
    stringCollected.batches shouldBe collected.batches
    dictionaryCollected.result shouldBe stringCollected.result
    dictionaryCollected.visits shouldBe collected.visits
    dictionaryCollected.batches shouldBe collected.batches
    boolCollected.result shouldBe Seq.fill(expected.length)(true)
    boolCollected.visits shouldBe collected.visits
    boolCollected.batches shouldBe collected.batches
    collected.visits shouldBe 3L
    floatMapped.visits shouldBe collected.visits
    // A single-step map publishes one output column per morsel and no copied input slice.
    floatMapped.batches shouldBe collected.batches
    floatMapped.batches shouldBe floatMapped.visits + 2L
  }

  it should "visit one zero-length morsel when the partition is empty" in {
    val ints = DistCollection.ofInts(Array.empty[Int], 1).filter(_ > 0)
    val longs = DistCollection.ofLongs(Array.empty[Long], 1).filter(_ > 0L)
    val floats = DistCollection.ofDoubles(Array.empty[Double], 1).filter(_ > 0.0)
    val strings = DistCollection.ofStrings(Array.empty[String], 1).filter(_ => true)
    val dictionary = DistCollection.ofStrings(Array.empty[String], 1).filterStringEquals("k")
    val bools = DistCollection.ofBools(Array.empty[Boolean], 1).filter(identity)

    arm(batchSize = 4)
    val collected = measure(ints.collect().toSeq)
    val counted = measure(ints.count())
    val longCollected = measure(longs.collect().toSeq)
    val floatCollected = measure(floats.collect().toSeq)
    val stringCollected = measure(strings.collect().toSeq)
    val dictionaryCollected = measure(dictionary.collect().toSeq)
    val boolCollected = measure(bools.collect().toSeq)

    collected.result shouldBe Seq.empty[Int]
    counted.result shouldBe 0L
    counted.visits shouldBe 1L
    collected.visits shouldBe 1L
    longCollected.result shouldBe Seq.empty[Long]
    longCollected.visits shouldBe 1L
    floatCollected.result shouldBe Seq.empty[Double]
    floatCollected.visits shouldBe 1L
    stringCollected.result shouldBe Seq.empty[String]
    stringCollected.visits shouldBe 1L
    dictionaryCollected.result shouldBe Seq.empty[String]
    dictionaryCollected.visits shouldBe 1L
    boolCollected.result shouldBe Seq.empty[Boolean]
    boolCollected.visits shouldBe 1L
    collected.batches shouldBe floatCollected.batches
    longCollected.batches shouldBe collected.batches
    stringCollected.batches shouldBe collected.batches
    dictionaryCollected.batches shouldBe collected.batches
    boolCollected.batches shouldBe collected.batches
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
    val rows = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val texts = rows.map(row => ('a' + row - 1).toChar.toString)
    val upper = (text: String) => text.toUpperCase(java.util.Locale.ENGLISH)
    arm(batchSize = 4)
    List(2, 4).foreach { steps =>
      val ints = (1 to steps).foldLeft(DistCollection.ofInts(rows, 1)) { (chain, _) =>
        chain.map(_ + 1)
      }
      val floats = (1 to steps).foldLeft(DistCollection.ofDoubles(rows.map(_.toDouble), 1)) {
        (chain, _) => chain.map(_ + 1.0)
      }
      val strings = (1 to steps).foldLeft(DistCollection.ofStrings(texts, 1)) { (chain, _) =>
        chain.map(upper)
      }
      val collected = measure(ints.collect().toSeq)
      val floatCollected = measure(floats.collect().iterator.map(_.toInt).toSeq)
      val textCollected = measure(strings.collect().toSeq)
      collected.result shouldBe rows.map(_ + steps).toSeq
      floatCollected.result shouldBe collected.result
      textCollected.result shouldBe texts.map(upper).toSeq
      collected.visits shouldBe 3L
      floatCollected.visits shouldBe collected.visits
      textCollected.visits shouldBe collected.visits
      // One fused column per morsel. A step-1 split adds one batch per visit.
      floatCollected.batches shouldBe collected.batches
      textCollected.batches shouldBe collected.batches
    }
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
    // An empty batch is not sliced. The two float maps fuse into one column, as the int chain does.
    floatCollected.batches shouldBe collected.batches
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
    // Int and float fuse both maps into one column per morsel. Long still publishes one per map.
    longCollected.batches shouldBe collected.batches + collected.visits
    floatCollected.batches shouldBe collected.batches
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

  "a float element map" should "project a range that starts past zero" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.floats(
      Morsel(sparseFloatBatch, 3, 3),
      List(projectFloatStep(calls), projectFloatStep(calls)),
    )

    BatchCodec.decodeNullableFloat64s(mapped) shouldBe Seq(Some(32.0), None, Some(52.0))
    mapped.length shouldBe 3
    // Two present rows in the range, two maps each. Six present rows sit in the batch.
    calls.get() shouldBe 4
  }

  it should "filter then map a range with nulls before, inside, and after it" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val mapped = ElementPipeline.floats(
      Morsel(sparseFloatBatch, 3, 3),
      List(filterFloatStep(predicates), projectFloatStep(mappers, value => value * 2.0)),
    )

    BatchCodec.decodeNullableFloat64s(mapped) shouldBe Seq(Some(60.0), Some(100.0))
    predicates.get() shouldBe 2
    mappers.get() shouldBe 2
  }

  it should "scan a tail shorter than the source batch" in {
    val calls = new AtomicInteger
    val batch = sparseFloatBatch
    val mapped = ElementPipeline.floats(
      Morsel(batch, 7, 2),
      List(projectFloatStep(calls), projectFloatStep(calls)),
    )

    batch.length should be > 2
    BatchCodec.decodeNullableFloat64s(mapped) shouldBe Seq(None, Some(82.0))
    mapped.length shouldBe 2
    calls.get() shouldBe 2
  }

  it should "return an empty batch for an empty range and call nothing" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.floats(
      Morsel(sparseFloatBatch, 4, 0),
      List(filterFloatStep(calls), projectFloatStep(calls)),
    )

    mapped.length shouldBe 0
    BatchCodec.decodeNullableFloat64s(mapped) shouldBe Seq.empty[Option[Double]]
    calls.get() shouldBe 0
  }

  it should "match a full-range scan to the whole-batch kernels" in {
    val batch = floatBitsBatch
    val morsel = Morsel(batch, 0, batch.length)
    val keepBits: Double => Double = value => value
    val fusedProjects = ElementPipeline.floats(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Project(keepBits),
        ColumnarPlanner.ElementStep.Project(keepBits),
      ),
    )
    val stepwiseProjects = ColumnKernel.projectFloat64(
      ColumnKernel.projectFloat64(batch, 0, value => value),
      0,
      value => value,
    )
    sameFloatColumn(fusedProjects, stepwiseProjects)
    bits(float64(fusedProjects).values(0)) shouldBe bits(negativeZero)
    bits(float64(fusedProjects).values(1)) shouldBe bits(payloadNaN)
    float64(fusedProjects).isValid(2) shouldBe false
    float64(fusedProjects).validity should be theSameInstanceAs float64(batch).validity

    val fusedFilter = ElementPipeline.floats(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Filter((value: Double) => value > 20.0),
        ColumnarPlanner.ElementStep.Project((value: Double) => value * 2.0),
      ),
    )
    val stepwiseFilter = ColumnKernel.projectFloat64(
      ColumnKernel.filterFloat64(batch, 0, _ > 20.0),
      0,
      _ * 2.0,
    )
    sameFloatColumn(fusedFilter, stepwiseFilter)
  }

  it should "leave rows outside the range uncalled" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val seen = new AtomicInteger
    val keep: Double => Boolean = value =>
      predicates.incrementAndGet()
      if (value == poison.toDouble || value < 30.0 || value > 50.0) then
        seen.incrementAndGet()
        ()
      value > 0.0
    val project: Double => Double = value =>
      mappers.incrementAndGet()
      if (value == poison.toDouble || value < 30.0 || value > 50.0) then
        seen.incrementAndGet()
        ()
      value
    val mapped = ElementPipeline.floats(
      Morsel(sparseFloatBatch, 3, 3),
      List(
        ColumnarPlanner.ElementStep.Filter(keep),
        ColumnarPlanner.ElementStep.Project(project),
      ),
    )

    BatchCodec.decodeNullableFloat64s(mapped) shouldBe Seq(Some(30.0), Some(50.0))
    predicates.get() shouldBe 2
    mappers.get() shouldBe 2
    seen.get() shouldBe 0
  }

  "a string element map" should "project a range that starts past zero" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.strings(
      Morsel(sparseStringBatch, 3, 3),
      List(projectStringStep(calls), projectStringStep(calls)),
    )

    BatchCodec.decodeNullableStrings(mapped) shouldBe Seq(Some("IN"), Some(""), None)
    mapped.length shouldBe 3
    // "in", the empty string, and the null, twice each. The empty string outside the window is not
    // one of them.
    calls.get() shouldBe 6
    utf8(mapped).dictionary shouldNot be theSameInstanceAs utf8(sparseStringBatch).dictionary
    utf8(mapped).dictionary.toSeq shouldBe Seq("IN", "")
  }

  it should "filter then map a range with nulls and empty cells before, inside, and after it" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val mapped = ElementPipeline.strings(
      Morsel(sparseStringBatch, 3, 3),
      List(filterStringStep(predicates), projectStringStep(mappers)),
    )

    BatchCodec.decodeNullableStrings(mapped) shouldBe Seq(Some("IN"))
    // The predicate sees the in-window null and the in-window empty string. The mapper does not.
    predicates.get() shouldBe 3
    mappers.get() shouldBe 1
    utf8(mapped).dictionary.toSeq shouldBe Seq("IN")
    utf8(mapped).dictionary shouldNot be theSameInstanceAs utf8(sparseStringBatch).dictionary
  }

  it should "scan a tail shorter than the source batch" in {
    val calls = new AtomicInteger
    val batch = sparseStringBatch
    val mapped = ElementPipeline.strings(
      Morsel(batch, 6, 2),
      List(projectStringStep(calls), projectStringStep(calls)),
    )

    batch.length should be > 2
    BatchCodec.decodeNullableStrings(mapped) shouldBe Seq(Some("CC"), None)
    mapped.length shouldBe 2
    calls.get() shouldBe 4
  }

  it should "return an empty batch for an empty range and call nothing" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.strings(
      Morsel(sparseStringBatch, 4, 0),
      List(filterStringStep(calls), projectStringStep(calls)),
    )

    mapped.length shouldBe 0
    BatchCodec.decodeNullableStrings(mapped) shouldBe Seq.empty[Option[String]]
    calls.get() shouldBe 0
  }

  it should "match a full-range scan to the whole-batch kernels" in {
    val batch = sparseStringBatch
    val morsel = Morsel(batch, 0, batch.length)
    val upper: String => String = value =>
      if (BatchCodec.isAbsent(value)) then BatchCodec.stringElement(None)
      else value.toUpperCase(java.util.Locale.ENGLISH)
    val piped = ElementPipeline.strings(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Project(upper),
        ColumnarPlanner.ElementStep.Project(upper),
      ),
    )
    val stepwise = ColumnKernel.projectUtf8(
      ColumnKernel.projectUtf8(
        batch,
        0,
        value =>
          if (BatchCodec.isAbsent(value)) then BatchCodec.stringElement(None)
          else value.toUpperCase(java.util.Locale.ENGLISH),
      ),
      0,
      value =>
        if (BatchCodec.isAbsent(value)) then BatchCodec.stringElement(None)
        else value.toUpperCase(java.util.Locale.ENGLISH),
    )
    val fused = Utf8Pipeline.PP(
      morsel,
      value =>
        if (BatchCodec.isAbsent(value)) then BatchCodec.stringElement(None)
        else value.toUpperCase(java.util.Locale.ENGLISH),
      value =>
        if (BatchCodec.isAbsent(value)) then BatchCodec.stringElement(None)
        else value.toUpperCase(java.util.Locale.ENGLISH),
    )
    sameStringColumn(piped, stepwise)
    utf8(piped).dictionary.toSeq shouldBe utf8(fused).dictionary.toSeq
    utf8(piped).dictionary shouldNot be theSameInstanceAs utf8(batch).dictionary

    val keep: String => Boolean = value =>
      value match
        case "in" | "" => true
        case other => BatchCodec.isAbsent(other)
    val filtered = ElementPipeline.strings(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Filter(keep),
        ColumnarPlanner.ElementStep.Filter(keep),
      ),
    )
    utf8(filtered).dictionary should be theSameInstanceAs utf8(batch).dictionary
    BatchCodec.decodeNullableStrings(filtered) shouldBe Seq(Some(""), None, Some("in"), Some(""), None, None)
  }

  it should "leave rows outside the range uncalled" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val seen = new AtomicInteger
    val keep: String => Boolean = value =>
      predicates.incrementAndGet()
      value match
        case "out" | "zz" =>
          seen.incrementAndGet()
          false
        case other => !BatchCodec.isAbsent(other) && other.nonEmpty
    val project: String => String = value =>
      mappers.incrementAndGet()
      value match
        case "out" | "zz" | "" =>
          seen.incrementAndGet()
          value
        case other if BatchCodec.isAbsent(other) =>
          seen.incrementAndGet()
          BatchCodec.stringElement(None)
        case text => text.toUpperCase(java.util.Locale.ENGLISH)
    val mapped = ElementPipeline.strings(
      Morsel(sparseStringBatch, 3, 3),
      List(
        ColumnarPlanner.ElementStep.Filter(keep),
        ColumnarPlanner.ElementStep.Project(project),
      ),
    )

    BatchCodec.decodeNullableStrings(mapped) shouldBe Seq(Some("IN"))
    predicates.get() shouldBe 3
    mappers.get() shouldBe 1
    seen.get() shouldBe 0
  }

  it should "keep partition order when morsels split a string map" in {
    val values = Array("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
    val keep = Set("A", "C", "E", "G", "I")
    val texts = DistCollection
      .ofStrings(values, 2)
      .map(_.toUpperCase(java.util.Locale.ENGLISH))
      .filter(keep.contains)

    arm(batchSize = 4, threads = 4)
    val collected = measure(texts.collect().toSeq)

    collected.result shouldBe Seq("A", "C", "E", "G", "I")
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

  private val negativeZero = -0.0
  private val payloadNaN = java.lang.Double.longBitsToDouble(0x7ff0000000000001L)

  private def bits(value: Double): Long = java.lang.Double.doubleToRawLongBits(value)

  private def float64(batch: ColumnBatch): Float64Column = batch.columns.headOption match
    case Some(column: Float64Column) => column
    case _ => fail("expected a Float64 column")

  private def utf8(batch: ColumnBatch): DictUtf8Column = batch.columns.headOption match
    case Some(column: DictUtf8Column) => column
    case _ => fail("expected a Utf8Dict column")

  private def sparseFloatBatch: ColumnBatch =
    val values = Array(10.0, 11.0, poison.toDouble, 30.0, poison.toDouble, 50.0, 60.0, poison.toDouble, 80.0)
    val present = Array(true, true, false, true, false, true, true, false, true)
    floatColumn(values, present)

  private def floatBitsBatch: ColumnBatch =
    val values = Array(negativeZero, payloadNaN, poison.toDouble, 30.0)
    val present = Array(true, true, false, true)
    floatColumn(values, present)

  private def floatColumn(values: Array[Double], present: Array[Boolean]): ColumnBatch =
    val column =
      Float64Column.of(values, Validity.pack(values.length, row => present(row)), values.length)
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  private def sameFloatColumn(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    (left.columns.headOption, right.columns.headOption) match
      case (Some(leftColumn: Float64Column), Some(rightColumn: Float64Column)) =>
        leftColumn.length shouldBe rightColumn.length
        leftColumn.values.indices.foreach { row =>
          bits(leftColumn.values(row)) shouldBe bits(rightColumn.values(row))
        }
        leftColumn.validity.capacity shouldBe rightColumn.validity.capacity
        leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq
      case _ => fail("expected Float64 columns")

  // Row 1 is an empty string outside the window. Row 4 is an empty string inside it. Rows 2 and 5
  // are nulls outside and inside. Row 7 is a null after the window.
  private def sparseStringBatch: ColumnBatch =
    val dictionary = Array("out", "", "in", "cc", "zz")
    val codes = Array(0, 1, 0, 2, 1, 0, 3, 0, 4)
    val present = Array(true, true, false, true, true, false, true, false, true)
    stringColumn(dictionary, codes, present)

  private def stringColumn(
      dictionary: Array[String],
      codes: Array[Int],
      present: Array[Boolean],
  ): ColumnBatch =
    val column = DictUtf8Column.of(
      dictionary,
      codes,
      Validity.pack(codes.length, row => present(row)),
      codes.length,
    )
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  private def sameStringColumn(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    val leftColumn = utf8(left)
    val rightColumn = utf8(right)
    leftColumn.length shouldBe rightColumn.length
    leftColumn.dictionary.toSeq shouldBe rightColumn.dictionary.toSeq
    leftColumn.codes.toSeq shouldBe rightColumn.codes.toSeq
    leftColumn.validity.capacity shouldBe rightColumn.validity.capacity
    leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq

  private def projectFloatStep(
      calls: AtomicInteger,
      fn: Double => Double = value => value + 1.0,
  ): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Project(countFloatMap(calls, fn))

  private def filterFloatStep(calls: AtomicInteger): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Filter(keepFloats(calls))

  private def countFloatMap(calls: AtomicInteger, fn: Double => Double): Double => Double =
    value =>
      calls.incrementAndGet()
      if (value == poison.toDouble) then
        throw new IllegalStateException("float map saw a null slot")
      fn(value)

  private def keepFloats(calls: AtomicInteger): Double => Boolean =
    value =>
      calls.incrementAndGet()
      if (value == poison.toDouble) then
        throw new IllegalStateException("float filter saw a null slot")
      value > 0.0

  private def projectStringStep(calls: AtomicInteger): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Project(countStringMap(calls))

  private def filterStringStep(calls: AtomicInteger): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Filter(keepStrings(calls))

  private def countStringMap(calls: AtomicInteger): String => String =
    value =>
      calls.incrementAndGet()
      value match
        case "out" | "zz" =>
          throw new IllegalStateException("string map saw a row outside the range")
        case other if BatchCodec.isAbsent(other) => BatchCodec.stringElement(None)
        case text => text.toUpperCase(java.util.Locale.ENGLISH)

  private def keepStrings(calls: AtomicInteger): String => Boolean =
    value =>
      calls.incrementAndGet()
      value match
        case "out" | "zz" | "" => false
        case other => !BatchCodec.isAbsent(other)

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
