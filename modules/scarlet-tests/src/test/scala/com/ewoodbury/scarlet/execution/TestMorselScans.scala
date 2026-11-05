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
 * out of a filtered count. Element filters for ints, longs, floats, strings, and bools collect
 * morsels without copying the input batch. A single-step element map publishes one output column
 * per morsel and does not copy the input. A float or string chain of 2–4 steps scans each morsel
 * once. The published batch count is that pass.
 */
class TestMorselScans extends AnyFlatSpec with Matchers with BeforeAndAfterEach with MorselFixture:

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

  "an int element map" should "visit each morsel of a map chain and publish no slice batch" in {
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

  "a long element map" should "visit each morsel of a map chain and publish no slice batch" in {
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

  "a string element map" should "keep partition order when morsels split a string map" in {
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

  "the production cut" should "keep its golden shapes" in {
    ColumnBatches.cuts(0, 4) shouldBe Vector((0, 0))
    ColumnBatches.cuts(4, 4) shouldBe Vector((0, 4))
    ColumnBatches.cuts(5, 2) shouldBe Vector((0, 2), (2, 2), (4, 1))
    ColumnBatches.cuts(10, 3) shouldBe Vector((0, 3), (3, 3), (6, 3), (9, 1))
  }
