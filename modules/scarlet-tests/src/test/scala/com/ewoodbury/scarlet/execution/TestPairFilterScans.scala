package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnBatches, ColumnMetrics, CountingSink, NoOpSink}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Pair and triple filters scan morsels of the shared batch. A partition longer than the batch
 * size is several visits and one published output per visit. Copying each slice would publish
 * one extra batch per visit, and the batch count below rejects that.
 */
class TestPairFilterScans extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val batchSize = 4

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "a pair filter" should "visit each morsel and publish no slice batch" in {
    val rows = 10
    val keys = Array.tabulate(rows)(identity)
    val values = Array.tabulate(rows)(row => row * 10)
    val pairs = DistCollection.ofIntPairs(keys, values, 1).filter { case (key, _) => key % 2 == 0 }
    val cuts = ColumnBatches.cuts(rows, batchSize)

    arm(threads = 1)
    val collected = measure(pairs.collect().toSeq)
    val counted = measure(pairs.count())

    collected.result shouldBe Seq(0 -> 0, 2 -> 20, 4 -> 40, 6 -> 60, 8 -> 80)
    counted.result shouldBe 5L
    collected.visits shouldBe cuts.length.toLong
    counted.visits shouldBe collected.visits
    // Encode, one output per morsel, and one regroup. A copied slice adds one batch per cut.
    collected.batches shouldBe 1L + cuts.length.toLong + 1L
    counted.batches shouldBe collected.batches
  }

  it should "visit one zero-length morsel when the partition is empty" in {
    val pairs = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 1).filter(_ => true)

    arm(threads = 1)
    val collected = measure(pairs.collect().toSeq)

    collected.result shouldBe Seq.empty[(Int, Int)]
    collected.visits shouldBe 1L
    collected.batches shouldBe 2L
  }

  it should "keep partition order when morsels split a pair filter" in {
    val rows = 15
    val keys = Array.tabulate(rows)(identity)
    val values = Array.tabulate(rows)(row => row * 10)
    val pairs = DistCollection.ofIntPairs(keys, values, 2).filter { case (_, value) =>
      value % 20 == 0
    }
    val lengths = Seq(8, 7)
    val visits = lengths.map(length => ColumnBatches.cuts(length, batchSize).length).sum

    arm(threads = 4)
    val collected = measure(pairs.collect().toSeq)

    collected.result shouldBe Seq(
      0 -> 0,
      2 -> 20,
      4 -> 40,
      6 -> 60,
      8 -> 80,
      10 -> 100,
      12 -> 120,
      14 -> 140,
    )
    collected.visits shouldBe visits.toLong
    collected.batches shouldBe 2L + visits.toLong + 2L
  }

  "a triple filter" should "visit each morsel of the join output and publish no slice batch" in {
    val rows = 10
    val keys = Array.tabulate(rows)(identity)
    val build = DistCollection.ofIntPairs(keys, Array.fill(rows)(1), 1)
    val probe = DistCollection.ofIntPairs(keys, Array.fill(rows)(2), 1)
    val joined = build.join(probe)
    val filtered = joined.filterJoinKey(key => key % 2 == 0)
    val cuts = ColumnBatches.cuts(rows, batchSize)
    val expected = (0 until rows by 2).map(key => key -> ((1, 2)))

    arm(threads = 1, partitions = 1)
    val baseline = measure(joined.count())
    val collected = measure(filtered.collect().toSeq)
    val counted = measure(filtered.count())

    collected.result shouldBe expected
    counted.result shouldBe expected.length.toLong
    collected.visits - baseline.visits shouldBe cuts.length.toLong
    counted.visits - baseline.visits shouldBe cuts.length.toLong
    // The join output is one batch. The filter adds one output per morsel and one regroup.
    // A copied slice of that batch would add one more batch per cut.
    val filterBatches = cuts.length.toLong + 1L
    collected.batches - baseline.batches shouldBe filterBatches
    counted.batches - baseline.batches shouldBe filterBatches
  }

  it should "keep partition order when morsels split a triple filter" in {
    val rows = 9
    val keys = Array.tabulate(rows)(_ + 1)
    val build = DistCollection.ofIntPairs(keys, Array.tabulate(rows)(row => row * 10), 1)
    val probe = DistCollection.ofIntPairs(keys, Array.tabulate(rows)(row => row + 3), 2)
    val joined = build.join(probe)
    val filtered = joined.filterJoinLeft(left => left % 20 == 0)
    // Even keys fill a 4-row partition (one morsel). Odd keys fill a 5-row partition (two).
    val filterVisits = ColumnBatches.cuts(4, batchSize).length + ColumnBatches.cuts(5, batchSize).length

    arm(threads = 4, partitions = 2)
    val baseline = measure(joined.count())
    val collected = measure(filtered.collect().toSeq)
    val rowResult = rowCollect(filtered)

    collected.result shouldBe rowResult
    collected.visits - baseline.visits shouldBe filterVisits.toLong
    // One output per morsel, plus a regroup of the 5-row partition. The 4-row partition is one
    // morsel, so it is not concatenated. Slicing the 5-row batch would add two copied batches.
    collected.batches - baseline.batches shouldBe filterVisits.toLong + 1L
  }

  private def arm(threads: Int, partitions: Int = 4): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = partitions,
        threadPoolSize = threads,
      ),
    )

  private final case class Footprint[A](result: A, batches: Long, visits: Long)

  private def measure[A](body: => A): Footprint[A] =
    ScarletRuntime.get.shuffle.clear()
    ColumnMetrics.withSink(new CountingSink) {
      Footprint(body, ColumnMetrics.batchCount, ColumnMetrics.visitCount)
    }

  private def rowCollect[A](collection: DistCollection[A]): Seq[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      collection.collect().toSeq
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
