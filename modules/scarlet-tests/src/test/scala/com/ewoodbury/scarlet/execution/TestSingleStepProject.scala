package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, NoOpSink}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * A single-step element map publishes one output column per morsel. It does not copy the input
 * batch. Encode and the regrouped partition are the other batches in a collect. A copied input
 * slice would add one batch per visit.
 */
class TestSingleStepProject extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "a single-step element map" should "publish one column per morsel and no input slice" in {
    val values = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val texts = values.map(_.toString)
    val ints = DistCollection.ofInts(values, 1).map(_ + 1)
    val longs = DistCollection.ofLongs(values.map(_.toLong), 1).map(_ + 1L)
    val floats = DistCollection.ofDoubles(values.map(_.toDouble), 1).map(_ + 1.0)
    val strings = DistCollection.ofStrings(texts, 1).map(text => text + "x")

    arm(batchSize = 4)
    val mapped = measure(ints.collect().toSeq)
    val longMapped = measure(longs.collect().toSeq)
    val floatMapped = measure(floats.collect().iterator.map(_.toInt).toSeq)
    val stringMapped = measure(strings.collect().toSeq)

    mapped.result shouldBe values.map(_ + 1).toSeq
    longMapped.result shouldBe values.map(value => (value + 1).toLong).toSeq
    floatMapped.result shouldBe mapped.result
    stringMapped.result shouldBe texts.map(text => text + "x").toSeq
    mapped.visits shouldBe 3L
    longMapped.visits shouldBe mapped.visits
    floatMapped.visits shouldBe mapped.visits
    stringMapped.visits shouldBe mapped.visits
    // Encode, one output column per morsel, and one regroup. A slice adds one batch per visit.
    val published = mapped.visits + 2L
    mapped.batches shouldBe published
    longMapped.batches shouldBe published
    floatMapped.batches shouldBe published
    stringMapped.batches shouldBe published
  }

  it should "visit one empty morsel and call nothing" in {
    val calls = new AtomicInteger
    val ints = DistCollection.ofInts(Array.empty[Int], 1).map { value =>
      calls.incrementAndGet()
      value + 1
    }
    val longs = DistCollection.ofLongs(Array.empty[Long], 1).map { value =>
      calls.incrementAndGet()
      value + 1L
    }
    val floats = DistCollection.ofDoubles(Array.empty[Double], 1).map { value =>
      calls.incrementAndGet()
      value + 1.0
    }
    val strings = DistCollection.ofStrings(Array.empty[String], 1).map { value =>
      calls.incrementAndGet()
      value
    }

    arm(batchSize = 4)
    val collected = measure(ints.collect().toSeq)
    val longCollected = measure(longs.collect().toSeq)
    val floatCollected = measure(floats.collect().toSeq)
    val stringCollected = measure(strings.collect().toSeq)

    collected.result shouldBe Seq.empty[Int]
    longCollected.result shouldBe Seq.empty[Long]
    floatCollected.result shouldBe Seq.empty[Double]
    stringCollected.result shouldBe Seq.empty[String]
    collected.visits shouldBe 1L
    longCollected.visits shouldBe 1L
    floatCollected.visits shouldBe 1L
    stringCollected.visits shouldBe 1L
    calls.get() shouldBe 0
  }

  it should "keep partition order when morsels split a single map" in {
    val values = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val ints = DistCollection.ofInts(values, 2).map(_ + 1)
    val longs = DistCollection.ofLongs(values.map(_.toLong), 2).map(_ + 1L)

    arm(batchSize = 4, threads = 4)
    val collected = measure(ints.collect().toSeq)
    val longCollected = measure(longs.collect().toSeq)

    collected.result shouldBe values.map(_ + 1).toSeq
    longCollected.result shouldBe values.map(value => (value + 1).toLong).toSeq
    collected.visits shouldBe 4L
    longCollected.visits shouldBe collected.visits
  }

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
