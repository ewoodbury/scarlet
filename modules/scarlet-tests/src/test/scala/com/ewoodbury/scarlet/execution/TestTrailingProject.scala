package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, NoOpSink}
import com.ewoodbury.scarlet.core.{Partition, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

import MaterialisationStudySupport.{batchSize, elementRows, expectPlus, generate, sourcePartitions}

class TestTrailingProject extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  // Four `+1` maps over the study generator. Five maps is that total plus one per row.
  private val fourMapSum = 236777139L
  private val fiveMapSum = 237777139L
  private val filteredFiveMapSum = 250179896646L
  private val filteredSurvivors = 500068L

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "sum" should "fold five int maps without an output column" in {
    val values = generate(elementRows)
    val source = DistCollection.ofInts(values, sourcePartitions)
    val four = plus(source, 4)
    val five = plus(source, 5)
    val partials = morselCount(elementRows, sourcePartitions, batchSize)
    val encodes = sourcePartitions.toLong

    armStudy()
    shape(four) shouldBe "Pipeline4(Scan)"
    shape(five) shouldBe "Project(Pipeline4(Scan))"

    val fourFoot = measure(four.sum)
    val fiveFoot = measure(five.sum)

    fourFoot.result shouldBe fourMapSum
    fiveFoot.result shouldBe fiveMapSum
    fourFoot.result shouldBe expectPlus(values, 4)
    fiveFoot.result shouldBe expectPlus(values, 5)
    fourFoot.result shouldBe row(four.sum)
    fiveFoot.result shouldBe row(five.sum)

    partials shouldBe 124L
    fourFoot.batches shouldBe encodes + partials
    fiveFoot.batches shouldBe 126L
    fiveFoot.batches shouldBe fourFoot.batches
    fiveFoot.batches shouldNot be(fourFoot.batches + partials)
    fourFoot.visits shouldBe partials
    fiveFoot.visits shouldBe 124L
    fourFoot.encodes shouldBe encodes
    fiveFoot.encodes shouldBe encodes
  }

  it should "fold five maps over an empty source to zero" in {
    val empty = plus(DistCollection.ofInts(Array.empty[Int], sourcePartitions), 5)

    armStudy()
    shape(empty) shouldBe "Project(Pipeline4(Scan))"
    val summed = measure(empty.sum)

    summed.result shouldBe 0L
    summed.result shouldBe row(empty.sum)
    summed.batches shouldBe sourcePartitions.toLong
    summed.visits shouldBe sourcePartitions.toLong
    summed.encodes shouldBe sourcePartitions.toLong
  }

  "count" should "fold five int maps without writing a column" in {
    val values = generate(elementRows)
    val five = plus(DistCollection.ofInts(values, sourcePartitions), 5)
    val partials = morselCount(elementRows, sourcePartitions, batchSize)

    armStudy()
    shape(five) shouldBe "Project(Pipeline4(Scan))"
    val counted = measure(five.count())

    counted.result shouldBe elementRows.toLong
    counted.result shouldBe row(five.count())
    counted.batches shouldBe 126L
    counted.batches shouldBe sourcePartitions.toLong + partials
    counted.batches shouldNot be(sourcePartitions.toLong + partials + partials)
    counted.visits shouldBe 124L
    counted.encodes shouldBe sourcePartitions.toLong
  }

  it should "count one trailing int map without writing that column" in {
    val mapped = DistCollection(Seq(1, 2, 3, 4), 2).map(_ + 1)

    armStudy()
    shape(mapped) shouldBe "Project(Scan)"
    val counted = measure(mapped.count())

    counted.result shouldBe 4L
    counted.result shouldBe row(mapped.count())
    counted.batches shouldBe 2L
    counted.visits shouldBe 2L
    DistCollection.ofInts(Array.empty[Int], 2).map(_ + 1).count() shouldBe 0L
  }

  it should "fold two steps that end in a filter without writing a column" in {
    val chained = DistCollection(Seq(1, 2, 3, 4), 1).map(_ + 1).filter(_ > 2)

    armStudy()
    shape(chained) shouldBe "Pipeline2(Scan)"
    val counted = measure(chained.count())

    counted.result shouldBe 3L
    counted.result shouldBe row(chained.count())
    counted.batches shouldBe 1L
    counted.visits shouldBe 1L
    counted.encodes shouldBe 0L
  }

  "a filter followed by five maps" should "sum without writing a column" in {
    val values = generate(elementRows)
    val chained = plus(DistCollection.ofInts(values, sourcePartitions).filter(_ > 0), 5)
    val partials = morselCount(elementRows, sourcePartitions, batchSize)

    armStudy()
    shape(chained) shouldBe "Pipeline2(Pipeline4(Scan))"
    val summed = measure(chained.sum)
    val counted = measure(chained.count())

    summed.result shouldBe filteredFiveMapSum
    summed.result shouldBe row(chained.sum)
    counted.result shouldBe filteredSurvivors
    counted.result shouldBe row(chained.count())
    summed.batches shouldBe 126L
    summed.batches shouldBe sourcePartitions.toLong + partials
    counted.batches shouldBe 126L
    counted.batches shouldBe summed.batches
    summed.visits shouldBe 124L
    counted.visits shouldBe 124L
    summed.encodes shouldBe sourcePartitions.toLong
    counted.encodes shouldBe sourcePartitions.toLong
  }

  "a filter that rejects every row after five maps" should
    "sum and count zero without an output column" in {
    val values = generate(elementRows)
    val chained = plus(DistCollection.ofInts(values, sourcePartitions), 5).filter(_ => false)
    val partials = morselCount(elementRows, sourcePartitions, batchSize)

    armStudy()
    shape(chained) shouldBe "Pipeline2(Pipeline4(Scan))"
    val summed = measure(chained.sum)
    val counted = measure(chained.count())

    summed.result shouldBe 0L
    counted.result shouldBe 0L
    summed.result shouldBe row(chained.sum)
    counted.result shouldBe row(chained.count())
    summed.batches shouldBe 126L
    summed.batches shouldBe sourcePartitions.toLong + partials
    counted.batches shouldBe 126L
    counted.batches shouldBe summed.batches
    summed.visits shouldBe 124L
    counted.visits shouldBe 124L
    summed.encodes shouldBe sourcePartitions.toLong
    counted.encodes shouldBe sourcePartitions.toLong
  }

  "a filter inside a long chain" should "run in source order and skip maps after a rejection" in {
    val calls = Array.fill(7)(new AtomicInteger)
    val chained = DistCollection(Seq(1, 2, 3), 1)
      .map { value =>
        calls(0).incrementAndGet()
        value + 1
      }
      .filter { value =>
        calls(1).incrementAndGet()
        value > 2
      }
      .map { value =>
        calls(2).incrementAndGet()
        value * 10
      }
      .map { value =>
        calls(3).incrementAndGet()
        value + 1
      }
      .map { value =>
        calls(4).incrementAndGet()
        value + 1
      }
      .map { value =>
        calls(5).incrementAndGet()
        value + 1
      }
      .map { value =>
        calls(6).incrementAndGet()
        value + 1
      }

    armStudy()
    shape(chained) shouldBe "Pipeline3(Pipeline4(Scan))"
    val summed = measure(chained.sum)

    summed.result shouldBe 78L
    summed.batches shouldBe 1L
    summed.visits shouldBe 1L
    calls.map(_.get()).toSeq shouldBe Seq(3, 3, 2, 2, 2, 2, 2)
    summed.result shouldBe row(chained.sum)
  }

  "five int maps" should "run each mapper once per row" in {
    val calls = Array.fill(5)(new AtomicInteger)
    val chained = DistCollection(Seq(1, 2, 3), 1)
      .map { value =>
        calls(0).incrementAndGet()
        value + 1
      }
      .map { value =>
        calls(1).incrementAndGet()
        value * 10
      }
      .map { value =>
        calls(2).incrementAndGet()
        value + 2
      }
      .map { value =>
        calls(3).incrementAndGet()
        value * 10
      }
      .map { value =>
        calls(4).incrementAndGet()
        value + 3
      }

    armStudy()
    shape(chained) shouldBe "Project(Pipeline4(Scan))"
    val summed = measure(chained.sum)

    // 1 -> 2 -> 20 -> 22 -> 220 -> 223. From 2 and 3 the totals are 323 and 423.
    summed.result shouldBe (223L + 323L + 423L)
    summed.batches shouldBe 1L
    summed.visits shouldBe 1L
    calls.map(_.get()).toSeq shouldBe Seq(3, 3, 3, 3, 3)
    summed.result shouldBe row(chained.sum)
  }

  "a type-changing trailing map" should "stay on the row path" in {
    val calls = Array.fill(5)(new AtomicInteger)
    val chained = DistCollection(Seq(1, 2, 3), 1)
      .map { value =>
        calls(0).incrementAndGet()
        value + 1
      }
      .map { value =>
        calls(1).incrementAndGet()
        value + 1
      }
      .map { value =>
        calls(2).incrementAndGet()
        value + 1
      }
      .map { value =>
        calls(3).incrementAndGet()
        value + 1
      }
      .map { value =>
        calls(4).incrementAndGet()
        value.toLong
      }

    armStudy()
    ColumnMetrics.withSink(new CountingSink) {
      ColumnarPlanner.lower(chained.plan) shouldBe Option.empty[PhysicalOp]
      ColumnarPlanner.partitioning(chained.plan) shouldBe Option.empty[PartitioningInfo]
      ColumnarPlanner.tryRun(chained.plan) shouldBe Option.empty[Seq[Partition[Long]]]
      ColumnarPlanner.tryCount(chained.plan) shouldBe Option.empty[Long]
      ColumnarPlanner.trySumInt(chained.plan) shouldBe Option.empty[Long]
      calls.map(_.get()).toSeq shouldBe Seq(0, 0, 0, 0, 0)
      ColumnMetrics.batchCount shouldBe 0L
      ColumnMetrics.visitCount shouldBe 0L
      ColumnMetrics.primitiveEncodeCount shouldBe 0L
    }

    val collected = chained.collect().toList
    collected shouldBe List(5L, 6L, 7L)
    calls.map(_.get()).toSeq shouldBe Seq(3, 3, 3, 3, 3)
    collected shouldBe row(chained.collect().toList)
    ColumnMetrics.withSink(new CountingSink) {
      chained.collect()
      ColumnMetrics.batchCount shouldBe 0L
      ColumnMetrics.visitCount shouldBe 0L
    }
  }

  "sum" should "keep a long total when five increments wrap" in {
    val values = Seq(Int.MaxValue, Int.MaxValue)
    val four = plus(DistCollection(values, 1), 4)
    val five = plus(DistCollection(values, 1), 5)

    armStudy()
    four.sum shouldBe 2L * (Int.MinValue.toLong + 3L)
    five.sum shouldBe 2L * (Int.MinValue.toLong + 4L)
    four.sum shouldBe row(four.sum)
    five.sum shouldBe row(five.sum)
  }

  "a six-step int chain" should "sum without writing a column" in {
    val chained = DistCollection(Seq(1), 1)
      .map(_ + 1)
      .map(_ * 10)
      .map(_ + 2)
      .map(_ * 10)
      .map(_ + 3)
      .map(_ * 10)

    armStudy()
    shape(chained) shouldBe "Pipeline2(Pipeline4(Scan))"
    val summed = measure(chained.sum)

    summed.result shouldBe 2230L
    summed.result shouldBe row(chained.sum)
    summed.batches shouldBe 1L
    summed.visits shouldBe 1L
  }

  "five maps on a pair column" should "sum and count without writing the pipeline" in {
    val pairs = DistCollection.ofIntPairs(Array(1, 2, 3), Array(10, 20, 30), 1)

    def check(base: String, rows: DistCollection[Int]): Unit =
      val four = plus(rows, 4)
      val five = plus(rows, 5)
      shape(four) shouldBe s"Pipeline4($base)"
      shape(five) shouldBe s"Project(Pipeline4($base))"
      val fourFoot = measure(four.sum)
      val fiveFoot = measure(five.sum)
      val counted = measure(five.count())

      fiveFoot.result shouldBe row(five.sum)
      counted.result shouldBe 3L
      counted.result shouldBe row(five.count())
      fiveFoot.batches shouldBe fourFoot.batches
      fiveFoot.visits shouldBe fourFoot.visits
      fiveFoot.encodes shouldBe fourFoot.encodes
      counted.batches shouldBe fourFoot.batches
      counted.visits shouldBe fourFoot.visits

    armStudy()
    check("Value(Scan)", pairs.values)
    check("Key(Scan)", pairs.keys)
    check("Value(MapValue(Scan))", pairs.mapValues[Int, Int, Int](_ + 1).values)
  }

  "collect" should "still write the column after five int maps" in {
    val width = 4
    val rows = DistCollection(1 to 20, 1)
    val five = plus(rows, 5)
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = width, threadPoolSize = 4),
    )
    val summed = measure(five.sum)
    val collected = measure(five.collect().toList)

    summed.batches shouldBe 6L
    summed.visits shouldBe 5L
    collected.result shouldBe (1 to 20).map(_ + 5).toList
    collected.result shouldBe row(five.collect().toList)
    collected.batches should be > summed.batches
  }

  private def plus(source: DistCollection[Int], steps: Int): DistCollection[Int] =
    (0 until steps).foldLeft(source)((current, _) => current.map(_ + 1))

  private def morselCount(rows: Int, parts: Int, size: Int): Long =
    val perPartition = rows / parts
    parts.toLong * ((perPartition + size - 1) / size).toLong

  private def studyConf: ScarletConf =
    originalConf.copy(
      columnarExecution = true,
      columnarBatchSize = batchSize,
      threadPoolSize = 4,
    )

  private def armStudy(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(studyConf)

  private final case class Footprint[A](
      result: A,
      batches: Long,
      visits: Long,
      encodes: Long,
  )

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

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved.copy(columnarExecution = false))
    try body
    finally ScarletConf.set(saved)

  private def shape[A](collection: DistCollection[A]): String =
    ColumnarPlanner
      .lower(collection.plan)
      .map(_.shape)
      .getOrElse(fail("plan stayed on the row path"))
