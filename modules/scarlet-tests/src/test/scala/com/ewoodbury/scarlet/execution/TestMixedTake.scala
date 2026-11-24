package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, HashJoin}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.execution.schema.Describe
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `take` and `count` split a mixed plan before any row limit.
 *
 * Wrapping [[com.ewoodbury.scarlet.core.Plan.MapPartitionsOp]] around the original plan makes the
 * root opaque, so the columnar child publishes nothing and the rows still match. A fully bound
 * sort stays on [[ColumnarPlanner.tryTake]]. A fully bound count stays on
 * [[ColumnarPlanner.tryCount]]. The tail's cardinality is not the child's. `flatMap` and untyped
 * `sortBy` publish nothing, even when a child would. [[Describe.describe]] has no tag for the
 * tail. The tag is not the route.
 */
class TestMixedTake extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()
    HashJoin.resetKernelCallsForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    HashJoin.resetKernelCallsForTest()
    ()

  "take of a fully bound sort" should "use tryTake and decode the prefix" in {
    val sorted = pairs.sortByIntKey
    val full = row(sorted.collect()).toList
    val n = 2
    full.length should be > n

    val (direct, directBatches, directDecoded) = footprint(ColumnarPlanner.tryTake(sorted.plan, n))
    val (taken, batches, decoded) = footprint(sorted.take(n))
    direct shouldBe Some(taken)
    batches shouldBe directBatches
    decoded shouldBe directDecoded
    batches should be > 0L
    decoded should be > 0L

    val (_, _, fullDecoded) = footprint(sorted.collect())
    decoded should be < fullDecoded
    taken shouldBe full.take(n)
    taken shouldBe row(sorted.take(n))
  }

  "count of a fully bound plan" should "stay on tryCount" in {
    val counted = pairs.countByKey
    val (direct, directBatches, directDecoded) = footprint(ColumnarPlanner.tryCount(counted.plan))
    val (found, batches, decoded) = footprint(counted.count())
    val expected = direct.getOrElse(fail("bound count declined"))
    found shouldBe expected
    batches shouldBe directBatches
    decoded shouldBe directDecoded
    batches should be > 0L
    found shouldBe row(counted.count())
  }

  "take of a mapPartitions tail" should "match the row prefix and publish the child batch" in {
    val child = kept
    val tailed = expand(child)
    Describe.describe(tailed.plan) shouldBe Option.empty[Describe.Described]
    ColumnarPlanner.lower(tailed.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.lower(child.plan).isDefined shouldBe true
    ColumnarPlanner.tryTake(tailed.plan, 2) shouldBe None
    ColumnarPlanner.tryCount(child.plan) shouldBe Some(4L)

    val (_, childBatches, _) = footprint(ColumnarPlanner.tryRun(child.plan))
    childBatches should be > 0L

    val (found, batches, _) = footprint(tailed.take(2))
    val expected = row(tailed.take(2))
    batches shouldBe childBatches
    found shouldBe List(3 -> 30, 3 -> 31)
    found shouldBe expected
    CrossEngineShapes.pairChecksum(found) shouldBe CrossEngineShapes.pairChecksum(expected)
  }

  "count of a mapPartitions tail" should "count the tail, not the child" in {
    val child = kept
    val tailed = expand(child)
    ColumnarPlanner.tryCount(tailed.plan) shouldBe None
    ColumnarPlanner.tryCount(child.plan) shouldBe Some(4L)

    val (found, batches, _) = footprint(tailed.count())
    found shouldBe 6L
    found shouldBe row(tailed.count())
    batches should be > 0L
  }

  "count of a wide prefix" should "count the doubled tail and publish the count batch" in {
    val child = pairs.countByKey
    val tailed = child.mapPartitions { (rows: Iterator[(Int, Int)]) =>
      rows.flatMap { case (key, count) => Iterator((key, count), (key, count + 1)) }
    }
    ColumnarPlanner.lower(child.plan).isDefined shouldBe true
    ColumnarPlanner.lower(tailed.plan) shouldBe Option.empty[PhysicalOp]
    val childCount = ColumnarPlanner.tryCount(child.plan).getOrElse(fail("missing child count"))

    val (found, batches, _) = footprint(tailed.count())
    found shouldBe childCount * 2L
    found shouldBe row(tailed.count())
    batches should be > 0L

    val (taken, takeBatches, _) = footprint(tailed.take(100))
    val expected = row(tailed.take(100))
    taken.toMap shouldBe expected.toMap
    takeBatches should be > 0L
    CrossEngineShapes.pairChecksum(taken) shouldBe CrossEngineShapes.pairChecksum(expected)
  }

  "take of zero" should "publish nothing" in {
    val (_, batches, decoded) = footprint(expand(kept).take(0))
    batches shouldBe 0L
    decoded shouldBe 0L
  }

  "flatMap" should "publish no column batch" in {
    val mapped = kept.flatMap { (row: (Int, Int)) => Seq(row, (row._1, row._2 + 1)) }
    ColumnarPlanner.lower(kept.plan).isDefined shouldBe true
    ColumnarPlanner.lower(mapped.plan) shouldBe Option.empty[PhysicalOp]
    val (taken, takeBatches, takeDecoded) = footprint(mapped.take(2))
    taken shouldBe row(mapped.take(2))
    takeBatches shouldBe 0L
    takeDecoded shouldBe 0L
    val (found, batches, decoded) = footprint(mapped.count())
    found shouldBe row(mapped.count())
    batches shouldBe 0L
    decoded shouldBe 0L
  }

  "untyped sortBy" should "publish no column batch" in {
    val sorted = kept.sortBy((pair: (Int, Int)) => pair._1)
    ColumnarPlanner.lower(sorted.plan) shouldBe Option.empty[PhysicalOp]
    val (taken, takeBatches, takeDecoded) = footprint(sorted.take(2))
    taken shouldBe row(sorted.take(2))
    takeBatches shouldBe 0L
    takeDecoded shouldBe 0L
    val (found, batches, decoded) = footprint(sorted.count())
    found shouldBe row(sorted.count())
    batches shouldBe 0L
    decoded shouldBe 0L
  }

  "a refused child under mapPartitions" should "publish nothing and match rows" in {
    val counted = pairs.mapValues[Int, Int, Long]((value: Int) => value.toLong).countByKey
    val tailed = counted.mapPartitions { (rows: Iterator[(Int, Int)]) =>
      rows.map { case (key, count) => (key, count + 1) }
    }
    ColumnarPlanner.lower(counted.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.lower(tailed.plan) shouldBe Option.empty[PhysicalOp]
    val (taken, takeBatches, _) = footprint(tailed.take(10))
    takeBatches shouldBe 0L
    taken.toMap shouldBe row(tailed.collect()).toMap
    val (found, batches, _) = footprint(tailed.count())
    found shouldBe row(tailed.count())
    batches shouldBe 0L
  }

  "a one-sided join" should "take and count the row join and publish the lowered side" in {
    val joined = pairs.join(others.flatMap(keep))
    ColumnarPlanner.lower(joined.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.lower(pairs.plan).isDefined shouldBe true
    HashJoin.resetKernelCallsForTest()
    val (taken, takeBatches, _) = footprint(joined.take(2))
    HashJoin.kernelCallsForTest shouldBe 0
    takeBatches should be > 0L
    taken shouldBe row(joined.take(2))

    HashJoin.resetKernelCallsForTest()
    val (found, batches, _) = footprint(joined.count())
    HashJoin.kernelCallsForTest shouldBe 0
    batches should be > 0L
    found shouldBe row(joined.count())
  }

  /**
   * Values under 20 are dropped. Each kept row becomes three, so the tail is longer than the
   * child.
   */
  private def expand(child: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    child.mapPartitions { (rows: Iterator[(Int, Int)]) =>
      rows.flatMap { case (key, value) =>
        if (value >= 20) Iterator((key, value), (key, value + 1), (key, value + 2))
        else Iterator.empty[(Int, Int)]
      }
    }

  /**
   * One partition. The first two rows are dropped by [[expand]], so a child prefix of two is
   * empty.
   */
  private def kept: DistCollection[(Int, Int)] =
    DistCollection
      .ofIntPairs(Array(1, 2, 3, 4), Array(1, 2, 30, 40), 1)
      .filterKeys((key: Int) => key > 0)

  private def pairs: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 2)

  private def others: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 3), Array(100, 300), 2)

  private def keep(row: (Int, Int)): Seq[(Int, Int)] = Seq(row)

  private def footprint[A](body: => A): (A, Long, Long) =
    val sink = new CountingSink
    val value = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      body
    }
    (value, sink.batchCount, sink.decodedValueCount)

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()
