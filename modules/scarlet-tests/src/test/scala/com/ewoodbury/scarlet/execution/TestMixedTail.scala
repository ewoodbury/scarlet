package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.execution.schema.Describe
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `countByKey` under `mapPartitions` keeps the count batch.
 *
 * The tail does not lower. [[Describe.describe]] has no tag for it, and the tag is not the route.
 * A whole-plan row rerun matches the checksum and publishes nothing. The bound child has to
 * publish a batch. `countByKey` of an `Int => Long` `mapValues` lowers the same way, and that tail
 * keeps the count batch. `take` is not this shape.
 */
class TestMixedTail extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    ()

  "countByKey under mapPartitions" should "publish the count batch and match rows" in {
    val counted = pairs.countByKey
    val tailed = bump(counted)
    Describe.describe(tailed.plan) shouldBe Option.empty[Describe.Described]
    ColumnarPlanner.lower(tailed.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.lower(counted.plan).isDefined shouldBe true

    val child = new CountingSink
    val childRan = ColumnMetrics.withSink(child) {
      ScarletRuntime.get.shuffle.clear()
      ColumnarPlanner.tryRun(counted.plan).isDefined
    }
    childRan shouldBe true
    child.batchCount should be > 0L

    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      tailed.collect()
    }
    val expected = row(tailed.collect())
    sink.batchCount shouldBe child.batchCount
    sink.batchCount should be > 0L
    found.toMap shouldBe Map(1 -> 3, 2 -> 2, 3 -> 2)
    found.toMap shouldBe expected.toMap
    CrossEngineShapes.pairChecksum(found) shouldBe CrossEngineShapes.pairChecksum(expected)
  }

  "countByKey of Int => Long under mapPartitions" should "publish the count batch and match rows" in {
    val counted = pairs.mapValues[Int, Int, Long]((value: Int) => value.toLong).countByKey
    val tailed = bump(counted)
    Describe.describe(tailed.plan) shouldBe Option.empty[Describe.Described]
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(IntLongMapValue(Scan)))",
    )
    ColumnarPlanner.lower(tailed.plan) shouldBe Option.empty[PhysicalOp]

    val child = new CountingSink
    val childRan = ColumnMetrics.withSink(child) {
      ScarletRuntime.get.shuffle.clear()
      ColumnarPlanner.tryRun(counted.plan).isDefined
    }
    childRan shouldBe true
    child.batchCount should be > 0L

    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      tailed.collect()
    }
    val expected = row(tailed.collect())
    sink.batchCount shouldBe child.batchCount
    sink.batchCount should be > 0L
    found.toMap shouldBe Map(1 -> 3, 2 -> 2, 3 -> 2)
    found.toMap shouldBe expected.toMap
    CrossEngineShapes.pairChecksum(found) shouldBe CrossEngineShapes.pairChecksum(expected)
  }

  /** One added to each count. The row function has to run; the raw counts are not this map. */
  private def bump(counts: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    counts.mapPartitions { (rows: Iterator[(Int, Int)]) =>
      rows.map { case (key, count) => (key, count + 1) }
    }

  private def pairs: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 2)

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()
