package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Count, sum, and take go through [[ColumnarQuery]]. A hand-built int filter tree counts the
 * published batches. A refused plan still counts on rows and does not enter that object.
 */
class TestColumnarQuery extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val rows = Array.tabulate(64)(index => index - 8)

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true, spillEnabled = false))
    ColumnarQuery.resetForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "columnar actions" should "count and sum a filtered int column through the entry" in {
    val filtered = ints.filter(value => value >= 0)
    val expectedCount = rows.count(value => value >= 0).toLong
    val expectedSum = rows.iterator.filter(value => value >= 0).map(_.toLong).sum

    filtered.count() shouldBe expectedCount
    ColumnarQuery.runsForTest should be > 0

    ColumnarQuery.resetForTest()
    filtered.sum shouldBe expectedSum
    ColumnarQuery.runsForTest should be > 0
  }

  it should "take the top 100 of an int value sort through the entry" in {
    val pairs = (0 until 200).map(index => (index, (index * 13) % 50))
    val sorted = DistCollection(pairs, 4).sortByIntValueDesc
    val taken = sorted.take(100)

    ColumnarQuery.runsForTest should be > 0
    taken should have length 100
    ScarletConf.set(originalConf.copy(columnarExecution = false, spillEnabled = false))
    sorted.take(100) shouldBe taken
  }

  it should "count a hand-built int filter and publish a batch" in {
    val filtered = ints.filter(value => value >= 0)
    filtered.plan match
      case Plan.FilterOp(source: Plan.Source[_], _) =>
        val hand = PhysicalOp.Filter(
          PhysicalOp.Scan(PhysicalOp.Kind.Ints, source.partitions),
          PhysicalOp.Slot.Element,
        )
        ColumnarPlanner.lower(filtered.plan) shouldBe Some(hand)
        val sink = new CountingSink
        val previous = ColumnMetrics.install(sink)
        val counted =
          try ColumnarQuery.count(filtered.plan, hand)
          finally ColumnMetrics.restore(previous)
        counted shouldBe Some(rows.count(value => value >= 0).toLong)
        sink.batchCount should be > 0L
      case other =>
        fail(s"expected one filter over a source, got $other")
  }

  it should "count a refused map on rows without entering" in {
    val mapped = ints.map(value => value.toString)
    mapped.count() shouldBe rows.length.toLong
    ColumnarQuery.runsForTest shouldBe 0
  }

  private def ints: DistCollection[Int] =
    DistCollection.ofInts(rows, 2)
