package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Pins a value filter under the int-to-long widening.
 *
 * An int-pair scan, a value filter that keeps positive values, then `mapValues` to long, lowers to
 * `IntLongMapValue(FilterValue(Scan))`. [[Describe.choice]] is [[KernelChoice.IntLongMapValue]]
 * and [[Layout.known]] is [[Output.IntLongPairs]]. The widening asks [[ColumnBinding.choose]] on
 * the filter child. The collect publishes a batch and matches the row collect. A value filter and
 * a whole-row filter above that chain lower, and so do the same filters above the map with no
 * filter under it. The shape comparison is the whole string.
 */
class TestFoundationChain extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()
    ColumnBinding.resetChooseCallsForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    ()

  "a value filter under the widening" should "choose IntLongMapValue and publish a batch" in {
    val mapped = filteredWidened
    ColumnBinding.resetChooseCallsForTest()
    val root = ColumnarPlanner
      .lower(mapped.plan)
      .getOrElse(fail("ColumnarPlanner.lower returned None"))
    root.shape shouldBe "IntLongMapValue(FilterValue(Scan))"
    root match
      case _: PhysicalOp.IntLongMapValue => succeed
      case other => fail(s"expected IntLongMapValue, got $other")
    describe(mapped.plan).choice shouldBe KernelChoice.IntLongMapValue
    Layout.known(root) match
      case _: Output.IntLongPairs => succeed
      case other => fail(s"expected Output.IntLongPairs, got $other")
    Layout.known(root) should not be Layout.AssumedIntLong
    ColumnBinding.chooseCallsForTest should be > 0
    val rows = columnar {
      ColumnMetrics.withSink(new CountingSink) {
        val collected = mapped.collect().toSeq
        ColumnMetrics.batchCount should be > 0L
        collected
      }
    }
    rows shouldBe row(mapped.collect().toSeq)
  }

  "a filter above the widened map" should "lower above the map and above the filtered map" in {
    val byValue = widened.filterValues((value: Long) => value > 0L)
    val byRow = widened.filter((pair: (Int, Long)) => pair._2 > 0L)
    ColumnarPlanner.lower(byValue.plan).map(_.shape) shouldBe Some(
      "FilterValue(IntLongMapValue(Scan))",
    )
    ColumnarPlanner.lower(byRow.plan).map(_.shape) shouldBe Some(
      "FilterRow(IntLongMapValue(Scan))",
    )
    val pinnedByValue = filteredWidened.filterValues((value: Long) => value > 0L)
    val pinnedByRow = filteredWidened.filter((pair: (Int, Long)) => pair._2 > 0L)
    ColumnarPlanner.lower(pinnedByValue.plan).map(_.shape) shouldBe Some(
      "FilterValue(IntLongMapValue(FilterValue(Scan)))",
    )
    ColumnarPlanner.lower(pinnedByRow.plan).map(_.shape) shouldBe Some(
      "FilterRow(IntLongMapValue(FilterValue(Scan)))",
    )
  }

  /** Present positive ints. Two partitions. Key 1 sits in both partitions. */
  private def intPairs: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 2)

  /** Int value widened to long, with no filter under the map. */
  private def widened: DistCollection[(Int, Long)] =
    intPairs.mapValues[Int, Int, Long]((value: Int) => value.toLong)

  /** Positive value filter, then the int value widened to long. */
  private def filteredWidened: DistCollection[(Int, Long)] =
    intPairs
      .filterValues((value: Int) => value > 0)
      .mapValues[Int, Int, Long]((value: Int) => value.toLong)

  private def describe(plan: Plan[_]): Describe.Described =
    Describe.describe(plan).getOrElse(fail("plan did not lower"))

  /** Columnar execution on for one collect, then the previous conf. */
  private def columnar[A](body: => A): A =
    val previous = ScarletConf.get
    try
      ScarletConf.set(previous.copy(columnarExecution = true))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(previous)
      ScarletRuntime.get.shuffle.clear()

  /** Row collect. Restores the previous conf. Columnar execution does not stay off. */
  private def row[A](body: => A): A =
    val previous = ScarletConf.get
    try
      ScarletConf.set(previous.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(previous)
      ScarletRuntime.get.shuffle.clear()
