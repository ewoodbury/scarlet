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
 * Pins the finished foundation contract.
 *
 * [[Describe.choice]] and the [[PhysicalOp]] class name the kernel. A collect that matches the row
 * path is not enough: a kind match can build [[PhysicalOp.IntLongMapValue]] without
 * [[ColumnBinding.choose]], and a widened plan can publish no batch. [[Describe.tag]] is not the
 * check. The row collect is a real run with columnar execution off.
 */
class TestFoundationLock extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

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

  "an int-pair mapValues to long" should "choose IntLongMapValue and publish a batch" in {
    val mapped = widened
    ColumnBinding.resetChooseCallsForTest()
    val found = describe(mapped.plan)
    found.choice shouldBe KernelChoice.IntLongMapValue
    found.root.shape shouldBe "IntLongMapValue(Scan)"
    found.root match
      case _: PhysicalOp.IntLongMapValue => succeed
      case other => fail(s"expected IntLongMapValue, got $other")
    Layout.known(found.root) match
      case _: Output.IntLongPairs => succeed
      case other => fail(s"expected Output.IntLongPairs, got $other")
    Layout.known(found.root) should not be Layout.AssumedIntLong
    Layout.known(found.root) should not be Layout.ProbeSum
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

  "reduceByKey of that widened map" should "choose IntLongProjectSum and publish a batch" in {
    val reduced =
      widened.reduceByKey[Int, Long]((left: Long, right: Long) => left + right)
    val found = describe(reduced.plan)
    found.choice shouldBe KernelChoice.IntLongProjectSum
    found.root.shape shouldBe "HashAggregate(Exchange(IntLongMapValue(Scan)))"
    found.root match
      case PhysicalOp.HashAggregate(
            PhysicalOp.Exchange(_: PhysicalOp.IntLongMapValue),
            PhysicalOp.HashAggregateTag.IntLongSum,
          ) =>
        succeed
      case other => fail(s"expected an int-long sum aggregate, got $other")
    val rows = columnar {
      ColumnMetrics.withSink(new CountingSink) {
        val collected = reduced.collect().toSeq
        ColumnMetrics.batchCount should be > 0L
        collected
      }
    }
    rows shouldBe row(reduced.collect().toSeq)
  }

  "an int-pair mapValues that stays int" should "choose MapValue and ask the binder" in {
    val mapped = intPairs.mapValues((value: Int) => value + 1)
    ColumnBinding.resetChooseCallsForTest()
    val found = describe(mapped.plan)
    found.choice shouldBe KernelChoice.MapValue
    found.root match
      case _: PhysicalOp.MapValue => succeed
      case other => fail(s"expected MapValue, got $other")
    ColumnBinding.chooseCallsForTest should be > 0
  }

  "a long-pair mapValues" should "choose MapValue and not build IntLongMapValue" in {
    val mapped = DistCollection
      .ofLongPairs(Array(1L, 2L, 3L, 4L), Array(10L, 20L, 30L, 40L), 2)
      .mapValues((value: Long) => value + 1L)
    val found = describe(mapped.plan)
    found.choice shouldBe KernelChoice.MapValue
    found.root match
      case _: PhysicalOp.IntLongMapValue => fail("long mapValues lowered as IntLongMapValue")
      case _: PhysicalOp.MapValue => succeed
      case other => fail(s"expected MapValue, got $other")
  }

  "an int-pair mapValues to string" should "keep the row boundary and publish a batch" in {
    val mapped = intPairs.mapValues((value: Int) => value.toString)
    ColumnarPlanner.lower(mapped.plan) shouldBe None
    mapped.plan match
      case Plan.MapValuesOp(source, f) =>
        RowBoundary.mapValues(source, f).isDefined shouldBe true
      case other => fail(s"expected mapValues, got $other")
    val rows = columnar {
      ColumnMetrics.withSink(new CountingSink) {
        val collected = mapped.collect().toSeq
        ColumnMetrics.batchCount should be > 0L
        collected
      }
    }
    rows shouldBe row(mapped.collect().toSeq)
  }

  "an element map from int to long" should "stay empty and not ask the binder" in {
    val mapped = DistCollection.ofInts(Array(1, 2, 3, 4), 2).map((value: Int) => value.toLong)
    ColumnBinding.resetChooseCallsForTest()
    ColumnarPlanner.lower(mapped.plan) shouldBe None
    ColumnBinding.chooseCallsForTest shouldBe 0
  }

  "a filter above the widened map" should "stay empty" in {
    val byValue = widened.filterValues((value: Long) => value > 0L)
    val byRow = widened.filter((pair: (Int, Long)) => pair._2 > 0L)
    ColumnarPlanner.lower(byValue.plan) shouldBe None
    ColumnarPlanner.lower(byRow.plan) shouldBe None
  }

  "countByKey of the widened map" should "stay empty" in {
    ColumnarPlanner.lower(widened.countByKey.plan) shouldBe None
  }

  "reduceByKey that keeps the left long" should "stay empty" in {
    val kept = widened.reduceByKey[Int, Long]((left: Long, _: Long) => left)
    ColumnarPlanner.lower(kept.plan) shouldBe None
  }

  "an int scan under flatMap" should "keep the row boundary and publish a batch" in {
    val scanned = DistCollection.ofInts(Array(1, 2, 3, 4), 2)
    val mapped = scanned.flatMap((n: Int) => Seq(n, n + 10))
    mapped.plan match
      case Plan.FlatMapOp(source, f) =>
        RowBoundary.flatMap(source, f).isDefined shouldBe true
      case other => fail(s"expected flatMap, got $other")
    val rows = columnar {
      ColumnMetrics.withSink(new CountingSink) {
        val collected = mapped.collect().toSeq
        ColumnMetrics.batchCount should be > 0L
        collected
      }
    }
    rows shouldBe row(mapped.collect().toSeq)
  }

  "IntLongMapBinding on a mapped triple project" should "ask and return none" in {
    val triples = PhysicalOp.Scan(PhysicalOp.Kind.Triples, Seq.empty)
    val child = PhysicalOp.MapKey(PhysicalOp.Project(triples))
    child.kind shouldBe PhysicalOp.Kind.Pairs
    Layout.known(child) shouldBe Layout.AssumedIntLong
    ColumnBinding.resetChooseCallsForTest()
    IntLongMapBinding.value(child) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
  }

  /** Present positive ints. Two partitions. Key 1 sits in both partitions. */
  private def intPairs: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 2)

  private def widened: DistCollection[(Int, Long)] =
    intPairs.mapValues[Int, Int, Long]((value: Int) => value.toLong)

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
