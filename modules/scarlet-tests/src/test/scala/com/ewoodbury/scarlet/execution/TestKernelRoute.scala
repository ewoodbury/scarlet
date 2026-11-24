package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{BatchCodec, ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.{Partition, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.ReducerWitness
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, Describe, KernelChoice, Layout}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * The interpreter routes on [[KernelChoice]], not on an `[Int32, Int64]` layout.
 *
 * [[KernelChoice.IntLongProjectSum]] is the only tag that decodes through the int-long sum.
 * [[KernelChoice.JoinProbeSum]] and a key map of a triple project have that layout and keep the
 * decoder their tag already used. A projected join sum still publishes a column batch.
 */
class TestKernelRoute extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    ()

  "KernelRoute" should "give every tag one arm, and the project sum the int-long arm" in {
    val arms = KernelChoice.values.map(kernel => kernel -> KernelRoute.arm(kernel))
    arms.length shouldBe KernelChoice.values.length
    val sums = arms.collect { case (kernel, KernelRoute.Arm.IntLongReduce) => kernel }
    sums shouldBe Array(KernelChoice.IntLongProjectSum)
    arms.foreach { case (kernel, arm) =>
      kernel match
        case KernelChoice.IntLongProjectSum =>
          arm shouldBe KernelRoute.Arm.IntLongReduce
        case _ =>
          arm shouldBe KernelRoute.Arm.Existing
    }
  }

  it should "not run the int-long sum for another tag on an int-long layout" in {
    val triples = PhysicalOp.Scan(PhysicalOp.Kind.Triples, Seq.empty)
    val pairs = PhysicalOp.Scan(PhysicalOp.Kind.Pairs, Seq.empty)
    val project = PhysicalOp.Project(triples)
    val mapped = PhysicalOp.MapKey(project)
    val probe = PhysicalOp.JoinProbeSum(pairs, pairs, PhysicalOp.ProbeMeasure.Left)
    val aggregate = PhysicalOp.HashAggregate(PhysicalOp.Exchange(project))
    Layout.known(project) shouldBe Layout.AssumedIntLong
    Layout.known(mapped) shouldBe Layout.AssumedIntLong
    Layout.known(probe) shouldBe Layout.AssumedIntLong
    project.schema shouldBe PhysicalSchema.IntLong
    probe.schema shouldBe PhysicalSchema.IntLong
    aggregate.schema shouldBe PhysicalSchema.IntLong
    PhysicalSchema.isIntLongAggregate(project) shouldBe false
    PhysicalSchema.isIntLongAggregate(mapped) shouldBe false
    PhysicalSchema.isIntLongAggregate(probe) shouldBe false
    PhysicalSchema.isIntLongAggregate(aggregate) shouldBe true
    Describe.tag(project) shouldBe KernelChoice.Project
    Describe.tag(mapped) shouldBe KernelChoice.MapKey
    Describe.tag(probe) shouldBe KernelChoice.JoinProbeSum
    Describe.tag(aggregate) shouldBe KernelChoice.IntLongProjectSum

    val ints = ColumnarInterpreter.Columns.Ints(Vector(BatchCodec.ints(Seq(1, 2))))
    rowCount(project, ints) shouldBe 2
    rowCount(mapped, ints) shouldBe 2
    rowCount(probe, ints) shouldBe 2
    val caught = intercept[IllegalStateException] {
      val _ = ColumnarInterpreter.decodeFor(ints, aggregate)
    }
    caught.getMessage should include("int-long reduceByKey expected pair batches")

    val mismatched = ColumnBinding.Choice(
      KernelChoice.MapValue,
      Set(0, 1),
      Vector.empty,
      ReducerWitness.AddsLongs,
      Layout.AssumedIntLong,
    )
    mismatched.output shouldBe Layout.AssumedIntLong
    KernelRoute.arm(mismatched.kernel) shouldBe KernelRoute.Arm.Existing
  }

  "reduceByKey of a projected int join" should "still publish a column batch" in {
    val left = DistCollection.ofIntPairs(Array(1, 2, 1), Array(10, 20, 30), 2)
    val right = DistCollection.ofIntPairs(Array(1, 2, 3), Array(100, 200, 300), 2)
    val summed = left
      .join(right)
      .map { case (key, pair) => (key, pair._1.toLong) }
      .reduceByKey((leftSum: Long, rightSum: Long) => leftSum + rightSum)
    val described = Describe.describe(summed.plan).getOrElse(fail("sum did not lower"))
    described.choice shouldBe KernelChoice.IntLongProjectSum
    val sink = new CountingSink
    val columnar = ColumnMetrics.withSink(sink)(summed.collect().toMap)
    sink.batchCount should be > 0L
    columnar shouldBe row(summed.collect().toMap)
  }

  "sumJoinLeft" should "stay JoinProbeSum and still publish a column batch" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 2)
    val right = DistCollection.ofIntPairs(Array(1, 2), Array(100, 200), 2)
    val summed = left.join(right).sumJoinLeft
    val described = Describe.describe(summed.plan).getOrElse(fail("probe sum did not lower"))
    described.choice shouldBe KernelChoice.JoinProbeSum
    Layout.known(described.root) shouldBe Layout.AssumedIntLong
    val sink = new CountingSink
    val columnar = ColumnMetrics.withSink(sink)(summed.collect().toMap)
    sink.batchCount should be > 0L
    columnar shouldBe row(summed.collect().toMap)
  }

  private def rowCount(op: PhysicalOp, columns: ColumnarInterpreter.Columns): Int =
    ColumnarInterpreter.decodeFor(columns, op).foldLeft(0) { (total, part) =>
      total + part.data.size
    }

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()
