package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, HashReduce}
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.{Describe, KernelChoice, Layout}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * The hash-aggregate route is the binder's tag.
 *
 * [[KernelChoice.IntLongProjectSum]] is the only tag that runs [[IntLongReduce]].
 * [[PhysicalSchema.isIntLongAggregate]] still labels that shape for [[Describe]]. A projected join
 * sum publishes a batch. [[KernelChoice.JoinProbeSum]] reports [[Layout.ProbeSum]], not
 * [[Layout.AssumedIntLong]], and does not enter [[IntLongReduce]]. A pass that maps every other
 * tag to the old match is not this test.
 */
class TestExecutionRoute extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()
    IntLongReduce.resetColumnsForTest()
    HashReduce.resetByKeyForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    IntLongReduce.resetColumnsForTest()
    HashReduce.resetByKeyForTest()
    ()

  "a projected join sum" should "run IntLongReduce from the binder" in {
    val left = pairs(Array(1, 2, 1), Array(10, 20, 30))
    val right = pairs(Array(1, 2, 3), Array(100, 200, 300))
    val summed = project(left, right).reduceByKey(add)
    val described = Describe.describe(summed.plan).getOrElse(fail("sum did not lower"))
    described.choice shouldBe KernelChoice.IntLongProjectSum
    val child = exchangeChild(described.root)
    ExecutionRoute.reduceArm(reduceSource(summed.plan), child, add) shouldBe
      ExecutionRoute.Arm.IntLongReduce
    ExecutionRoute.decodesAsIntLongSum(Some(summed.plan), described.root) shouldBe true
    val columnar = published(summed.collect().toMap)
    IntLongReduce.columnsCallsForTest should be > 0
    columnar shouldBe row(summed.collect().toMap)
  }

  "an int pair reduce" should "stay off IntLongReduce" in {
    val reduced = pairs(Array(1, 2, 1), Array(10, 20, 30)).reduceByKey(intAdd)
    val described = Describe.describe(reduced.plan).getOrElse(fail("reduce did not lower"))
    described.choice shouldBe KernelChoice.IntHashAggregate
    val source = reduceSource(reduced.plan)
    ExecutionRoute.reduceArm(source, exchangeChild(described.root), intAdd) shouldBe
      ExecutionRoute.Arm.IntHashAggregate
    ExecutionRoute.decodesAsIntLongSum(Some(reduced.plan), described.root) shouldBe false
    val columnar = published(reduced.collect().toMap)
    IntLongReduce.columnsCallsForTest shouldBe 0
    HashReduce.byKeyCallsForTest should be > 0
    columnar shouldBe row(reduced.collect().toMap)
  }

  "sumJoinLeft" should "keep its own layout off IntLongReduce" in {
    val left = pairs(Array(1, 2), Array(10, 20))
    val right = pairs(Array(1, 2), Array(100, 200))
    val summed = left.join(right).sumJoinLeft
    val described = Describe.describe(summed.plan).getOrElse(fail("probe sum did not lower"))
    described.choice shouldBe KernelChoice.JoinProbeSum
    Layout.known(described.root) shouldBe Layout.ProbeSum
    Layout.known(described.root) should not be Layout.AssumedIntLong
    PhysicalSchema.isIntLongAggregate(described.root) shouldBe false
    ExecutionRoute.decodesAsIntLongSum(Some(summed.plan), described.root) shouldBe false
    val columnar = published(summed.collect().toMap)
    IntLongReduce.columnsCallsForTest shouldBe 0
    columnar shouldBe row(summed.collect().toMap)
  }

  "a non-adding reducer on an int-long project" should "not take the schema route" in {
    val triples = PhysicalOp.Scan(PhysicalOp.Kind.Triples, Seq.empty)
    val project = PhysicalOp.Project(triples)
    val aggregate = PhysicalOp.HashAggregate(PhysicalOp.Exchange(project))
    val source = Plan.MapOp(
      Plan.JoinOp(intPairs, intPairs, None),
      (row: (Int, (Int, Int))) => (row._1, row._2._1.toLong),
    )
    Layout.known(project) shouldBe Layout.AssumedIntLong
    PhysicalSchema.isIntLongAggregate(aggregate) shouldBe true
    val longest = (left: Long, right: Long) => math.max(left, right)
    ExecutionRoute.reduceArm(source, project, longest) shouldBe
      ExecutionRoute.Arm.IntHashAggregate
    val reduced = Plan.ReduceByKeyOp(source, longest)
    ExecutionRoute.decodesAsIntLongSum(Some(reduced), aggregate) shouldBe false
    ExecutionRoute.reduceArm(source, project, add) shouldBe ExecutionRoute.Arm.IntLongReduce
    ColumnarPlanner.lower(reduced) shouldBe None
    // This node lowers to nothing, so collect never reaches it. interpret still must not use the
    // schema test: that test is true here, and it would enter IntLongReduce before declining.
    IntLongReduce.resetColumnsForTest()
    try ColumnarInterpreter.interpret(reduced, aggregate)
    catch case _: RuntimeException => ()
    IntLongReduce.columnsCallsForTest shouldBe 0
  }

  private def add: (Long, Long) => Long = (left, right) => left + right

  private def intAdd: (Int, Int) => Int = (left, right) => left + right

  private def pairs(keys: Array[Int], values: Array[Int]): DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(keys, values, 2)

  private def intPairs: Plan[(Int, Int)] =
    Plan.Source(Seq(Partition(Seq((1, 10)))))

  private def project(
      left: DistCollection[(Int, Int)],
      right: DistCollection[(Int, Int)],
  ): DistCollection[(Int, Long)] =
    left.join(right).map { case (key, pair) => (key, pair._1.toLong) }

  private def reduceSource(plan: Plan[_]): Plan[_] = plan match
    case Plan.ReduceByKeyOp(source, _) => source
    case other => fail(s"expected reduceByKey, got $other")

  private def exchangeChild(op: PhysicalOp): PhysicalOp = op match
    case PhysicalOp.HashAggregate(PhysicalOp.Exchange(child)) => child
    case other => fail(s"expected a hash aggregate exchange, got ${other.shape}")

  private def published[A](body: => A): A =
    val sink = new CountingSink
    val result = ColumnMetrics.withSink(sink)(body)
    sink.batchCount should be > 0L
    result

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()
