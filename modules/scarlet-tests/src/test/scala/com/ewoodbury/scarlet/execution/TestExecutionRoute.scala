package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.{Describe, KernelChoice, Layout}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * The execution route is the binder tag for a hash aggregate and the child layout for a pair.
 *
 * [[KernelChoice.IntLongProjectSum]] is the only tag that runs [[IntLongReduce]]. [[Describe]]
 * reads the tag stored on the hash aggregate. [[PhysicalSchema.isIntLongAggregate]] is the schema
 * fact and is not that label. A projected join sum publishes a batch.
 * [[KernelChoice.JoinProbeSum]] reports [[Layout.ProbeSum]], not [[Layout.AssumedIntLong]], and
 * does not enter [[IntLongReduce]]. A count of that project is [[ExecutionRoute.CountArm.IntLong]]
 * because [[ExecutionRoute.countArm]] still calls [[PhysicalSchema.isIntLong]]. Long and string
 * counts publish through their own kernels. A pass that maps every other tag to the old match is
 * not this test.
 */
class TestExecutionRoute extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()
    IntLongReduce.resetColumnsForTest()
    HashReduce.resetByKeyForTest()
    HashCount.resetByKeyForTest()
    LongHashCount.resetByKeyForTest()
    StringHashCount.resetByKeyForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    IntLongReduce.resetColumnsForTest()
    HashReduce.resetByKeyForTest()
    HashCount.resetByKeyForTest()
    LongHashCount.resetByKeyForTest()
    StringHashCount.resetByKeyForTest()
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
    val aggregate = PhysicalOp.HashAggregate(
      PhysicalOp.Exchange(project),
      PhysicalOp.HashAggregateTag.IntLongSum,
    )
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

  "pair layouts" should "name the arm the tag shares" in {
    val ints = PhysicalOp.Scan(PhysicalOp.Kind.Ints, Seq.empty)
    val strings = PhysicalOp.Scan(PhysicalOp.Kind.Strings, Seq.empty)
    val pairs = PhysicalOp.Scan(PhysicalOp.Kind.Pairs, Seq.empty)
    val longs = PhysicalOp.Scan(PhysicalOp.Kind.LongPairs, Seq.empty)
    val text = PhysicalOp.Scan(PhysicalOp.Kind.StringPairs, Seq.empty)
    val grouped = PhysicalOp.GroupByKey(PhysicalOp.Exchange(pairs))
    val triples = PhysicalOp.Project(PhysicalOp.Scan(PhysicalOp.Kind.Triples, Seq.empty))
    val aggregate = PhysicalOp.HashAggregate(
      PhysicalOp.Exchange(triples),
      PhysicalOp.HashAggregateTag.IntHash,
    )
    ExecutionRoute.pairArm(pairs) shouldBe Some(ExecutionRoute.PairArm.IntPairs)
    ExecutionRoute.pairArm(longs) shouldBe Some(ExecutionRoute.PairArm.LongPairs)
    ExecutionRoute.pairArm(text) shouldBe Some(ExecutionRoute.PairArm.StringPairs)
    ExecutionRoute.pairArm(grouped) shouldBe None
    ExecutionRoute.pairArm(triples) shouldBe None
    Layout.known(grouped) shouldBe Layout.Grouped
    ExecutionRoute.distinctArm(ints) shouldBe ExecutionRoute.DistinctArm.Ints
    ExecutionRoute.distinctArm(strings) shouldBe ExecutionRoute.DistinctArm.Strings
    ExecutionRoute.mapValueArm(pairs) shouldBe ExecutionRoute.MapValueArm.IntValues
    ExecutionRoute.mapValueArm(longs) shouldBe ExecutionRoute.MapValueArm.LongValues
    ExecutionRoute.mapValueArm(text) shouldBe ExecutionRoute.MapValueArm.StringValues
    ExecutionRoute.stringPairs(text) shouldBe true
    ExecutionRoute.stringPairs(pairs) shouldBe false
    ExecutionRoute.countArm(pairs) shouldBe ExecutionRoute.CountArm.IntPairs
    ExecutionRoute.countArm(longs) shouldBe ExecutionRoute.CountArm.LongPairs
    ExecutionRoute.countArm(text) shouldBe ExecutionRoute.CountArm.StringPairs
    ExecutionRoute.countArm(triples) shouldBe ExecutionRoute.CountArm.IntLong
    Layout.known(aggregate) should not be Layout.AssumedIntLong
    ExecutionRoute.countArm(aggregate) shouldBe ExecutionRoute.CountArm.IntLong
  }

  "a long count" should "run the long kernel" in {
    val counted = DistCollection.ofLongPairs(Array(1L, 2L, 1L), Array(10L, 20L, 30L), 2).countByKey
    val described = Describe.describe(counted.plan).getOrElse(fail("long count did not lower"))
    described.choice shouldBe KernelChoice.CountByKey
    ExecutionRoute.countArm(countChild(described.root)) shouldBe ExecutionRoute.CountArm.LongPairs
    val columnar = published(counted.collect().toMap)
    LongHashCount.byKeyCallsForTest should be > 0
    HashCount.byKeyCallsForTest shouldBe 0
    StringHashCount.byKeyCallsForTest shouldBe 0
    columnar shouldBe row(counted.collect().toMap)
  }

  "a string count" should "run the string kernel" in {
    val counted = DistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3), 2).countByKey
    val described = Describe.describe(counted.plan).getOrElse(fail("string count did not lower"))
    described.choice shouldBe KernelChoice.CountByKey
    ExecutionRoute.countArm(countChild(described.root)) shouldBe
      ExecutionRoute.CountArm.StringPairs
    val columnar = published(counted.collect().toMap)
    StringHashCount.byKeyCallsForTest should be > 0
    LongHashCount.byKeyCallsForTest shouldBe 0
    HashCount.byKeyCallsForTest shouldBe 0
    columnar shouldBe row(counted.collect().toMap)
  }

  "a count of a triple project" should "enter IntLongCount and publish" in {
    val left = pairs(Array(1, 2, 1), Array(10, 20, 30))
    val right = pairs(Array(1, 2, 3), Array(100, 200, 300))
    val counted = project(left, right).countByKey
    val described = Describe.describe(counted.plan).getOrElse(fail("count did not lower"))
    described.choice shouldBe KernelChoice.CountByKey
    ExecutionRoute.countArm(countChild(described.root)) shouldBe ExecutionRoute.CountArm.IntLong
    val columnar = published(counted.collect().toMap)
    HashCount.byKeyCallsForTest shouldBe 0
    LongHashCount.byKeyCallsForTest shouldBe 0
    StringHashCount.byKeyCallsForTest shouldBe 0
    columnar shouldBe row(counted.collect().toMap)
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
    case PhysicalOp.HashAggregate(PhysicalOp.Exchange(child), _) => child
    case other => fail(s"expected a hash aggregate exchange, got ${other.shape}")

  private def countChild(op: PhysicalOp): PhysicalOp = op match
    case PhysicalOp.CountByKey(PhysicalOp.Exchange(child)) => child
    case other => fail(s"expected a count exchange, got ${other.shape}")

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
