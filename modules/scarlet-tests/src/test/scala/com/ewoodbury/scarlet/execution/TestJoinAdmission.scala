package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{LogicalType, Selection}
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.*
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.{
  NullAdmission,
  Operator,
  ReducerWitness,
  Side,
}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Joins feed [[ColumnBinding]].
 *
 * A hash, broadcast, or sort-merge join reports three columns on its [[Bound]]. `countByKey` of
 * that join is [[KernelChoice.CountThroughProbe]] and matches the row checksum. `reduceByKey` that
 * adds longs reads the project above the join and is [[KernelChoice.IntLongProjectSum]]. Any other
 * reducer on that project does not lower. An outer join and a source [[ScanLowering]] would refuse
 * are [[scala.None]] from the null policy. `sumJoinLeft` stays [[KernelChoice.JoinProbeSum]].
 */
class TestJoinAdmission extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    ()

  "a hash, broadcast, or sort-merge join" should "report three columns on its Bound" in {
    val left = pairs(Array(1, 2), Array(10, 20))
    val right = pairs(Array(1, 3), Array(100, 300))
    val hash = boundOf(left.shuffleHashJoin(right).plan)
    val broadcast = boundOf(left.broadcastJoin(right).plan)
    val merged = boundOf(left.sortMergeJoin(right).plan)
    threeInts(hash, KernelChoice.HashJoin)
    threeInts(broadcast, KernelChoice.BroadcastJoin)
    threeInts(merged, KernelChoice.SortMergeJoin)
    val joinOp = hash.op
    PhysicalSchema.of(joinOp) shouldBe Vector(
      LogicalType.Int32,
      LogicalType.Int32,
      LogicalType.Int32,
    )
    PhysicalSchema.isIntLong(PhysicalSchema.of(joinOp)) shouldBe false
    PhysicalSchema.isIntLongAggregate(joinOp) shouldBe false
  }

  "countByKey over a join" should "count through the probe and match the row checksum" in {
    val left = pairs(Array(1, 2, 1), Array(10, 20, 30))
    val right = pairs(Array(1, 2, 3), Array(100, 200, 300))
    List(
      left.join(right),
      left.broadcastJoin(right),
      left.sortMergeJoin(right),
    ).foreach { joined =>
      val counted = joined.countByKey
      val described = published(counted.plan, KernelChoice.CountThroughProbe)
      described.root.shape should include("CountByKey(HashJoin(")
      described.root.shape should not include "Project"
      val columnar = counted.collect().toMap
      columnar shouldBe row(counted.collect().toMap)
      val checksum = CrossEngineShapes.pairChecksum(columnar)
      println(s"countByKey ${described.root.shape} checksum $checksum tag ${described.choice.tag}")
      checksum shouldBe CrossEngineShapes.pairChecksum(row(counted.collect()))
    }
  }

  "reduceByKey over a projected join" should "sum longs only when the reducer adds them" in {
    val left = pairs(Array(1, 2, 1), Array(10, 20, 30))
    val right = pairs(Array(1, 2, 3), Array(100, 200, 300))
    val joined = left.join(right)
    val projected = joined.map { case (key, pair) => (key, pair._1.toLong) }
    Layout.known(lowered(projected.plan)) shouldBe Layout.AssumedIntLong
    val summed = projected.reduceByKey((leftSum: Long, rightSum: Long) => leftSum + rightSum)
    val described = published(summed.plan, KernelChoice.IntLongProjectSum)
    described.root.shape shouldBe
      "HashAggregate(Exchange(Project(HashJoin(Exchange(Scan), Exchange(Scan)))))"
    val columnar = summed.collect().toMap
    columnar shouldBe row(summed.collect().toMap)
    val checksum = CrossEngineShapes.sumChecksum(columnar)
    println(s"reduceByKey over join checksum $checksum tag ${described.choice.tag}")
    checksum shouldBe CrossEngineShapes.sumChecksum(row(summed.collect()))
    val longest =
      projected.reduceByKey((leftSum: Long, rightSum: Long) => math.max(leftSum, rightSum))
    ColumnarPlanner.lower(longest.plan) shouldBe None
    longest.collect().toMap shouldBe row(longest.collect().toMap)
    val nested = joined
      .map { case (key, pair) => (key, pair) }
      .reduceByKey((leftPair: (Int, Int), rightPair: (Int, Int)) =>
        (leftPair._1 + rightPair._1, leftPair._2 + rightPair._2),
      )
    ColumnarPlanner.lower(nested.plan) shouldBe None
    nested.collect().toMap shouldBe row(nested.collect().toMap)
  }

  "reduceByKey of the join node" should "not pretend the join is an int-long source" in {
    val left = pairs(Array(1, 2), Array(10, 20))
    val right = pairs(Array(1, 2), Array(100, 200))
    val joined = left.join(right).plan
    val child = lowered(joined)
    AggregateBinding.choiceFor(
      joined,
      child,
      Operator.ReduceByKey,
      ReducerWitness.AddsLongs,
    ) shouldBe None
    AggregateBinding.choiceFor(
      joined,
      child,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
    ) shouldBe Some(KernelChoice.CountThroughProbe)
  }

  "an outer join" should "be None from the Absence policy on the Bound" in {
    val left = pairs(Array(1, 2), Array(10, 20))
    val right = pairs(Array(1), Array(100))
    val outer = left.leftOuterJoin(right)
    val bound = boundOf(outer.plan)
    Bound.nulls(bound) shouldBe NullAdmission.Policy(NullPolicy.Absence)
    Bound.side(bound) shouldBe Side.Outer
    val columns = bound.output match
      case Output.Columns(found) => found
      case Output.IntLongPairs(_) => fail("int-long map values are not join columns")
      case Output.Opaque(reason) => fail(reason)
    columns.exists(absent) shouldBe true
    ColumnBinding.choose(
      columns,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
      Bound.nulls(bound),
      Bound.side(bound),
    ) shouldBe None
    ColumnarPlanner.lower(outer.countByKey.plan) shouldBe None
    AggregateBinding.choiceFor(
      outer.plan,
      bound.op,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
    ) shouldBe None
  }

  "a source ScanLowering refuses" should "not count through a join" in {
    val refused = DistCollection(Plan.Source(Seq(Partition(Seq(nullPair)))))
    val right = pairs(Array(1), Array(10))
    ColumnarPlanner.lower(refused.plan) shouldBe None
    ColumnarPlanner.lower(refused.join(right).plan) shouldBe None
    ColumnarPlanner.lower(refused.join(right).countByKey.plan) shouldBe None
  }

  "sumJoinLeft" should "stay JoinProbeSum" in {
    val left = pairs(Array(1, 2), Array(10, 20))
    val right = pairs(Array(1, 2), Array(100, 200))
    val summed = left.join(right).sumJoinLeft
    val described = published(summed.plan, KernelChoice.JoinProbeSum)
    described.root.shape shouldBe "JoinProbeSumLeft(Exchange(Scan), Exchange(Scan))"
    summed.collect().toMap shouldBe row(summed.collect().toMap)
  }

  private def absent(column: ColumnLayout): Boolean = column.nullPolicy match
    case NullPolicy.Absence => true
    case NullPolicy.Present | NullPolicy.Nullable => false

  private def pairs(keys: Array[Int], values: Array[Int]): DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(keys, values, 2)

  private def boundOf(plan: Plan[_]): Bound =
    Bound.of(lowered(plan), Vector.empty, ReducerWitness.NotApplicable)

  private def lowered(plan: Plan[_]): PhysicalOp =
    ColumnarPlanner.lower(plan).getOrElse(fail(s"expected a root for $plan"))

  private def threeInts(bound: Bound, kernel: KernelChoice): Unit =
    bound.kernel shouldBe kernel
    bound.selection shouldBe Vector.empty[Selection.Step]
    bound.reducer shouldBe ReducerWitness.NotApplicable
    bound.output match
      case Output.Columns(Vector(key, left, right)) =>
        key.columnType shouldBe ColumnType.Int32
        left.columnType shouldBe ColumnType.Int32
        right.columnType shouldBe ColumnType.Int32
        key.nullPolicy shouldBe NullPolicy.Nullable
        left.nullPolicy shouldBe NullPolicy.Nullable
        right.nullPolicy shouldBe NullPolicy.Nullable
      case other => fail(s"join bound was $other")
    bound.output should not be Layout.AssumedIntLong

  /** Tag and a published batch. A row checksum still matches after [[ColumnarFallback]]. */
  private def published(plan: Plan[_], tag: KernelChoice): Describe.Described =
    val described = Describe.describe(plan).getOrElse(fail("plan did not lower"))
    described.choice shouldBe tag
    ColumnarPlanner.tryRun(plan).isDefined shouldBe true
    described

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullPair: (Int, Int) =
    Tuple2(null, Int.box(1)).asInstanceOf[(Int, Int)]
