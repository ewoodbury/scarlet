package com.ewoodbury.scarlet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Partition, Plan}
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.{Operator, ReducerWitness}
import com.ewoodbury.scarlet.execution.schema.{Describe, KernelChoice}

/**
 * The three aggregate parents consult [[AggregateBinding]] and still build today's root.
 *
 * A slot filter is peeled only when selection steps exist. A key filter over an int-long project
 * does not compile to steps, so the binder is not asked for the project underneath. `countByKey`
 * keeps today's root, which still holds the filter. Two slots above that project do not lower.
 * `reduceByKey` has no arm there and stays empty. A join count is
 * [[KernelChoice.CountThroughProbe]] from the join's three columns. A probe
 * sum stays [[KernelChoice.JoinProbeSum]]. A projected `(Int32, Int64)` whose reducer does not add
 * longs does not lower.
 */
class TestAggregateBinding extends AnyFlatSpec with Matchers:

  private val pairs: Plan[(Int, Int)] = Plan.Source(
    Seq(Partition(Seq(1 -> 10)), Partition(Seq(2 -> 20))),
  )
  private val rightPairs: Plan[(Int, Int)] = Plan.Source(
    Seq(Partition(Seq(1 -> 100)), Partition(Seq(2 -> 200))),
  )
  private val longPairs: Plan[(Long, Long)] =
    DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 2).plan
  private val stringPairs: Plan[(String, Int)] = Plan.Source(
    Seq(Partition(Seq("red" -> 1))),
  )

  private val joined: Plan[(Int, (Int, Int))] = Plan.JoinOp(pairs, rightPairs, None)
  private val projected: Plan[(Int, Long)] = Plan.MapOp(
    joined,
    (row: (Int, (Int, Int))) => (row._1, row._2._1.toLong),
  )
  private val keyAboveProject: Plan[(Int, Long)] =
    Plan.FilterKeysOp(projected, (key: Int) => key > 0)
  private val addLongs: (Long, Long) => Long = (left, right) => left + right

  "countByKey" should "keep today's root when the binder tag matches" in {
    val plan = Plan.CountByKeyOp(pairs)
    val child = lowered(pairs)
    AggregateBinding.choiceFor(
      pairs,
      child,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
    ) shouldBe Some(
      KernelChoice.CountByKey,
    )
    val root = lowered(plan)
    root.shape shouldBe "CountByKey(Exchange(Scan))"
    Describe.tag(root) shouldBe KernelChoice.CountByKey
  }

  it should "keep a compiled key filter on today's count" in {
    val filtered = Plan.FilterKeysOp(pairs, (key: Int) => key > 0)
    val plan = Plan.CountByKeyOp(filtered)
    AggregateBinding.choiceFor(
      filtered,
      lowered(filtered),
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
    ) shouldBe Some(KernelChoice.CountByKey)
    lowered(plan).shape shouldBe "CountByKey(Exchange(FilterKey(Scan)))"
  }

  it should "keep a key filter over an int-long project on today's count" in {
    val plan = Plan.CountByKeyOp(keyAboveProject)
    AggregateBinding.choiceFor(
      keyAboveProject,
      lowered(keyAboveProject),
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
    ) shouldBe None
    lowered(plan).shape shouldBe
      "CountByKey(Exchange(FilterKey(Project(HashJoin(Exchange(Scan), Exchange(Scan))))))"
  }

  it should "refuse a slot stack above an int-long project" in {
    val values = Plan.FilterValuesOp(projected, (value: Long) => value >= 0L)
    val stacked = Plan.FilterKeysOp(values, (key: Int) => key > 0)
    ColumnarPlanner.lower(Plan.CountByKeyOp(stacked)) shouldBe None
  }

  it should "tag count-through-probe from the join binder" in {
    val plan = Plan.CountByKeyOp(joined)
    val child = lowered(joined)
    AggregateBinding.choiceFor(
      joined,
      child,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
    ) shouldBe Some(KernelChoice.CountThroughProbe)
    AggregateBinding.choiceFor(
      joined,
      child,
      Operator.ReduceByKey,
      ReducerWitness.AddsLongs,
    ) shouldBe None
    val root = lowered(plan)
    root.shape shouldBe "CountByKey(HashJoin(Exchange(Scan), Exchange(Scan)))"
    Describe.tag(root) shouldBe KernelChoice.CountThroughProbe
  }

  "reduceByKey" should "tag an int-long project only when the reducer adds longs" in {
    val child = lowered(projected)
    AggregateBinding.choiceFor(
      projected,
      child,
      Operator.ReduceByKey,
      ReducerWitness.AddsLongs,
    ) shouldBe Some(KernelChoice.IntLongProjectSum)
    AggregateBinding.choiceFor(
      projected,
      child,
      Operator.ReduceByKey,
      ReducerWitness.Other,
    ) shouldBe None
    val summed = lowered(Plan.ReduceByKeyOp(projected, addLongs))
    summed.shape shouldBe
      "HashAggregate(Exchange(Project(HashJoin(Exchange(Scan), Exchange(Scan)))))"
    Describe.tag(summed) shouldBe KernelChoice.IntLongProjectSum
    val longest = (left: Long, right: Long) => math.max(left, right)
    ColumnarPlanner.lower(Plan.ReduceByKeyOp(projected, longest)) shouldBe None
  }

  it should "refuse a key filter over an int-long project" in {
    AggregateBinding.choiceFor(
      keyAboveProject,
      lowered(keyAboveProject),
      Operator.ReduceByKey,
      ReducerWitness.AddsLongs,
    ) shouldBe None
    ColumnarPlanner.lower(Plan.ReduceByKeyOp(keyAboveProject, addLongs)) shouldBe None
  }

  it should "keep a filtered int reduce and a probe sum" in {
    val filtered = Plan.ReduceByKeyOp(
      Plan.FilterValuesOp(pairs, (value: Int) => value > 0),
      (left: Int, right: Int) => left + right,
    )
    lowered(filtered).shape shouldBe "HashAggregate(Exchange(FilterValue(Scan)))"
    val probe = DistCollection(joined).sumJoinLeft.plan
    val root = lowered(probe)
    root.shape shouldBe "JoinProbeSumLeft(Exchange(Scan), Exchange(Scan))"
    Describe.tag(root) shouldBe KernelChoice.JoinProbeSum
  }

  it should "admit a slot stack above mapIntKey and leave the filter plan empty" in {
    val mapped = Plan.MapIntKeyOp(pairs, (key: Int) => key + 1)
    val once = Plan.FilterKeysOp(mapped, (key: Int) => key > 0)
    val twice = Plan.FilterKeysOp(once, (key: Int) => key < 10)
    ColumnarPlanner.lower(twice) shouldBe None
    val counted = lowered(Plan.CountByKeyOp(twice))
    counted.shape shouldBe "CountByKey(Exchange(FilterKey(FilterKey(MapKey(Scan)))))"
    Describe.tag(counted) shouldBe KernelChoice.CountByKey
    val reduced = lowered(
      Plan.ReduceByKeyOp(twice, (left: Int, right: Int) => left + right),
    )
    reduced.shape shouldBe "HashAggregate(Exchange(FilterKey(FilterKey(MapKey(Scan)))))"
    Describe.tag(reduced) shouldBe KernelChoice.IntHashAggregate
    val summed = lowered(Plan.CountSumByKeyOp(twice))
    summed.shape shouldBe "CountSum(Exchange(FilterKey(FilterKey(MapKey(Scan)))))"
    Describe.tag(summed) shouldBe KernelChoice.CountSum
  }

  "countSumByKey" should "keep today's int root" in {
    val plan = Plan.CountSumByKeyOp(pairs)
    AggregateBinding.choiceFor(
      pairs,
      lowered(pairs),
      Operator.CountSumByKey,
      ReducerWitness.NotApplicable,
    ) shouldBe Some(KernelChoice.CountSum)
    val root = lowered(plan)
    root.shape shouldBe "CountSum(Exchange(Scan))"
    Describe.tag(root) shouldBe KernelChoice.CountSum
  }

  it should "keep long and string counts" in {
    lowered(Plan.CountByKeyOp(longPairs)).shape shouldBe "CountByKey(Exchange(Scan))"
    lowered(Plan.CountByKeyOp(stringPairs)).shape shouldBe "CountByKey(Exchange(Scan))"
    lowered(
      Plan.ReduceByKeyOp(longPairs, (left: Long, right: Long) => left + right),
    ).shape shouldBe "LongHashAggregate(Exchange(Scan))"
    lowered(
      Plan.ReduceByKeyOp(stringPairs, (left: Int, right: Int) => left + right),
    ).shape shouldBe "StringHashAggregate(Exchange(Scan))"
  }

  private def lowered(plan: Plan[_]): PhysicalOp =
    ColumnarPlanner.lower(plan).getOrElse(fail(s"expected a root for $plan"))
