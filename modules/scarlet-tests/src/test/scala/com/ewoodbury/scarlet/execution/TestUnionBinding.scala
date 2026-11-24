package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `union` asks [[ColumnBinding]] before it builds.
 *
 * An accepted int-pair union goes through [[ColumnarPlanner.lower]].
 * [[ColumnBinding.chooseCallsForTest]] moves. A lower that refuses a mixed union from
 * [[PhysicalOp.Kind]] alone leaves that counter at zero. [[Describe.tag]] is not the check.
 *
 * A join-probe sum and a triple project both report `[Int32, Int64]` and their kinds differ.
 * That union stays unbound. [[ColumnarPlanner.tryRun]] does not throw, and `collect` matches the
 * row path. With the sum on the left, an interpreter miss is an `IllegalStateException`, which
 * [[ColumnarPlanner.tryRun]] does not catch. With the project on the left, [[PhysicalOp.Union]]
 * would report three [[com.ewoodbury.scarlet.columnar.LogicalType.Int32]] columns.
 */
class TestUnionBinding extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(originalConf.copy(columnarExecution = true, threadPoolSize = 1))
    ScarletRuntime.get.shuffle.clear()
    ColumnBinding.resetChooseCallsForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    super.afterEach()

  "union" should "ask the binder for an accepted int pair union" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 1)
    val right = DistCollection.ofIntPairs(Array(2, 3), Array(20, 30), 1)
    val united = left.union(right)
    val lowered = ColumnarPlanner.lower(united.plan)
    lowered.map(_.shape) shouldBe Some("Union(Scan, Scan)")
    ColumnBinding.chooseCallsForTest should be > 0
    val ran = ColumnarPlanner.tryRun(united.plan).getOrElse(fail("int pair union did not run"))
    val expected = row(united.collect().toList)
    ran.flatMap(_.data).toList shouldBe expected
    ScarletRuntime.get.shuffle.clear()
    united.collect().toList shouldBe expected
  }

  it should "ask the binder before refusing int pairs union long pairs" in {
    val ints = DistCollection.ofIntPairs(Array(1), Array(10), 1).plan
    val longs = DistCollection.ofLongPairs(Array(1L), Array(10L), 1).plan
    ColumnarPlanner.lower(Plan.UnionOp(ints, erase(longs))) shouldBe Option.empty[PhysicalOp]
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "keep a join-probe sum union a triple project on the row path" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 2)
    val right = DistCollection.ofIntPairs(Array(1, 2), Array(100, 200), 2)
    val joined = left.join(right)
    val sum = joined.sumJoinLeft
    val project = joined.map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
    differentKinds(sum.plan, project.plan)
    onRows(sum.union(project))
    onRows(project.union(sum))
    onRows(sum.union(sum))
  }

  it should "refuse an inner join union a left outer join" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 1)
    val right = DistCollection.ofIntPairs(Array(1), Array(100), 1)
    val inner = left.join(right).plan
    val outer = left.leftOuterJoin(right).plan
    ColumnBinding.resetChooseCallsForTest()
    ColumnarPlanner.lower(Plan.UnionOp(inner, erase(outer))) shouldBe Option.empty[PhysicalOp]
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "keep a union of two left outer joins" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 1)
    val right = DistCollection.ofIntPairs(Array(1), Array(100), 1)
    val outer = left.leftOuterJoin(right)
    val united = outer.union(outer)
    ColumnBinding.resetChooseCallsForTest()
    val lowered = ColumnarPlanner.lower(united.plan)
    lowered.map(_.shape).getOrElse(fail("outer union did not lower")) should startWith("Union(")
    ColumnBinding.chooseCallsForTest should be > 0
    ColumnarPlanner.tryRun(united.plan).isDefined shouldBe true
    val expected = row(united.collect().toList)
    ScarletRuntime.get.shuffle.clear()
    united.collect().toList should contain theSameElementsAs expected
  }

  /** Same `[Int32, Int64]` layout, different kinds. The binder is asked by [[onRows]]. */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def differentKinds(leftPlan: Plan[_], rightPlan: Plan[_]): Unit =
    val left = ColumnarPlanner.lower(leftPlan).getOrElse(fail("left child did not lower"))
    val right = ColumnarPlanner.lower(rightPlan).getOrElse(fail("right child did not lower"))
    left match
      case _: PhysicalOp.JoinProbeSum => ()
      case other => fail(s"expected JoinProbeSum, got $other")
    right match
      case _: PhysicalOp.Project => ()
      case other => fail(s"expected Project, got $other")
    Layout.known(left) shouldBe Layout.known(right)
    left.kind == right.kind shouldBe false

  /**
   * Unbound, or a lowered union whose schema is `[Int32, Int64]` and whose `tryRun` returns the
   * row rows. A sum on the left must not throw. A project on the left must not report three
   * `Int32` columns.
   */
  private def onRows(united: DistCollection[(Int, Long)]): Unit =
    ColumnBinding.resetChooseCallsForTest()
    val lowered = ColumnarPlanner.lower(united.plan)
    ColumnBinding.chooseCallsForTest should be > 0
    val expected = row(united.collect().toList)
    lowered match
      case Some(op) =>
        PhysicalSchema.of(op) shouldBe PhysicalSchema.IntLong
        val ran = ColumnarPlanner.tryRun(united.plan).getOrElse(fail("lowered union did not run"))
        ran.flatMap(_.data).toList should contain theSameElementsAs expected
      case None =>
        ColumnarPlanner.tryRun(united.plan) shouldBe
          Option.empty[Seq[Partition[(Int, Long)]]]
    ScarletRuntime.get.shuffle.clear()
    united.collect().toList should contain theSameElementsAs expected

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def erase[A](plan: Plan[_]): Plan[A] =
    plan.asInstanceOf[Plan[A]]

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
