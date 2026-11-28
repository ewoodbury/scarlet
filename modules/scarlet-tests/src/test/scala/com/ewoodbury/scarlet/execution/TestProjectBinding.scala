package com.ewoodbury.scarlet.execution
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.*
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.{
  NullAdmission,
  Operator,
  ReducerWitness,
  Side,
}
import com.ewoodbury.scarlet.support.Check.shouldBeEmpty

/**
 * Same-type element maps ask [[ColumnBinding]] before they build.
 *
 * One `map` builds [[PhysicalOp.Project]] when the tag is [[KernelChoice.Project]]. Two maps build
 * [[PhysicalOp.Pipeline]] when the tag is [[KernelChoice.Pipeline]]. A kind match that builds
 * either class and ignores [[scala.None]] still builds for a [[PhysicalOp.Kind.Pairs]] child whose
 * layout is [[Layout.AssumedIntLong]]. [[ProjectBinding.project]] returns [[scala.None]] for that
 * child, and [[ColumnBinding.chooseCallsForTest]] moves. A kind-only lower of an int map leaves
 * the counter at zero. [[Describe.tag]] is not the check.
 *
 * `Int => Long` stays [[scala.None]] even though one int column tags [[KernelChoice.Project]]. The
 * witness refuses it. A boolean map stays [[scala.None]]. An int-pair project stays the class it
 * lowers as today and does not ask this operator.
 */
class TestProjectBinding extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  private val ints: Plan[Int] = Plan.Source(
    Seq(Partition(Seq(1, 2, 3))),
    Plan.SourceDescriptor.Ints,
  )

  private val longs: Plan[Long] = Plan.Source(
    Seq(Partition(Seq(1L, 2L, 3L))),
    Plan.SourceDescriptor.Longs,
  )

  private val doubles: Plan[Double] = Plan.Source(
    Seq(Partition(Seq(1.0, 2.0))),
    Plan.SourceDescriptor.Doubles,
  )

  private val strings: Plan[String] = Plan.Source(
    Seq(Partition(Seq("a", "b"))),
    Plan.SourceDescriptor.Strings,
  )

  private val bools: Plan[Boolean] = Plan.Source(
    Seq(Partition(Seq(true, false))),
    Plan.SourceDescriptor.Bools,
  )

  private val intPairs: Plan[(Int, Int)] = Plan.Source(
    Seq(Partition(Seq((1, 10), (2, 20)))),
    Plan.SourceDescriptor.Pairs,
  )

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    ColumnBinding.resetChooseCallsForTest()

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ()

  "ProjectBinding" should "ask the binder for an accepted int map" in {
    val mapped = lower(Plan.MapOp(ints, (value: Int) => value + 1))
    mapped.map(_.shape) shouldBe Some("Project(Scan)")
    ColumnBinding.chooseCallsForTest shouldBe 1
    follows(mapped, Operator.Project, KernelChoice.Project)
  }

  it should "ask for a long, double, and string map" in {
    val longMap = lower(Plan.MapOp(longs, (value: Long) => value + 1L))
    longMap.map(_.shape) shouldBe Some("Project(Scan)")
    ColumnBinding.chooseCallsForTest shouldBe 1
    follows(longMap, Operator.Project, KernelChoice.Project)
    val doubleMap = lower(Plan.MapOp(doubles, (value: Double) => value + 1.0))
    doubleMap.map(_.shape) shouldBe Some("Project(Scan)")
    ColumnBinding.chooseCallsForTest shouldBe 1
    follows(doubleMap, Operator.Project, KernelChoice.Project)
    val stringMap = lower(Plan.MapOp(strings, (value: String) => value + "!"))
    stringMap.map(_.shape) shouldBe Some("Project(Scan)")
    ColumnBinding.chooseCallsForTest shouldBe 1
    follows(stringMap, Operator.Project, KernelChoice.Project)
  }

  it should "ask for a pipeline of two int maps" in {
    val twice = Plan.MapOp(
      Plan.MapOp(ints, (value: Int) => value + 1),
      (value: Int) => value + 2,
    )
    val mapped = lower(twice)
    mapped.map(_.shape) shouldBe Some("Pipeline2(Scan)")
    ColumnBinding.chooseCallsForTest shouldBe 1
    follows(mapped, Operator.Pipeline, KernelChoice.Pipeline)
  }

  it should "keep a filter beside a same-type map on the pipeline tag" in {
    val mixed = Plan.MapOp(
      Plan.FilterOp(ints, (value: Int) => value > 0),
      (value: Int) => value + 1,
    )
    val mapped = lower(mixed)
    mapped.map(_.shape) shouldBe Some("Pipeline2(Scan)")
    ColumnBinding.chooseCallsForTest should be > 0
    follows(mapped, Operator.Pipeline, KernelChoice.Pipeline)
  }

  it should "leave an int value widened to long unasked" in {
    lower(Plan.MapOp(ints, (value: Int) => value.toLong)).shouldBeEmpty
    ColumnBinding.chooseCallsForTest shouldBe 0
    choose(Vector(ColumnLayout(ColumnType.Int32, NullPolicy.Nullable)), Operator.Project) shouldBe
      Some(KernelChoice.Project)
  }

  it should "leave a boolean map empty" in {
    lower(Plan.MapOp(bools, (value: Boolean) => !value)).shouldBeEmpty
    ColumnBinding.chooseCallsForTest shouldBe 0
    val column = Vector(ColumnLayout(ColumnType.Bool, NullPolicy.Nullable))
    choose(column, Operator.Project).shouldBeEmpty
    choose(column, Operator.Pipeline).shouldBeEmpty
  }

  it should "not build a project from Kind.Pairs when the binder returns None" in {
    val triples = PhysicalOp.Scan(PhysicalOp.Kind.Triples, Seq.empty)
    val child = PhysicalOp.MapKey(PhysicalOp.Project(triples))
    child.kind shouldBe PhysicalOp.Kind.Pairs
    Layout.known(child) shouldBe Layout.AssumedIntLong
    PhysicalSchema.isIntLongProject(child) shouldBe false
    ColumnBinding.resetChooseCallsForTest()
    ProjectBinding.project(child).shouldBeEmpty
    ColumnBinding.chooseCallsForTest should be > 0
    ColumnBinding.asked(child, Operator.Project).shouldBeEmpty
    ColumnBinding.resetChooseCallsForTest()
    ProjectBinding.pipeline(child, 2).shouldBeEmpty
    ColumnBinding.chooseCallsForTest should be > 0
    ColumnBinding.asked(child, Operator.Pipeline).shouldBeEmpty
  }

  it should "keep an int-pair project off this ask" in {
    val mapped = lower(Plan.MapOp(intPairs, (row: (Int, Int)) => row))
    mapped.map(_.shape) shouldBe Some("Project(Scan)")
    ColumnBinding.chooseCallsForTest shouldBe 0
  }

  /**
   * The class is the tag [[ColumnBinding.choose]] returns for the child the map wrapped.
   * [[Describe.tag]] is not this comparison. Call this after the counter assertion: [[asked]]
   * counts too.
   */
  private def follows(
      lowered: Option[PhysicalOp],
      operator: Operator,
      tag: KernelChoice,
  ): Unit =
    className(lowered) shouldBe tag.tag
    ColumnBinding.asked(childOf(lowered), operator) shouldBe Some(tag)

  private def lower(plan: Plan[_]): Option[PhysicalOp] =
    ColumnBinding.resetChooseCallsForTest()
    ColumnarPlanner.lower(plan)

  private def childOf(lowered: Option[PhysicalOp]): PhysicalOp = lowered match
    case Some(PhysicalOp.Project(child)) => child
    case Some(PhysicalOp.Pipeline(child, _)) => child
    case other => fail(s"expected a project or a pipeline, got $other")

  private def className(lowered: Option[PhysicalOp]): String = lowered match
    case Some(_: PhysicalOp.Project) => "Project"
    case Some(_: PhysicalOp.Pipeline) => "Pipeline"
    case other => fail(s"expected Project or Pipeline, got $other")

  private def choose(
      layout: Vector[ColumnLayout],
      operator: Operator,
  ): Option[KernelChoice] =
    ColumnBinding.choose(
      layout,
      operator,
      ReducerWitness.NotApplicable,
      NullAdmission.Policy(NullPolicy.Nullable),
      Side.Inner,
    )
