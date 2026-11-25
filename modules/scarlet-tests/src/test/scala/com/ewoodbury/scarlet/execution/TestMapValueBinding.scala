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

/**
 * Same-type `mapValues` asks [[ColumnBinding]] before it builds.
 *
 * The built class is [[PhysicalOp.MapValue]] when the tag is [[KernelChoice.MapValue]]. A kind
 * match that also calls [[choose]] and ignores [[scala.None]] still builds for a
 * [[PhysicalOp.Kind.Pairs]] child whose layout is [[Layout.AssumedIntLong]].
 * [[MapValueBinding.value]] returns [[scala.None]] for that child, and
 * [[ColumnBinding.chooseCallsForTest]] moves. A kind-only lower of an int `mapValues` leaves the
 * counter at zero. [[Describe.tag]] is not the check.
 *
 * `Int => Long` builds [[PhysicalOp.IntLongMapValue]] when the tag is
 * [[KernelChoice.IntLongMapValue]]. The same-type [[Operator.MapValues]] choice on that pair stays
 * [[KernelChoice.MapValue]]. A `mapValues` above [[Plan.MapIntKeyOp]] stays [[scala.None]] before
 * the ask. A string pair widened to long fails the kind check and does not ask.
 */
class TestMapValueBinding extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  private val intPairs: Plan[(Int, Int)] = Plan.Source(
    Seq(Partition(Seq((1, 10), (2, 20)))),
    Plan.SourceDescriptor.Pairs,
  )

  private val longPairs: Plan[(Long, Long)] = Plan.Source(
    Seq(Partition(Seq((1L, 10L), (2L, 20L)))),
    Plan.SourceDescriptor.LongPairs,
  )

  private val stringPairs: Plan[(String, Int)] = Plan.Source(
    Seq(Partition(Seq("b" -> 2, "a" -> 1))),
  )

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    ColumnBinding.resetChooseCallsForTest()

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ()

  "MapValueBinding" should "ask the binder for an accepted int mapValues" in {
    val mapped = lower(Plan.MapValuesOp(intPairs, (value: Int) => value + 1))
    mapped.map(_.shape) shouldBe Some("MapValue(Scan)")
    ColumnBinding.chooseCallsForTest shouldBe 1
    follows(mapped)
  }

  it should "ask for a long mapValues and a string-pair mapValues" in {
    val longs = lower(Plan.MapValuesOp(longPairs, (value: Long) => value + 1L))
    longs.map(_.shape) shouldBe Some("MapValue(Scan)")
    ColumnBinding.chooseCallsForTest shouldBe 1
    follows(longs)
    val strings = lower(Plan.MapValuesOp(stringPairs, (value: Int) => value + 1))
    strings.map(_.shape) shouldBe Some("MapValue(Scan)")
    ColumnBinding.chooseCallsForTest shouldBe 1
    follows(strings)
  }

  it should "ask before widening an int value to long" in {
    val mapped = lower(Plan.MapValuesOp(intPairs, (value: Int) => value.toLong))
    mapped.map(_.shape) shouldBe Some("IntLongMapValue(Scan)")
    ColumnBinding.chooseCallsForTest shouldBe 1
    className(mapped, "IntLongMapValue")
    val root = mapped.getOrElse(fail("int mapValues to long did not lower"))
    root.schema shouldBe PhysicalSchema.IntLong
    Layout.known(root) should not be Layout.AssumedIntLong
    Layout.known(root) should not be Layout.ProbeSum
    choose(
      Vector(
        ColumnLayout(ColumnType.Int32, NullPolicy.Nullable),
        ColumnLayout(ColumnType.Int32, NullPolicy.Nullable),
      ),
    ) shouldBe Some(KernelChoice.MapValue)
    ColumnBinding.choose(
      Vector(
        ColumnLayout(ColumnType.Int32, NullPolicy.Nullable),
        ColumnLayout(ColumnType.Int32, NullPolicy.Nullable),
      ),
      Operator.IntLongMapValue,
      ReducerWitness.NotApplicable,
      NullAdmission.Policy(NullPolicy.Nullable),
      Side.Inner,
    ) shouldBe Some(KernelChoice.IntLongMapValue)
  }

  it should "recognize apply$mcJI$sp without calling the function" in {
    val bomb = new Function1[Int, Long]:
      def apply(value: Int): Long = throw new IllegalStateException("called")
    FunctionWitness.declaredIntToLong(bomb) shouldBe true
    FunctionWitness.declaredIntToLong((value: Int) => value + 1) shouldBe false
    FunctionWitness.declaredIntToLong((pair: (Int, Int)) => pair) shouldBe false
    FunctionWitness.declaredElement(PhysicalOp.Kind.Ints, (value: Int) => value.toLong) shouldBe
      false
  }

  it should "leave a string pair widened to long unasked" in {
    lower(Plan.MapValuesOp(stringPairs, (value: Int) => value.toLong)) shouldBe None
    ColumnBinding.chooseCallsForTest shouldBe 0
  }

  it should "leave mapValues above an int key map unasked" in {
    val keyed = Plan.MapIntKeyOp(intPairs, (key: Int) => key + 1)
    lower(Plan.MapValuesOp(keyed, (value: Int) => value + 1)) shouldBe None
    ColumnBinding.chooseCallsForTest shouldBe 0
    lower(Plan.MapValuesOp(keyed, (value: Int) => value.toLong)) shouldBe None
    ColumnBinding.chooseCallsForTest shouldBe 0
  }

  it should "not build a map value from Kind.Pairs when the binder returns None" in {
    val triples = PhysicalOp.Scan(PhysicalOp.Kind.Triples, Seq.empty)
    val child = PhysicalOp.MapKey(PhysicalOp.Project(triples))
    child.kind shouldBe PhysicalOp.Kind.Pairs
    Layout.known(child) shouldBe Layout.AssumedIntLong
    PhysicalSchema.isIntLongProject(child) shouldBe false
    ColumnBinding.resetChooseCallsForTest()
    MapValueBinding.value(child) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
    ColumnBinding.asked(child, Operator.MapValues) shouldBe None
    ColumnBinding.resetChooseCallsForTest()
    IntLongMapBinding.value(child) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
    ColumnBinding.asked(child, Operator.IntLongMapValue) shouldBe None
  }

  /**
   * The class is the tag [[ColumnBinding.choose]] returns for the child `mapValues` wrapped.
   * [[Describe.tag]] is not this comparison. Call this after the counter assertion: [[asked]]
   * counts too.
   */
  private def follows(lowered: Option[PhysicalOp]): Unit =
    className(lowered) shouldBe KernelChoice.MapValue.tag
    ColumnBinding.asked(childOf(lowered), Operator.MapValues) shouldBe Some(KernelChoice.MapValue)

  private def lower(plan: Plan[_]): Option[PhysicalOp] =
    ColumnBinding.resetChooseCallsForTest()
    ColumnarPlanner.lower(plan)

  private def childOf(lowered: Option[PhysicalOp]): PhysicalOp = lowered match
    case Some(PhysicalOp.MapValue(child)) => child
    case other => fail(s"expected a map value, got $other")

  private def className(lowered: Option[PhysicalOp], expected: String = "MapValue"): String =
    lowered match
      case Some(_: PhysicalOp.MapValue) => "MapValue"
      case Some(_: PhysicalOp.IntLongMapValue) => "IntLongMapValue"
      case other => fail(s"expected $expected, got $other")

  private def choose(layout: Vector[ColumnLayout]): Option[KernelChoice] =
    ColumnBinding.choose(
      layout,
      Operator.MapValues,
      ReducerWitness.NotApplicable,
      NullAdmission.Policy(NullPolicy.Nullable),
      Side.Inner,
    )
