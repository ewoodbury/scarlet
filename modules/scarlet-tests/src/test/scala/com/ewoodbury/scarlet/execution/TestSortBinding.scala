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
 * Typed sorts ask [[ColumnBinding]] before they build.
 *
 * An accepted sort goes through [[ColumnarPlanner.lower]]. The built class has to be the tag
 * [[ColumnBinding.choose]] returns for the child layout. [[PhysicalOp.Kind.Pairs]] is the kind of
 * both the int key sort and the value sort, so a kind match that ignores the tag cannot name the
 * class. A [[PhysicalOp.Kind.Pairs]] child whose layout the binder refuses stays [[scala.None]].
 * [[Describe.tag]] is not the check.
 */
class TestSortBinding extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

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

  "SortLowering" should "ask the binder for an accepted int sort" in {
    val lowered = ColumnarPlanner.lower(Plan.SortByIntKeyOp(intPairs))
    lowered.map(_.shape) shouldBe Some("SortByIntKey(Scan)")
    ColumnBinding.chooseCallsForTest should be > 0
    follows(lowered, Operator.SortByIntKey, KernelChoice.SortByIntKey)
  }

  it should "keep the accepted long, string, and value-desc classes" in {
    val intKey = ColumnarPlanner.lower(Plan.SortByIntKeyOp(intPairs))
    val value = ColumnarPlanner.lower(Plan.SortByIntValueDescOp(intPairs))
    follows(intKey, Operator.SortByIntKey, KernelChoice.SortByIntKey)
    follows(value, Operator.SortByIntValueDesc, KernelChoice.SortByIntValueDesc)
    sortChild(intKey).kind shouldBe sortChild(value).kind
    follows(
      ColumnarPlanner.lower(Plan.SortByLongKeyOp(longPairs)),
      Operator.SortByLongKey,
      KernelChoice.SortByLongKey,
    )
    follows(
      ColumnarPlanner.lower(Plan.SortByStringKeyOp(stringPairs)),
      Operator.SortByStringKey,
      KernelChoice.SortByStringKey,
    )
  }

  it should "not build an int sort from Kind.Pairs when the binder returns None" in {
    val triples = PhysicalOp.Scan(PhysicalOp.Kind.Triples, Seq.empty)
    val child = PhysicalOp.MapKey(PhysicalOp.Project(triples))
    child.kind shouldBe PhysicalOp.Kind.Pairs
    Layout.known(child) shouldBe Layout.AssumedIntLong
    ColumnBinding.resetChooseCallsForTest()
    SortLowering.lowerInt(intPairs, _ => Some(child)).shouldBeEmpty
    ColumnBinding.chooseCallsForTest should be > 0
    ColumnBinding.asked(child, Operator.SortByIntKey).shouldBeEmpty
  }

  it should "return None for a layout the int sort refuses" in {
    refuse(pair(ColumnType.Int64, ColumnType.Int64))
    val assumed = Layout.AssumedIntLong match
      case Output.Columns(columns) => columns
      case Output.IntLongPairs(_) => fail("int-long map values are not an int sort")
      case Output.Opaque(reason) => fail(reason)
    refuse(assumed)

    ColumnBinding.resetChooseCallsForTest()
    val rows = Seq[Partition[_]](Partition(Seq(1L -> 2L)))
    val longScan = PhysicalOp.Scan(PhysicalOp.Kind.LongPairs, rows)
    SortLowering.lowerInt(intPairs, _ => Some(longScan)).shouldBeEmpty
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "keep mapValues off an int key sort and on the value sort" in {
    val mapped = Plan.MapValuesOp(intPairs, (value: Int) => value + 1)
    ColumnarPlanner.lower(Plan.SortByIntKeyOp(mapped)).shouldBeEmpty
    ColumnBinding.chooseCallsForTest should be > 0
    val valueSort = ColumnarPlanner.lower(Plan.SortByIntValueDescOp(mapped))
    sortClass(valueSort, "SortByIntValueDesc")
    valueSort.map(_.shape) shouldBe Some("SortByIntValueDesc(MapValue(Scan))")
  }

  private def refuse(layout: Vector[ColumnLayout]): Unit =
    ColumnBinding
      .choose(
        layout,
        Operator.SortByIntKey,
        ReducerWitness.NotApplicable,
        NullAdmission.Policy(NullPolicy.Nullable),
        Side.Inner,
      )
      .shouldBeEmpty

  private def pair(key: ColumnType, value: ColumnType): Vector[ColumnLayout] =
    Vector(ColumnLayout(key, NullPolicy.Nullable), ColumnLayout(value, NullPolicy.Nullable))

  /**
   * The class is the tag [[ColumnBinding.choose]] returns for the child the sort wrapped.
   * [[Describe.tag]] is not this comparison.
   */
  private def follows(
      lowered: Option[PhysicalOp],
      operator: Operator,
      tag: KernelChoice,
  ): Unit =
    sortClass(lowered, tag.tag)
    ColumnBinding.asked(sortChild(lowered), operator) shouldBe Some(tag)

  private def sortChild(lowered: Option[PhysicalOp]): PhysicalOp = lowered match
    case Some(PhysicalOp.SortByIntKey(child)) => child
    case Some(PhysicalOp.SortByIntValueDesc(child)) => child
    case Some(PhysicalOp.SortByLongKey(child)) => child
    case Some(PhysicalOp.SortByStringKey(child)) => child
    case other => fail(s"expected a sort, got $other")

  private def sortClass(lowered: Option[PhysicalOp], name: String): Unit =
    val found = lowered match
      case Some(_: PhysicalOp.SortByIntKey) => "SortByIntKey"
      case Some(_: PhysicalOp.SortByIntValueDesc) => "SortByIntValueDesc"
      case Some(_: PhysicalOp.SortByLongKey) => "SortByLongKey"
      case Some(_: PhysicalOp.SortByStringKey) => "SortByStringKey"
      case other => fail(s"expected $name, got $other")
    found shouldBe name
