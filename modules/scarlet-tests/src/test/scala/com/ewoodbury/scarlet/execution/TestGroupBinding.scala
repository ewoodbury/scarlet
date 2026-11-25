package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.*
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.{Operator, ReducerWitness}

/**
 * `groupByKey`, `distinct`, `keys`, and `values` ask [[ColumnBinding]] before they build.
 *
 * An accepted plan goes through [[ColumnarPlanner.lower]]. The built class has to be the tag
 * [[ColumnBinding.choose]] returns for the child layout. A child whose [[PhysicalOp.Kind]] the arm
 * accepts, and whose layout the binder refuses, stays [[scala.None]]. [[Describe.tag]] is not the
 * check. A map above the group stays [[PhysicalOp.GroupedDecode]]. It is not a columnar project
 * over that decode.
 */
class TestGroupBinding extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

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

  private val ints: Plan[Int] = Plan.Source(
    Seq(Partition(Seq(1, 2, 1))),
    Plan.SourceDescriptor.Ints,
  )

  private val strings: Plan[String] = Plan.Source(
    Seq(Partition(Seq("b", "a", "b"))),
    Plan.SourceDescriptor.Strings,
  )

  private val mapIntKey: Plan[(Int, Int)] =
    Plan.MapIntKeyOp(intPairs, (key: Int) => key + 1)

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    ColumnBinding.resetChooseCallsForTest()

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ()

  "groupByKey" should "ask the binder for an accepted int group" in {
    val lowered = ColumnarPlanner.lower(Plan.GroupByKeyOp(intPairs))
    lowered.map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    ColumnBinding.chooseCallsForTest should be > 0
    follows(lowered, Operator.GroupByKey, KernelChoice.GroupByKey)
  }

  it should "keep the accepted long and string group classes" in {
    keeps(
      Plan.GroupByKeyOp(longPairs),
      Operator.GroupByKey,
      KernelChoice.GroupByKey,
      "GroupByKey(Exchange(Scan))",
    )
    keeps(
      Plan.GroupByKeyOp(stringPairs),
      Operator.GroupByKey,
      KernelChoice.GroupByKey,
      "GroupByKey(Exchange(Scan))",
    )
  }

  it should "ask before refusing a column the group does not run" in {
    val rows = Seq[Partition[_]](Partition(Seq(1)))
    val column = PhysicalOp.Scan(PhysicalOp.Kind.Ints, rows)
    GroupByKeyLowering.apply(column) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "refuse Kind.Pairs when the binder does not tag a group" in {
    val child =
      PhysicalOp.MapKey(PhysicalOp.Project(PhysicalOp.Scan(PhysicalOp.Kind.Triples, Seq.empty)))
    child.kind shouldBe PhysicalOp.Kind.Pairs
    ColumnBinding.resetChooseCallsForTest()
    GroupByKeyLowering.apply(child) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
    asked(child, Operator.GroupByKey) shouldBe None
  }

  "distinct" should "ask the binder for an accepted int column" in {
    val lowered = ColumnarPlanner.lower(Plan.DistinctOp(ints))
    lowered.map(_.shape) shouldBe Some("Dedupe(Exchange(Scan))")
    ColumnBinding.chooseCallsForTest should be > 0
    follows(lowered, Operator.Distinct, KernelChoice.Dedupe)
  }

  it should "keep an accepted string distinct" in {
    keeps(
      Plan.DistinctOp(strings),
      Operator.Distinct,
      KernelChoice.Dedupe,
      "Dedupe(Exchange(Scan))",
    )
  }

  it should "refuse Kind.Ints when the grouped layout is not a column" in {
    val child = PhysicalOp.GroupedDecode(PhysicalOp.Scan(PhysicalOp.Kind.Ints, Seq.empty))
    child.kind shouldBe PhysicalOp.Kind.Ints
    Layout.known(child) shouldBe Layout.Grouped
    DistinctLowering.apply(child) shouldBe None
    asked(child, Operator.Distinct) shouldBe None
  }

  "keys and values" should "ask the binder and keep Column" in {
    val keys = ColumnarPlanner.lower(Plan.KeysOp(intPairs))
    keys.map(_.shape) shouldBe Some("Key(Scan)")
    ColumnBinding.chooseCallsForTest should be > 0
    follows(keys, Operator.Keys, KernelChoice.Column)

    ColumnBinding.resetChooseCallsForTest()
    val values = ColumnarPlanner.lower(Plan.ValuesOp(intPairs))
    values.map(_.shape) shouldBe Some("Value(Scan)")
    ColumnBinding.chooseCallsForTest should be > 0
    follows(values, Operator.Values, KernelChoice.Column)
  }

  it should "refuse keys of Kind.Pairs when the binder does not tag a column" in {
    val child =
      PhysicalOp.MapKey(PhysicalOp.Project(PhysicalOp.Scan(PhysicalOp.Kind.Triples, Seq.empty)))
    child.kind shouldBe PhysicalOp.Kind.Pairs
    ColumnBinding.resetChooseCallsForTest()
    KeyValueLowering.project(intPairs, 0, _ => Some(child)) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
    asked(child, Operator.Keys) shouldBe None
  }

  it should "keep keys of groupByKey on the count witness" in {
    val lowered = ColumnarPlanner.lower(Plan.KeysOp(Plan.GroupByKeyOp(intPairs)))
    lowered.map(_.shape) shouldBe Some("Key(CountByKey(Exchange(Scan)))")
    ColumnBinding.chooseCallsForTest should be > 0
    follows(lowered, Operator.Keys, KernelChoice.Column)
    keeps(
      Plan.KeysOp(Plan.GroupByKeyOp(longPairs)),
      Operator.Keys,
      KernelChoice.Column,
      "Key(CountByKey(Exchange(Scan)))",
    )
    keeps(
      Plan.KeysOp(Plan.GroupByKeyOp(stringPairs)),
      Operator.Keys,
      KernelChoice.Column,
      "Key(CountByKey(Exchange(Scan)))",
    )
  }

  it should "keep accepted long, string, and mapIntKey projections" in {
    keeps(Plan.KeysOp(longPairs), Operator.Keys, KernelChoice.Column, "Key(Scan)")
    keeps(Plan.ValuesOp(longPairs), Operator.Values, KernelChoice.Column, "Value(Scan)")
    keeps(Plan.KeysOp(stringPairs), Operator.Keys, KernelChoice.Column, "Key(Scan)")
    keeps(Plan.ValuesOp(stringPairs), Operator.Values, KernelChoice.Column, "Value(Scan)")
    keeps(Plan.KeysOp(mapIntKey), Operator.Keys, KernelChoice.Column, "Key(MapKey(Scan))")
    keeps(Plan.ValuesOp(mapIntKey), Operator.Values, KernelChoice.Column, "Value(MapKey(Scan))")
    val slotted = Plan.FilterKeysOp(mapIntKey, (key: Int) => key > 0)
    keeps(Plan.KeysOp(slotted), Operator.Keys, KernelChoice.Column, "Key(FilterKey(MapKey(Scan)))")
    keeps(
      Plan.ValuesOp(slotted),
      Operator.Values,
      KernelChoice.Column,
      "Value(FilterKey(MapKey(Scan)))",
    )
  }

  "a map above groupByKey" should "stay GroupedDecode and not a columnar project" in {
    val mapped = Plan.MapOp(
      Plan.GroupByKeyOp(intPairs),
      (row: (Int, Iterable[Int])) => row._1,
    )
    val lowered = ColumnarPlanner.lower(mapped)
    val root = lowered.getOrElse(fail("map above groupByKey stayed on rows"))
    root match
      case PhysicalOp.GroupedDecode(_: PhysicalOp.GroupByKey) => ()
      case _: PhysicalOp.Project => fail("columnar project over GroupedDecode")
      case other => fail(s"expected GroupedDecode, got $other")
    root.shape shouldBe "GroupedDecode(GroupByKey(Exchange(Scan)))"
    Layout.known(root) shouldBe Layout.Grouped
    ColumnBinding.chooseCallsForTest should be > 0
    ColumnarPlanner.lower(Plan.ValuesOp(Plan.GroupByKeyOp(intPairs))) shouldBe None
  }

  private def keeps(
      plan: Plan[_],
      operator: Operator,
      tag: KernelChoice,
      shape: String,
  ): Unit =
    val lowered = ColumnarPlanner.lower(plan)
    follows(lowered, operator, tag)
    lowered.map(_.shape) shouldBe Some(shape)

  /**
   * The class is the tag [[ColumnBinding.choose]] returns for the child that was asked. `keys` of
   * `groupByKey` asks the exchange under the count, not the count node.
   */
  private def follows(
      lowered: Option[PhysicalOp],
      operator: Operator,
      tag: KernelChoice,
  ): Unit =
    val op = lowered.getOrElse(fail(s"expected ${tag.tag}"))
    opClass(Some(op), tag.tag) shouldBe tag.tag
    asked(askedChild(op), operator) shouldBe Some(tag)

  private def askedChild(op: PhysicalOp): PhysicalOp = op match
    case PhysicalOp.GroupByKey(child) => child
    case PhysicalOp.Dedupe(child) => child
    case PhysicalOp.Column(PhysicalOp.CountByKey(child), _) => child
    case PhysicalOp.Column(child, _) => child
    case other => fail(s"no asked child in ${other.shape}")

  /**
   * Same question the group lowerings ask. An opaque child does not call [[ColumnBinding.choose]].
   */
  private def asked(child: PhysicalOp, operator: Operator): Option[KernelChoice] =
    val bound = Bound.of(child, Vector.empty, ReducerWitness.NotApplicable)
    bound.output match
      case Output.Columns(columns) =>
        ColumnBinding.choose(
          columns,
          operator,
          ReducerWitness.NotApplicable,
          Bound.nulls(bound),
          Bound.side(bound),
        )
      case Output.Opaque(_) => None

  private def opClass(lowered: Option[PhysicalOp], expected: String): String =
    lowered match
      case Some(_: PhysicalOp.GroupByKey) => "GroupByKey"
      case Some(_: PhysicalOp.Dedupe) => "Dedupe"
      case Some(_: PhysicalOp.Column) => "Column"
      case Some(_: PhysicalOp.GroupedDecode) => "GroupedDecode"
      case other => fail(s"expected $expected, got $other")
