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
 * Filters ask [[ColumnBinding]] before they build.
 *
 * An accepted filter goes through [[ColumnarPlanner.lower]]. The built class has to be the tag
 * [[ColumnBinding.choose]] returns for the child layout. A kind match that also calls [[choose]]
 * and ignores [[scala.None]] still builds [[PhysicalOp.Filter]] for a [[PhysicalOp.Kind.Pairs]]
 * child whose layout is [[Layout.AssumedIntLong]]. [[FilterBinding.slot]] returns [[scala.None]]
 * for that child, and [[ColumnBinding.chooseCallsForTest]] moves. A kind-only lower of an int-pair
 * `filterKeys` leaves the counter at zero.
 *
 * One slot above a triple project whose schema is `[Int32, Int64]` still lowers. That layout is
 * [[Layout.AssumedIntLong]], and [[ColumnBinding.choose]] of a filter on it is [[scala.None]].
 * [[PhysicalSchema.isIntLongProject]] is the fact the layout cannot say. [[Describe.tag]] is not
 * the check. A same-type element map asks in [[TestProjectBinding]].
 */
class TestFilterBinding extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

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
    Seq(Partition(Seq(1, 2, 3))),
    Plan.SourceDescriptor.Ints,
  )

  private val bools: Plan[Boolean] = Plan.Source(
    Seq(Partition(Seq(true, false))),
    Plan.SourceDescriptor.Bools,
  )

  private val rightPairs: Plan[(Int, Int)] = Plan.Source(
    Seq(Partition(Seq((1, 100), (2, 200)))),
    Plan.SourceDescriptor.Pairs,
  )

  private val projected: Plan[(Int, Long)] = Plan.MapOp(
    Plan.JoinOp(intPairs, rightPairs, None),
    (row: (Int, (Int, Int))) => (row._1, row._2._1.toLong),
  )

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    ColumnBinding.resetChooseCallsForTest()

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ()

  "FilterBinding" should "ask the binder for an accepted int filterKeys" in {
    val lowered = lower(Plan.FilterKeysOp(intPairs, (key: Int) => key > 0))
    lowered.map(_.shape) shouldBe Some("FilterKey(Scan)")
    ColumnBinding.chooseCallsForTest should be > 0
    follows(lowered, Operator.FilterKeys)
  }

  it should "ask for filterValues, a whole-row filter, and the long and string slots" in {
    val values = lower(Plan.FilterValuesOp(intPairs, (value: Int) => value > 0))
    ColumnBinding.chooseCallsForTest should be > 0
    follows(values, Operator.FilterValues)
    values.map(_.shape) shouldBe Some("FilterValue(Scan)")
    val row = lower(Plan.FilterOp(intPairs, (pair: (Int, Int)) => pair._2 > 0))
    ColumnBinding.chooseCallsForTest should be > 0
    follows(row, Operator.Filter)
    row.map(_.shape) shouldBe Some("FilterRow(Scan)")
    val longs = lower(Plan.FilterKeysOp(longPairs, (key: Long) => key > 0L))
    ColumnBinding.chooseCallsForTest should be > 0
    follows(longs, Operator.FilterKeys)
    val strings = lower(Plan.FilterKeysOp(stringPairs, (key: String) => key.nonEmpty))
    ColumnBinding.chooseCallsForTest should be > 0
    follows(strings, Operator.FilterKeys)
  }

  it should "ask for an int element filter and a boolean filter" in {
    val intsFiltered = lower(Plan.FilterOp(ints, (value: Int) => value > 1))
    ColumnBinding.chooseCallsForTest should be > 0
    follows(intsFiltered, Operator.Filter)
    intsFiltered.map(_.shape) shouldBe Some("Filter(Scan)")
    val boolsFiltered = lower(Plan.FilterOp(bools, (value: Boolean) => value))
    ColumnBinding.chooseCallsForTest should be > 0
    follows(boolsFiltered, Operator.Filter)
    boolsFiltered.map(_.shape) shouldBe Some("Filter(Scan)")
  }

  it should "not build a filter from Kind.Pairs when the binder returns None" in {
    val triples = PhysicalOp.Scan(PhysicalOp.Kind.Triples, Seq.empty)
    val child = PhysicalOp.MapKey(PhysicalOp.Project(triples))
    child.kind shouldBe PhysicalOp.Kind.Pairs
    Layout.known(child) shouldBe Layout.AssumedIntLong
    PhysicalSchema.isIntLongProject(child) shouldBe false
    ColumnBinding.resetChooseCallsForTest()
    FilterBinding.slot(child, PhysicalOp.Slot.Key) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
    ColumnBinding.asked(child, Operator.FilterKeys) shouldBe None
  }

  it should "keep one slot above an int-long project the binder refuses" in {
    val lowered = lower(Plan.FilterKeysOp(projected, (key: Int) => key > 0))
    lowered.map(_.shape) shouldBe Some(
      "FilterKey(Project(HashJoin(Exchange(Scan), Exchange(Scan))))",
    )
    val child = filterChild(lowered)
    PhysicalSchema.isIntLongProject(child) shouldBe true
    Layout.known(child) shouldBe Layout.AssumedIntLong
    val columns = Layout.AssumedIntLong match
      case Output.Columns(found) => found
      case Output.Opaque(reason) => fail(reason)
    ColumnBinding.choose(
      columns,
      Operator.FilterKeys,
      ReducerWitness.NotApplicable,
      NullAdmission.Policy(NullPolicy.Nullable),
      Side.Inner,
    ) shouldBe None
    lower(Plan.FilterValuesOp(projected, (value: Long) => value >= 0L)).isDefined shouldBe true
  }

  it should "leave filterKeys on an unprojected int join empty" in {
    val joined = Plan.JoinOp(intPairs, rightPairs, None)
    val columns = Vector(
      ColumnLayout(ColumnType.Int32, NullPolicy.Nullable),
      ColumnLayout(ColumnType.Int32, NullPolicy.Nullable),
      ColumnLayout(ColumnType.Int32, NullPolicy.Nullable),
    )
    ColumnBinding.choose(
      columns,
      Operator.FilterKeys,
      ReducerWitness.NotApplicable,
      NullAdmission.Policy(NullPolicy.Nullable),
      Side.Inner,
    ) shouldBe None
    ColumnBinding.choose(
      columns,
      Operator.FilterValues,
      ReducerWitness.NotApplicable,
      NullAdmission.Policy(NullPolicy.Nullable),
      Side.Inner,
    ) shouldBe None
    ColumnBinding.choose(
      columns,
      Operator.Filter,
      ReducerWitness.NotApplicable,
      NullAdmission.Policy(NullPolicy.Nullable),
      Side.Inner,
    ) shouldBe Some(KernelChoice.Filter)
    lower(Plan.FilterKeysOp(joined, (key: Int) => key > 0)) shouldBe None
    val row = lower(Plan.FilterOp(joined, (row: (Int, (Int, Int))) => row._1 > 0))
    ColumnBinding.chooseCallsForTest should be > 0
    follows(row, Operator.Filter)
    row.map(_.shape) shouldBe Some(
      "FilterTripleRow(HashJoin(Exchange(Scan), Exchange(Scan)))",
    )
  }

  /**
   * The class is the tag [[ColumnBinding.choose]] returns for the child the filter wrapped.
   * [[Describe.tag]] is not this comparison.
   */
  private def follows(lowered: Option[PhysicalOp], operator: Operator): Unit =
    className(lowered) shouldBe KernelChoice.Filter.tag
    ColumnBinding.asked(filterChild(lowered), operator) shouldBe Some(KernelChoice.Filter)

  private def lower(plan: Plan[_]): Option[PhysicalOp] =
    ColumnBinding.resetChooseCallsForTest()
    ColumnarPlanner.lower(plan)

  private def filterChild(lowered: Option[PhysicalOp]): PhysicalOp = lowered match
    case Some(PhysicalOp.Filter(child, _)) => child
    case other => fail(s"expected a filter, got $other")

  private def className(lowered: Option[PhysicalOp]): String = lowered match
    case Some(_: PhysicalOp.Filter) => "Filter"
    case other => fail(s"expected Filter, got $other")
