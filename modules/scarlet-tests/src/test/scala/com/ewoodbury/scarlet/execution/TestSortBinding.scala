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
 * Typed sorts ask [[ColumnBinding]] before they build.
 *
 * An accepted int sort goes through [[ColumnarPlanner.lower]]. [[ColumnBinding.chooseCallsForTest]]
 * moves. A lower that still decides from [[PhysicalOp.Kind]] alone leaves that counter at zero.
 * [[Describe.tag]] is not the check. A layout today's sort refuses is [[scala.None]] from the
 * binder, and [[SortLowering]] returns [[scala.None]] with it. Accepted sorts keep their
 * [[PhysicalOp]] class.
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
    sortClass(lowered, "SortByIntKey")
    lowered.map(_.shape) shouldBe Some("SortByIntKey(Scan)")
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "keep the accepted long, string, and value-desc classes" in {
    sortClass(ColumnarPlanner.lower(Plan.SortByLongKeyOp(longPairs)), "SortByLongKey")
    sortClass(ColumnarPlanner.lower(Plan.SortByStringKeyOp(stringPairs)), "SortByStringKey")
    sortClass(ColumnarPlanner.lower(Plan.SortByIntValueDescOp(intPairs)), "SortByIntValueDesc")
  }

  it should "return None for a layout the int sort refuses" in {
    refuse(pair(ColumnType.Int64, ColumnType.Int64))
    val assumed = Layout.AssumedIntLong match
      case Output.Columns(columns) => columns
      case Output.Opaque(reason) => fail(reason)
    refuse(assumed)

    ColumnBinding.resetChooseCallsForTest()
    val rows = Seq[Partition[_]](Partition(Seq(1L -> 2L)))
    val longScan = PhysicalOp.Scan(PhysicalOp.Kind.LongPairs, rows)
    SortLowering.lowerInt(intPairs, _ => Some(longScan)) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "keep mapValues off an int key sort and on the value sort" in {
    val mapped = Plan.MapValuesOp(intPairs, (value: Int) => value + 1)
    ColumnarPlanner.lower(Plan.SortByIntKeyOp(mapped)) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
    val valueSort = ColumnarPlanner.lower(Plan.SortByIntValueDescOp(mapped))
    sortClass(valueSort, "SortByIntValueDesc")
    valueSort.map(_.shape) shouldBe Some("SortByIntValueDesc(MapValue(Scan))")
  }

  private def refuse(layout: Vector[ColumnLayout]): Unit =
    ColumnBinding.choose(
      layout,
      Operator.SortByIntKey,
      ReducerWitness.NotApplicable,
      NullAdmission.Policy(NullPolicy.Nullable),
      Side.Inner,
    ) shouldBe None

  private def pair(key: ColumnType, value: ColumnType): Vector[ColumnLayout] =
    Vector(ColumnLayout(key, NullPolicy.Nullable), ColumnLayout(value, NullPolicy.Nullable))

  private def sortClass(lowered: Option[PhysicalOp], name: String): Unit =
    val found = lowered match
      case Some(_: PhysicalOp.SortByIntKey) => "SortByIntKey"
      case Some(_: PhysicalOp.SortByIntValueDesc) => "SortByIntValueDesc"
      case Some(_: PhysicalOp.SortByLongKey) => "SortByLongKey"
      case Some(_: PhysicalOp.SortByStringKey) => "SortByStringKey"
      case other => fail(s"expected $name, got $other")
    found shouldBe name
