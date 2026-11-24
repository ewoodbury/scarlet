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
 * `union` asks [[ColumnBinding]] before it builds.
 *
 * An accepted int-pair union goes through [[ColumnarPlanner.lower]].
 * [[ColumnBinding.chooseCallsForTest]] moves. A lower that refuses a mixed union from
 * [[PhysicalOp.Kind]] and never calls [[ColumnBinding.choose]] leaves that counter at zero.
 * [[Describe.tag]] is not the check. Int pairs union int pairs still lower. Int pairs union long
 * pairs stay [[scala.None]]. The same kind with two column layouts stays [[scala.None]]. A grouped
 * child shares the pair columns and stays [[scala.None]] after the binder runs.
 */
class TestUnionBinding extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  private val intPairs: Plan[(Int, Int)] = Plan.Source(
    Seq(Partition(Seq((1, 10), (2, 20)))),
    Plan.SourceDescriptor.Pairs,
  )

  private val moreIntPairs: Plan[(Int, Int)] = Plan.Source(
    Seq(Partition(Seq((3, 30)))),
    Plan.SourceDescriptor.Pairs,
  )

  private val longPairs: Plan[(Long, Long)] = Plan.Source(
    Seq(Partition(Seq((1L, 10L), (2L, 20L)))),
    Plan.SourceDescriptor.LongPairs,
  )

  private val moreLongPairs: Plan[(Long, Long)] = Plan.Source(
    Seq(Partition(Seq((3L, 30L)))),
    Plan.SourceDescriptor.LongPairs,
  )

  private val ints: Plan[Int] = Plan.Source(
    Seq(Partition(Seq(1, 2))),
    Plan.SourceDescriptor.Ints,
  )

  private val moreInts: Plan[Int] = Plan.Source(
    Seq(Partition(Seq(3))),
    Plan.SourceDescriptor.Ints,
  )

  private val emptyRows: Seq[Partition[_]] = Seq.empty

  private val intScan: PhysicalOp = PhysicalOp.Scan(PhysicalOp.Kind.Pairs, emptyRows)

  private val longScan: PhysicalOp = PhysicalOp.Scan(PhysicalOp.Kind.LongPairs, emptyRows)

  private val triples: PhysicalOp = PhysicalOp.Scan(PhysicalOp.Kind.Triples, emptyRows)

  private val projected: PhysicalOp = PhysicalOp.Project(triples)

  private val exchanged: PhysicalOp = PhysicalOp.Exchange(intScan)

  private val leftOuter: PhysicalOp = PhysicalOp.HashLeftOuter(exchanged, exchanged)

  private val rightOuter: PhysicalOp = PhysicalOp.HashRightOuter(exchanged, exchanged)

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    ColumnBinding.resetChooseCallsForTest()

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ()

  "union" should "ask the binder for int pairs of the same layout" in {
    val lowered = ColumnarPlanner.lower(Plan.UnionOp(intPairs, moreIntPairs))
    opClass(lowered) shouldBe "Union"
    lowered.map(_.shape) shouldBe Some("Union(Scan, Scan)")
    ColumnBinding.chooseCallsForTest should be > 0
    ColumnBinding.choose(
      pair(ColumnType.Int32, ColumnType.Int32),
      Operator.Union,
      ReducerWitness.NotApplicable,
      NullAdmission.Policy(NullPolicy.Nullable),
      Side.Inner,
    ) shouldBe Some(KernelChoice.Union)
  }

  it should "keep a long-pair union and an int-column union" in {
    val longs = ColumnarPlanner.lower(Plan.UnionOp(longPairs, moreLongPairs))
    opClass(longs) shouldBe "Union"
    longs.map(_.shape) shouldBe Some("Union(Scan, Scan)")
    ColumnBinding.chooseCallsForTest should be > 0

    ColumnBinding.resetChooseCallsForTest()
    val columns = ColumnarPlanner.lower(Plan.UnionOp(ints, moreInts))
    opClass(columns) shouldBe "Union"
    columns.map(_.shape) shouldBe Some("Union(Scan, Scan)")
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "ask the binder before refusing int pairs union long pairs" in {
    ColumnBinding.resetChooseCallsForTest()
    UnionLowering.lower(intPairs, longPairs, inOrder(intScan, longScan)) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "refuse the same kind when the column layouts differ" in {
    projected.kind shouldBe triples.kind
    columnsOf(projected) should not be columnsOf(triples)
    ColumnBinding.resetChooseCallsForTest()
    UnionLowering.lower(intPairs, moreIntPairs, inOrder(projected, triples)) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0

    leftOuter.kind shouldBe rightOuter.kind
    absenceOrdinal(leftOuter) shouldBe 2
    absenceOrdinal(rightOuter) shouldBe 1
    ColumnBinding.resetChooseCallsForTest()
    UnionLowering.lower(intPairs, moreIntPairs, inOrder(leftOuter, rightOuter)) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "keep two outer joins that share a layout" in {
    ColumnBinding.resetChooseCallsForTest()
    val loweredUnion =
      UnionLowering.lower(intPairs, moreIntPairs, inOrder(leftOuter, leftOuter))
    opClass(loweredUnion) shouldBe "Union"
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "refuse a grouped batch that shares the pair columns" in {
    val grouped = PhysicalOp.GroupByKey(intScan)
    columnsOf(grouped) shouldBe columnsOf(intScan)
    ColumnBinding.resetChooseCallsForTest()
    UnionLowering.lower(intPairs, moreIntPairs, inOrder(grouped, grouped)) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
  }

  /** [[UnionLowering.lower]] asks the left child, then the right. */
  private def inOrder(leftOp: PhysicalOp, rightOp: PhysicalOp): Plan[_] => Option[PhysicalOp] =
    val ops = Iterator(leftOp, rightOp)
    _ => if ops.hasNext then Some(ops.next()) else None

  private def pair(key: ColumnType, value: ColumnType): Vector[ColumnLayout] =
    Vector(ColumnLayout(key, NullPolicy.Nullable), ColumnLayout(value, NullPolicy.Nullable))

  private def columnsOf(op: PhysicalOp): Vector[ColumnLayout] =
    Layout.known(op) match
      case Output.Columns(columns) => columns
      case Output.Opaque(reason) => fail(reason)

  private def absenceOrdinal(op: PhysicalOp): Int =
    columnsOf(op).indexWhere { column =>
      column.nullPolicy match
        case NullPolicy.Absence => true
        case NullPolicy.Present | NullPolicy.Nullable => false
    }

  private def opClass(loweredOp: Option[PhysicalOp]): String =
    loweredOp match
      case Some(_: PhysicalOp.Union) => "Union"
      case other => fail(s"expected Union, got $other")
