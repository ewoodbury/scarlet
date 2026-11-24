package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.*

/**
 * `groupByKey`, `distinct`, `keys`, and `values` ask [[ColumnBinding]] before they build.
 *
 * An accepted int `groupByKey` goes through [[ColumnarPlanner.lower]].
 * [[ColumnBinding.chooseCallsForTest]] moves. A lower that still decides from
 * [[PhysicalOp.Kind]] alone leaves that counter at zero. [[Describe.tag]] is not the check.
 * A map above the group stays [[PhysicalOp.GroupedDecode]]. It is not a columnar project over
 * that decode. Accepted plans keep their [[PhysicalOp]] class.
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
    opClass(lowered, "GroupByKey") shouldBe "GroupByKey"
    lowered.map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "keep the accepted long and string group classes" in {
    keeps(Plan.GroupByKeyOp(longPairs), "GroupByKey", "GroupByKey(Exchange(Scan))")
    keeps(Plan.GroupByKeyOp(stringPairs), "GroupByKey", "GroupByKey(Exchange(Scan))")
  }

  it should "ask before refusing a column the group does not run" in {
    val rows = Seq[Partition[_]](Partition(Seq(1)))
    val column = PhysicalOp.Scan(PhysicalOp.Kind.Ints, rows)
    GroupByKeyLowering.apply(column) shouldBe None
    ColumnBinding.chooseCallsForTest should be > 0
  }

  "distinct" should "ask the binder for an accepted int column" in {
    val lowered = ColumnarPlanner.lower(Plan.DistinctOp(ints))
    opClass(lowered, "Dedupe") shouldBe "Dedupe"
    lowered.map(_.shape) shouldBe Some("Dedupe(Exchange(Scan))")
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "keep an accepted string distinct" in {
    keeps(Plan.DistinctOp(strings), "Dedupe", "Dedupe(Exchange(Scan))")
  }

  "keys and values" should "ask the binder and keep Column" in {
    val keys = ColumnarPlanner.lower(Plan.KeysOp(intPairs))
    opClass(keys, "Column") shouldBe "Column"
    keys.map(_.shape) shouldBe Some("Key(Scan)")
    ColumnBinding.chooseCallsForTest should be > 0

    ColumnBinding.resetChooseCallsForTest()
    val values = ColumnarPlanner.lower(Plan.ValuesOp(intPairs))
    opClass(values, "Column") shouldBe "Column"
    values.map(_.shape) shouldBe Some("Value(Scan)")
    ColumnBinding.chooseCallsForTest should be > 0
  }

  it should "keep keys of groupByKey on the count witness" in {
    val lowered = ColumnarPlanner.lower(Plan.KeysOp(Plan.GroupByKeyOp(intPairs)))
    opClass(lowered, "Column") shouldBe "Column"
    lowered.map(_.shape) shouldBe Some("Key(CountByKey(Exchange(Scan)))")
    ColumnBinding.chooseCallsForTest should be > 0
    keeps(
      Plan.KeysOp(Plan.GroupByKeyOp(longPairs)),
      "Column",
      "Key(CountByKey(Exchange(Scan)))",
    )
    keeps(
      Plan.KeysOp(Plan.GroupByKeyOp(stringPairs)),
      "Column",
      "Key(CountByKey(Exchange(Scan)))",
    )
  }

  it should "keep accepted long, string, and mapIntKey projections" in {
    keeps(Plan.KeysOp(longPairs), "Column", "Key(Scan)")
    keeps(Plan.ValuesOp(longPairs), "Column", "Value(Scan)")
    keeps(Plan.KeysOp(stringPairs), "Column", "Key(Scan)")
    keeps(Plan.ValuesOp(stringPairs), "Column", "Value(Scan)")
    keeps(Plan.KeysOp(mapIntKey), "Column", "Key(MapKey(Scan))")
    keeps(Plan.ValuesOp(mapIntKey), "Column", "Value(MapKey(Scan))")
    val slotted = Plan.FilterKeysOp(mapIntKey, (key: Int) => key > 0)
    keeps(Plan.KeysOp(slotted), "Column", "Key(FilterKey(MapKey(Scan)))")
    keeps(Plan.ValuesOp(slotted), "Column", "Value(FilterKey(MapKey(Scan)))")
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
    Layout.known(root) shouldBe Output.Opaque("grouped values")
    ColumnBinding.chooseCallsForTest should be > 0
    ColumnarPlanner.lower(Plan.ValuesOp(Plan.GroupByKeyOp(intPairs))) shouldBe None
  }

  private def keeps(plan: Plan[_], name: String, shape: String): Unit =
    val lowered = ColumnarPlanner.lower(plan)
    opClass(lowered, name) shouldBe name
    lowered.map(_.shape) shouldBe Some(shape)

  private def opClass(lowered: Option[PhysicalOp], expected: String): String =
    lowered match
      case Some(_: PhysicalOp.GroupByKey) => "GroupByKey"
      case Some(_: PhysicalOp.Dedupe) => "Dedupe"
      case Some(_: PhysicalOp.Column) => "Column"
      case Some(_: PhysicalOp.GroupedDecode) => "GroupedDecode"
      case other => fail(s"expected $expected, got $other")
