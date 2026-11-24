package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.{ColumnType, KernelChoice}

/**
 * The join class is the key [[ColumnType]] on the two children, plus the strategy hint.
 *
 * [[JoinClass.choose]] with key [[ColumnType.Int64]] and [[PhysicalOp.Kind.Pairs]] is the long
 * class. A choice that reads [[PhysicalOp.Kind]] alone returns the int class. A real long-pair
 * join still lowers to [[PhysicalOp.LongHashJoin]]. A real int-pair join still lowers to
 * [[PhysicalOp.HashJoin]]. An outer child and a null source [[ScanLowering]] refuses stay
 * [[scala.None]]. No new outer or null shape is accepted. [[Describe.tag]] is not the check.
 */
class TestJoinClass extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  private val ints: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 1)

  private val longs: DistCollection[(Long, Long)] =
    DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 1)

  private val strings: Plan[(String, Int)] =
    Plan.Source(Seq(Partition(Seq("b" -> 2, "a" -> 1))))

  private val doubles: Plan[(Double, Int)] =
    Plan.Source(Seq(Partition(Seq((1.0, 1)))))

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    super.afterEach()

  "JoinClass" should "read an Int64 key as the long class when the kind is pairs" in {
    val chosen = JoinClass.choose(
      ColumnType.Int64,
      ColumnType.Int64,
      PhysicalOp.Kind.Pairs,
      None,
      JoinClass.Place.Inner,
    )
    chosen shouldBe Some(KernelChoice.LongHashJoin)
    val child = PhysicalOp.Scan(PhysicalOp.Kind.Pairs, Seq.empty[Partition[_]])
    built(chosen, child) shouldBe "LongHashJoin"
  }

  it should "keep the strategy hint inside the key family" in {
    JoinClass.choose(
      ColumnType.Int64,
      ColumnType.Int64,
      PhysicalOp.Kind.Pairs,
      Some(Plan.JoinStrategy.Broadcast),
      JoinClass.Place.Inner,
    ) shouldBe Some(KernelChoice.LongBroadcastJoin)
    JoinClass.choose(
      ColumnType.Int64,
      ColumnType.Int64,
      PhysicalOp.Kind.Pairs,
      Some(Plan.JoinStrategy.SortMerge),
      JoinClass.Place.Left,
    ) shouldBe Some(KernelChoice.LongSortMergeLeftOuter)
    JoinClass.choose(
      ColumnType.Int64,
      ColumnType.Int64,
      PhysicalOp.Kind.Pairs,
      Some(Plan.JoinStrategy.ShuffleHash),
      JoinClass.Place.Inner,
    ) shouldBe Some(KernelChoice.LongHashJoin)
  }

  it should "read an Int32 key as the int class and a string key as the string class" in {
    JoinClass.choose(
      ColumnType.Int32,
      ColumnType.Int32,
      PhysicalOp.Kind.LongPairs,
      None,
      JoinClass.Place.Inner,
    ) shouldBe Some(KernelChoice.HashJoin)
    JoinClass.choose(
      ColumnType.Utf8Dict,
      ColumnType.Utf8Dict,
      PhysicalOp.Kind.Pairs,
      None,
      JoinClass.Place.Inner,
    ) shouldBe Some(KernelChoice.StringHashJoin)
  }

  it should "refuse disagreeing keys, a non-pair kind, and a key with no join class" in {
    JoinClass.choose(
      ColumnType.Int32,
      ColumnType.Int64,
      PhysicalOp.Kind.Pairs,
      None,
      JoinClass.Place.Inner,
    ) shouldBe Option.empty[KernelChoice]
    JoinClass.choose(
      ColumnType.Int64,
      ColumnType.Int64,
      PhysicalOp.Kind.LongOuter,
      None,
      JoinClass.Place.Inner,
    ) shouldBe Option.empty[KernelChoice]
    JoinClass.choose(
      ColumnType.Int32,
      ColumnType.Int32,
      PhysicalOp.Kind.Triples,
      None,
      JoinClass.Place.Inner,
    ) shouldBe Option.empty[KernelChoice]
    JoinClass.choose(
      ColumnType.Float64,
      ColumnType.Float64,
      PhysicalOp.Kind.Pairs,
      None,
      JoinClass.Place.Inner,
    ) shouldBe Option.empty[KernelChoice]
  }

  "a real join" should "lower int pairs to the int class and long pairs to the long class" in {
    shape(ints.join(ints).plan, "HashJoin(Exchange(Scan), Exchange(Scan))")
    shape(longs.join(longs).plan, "LongHashJoin(Exchange(Scan), Exchange(Scan))")
    shape(
      Plan.JoinOp(strings, strings, None),
      "StringHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    shape(longs.broadcastJoin(longs).plan, "LongBroadcastJoin(Scan, Scan)")
    shape(ints.sortMergeJoin(ints).plan, "SortMergeJoin(Exchange(Scan), Exchange(Scan))")
  }

  it should "keep the existing outer classes" in {
    shape(ints.leftOuterJoin(ints).plan, "HashLeftOuter(Exchange(Scan), Exchange(Scan))")
    shape(longs.leftOuterJoin(longs).plan, "LongHashLeftOuter(Exchange(Scan), Exchange(Scan))")
    shape(
      Plan.LeftOuterJoinOp(strings, strings, None),
      "StringHashLeftOuter(Exchange(Scan), Exchange(Scan))",
    )
    shape(
      Plan.LeftOuterJoinOp(ints.plan, ints.plan, Some(Plan.JoinStrategy.Broadcast)),
      "BroadcastLeftOuter(Scan, Scan)",
    )
  }

  it should "leave an outer child and a null or double key refused" in {
    val outer = ints.leftOuterJoin(ints).plan
    ColumnarPlanner.lower(Plan.JoinOp(outer, outer, None)) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.lower(Plan.LeftOuterJoinOp(outer, outer, None)) shouldBe
      Option.empty[PhysicalOp]
    val refused = Plan.Source(Seq(Partition(Seq(nullPair))))
    ColumnarPlanner.lower(Plan.JoinOp(refused, ints.plan, None)) shouldBe
      Option.empty[PhysicalOp]
    ColumnarPlanner.lower(Plan.LeftOuterJoinOp(refused, ints.plan, None)) shouldBe
      Option.empty[PhysicalOp]
    ColumnarPlanner.lower(Plan.JoinOp(doubles, doubles, None)) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.lower(Plan.LeftOuterJoinOp(doubles, doubles, None)) shouldBe
      Option.empty[PhysicalOp]
  }

  private def shape(plan: Plan[_], expected: String): Unit =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail(s"$expected did not lower"))
    lowered.shape shouldBe expected

  private def built(choice: Option[KernelChoice], child: PhysicalOp): String =
    val op = choice
      .flatMap(tag => JoinClass.build(tag, child, child))
      .getOrElse(fail(s"no node for $choice"))
    op match
      case _: PhysicalOp.LongHashJoin => "LongHashJoin"
      case _: PhysicalOp.HashJoin => "HashJoin"
      case other => fail(s"expected a hash join class, got $other")

  /**
   * Named erasure boundary. `Tuple2` keeps a null `Integer`. `(Int, Int)` would unbox it, so the
   * source would not be the null key [[ScanLowering]] refuses.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullPair: (Int, Int) =
    Tuple2(null, Int.box(1)).asInstanceOf[(Int, Int)]
