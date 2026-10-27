package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{BatchCodec, ColumnBatch, ColumnPipeline, Int32Column, OuterKind}
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Broadcast left outer join against the row-path `leftOuterJoin`. The row executor ignores the
 * broadcast hint, so turning columnar execution off is the oracle.
 */
class TestLeftOuterColumnar extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "broadcast left outer join" should "match the row path for matches and unmatched probe rows" in {
    val cases = Seq(
      "matches" -> (
        Seq(Seq(1 -> 10, 2 -> 20)),
        Seq(Seq(1 -> 100, 2 -> 200)),
      ),
      "unmatched probe rows" -> (
        Seq(Seq(1 -> 10, 2 -> 20, 4 -> 40)),
        Seq(Seq(1 -> 100, 3 -> 30)),
      ),
      "duplicate keys" -> (
        Seq(Seq(1 -> 10, 1 -> 11), Seq(2 -> 20)),
        Seq(Seq(1 -> 100, 1 -> 101, 2 -> 200)),
      ),
      "null keys on both sides" -> (
        Seq(Seq(pair(Some(1), Some(10)), pair(None, Some(4))), Seq(pair(None, Some(5)), pair(Some(2), Some(20)))),
        Seq(Seq(pair(Some(1), Some(100)), pair(None, Some(9))), Seq(pair(None, Some(8)), pair(Some(3), Some(30)))),
      ),
      "null left value" -> (
        Seq(Seq(pair(Some(2), Some(20)), pair(Some(1), None))),
        Seq(Seq(pair(Some(2), Some(200)))),
      ),
    )
    cases.foreach { case (label, (leftParts, rightParts)) =>
      withClue(s"$label: ") {
        // The row-path oracle turns columnar execution off. Restore it before the next lower.
        ScarletConf.set(originalConf.copy(columnarExecution = true))
        val plan = outerPlan(leftParts, rightParts)
        ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
          "BroadcastLeftOuter(Scan, Scan)",
        )
        val columnar = collect(plan, columnar = true)
        val row = collect(plan, columnar = false)
        canonical(columnar) should contain theSameElementsAs canonical(row)
      }
    }
  }

  it should "match the row path when the build or the probe is empty" in {
    val present = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 2)
    val noRows = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 3)
    val emptyBuild = Plan.LeftOuterJoinOp(
      present.plan,
      noRows.plan,
      Some(Plan.JoinStrategy.Broadcast),
    )
    val emptyProbe = Plan.LeftOuterJoinOp(
      noRows.plan,
      present.plan,
      Some(Plan.JoinStrategy.Broadcast),
    )

    ColumnarPlanner.lower(emptyBuild).map(_.shape) shouldBe Some("BroadcastLeftOuter(Scan, Scan)")
    ColumnarPlanner.lower(emptyProbe).map(_.shape) shouldBe Some("BroadcastLeftOuter(Scan, Scan)")
    canonical(collect(emptyBuild, columnar = true)) should contain theSameElementsAs canonical(
      collect(emptyBuild, columnar = false),
    )
    canonical(collect(emptyProbe, columnar = true)) shouldBe empty
    canonical(collect(emptyProbe, columnar = false)) shouldBe empty
  }

  it should "keep a null probe value and report None on the right when the key misses" in {
    // A present pair comes first. Classification reads the first value, and a leading null
    // component stays on the row path.
    val plan = outerPlan(
      Seq(Seq(pair(Some(2), Some(20)), pair(Some(1), None))),
      Seq(Seq(2 -> 200)),
    )
    ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("BroadcastLeftOuter(Scan, Scan)")
    val columnar = canonical(collect(plan, columnar = true))
    columnar should contain theSameElementsAs Seq(
      (Some(1), None, None),
      (Some(2), Some(20), Some(200)),
    )
  }

  it should "record absence in the right validity bit" in {
    val probe = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), Some(10)),
        (Some(2), None),
        (Some(3), Some(30)),
        (Some(4), None),
      ),
    )
    val build = BatchCodec.nullableIntPairs(Seq((Some(1), Some(100)), (Some(4), Some(7))))
    val parts = ColumnPipeline.broadcastLeftOuterJoin(
      Vector(probe),
      Vector(build),
      parallelism = 1,
      ScarletConf.get.broadcastBuildBudgetBytes,
    )
    parts.length shouldBe 1
    val batch = parts.headOption.getOrElse(fail("missing probe batch"))
    val keys = int32(batch, 0)
    val left = int32(batch, 1)
    val right = int32(batch, 2)

    val rows = (0 until batch.length).map { row =>
      val key = if (keys.isValid(row)) Some(keys.values(row)) else None
      (key, left.isValid(row), right.isValid(row))
    }
    rows.find(_._1.contains(1)).getOrElse(fail("missing matched row")) shouldBe
      ((Some(1), true, true))
    rows.find(_._1.contains(2)).getOrElse(fail("missing unmatched null value")) shouldBe
      ((Some(2), false, false))
    rows.find(_._1.contains(3)).getOrElse(fail("missing unmatched probe row")) shouldBe
      ((Some(3), true, false))
    // A null left value on a matched row stays invalid, and the right value stays present.
    rows.find(_._1.contains(4)).getOrElse(fail("missing matched null value")) shouldBe
      ((Some(4), false, true))

    canonical(BatchCodec.decodeOuter(batch, OuterKind.Left)) should contain theSameElementsAs Seq(
      (Some(1), Some(10), Some(100)),
      (Some(2), None, None),
      (Some(3), Some(30), None),
      (Some(4), None, Some(7)),
    )
  }

  it should "swap value columns when the left input is the build side" in {
    val left = DistCollection(Seq(1 -> 10, 1 -> 11), 2)
    val right = DistCollection(Seq(1 -> 100, 1 -> 101), 1)
    val plan = Plan.LeftOuterJoinOp(left.plan, right.plan, Some(Plan.JoinStrategy.Broadcast))
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("broadcast left outer was refused"))
    val (leftOp, rightOp) = lowered match
      case PhysicalOp.BroadcastLeftOuter(leftScan, rightScan, PhysicalOp.BuildSide.Right) =>
        (leftScan, rightScan)
      case other => fail(s"planner did not build the right side: $other")
    val expected = canonical(collect(plan, columnar = true))
    val builtLeft = ColumnarInterpreter.interpret(
      plan,
      PhysicalOp.BroadcastLeftOuter(leftOp, rightOp, PhysicalOp.BuildSide.Left),
    )
    builtLeft match
      case ColumnarInterpreter.Columns.Outer(parts, OuterKind.Left) =>
        canonical(decodeParts(parts)) should contain theSameElementsAs expected
      case other => fail(s"left build did not return an outer batch: $other")
  }

  it should "leave an unhinted, shuffle-hash, or sort-merge left outer on the row path" in {
    val left = DistCollection(Seq(1 -> 10), 1).plan
    val right = DistCollection(Seq(1 -> 20, 2 -> 30), 1).plan
    val hints = Seq(
      None,
      Some(Plan.JoinStrategy.ShuffleHash),
      Some(Plan.JoinStrategy.SortMerge),
    )
    hints.foreach { hint =>
      ColumnarPlanner.lower(Plan.LeftOuterJoinOp(left, right, hint)) shouldBe None
    }
    ColumnarPlanner.lower(
      Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
    ) shouldBe None
    ColumnarPlanner.lower(
      Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
    ) shouldBe None
    an[UnsupportedOperationException] should be thrownBy {
      BatchCodec.decodeOuter(
        BatchCodec.intPairs(Seq((1, 2))),
        OuterKind.Right,
      )
    }
    an[UnsupportedOperationException] should be thrownBy {
      BatchCodec.decodeOuter(
        BatchCodec.intPairs(Seq((1, 2))),
        OuterKind.Full,
      )
    }
  }

  it should "keep a null probe key in the result" in {
    val plan = outerPlan(
      Seq(Seq(pair(Some(1), Some(10)), pair(None, Some(7)))),
      Seq(Seq(pair(Some(1), Some(100)), pair(None, Some(9)))),
    )
    val columnar = canonical(collect(plan, columnar = true))
    columnar.exists(_._1.isEmpty) shouldBe true
    columnar should contain theSameElementsAs canonical(collect(plan, columnar = false))
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def decodeParts(parts: Vector[ColumnBatch]): Seq[Any] =
    parts.flatMap(batch => BatchCodec.decodeOuter(batch, OuterKind.Left))

  private def outerPlan(
      leftParts: Seq[Seq[(Int, Int)]],
      rightParts: Seq[Seq[(Int, Int)]],
  ): Plan[(Int, (Int, Option[Int]))] =
    Plan.LeftOuterJoinOp(source(leftParts), source(rightParts), Some(Plan.JoinStrategy.Broadcast))

  private def source(parts: Seq[Seq[(Int, Int)]]): Plan[(Int, Int)] =
    Plan.Source(parts.map(rows => Partition(rows)))

  private def collect(plan: Plan[_], columnar: Boolean): Seq[Any] =
    ScarletConf.set(originalConf.copy(columnarExecution = columnar))
    ScarletRuntime.get.shuffle.clear()
    DistCollection(plan).collect().toSeq

  /** `(key, left, right)` with a null key or left value as `None`, so a null is not unboxed. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def canonical(rows: Seq[Any]): Seq[(Option[Int], Option[Int], Option[Int])] =
    rows.map { row =>
      val outer = row.asInstanceOf[Product]
      val values = outer.productElement(1).asInstanceOf[Product]
      (
        cell(outer.productElement(0)),
        cell(values.productElement(0)),
        cell(values.productElement(1)),
      )
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null", "org.wartremover.warts.Product"))
  private def cell(value: Any): Option[Int] = value match
    case null => None
    case number: Int => Some(number)
    case Some(number: Int) => Some(number)
    case None => None
    case other => fail(s"expected an int cell, found $other")

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def pair(key: Option[Int], value: Option[Int]): (Int, Int) =
    val boxedKey: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val boxedValue: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(boxedKey, boxedValue).asInstanceOf[(Int, Int)]

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")
