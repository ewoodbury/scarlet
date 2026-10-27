package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
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

  it should "stay on the row path when a build value is null" in {
    // A present pair comes first, so lowering still records the op. The null value is later.
    val plan = outerPlan(
      Seq(Seq(1 -> 10, 2 -> 20)),
      Seq(Seq(pair(Some(2), Some(200)), pair(Some(1), None))),
    )
    ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("BroadcastLeftOuter(Scan, Scan)")
    ColumnarPlanner.tryRun(plan) shouldBe None
    val row = collect(plan, columnar = false)
    nullPartner(row, ordinal = 1) shouldBe true
    sameRows(collect(plan, columnar = true), row)
  }

  it should "stay on the row path when a probe value is null" in {
    val plan = outerPlan(
      Seq(Seq(pair(Some(2), Some(20)), pair(Some(1), None))),
      Seq(Seq(1 -> 100, 2 -> 200)),
    )
    ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("BroadcastLeftOuter(Scan, Scan)")
    ColumnarPlanner.tryRun(plan) shouldBe None
    val row = collect(plan, columnar = false)
    nullPartner(row, ordinal = 0) shouldBe true
    sameRows(collect(plan, columnar = true), row)
  }

  it should "not match a null build key to probe key 0" in {
    val cases = Seq(
      "null build key beside key 0" -> (
        Seq(Seq(1 -> 10, 0 -> 5)),
        Seq(Seq(0 -> 50, pair(None, Some(99)))),
        Seq((Some(1), Some(10), None), (Some(0), Some(5), Some(50))),
      ),
      "null build key and no real key 0" -> (
        Seq(Seq(0 -> 5)),
        Seq(Seq(pair(Some(1), Some(100)), pair(None, Some(99)))),
        Seq((Some(0), Some(5), None)),
      ),
    )
    cases.foreach { case (label, (leftParts, rightParts, expected)) =>
      withClue(s"$label: ") {
        ScarletConf.set(originalConf.copy(columnarExecution = true))
        val plan = outerPlan(leftParts, rightParts)
        ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("BroadcastLeftOuter(Scan, Scan)")
        val columnar = canonical(collect(plan, columnar = true))
        columnar should contain theSameElementsAs expected
        columnar.exists(_._3.contains(99)) shouldBe false
        columnar should contain theSameElementsAs canonical(collect(plan, columnar = false))
      }
    }
  }

  it should "record absence in the right validity bit" in {
    val probe = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), Some(10)),
        (Some(2), Some(11)),
        (Some(3), Some(30)),
        (Some(4), Some(13)),
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
    rows.find(_._1.contains(2)).getOrElse(fail("missing unmatched probe row")) shouldBe
      ((Some(2), true, false))
    rows.find(_._1.contains(3)).getOrElse(fail("missing unmatched probe row")) shouldBe
      ((Some(3), true, false))
    rows.find(_._1.contains(4)).getOrElse(fail("missing matched row")) shouldBe
      ((Some(4), true, true))

    canonical(BatchCodec.decodeOuter(batch)) should contain theSameElementsAs Seq(
      (Some(1), Some(10), Some(100)),
      (Some(2), Some(11), None),
      (Some(3), Some(30), None),
      (Some(4), Some(13), Some(7)),
    )
  }

  it should "refuse a null value before the kernel runs" in {
    val probe = BatchCodec.nullableIntPairs(Seq((Some(1), Some(10)), (Some(2), None)))
    val build = BatchCodec.nullableIntPairs(Seq((Some(1), Some(100))))
    val refused = intercept[IllegalArgumentException] {
      ColumnPipeline.broadcastLeftOuterJoin(
        Vector(probe),
        Vector(build),
        parallelism = 1,
        ScarletConf.get.broadcastBuildBudgetBytes,
      )
    }
    refused.getMessage should include("fully present")
  }

  it should "never record a left build for an outer join" in {
    val left = DistCollection(Seq(1 -> 10), 2).plan
    val right = DistCollection(Seq(1 -> 20, 2 -> 30), 1).plan
    val broadcast = Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast))
    ColumnarPlanner.lower(broadcast) match
      case Some(PhysicalOp.BroadcastLeftOuter(_, _)) => succeed
      case other => fail(s"outer join recorded a build side: $other")
    val refused = Seq(
      Plan.LeftOuterJoinOp(left, right, None),
      Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.ShuffleHash)),
      Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)),
      Plan.RightOuterJoinOp(left, right, None),
      Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
      Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.ShuffleHash)),
      Plan.FullOuterJoinOp(left, right, None),
      Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
      Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)),
    )
    refused.foreach(plan => ColumnarPlanner.lower(plan) shouldBe None)
  }

  it should "decode at most the requested rows of an outer batch" in {
    val probe = BatchCodec.intPairs((0 until 8).map(index => (index, index + 10)))
    val build = BatchCodec.intPairs(Seq((0, 100)))
    val parts = ColumnPipeline.broadcastLeftOuterJoin(
      Vector(probe),
      Vector(build),
      parallelism = 1,
      ScarletConf.get.broadcastBuildBudgetBytes,
    )
    val batch = parts.headOption.getOrElse(fail("missing probe batch"))
    batch.length should be > 3
    ColumnMetrics.withSink(new CountingSink) {
      val decoded = ColumnarInterpreter.decodePrefix(
        ColumnarInterpreter.Columns.Outer(Vector(batch)),
        3,
      )
      decoded.length shouldBe 3
      ColumnMetrics.decodedValueCount shouldBe 3L
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

  private def outerPlan(
      leftParts: Seq[Seq[(Int, Int)]],
      rightParts: Seq[Seq[(Int, Int)]],
  ): Plan[(Int, (Int, Option[Int]))] =
    Plan.LeftOuterJoinOp(source(leftParts), source(rightParts), Some(Plan.JoinStrategy.Broadcast))

  private def source(parts: Seq[Seq[(Int, Int)]]): Plan[(Int, Int)] =
    Plan.Source(parts.map(rows => Partition(rows)))

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def sameRows(actual: Seq[Any], expected: Seq[Any]): org.scalatest.Assertion =
    actual should contain theSameElementsAs expected

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

  /**
   * True when a collected outer row has a null at `ordinal` of the value pair. Ordinal 0 is the
   * probe value. Ordinal 1 is the build value, which the row path wraps in `Some`.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def nullPartner(rows: Seq[Any], ordinal: Int): Boolean =
    rows.exists { row =>
      val values = row.asInstanceOf[Product].productElement(1).asInstanceOf[Product]
      values.productElement(ordinal) match
        case null => true
        case Some(value) =>
          value match
            case null => true
            case _ => false
        case _ => false
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
