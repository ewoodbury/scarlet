package com.ewoodbury.scarlet.execution

import java.nio.file.Path

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

import SortMergeOuterFixture.{canonical, collect}

/**
 * Right outer join against the row-path `rightOuterJoin`. The row executor ignores the strategy
 * hint and always shuffle-hashes, so turning columnar execution off is the oracle. The preserved
 * side is the right input.
 */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class TestRightOuterColumnar extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    SortMergeRuns.resetForTest()
    ()

  "broadcast right outer join" should "match the row path for matches and unmatched right rows" in {
    val cases = Seq(
      "matches" -> (
        Seq(Seq(1 -> 10, 2 -> 20)),
        Seq(Seq(1 -> 100, 2 -> 200)),
      ),
      "unmatched right rows" -> (
        Seq(Seq(1 -> 10, 3 -> 30)),
        Seq(Seq(1 -> 100, 2 -> 20, 4 -> 40)),
      ),
      "duplicate keys" -> (
        Seq(Seq(1 -> 10, 1 -> 11), Seq(2 -> 20)),
        Seq(Seq(1 -> 100, 1 -> 101, 2 -> 200)),
      ),
      "null right key kept with no left partner" -> (
        Seq(Seq(pair(Some(1), Some(10)))),
        Seq(Seq(pair(Some(1), Some(100)), pair(None, Some(7)))),
      ),
      "null left key never matches" -> (
        Seq(Seq(pair(Some(0), Some(5)), pair(None, Some(99)))),
        Seq(Seq(pair(Some(0), Some(50)), pair(Some(1), Some(100)))),
      ),
    )
    cases.foreach { case (label, (leftParts, rightParts)) =>
      withClue(s"$label: ") {
        ScarletConf.set(originalConf.copy(columnarExecution = true))
        val plan = broadcastPlan(leftParts, rightParts)
        ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
          "BroadcastRightOuter(Scan, Scan)",
        )
        val columnar = canonical(collect(plan, columnar = true))
        val row = canonical(collect(plan, columnar = false))
        columnar should contain theSameElementsAs row
        if (label.contains("null right")) then
          columnar.exists(row => row._1.isEmpty && row._2.isEmpty && row._3.contains(7)) shouldBe true
        if (label.contains("null left")) then
          columnar.exists(_._3.contains(99)) shouldBe false
          columnar.exists { row =>
            row._1.contains(0) && row._2.contains(5) && row._3.contains(50)
          } shouldBe true
      }
    }
  }

  it should "match the row path when either side is empty" in {
    val present = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 2)
    val noRows = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 3)
    val emptyBuild = Plan.RightOuterJoinOp(
      noRows.plan,
      present.plan,
      Some(Plan.JoinStrategy.Broadcast),
    )
    val emptyProbe = Plan.RightOuterJoinOp(
      present.plan,
      noRows.plan,
      Some(Plan.JoinStrategy.Broadcast),
    )

    ColumnarPlanner.lower(emptyBuild).map(_.shape) shouldBe Some("BroadcastRightOuter(Scan, Scan)")
    ColumnarPlanner.lower(emptyProbe).map(_.shape) shouldBe Some("BroadcastRightOuter(Scan, Scan)")
    canonical(collect(emptyBuild, columnar = true)) should contain theSameElementsAs canonical(
      collect(emptyBuild, columnar = false),
    )
    canonical(collect(emptyProbe, columnar = true)) shouldBe empty
    canonical(collect(emptyProbe, columnar = false)) shouldBe empty
  }

  it should "follow the right input's partition count" in {
    val left = DistCollection.ofIntPairs(Array(1), Array(10), 2).plan
    val right = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 5).plan
    val plan = Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast))
    ColumnarPlanner.partitioning(plan).map(_.distribution) shouldBe Some(Distribution.Unknown(5))
  }

  it should "stay on the row path when a value is null" in {
    val nullLeft = broadcastPlan(
      Seq(Seq(pair(Some(2), Some(20)), pair(Some(1), None))),
      Seq(Seq(1 -> 100, 2 -> 200)),
    )
    val nullRight = broadcastPlan(
      Seq(Seq(1 -> 10, 2 -> 20)),
      Seq(Seq(pair(Some(2), Some(200)), pair(Some(1), None))),
    )
    Seq(nullLeft, nullRight).foreach { plan =>
      ScarletConf.set(originalConf.copy(columnarExecution = true))
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("BroadcastRightOuter(Scan, Scan)")
      ColumnarPlanner.tryRun(plan) shouldBe None
      sameRows(collect(plan, columnar = true), collect(plan, columnar = false))
    }
  }

  it should "record absence in the left validity bit" in {
    val plan = broadcastPlan(
      Seq(Seq(1 -> 100, 4 -> 7)),
      Seq(Seq(1 -> 10, 2 -> 11, 3 -> 30, 4 -> 13)),
    )
    val (parts, absent) = published(plan)
    absent shouldBe AbsentSide.Left
    parts.length shouldBe 1
    val batch = parts.headOption.getOrElse(fail("missing probe batch"))
    val rows = validityRows(batch).map { row => (row.key, row.leftValid, row.rightValid) }
    rows.find(_._1.contains(1)).getOrElse(fail("missing matched row")) shouldBe
      ((Some(1), true, true))
    rows.find(_._1.contains(2)).getOrElse(fail("missing unmatched right row")) shouldBe
      ((Some(2), false, true))
    rows.find(_._1.contains(3)).getOrElse(fail("missing unmatched right row")) shouldBe
      ((Some(3), false, true))
    rows.find(_._1.contains(4)).getOrElse(fail("missing matched row")) shouldBe
      ((Some(4), true, true))
    rows.exists(!_._3) shouldBe false
  }

  "shuffle-hash right outer join" should "match the row path across four partitions" in {
    val cases = Seq(
      "matches" -> (
        Seq(Seq(1 -> 10), Seq(2 -> 20), Seq(3 -> 30), Seq(4 -> 40)),
        Seq(Seq(1 -> 100, 2 -> 200), Seq.empty[(Int, Int)], Seq(3 -> 300), Seq(4 -> 400)),
      ),
      "unmatched right rows" -> (
        Seq(Seq(1 -> 10), Seq.empty[(Int, Int)], Seq(3 -> 30), Seq.empty[(Int, Int)]),
        Seq(Seq(1 -> 100), Seq(2 -> 20), Seq(4 -> 40), Seq(6 -> 60)),
      ),
      "duplicate keys" -> (
        Seq(Seq(1 -> 10, 1 -> 11), Seq.empty[(Int, Int)], Seq(2 -> 20), Seq(2 -> 21)),
        Seq(Seq(1 -> 100, 1 -> 101), Seq(2 -> 200), Seq.empty[(Int, Int)], Seq(1 -> 102)),
      ),
      "null right key kept with no left partner" -> (
        Seq(
          Seq(pair(Some(1), Some(10))),
          Seq(pair(None, Some(99))),
          Seq(pair(Some(5), Some(50))),
          Seq.empty[(Int, Int)],
        ),
        Seq(
          Seq(pair(Some(1), Some(100))),
          Seq(pair(None, Some(7))),
          Seq(pair(Some(3), Some(30))),
          Seq(pair(Some(1), Some(101))),
        ),
      ),
      "null left key never matches" -> (
        Seq(
          Seq(pair(Some(0), Some(5))),
          Seq(pair(None, Some(99))),
          Seq.empty[(Int, Int)],
          Seq(pair(Some(2), Some(20))),
        ),
        Seq(
          Seq(pair(Some(0), Some(50))),
          Seq(pair(Some(1), Some(100))),
          Seq.empty[(Int, Int)],
          Seq(pair(Some(2), Some(200))),
        ),
      ),
    )
    val hints = Seq(
      "shuffle-hash" -> Some(Plan.JoinStrategy.ShuffleHash),
      "absent hint" -> Option.empty[Plan.JoinStrategy],
    )
    hints.foreach { case (hint, strategy) =>
      cases.foreach { case (label, (leftParts, rightParts)) =>
        withClue(s"$hint $label: ") {
          ScarletConf.set(shuffledConf)
          leftParts.length should be >= 4
          rightParts.length should be >= 4
          val plan = Plan.RightOuterJoinOp(source(leftParts), source(rightParts), strategy)
          ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
            "HashRightOuter(Exchange(Scan), Exchange(Scan))",
          )
          val columnar = canonical(collect(plan, columnar = true))
          val row = canonical(collect(plan, columnar = false))
          columnar should contain theSameElementsAs row
          if (label.contains("null right")) then
            columnar.exists(row => row._1.isEmpty && row._2.isEmpty && row._3.contains(7)) shouldBe true
            columnar.exists(_._2.contains(99)) shouldBe false
          if (label.contains("null left")) then
            columnar.exists(_._3.contains(99)) shouldBe false
            columnar.exists { row =>
              row._1.contains(0) && row._2.contains(5) && row._3.contains(50)
            } shouldBe true
        }
      }
    }
  }

  it should "match the row path when either side is empty" in {
    val present = DistCollection.ofIntPairs(Array(1, 2, 3, 4), Array(10, 20, 30, 40), 4)
    val noRows = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 4)
    val emptyBuild = Plan.RightOuterJoinOp(
      noRows.plan,
      present.plan,
      Some(Plan.JoinStrategy.ShuffleHash),
    )
    val emptyProbe = Plan.RightOuterJoinOp(
      present.plan,
      noRows.plan,
      Some(Plan.JoinStrategy.ShuffleHash),
    )

    ScarletConf.set(shuffledConf)
    ColumnarPlanner.lower(emptyBuild).map(_.shape) shouldBe Some(
      "HashRightOuter(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(emptyProbe).map(_.shape) shouldBe Some(
      "HashRightOuter(Exchange(Scan), Exchange(Scan))",
    )
    canonical(collect(emptyBuild, columnar = true)) should contain theSameElementsAs canonical(
      collect(emptyBuild, columnar = false),
    )
    canonical(collect(emptyProbe, columnar = true)) shouldBe empty
    canonical(collect(emptyProbe, columnar = false)) shouldBe empty
  }

  it should "stay on the row path when a shuffled value is null" in {
    val nullLeft = shufflePlan(
      Seq(
        Seq(pair(Some(2), Some(20))),
        Seq(pair(Some(1), None)),
        Seq.empty[(Int, Int)],
        Seq.empty[(Int, Int)],
      ),
      Seq(Seq(1 -> 100), Seq(2 -> 200), Seq.empty[(Int, Int)], Seq.empty[(Int, Int)]),
    )
    val nullRight = shufflePlan(
      Seq(Seq(1 -> 10), Seq(2 -> 20), Seq.empty[(Int, Int)], Seq.empty[(Int, Int)]),
      Seq(
        Seq(pair(Some(2), Some(200))),
        Seq(pair(Some(1), None)),
        Seq.empty[(Int, Int)],
        Seq.empty[(Int, Int)],
      ),
    )
    Seq(nullLeft, nullRight).foreach { plan =>
      ScarletConf.set(shuffledConf)
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
        "HashRightOuter(Exchange(Scan), Exchange(Scan))",
      )
      ColumnarPlanner.tryRun(plan) shouldBe None
      sameRows(collect(plan, columnar = true), collect(plan, columnar = false))
    }
  }

  it should "record absence in the left validity bit after the exchange" in {
    ScarletConf.set(shuffledConf)
    val plan = shufflePlan(
      Seq(Seq((1, 100), (8, 80)), Seq(pair(None, Some(99)))),
      Seq(
        Seq((1, 10), (2, 20)),
        Seq(pair(None, Some(7)), (4, 40)),
      ),
    )
    val (parts, absent) = published(plan)
    absent shouldBe AbsentSide.Left
    parts.length should be >= 4
    val rows = parts.zipWithIndex.flatMap { (batch, part) =>
      validityRows(batch).map { row => (part, row.key, row.leftValid, row.rightValid) }
    }
    rows.find(_._2.contains(1)).getOrElse(fail("missing matched row")) shouldBe
      ((Math.floorMod(1, parts.length), Some(1), true, true))
    rows.find(_._2.contains(2)).getOrElse(fail("missing unmatched right row")) shouldBe
      ((Math.floorMod(2, parts.length), Some(2), false, true))
    rows.find(_._2.contains(4)).getOrElse(fail("missing unmatched key 4")) shouldBe
      ((0, Some(4), false, true))
    rows.find(_._2.isEmpty).getOrElse(fail("missing null right key")) shouldBe
      ((0, None, false, true))
    rows.exists(_._2.contains(8)) shouldBe false
    rows.exists(!_._4) shouldBe false
    val decoded = parts.flatMap { batch =>
      canonical(BatchCodec.decodeOuter(batch, AbsentSide.Left))
    }
    decoded.exists(_._2.contains(99)) shouldBe false
    decoded.exists(_._3.contains(99)) shouldBe false
    val unmatched = parts.lift(Math.floorMod(2, parts.length)).getOrElse(fail("missing partition"))
    canonical(BatchCodec.decodeOuter(unmatched, AbsentSide.Left)) should contain theSameElementsAs Seq(
      (Some(2), None, Some(20)),
    )
  }

  it should "decode either absent side from one three-column layout" in {
    val batch = outerBatch(
      keys = Array(1, 2, 3),
      left = Array(10, 20, 0),
      right = Array(100, 0, 30),
      leftPresent = Array(true, true, false),
      rightPresent = Array(true, false, true),
    )
    assertDecodeShapes(batch)
  }

  it should "lower sort-merge to its own node" in {
    val left = DistCollection(Seq(1 -> 10), 2).plan
    val right = DistCollection(Seq(1 -> 20, 2 -> 30), 1).plan
    ColumnarPlanner.lower(
      Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
    ) match
      case Some(PhysicalOp.BroadcastRightOuter(_, _)) => succeed
      case other => fail(s"broadcast right outer did not lower: $other")
    val shuffleShape = "HashRightOuter(Exchange(Scan), Exchange(Scan))"
    ColumnarPlanner.lower(
      Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.ShuffleHash)),
    ) match
      case Some(PhysicalOp.HashRightOuter(PhysicalOp.Exchange(_), PhysicalOp.Exchange(_))) =>
        succeed
      case other => fail(s"shuffle right outer did not exchange both sides: $other")
    ColumnarPlanner
      .lower(Plan.RightOuterJoinOp(left, right, None))
      .map(_.shape) shouldBe Some(shuffleShape)
    ColumnarPlanner
      .lower(Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)))
      .map(_.shape) shouldBe Some("SortMergeRightOuter(Exchange(Scan), Exchange(Scan))")
    ColumnarPlanner
      .lower(Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)))
      .map(_.shape) shouldBe Some("SortMergeFullOuter(Exchange(Scan), Exchange(Scan))")
  }

  "sort-merge right outer join" should "match the row path in memory and over spilled runs" in {
    val plan = Plan.RightOuterJoinOp(
      source(Seq(sortMergeLeftRows)),
      source(Seq(sortMergeRightRows)),
      Some(Plan.JoinStrategy.SortMerge),
    )
    val budget = SortMergeRuns.retainedBytes(2) + SortMergeRuns.retainedBytes(1)
    SortMergeRuns.retainedBytes(4) + SortMergeRuns.retainedBytes(3) should be > budget

    ScarletConf.set(shuffledConf)
    SortMergeRuns.resetForTest()
    val lowered =
      ColumnarPlanner.lower(plan).getOrElse(fail("sort-merge right outer did not lower"))
    lowered.kind shouldBe PhysicalOp.Kind.Triples
    lowered.shape shouldBe "SortMergeRightOuter(Exchange(Scan), Exchange(Scan))"
    val memory = canonical(collect(plan, columnar = true))
    val row = canonical(collect(plan, columnar = false))
    memory should contain theSameElementsAs row
    assertRightShape(memory)
    ScarletConf.set(shuffledConf)
    val fromBits = assertOuterBits(plan, AbsentSide.Left, memory)
    assertRightShape(fromBits)
    SortMergeRuns.filesWrittenForTest shouldBe 0L

    val spilled = withSpill(budget) { dir =>
      val rows = canonical(collectCurrent(plan))
      assertOuterBits(plan, AbsentSide.Left, rows)
      // 96/32 covers two executions (collectCurrent and assertOuterBits's interpret). 20/8 and 2/0 are single executions.
      SortMergeOuterFixture.assertSpill(dir, 96L, 32L)
      rows
    }
    spilled should contain theSameElementsAs memory
  }

  it should "emit only the preserved side when a side is empty" in {
    val noRows = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 4).plan
    val left = source(Seq(spreadKeys(100, 7)))
    val right = source(Seq(spreadKeys(500, 9)))
    val budget = SortMergeRuns.retainedBytes(1)
    SortMergeRuns.retainedBytes(3) should be > budget
    val emptyLeft = Plan.RightOuterJoinOp(noRows, right, Some(Plan.JoinStrategy.SortMerge))
    val emptyRight = Plan.RightOuterJoinOp(left, noRows, Some(Plan.JoinStrategy.SortMerge))
    val both = Plan.RightOuterJoinOp(noRows, noRows, Some(Plan.JoinStrategy.SortMerge))

    ScarletConf.set(shuffledConf)
    val kept = canonical(collect(emptyLeft, columnar = true))
    kept should contain theSameElementsAs canonical(collect(emptyLeft, columnar = false))
    kept.count(_._1.isEmpty) shouldBe 1
    kept.forall(_._2.isEmpty) shouldBe true
    canonical(collect(emptyRight, columnar = true)).isEmpty shouldBe true
    canonical(collect(both, columnar = true)).isEmpty shouldBe true

    val spilledKept = withSpill(budget) { dir =>
      val rows = canonical(collectCurrent(emptyLeft))
      assertOuterBits(emptyLeft, AbsentSide.Left, rows)
      // 40/16 covers two executions (collectCurrent and assertOuterBits's interpret). 20/8 and 2/0 are single executions.
      SortMergeOuterFixture.assertSpill(dir, 40L, 16L)
      rows
    }
    spilledKept should contain theSameElementsAs kept

    val spilledEmptyRight = withSpill(budget) { dir =>
      val rows = canonical(collectCurrent(emptyRight))
      SortMergeOuterFixture.assertSpill(dir, 20L, 8L)
      rows
    }
    spilledEmptyRight.isEmpty shouldBe true

    val spilledBoth = withSpill(budget) { dir =>
      val rows = canonical(collectCurrent(both))
      SortMergeRuns.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      assertOuterBits(both, AbsentSide.Left, Seq.empty)
      rows
    }
    spilledBoth.isEmpty shouldBe true
  }

  it should "keep a null value on the row path" in {
    val left = source(Seq(Seq(1 -> 10, pair(Some(2), None))))
    val right = source(Seq(Seq(1 -> 100)))
    val plan = Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge))
    ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
      "SortMergeRightOuter(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.tryRun(plan) shouldBe None
    canonical(collect(plan, columnar = true)) should contain theSameElementsAs canonical(
      collect(plan, columnar = false),
    )
    // The plan's null value is on the left. The direct call uses that same side.
    val leftBatch = BatchCodec.nullableIntPairs(Seq((Some(1), Some(10)), (Some(2), None)))
    val rightBatch = BatchCodec.nullableIntPairs(Seq((Some(1), Some(100))))
    val refused = intercept[IllegalArgumentException] {
      ColumnPipeline.sortMergeRightOuterJoin(
        Vector(leftBatch),
        Vector(rightBatch),
        numPartitions = 1,
        parallelism = 1,
        batchSize = 8,
      )
    }
    refused.getMessage should include("fully present")
    val kernel = intercept[IllegalArgumentException] {
      SortMerge.rightOuter(leftBatch, rightBatch)
    }
    kernel.getMessage should include("fully present")
  }

  private def shuffledConf: ScarletConf =
    originalConf.copy(columnarExecution = true, defaultShufflePartitions = 4)

  private def broadcastPlan(
      leftParts: Seq[Seq[(Int, Int)]],
      rightParts: Seq[Seq[(Int, Int)]],
  ): Plan[(Int, (Option[Int], Int))] =
    Plan.RightOuterJoinOp(
      source(leftParts),
      source(rightParts),
      Some(Plan.JoinStrategy.Broadcast),
    )

  private def shufflePlan(
      leftParts: Seq[Seq[(Int, Int)]],
      rightParts: Seq[Seq[(Int, Int)]],
  ): Plan[(Int, (Option[Int], Int))] =
    Plan.RightOuterJoinOp(
      source(leftParts),
      source(rightParts),
      Some(Plan.JoinStrategy.ShuffleHash),
    )

  private def source(parts: Seq[Seq[(Int, Int)]]): Plan[(Int, Int)] =
    Plan.Source(parts.map(rows => Partition(rows)))

  /** Published `(key, left, right)` batches, after the value-column swap. */
  private def published(plan: Plan[_]): (Vector[ColumnBatch], AbsentSide) =
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("right outer did not lower"))
    ColumnarInterpreter.interpret(plan, op) match
      case ColumnarInterpreter.Columns.Outer(parts, absent) => (parts, absent)
      case other => fail(s"expected an outer batch, got $other")

  private case class ValidityRow(key: Option[Int], leftValid: Boolean, rightValid: Boolean)

  private def validityRows(batch: ColumnBatch): Seq[ValidityRow] =
    val keys = BatchColumns.int32(batch, 0)
    val left = BatchColumns.int32(batch, 1)
    val right = BatchColumns.int32(batch, 2)
    (0 until batch.length).map { row =>
      val key = if (keys.isValid(row)) Some(keys.values(row)) else None
      ValidityRow(key, left.isValid(row), right.isValid(row))
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def sameRows(actual: Seq[Any], expected: Seq[Any]): org.scalatest.Assertion =
    actual should contain theSameElementsAs expected

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

  /**
   * The same `(key, left, right)` batch decodes as `(Int, Option[Int])` when the right column is
   * the absent side and as `(Option[Int], Int)` when the left column is.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def assertDecodeShapes(batch: ColumnBatch): org.scalatest.Assertion =
    val leftOuter = BatchCodec.decodeOuter(batch, AbsentSide.Right)
    val rightOuter = BatchCodec.decodeOuter(batch, AbsentSide.Left)
    val absentLeft: Any = null
    // Row 3 clears the left bit. Left-outer decode still reads that column as a present int,
    // so the clear bit stays null. Only the right column is the Option.
    leftOuter shouldBe Seq(
      (1, (10, Some(100))),
      (2, (20, None)),
      (3, (absentLeft, Some(30))),
    )
    rightOuter shouldBe Seq(
      (1, (Some(10), 100)),
      (2, (Some(20), absentLeft)),
      (3, (None, 30)),
    )
    def pairOf(row: Any): (Any, Any) =
      val values = row.asInstanceOf[Product].productElement(1).asInstanceOf[Product]
      (values.productElement(0), values.productElement(1))
    pairOf(leftOuter.headOption.getOrElse(fail("missing left outer row"))) match
      case (_: Int, _: Option[?]) => succeed
      case other => fail(s"left outer decode shape: $other")
    pairOf(rightOuter.headOption.getOrElse(fail("missing right outer row"))) match
      case (_: Option[?], _: Int) => succeed
      case other => fail(s"right outer decode shape: $other")
    pairOf(rightOuter.lift(2).getOrElse(fail("missing absent left row"))) match
      case (None, _: Int) => succeed
      case other => fail(s"right outer absence shape: $other")

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def outerBatch(
      keys: Array[Int],
      left: Array[Int],
      right: Array[Int],
      leftPresent: Array[Boolean],
      rightPresent: Array[Boolean],
  ): ColumnBatch =
    val width = keys.length
    def column(values: Array[Int], present: Array[Boolean]): Int32Column =
      Int32Column.of(values, Validity.pack(width, row => present(row)), width)
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
      Vector(
        column(keys, Array.fill(width)(true)),
        column(left, leftPresent),
        column(right, rightPresent),
      ),
      width,
    )

  private def sortMergeLeftRows: Seq[(Int, Int)] =
    SortMergeOuterFixture.sortMergeLeftRows

  private def sortMergeRightRows: Seq[(Int, Int)] =
    SortMergeOuterFixture.sortMergeRightRows

  private def spreadKeys(base: Int, nullValue: Int): Seq[(Int, Int)] =
    SortMergeOuterFixture.spreadKeys(base, nullValue)

  private def assertRightShape(rows: Seq[(Option[Int], Option[Int], Option[Int])]): Unit =
    rows.count(row => row._1.isEmpty && row._2.isEmpty && row._3.contains(9)) shouldBe 1
    rows.exists(row => row._2.contains(7)) shouldBe false
    rows.exists(row => row._1.contains(8)) shouldBe false
    rows.filter(_._1.contains(12)) should contain theSameElementsAs Seq(
      (Some(12), None, Some(700)),
    )
    rows.filter(_._1.contains(0)) should contain theSameElementsAs Seq(
      (Some(0), Some(100), Some(500)),
      (Some(0), Some(200), Some(500)),
    )
    rows.count(_._1.contains(1)) shouldBe 2

  private def assertOuterBits(
      plan: Plan[_],
      absent: AbsentSide,
      expected: Seq[(Option[Int], Option[Int], Option[Int])],
  ): Seq[(Option[Int], Option[Int], Option[Int])] =
    SortMergeOuterFixture.assertOuterBits(plan, absent, expected, "right outer")

  private def collectCurrent(plan: Plan[_]): Seq[Any] =
    ScarletRuntime.get.shuffle.clear()
    DistCollection(plan).collect().toSeq

  private def withSpill[A](budget: Long)(body: Path => A): A =
    SortMergeOuterFixture.withSpill(shuffledConf, budget)(body)

