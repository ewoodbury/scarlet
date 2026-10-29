package com.ewoodbury.scarlet.execution

import java.nio.file.Path

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Left outer join against the row-path `leftOuterJoin`. The row executor ignores the strategy
 * hint and always shuffle-hashes, so turning columnar execution off is the oracle.
 */
class TestLeftOuterColumnar extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    SortMergeRuns.resetForTest()
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

    canonical(BatchCodec.decodeOuter(batch, AbsentSide.Right)) should contain theSameElementsAs Seq(
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
    val hashed = Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.ShuffleHash))
    val unhinted = Plan.LeftOuterJoinOp(left, right, None)
    val shuffleShape = "HashLeftOuter(Exchange(Scan), Exchange(Scan))"
    ColumnarPlanner.lower(hashed) match
      case Some(PhysicalOp.HashLeftOuter(PhysicalOp.Exchange(_), PhysicalOp.Exchange(_))) =>
        succeed
      case other => fail(s"shuffle left outer did not exchange both sides: $other")
    ColumnarPlanner.lower(unhinted).map(_.shape) shouldBe Some(shuffleShape)
    val sortMerge = Seq(
      Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) ->
        "SortMergeLeftOuter(Exchange(Scan), Exchange(Scan))",
      Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) ->
        "SortMergeRightOuter(Exchange(Scan), Exchange(Scan))",
      Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)) ->
        "SortMergeFullOuter(Exchange(Scan), Exchange(Scan))",
    )
    sortMerge.foreach { case (plan, shape) =>
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(shape)
    }
  }

  "shuffle-hash left outer join" should "match the row path for matches and unmatched probe rows" in {
    val cases = Seq(
      "matches" -> (
        Seq(Seq(1 -> 10, 2 -> 20), Seq(3 -> 30), Seq.empty[(Int, Int)], Seq(4 -> 40)),
        Seq(Seq(1 -> 100), Seq(2 -> 200), Seq(3 -> 300), Seq(4 -> 400)),
      ),
      "unmatched probe rows" -> (
        Seq(Seq(1 -> 10), Seq(2 -> 20), Seq(4 -> 40), Seq(6 -> 60)),
        Seq(Seq(1 -> 100), Seq.empty[(Int, Int)], Seq(3 -> 30), Seq.empty[(Int, Int)]),
      ),
      "duplicate keys" -> (
        Seq(Seq(1 -> 10, 1 -> 11), Seq(2 -> 20), Seq.empty[(Int, Int)], Seq(1 -> 12)),
        Seq(Seq(1 -> 100, 1 -> 101), Seq.empty[(Int, Int)], Seq(2 -> 200), Seq(2 -> 201)),
      ),
      "null probe key across four partitions" -> (
        Seq(
          Seq(pair(Some(1), Some(10))),
          Seq(pair(None, Some(7))),
          Seq(pair(Some(5), Some(50))),
          Seq(pair(Some(2), Some(20)), pair(Some(1), Some(11))),
        ),
        Seq(
          Seq(pair(Some(1), Some(100))),
          Seq(pair(None, Some(99))),
          Seq(pair(Some(3), Some(30))),
          Seq(pair(Some(1), Some(101)), pair(None, Some(98))),
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
          val plan = Plan.LeftOuterJoinOp(source(leftParts), source(rightParts), strategy)
          ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
            "HashLeftOuter(Exchange(Scan), Exchange(Scan))",
          )
          val columnar = canonical(collect(plan, columnar = true))
          val row = canonical(collect(plan, columnar = false))
          columnar should contain theSameElementsAs row
          if (label.contains("null probe")) then
            columnar.exists(_._1.isEmpty) shouldBe true
            columnar.exists(_._3.contains(99)) shouldBe false
            columnar.exists(_._3.contains(98)) shouldBe false
        }
      }
    }
  }

  it should "match the row path when the build or the probe is empty" in {
    val present = DistCollection.ofIntPairs(Array(1, 2, 3, 4), Array(10, 20, 30, 40), 4)
    val noRows = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 4)
    val emptyBuild = Plan.LeftOuterJoinOp(
      present.plan,
      noRows.plan,
      Some(Plan.JoinStrategy.ShuffleHash),
    )
    val emptyProbe = Plan.LeftOuterJoinOp(
      noRows.plan,
      present.plan,
      Some(Plan.JoinStrategy.ShuffleHash),
    )

    ColumnarPlanner.lower(emptyBuild).map(_.shape) shouldBe Some(
      "HashLeftOuter(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(emptyProbe).map(_.shape) shouldBe Some(
      "HashLeftOuter(Exchange(Scan), Exchange(Scan))",
    )
    canonical(collect(emptyBuild, columnar = true)) should contain theSameElementsAs canonical(
      collect(emptyBuild, columnar = false),
    )
    canonical(collect(emptyProbe, columnar = true)) shouldBe empty
    canonical(collect(emptyProbe, columnar = false)) shouldBe empty
  }

  it should "stay on the row path when a value is null" in {
    val nullBuild = shufflePlan(
      Seq(Seq(1 -> 10), Seq(2 -> 20), Seq.empty[(Int, Int)], Seq.empty[(Int, Int)]),
      Seq(
        Seq(pair(Some(2), Some(200))),
        Seq(pair(Some(1), None)),
        Seq.empty[(Int, Int)],
        Seq.empty[(Int, Int)],
      ),
    )
    val nullProbe = shufflePlan(
      Seq(
        Seq(pair(Some(2), Some(20))),
        Seq(pair(Some(1), None)),
        Seq.empty[(Int, Int)],
        Seq.empty[(Int, Int)],
      ),
      Seq(Seq(1 -> 100), Seq(2 -> 200), Seq.empty[(Int, Int)], Seq.empty[(Int, Int)]),
    )
    Seq(nullBuild, nullProbe).foreach { plan =>
      ScarletConf.set(shuffledConf)
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
        "HashLeftOuter(Exchange(Scan), Exchange(Scan))",
      )
      ColumnarPlanner.tryRun(plan) shouldBe None
      sameRows(collect(plan, columnar = true), collect(plan, columnar = false))
    }
  }

  it should "record absence in the right validity bit after the exchange" in {
    val probe = BatchCodec.nullableIntPairs(
      Seq(
        (Some(1), Some(10)),
        (Some(2), Some(20)),
        (None, Some(7)),
        (Some(4), Some(40)),
      ),
    )
    val build = BatchCodec.nullableIntPairs(
      Seq((Some(1), Some(100)), (None, Some(99)), (Some(8), Some(80))),
    )
    val parts = ColumnPipeline.hashLeftOuterJoin(
      Vector(probe),
      Vector(build),
      numPartitions = 4,
      parallelism = 4,
      batchSize = 8,
      ScarletConf.get.broadcastBuildBudgetBytes,
    )
    parts.length shouldBe 4
    val rows = parts.zipWithIndex.flatMap { (batch, part) =>
      val keys = int32(batch, 0)
      val left = int32(batch, 1)
      val right = int32(batch, 2)
      (0 until batch.length).map { row =>
        val key = if (keys.isValid(row)) Some(keys.values(row)) else None
        (part, key, left.isValid(row), right.isValid(row))
      }
    }
    rows.find(_._2.contains(1)).getOrElse(fail("missing matched row")) shouldBe
      ((Math.floorMod(1, 4), Some(1), true, true))
    rows.find(_._2.contains(2)).getOrElse(fail("missing unmatched probe row")) shouldBe
      ((Math.floorMod(2, 4), Some(2), true, false))
    rows.find(_._2.contains(4)).getOrElse(fail("missing unmatched key 4")) shouldBe
      ((0, Some(4), true, false))
    rows.find(_._2.isEmpty).getOrElse(fail("missing null probe key")) shouldBe
      ((0, None, true, false))
    rows.exists(_._2.contains(8)) shouldBe false
    val unmatched = parts.lift(Math.floorMod(2, 4)).getOrElse(fail("missing partition"))
    canonical(BatchCodec.decodeOuter(unmatched, AbsentSide.Right)) should contain theSameElementsAs Seq(
      (Some(2), Some(20), None),
    )
    val carried = parts.headOption.getOrElse(fail("missing partition 0"))
    canonical(BatchCodec.decodeOuter(carried, AbsentSide.Right)) should contain theSameElementsAs Seq(
      (None, Some(7), None),
      (Some(4), Some(40), None),
    )
  }

  it should "refuse a null value before the shuffle kernel runs" in {
    val probe = BatchCodec.nullableIntPairs(Seq((Some(1), Some(10)), (Some(2), None)))
    val build = BatchCodec.nullableIntPairs(Seq((Some(1), Some(100))))
    val refused = intercept[IllegalArgumentException] {
      ColumnPipeline.hashLeftOuterJoin(
        Vector(probe),
        Vector(build),
        numPartitions = 4,
        parallelism = 1,
        batchSize = 8,
        ScarletConf.get.broadcastBuildBudgetBytes,
      )
    }
    refused.getMessage should include("fully present")
  }

  it should "bound each partition build table by the broadcast budget" in {
    val probe = Vector(BatchCodec.intPairs(Seq((1, 10))))
    val build = Vector(BatchCodec.intPairs(Seq((1, 100), (1, 101), (1, 102))))
    val estimate = HashJoin.retainedBuildBytes(3)
    val refused = intercept[IllegalStateException] {
      ColumnPipeline.hashLeftOuterJoin(
        probe,
        build,
        numPartitions = 4,
        parallelism = 1,
        batchSize = 8,
        buildBudgetBytes = estimate - 1,
      )
    }
    refused.getMessage should include("above the budget")
    val fitted = ColumnPipeline.hashLeftOuterJoin(
      probe,
      build,
      numPartitions = 4,
      parallelism = 1,
      batchSize = 8,
      buildBudgetBytes = estimate,
    )
    fitted.map(_.length).sum shouldBe 3
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
        ColumnarInterpreter.Columns.Outer(Vector(batch), AbsentSide.Right),
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

  "sort-merge left outer join" should "match the row path in memory and over spilled runs" in {
    val plan = Plan.LeftOuterJoinOp(
      source(Seq(sortMergeLeftRows)),
      source(Seq(sortMergeRightRows)),
      Some(Plan.JoinStrategy.SortMerge),
    )
    val budget = SortMergeRuns.retainedBytes(2) + SortMergeRuns.retainedBytes(1)
    SortMergeRuns.retainedBytes(4) + SortMergeRuns.retainedBytes(3) should be > budget

    ScarletConf.set(shuffledConf)
    SortMergeRuns.resetForTest()
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("sort-merge left outer did not lower"))
    lowered.kind shouldBe PhysicalOp.Kind.Triples
    lowered.shape shouldBe "SortMergeLeftOuter(Exchange(Scan), Exchange(Scan))"
    val memory = canonical(collect(plan, columnar = true))
    val row = canonical(collect(plan, columnar = false))
    memory should contain theSameElementsAs row
    assertLeftShape(memory)
    ScarletConf.set(shuffledConf)
    val fromBits = assertOuterBits(plan, AbsentSide.Right, memory)
    assertLeftShape(fromBits)
    SortMergeRuns.filesWrittenForTest shouldBe 0L

    val spilled = withSpill(budget) { dir =>
      val rows = canonical(collectCurrent(plan))
      assertOuterBits(plan, AbsentSide.Right, rows)
      SortMergeRuns.filesWrittenForTest should be >= 28L
      SortMergeRuns.mergePassesForTest should be > 1L
      Path.of(SortMergeRuns.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
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
    val emptyLeft = Plan.LeftOuterJoinOp(noRows, right, Some(Plan.JoinStrategy.SortMerge))
    val emptyRight = Plan.LeftOuterJoinOp(left, noRows, Some(Plan.JoinStrategy.SortMerge))
    val both = Plan.LeftOuterJoinOp(noRows, noRows, Some(Plan.JoinStrategy.SortMerge))

    ScarletConf.set(shuffledConf)
    canonical(collect(emptyLeft, columnar = true)).isEmpty shouldBe true
    val kept = canonical(collect(emptyRight, columnar = true))
    kept should contain theSameElementsAs canonical(collect(emptyRight, columnar = false))
    kept.count(_._1.isEmpty) shouldBe 1
    kept.forall(_._3.isEmpty) shouldBe true
    canonical(collect(both, columnar = true)).isEmpty shouldBe true

    val spilledEmptyLeft = withSpill(budget) { dir =>
      val rows = canonical(collectCurrent(emptyLeft))
      SortMergeRuns.filesWrittenForTest should be > 0L
      SortMergeRuns.mergePassesForTest should be > 1L
      Path.of(SortMergeRuns.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      rows
    }
    spilledEmptyLeft.isEmpty shouldBe true

    val spilledKept = withSpill(budget) { dir =>
      val rows = canonical(collectCurrent(emptyRight))
      assertOuterBits(emptyRight, AbsentSide.Right, rows)
      SortMergeRuns.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      rows
    }
    spilledKept should contain theSameElementsAs kept

    val spilledBoth = withSpill(budget) { dir =>
      val rows = canonical(collectCurrent(both))
      SortMergeRuns.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      assertOuterBits(both, AbsentSide.Right, Seq.empty)
      rows
    }
    spilledBoth.isEmpty shouldBe true
  }

  it should "keep a null value on the row path" in {
    val left = source(Seq(Seq(1 -> 10, pair(Some(2), None))))
    val right = source(Seq(Seq(1 -> 100)))
    val plan = Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge))
    ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
      "SortMergeLeftOuter(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.tryRun(plan) shouldBe None
    canonical(collect(plan, columnar = true)) should contain theSameElementsAs canonical(
      collect(plan, columnar = false),
    )
    val probe = BatchCodec.nullableIntPairs(Seq((Some(1), Some(10)), (Some(2), None)))
    val build = BatchCodec.nullableIntPairs(Seq((Some(1), Some(100))))
    val refused = intercept[IllegalArgumentException] {
      ColumnPipeline.sortMergeLeftOuterJoin(
        Vector(probe),
        Vector(build),
        numPartitions = 1,
        parallelism = 1,
        batchSize = 8,
      )
    }
    refused.getMessage should include("fully present")
    val kernel = intercept[IllegalArgumentException] {
      SortMerge.leftOuter(probe, build)
    }
    kernel.getMessage should include("fully present")
  }

  private def shuffledConf: ScarletConf =
    originalConf.copy(columnarExecution = true, defaultShufflePartitions = 4)

  private def outerPlan(
      leftParts: Seq[Seq[(Int, Int)]],
      rightParts: Seq[Seq[(Int, Int)]],
  ): Plan[(Int, (Int, Option[Int]))] =
    Plan.LeftOuterJoinOp(source(leftParts), source(rightParts), Some(Plan.JoinStrategy.Broadcast))

  private def shufflePlan(
      leftParts: Seq[Seq[(Int, Int)]],
      rightParts: Seq[Seq[(Int, Int)]],
  ): Plan[(Int, (Int, Option[Int]))] =
    Plan.LeftOuterJoinOp(
      source(leftParts),
      source(rightParts),
      Some(Plan.JoinStrategy.ShuffleHash),
    )

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

  private def sortMergeLeftRows: Seq[(Int, Int)] =
    SortMergeOuterFixture.sortMergeLeftRows

  private def sortMergeRightRows: Seq[(Int, Int)] =
    SortMergeOuterFixture.sortMergeRightRows

  private def spreadKeys(base: Int, nullValue: Int): Seq[(Int, Int)] =
    SortMergeOuterFixture.spreadKeys(base, nullValue)

  private def assertLeftShape(rows: Seq[(Option[Int], Option[Int], Option[Int])]): Unit =
    rows.count(row => row._1.isEmpty && row._2.contains(7) && row._3.isEmpty) shouldBe 1
    rows.exists(row => row._1.isEmpty && row._3.isDefined) shouldBe false
    rows.exists(row => row._3.contains(9)) shouldBe false
    rows.exists(row => row._1.contains(12)) shouldBe false
    rows.filter(_._1.contains(0)) should contain theSameElementsAs Seq(
      (Some(0), Some(100), Some(500)),
      (Some(0), Some(200), Some(500)),
    )
    rows.filter(_._1.contains(8)) should contain theSameElementsAs Seq(
      (Some(8), Some(400), None),
    )
    rows.count(_._1.contains(1)) shouldBe 2

  private def assertOuterBits(
      plan: Plan[_],
      absent: AbsentSide,
      expected: Seq[(Option[Int], Option[Int], Option[Int])],
  ): Seq[(Option[Int], Option[Int], Option[Int])] =
    SortMergeOuterFixture.assertOuterBits(plan, absent, expected, "left outer")

  private def collectCurrent(plan: Plan[_]): Seq[Any] =
    ScarletRuntime.get.shuffle.clear()
    DistCollection(plan).collect().toSeq

  private def withSpill[A](budget: Long)(body: Path => A): A =
    SortMergeOuterFixture.withSpill(shuffledConf, budget)(body)

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")
