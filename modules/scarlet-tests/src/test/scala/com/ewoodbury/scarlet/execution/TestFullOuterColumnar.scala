package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Full outer join against the row-path `fullOuterJoin`. The row executor ignores the strategy
 * hint and always shuffle-hashes, so turning columnar execution off is the oracle. The probe is
 * the left input. Both values are optional.
 */
class TestFullOuterColumnar extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(runningConf)

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "broadcast full outer join" should "match the row path" in {
    val cases = Seq(
      "matches" -> (
        Seq(Seq(1 -> 10), Seq(2 -> 20)),
        Seq(Seq(1 -> 100, 2 -> 200)),
      ),
      "left-only rows" -> (
        Seq(Seq(1 -> 10, 3 -> 30), Seq(4 -> 40)),
        Seq(Seq(1 -> 100)),
      ),
      "right-only rows" -> (
        Seq(Seq(1 -> 10), Seq(2 -> 20)),
        Seq(Seq(1 -> 100, 4 -> 40, 5 -> 50)),
      ),
      "duplicate keys" -> (
        Seq(Seq(1 -> 10, 1 -> 11), Seq(2 -> 20)),
        Seq(Seq(1 -> 100, 1 -> 101), Seq(2 -> 200, 2 -> 201)),
      ),
      "null key on the left only" -> (
        Seq(Seq(pair(Some(0), Some(5))), Seq(pair(None, Some(7)))),
        Seq(Seq(0 -> 50, 1 -> 100)),
      ),
      "null key on the right only" -> (
        Seq(Seq(0 -> 5), Seq(1 -> 10)),
        Seq(Seq(pair(Some(0), Some(50)), pair(None, Some(99)))),
      ),
      // A present pair is first so the source is classified as int pairs. A leading null key
      // does not witness a pair, and that plan stays on the row path.
      "null key on both sides" -> (
        Seq(Seq(pair(Some(1), Some(10)), pair(None, Some(4))), Seq(pair(None, Some(5)))),
        Seq(Seq(pair(Some(1), Some(100)), pair(None, Some(8))), Seq(pair(None, Some(9)))),
      ),
      // Keys 0 and 1 are bits of one validity word. Four probe batches and four threads OR
      // those bits after the probes join.
      "matches in one validity word" -> (
        Seq(Seq(0 -> 10), Seq(1 -> 11), Seq(0 -> 12), Seq(3 -> 30)),
        Seq(Seq(0 -> 100, 1 -> 101, 8 -> 80)),
      ),
    )
    cases.foreach { case (label, (leftParts, rightParts)) =>
      val rows = matchesRow(label, Some(Plan.JoinStrategy.Broadcast), leftParts, rightParts)
      if (label.contains("null key on the left")) then
        rows.exists(_._3.contains(7)) shouldBe false
        rows.exists { row =>
          row._1.contains(0) && row._2.contains(5) && row._3.contains(50)
        } shouldBe true
        rows.count(row => row._1.isEmpty && row._2.contains(7) && row._3.isEmpty) shouldBe 1
      if (label.contains("null key on the right")) then
        rows.exists { row =>
          row._1.contains(0) && row._2.contains(5) && row._3.contains(99)
        } shouldBe false
        rows.count(row => row._1.isEmpty && row._2.isEmpty && row._3.contains(99)) shouldBe 1
      if (label.contains("both sides")) then
        rows.count(row => row._1.isEmpty && row._2.isDefined && row._3.isEmpty) shouldBe 2
        rows.count(row => row._1.isEmpty && row._2.isEmpty && row._3.isDefined) shouldBe 2
        rows.count(row => row._1.contains(1) && row._2.isDefined && row._3.isDefined) shouldBe 1
      if (label.contains("validity word")) then
        copies(rows, (Some(0), None, Some(100))) shouldBe 0
        copies(rows, (Some(1), None, Some(101))) shouldBe 0
        copies(rows, (Some(8), None, Some(80))) shouldBe 1
    }
  }

  it should "emit every build row when the probe is empty" in {
    val build = source(
      Seq(
        Seq(pair(Some(1), Some(100)), pair(None, Some(7))),
        Seq(pair(Some(4), Some(40))),
      ),
    )
    val probe = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 2).plan
    val plan = Plan.FullOuterJoinOp(probe, build, Some(Plan.JoinStrategy.Broadcast))
    ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("BroadcastFullOuter(Scan, Scan)")
    val columnar = canonical(collect(plan, columnar = true))
    columnar should contain theSameElementsAs Seq(
      (Some(1), None, Some(100)),
      (None, None, Some(7)),
      (Some(4), None, Some(40)),
    )
    columnar should contain theSameElementsAs canonical(collect(plan, columnar = false))

    val emptyBuild = Plan.FullOuterJoinOp(
      source(Seq(Seq(pair(Some(1), Some(10))), Seq(pair(None, Some(3))))),
      DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 2).plan,
      Some(Plan.JoinStrategy.Broadcast),
    )
    val bothEmpty = Plan.FullOuterJoinOp(
      DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 2).plan,
      DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 3).plan,
      Some(Plan.JoinStrategy.Broadcast),
    )
    canonical(collect(emptyBuild, columnar = true)) should contain theSameElementsAs Seq(
      (Some(1), Some(10), None),
      (None, Some(3), None),
    )
    canonical(collect(emptyBuild, columnar = true)) should contain theSameElementsAs canonical(
      collect(emptyBuild, columnar = false),
    )
    canonical(collect(bothEmpty, columnar = true)) shouldBe empty
    canonical(collect(bothEmpty, columnar = false)) shouldBe empty
  }

  it should "follow the left input's partition count" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 5).plan
    val right = DistCollection.ofIntPairs(Array(1), Array(10), 2).plan
    val plan = Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast))
    ColumnarPlanner.partitioning(plan).map(_.distribution) shouldBe Some(Distribution.Unknown(5))
  }

  it should "record absence in either value column" in {
    val probe = BatchCodec.nullableIntPairs(
      Seq((Some(1), Some(10)), (Some(2), Some(11)), (None, Some(7))),
    )
    val build = BatchCodec.nullableIntPairs(
      Seq((Some(1), Some(100)), (Some(4), Some(40)), (None, Some(99))),
    )
    val parts = ColumnPipeline.broadcastFullOuterJoin(
      Vector(probe),
      Vector(build),
      parallelism = 1,
      ScarletConf.get.broadcastBuildBudgetBytes,
    )
    val rows = parts.flatMap(validityRows)
    rows.find(_.key.contains(1)).getOrElse(fail("missing matched row")) shouldBe
      ValidityRow(Some(1), leftValid = true, rightValid = true)
    rows.find(_.key.contains(2)).getOrElse(fail("missing left-only row")) shouldBe
      ValidityRow(Some(2), leftValid = true, rightValid = false)
    rows.find(_.key.contains(4)).getOrElse(fail("missing right-only row")) shouldBe
      ValidityRow(Some(4), leftValid = false, rightValid = true)
    rows.find(row => row.key.isEmpty && row.leftValid).getOrElse(fail("missing null probe key")) shouldBe
      ValidityRow(None, leftValid = true, rightValid = false)
    rows.find(row => row.key.isEmpty && !row.leftValid).getOrElse(fail("missing null build key")) shouldBe
      ValidityRow(None, leftValid = false, rightValid = true)
    rows.exists(row => !row.leftValid && !row.rightValid) shouldBe false
    assertBothOptional(parts)
  }

  it should "emit every build row from an empty probe vector" in {
    val build = BatchCodec.nullableIntPairs(
      Seq((Some(1), Some(100)), (None, Some(7)), (Some(4), Some(40))),
    )
    val parts = ColumnPipeline.broadcastFullOuterJoin(
      Vector.empty,
      Vector(build),
      parallelism = 4,
      ScarletConf.get.broadcastBuildBudgetBytes,
    )
    val rows = parts.flatMap(validityRows)
    rows should contain theSameElementsAs Seq(
      ValidityRow(Some(1), leftValid = false, rightValid = true),
      ValidityRow(Some(4), leftValid = false, rightValid = true),
      ValidityRow(None, leftValid = false, rightValid = true),
    )
    parts.flatMap(batch => canonical(BatchCodec.decodeOuter(batch, AbsentSide.Both))) should
      contain theSameElementsAs Seq(
        (Some(1), None, Some(100)),
        (Some(4), None, Some(40)),
        (None, None, Some(7)),
      )
  }

  it should "keep both bits when two threads match different rows of one word" in {
    val probe = Vector(
      BatchCodec.intPairs(Seq((0, 10))),
      BatchCodec.intPairs(Seq((1, 11))),
      BatchCodec.intPairs(Seq((0, 12))),
      BatchCodec.intPairs(Seq((3, 30))),
    )
    val build = Vector(BatchCodec.intPairs(Seq((0, 100), (1, 101), (8, 80))))
    val parts = ColumnPipeline.broadcastFullOuterJoin(
      probe,
      build,
      parallelism = 4,
      ScarletConf.get.broadcastBuildBudgetBytes,
    )
    val decoded = parts.flatMap(batch => canonical(BatchCodec.decodeOuter(batch, AbsentSide.Both)))
    decoded should contain theSameElementsAs Seq(
      (Some(0), Some(10), Some(100)),
      (Some(1), Some(11), Some(101)),
      (Some(0), Some(12), Some(100)),
      (Some(3), Some(30), None),
      (Some(8), None, Some(80)),
    )
    copies(decoded, (Some(0), None, Some(100))) shouldBe 0
    copies(decoded, (Some(1), None, Some(101))) shouldBe 0
  }

  it should "stay on the row path when a value is null" in {
    val nullProbe = Plan.FullOuterJoinOp(
      source(Seq(Seq(pair(Some(2), Some(20)), pair(Some(1), None)))),
      source(Seq(Seq(1 -> 100, 2 -> 200))),
      Some(Plan.JoinStrategy.Broadcast),
    )
    val nullBuild = Plan.FullOuterJoinOp(
      source(Seq(Seq(1 -> 10, 2 -> 20))),
      source(Seq(Seq(pair(Some(2), Some(200)), pair(Some(1), None)))),
      Some(Plan.JoinStrategy.Broadcast),
    )
    Seq(nullProbe, nullBuild).foreach { plan =>
      ScarletConf.set(runningConf)
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("BroadcastFullOuter(Scan, Scan)")
      ColumnarPlanner.tryRun(plan) shouldBe None
      sameRows(collect(plan, columnar = true), collect(plan, columnar = false))
    }
  }

  "shuffle-hash full outer join" should "match the row path across four partitions" in {
    val cases = Seq(
      "matches" -> (
        Seq(Seq(1 -> 10), Seq(2 -> 20), Seq(3 -> 30), Seq(4 -> 40)),
        Seq(Seq(1 -> 100), Seq(2 -> 200), Seq.empty[(Int, Int)], Seq(4 -> 400)),
      ),
      "left-only rows" -> (
        Seq(Seq(1 -> 10), Seq(3 -> 30), Seq(4 -> 40), Seq(6 -> 60)),
        Seq(Seq(1 -> 100), Seq.empty[(Int, Int)], Seq.empty[(Int, Int)], Seq.empty[(Int, Int)]),
      ),
      "right-only rows" -> (
        Seq(Seq(1 -> 10), Seq.empty[(Int, Int)], Seq.empty[(Int, Int)], Seq.empty[(Int, Int)]),
        Seq(Seq(1 -> 100), Seq(2 -> 20), Seq(5 -> 50), Seq(4 -> 40)),
      ),
      "duplicate keys" -> (
        Seq(Seq(1 -> 10, 1 -> 11), Seq.empty[(Int, Int)], Seq(2 -> 20), Seq(2 -> 21)),
        Seq(Seq(1 -> 100, 1 -> 101), Seq(2 -> 200), Seq.empty[(Int, Int)], Seq(1 -> 102)),
      ),
      "null key on the left only" -> (
        Seq(
          Seq(pair(Some(0), Some(5))),
          Seq(pair(None, Some(7))),
          Seq.empty[(Int, Int)],
          Seq(pair(Some(2), Some(20))),
        ),
        Seq(Seq(0 -> 50), Seq(1 -> 100), Seq.empty[(Int, Int)], Seq(2 -> 200)),
      ),
      "null key on the right only" -> (
        Seq(Seq(0 -> 5), Seq(1 -> 10), Seq.empty[(Int, Int)], Seq(2 -> 20)),
        Seq(
          Seq(pair(Some(0), Some(50))),
          Seq(pair(None, Some(99))),
          Seq.empty[(Int, Int)],
          Seq(pair(Some(3), Some(30))),
        ),
      ),
      "null key on both sides" -> (
        Seq(
          Seq(pair(Some(1), Some(10)), pair(None, Some(4))),
          Seq(pair(None, Some(5))),
          Seq.empty[(Int, Int)],
          Seq.empty[(Int, Int)],
        ),
        Seq(
          Seq(pair(Some(1), Some(100)), pair(None, Some(8))),
          Seq.empty[(Int, Int)],
          Seq.empty[(Int, Int)],
          Seq(pair(None, Some(9))),
        ),
      ),
    )
    val hints = Seq(
      "shuffle-hash" -> Some(Plan.JoinStrategy.ShuffleHash),
      "absent hint" -> Option.empty[Plan.JoinStrategy],
    )
    hints.foreach { case (hint, strategy) =>
      cases.foreach { case (label, (leftParts, rightParts)) =>
        withClue(s"$hint: ") {
          leftParts.length should be >= 4
          rightParts.length should be >= 4
          val rows = matchesRow(label, strategy, leftParts, rightParts)
          if (label.contains("null key on the left")) then
            rows.count(row => row._1.isEmpty && row._2.contains(7) && row._3.isEmpty) shouldBe 1
            rows.exists(_._3.contains(7)) shouldBe false
          if (label.contains("null key on the right")) then
            rows.count(row => row._1.isEmpty && row._2.isEmpty && row._3.contains(99)) shouldBe 1
            rows.exists { row =>
              row._1.contains(0) && row._2.contains(5) && row._3.contains(99)
            } shouldBe false
          if (label.contains("both sides")) then
            rows.count(row => row._1.isEmpty && row._2.isDefined && row._3.isEmpty) shouldBe 2
            rows.count(row => row._1.isEmpty && row._2.isEmpty && row._3.isDefined) shouldBe 2
        }
      }
    }
  }

  it should "meet a key that starts in different source partitions" in {
    // Key 4 hashes to partition 0, the same bucket as key 0, but the two inputs start in
    // different source partitions. Key 0 is partition 0's first build row and matches. Key 1
    // is partition 1's first build row and does not. A bitmap shared across partitions would
    // emit key 0 again or drop key 1.
    val leftParts = Seq(
      Seq(4 -> 40),
      Seq.empty[(Int, Int)],
      Seq.empty[(Int, Int)],
      Seq(0 -> 10),
    )
    val rightParts = Seq(
      Seq(1 -> 200),
      Seq(0 -> 100),
      Seq(4 -> 400, 4 -> 401),
      Seq(5 -> 500),
    )
    val expected = Seq(
      (Some(4), Some(40), Some(400)),
      (Some(4), Some(40), Some(401)),
      (Some(0), Some(10), Some(100)),
      (Some(1), None, Some(200)),
      (Some(5), None, Some(500)),
    )
    val hints = Seq(
      "shuffle-hash" -> Some(Plan.JoinStrategy.ShuffleHash),
      "absent hint" -> Option.empty[Plan.JoinStrategy],
    )
    hints.foreach { case (hint, strategy) =>
      withClue(s"$hint: ") {
        val plan = Plan.FullOuterJoinOp(source(leftParts), source(rightParts), strategy)
        ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
          "HashFullOuter(Exchange(Scan), Exchange(Scan))",
        )
        val rows = canonical(collect(plan, columnar = true))
        rows should contain theSameElementsAs expected
        rows should contain theSameElementsAs canonical(collect(plan, columnar = false))
        copies(rows, (Some(0), None, Some(100))) shouldBe 0
        copies(rows, (Some(4), None, Some(400))) shouldBe 0
        copies(rows, (Some(4), None, Some(401))) shouldBe 0
        copies(rows, (Some(1), None, Some(200))) shouldBe 1
        val (parts, absent) = published(plan)
        absent shouldBe AbsentSide.Both
        parts.length should be >= 4
        val located = parts.zipWithIndex.flatMap { (batch, part) =>
          validityRows(batch).map(row => (part, row))
        }
        def home(key: Int): Seq[Int] =
          located.filter(_._2.key.contains(key)).map(_._1).distinct
        home(4) shouldBe Seq(Math.floorMod(4, 4))
        home(0) shouldBe Seq(Math.floorMod(0, 4))
        home(1) shouldBe Seq(Math.floorMod(1, 4))
        home(5) shouldBe Seq(Math.floorMod(5, 4))
        located.find(_._2.key.contains(0)).map(_._2.leftValid) shouldBe Some(true)
        located.find(_._2.key.contains(1)).map(_._2.leftValid) shouldBe Some(false)
        located.find(_._2.key.contains(1)).map(_._2.rightValid) shouldBe Some(true)
      }
    }
  }

  it should "emit every build row when a shuffled probe is empty" in {
    val probe = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 4).plan
    val build = source(
      Seq(
        Seq(pair(Some(1), Some(100))),
        Seq(pair(None, Some(7))),
        Seq(pair(Some(4), Some(40))),
        Seq(pair(Some(5), Some(50))),
      ),
    )
    val hints = Seq(Some(Plan.JoinStrategy.ShuffleHash), Option.empty[Plan.JoinStrategy])
    hints.foreach { strategy =>
      val plan = Plan.FullOuterJoinOp(probe, build, strategy)
      val rows = canonical(collect(plan, columnar = true))
      rows should contain theSameElementsAs Seq(
        (Some(1), None, Some(100)),
        (None, None, Some(7)),
        (Some(4), None, Some(40)),
        (Some(5), None, Some(50)),
      )
      rows should contain theSameElementsAs canonical(collect(plan, columnar = false))
    }
    val emptyBuild = Plan.FullOuterJoinOp(
      source(
        Seq(
          Seq(pair(Some(1), Some(10))),
          Seq(pair(None, Some(3))),
          Seq.empty[(Int, Int)],
          Seq.empty[(Int, Int)],
        ),
      ),
      DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 4).plan,
      Some(Plan.JoinStrategy.ShuffleHash),
    )
    val bothEmpty = Plan.FullOuterJoinOp(
      DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 4).plan,
      DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 4).plan,
      None,
    )
    canonical(collect(emptyBuild, columnar = true)) should contain theSameElementsAs Seq(
      (Some(1), Some(10), None),
      (None, Some(3), None),
    )
    canonical(collect(bothEmpty, columnar = true)) shouldBe empty
    canonical(collect(bothEmpty, columnar = false)) shouldBe empty
  }

  it should "stay on the row path when a shuffled value is null" in {
    val nullProbe = Plan.FullOuterJoinOp(
      source(
        Seq(
          Seq(pair(Some(2), Some(20))),
          Seq(pair(Some(1), None)),
          Seq.empty[(Int, Int)],
          Seq.empty[(Int, Int)],
        ),
      ),
      source(Seq(Seq(1 -> 100), Seq(2 -> 200), Seq.empty[(Int, Int)], Seq.empty[(Int, Int)])),
      Some(Plan.JoinStrategy.ShuffleHash),
    )
    val nullBuild = Plan.FullOuterJoinOp(
      source(Seq(Seq(1 -> 10), Seq(2 -> 20), Seq.empty[(Int, Int)], Seq.empty[(Int, Int)])),
      source(
        Seq(
          Seq(pair(Some(2), Some(200))),
          Seq(pair(Some(1), None)),
          Seq.empty[(Int, Int)],
          Seq.empty[(Int, Int)],
        ),
      ),
      None,
    )
    Seq(nullProbe, nullBuild).foreach { plan =>
      ScarletConf.set(runningConf)
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
        "HashFullOuter(Exchange(Scan), Exchange(Scan))",
      )
      ColumnarPlanner.tryRun(plan) shouldBe None
      sameRows(collect(plan, columnar = true), collect(plan, columnar = false))
    }
  }

  it should "refuse a null value before the kernel runs" in {
    val probe = BatchCodec.nullableIntPairs(Seq((Some(1), Some(10)), (Some(2), None)))
    val build = BatchCodec.nullableIntPairs(Seq((Some(1), Some(100))))
    val present = BatchCodec.intPairs(Seq((1, 10)))
    val nullBuild = BatchCodec.nullableIntPairs(Seq((Some(1), None)))
    val budget = ScarletConf.get.broadcastBuildBudgetBytes
    val refusedProbe = intercept[IllegalArgumentException] {
      ColumnPipeline.broadcastFullOuterJoin(Vector(probe), Vector(build), parallelism = 1, budget)
    }
    refusedProbe.getMessage should include("fully present")
    val refusedBuild = intercept[IllegalArgumentException] {
      ColumnPipeline.hashFullOuterJoin(
        Vector(present),
        Vector(nullBuild),
        numPartitions = 4,
        parallelism = 1,
        batchSize = 8,
        budget,
      )
    }
    refusedBuild.getMessage should include("fully present")
  }

  it should "charge the build table and not the matched-row bitmap" in {
    val probe = Vector(BatchCodec.intPairs(Seq((1, 10))))
    // Three present keys enter the table. The null key is carried beside it.
    val build = Vector(
      BatchCodec.nullableIntPairs(
        Seq((Some(1), Some(100)), (Some(2), Some(200)), (Some(3), Some(300)), (None, Some(9))),
      ),
    )
    val estimate = HashJoin.retainedBuildBytes(3)
    val refused = intercept[IllegalStateException] {
      ColumnPipeline.broadcastFullOuterJoin(
        probe,
        build,
        parallelism = 4,
        buildBudgetBytes = estimate - 1,
      )
    }
    refused.getMessage should include("above the budget")
    val fitted = ColumnPipeline.broadcastFullOuterJoin(
      probe,
      build,
      parallelism = 4,
      buildBudgetBytes = estimate,
    )
    fitted.flatMap(batch => canonical(BatchCodec.decodeOuter(batch, AbsentSide.Both))) should
      contain theSameElementsAs Seq(
        (Some(1), Some(10), Some(100)),
        (Some(2), None, Some(200)),
        (Some(3), None, Some(300)),
        (None, None, Some(9)),
      )
  }

  it should "refuse sort-merge" in {
    val left = DistCollection(Seq(1 -> 10), 2).plan
    val right = DistCollection(Seq(1 -> 20, 2 -> 30), 1).plan
    val sortMerge = Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge))
    ColumnarPlanner.lower(sortMerge) shouldBe None
    sameRows(collect(sortMerge, columnar = true), collect(sortMerge, columnar = false))
    ScarletConf.set(runningConf)
    ColumnarPlanner.lower(
      Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
    ) match
      case Some(PhysicalOp.BroadcastFullOuter(_, _)) => succeed
      case other => fail(s"broadcast full outer did not lower: $other")
    val shuffleShape = "HashFullOuter(Exchange(Scan), Exchange(Scan))"
    ColumnarPlanner.lower(
      Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.ShuffleHash)),
    ) match
      case Some(PhysicalOp.HashFullOuter(PhysicalOp.Exchange(_), PhysicalOp.Exchange(_))) =>
        succeed
      case other => fail(s"shuffle full outer did not exchange both sides: $other")
    ColumnarPlanner
      .lower(Plan.FullOuterJoinOp(left, right, None))
      .map(_.shape) shouldBe Some(shuffleShape)
  }

  "left and right outer joins" should "keep the batch and visit counts from before full outer" in {
    val left = source(
      Seq(Seq(1 -> 10, 2 -> 20), Seq(3 -> 30), Seq(4 -> 40), Seq(6 -> 60)),
    )
    val right = source(
      Seq(Seq(1 -> 100), Seq(2 -> 200, 5 -> 50), Seq.empty[(Int, Int)], Seq(4 -> 400)),
    )
    val plans = Seq(
      "left-broadcast" -> Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
      "left-shuffle" -> Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.ShuffleHash)),
      "left-absent" -> Plan.LeftOuterJoinOp(left, right, None),
      "right-broadcast" -> Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
      "right-shuffle" -> Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.ShuffleHash)),
      "right-absent" -> Plan.RightOuterJoinOp(left, right, None),
    )
    val expected = Map(
      "left-broadcast" -> ((12L, 4L)),
      "left-shuffle" -> ((51L, 12L)),
      "left-absent" -> ((51L, 12L)),
      "right-broadcast" -> ((16L, 4L)),
      "right-shuffle" -> ((55L, 12L)),
      "right-absent" -> ((55L, 12L)),
    )
    plans.foreach { case (label, plan) =>
      withClue(s"$label: ") {
        val sink = new CountingSink
        ColumnMetrics.withSink(sink) {
          ScarletConf.set(runningConf)
          ScarletRuntime.get.shuffle.clear()
          val _ = DistCollection(plan).collect()
        }
        val (batches, visits) = expected(label)
        sink.batchCount shouldBe batches
        sink.visitCount shouldBe visits
      }
    }
  }

  /** How many times `row` occurs. Avoids `==`, which WartRemover rejects. */
  private def copies(
      rows: Seq[(Option[Int], Option[Int], Option[Int])],
      row: (Option[Int], Option[Int], Option[Int]),
  ): Int =
    rows.count {
      case (key, left, right) =>
        sameCell(key, row._1) && sameCell(left, row._2) && sameCell(right, row._3)
    }

  private def sameCell(left: Option[Int], right: Option[Int]): Boolean =
    (left, right) match
      case (Some(leftValue), Some(rightValue)) => Ordering[Int].equiv(leftValue, rightValue)
      case (None, None) => true
      case _ => false

  /**
   * Both value columns decode as `Option`. A clear key bit stays null. This is the only place the
   * suite builds that null key.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def assertBothOptional(parts: Vector[ColumnBatch]): org.scalatest.Assertion =
    val decoded = parts.flatMap(batch => BatchCodec.decodeOuter(batch, AbsentSide.Both))
    val missingKey: Any = null
    decoded should contain theSameElementsAs Seq(
      (1, (Some(10), Some(100))),
      (2, (Some(11), None)),
      (missingKey, (Some(7), None)),
      (4, (None, Some(40))),
      (missingKey, (None, Some(99))),
    )
    decoded.headOption.getOrElse(fail("missing decoded row")) match
      case (_, (_: Option[?], _: Option[?])) => succeed
      case other => fail(s"full outer decode shape: $other")

  private def runningConf: ScarletConf =
    originalConf.copy(
      columnarExecution = true,
      defaultShufflePartitions = 4,
      threadPoolSize = 4,
    )

  private def matchesRow(
      label: String,
      strategy: Option[Plan.JoinStrategy],
      leftParts: Seq[Seq[(Int, Int)]],
      rightParts: Seq[Seq[(Int, Int)]],
  ): Seq[(Option[Int], Option[Int], Option[Int])] =
    withClue(s"$label: ") {
      ScarletConf.set(runningConf)
      val plan = Plan.FullOuterJoinOp(source(leftParts), source(rightParts), strategy)
      val shape = strategy match
        case Some(Plan.JoinStrategy.Broadcast) => "BroadcastFullOuter(Scan, Scan)"
        case Some(Plan.JoinStrategy.ShuffleHash) | None =>
          "HashFullOuter(Exchange(Scan), Exchange(Scan))"
        case Some(Plan.JoinStrategy.SortMerge) => fail("sort-merge is not a columnar full outer")
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(shape)
      val columnar = canonical(collect(plan, columnar = true))
      columnar should contain theSameElementsAs canonical(collect(plan, columnar = false))
      columnar
    }

  private def source(parts: Seq[Seq[(Int, Int)]]): Plan[(Int, Int)] =
    Plan.Source(parts.map(rows => Partition(rows)))

  private def published(plan: Plan[_]): (Vector[ColumnBatch], AbsentSide) =
    ScarletConf.set(runningConf)
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("full outer did not lower"))
    ColumnarInterpreter.interpret(plan, op) match
      case ColumnarInterpreter.Columns.Outer(parts, absent) => (parts, absent)
      case other => fail(s"expected an outer batch, got $other")

  private case class ValidityRow(key: Option[Int], leftValid: Boolean, rightValid: Boolean)

  private def validityRows(batch: ColumnBatch): Seq[ValidityRow] =
    val keys = int32(batch, 0)
    val left = int32(batch, 1)
    val right = int32(batch, 2)
    (0 until batch.length).map { row =>
      val key = if (keys.isValid(row)) Some(keys.values(row)) else None
      ValidityRow(key, left.isValid(row), right.isValid(row))
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def sameRows(actual: Seq[Any], expected: Seq[Any]): org.scalatest.Assertion =
    actual should contain theSameElementsAs expected

  private def collect(plan: Plan[_], columnar: Boolean): Seq[Any] =
    ScarletConf.set(runningConf.copy(columnarExecution = columnar))
    ScarletRuntime.get.shuffle.clear()
    DistCollection(plan).collect().toSeq

  /** `(key, left, right)` with a null key or value as `None`, so a null is not unboxed. */
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
