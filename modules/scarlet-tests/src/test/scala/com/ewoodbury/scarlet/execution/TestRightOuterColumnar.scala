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

  it should "keep a null value columnar and match the row path" in {
    val noRows = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 1).plan
    // A present pair comes first, so the source still witnesses int pairs. The null value is later.
    // The right input is the probe. The left input is the build.
    val nullProbe = source(
      Seq(Seq(pair(Some(2), Some(0)), pair(Some(1), None), pair(None, Some(7)))),
    )
    val nullBuild = source(Seq(Seq(pair(Some(0), Some(0)), pair(Some(1), None))))
    val cases = Seq(
      "null probe value" -> (
        Seq(Seq(1 -> 100, 2 -> 200)),
        Seq(Seq(pair(Some(2), Some(20)), pair(Some(1), None))),
      ),
      "null build value" -> (
        Seq(Seq(pair(Some(2), Some(200)), pair(Some(1), None))),
        Seq(Seq(1 -> 10, 2 -> 20)),
      ),
      "missing left partner" -> (
        Seq(Seq(1 -> 100)),
        Seq(Seq(1 -> 10, 2 -> 20)),
      ),
      "null probe key and dropped null build key" -> (
        Seq(Seq(pair(Some(1), Some(100)), pair(None, Some(99)), 0 -> 50)),
        Seq(Seq(pair(Some(1), Some(10)), pair(None, Some(7)), 0 -> 5)),
      ),
      "real zero" -> (
        Seq(Seq(pair(Some(1), Some(0)))),
        Seq(Seq(pair(Some(1), Some(0)), pair(Some(3), Some(0)))),
      ),
    )
    cases.foreach { case (label, (leftParts, rightParts)) =>
      withClue(s"$label: ") {
        ScarletConf.set(originalConf.copy(columnarExecution = true))
        val plan = broadcastPlan(leftParts, rightParts)
        ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("BroadcastRightOuter(Scan, Scan)")
        ColumnarPlanner.tryRun(plan).isDefined shouldBe true
        val columnar = collect(plan, columnar = true)
        agree(columnar, collect(plan, columnar = false))
        if (label.contains("dropped null build")) then
          containsInt(columnar, 99) shouldBe false
      }
    }
    withClue("empty build keeps a null probe value: ") {
      ScarletConf.set(originalConf.copy(columnarExecution = true))
      val plan = Plan.RightOuterJoinOp(noRows, nullProbe, Some(Plan.JoinStrategy.Broadcast))
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("BroadcastRightOuter(Scan, Scan)")
      ColumnarPlanner.tryRun(plan).isDefined shouldBe true
      agree(collect(plan, columnar = true), collect(plan, columnar = false))
    }
    withClue("empty probe drops a null build value: ") {
      ScarletConf.set(originalConf.copy(columnarExecution = true))
      val plan = Plan.RightOuterJoinOp(nullBuild, noRows, Some(Plan.JoinStrategy.Broadcast))
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("BroadcastRightOuter(Scan, Scan)")
      ColumnarPlanner.tryRun(plan).isDefined shouldBe true
      val row = collect(plan, columnar = false)
      agree(collect(plan, columnar = true), row)
      JoinDigest.fromOuter(row).total shouldBe 0L
    }
  }

  it should "publish a null value as a clear bit and a missing partner as absence" in {
    val plan = broadcastPlan(
      Seq(
        Seq(
          pair(Some(1), Some(100)),
          pair(Some(2), None),
          pair(None, Some(99)),
          pair(Some(0), Some(0)),
          pair(Some(4), Some(0)),
          pair(Some(8), Some(80)),
        ),
      ),
      Seq(
        Seq(
          pair(Some(2), Some(20)),
          pair(Some(1), None),
          pair(Some(3), Some(30)),
          pair(None, Some(7)),
          pair(Some(0), Some(0)),
          pair(Some(4), Some(40)),
          pair(Some(5), Some(0)),
        ),
      ),
    )
    ColumnarPlanner.tryRun(plan).isDefined shouldBe true
    val (parts, absent) = published(plan)
    absent shouldBe AbsentSide.Left
    parts.flatMap(bitMarks) should contain theSameElementsAs Seq(
      "key=1 left=present:100 right=null",
      "key=2 left=null right=present:20",
      "key=3 left=absent right=present:30",
      "key=null left=absent right=present:7",
      "key=0 left=present:0 right=present:0",
      "key=4 left=present:0 right=present:40",
      "key=5 left=absent right=present:0",
    )
    agree(decodedLeft(parts), expectedNullBits)

    val noRows = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 1).plan
    val emptyProbe = Plan.RightOuterJoinOp(
      source(
        Seq(
          Seq(
            pair(Some(1), Some(100)),
            pair(Some(2), None),
            pair(None, Some(99)),
          ),
        ),
      ),
      noRows,
      Some(Plan.JoinStrategy.Broadcast),
    )
    val emptyProbeParts = published(emptyProbe)._1
    emptyProbeParts.map(_.length).sum shouldBe 0
    JoinDigest.fromOuter(decodedLeft(emptyProbeParts)).total shouldBe 0L

    val emptyBuild = Plan.RightOuterJoinOp(
      noRows,
      source(
        Seq(
          Seq(
            pair(Some(2), Some(0)),
            pair(Some(1), None),
            pair(None, Some(7)),
          ),
        ),
      ),
      Some(Plan.JoinStrategy.Broadcast),
    )
    published(emptyBuild)._1.flatMap(bitMarks) should contain theSameElementsAs Seq(
      "key=1 left=absent right=null",
      "key=2 left=absent right=present:0",
      "key=null left=absent right=present:7",
    )
  }

  it should "record an absence bit on the left value" in {
    val plan = broadcastPlan(
      Seq(Seq(1 -> 100, 4 -> 7)),
      Seq(Seq(1 -> 10, 2 -> 11, 3 -> 30, 4 -> 13)),
    )
    val (parts, absent) = published(plan)
    absent shouldBe AbsentSide.Left
    parts.length shouldBe 1
    val batch = parts.headOption.getOrElse(fail("missing probe batch"))
    val rows = validityRows(batch).map { row =>
      (row.key, row.leftValid, row.rightValid, row.leftAbsent, row.rightAbsent)
    }
    rows.find(_._1.contains(1)).getOrElse(fail("missing matched row")) shouldBe
      ((Some(1), true, true, false, false))
    rows.find(_._1.contains(2)).getOrElse(fail("missing unmatched right row")) shouldBe
      ((Some(2), false, true, true, false))
    rows.find(_._1.contains(3)).getOrElse(fail("missing unmatched right row")) shouldBe
      ((Some(3), false, true, true, false))
    rows.find(_._1.contains(4)).getOrElse(fail("missing matched row")) shouldBe
      ((Some(4), true, true, false, false))
    rows.exists(!_._3) shouldBe false
    rows.exists(_._5) shouldBe false
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

  it should "keep a null value columnar and match the row path" in {
    val noRows = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 4).plan
    // A present pair comes first, so the source still witnesses int pairs. The null value is later.
    // The right input is the probe. The left input is the build.
    val nullProbe = source(
      Seq(Seq(pair(Some(2), Some(0)), pair(Some(1), None), pair(None, Some(7)))),
    )
    val nullBuild = source(Seq(Seq(pair(Some(0), Some(0)), pair(Some(1), None))))
    val hints = Seq(
      "shuffle-hash" -> Some(Plan.JoinStrategy.ShuffleHash),
      "absent hint" -> Option.empty[Plan.JoinStrategy],
    )
    val cases = Seq(
      "null probe value" -> (
        Seq(Seq(1 -> 100, 2 -> 200)),
        Seq(Seq(pair(Some(2), Some(20)), pair(Some(1), None))),
      ),
      "null build value" -> (
        Seq(Seq(pair(Some(2), Some(200)), pair(Some(1), None))),
        Seq(Seq(1 -> 10, 2 -> 20)),
      ),
      "missing partner" -> (
        Seq(Seq(1 -> 100)),
        Seq(Seq(1 -> 10, 2 -> 20)),
      ),
      "null probe key and dropped null build key" -> (
        Seq(Seq(pair(Some(1), Some(100)), pair(None, Some(99)), 0 -> 50)),
        Seq(Seq(pair(Some(1), Some(10)), pair(None, Some(7)), 0 -> 5)),
      ),
      "real zero" -> (
        Seq(Seq(pair(Some(1), Some(0)))),
        Seq(Seq(pair(Some(1), Some(0)), pair(Some(3), Some(0)))),
      ),
      "duplicate key with a null probe value" -> (
        Seq(Seq(1 -> 100, 1 -> 101)),
        Seq(Seq(1 -> 10, pair(Some(1), None))),
      ),
    )
    hints.foreach { case (hint, strategy) =>
      cases.foreach { case (label, (leftParts, rightParts)) =>
        withClue(s"$hint $label: ") {
          ScarletConf.set(shuffledConf)
          val plan = Plan.RightOuterJoinOp(source(leftParts), source(rightParts), strategy)
          ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
            "HashRightOuter(Exchange(Scan), Exchange(Scan))",
          )
          ColumnarPlanner.tryRun(plan).isDefined shouldBe true
          val columnar = collect(plan, columnar = true)
          val row = collect(plan, columnar = false)
          agree(columnar, row)
          if (label.contains("dropped null build")) then
            containsInt(columnar, 99) shouldBe false
        }
      }
      withClue(s"$hint empty build keeps a null probe value: ") {
        ScarletConf.set(shuffledConf)
        val plan = Plan.RightOuterJoinOp(noRows, nullProbe, strategy)
        ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
          "HashRightOuter(Exchange(Scan), Exchange(Scan))",
        )
        ColumnarPlanner.tryRun(plan).isDefined shouldBe true
        agree(collect(plan, columnar = true), collect(plan, columnar = false))
      }
      withClue(s"$hint empty probe drops a null build value: ") {
        ScarletConf.set(shuffledConf)
        val plan = Plan.RightOuterJoinOp(nullBuild, noRows, strategy)
        ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(
          "HashRightOuter(Exchange(Scan), Exchange(Scan))",
        )
        ColumnarPlanner.tryRun(plan).isDefined shouldBe true
        val row = collect(plan, columnar = false)
        agree(collect(plan, columnar = true), row)
        JoinDigest.fromOuter(row).total shouldBe 0L
      }
    }
  }

  it should "publish a null value as a clear bit and a missing partner as absence" in {
    ScarletConf.set(shuffledConf)
    val plan = Plan.RightOuterJoinOp(
      source(
        Seq(
          Seq(
            pair(Some(1), Some(100)),
            pair(Some(2), None),
            pair(None, Some(99)),
            pair(Some(0), Some(0)),
            pair(Some(4), Some(0)),
            pair(Some(8), Some(80)),
          ),
        ),
      ),
      source(
        Seq(
          Seq(
            pair(Some(2), Some(20)),
            pair(Some(1), None),
            pair(Some(3), Some(30)),
            pair(None, Some(7)),
            pair(Some(0), Some(0)),
            pair(Some(4), Some(40)),
            pair(Some(5), Some(0)),
          ),
        ),
      ),
      Some(Plan.JoinStrategy.ShuffleHash),
    )
    ColumnarPlanner.tryRun(plan).isDefined shouldBe true
    val (parts, absent) = published(plan)
    absent shouldBe AbsentSide.Left
    parts.flatMap(bitMarks) should contain theSameElementsAs Seq(
      "key=1 left=present:100 right=null",
      "key=2 left=null right=present:20",
      "key=3 left=absent right=present:30",
      "key=null left=absent right=present:7",
      "key=0 left=present:0 right=present:0",
      "key=4 left=present:0 right=present:40",
      "key=5 left=absent right=present:0",
    )
    agree(decodedLeft(parts), expectedNullBits)

    val noRows = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 4).plan
    val emptyProbe = Plan.RightOuterJoinOp(
      source(
        Seq(
          Seq(
            pair(Some(1), Some(100)),
            pair(Some(2), None),
            pair(None, Some(99)),
          ),
        ),
      ),
      noRows,
      Some(Plan.JoinStrategy.ShuffleHash),
    )
    val emptyProbeParts = published(emptyProbe)._1
    emptyProbeParts.map(_.length).sum shouldBe 0
    JoinDigest.fromOuter(decodedLeft(emptyProbeParts)).total shouldBe 0L

    val emptyBuild = Plan.RightOuterJoinOp(
      noRows,
      source(
        Seq(
          Seq(
            pair(Some(2), Some(0)),
            pair(Some(1), None),
            pair(None, Some(7)),
          ),
        ),
      ),
      Some(Plan.JoinStrategy.ShuffleHash),
    )
    published(emptyBuild)._1.flatMap(bitMarks) should contain theSameElementsAs Seq(
      "key=1 left=absent right=null",
      "key=2 left=absent right=present:0",
      "key=null left=absent right=present:7",
    )
  }

  it should "record an absence bit on the left value after the exchange" in {
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
      validityRows(batch).map { row =>
        (part, row.key, row.leftValid, row.rightValid, row.leftAbsent, row.rightAbsent)
      }
    }
    rows.find(_._2.contains(1)).getOrElse(fail("missing matched row")) shouldBe
      ((Math.floorMod(1, parts.length), Some(1), true, true, false, false))
    rows.find(_._2.contains(2)).getOrElse(fail("missing unmatched right row")) shouldBe
      ((Math.floorMod(2, parts.length), Some(2), false, true, true, false))
    rows.find(_._2.contains(4)).getOrElse(fail("missing unmatched key 4")) shouldBe
      ((0, Some(4), false, true, true, false))
    rows.find(_._2.isEmpty).getOrElse(fail("missing null right key")) shouldBe
      ((0, None, false, true, true, false))
    rows.exists(_._2.contains(8)) shouldBe false
    rows.exists(!_._4) shouldBe false
    rows.exists(_._6) shouldBe false
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

  "full outer, broadcast left, sort-merge left, and sort-merge right" should
    "decline a null value and match the row path" in {
      val left = source(Seq(Seq(1 -> 10, pair(Some(2), None))))
      val right = source(Seq(Seq(1 -> 100)))
      val plans = Seq(
        (
          "full outer",
          Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.ShuffleHash)),
          "HashFullOuter(Exchange(Scan), Exchange(Scan))",
        ),
        (
          "broadcast left",
          Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
          "BroadcastLeftOuter(Scan, Scan)",
        ),
        (
          "sort-merge left",
          Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)),
          "SortMergeLeftOuter(Exchange(Scan), Exchange(Scan))",
        ),
        (
          "sort-merge right",
          Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)),
          "SortMergeRightOuter(Exchange(Scan), Exchange(Scan))",
        ),
      )
      plans.foreach { case (label, plan, shape) =>
        withClue(s"$label: ") {
          ScarletConf.set(shuffledConf)
          ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(shape)
          ColumnarPlanner.tryRun(plan) shouldBe None
          agree(collect(plan, columnar = true), collect(plan, columnar = false))
        }
      }
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

  private case class ValidityRow(
      key: Option[Int],
      leftValid: Boolean,
      rightValid: Boolean,
      leftAbsent: Boolean,
      rightAbsent: Boolean,
  )

  private def validityRows(batch: ColumnBatch): Seq[ValidityRow] =
    val keys = BatchColumns.int32(batch, 0)
    val left = BatchColumns.int32(batch, 1)
    val right = BatchColumns.int32(batch, 2)
    (0 until batch.length).map { row =>
      keys.isAbsent(row) shouldBe false
      val key = if (keys.isValid(row)) Some(keys.values(row)) else None
      ValidityRow(
        key,
        left.isValid(row),
        right.isValid(row),
        left.isAbsent(row),
        right.isAbsent(row),
      )
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def agree(actual: Seq[Any], expected: Seq[Any]): org.scalatest.Assertion =
    val left = JoinDigest.fromOuter(actual)
    val right = JoinDigest.fromOuter(expected)
    withClue(JoinDigest.difference(left, right)) {
      JoinDigest.agrees(left, right) shouldBe true
    }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Product",
    ),
  )
  private def containsInt(rows: Seq[Any], value: Int): Boolean =
    rows.exists { row =>
      val values = row.asInstanceOf[Product].productElement(1).asInstanceOf[Product]
      (0 until values.productArity).exists { ordinal =>
        values.productElement(ordinal) match
          case number: Int => number == value
          case Some(number: Int) => number == value
          case _ => false
      }
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def decodedLeft(parts: Vector[ColumnBatch]): Seq[Any] =
    parts.flatMap(batch => BatchCodec.decodeOuter(batch, AbsentSide.Left))

  private def bitMarks(batch: ColumnBatch): Seq[String] =
    val keys = BatchColumns.int32(batch, 0)
    val left = BatchColumns.int32(batch, 1)
    val right = BatchColumns.int32(batch, 2)
    (0 until batch.length).map { row =>
      keys.isAbsent(row) shouldBe false
      val key = if (keys.isValid(row)) keys.values(row).toString else "null"
      s"key=$key left=${mark(left, row)} right=${mark(right, row)}"
    }

  /** A present `0` is not null and not absence. A clear bit with no absence mark is null. */
  private def mark(column: Int32Column, row: Int): String =
    val valid = column.isValid(row)
    val absent = column.isAbsent(row)
    if (absent && valid) then fail(s"row $row is absent and present")
    else if (absent) then
      column.values(row) shouldBe 0
      "absent"
    else if (valid) then s"present:${column.values(row)}"
    else
      column.values(row) shouldBe 0
      "null"

  /**
   * Hand-built rows for the kernel bit case. A null value is a bare null, which [[JoinDigest]]
   * classifies the same way as `Some(null)` from [[BatchCodec.optionalInt]]. A missing partner is
   * `None`. A present `0` stays boxed so it is not that null. Tuple `==` would unbox the null.
   * The left cell is the optional side.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def expectedNullBits: Seq[Any] =
    Seq(
      outerRow(Some(1), Int.box(100), null),
      outerRow(Some(2), null, Int.box(20)),
      outerRow(Some(3), None, Int.box(30)),
      outerRow(None, None, Int.box(7)),
      outerRow(Some(0), Int.box(0), Int.box(0)),
      outerRow(Some(4), Int.box(0), Int.box(40)),
      outerRow(Some(5), None, Int.box(0)),
    )

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def outerRow(key: Option[Int], left: Any, right: Any): Any =
    val boxedKey: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(boxedKey, Tuple2(left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]))

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

  private def outerBatch(
      keys: Array[Int],
      left: Array[Int],
      right: Array[Int],
      leftPresent: Array[Boolean],
      rightPresent: Array[Boolean],
  ): ColumnBatch =
    TestOuterDecode.outerBatch(keys, left, right, leftPresent, rightPresent)

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

