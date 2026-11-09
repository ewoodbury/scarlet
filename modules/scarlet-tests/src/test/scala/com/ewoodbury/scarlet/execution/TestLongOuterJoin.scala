package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Long outer joins. A null value stays on the row path. A missing partner is an absence bit. Order
 * is not part of the contract.
 */
class TestLongOuterJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true, defaultShufflePartitions = 4))
    ScarletRuntime.get.shuffle.clear()
    LongHashJoinSpill.resetForTest()
    SortMergeRuns.resetForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    LongHashJoinSpill.resetForTest()
    SortMergeRuns.resetForTest()
    ()

  "long outer join" should "lower all nine ops and refuse take" in {
    val left = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 4)
    val right = DistCollection.ofLongPairs(Array(1L, 3L), Array(100L, 30L), 4)
    val strategies = Seq(
      None,
      Some(Plan.JoinStrategy.ShuffleHash),
      Some(Plan.JoinStrategy.Broadcast),
      Some(Plan.JoinStrategy.SortMerge),
    )
    val sides = Seq("left", "right", "full")
    sides.foreach { side =>
      strategies.foreach { strategy =>
        val plan = outerPlan(side, left, right, strategy)
        val op = ColumnarPlanner.lower(plan).getOrElse(fail(s"$side $strategy did not lower"))
        op.shape shouldBe expectedShape(side, strategy)
        op.kind shouldBe PhysicalOp.Kind.LongOuter
        (op.kind == PhysicalOp.Kind.LongTriples) shouldBe false
        (op.kind == PhysicalOp.Kind.Triples) shouldBe false
        ColumnarPlanner.tryTake(plan, 1).isDefined shouldBe false
      }
    }
  }

  it should "match the row path for each strategy" in {
    val left = DistCollection.ofLongPairs(Array(1L, 1L, 2L, 0L), Array(10L, 11L, 20L, 0L), 4)
    val right = DistCollection.ofLongPairs(Array(1L, 1L, 3L), Array(100L, 101L, 0L), 4)
    val strategies = Seq(
      None,
      Some(Plan.JoinStrategy.ShuffleHash),
      Some(Plan.JoinStrategy.Broadcast),
      Some(Plan.JoinStrategy.SortMerge),
    )
    Seq("left", "right", "full").foreach { side =>
      strategies.foreach { strategy =>
        val plan = outerPlan(side, left, right, strategy)
        ran(plan) should be > 0
        same(DistCollection(plan))
        if (strategy.contains(Plan.JoinStrategy.SortMerge)) then
          SortMergeRuns.filesWrittenForTest shouldBe 0L
          SortMergeRuns.lastDirectoryForTest shouldBe ""
          LongHashJoinSpill.filesWrittenForTest shouldBe 0L
      }
    }
  }

  it should "keep an empty side on the columnar path" in {
    val present = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 4)
    val empty = DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], 4)
    val strategies = Seq(
      None,
      Some(Plan.JoinStrategy.Broadcast),
      Some(Plan.JoinStrategy.SortMerge),
    )
    Seq("left", "right", "full").foreach { side =>
      strategies.foreach { strategy =>
        Seq(
          (empty, present),
          (present, empty),
          (empty, empty),
        ).foreach { (left, right) =>
          val plan = outerPlan(side, left, right, strategy)
          ColumnarPlanner.tryRun(plan).isDefined shouldBe true
          same(DistCollection(plan))
        }
      }
    }
  }

  it should "keep a null value on the row path" in {
    val left = DistCollection(
      Seq(
        pair(longRef(1L), longRef(10L)),
        pair(longRef(2L), absent),
      ),
      1,
    )
    val right = DistCollection(
      Seq(
        pair(longRef(2L), longRef(20L)),
        pair(longRef(3L), absent),
      ),
      1,
    )
    Seq("left", "right", "full").foreach { side =>
      val plan = outerPlan(side, left, right, None)
      val op = ColumnarPlanner.lower(plan).getOrElse(fail(s"$side did not lower"))
      op.shape shouldBe expectedShape(side, None)
      val thrown = intercept[ColumnarFallback] {
        val _ = ColumnarInterpreter.interpret(plan, op)
      }
      thrown.getMessage shouldBe s"a null value on a $side outer join stays on the row path"
      ColumnarPlanner.tryRun(plan) shouldBe None
      same(DistCollection(plan))
    }
  }

  it should "not match a null build key to probe key 0" in {
    val probeZero = DistCollection(Seq(pair(longRef(0L), longRef(10L))), 1)
    // The present pair is first so the source classifies as `(Long, Long)`.
    val buildNull = DistCollection(
      Seq(
        pair(longRef(1L), longRef(30L)),
        pair(absent, longRef(20L)),
      ),
      1,
    )
    // Left and right broadcast insert the build side. Full hash separates null build keys.
    Seq(
      ("left", Some(Plan.JoinStrategy.Broadcast), probeZero, buildNull),
      ("right", Some(Plan.JoinStrategy.Broadcast), buildNull, probeZero),
      ("full", Some(Plan.JoinStrategy.Broadcast), probeZero, buildNull),
      ("full", None, probeZero, buildNull),
    ).foreach { (side, strategy, left, right) =>
      val plan = outerPlan(side, left, right, strategy)
      ran(plan) should be > 0
      same(DistCollection(plan))
      val (parts, absentSide) = outerColumns(plan)
      assertZeroMissesNull(side, parts, absentSide)
    }
  }

  it should "reject a null value in the kernel with IllegalArgumentException" in {
    val clear = pairBatch(Array(1L), Array(0L), valuePresent = false)
    val present = pairBatch(Array(1L), Array(10L), valuePresent = true)
    val hash = intercept[IllegalArgumentException] {
      val _ = LongOuterPipeline.hashLeftOuterJoin(
        Vector(clear),
        Vector(present),
        numPartitions = 2,
        parallelism = 1,
        batchSize = 4,
        buildBudgetBytes = 1024L * 1024L,
      )
    }
    val broadcast = intercept[IllegalArgumentException] {
      val _ = LongOuterPipeline.broadcastLeftOuterJoin(
        Vector(clear),
        Vector(present),
        parallelism = 1,
        buildBudgetBytes = 1024L * 1024L,
      )
    }
    val sorted = intercept[IllegalArgumentException] {
      val _ = LongSortMerge.leftOuter(clear, present)
    }
    hash.getMessage should include("fully present")
    broadcast.getMessage should include("fully present")
    sorted.getMessage should include("fully present")
  }

  it should "keep a null key off partition 0's present keys" in {
    val left = DistCollection(
      Seq(
        pair(longRef(1L), longRef(10L)),
        pair(absent, longRef(20L)),
      ),
      1,
    )
    val right = DistCollection(
      Seq(
        pair(longRef(0L), longRef(30L)),
        pair(longRef(Long.MinValue), longRef(40L)),
      ),
      1,
    )
    Seq("left", "right", "full").foreach { side =>
      val plan = outerPlan(side, left, right, None)
      ran(plan) should be > 0
      same(DistCollection(plan))
      val (parts, absent) = outerColumns(plan)
      parts.length shouldBe 4
      parts.zipWithIndex.foreach { (batch, part) =>
        val keys = int64(batch, 0)
        (0 until batch.length).foreach { row =>
          keys.isAbsent(row) shouldBe false
          if (!keys.isValid(row)) then part shouldBe 0
          else
            val key = keys.values(row)
            Math.floorMod(java.lang.Long.hashCode(key), parts.length) shouldBe part
        }
        assertDecoded(side, batch, absent)
      }
    }
  }

  it should "throw on an over-budget hash build and write no spill file" in {
    ScarletConf.set(ScarletConf.get.copy(broadcastBuildBudgetBytes = 1L))
    val left = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 4)
    val right = DistCollection.ofLongPairs(Array(1L), Array(100L), 4)
    LongHashJoin.retainedBuildBytes(1) should be > 1L
    val plan = outerPlan("left", left, right, Some(Plan.JoinStrategy.Broadcast))
    val thrown = intercept[IllegalStateException] {
      val _ = ColumnarPlanner.tryRun(plan)
    }
    val estimate = LongHashJoin.retainedBuildBytes(1)
    thrown.getMessage shouldBe
      s"broadcast build table retains about $estimate bytes for 1 rows, " +
        "above the budget of 1 bytes"
    LongHashJoinSpill.filesWrittenForTest shouldBe 0L
    LongHashJoinSpill.lastDirectoryForTest shouldBe ""
    val hashed = outerPlan("left", left, right, Some(Plan.JoinStrategy.ShuffleHash))
    val hashedThrow = intercept[IllegalStateException] {
      val _ = ColumnarPlanner.tryRun(hashed)
    }
    hashedThrow.getMessage shouldBe thrown.getMessage
    LongHashJoinSpill.filesWrittenForTest shouldBe 0L
  }

  it should "not open a build table for an empty left probe" in {
    val build = Vector(pairBatch(Array(1L), Array(10L), valuePresent = true))
    val emptyBatch = pairBatch(Array.empty[Long], Array.empty[Long], valuePresent = true)
    val emptyProbe = LongOuterPipeline.broadcastLeftOuterJoin(
      Vector.empty,
      build,
      parallelism = 1,
      buildBudgetBytes = 1L,
    )
    val zeroProbe = LongOuterPipeline.broadcastLeftOuterJoin(
      Vector(emptyBatch),
      build,
      parallelism = 1,
      buildBudgetBytes = 1L,
    )
    emptyProbe shouldBe Vector.empty[ColumnBatch]
    zeroProbe shouldBe Vector.empty[ColumnBatch]
  }

  it should "throw on an over-budget sort-merge and write no spill file" in {
    ScarletConf.set(
      ScarletConf.get.copy(spillEnabled = true, spillBudgetBytes = 1L, columnarBatchSize = 4),
    )
    val left = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 4)
    val right = DistCollection.ofLongPairs(Array(1L, 3L), Array(100L, 30L), 4)
    val plan = outerPlan("left", left, right, Some(Plan.JoinStrategy.SortMerge))
    val thrown = intercept[IllegalStateException] {
      val _ = ColumnarPlanner.tryRun(plan)
    }
    thrown.getMessage should include("long outer sort-merge spill retains about")
    thrown.getMessage should include("above the budget of 1 bytes")
    SortMergeRuns.filesWrittenForTest shouldBe 0L
    SortMergeRuns.lastDirectoryForTest shouldBe ""
    LongHashJoinSpill.filesWrittenForTest shouldBe 0L
    LongHashJoinSpill.lastDirectoryForTest shouldBe ""
  }

  private def outerPlan(
      side: String,
      left: DistCollection[(Long, Long)],
      right: DistCollection[(Long, Long)],
      strategy: Option[Plan.JoinStrategy],
  ): Plan[_] =
    side match
      case "left" => Plan.LeftOuterJoinOp(left.plan, right.plan, strategy)
      case "right" => Plan.RightOuterJoinOp(left.plan, right.plan, strategy)
      case "full" => Plan.FullOuterJoinOp(left.plan, right.plan, strategy)
      case other => fail(s"unknown side $other")

  private def expectedShape(side: String, strategy: Option[Plan.JoinStrategy]): String =
    val node = (side, strategy) match
      case ("left", None | Some(Plan.JoinStrategy.ShuffleHash)) => "LongHashLeftOuter"
      case ("left", Some(Plan.JoinStrategy.Broadcast)) => "LongBroadcastLeftOuter"
      case ("left", Some(Plan.JoinStrategy.SortMerge)) => "LongSortMergeLeftOuter"
      case ("right", None | Some(Plan.JoinStrategy.ShuffleHash)) => "LongHashRightOuter"
      case ("right", Some(Plan.JoinStrategy.Broadcast)) => "LongBroadcastRightOuter"
      case ("right", Some(Plan.JoinStrategy.SortMerge)) => "LongSortMergeRightOuter"
      case ("full", None | Some(Plan.JoinStrategy.ShuffleHash)) => "LongHashFullOuter"
      case ("full", Some(Plan.JoinStrategy.Broadcast)) => "LongBroadcastFullOuter"
      case ("full", Some(Plan.JoinStrategy.SortMerge)) => "LongSortMergeFullOuter"
      case other => fail(s"unknown outer $other")
    val child =
      if (strategy.contains(Plan.JoinStrategy.Broadcast)) then "Scan" else "Exchange(Scan)"
    s"$node($child, $child)"

  private def ran(plan: Plan[_]): Int =
    ColumnarPlanner.tryRun(plan) match
      case Some(parts) => parts.iterator.map(_.data.size).sum
      case None => fail("columnar path declined")

  private def same[A](joined: DistCollection[A]): Unit =
    joined.collect().toList should contain theSameElementsAs row(joined.collect().toList)

  private def outerColumns(plan: Plan[_]): (Vector[ColumnBatch], AbsentSide) =
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("expected a long outer plan"))
    ColumnarInterpreter.interpret(plan, op) match
      case ColumnarInterpreter.Columns.Outer(parts, absent) => (parts, absent)
      case other => fail(s"expected outer columns, got $other")

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def assertDecoded(side: String, batch: ColumnBatch, absent: AbsentSide): Unit =
    LongOuterDecode.decode(batch, absent).foreach { row =>
      assertNullKeyPartner(side, row)
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def assertZeroMissesNull(
      side: String,
      parts: Vector[ColumnBatch],
      absentSide: AbsentSide,
  ): Unit =
    val rows = parts.flatMap(batch => LongOuterDecode.decode(batch, absentSide))
    val zero = rows.collect { case (0L, partner) => partner }
    zero should not be empty
    zero.foreach {
      case (_: Long, None) if side == "left" => ()
      case (None, _: Long) if side == "right" => ()
      case (Some(_: Long), None) if side == "full" => ()
      case other => fail(s"$side paired key 0 with $other")
    }
    val nullKey = rows.exists {
      case (null, _) => true
      case _ => false
    }
    // Right outer drops an unmatched left row, so a null key on the left is not preserved.
    if (side == "full") then nullKey shouldBe true else nullKey shouldBe false

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def assertNullKeyPartner(side: String, row: Any): Unit =
    row match
      case (null, (_, None)) if side == "left" || side == "full" => ()
      case (null, (None, _)) if side == "right" || side == "full" => ()
      case (null, _) => fail(s"$side matched a null key: $row")
      case _ => ()

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")

  private def pairBatch(
      keys: Array[Long],
      values: Array[Long],
      valuePresent: Boolean,
  ): ColumnBatch =
    val keyColumn = Int64Column.of(keys, Validity.allValid(keys.length), keys.length)
    val valueColumn =
      Int64Column.of(values, Validity.pack(values.length, _ => valuePresent), values.length)
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(keyColumn, valueColumn),
      keys.length,
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def pair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absent: java.lang.Long = null

  private def longRef(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
