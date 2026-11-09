package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * String outer joins. A null value stays on the row path. A missing partner is an absence bit. A
 * null key is a clear validity bit and is not a dictionary lookup. Order is not part of the
 * contract.
 */
class TestStringOuterJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val accent = "é"
  private val decomposed = "e\u0301"

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true, defaultShufflePartitions = 4))
    ScarletRuntime.get.shuffle.clear()
    StringHashJoinSpill.resetForTest()
    SortMergeRuns.resetForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    StringHashJoinSpill.resetForTest()
    SortMergeRuns.resetForTest()
    ()

  "string outer join" should "lower all nine ops and refuse take" in {
    val left = DistCollection(Seq("a" -> 1, "b" -> 2), 4)
    val right = DistCollection(Seq("a" -> 10, "c" -> 3), 4)
    val strategies = Seq(
      None,
      Some(Plan.JoinStrategy.ShuffleHash),
      Some(Plan.JoinStrategy.Broadcast),
      Some(Plan.JoinStrategy.SortMerge),
    )
    Seq("left", "right", "full").foreach { side =>
      strategies.foreach { strategy =>
        val plan = outerPlan(side, left, right, strategy)
        val op = ColumnarPlanner.lower(plan).getOrElse(fail(s"$side $strategy did not lower"))
        op.shape shouldBe expectedShape(side, strategy)
        op.kind shouldBe PhysicalOp.Kind.StringOuter
        (op.kind == PhysicalOp.Kind.StringTriples) shouldBe false
        (op.kind == PhysicalOp.Kind.Triples) shouldBe false
        ColumnarPlanner.tryTake(plan, 1) shouldBe None
      }
    }
  }

  it should "match the row path for each strategy" in {
    val left = DistCollection(Seq("a" -> 1, "a" -> 2, "b" -> 3, "z" -> 0), 4)
    val right = DistCollection(Seq("a" -> 10, "a" -> 11, "c" -> 4), 4)
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
          StringHashJoinSpill.filesWrittenForTest shouldBe 0L
          StringHashJoinSpill.lastDirectoryForTest shouldBe ""
      }
    }
  }

  it should "keep an empty side on the columnar path" in {
    val present = DistCollection(Seq("a" -> 1, "b" -> 2), 4)
    val empty = present.filterKeys((_: String) => false)
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

  it should "match an empty string and keep each accent on its own side" in {
    accent should not be decomposed
    val left = DistCollection(
      Plan.Source(
        Seq(Partition(Seq("b" -> 1, "" -> 2, accent -> 3))),
      ),
    )
    val right = DistCollection(
      Plan.Source(
        Seq(Partition(Seq("" -> 10, "b" -> 20, decomposed -> 4))),
      ),
    )
    val plan = Plan.FullOuterJoinOp(left.plan, right.plan, None)
    ran(plan) should be > 0
    same(DistCollection(plan))
  }

  it should "keep a null value on the row path" in {
    val left = DistCollection(
      Plan.Source(Seq(Partition(Seq("red" -> 1, erasedNullValue("blue"))))),
    )
    val right = DistCollection(
      Plan.Source(Seq(Partition(Seq("blue" -> 2, erasedNullValue("red"))))),
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

  it should "reject a null value in the kernel with IllegalArgumentException" in {
    val clear = stringPair("a", 0, valuePresent = false)
    val present = stringPair("a", 10, valuePresent = true)
    val hash = intercept[IllegalArgumentException] {
      val _ = StringOuterPipeline.hashLeftOuterJoin(
        Vector(clear),
        Vector(present),
        numPartitions = 2,
        parallelism = 1,
        batchSize = 4,
        buildBudgetBytes = 1024L * 1024L,
      )
    }
    val broadcast = intercept[IllegalArgumentException] {
      val _ = StringOuterPipeline.broadcastLeftOuterJoin(
        Vector(clear),
        Vector(present),
        parallelism = 1,
        buildBudgetBytes = 1024L * 1024L,
      )
    }
    val sorted = intercept[IllegalArgumentException] {
      val _ = StringSortMerge.leftOuter(clear, present)
    }
    hash.getMessage should include("fully present")
    broadcast.getMessage should include("fully present")
    sorted.getMessage should include("fully present")
  }

  it should "keep a null key off partition 0's present keys" in {
    // A present pair is first so the source classifies as `(String, Int)`. Code 0 is `"a"` or
    // `""`. A clear key bit must not match either of those.
    val left = DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq("a" -> 1, nullKey(9), "" -> 2)),
        ),
      ),
    )
    val right = DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq("" -> 20, nullKey(8), "a" -> 10)),
        ),
      ),
    )
    Seq("left", "right", "full").foreach { side =>
      val plan = outerPlan(side, left, right, None)
      ran(plan) should be > 0
      same(DistCollection(plan))
      val (parts, absent) = outerColumns(plan)
      parts.length shouldBe 4
      parts.zipWithIndex.foreach { (batch, part) =>
        val keys = utf8(batch)
        val leftValue = int32(batch, 1)
        val rightValue = int32(batch, 2)
        (0 until batch.length).foreach { row =>
          if (!keys.isValid(row)) then
            part shouldBe 0
            absentValue(side, leftValue, rightValue, row)
          else
            val key = keys.dictionary(keys.codes(row))
            Math.floorMod(StringHash.hash(key), parts.length) shouldBe part
        }
        assertDecoded(side, batch, absent)
      }
    }
  }

  it should "throw on an over-budget hash build and write no spill file" in {
    ScarletConf.set(ScarletConf.get.copy(broadcastBuildBudgetBytes = 1L))
    val left = DistCollection(Seq("a" -> 1, "b" -> 2), 4)
    val right = DistCollection(Seq("a" -> 10), 4)
    val plan = outerPlan("left", left, right, Some(Plan.JoinStrategy.Broadcast))
    val thrown = intercept[IllegalStateException] {
      val _ = ColumnarPlanner.tryRun(plan)
    }
    thrown.getMessage should include("string shuffle-hash build piece retains about")
    thrown.getMessage should include("above the budget of 1 bytes")
    (thrown.getMessage.contains("broadcast build table retains about")) shouldBe false
    StringHashJoinSpill.filesWrittenForTest shouldBe 0L
    StringHashJoinSpill.lastDirectoryForTest shouldBe ""
    SortMergeRuns.filesWrittenForTest shouldBe 0L
    val hashed = outerPlan("left", left, right, Some(Plan.JoinStrategy.ShuffleHash))
    val hashedThrow = intercept[IllegalStateException] {
      val _ = ColumnarPlanner.tryRun(hashed)
    }
    hashedThrow.getMessage should include("string shuffle-hash build piece retains about")
    StringHashJoinSpill.filesWrittenForTest shouldBe 0L
  }

  it should "not open a build table for an empty left probe" in {
    val build = Vector(stringPair("a", 10, valuePresent = true))
    val emptyProbe = StringOuterPipeline.broadcastLeftOuterJoin(
      Vector.empty,
      build,
      parallelism = 1,
      buildBudgetBytes = 1L,
    )
    val zero = stringPairBatch(Array.empty[String], Array.empty[Int], valuePresent = true)
    val zeroProbe = StringOuterPipeline.broadcastLeftOuterJoin(
      Vector(zero),
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
    val left = DistCollection(Seq("a" -> 1, "b" -> 2), 4)
    val right = DistCollection(Seq("a" -> 10, "c" -> 3), 4)
    val plan = outerPlan("left", left, right, Some(Plan.JoinStrategy.SortMerge))
    val thrown = intercept[IllegalStateException] {
      val _ = ColumnarPlanner.tryRun(plan)
    }
    thrown.getMessage should include("string outer sort-merge spill")
    thrown.getMessage should include("above the budget of 1 bytes")
    SortMergeRuns.filesWrittenForTest shouldBe 0L
    SortMergeRuns.lastDirectoryForTest shouldBe ""
    StringHashJoinSpill.filesWrittenForTest shouldBe 0L
    StringHashJoinSpill.lastDirectoryForTest shouldBe ""
  }

  private def outerPlan(
      side: String,
      left: DistCollection[(String, Int)],
      right: DistCollection[(String, Int)],
      strategy: Option[Plan.JoinStrategy],
  ): Plan[_] =
    side match
      case "left" => Plan.LeftOuterJoinOp(left.plan, right.plan, strategy)
      case "right" => Plan.RightOuterJoinOp(left.plan, right.plan, strategy)
      case "full" => Plan.FullOuterJoinOp(left.plan, right.plan, strategy)
      case other => fail(s"unknown side $other")

  private def expectedShape(side: String, strategy: Option[Plan.JoinStrategy]): String =
    val node = (side, strategy) match
      case ("left", None | Some(Plan.JoinStrategy.ShuffleHash)) => "StringHashLeftOuter"
      case ("left", Some(Plan.JoinStrategy.Broadcast)) => "StringBroadcastLeftOuter"
      case ("left", Some(Plan.JoinStrategy.SortMerge)) => "StringSortMergeLeftOuter"
      case ("right", None | Some(Plan.JoinStrategy.ShuffleHash)) => "StringHashRightOuter"
      case ("right", Some(Plan.JoinStrategy.Broadcast)) => "StringBroadcastRightOuter"
      case ("right", Some(Plan.JoinStrategy.SortMerge)) => "StringSortMergeRightOuter"
      case ("full", None | Some(Plan.JoinStrategy.ShuffleHash)) => "StringHashFullOuter"
      case ("full", Some(Plan.JoinStrategy.Broadcast)) => "StringBroadcastFullOuter"
      case ("full", Some(Plan.JoinStrategy.SortMerge)) => "StringSortMergeFullOuter"
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
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("expected a string outer plan"))
    ColumnarInterpreter.interpret(plan, op) match
      case ColumnarInterpreter.Columns.Outer(parts, absent) => (parts, absent)
      case other => fail(s"expected outer columns, got $other")

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def assertDecoded(side: String, batch: ColumnBatch, absent: AbsentSide): Unit =
    StringOuterDecode.decode(batch, absent).foreach { row =>
      row match
        case (null, (_, None)) if side == "left" || side == "full" => ()
        case (null, (None, _)) if side == "right" || side == "full" => ()
        case (null, _) => fail(s"$side matched a null key: $row")
        case _ => ()
    }

  private def absentValue(
      side: String,
      leftValue: Int32Column,
      rightValue: Int32Column,
      row: Int,
  ): Unit =
    side match
      case "left" => absentCell(rightValue, row)
      case "right" => absentCell(leftValue, row)
      case "full" =>
        val leftMissing = leftValue.isAbsent(row)
        val rightMissing = rightValue.isAbsent(row)
        (leftMissing || rightMissing) shouldBe true
        absentCell(if (leftMissing) then leftValue else rightValue, row)
      case other => fail(s"unknown side $other")

  private def absentCell(column: Int32Column, row: Int): Unit =
    column.isAbsent(row) shouldBe true
    column.isValid(row) shouldBe false
    column.values(row) shouldBe 0

  private def utf8(batch: ColumnBatch): DictUtf8Column = batch.columns.headOption match
    case Some(column: DictUtf8Column) => column
    case other => fail(s"expected a dictionary key, found $other")

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def stringPair(key: String, value: Int, valuePresent: Boolean): ColumnBatch =
    stringPairBatch(Array(key), Array(value), valuePresent)

  private def stringPairBatch(
      keys: Array[String],
      values: Array[Int],
      valuePresent: Boolean,
  ): ColumnBatch =
    val width = keys.length
    val keyColumn = DictUtf8Column.of(
      keys,
      Array.tabulate(width)(identity),
      Validity.allValid(width),
      width,
    )
    val valueColumn = Int32Column.of(values, Validity.pack(width, _ => valuePresent), width)
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(keyColumn, valueColumn),
      width,
    )

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erasedNullValue(key: String): (String, Int) =
    Tuple2[String, Integer](key, null).asInstanceOf[(String, Int)]

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
