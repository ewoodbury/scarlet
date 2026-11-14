package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Long inner sort-merge against the row path.
 *
 * [[JoinDigest.fromLong]] compares the multiset. A direct kernel check pins match order. The
 * over-budget case throws and writes no spill file.
 */
class TestLongSortMergeJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val shape = "LongSortMergeJoin(Exchange(Scan), Exchange(Scan))"

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

  "long inner sort-merge" should "match the row path when either side is empty" in {
    Seq(1, 4).foreach { width =>
      val empty = DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], width)
      val present =
        DistCollection.ofLongPairs(Array(1L, -4L, Long.MinValue), Array(2L, 5L, 7L), width)
      val noRows = Seq.empty[(Long, (Long, Long))]
      agree(empty.sortMergeJoin(present), noRows, width)
      agree(present.sortMergeJoin(empty), noRows, width)
      agree(empty.sortMergeJoin(empty), noRows, width)
    }
  }

  it should "emit duplicate keys in match order and keep a null value" in {
    // The last left row is a null key whose payload is 0. It must not match key 0.
    val left = batch(
      Array(2L, 1L, 0L, 1L, 0L),
      Array(true, true, true, true, false),
      Array(20L, 10L, 7L, 11L, 99L),
      Array(true, true, true, false, true),
    )
    val right = batch(
      Array(1L, 3L, 1L, 0L, 0L),
      Array(true, true, true, true, false),
      Array(100L, 30L, 0L, 8L, 5L),
      Array(true, true, false, true, true),
    )
    // Stable sort, then each left row of a group with every right row of that group.
    // Key 0, then the two left 1s against (100, null). Key 2 has no partner.
    val joined = LongSortMerge.inner(left, right)
    BatchCodec.decodeLongTriplesPreservingAbsence(joined) shouldBe Seq(
      (0L, pair(longRef(7L), longRef(8L))),
      (1L, pair(longRef(10L), longRef(100L))),
      (1L, pair(longRef(10L), absent)),
      (1L, pair(absent, longRef(100L))),
      (1L, pair(absent, absent)),
    )
  }

  it should "drop a null key and agree with the row path" in {
    Seq(1, 4).foreach { width =>
      val left = DistCollection(
        Seq(
          pair(longRef(1L), longRef(10L)),
          pair(absent, longRef(99L)),
          pair(longRef(0L), longRef(7L)),
          pair(longRef(4L), longRef(40L)),
        ),
        width,
      )
      val right = DistCollection(
        Seq(
          pair(longRef(1L), longRef(100L)),
          pair(absent, longRef(7L)),
          pair(longRef(0L), longRef(8L)),
          pair(longRef(4L), absent),
        ),
        width,
      )
      val expected = Seq(
        (0L, (7L, 8L)),
        (1L, (10L, 100L)),
        (4L, pair(longRef(40L), absent)),
      )
      agree(left.sortMergeJoin(right), expected, width)
    }
  }

  it should "match a key present ten times on one side" in {
    Seq(1, 4).foreach { width =>
      val leftKeys = Array.tabulate(10)(_ => 5L) :+ 9L
      val leftValues = Array.tabulate(10)(_.toLong) :+ 90L
      val left = DistCollection.ofLongPairs(leftKeys, leftValues, width)
      val right = DistCollection.ofLongPairs(Array(5L, 5L, 8L), Array(100L, 101L, 80L), width)
      val expected = for
        leftValue <- 0L until 10L
        rightValue <- Seq(100L, 101L)
      yield (5L, (leftValue, rightValue))
      agree(left.sortMergeJoin(right), expected, width)
    }
  }

  it should "lower to LongSortMergeJoin and leave hash and broadcast on their nodes" in {
    val left = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 4)
    val right = DistCollection.ofLongPairs(Array(1L, 3L), Array(100L, 30L), 4)
    ColumnarPlanner.lower(left.sortMergeJoin(right).plan).map(_.shape) shouldBe Some(shape)
    ColumnarPlanner.lower(left.sortMergeJoin(right).plan).map(_.kind) shouldBe Some(
      PhysicalOp.Kind.LongTriples,
    )
    ColumnarPlanner.lower(left.join(right).plan).map(_.shape) shouldBe Some(
      "LongHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(left.shuffleHashJoin(right).plan).map(_.shape) shouldBe Some(
      "LongHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(left.broadcastJoin(right).plan).map(_.shape) shouldBe Some(
      "LongBroadcastJoin(Scan, Scan)",
    )
    ColumnarPlanner.tryTake(left.sortMergeJoin(right).plan, 1) shouldBe None
  }

  it should "throw on an over-budget sort-merge and write no spill file" in {
    ScarletConf.set(
      ScarletConf.get.copy(spillEnabled = true, spillBudgetBytes = 1L, columnarBatchSize = 4),
    )
    val left = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 4)
    val right = DistCollection.ofLongPairs(Array(1L, 3L), Array(100L, 30L), 4)
    val plan = left.sortMergeJoin(right).plan
    val thrown = intercept[IllegalStateException] {
      val _ = ColumnarPlanner.tryRun(plan)
    }
    thrown should not be a[ColumnarFallback]
    thrown.getMessage should include("long inner sort-merge spill retains about")
    thrown.getMessage should include("above the budget of 1 bytes")
    SortMergeRuns.filesWrittenForTest shouldBe 0L
    SortMergeRuns.lastDirectoryForTest shouldBe ""
    LongHashJoinSpill.filesWrittenForTest shouldBe 0L
    LongHashJoinSpill.lastDirectoryForTest shouldBe ""
    val collected = intercept[IllegalStateException] {
      val _ = left.sortMergeJoin(right).collect()
    }
    collected should not be a[ColumnarFallback]
    SortMergeRuns.filesWrittenForTest shouldBe 0L
    LongHashJoinSpill.filesWrittenForTest shouldBe 0L
  }

  private def agree(
      joined: DistCollection[(Long, (Long, Long))],
      expected: Seq[(Long, (Long, Long))],
      width: Int,
  ): Unit =
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = width))
    val columnar = joined.collect().toList
    val rowRows = row(joined.collect().toList)
    columnar should contain theSameElementsAs expected
    val left = JoinDigest.fromLong(columnar)
    val right = JoinDigest.fromLong(rowRows)
    withClue(JoinDigest.difference(left, right)) {
      JoinDigest.agrees(left, right) shouldBe true
    }
    ColumnarPlanner.lower(joined.plan).map(_.shape) shouldBe Some(shape)
    longTripleBatches(joined.plan).length shouldBe width

  private def longTripleBatches(plan: Plan[_]): Vector[ColumnBatch] =
    val lowered =
      ColumnarPlanner.lower(plan).getOrElse(fail(s"expected a lowered plan, got $plan"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.LongTriples(parts) => parts
      case other => fail(s"expected long triples, got $other")

  private def batch(
      keys: Array[Long],
      keyPresent: Array[Boolean],
      values: Array[Long],
      valuePresent: Array[Boolean],
  ): ColumnBatch =
    val keyColumn =
      Int64Column.of(keys, Validity.pack(keys.length, row => keyPresent(row)), keys.length)
    val valueColumn =
      Int64Column.of(values, Validity.pack(values.length, row => valuePresent(row)), values.length)
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
