package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

class TestLongKeyJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val wide = 1L << 32
  private val tripleSchema = Vector(LogicalType.Int64, LogicalType.Int64, LogicalType.Int64)

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true, defaultShufflePartitions = 4))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "long inner join" should "match the row path when either side is empty" in {
    val empty = DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], 4)
    val present = DistCollection.ofLongPairs(Array(1L, -4L, Long.MinValue), Array(2L, 5L, 7L), 4)
    val noRows = Seq.empty[(Long, (Long, Long))]

    agree(empty.join(present), noRows)
    agree(present.join(empty), noRows)
    agree(empty.join(empty), noRows)
    agree(empty.shuffleHashJoin(present), noRows)
  }

  it should "emit the cartesian product of duplicate keys" in {
    val left = DistCollection.ofLongPairs(Array(1L, 1L, 2L), Array(10L, 11L, 20L), 4)
    val right = DistCollection.ofLongPairs(Array(1L, 1L, 3L), Array(100L, 101L, 30L), 4)
    val expected = Seq(
      (1L, (10L, 100L)),
      (1L, (10L, 101L)),
      (1L, (11L, 100L)),
      (1L, (11L, 101L)),
    )

    agree(left.join(right), expected)
    agree(left.shuffleHashJoin(right), expected)
  }

  it should "drop a null key on each side" in {
    // The first row is a present pair, so classification witnesses `(Long, Long)` before any null.
    val left = DistCollection(
      Seq(
        pair(longRef(1L), longRef(10L)),
        pair(absent, longRef(99L)),
        pair(longRef(4L), longRef(40L)),
      ),
      4,
    )
    val right = DistCollection(
      Seq(
        pair(longRef(1L), longRef(100L)),
        pair(absent, longRef(7L)),
        pair(longRef(4L), longRef(8L)),
      ),
      4,
    )
    val expected = Seq((1L, (10L, 100L)), (4L, (40L, 8L)))

    agree(left.join(right), expected)
  }

  it should "drop a key present on only one side" in {
    val left = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 4)
    val right = DistCollection.ofLongPairs(Array(1L, 3L), Array(100L, 30L), 4)

    agree(left.join(right), Seq((1L, (10L, 100L))))
  }

  it should "match negative keys and Long.MinValue across four partitions" in {
    val left = DistCollection.ofLongPairs(
      Array(Long.MinValue, -4L, -1L, 0L, 7L),
      Array(1L, 2L, 3L, 4L, 5L),
      4,
    )
    val right = DistCollection.ofLongPairs(
      Array(Long.MinValue, -4L, -1L, 8L),
      Array(10L, 20L, 30L, 80L),
      4,
    )
    val expected = Seq(
      (Long.MinValue, (1L, 10L)),
      (-4L, (2L, 20L)),
      (-1L, (3L, 30L)),
    )

    val joined = left.join(right)
    agree(joined, expected)
    val parts = longTripleBatches(joined.plan)
    parts.length shouldBe 4
    parts.zipWithIndex.foreach { (batch, index) =>
      assertPresent(batch)
      val keys = int64(batch, 0)
      (0 until batch.length).foreach { row =>
        Math.floorMod(java.lang.Long.hashCode(keys.values(row)), parts.length) shouldBe index
      }
    }
    Math.floorMod(java.lang.Long.hashCode(Long.MinValue), 4) shouldBe 0
  }

  it should "keep distinct keys that share a hash code in one partition" in {
    java.lang.Long.hashCode(1L) shouldBe java.lang.Long.hashCode(wide)
    val left = DistCollection.ofLongPairs(Array(1L, wide, 2L), Array(10L, 11L, 20L), 4)
    val right = DistCollection.ofLongPairs(Array(1L, wide, wide), Array(100L, 200L, 201L), 4)
    val expected = Seq(
      (1L, (10L, 100L)),
      (wide, (11L, 200L)),
      (wide, (11L, 201L)),
    )

    val joined = left.join(right)
    agree(joined, expected)
    val parts = longTripleBatches(joined.plan)
    parts.length shouldBe 4
    val home = Math.floorMod(java.lang.Long.hashCode(1L), 4)
    val matched = parts.lift(home).getOrElse(fail("missing partition"))
    BatchCodec.decodeLongTriples(matched).map(_._1).distinct.sorted shouldBe Seq(1L, wide).sorted
    parts.zipWithIndex.foreach { (batch, index) =>
      if (index != home) then BatchCodec.decodeLongTriples(batch).map(_._1) should not contain wide
    }
  }

  it should "report the hash layout and keep take on the row path" in {
    val left = DistCollection.ofLongPairs(Array(1L, -4L), Array(10L, 20L), 2)
    val right = DistCollection.ofLongPairs(Array(1L, 3L), Array(100L, 30L), 2)
    val joined = left.join(right)
    val hinted = left.shuffleHashJoin(right)
    val layout = Layout.Columnar(ScarletConf.get.columnarBatchSize)
    val info = PartitioningInfo(
      Distribution.Hash(4, PartitioningInfo.RowKeyOrdinals),
      OrderingTag.Unsorted,
      layout,
    )

    ColumnarPlanner.partitioning(joined.plan) shouldBe Some(info)
    ColumnarPlanner.partitioning(hinted.plan) shouldBe Some(info)
    ColumnarPlanner.lower(joined.plan).map(_.shape) shouldBe Some(
      "LongHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(hinted.plan).map(_.shape) shouldBe Some(
      "LongHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(joined.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.LongTriples)
    ColumnarPlanner.tryTake(joined.plan, 1) shouldBe None
    ColumnarPlanner.tryTake(hinted.plan, 1) shouldBe None
    joined.take(1) shouldBe row(joined.take(1))
    longTripleBatches(joined.plan).foreach(assertPresent)
  }

  it should "keep a matched null value" in {
    // The first row of each source is a present pair, so classification witnesses (Long, Long).
    // Each null value is a later row and shares its key with the other side.
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
        pair(longRef(1L), absent),
      ),
      1,
    )
    val joined = left.join(right)
    joined.plan match
      case Plan.JoinOp(Plan.Source(_, leftDescriptor), Plan.Source(_, rightDescriptor), _) =>
        leftDescriptor shouldBe Plan.SourceDescriptor.Unknown
        rightDescriptor shouldBe Plan.SourceDescriptor.Unknown
      case other => fail(s"expected an unknown-descriptor join, got $other")
    ColumnarPlanner.lower(joined.plan).map(_.shape) shouldBe Some(
      "LongHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.tryRun(joined.plan).isDefined shouldBe true
    val expected = Seq(
      (1L, pair(longRef(10L), absent)),
      (2L, pair(absent, longRef(20L))),
    )
    val collected = joined.collect().toList
    collected should contain theSameElementsAs expected
    collected should contain theSameElementsAs row(joined.collect().toList)
  }

  it should "match the row path for an explicit broadcast hint" in {
    Seq(1, 4).foreach { width =>
      val empty = DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], width)
      val present =
        DistCollection.ofLongPairs(Array(1L, -4L, Long.MinValue), Array(2L, 5L, 7L), width)
      val noRows = Seq.empty[(Long, (Long, Long))]
      broadcastAgrees(empty.broadcastJoin(present), noRows, width)
      broadcastAgrees(present.broadcastJoin(empty), noRows, width)
      broadcastAgrees(empty.broadcastJoin(empty), noRows, width)

      val left = DistCollection.ofLongPairs(Array(1L, 1L, 2L), Array(10L, 11L, 20L), width)
      val right = DistCollection.ofLongPairs(Array(1L, 1L, 3L), Array(100L, 101L, 30L), width)
      val duplicates = Seq(
        (1L, (10L, 100L)),
        (1L, (10L, 101L)),
        (1L, (11L, 100L)),
        (1L, (11L, 101L)),
      )
      broadcastAgrees(left.broadcastJoin(right), duplicates, width)

      val nullKeysLeft = DistCollection(
        Seq(
          pair(longRef(1L), longRef(10L)),
          pair(absent, longRef(99L)),
          pair(longRef(4L), longRef(40L)),
        ),
        width,
      )
      val nullKeysRight = DistCollection(
        Seq(
          pair(longRef(1L), longRef(100L)),
          pair(absent, longRef(7L)),
          pair(longRef(4L), longRef(8L)),
        ),
        width,
      )
      broadcastAgrees(
        nullKeysLeft.broadcastJoin(nullKeysRight),
        Seq((1L, (10L, 100L)), (4L, (40L, 8L))),
        width,
      )

      val nullValuesLeft = DistCollection(
        Seq(
          pair(longRef(1L), longRef(10L)),
          pair(longRef(2L), absent),
        ),
        width,
      )
      val nullValuesRight = DistCollection(
        Seq(
          pair(longRef(2L), longRef(20L)),
          pair(longRef(1L), absent),
        ),
        width,
      )
      val nullValues = Seq(
        (1L, pair(longRef(10L), absent)),
        (2L, pair(absent, longRef(20L))),
      )
      val joined = nullValuesLeft.broadcastJoin(nullValuesRight)
      val collected = joined.collect().toList
      collected should contain theSameElementsAs nullValues
      collected should contain theSameElementsAs row(joined.collect().toList)
      longTripleBatches(joined.plan).length shouldBe width
    }
  }

  it should "lower an explicit broadcast hint to a scan join and an absent hint to a shuffle" in {
    val left = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 4)
    val right = DistCollection.ofLongPairs(Array(1L, 3L), Array(100L, 30L), 4)
    val broadcast = left.broadcastJoin(right)
    val plain = left.join(right)

    ColumnarPlanner.lower(broadcast.plan).map(_.shape) shouldBe Some(
      "LongBroadcastJoin(Scan, Scan)",
    )
    ColumnarPlanner.lower(plain.plan).map(_.shape) shouldBe Some(
      "LongHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    val lowered =
      ColumnarPlanner.lower(broadcast.plan).getOrElse(fail("broadcast was refused"))
    lowered.shape should not include "Exchange"
    PartitionPreserving.of(lowered) shouldBe Option.empty[Int]
    ColumnarPlanner.tryTake(broadcast.plan, 1) shouldBe None
  }

  it should "fail an over-budget broadcast build without a columnar fallback or a spill file" in {
    val left = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 1)
    val right = DistCollection.ofLongPairs(Array(1L, 1L, 3L), Array(100L, 101L, 30L), 1)
    val rows = 3
    val estimate = LongHashJoin.retainedBuildBytes(rows)
    estimate should be > 1L
    LongHashJoinSpill.resetForTest()
    ScarletConf.set(ScarletConf.get.copy(broadcastBuildBudgetBytes = estimate - 1L))
    val plan = left.broadcastJoin(right).plan
    val thrown = intercept[IllegalStateException] {
      val _ = ColumnarPlanner.tryRun(plan)
    }
    thrown should not be a[ColumnarFallback]
    thrown.getMessage shouldBe
      s"broadcast build table retains about $estimate bytes for $rows rows, " +
        s"above the budget of ${estimate - 1L} bytes"
    LongHashJoinSpill.filesWrittenForTest shouldBe 0L
    an[IllegalStateException] should be thrownBy left.broadcastJoin(right).collect()
    LongHashJoinSpill.filesWrittenForTest shouldBe 0L
  }

  it should "keep a broadcast hint on a long outer join on the same nodes" in {
    val left = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 4)
    val right = DistCollection.ofLongPairs(Array(1L, 3L), Array(100L, 30L), 4)
    val hint = Some(Plan.JoinStrategy.Broadcast)
    val leftOuter = DistCollection(Plan.LeftOuterJoinOp(left.plan, right.plan, hint))
    val rightOuter = DistCollection(Plan.RightOuterJoinOp(left.plan, right.plan, hint))
    val fullOuter = DistCollection(Plan.FullOuterJoinOp(left.plan, right.plan, hint))
    ColumnarPlanner.lower(leftOuter.plan).map(_.shape) shouldBe Some(
      "LongBroadcastLeftOuter(Scan, Scan)",
    )
    ColumnarPlanner.lower(rightOuter.plan).map(_.shape) shouldBe Some(
      "LongBroadcastRightOuter(Scan, Scan)",
    )
    ColumnarPlanner.lower(fullOuter.plan).map(_.shape) shouldBe Some(
      "LongBroadcastFullOuter(Scan, Scan)",
    )
    leftOuter.collect().toList should contain theSameElementsAs row(leftOuter.collect().toList)
    rightOuter.collect().toList should contain theSameElementsAs row(rightOuter.collect().toList)
    fullOuter.collect().toList should contain theSameElementsAs row(fullOuter.collect().toList)
  }

  it should "lower long inner sort-merge and keep an unhinted outer join on the hash node" in {
    val left = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 4)
    val right = DistCollection.ofLongPairs(Array(1L, 3L), Array(100L, 30L), 4)
    ColumnarPlanner.lower(left.sortMergeJoin(right).plan).map(_.shape) shouldBe Some(
      "LongSortMergeJoin(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(left.join(right).plan).map(_.shape) shouldBe Some(
      "LongHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(left.broadcastJoin(right).plan).map(_.shape) shouldBe Some(
      "LongBroadcastJoin(Scan, Scan)",
    )
    ColumnarPlanner.lower(left.shuffleHashJoin(right).plan).map(_.shape) shouldBe Some(
      "LongHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    val leftOuter = left.leftOuterJoin(right)
    val rightOuter = left.rightOuterJoin(right)
    val fullOuter = left.fullOuterJoin(right)
    ColumnarPlanner.lower(leftOuter.plan).map(_.shape) shouldBe Some(
      "LongHashLeftOuter(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(rightOuter.plan).map(_.shape) shouldBe Some(
      "LongHashRightOuter(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(fullOuter.plan).map(_.shape) shouldBe Some(
      "LongHashFullOuter(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(leftOuter.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.LongOuter)
    ColumnarPlanner.tryRun(leftOuter.plan).isDefined shouldBe true
    ColumnarPlanner.tryRun(rightOuter.plan).isDefined shouldBe true
    ColumnarPlanner.tryRun(fullOuter.plan).isDefined shouldBe true
    leftOuter.collect().toList should contain theSameElementsAs row(leftOuter.collect().toList)
    rightOuter.collect().toList should contain theSameElementsAs row(rightOuter.collect().toList)
    fullOuter.collect().toList should contain theSameElementsAs row(fullOuter.collect().toList)
  }

  private def broadcastAgrees(
      joined: DistCollection[(Long, (Long, Long))],
      expected: Seq[(Long, (Long, Long))],
      width: Int,
  ): Unit =
    val columnar = joined.collect().toList
    columnar should contain theSameElementsAs expected
    columnar should contain theSameElementsAs row(joined.collect().toList)
    longTripleBatches(joined.plan).length shouldBe width

  private def agree(
      joined: DistCollection[(Long, (Long, Long))],
      expected: Seq[(Long, (Long, Long))],
  ): Unit =
    val columnar = joined.collect().toList
    columnar should contain theSameElementsAs expected
    columnar should contain theSameElementsAs row(joined.collect().toList)
    val parts = longTripleBatches(joined.plan)
    parts.length shouldBe 4
    parts.foreach(assertPresent)

  private def longTripleBatches(plan: Plan[_]): Vector[ColumnBatch] =
    val lowered =
      ColumnarPlanner.lower(plan).getOrElse(fail(s"expected a lowered plan, got $plan"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.LongTriples(parts) => parts
      case other => fail(s"expected long triples, got $other")

  private def assertPresent(batch: ColumnBatch): Unit =
    batch.schema shouldBe tripleSchema
    val decoded = BatchCodec.decodeLongTriples(batch)
    decoded.length shouldBe batch.length
    (0 until 3).foreach { ordinal =>
      val column = int64(batch, ordinal)
      (0 until batch.length).foreach(row => column.isValid(row) shouldBe true)
    }

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")

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
