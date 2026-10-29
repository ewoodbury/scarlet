package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{BatchCodec, ColumnBatch, Int64Column, LogicalType}
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
      if (index != home) then
        BatchCodec.decodeLongTriples(batch).map(_._1) should not contain wide
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

  it should "keep a matched null value on the row path" in {
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
    ColumnarPlanner.tryRun(joined.plan).isEmpty shouldBe true
    val expected = Seq(
      (1L, pair(longRef(10L), absent)),
      (2L, pair(absent, longRef(20L))),
    )
    val collected = joined.collect().toList
    collected should contain theSameElementsAs expected
    collected should contain theSameElementsAs row(joined.collect().toList)
  }

  it should "keep broadcast, sort-merge, and outer joins on the row path" in {
    val left = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 4)
    val right = DistCollection.ofLongPairs(Array(1L, 3L), Array(100L, 30L), 4)
    refuse(left.broadcastJoin(right))
    refuse(left.sortMergeJoin(right))
    refuse(left.leftOuterJoin(right))
    refuse(left.rightOuterJoin(right))
    refuse(left.fullOuterJoin(right))
  }

  private def refuse[A](collection: DistCollection[A]): Unit =
    ColumnarPlanner.lower(collection.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.partitioning(collection.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.tryTake(collection.plan, 1) shouldBe Option.empty[Seq[A]]
    collection.collect().toList should contain theSameElementsAs row(collection.collect().toList)

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
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail(s"expected a lowered plan, got $plan"))
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
