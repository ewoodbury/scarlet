package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{BatchCodec, ColumnBatch, Int64Column, LogicalType}
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.HashPartitioner

class TestLongPairReduce extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val wide = (1L << 32) + 1L

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true, defaultShufflePartitions = 4))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "reduceByKey" should "match the row path for extreme keys across four partitions" in {
    val keys = Array(-1L, -4L, Long.MinValue, Long.MaxValue, wide, 7L, 7L, 1L)
    val values = Array(1L, 2L, 3L, 4L, 5L, 6L, 8L, 9L)
    val rows = DistCollection.ofLongPairs(keys, values, 4)
    val reduced = rows.reduceByKey[Long, Long](_ + _)
    val columnar = reduced.collect().toList
    val expected = row(rows.reduceByKey[Long, Long](_ + _).collect().toList)

    columnar should contain theSameElementsAs expected
    columnar should contain theSameElementsAs Seq(
      (-1L, 1L),
      (-4L, 2L),
      (Long.MinValue, 3L),
      (Long.MaxValue, 4L),
      (wide, 5L),
      (7L, 14L),
      (1L, 9L),
    )
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "LongHashAggregate(Exchange(Scan))",
    )
    ColumnarPlanner.tryTake(reduced.plan, 1) shouldBe None
    val parts = longPairBatches(reduced.plan)
    parts.length shouldBe 4
    val partitioner = new HashPartitioner
    parts.zipWithIndex.foreach { (batch, index) =>
      val keyColumn = int64(batch, 0)
      (0 until batch.length).foreach { row =>
        keyColumn.isValid(row) shouldBe true
        val key = keyColumn.values(row)
        partitioner.partition(key, parts.length) shouldBe index
        Math.floorMod(java.lang.Long.hashCode(key), parts.length) shouldBe index
      }
      assertValidity(batch)
    }
    Math.floorMod(wide, 4) shouldBe 1
    Math.floorMod(java.lang.Long.hashCode(wide), 4) shouldBe 0
    Math.floorMod(java.lang.Long.hashCode(Long.MinValue), 4) shouldBe 0
  }

  it should "match the row path for null keys and null values" in {
    val input = Seq(
      pair(longRef(1L), longRef(10L)),
      pair(longRef(1L), absent),
      pair(absent, longRef(7L)),
      pair(longRef(2L), absent),
      pair(longRef(3L), longRef(4L)),
      pair(longRef(3L), longRef(5L)),
      pair(longRef(4L), absent),
      pair(longRef(4L), absent),
    )
    val rows = DistCollection(input, 1)
    val reduced = rows.reduceByKey[Long, Long](_ + _)
    val columnar = reduced.collect().toList
    val expected = Seq(
      pair(longRef(1L), longRef(10L)),
      pair(longRef(2L), absent),
      pair(longRef(3L), longRef(9L)),
      pair(longRef(4L), longRef(0L)),
    )

    columnar should contain theSameElementsAs row(rows.reduceByKey[Long, Long](_ + _).collect().toList)
    columnar should contain theSameElementsAs expected
    longPairBatches(reduced.plan).foreach(assertValidity)
  }

  it should "unbox a null to zero for a non-additive operator" in {
    val input = Seq(
      pair(longRef(1L), longRef(10L)),
      pair(longRef(1L), absent),
      pair(longRef(2L), absent),
      pair(longRef(2L), longRef(4L)),
      pair(longRef(3L), longRef(10L)),
      pair(longRef(3L), absent),
      pair(longRef(4L), absent),
      pair(longRef(4L), longRef(8L)),
    )
    val rows = DistCollection(input, 1)
    val multiplied = rows.reduceByKey[Long, Long](_ * _)
    val kept = rows.reduceByKey[Long, Long]((left, _) => left)

    multiplied.collect().toList should contain theSameElementsAs row(
      rows.reduceByKey[Long, Long](_ * _).collect().toList,
    )
    multiplied.collect().toList should contain theSameElementsAs Seq(
      pair(longRef(1L), longRef(0L)),
      pair(longRef(2L), longRef(0L)),
      pair(longRef(3L), longRef(0L)),
      pair(longRef(4L), longRef(0L)),
    )
    kept.collect().toList should contain theSameElementsAs row(
      rows.reduceByKey[Long, Long]((left, _) => left).collect().toList,
    )
    kept.collect().toList should contain theSameElementsAs Seq(
      pair(longRef(1L), longRef(10L)),
      pair(longRef(2L), longRef(0L)),
      pair(longRef(3L), longRef(10L)),
      pair(longRef(4L), longRef(0L)),
    )
    longPairBatches(multiplied.plan).foreach(assertValidity)
    longPairBatches(kept.plan).foreach(assertValidity)
  }

  it should "match the row path when morsels split one partition" in {
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 2, threadPoolSize = 2),
    )
    val keys = Array.tabulate(20)(index => (index % 5).toLong)
    val values = Array.tabulate(20)(index => index.toLong + 1L)
    val rows = DistCollection.ofLongPairs(keys, values, 4)
    val columnar = rows.reduceByKey[Long, Long](_ + _).collect().toList

    columnar should contain theSameElementsAs row(
      rows.reduceByKey[Long, Long](_ + _).collect().toList,
    )
  }

  it should "unbox a missing value that meets a present value in a later morsel" in {
    // One input partition, so partial combine merges every morsel before the exchange. Key 1 is
    // null in one morsel and 5 in a later one: that merge unboxes the null to 0 and applies the
    // operator. Key 2 has a single null row. A lone null stays null. The exchange and the bucket
    // reduce each see one row per key.
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 1, threadPoolSize = 2),
    )
    // The first row is a present pair, so classification witnesses `(Long, Long)` before any null.
    val input = Seq(
      pair(longRef(3L), longRef(4L)),
      pair(longRef(1L), absent),
      pair(longRef(2L), absent),
      pair(longRef(1L), longRef(5L)),
    )
    val rows = DistCollection(input, 1)
    val combine = (left: Long, right: Long) => left + right + 1L
    val reduced = rows.reduceByKey[Long, Long](combine)
    val columnar = reduced.collect().toList
    val literal = Seq(
      pair(longRef(1L), longRef(6L)),
      pair(longRef(2L), absent),
      pair(longRef(3L), longRef(4L)),
    )

    columnar should contain theSameElementsAs literal
    columnar should contain theSameElementsAs row(
      rows.reduceByKey[Long, Long](combine).collect().toList,
    )
    longPairBatches(reduced.plan).foreach(assertValidity)
  }

  it should "merge two missing values from separate morsels of one input batch" in {
    // Batch size 1 puts each row in its own morsel. One input partition makes partial combine
    // merge those morsels map-side. Key 1 is null in two morsels, so the merge applies the
    // operator to two zeros. Key 2 is a lone null and stays null. The exchange then sees one
    // row per key.
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 1, threadPoolSize = 2),
    )
    val input = Seq(
      pair(longRef(3L), longRef(4L)),
      pair(longRef(1L), absent),
      pair(longRef(2L), absent),
      pair(longRef(1L), absent),
    )
    val rows = DistCollection(input, 1)
    val combine = (left: Long, right: Long) => left + right + 1L
    val reduced = rows.reduceByKey[Long, Long](combine)
    val columnar = reduced.collect().toList
    val literal = Seq(
      pair(longRef(1L), longRef(1L)),
      pair(longRef(2L), absent),
      pair(longRef(3L), longRef(4L)),
    )

    columnar should contain theSameElementsAs literal
    columnar should contain theSameElementsAs row(
      rows.reduceByKey[Long, Long](combine).collect().toList,
    )
    longPairBatches(reduced.plan).foreach(assertValidity)
    ColumnarPlanner.tryTake(reduced.plan, 1) shouldBe None
  }

  it should "merge two missing values that arrive in separate input partitions" in {
    // Three partitions and batch size 1. Key 1 is null in the first partition and again in the
    // third, so each partial stays missing and the exchange places both in one bucket. The bucket
    // reduce applies the operator to two zeros. Key 2 is a lone null in its own partition.
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 1, threadPoolSize = 2),
    )
    val input = Seq(
      pair(longRef(3L), longRef(4L)),
      pair(longRef(1L), absent),
      pair(longRef(2L), absent),
      pair(longRef(1L), absent),
    )
    val rows = DistCollection(input, 3)
    val combine = (left: Long, right: Long) => left + right + 1L
    val reduced = rows.reduceByKey[Long, Long](combine)
    val columnar = reduced.collect().toList
    val literal = Seq(
      pair(longRef(1L), longRef(1L)),
      pair(longRef(2L), absent),
      pair(longRef(3L), longRef(4L)),
    )

    columnar should contain theSameElementsAs literal
    columnar should contain theSameElementsAs row(
      rows.reduceByKey[Long, Long](combine).collect().toList,
    )
    longPairBatches(reduced.plan).foreach(assertValidity)
    ColumnarPlanner.tryTake(reduced.plan, 1) shouldBe None
  }

  it should "collect an empty source as no rows and four empty batches" in {
    val rows = DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], 4)
    val reduced = rows.reduceByKey[Long, Long](_ + _)

    reduced.collect() shouldEqual Seq.empty[(Long, Long)]
    row(rows.reduceByKey[Long, Long](_ + _).collect()) shouldEqual Seq.empty[(Long, Long)]
    val parts = longPairBatches(reduced.plan)
    parts.length shouldBe 4
    parts.foreach { batch =>
      batch.length shouldBe 0
      batch.schema shouldBe Vector(LogicalType.Int64, LogicalType.Int64)
    }
  }

  "take" should "return the same long-pair prefix as the row path, in partition order" in {
    val rows = DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq((10L, 1L), (1L, 2L))),
          Partition(Seq.empty[(Long, Long)]),
          Partition(Seq((30L, 3L), (2L, 4L))),
        ),
        Plan.SourceDescriptor.LongPairs,
      ),
    )
    rows.plan match
      case Plan.Source(partitions, descriptor) =>
        descriptor shouldBe Plan.SourceDescriptor.LongPairs
        partitions.map(_.data.toList) shouldBe Seq(
          List((10L, 1L), (1L, 2L)),
          Nil,
          List((30L, 3L), (2L, 4L)),
        )
      case other => fail(s"expected a long-pair source, got $other")

    val prefix = rows.take(3)
    prefix shouldBe List((10L, 1L), (1L, 2L), (30L, 3L))
    prefix shouldBe row(rows.take(3))
    ColumnarPlanner.tryTake(rows.plan, 3).map(_.toList) shouldBe Some(prefix)
  }

  "a long-pair source" should "keep keys, values, mapValues, and filter on the row path" in {
    val rows = DistCollection.ofLongPairs(Array(1L, -2L, 1L), Array(10L, 20L, 5L), 4)
    val keyed = rows.keys
    val valued = rows.values
    val mapped = rows.mapValues[Long, Long, Long](_ + 1L)
    val filtered = rows.filter { case (key, _) => key < 0L }

    ColumnarPlanner.partitioning(keyed.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(valued.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(mapped.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(filtered.plan) shouldBe Option.empty[PartitioningInfo]
    keyed.collect().toList should contain theSameElementsAs row(keyed.collect().toList)
    valued.collect().toList should contain theSameElementsAs row(valued.collect().toList)
    mapped.collect().toList should contain theSameElementsAs row(mapped.collect().toList)
    filtered.collect().toList should contain theSameElementsAs row(filtered.collect().toList)
  }

  private def longPairBatches(plan: Plan[_]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail(s"expected a lowered plan, got $plan"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.LongPairs(parts) => parts
      case other => fail(s"expected long pairs, got $other")

  private def assertValidity(batch: ColumnBatch): Unit =
    val keyColumn = int64(batch, 0)
    val valueColumn = int64(batch, 1)
    val decoded = BatchCodec.decodeLongPairs(batch)
    batch.schema shouldBe Vector(LogicalType.Int64, LogicalType.Int64)
    decoded.zipWithIndex.foreach { (row, index) =>
      keyColumn.isValid(index) shouldBe true
      valueColumn.isValid(index) shouldBe !valueMissing(row)
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

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def valueMissing(row: (Long, Long)): Boolean =
    row.productElement(1).asInstanceOf[AnyRef] eq null

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
