package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Columnar rank split of a long-pair scan. A null key orders as zero and stays null. A null value
 * stays null. Ties keep input order. Partition boundaries are [[RankBounds]], shared with the int
 * split. There is no spill: over the budget the in-memory split still runs.
 */
class TestSortByLongKey extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    RankSplitRuns.resetForTest()
    ()

  "sortByLongKey" should "match the row path for distinct, duplicate, and negative keys" in {
    agree(Seq(4L -> 1L, 1L -> 2L, 3L -> 3L, 2L -> 4L))
    agree((0 until 40).map(index => (index.toLong % 3L, index.toLong)))
    agree(
      Seq(
        Long.MinValue -> 1L,
        -1L -> 2L,
        0L -> 3L,
        Long.MaxValue -> 4L,
        -100L -> 5L,
        5L -> 6L,
      ),
    )
  }

  it should "retain null keys and null values and order a null key as zero" in {
    val rows = Seq(
      longPair(boxedLong(1L), boxedLong(10L)),
      longPair(noLong, boxedLong(99L)),
      longPair(boxedLong(-5L), boxedLong(3L)),
      longPair(boxedLong(0L), boxedLong(7L)),
      longPair(noLong, noLong),
    )
    val source = DistCollection(rows, 3)
    val sorted = source.sortByLongKey
    val columnar = collect(sorted)
    val row = rowCollect(sorted)

    columnar shouldBe row
    columnar shouldBe rows.sortBy(effectiveKey)
    effectiveKeys(columnar) shouldBe effectiveKeys(columnar).sorted
    assertBatchValidity(sortedParts(sorted.plan), rows.sortBy(effectiveKey))
  }

  it should "retain a null value beside a present key" in {
    val rows = Seq(
      longPair(boxedLong(2L), boxedLong(20L)),
      longPair(boxedLong(-1L), noLong),
      longPair(boxedLong(2L), boxedLong(21L)),
      longPair(boxedLong(0L), boxedLong(1L)),
    )
    val source = DistCollection(rows, 2)
    val sorted = source.sortByLongKey
    val columnar = collect(sorted)

    columnar shouldBe rowCollect(sorted)
    columnar should contain theSameElementsAs(rows)
    assertBatchValidity(sortedParts(sorted.plan), rows.sortBy(effectiveKey))
  }

  it should "sort an empty long-pair source into empty output partitions" in {
    val source = DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], 4)
    val sorted = source.sortByLongKey
    val parts = sortedParts(sorted.plan)

    collect(sorted) shouldBe Seq.empty[(Long, Long)]
    rowCollect(sorted) shouldBe Seq.empty[(Long, Long)]
    parts.length shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach { batch =>
      batch.length shouldBe 0
      batch.schema shouldBe Vector(LogicalType.Int64, LogicalType.Int64)
    }
  }

  it should "split ranks so partition sizes differ by at most one" in {
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = 8))
    val source = DistCollection.ofLongPairs(Array(2L, 0L, 1L), Array(20L, 0L, 10L), 2)
    val parts = sortedParts(source.sortByLongKey.plan)

    parts.map(_.length) shouldBe Vector(1, 0, 1, 0, 0, 1, 0, 0)
    assertBoundaries(parts)
    parts.flatMap(batch => BatchCodec.decodeLongPairs(batch)) shouldBe Seq(
      0L -> 0L,
      1L -> 10L,
      2L -> 20L,
    )
  }

  it should "report range distribution, ascending order, and SortByLongKey(Scan)" in {
    val source = DistCollection.ofLongPairs(Array(3L, 1L, 2L), Array(30L, 10L, 20L), 2)
    val sorted = source.sortByLongKey
    val info = ColumnarPlanner.partitioning(sorted.plan).getOrElse(fail("expected a layout"))

    info.distribution shouldBe Distribution.Range(
      ScarletConf.get.defaultShufflePartitions,
      PartitioningInfo.RowKeyOrdinals,
    )
    info.ordering shouldBe OrderingTag.Sorted(PartitioningInfo.RowKeyOrdinals, Seq(true))
    info.layout shouldBe Layout.Columnar(ScarletConf.get.columnarBatchSize)
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some("SortByLongKey(Scan)")
    ColumnarPlanner.lower(sorted.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.LongPairs)
  }

  it should "decode the same prefix as the row path, including zero and past the end" in {
    val source = DistCollection.ofLongPairs(
      Array(3L, -1L, 3L, 0L, 2L, 8L, -4L, 1L),
      Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L),
      3,
    )
    val sorted = source.sortByLongKey
    val full = rowCollect(sorted)
    Seq(0, 1, 2, full.length, full.length + 5).foreach { count =>
      ColumnarPlanner.tryTake(sorted.plan, count) shouldBe Some(full.take(count))
      sorted.take(count) shouldBe rowTake(sorted, count)
      sorted.take(count) shouldBe full.take(count).toList
    }
  }

  it should "decode a prefix that keeps null keys and null values" in {
    val rows = Seq(
      longPair(boxedLong(1L), boxedLong(10L)),
      longPair(noLong, boxedLong(99L)),
      longPair(boxedLong(-5L), boxedLong(3L)),
      longPair(boxedLong(0L), boxedLong(7L)),
      longPair(noLong, noLong),
      longPair(boxedLong(2L), noLong),
    )
    val source = DistCollection(rows, 3)
    val sorted = source.sortByLongKey
    val full = rowCollect(sorted)
    Seq(0, 2, 4, full.length).foreach { count =>
      val taken = ColumnarPlanner.tryTake(sorted.plan, count)
      taken shouldBe Some(full.take(count))
      sorted.take(count) shouldBe full.take(count).toList
    }
    val prefix = ColumnarPlanner.tryTake(sorted.plan, 4).getOrElse(fail("missing prefix"))
    prefix.exists(pair => !present(pair, 0)) shouldBe true
    prefix.exists(pair => !present(pair, 1)) shouldBe true
  }

  it should "leave a filter above the sort, and any non-scan child, on the row path" in {
    val source = DistCollection.ofLongPairs(Array(3L, 1L, 2L, 2L), Array(30L, 10L, 20L, 21L), 2)
    val above = source.sortByLongKey.filter((pair: (Long, Long)) => pair._2 != 20L)
    val below = source.filter((pair: (Long, Long)) => pair._1 > 0L).sortByLongKey
    val mapped = source.map((pair: (Long, Long)) => (pair._1, pair._2)).sortByLongKey
    val rewritten = source.mapValues((value: Long) => value + 1L).sortByLongKey

    ColumnarPlanner.partitioning(above.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.lower(above.plan) shouldBe Option.empty[PhysicalOp]
    collect(above) shouldBe rowCollect(above)
    ColumnarPlanner.partitioning(below.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(mapped.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(rewritten.plan) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(source.sortByLongKey.keys.plan) shouldBe
      Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(source.sortByLongKey.values.plan) shouldBe
      Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(
      source.sortByLongKey.reduceByKey((left: Long, right: Long) => left + right).plan,
    ) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.partitioning(source.sortByLongKey.join(source).plan) shouldBe
      Option.empty[PartitioningInfo]
  }

  it should "stay in memory when spill is on and the budget is exhausted" in {
    ScarletConf.set(ScarletConf.get.copy(spillEnabled = true, spillBudgetBytes = 1L))
    RankSplitRuns.resetForTest()
    val source = DistCollection.ofLongPairs(Array(3L, 1L, 2L, 2L), Array(30L, 10L, 20L, 21L), 2)
    val sorted = source.sortByLongKey

    collect(sorted) shouldBe rowCollect(sorted)
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some("SortByLongKey(Scan)")
    RankSplitRuns.filesWrittenForTest shouldBe 0L
  }

  it should "match the int rank split on partition sizes and relative order" in {
    val rows = Seq(
      (Some(2), Some(20)),
      (Some(1), Some(10)),
      (None, Some(99)),
      (Some(1), Some(11)),
      (Some(0), Some(1)),
      (Some(1), Some(12)),
      (Some(3), Some(30)),
      (None, Some(98)),
    )
    val intParts = RankSplit.sortAndSplit(
      Vector(
        BatchCodec.nullableIntPairs(rows.take(4)),
        BatchCodec.nullableIntPairs(rows.drop(4)),
      ),
      partitions = 3,
      parallelism = 2,
    )
    val longParts = RankSplitLong.sortAndSplit(
      Vector(longNullable(rows.take(4)), longNullable(rows.drop(4))),
      partitions = 3,
      parallelism = 2,
    )

    longParts.map(_.length) shouldBe intParts.map(_.length)
    longOrder(longParts) shouldBe intOrder(intParts)
  }

  "RankSplitLong" should "keep cross-partition ties in input order and place ranks by r * partitions / rows" in {
    val left = BatchCodec.longPairs(Seq((5L, 50L), (1L, 10L)))
    val right = BatchCodec.longPairs(Seq((1L, 11L), (4L, 40L), (0L, 0L)))
    val parts = RankSplitLong.sortAndSplit(Vector(left, right), partitions = 2, parallelism = 1)
    val ints = RankSplit.sortAndSplit(
      Vector(
        BatchCodec.intPairs(Seq((5, 50), (1, 10))),
        BatchCodec.intPairs(Seq((1, 11), (4, 40), (0, 0))),
      ),
      partitions = 2,
      parallelism = 1,
    )

    val first = parts.lift(0).getOrElse(fail("missing partition 0"))
    val second = parts.lift(1).getOrElse(fail("missing partition 1"))
    parts.map(_.length) shouldBe ints.map(_.length)
    parts.map(_.length) shouldBe Vector(3, 2)
    BatchCodec.decodeLongPairs(first) shouldBe Seq(0L -> 0L, 1L -> 10L, 1L -> 11L)
    BatchCodec.decodeLongPairs(second) shouldBe Seq(4L -> 40L, 5L -> 50L)
    assertBoundaries(parts)
  }

  it should "order a null key as zero even when its payload is not zero" in {
    val keys = Int64Column.of(Array(7L, 1L), Validity.pack(2, row => row == 1), 2)
    val values = Int64Column.of(Array(10L, 20L), Validity.allValid(2), 2)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(keys, values),
      2,
    )
    val parts = RankSplitLong.sortAndSplit(Vector(batch), partitions = 1, parallelism = 1)
    val sorted = parts.headOption.getOrElse(fail("expected one partition"))
    val outKeys = longColumn(sorted, 0)
    val outValues = longColumn(sorted, 1)

    outKeys.isValid(0) shouldBe false
    outValues.isValid(0) shouldBe true
    outValues.values(0) shouldBe 10L
    outKeys.isValid(1) shouldBe true
    outKeys.values(1) shouldBe 1L
    outValues.values(1) shouldBe 20L
  }

  it should "reject an empty batch vector, a non-positive width, and a non-positive parallelism" in {
    val empty = BatchCodec.longPairs(Seq.empty[(Long, Long)])
    val missing = the[IllegalArgumentException] thrownBy {
      RankSplitLong.sortAndSplit(Vector.empty, partitions = 2, parallelism = 1)
    }
    val width = the[IllegalArgumentException] thrownBy {
      RankSplitLong.sortAndSplit(Vector(empty), partitions = 0, parallelism = 1)
    }
    val negative = the[IllegalArgumentException] thrownBy {
      RankSplitLong.sortAndSplit(Vector(empty), partitions = -1, parallelism = 1)
    }
    val workers = the[IllegalArgumentException] thrownBy {
      RankSplitLong.sortAndSplit(Vector(empty), partitions = 1, parallelism = 0)
    }

    missing.getMessage should include("sortAndSplit needs at least one batch")
    width.getMessage should include("numPartitions must be positive, got 0")
    negative.getMessage should include("numPartitions must be positive, got -1")
    workers.getMessage should include("parallelism must be positive, got 0")
  }

  it should "return one empty pair batch per output partition when every input row is absent" in {
    val empty = BatchCodec.longPairs(Seq.empty[(Long, Long)])
    val parts = RankSplitLong.sortAndSplit(Vector(empty, empty), partitions = 3, parallelism = 1)

    parts should have length 3
    parts.foreach { batch =>
      batch.length shouldBe 0
      batch.schema shouldBe Vector(LogicalType.Int64, LogicalType.Int64)
    }
  }

  it should "agree for parallelism greater than one" in {
    val batches = Vector(
      BatchCodec.longPairs(Seq((3L, 1L), (1L, 2L))),
      BatchCodec.longPairs(Seq((2L, 3L))),
      BatchCodec.longPairs(Seq((2L, 4L), (0L, 5L), (4L, 6L))),
      BatchCodec.longPairs(Seq((-1L, 8L), (1L, 7L))),
    )
    val serial = RankSplitLong.sortAndSplit(batches, partitions = 3, parallelism = 1)
    val parallel = RankSplitLong.sortAndSplit(batches, partitions = 3, parallelism = 2)

    parallel.map(batch => BatchCodec.decodeLongPairs(batch)) shouldBe
      serial.map(batch => BatchCodec.decodeLongPairs(batch))
    parallel.map(validityBits) shouldBe serial.map(validityBits)
    assertBoundaries(parallel)
  }

  private def longNullable(rows: Seq[(Option[Int], Option[Int])]): ColumnBatch =
    BatchCodec.longPairs(
      rows.map { (key, value) =>
        longPair(
          key.fold(noLong)(item => boxedLong(item.toLong)),
          value.fold(noLong)(item => boxedLong(item.toLong)),
        )
      },
    )

  private def intOrder(
      parts: Vector[ColumnBatch],
  ): Seq[Seq[(Option[Long], Option[Long])]] =
    parts.map { batch =>
      BatchCodec.decodeNullableIntPairs(batch).map { (key, value) =>
        (key.map(_.toLong), value.map(_.toLong))
      }
    }

  private def longOrder(
      parts: Vector[ColumnBatch],
  ): Seq[Seq[(Option[Long], Option[Long])]] =
    parts.map { batch =>
      val keys = longColumn(batch, 0)
      val values = longColumn(batch, 1)
      (0 until batch.length).map { row =>
        val key = if (keys.isValid(row)) Some(keys.values(row)) else None
        val value = if (values.isValid(row)) Some(values.values(row)) else None
        (key, value)
      }
    }

  private def agree(rows: Seq[(Long, Long)]): Unit =
    val source = DistCollection(rows, 4)
    val sorted = source.sortByLongKey
    val columnar = collect(sorted)
    val row = rowCollect(sorted)

    columnar shouldBe row
    effectiveKeys(columnar) shouldBe effectiveKeys(columnar).sorted
    assertBoundaries(sortedParts(sorted.plan))

  private def assertBoundaries(parts: Vector[ColumnBatch]): Unit =
    val span = parts.foldLeft(Option.empty[(Int, Int)]) { (bounds, batch) =>
      val length = batch.length
      bounds match
        case None => Some((length, length))
        case Some((low, high)) => Some((math.min(low, length), math.max(high, length)))
    }
    span.foreach { (low, high) =>
      (high - low) should be <= 1
    }
    parts.foreach(assertSortedPartition)
    parts.zip(parts.drop(1)).foreach { (current, next) =>
      if (current.length > 0 && next.length > 0) then
        lastEffective(current) should be <= firstEffective(next)
    }
    val keys = parts.flatMap { batch =>
      val column = longColumn(batch, 0)
      (0 until batch.length).map(row => effectiveAt(column, row))
    }
    keys shouldBe keys.sorted

  private def assertSortedPartition(batch: ColumnBatch): Unit =
    val keys = longColumn(batch, 0)
    (0 until batch.length - 1).foreach { row =>
      effectiveAt(keys, row) should be <= effectiveAt(keys, row + 1)
    }

  private def assertBatchValidity(
      parts: Vector[ColumnBatch],
      expected: Seq[(Long, Long)],
  ): Unit =
    val located = parts.flatMap { batch =>
      (0 until batch.length).map(row => (batch, row))
    }
    located.length shouldBe expected.length
    located.zip(expected).foreach { case ((batch, row), pair) =>
      val keys = longColumn(batch, 0)
      val values = longColumn(batch, 1)
      keys.isValid(row) shouldBe present(pair, 0)
      values.isValid(row) shouldBe present(pair, 1)
      if (present(pair, 0)) then keys.values(row) shouldBe storedLong(pair, 0)
      if (present(pair, 1)) then values.values(row) shouldBe storedLong(pair, 1)
    }

  private def sortedParts(plan: Plan[(Long, Long)]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered sort"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.LongPairs(parts) => parts
      case other => fail(s"expected long pairs, got $other")

  private def validityBits(batch: ColumnBatch): Seq[(Boolean, Boolean)] =
    val keys = longColumn(batch, 0)
    val values = longColumn(batch, 1)
    (0 until batch.length).map(row => (keys.isValid(row), values.isValid(row)))

  private def longColumn(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal).getOrElse(fail(s"missing column $ordinal")) match
      case column: Int64Column => column
      case other => fail(s"column $ordinal is ${other.logicalType}")

  private def effectiveAt(column: Int64Column, row: Int): Long =
    if (column.isValid(row)) column.values(row) else 0L

  private def firstEffective(batch: ColumnBatch): Long =
    effectiveAt(longColumn(batch, 0), 0)

  private def lastEffective(batch: ColumnBatch): Long =
    effectiveAt(longColumn(batch, 0), batch.length - 1)

  private def effectiveKeys(rows: Seq[(Long, Long)]): Seq[Long] =
    rows.map(effectiveKey)

  private def effectiveKey(pair: (Long, Long)): Long =
    if (present(pair, 0)) storedLong(pair, 0) else 0L

  private def present(pair: (Long, Long), ordinal: Int): Boolean =
    pair.productElement(ordinal) match
      case _: java.lang.Long => true
      case _ => false

  private def storedLong(pair: (Long, Long), ordinal: Int): Long =
    pair.productElement(ordinal) match
      case key: java.lang.Long => key.longValue()
      case other => fail(s"expected a long component, found $other")

  private def collect[A](collection: DistCollection[A]): Seq[A] =
    ScarletRuntime.get.shuffle.clear()
    collection.collect().toSeq

  private def rowCollect[A](collection: DistCollection[A]): Seq[A] =
    rowAction(collection.collect().toSeq)

  private def rowTake[A](collection: DistCollection[A], count: Int): List[A] =
    rowAction(collection.take(count))

  private def rowAction[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def longPair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noLong: java.lang.Long = null

  private def boxedLong(value: Long): java.lang.Long = java.lang.Long.valueOf(value)
