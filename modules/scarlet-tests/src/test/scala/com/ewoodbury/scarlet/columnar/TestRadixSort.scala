package com.ewoodbury.scarlet.columnar

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Radix rank split for one present int or long key.
 *
 * A null key or a null value keeps the comparison path and the null bit. Equal keys keep input
 * order, including across runs. The key range does not change that order. A survivor index radixes
 * when every live key and value is present, and a null in a dropped row does not force the
 * fallback.
 */
class TestRadixSort extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true, spillEnabled = false))
    ScarletRuntime.get.shuffle.clear()
    RadixPass.resetForTest()
    RankSplit.resetForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    RadixPass.resetForTest()
    RankSplit.resetForTest()
    ScarletConf.set(originalConf)
    ()

  "an int radix split" should "order negative, duplicate, and extreme keys like a stable sort" in {
    agreeInts(
      Seq(
        (Int.MaxValue, 1),
        (Int.MinValue, 2),
        (Int.MinValue, 3),
        (-1, 4),
        (0, 5),
        (0, 6),
        (-100, 7),
        (5, 8),
        (1 << 24, 9),
        (-1 << 24, 10),
        (255, 11),
        (-256, 12),
      ),
      inputGroups = 3,
      partitions = 4,
    )
  }

  it should "keep duplicate keys in input order across three runs" in {
    val batches = Vector(
      BatchCodec.intPairs(Seq((2, 0))),
      BatchCodec.intPairs(Seq((1, 1), (2, 2))),
      BatchCodec.intPairs(Seq((2, 3))),
    )
    RadixPass.resetForTest()
    flatInts(RankSplit.sortAndSplit(batches, partitions = 1, parallelism = 4)) shouldBe Seq(
      (1, 1),
      (2, 0),
      (2, 2),
      (2, 3),
    )
    RadixPass.intSortsForTest should be > 0
  }

  it should "keep all-equal keys in input order" in {
    agreeInts((0 until 80).map(index => (-3, index)), inputGroups = 4, partitions = 4)
  }

  it should "return one row without counting a radix sort" in {
    val parts = RankSplit.sortAndSplit(
      Vector(BatchCodec.intPairs(Seq((5, 9)))),
      partitions = 4,
      parallelism = 2,
    )
    flatInts(parts) shouldBe Seq((5, 9))
    parts.map(_.length) shouldBe RankBounds.outputSizes(1, 4).toVector
    RadixPass.intSortsForTest shouldBe 0
  }

  it should "return empty partitions for an empty input" in {
    val empty = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val parts = RankSplit.sortAndSplit(Vector(empty, empty), partitions = 3, parallelism = 1)
    parts should have length 3
    parts.foreach { batch =>
      batch.length shouldBe 0
      batch.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int32)
    }
    RadixPass.intSortsForTest shouldBe 0
  }

  it should "fall back when a key is null and keep the null bit" in {
    val rows = Seq(
      (Some(1), Some(10)),
      (None, Some(99)),
      (Some(-5), Some(3)),
      (Some(0), Some(7)),
      (None, None),
    )
    agreeNullableInts(rows)
  }

  it should "fall back when a value is null and keep the null bit" in {
    agreeNullableInts(
      Seq(
        (Some(2), None),
        (Some(-1), Some(4)),
        (Some(2), Some(5)),
      ),
    )
  }

  it should "radix a selection that skips a null and fall back when that null is live" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq(
        (Some(3), Some(1)),
        (None, Some(9)),
        (Some(1), Some(2)),
        (Some(3), Some(0)),
        (Some(4), None),
      ),
    )
    val skipped = Selected(batch, Array(0, 2, 3), 3)
    RadixPass.resetForTest()
    val skippedParts =
      RankSplit.sortAndSplitSelected(Vector(skipped), partitions = 1, parallelism = 1)
    intCells(skippedParts) shouldBe Seq((Some(1), Some(2)), (Some(3), Some(1)), (Some(3), Some(0)))
    RadixPass.intSortsForTest shouldBe 1

    val liveNull = Selected(batch, Array(0, 1, 2, 3), 4)
    RadixPass.resetForTest()
    val nullParts =
      RankSplit.sortAndSplitSelected(Vector(liveNull), partitions = 1, parallelism = 1)
    intCells(nullParts) shouldBe Seq(
      (None, Some(9)),
      (Some(1), Some(2)),
      (Some(3), Some(1)),
      (Some(3), Some(0)),
    )
    RadixPass.intSortsForTest shouldBe 0

    val liveValue = Selected(batch, Array(0, 2, 4), 3)
    RadixPass.resetForTest()
    val valueParts =
      RankSplit.sortAndSplitSelected(Vector(liveValue), partitions = 1, parallelism = 1)
    intCells(valueParts) shouldBe Seq((Some(1), Some(2)), (Some(3), Some(1)), (Some(4), None))
    RadixPass.intSortsForTest shouldBe 0
  }

  it should "ignore an empty run and match a stable sort of a random int input" in {
    val random = new java.util.Random(58L)
    val rows = (0 until 4096).map { index =>
      (random.nextInt(), index)
    }
    val batches = groupedInts(rows, 5) :+ BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val expected = rows.sortBy(_._1)
    RadixPass.resetForTest()
    val serial = RankSplit.sortAndSplit(batches, partitions = 4, parallelism = 1)
    val sorts = RadixPass.intSortsForTest
    RadixPass.resetForTest()
    val parallel = RankSplit.sortAndSplit(batches, partitions = 4, parallelism = 4)
    flatInts(serial) shouldBe expected
    flatInts(parallel) shouldBe expected
    serial.map(_.length) shouldBe RankBounds.outputSizes(rows.length, 4).toVector
    parallel.map(_.length) shouldBe serial.map(_.length)
    sorts should be > 0
    RadixPass.intSortsForTest should be > 0
  }

  it should "match the row sort on a filtered permutation of small keys" in {
    val random = new scala.util.Random(58)
    val keys = random.shuffle((0 until 240).toVector).toArray
    val values = Array.tabulate(240)(index => index % 17)
    ScarletConf.set(
      ScarletConf.get.copy(
        columnarExecution = true,
        spillEnabled = false,
        threadPoolSize = 4,
        defaultShufflePartitions = 4,
      ),
    )
    val source = DistCollection.ofIntPairs(keys, values, 4)
    val sorted = source.filterKeys((key: Int) => key % 3 != 0).sortByIntKey
    RadixPass.resetForTest()
    val columnar = sorted.collect().toSeq
    val expected = keys.indices
      .map(index => (keys(index), values(index)))
      .filter(_._1 % 3 != 0)
      .sortBy(_._1)
    columnar shouldBe expected
    columnar shouldBe rowCollect(sorted)
    RadixPass.intSortsForTest should be > 0
  }

  "a long radix split" should "order negative, duplicate, and extreme keys like a stable sort" in {
    agreeLongs(
      Seq(
        (Long.MaxValue, 1L),
        (Long.MinValue, 2L),
        (Long.MinValue, 3L),
        (-1L, 4L),
        (0L, 5L),
        (0L, 6L),
        (-100L, 7L),
        (5L, 8L),
        (1L << 56, 9L),
        (-1L << 56, 10L),
      ),
      inputGroups = 3,
      partitions = 4,
    )
  }

  it should "keep all-equal long keys in input order" in {
    agreeLongs((0 until 80).map(index => (-3L, index.toLong)), inputGroups = 4, partitions = 4)
  }

  it should "return one long row and empty long partitions without a radix sort" in {
    val one = RankSplitLong.sortAndSplit(
      Vector(BatchCodec.copiedLongPairs(Array(5L), Array(9L))),
      partitions = 4,
      parallelism = 1,
    )
    flatLongs(one) shouldBe Seq((5L, 9L))
    one.map(_.length) shouldBe RankBounds.outputSizes(1, 4).toVector
    val empty = RankSplitLong.sortAndSplit(
      Vector(BatchCodec.copiedLongPairs(Array.empty[Long], Array.empty[Long])),
      partitions = 2,
      parallelism = 1,
    )
    empty.map(_.length) shouldBe Vector(0, 0)
    empty.foreach { batch =>
      batch.schema shouldBe Vector(LogicalType.Int64, LogicalType.Int64)
    }
    RadixPass.longSortsForTest shouldBe 0
  }

  it should "fall back when a long key or value is null" in {
    agreeNullableLongs(
      Seq(
        (Some(1L), Some(10L)),
        (None, Some(99L)),
        (Some(Long.MinValue), Some(3L)),
        (Some(0L), None),
        (None, None),
      ),
    )
  }

  it should "radix a long selection that skips a null key" in {
    val batch = nullableLongPairs(
      Seq(
        (Some(4L), Some(1L)),
        (None, Some(8L)),
        (Some(-2L), Some(3L)),
      ),
    )
    RadixPass.resetForTest()
    val parts = RankSplitLong.sortAndSplitSelected(
      Vector(Selected(batch, Array(0, 2), 2)),
      partitions = 1,
      parallelism = 1,
    )
    longCells(parts) shouldBe Seq((Some(-2L), Some(3L)), (Some(4L), Some(1L)))
    RadixPass.longSortsForTest shouldBe 1
  }

  it should "match a stable sort of a random long input at one and four threads" in {
    val random = new java.util.Random(58L)
    val rows = (0 until 4096).map(index => (random.nextLong(), index.toLong))
    val batches = groupedLongs(rows, 5)
    val expected = rows.sortBy(_._1)
    RadixPass.resetForTest()
    val serial = RankSplitLong.sortAndSplit(batches, partitions = 4, parallelism = 1)
    val sorts = RadixPass.longSortsForTest
    RadixPass.resetForTest()
    val parallel = RankSplitLong.sortAndSplit(batches, partitions = 4, parallelism = 4)
    flatLongs(serial) shouldBe expected
    flatLongs(parallel) shouldBe expected
    serial.map(_.length) shouldBe RankBounds.outputSizes(rows.length, 4).toVector
    sorts should be > 0
    RadixPass.longSortsForTest should be > 0
  }

  it should "match the row sort on filtered long pairs" in {
    val keys = Array.tabulate(180)(index => (index.toLong * 13L) % 40L - 15L)
    val values = Array.tabulate(180)(index => index.toLong)
    val source = DistCollection.ofLongPairs(keys, values, 3)
    val sorted = source.filterKeys((key: Long) => key % 3L != 0L).sortByLongKey
    RadixPass.resetForTest()
    val columnar = sorted.collect().toSeq
    val expected = keys.indices
      .map(index => (keys(index), values(index)))
      .filter(_._1 % 3L != 0L)
      .sortBy(_._1)
    columnar shouldBe expected
    columnar shouldBe rowCollect(sorted)
    RadixPass.longSortsForTest should be > 0
  }

  private def agreeInts(rows: Seq[(Int, Int)], inputGroups: Int, partitions: Int): Unit =
    val batches = groupedInts(rows, inputGroups)
    val expected = rows.sortBy(_._1)
    RadixPass.resetForTest()
    val serial = RankSplit.sortAndSplit(batches, partitions, parallelism = 1)
    val sorts = RadixPass.intSortsForTest
    RadixPass.resetForTest()
    val parallel = RankSplit.sortAndSplit(batches, partitions, parallelism = 4)
    flatInts(serial) shouldBe expected
    flatInts(parallel) shouldBe expected
    serial.map(_.length) shouldBe RankBounds.outputSizes(rows.length, partitions).toVector
    parallel.map(_.length) shouldBe serial.map(_.length)
    if (batches.exists(_.length > 1)) then
      sorts should be > 0
      RadixPass.intSortsForTest should be > 0

  private def agreeLongs(rows: Seq[(Long, Long)], inputGroups: Int, partitions: Int): Unit =
    val batches = groupedLongs(rows, inputGroups)
    val expected = rows.sortBy(_._1)
    RadixPass.resetForTest()
    val serial = RankSplitLong.sortAndSplit(batches, partitions, parallelism = 1)
    val sorts = RadixPass.longSortsForTest
    RadixPass.resetForTest()
    val parallel = RankSplitLong.sortAndSplit(batches, partitions, parallelism = 4)
    flatLongs(serial) shouldBe expected
    flatLongs(parallel) shouldBe expected
    serial.map(_.length) shouldBe RankBounds.outputSizes(rows.length, partitions).toVector
    if (batches.exists(_.length > 1)) then
      sorts should be > 0
      RadixPass.longSortsForTest should be > 0

  private def agreeNullableInts(rows: Seq[(Option[Int], Option[Int])]): Unit =
    val batch = BatchCodec.nullableIntPairs(rows)
    RadixPass.resetForTest()
    val parts = RankSplit.sortAndSplit(Vector(batch), partitions = 3, parallelism = 2)
    intCells(parts) shouldBe rows.sortBy(pair => pair._1.getOrElse(0))
    parts.map(_.length) shouldBe RankBounds.outputSizes(rows.length, 3).toVector
    RadixPass.intSortsForTest shouldBe 0

  private def agreeNullableLongs(rows: Seq[(Option[Long], Option[Long])]): Unit =
    RadixPass.resetForTest()
    val parts = RankSplitLong.sortAndSplit(
      Vector(nullableLongPairs(rows)),
      partitions = 3,
      parallelism = 2,
    )
    longCells(parts) shouldBe rows.sortBy(pair => pair._1.getOrElse(0L))
    RadixPass.longSortsForTest shouldBe 0

  private def groupedInts(rows: Seq[(Int, Int)], groups: Int): Vector[ColumnBatch] =
    val size = math.max(1, (rows.length + groups - 1) / groups)
    rows.grouped(size).map(group => BatchCodec.intPairs(group)).toVector

  private def groupedLongs(rows: Seq[(Long, Long)], groups: Int): Vector[ColumnBatch] =
    val size = math.max(1, (rows.length + groups - 1) / groups)
    rows.grouped(size).map(group => BatchCodec.longPairs(group)).toVector

  private def flatInts(parts: Vector[ColumnBatch]): Seq[(Int, Int)] =
    parts.flatMap(batch => BatchCodec.decodeIntPairs(batch))

  private def flatLongs(parts: Vector[ColumnBatch]): Seq[(Long, Long)] =
    parts.flatMap(batch => BatchCodec.decodeLongPairs(batch))

  private def intCells(parts: Vector[ColumnBatch]): Seq[(Option[Int], Option[Int])] =
    parts.flatMap { batch =>
      val keys = ColumnHash.int32At(batch, 0)
      val values = ColumnHash.int32At(batch, 1)
      Seq.tabulate(batch.length) { row =>
        (
          if keys.isValid(row) then Some(keys.values(row)) else None,
          if values.isValid(row) then Some(values.values(row)) else None,
        )
      }
    }

  private def longCells(parts: Vector[ColumnBatch]): Seq[(Option[Long], Option[Long])] =
    parts.flatMap { batch =>
      val keys = ColumnHash.int64At(batch, 0)
      val values = ColumnHash.int64At(batch, 1)
      Seq.tabulate(batch.length) { row =>
        (
          if keys.isValid(row) then Some(keys.values(row)) else None,
          if values.isValid(row) then Some(values.values(row)) else None,
        )
      }
    }

  private def nullableLongPairs(rows: Seq[(Option[Long], Option[Long])]): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.fromNullable(rows.map(_._1)),
        Int64Column.fromNullable(rows.map(_._2)),
      ),
      rows.length,
    )

  private def rowCollect[A](collection: DistCollection[A]): Seq[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      collection.collect().toSeq
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
