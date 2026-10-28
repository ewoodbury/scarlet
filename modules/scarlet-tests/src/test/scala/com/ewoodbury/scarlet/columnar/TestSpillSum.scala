package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.ScarletConf

class TestSpillSum extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    SumSpill.resetForTest()

  "spill defaults" should "stay off and match the store caps" in {
    val conf = ScarletConf()

    conf.spillEnabled shouldBe false
    conf.spillBudgetBytes shouldBe 64L * 1024L * 1024L
    conf.spillDirectory shouldBe ""
    conf.spillMaxFiles shouldBe SpillStore.DefaultMaxFiles
    conf.spillMaxBytes shouldBe SpillStore.DefaultMaxBytes
    conf.spillFanIn shouldBe 8
    SumSpill.retainedBytes(0) shouldBe 0L
  }

  "a bucket at the budget" should "sum in memory and write no spill files" in {
    val batches = twoPasses
    val memory = sum(batches)
    // Two partials of 32 groups, one partition, so the exchanged bucket is 64 rows.
    val budget = SumSpill.retainedBytes(twoPassRows)

    withConf(budget) { dir =>
      val summed = sum(batches)

      groups(summed) shouldBe groups(memory)
      summed.length shouldBe 1
      SumSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a bucket over a tiny budget" should "spill and match the in-memory group map" in {
    val batches = twoPasses
    val memory = sum(batches)

    withConf(1L) { dir =>
      val spilled = sum(batches)

      groups(spilled) shouldBe groups(memory)
      groups(spilled) shouldBe (0 until 32).map(key => key -> Some((key + 2).toLong)).toMap
      SumSpill.filesWrittenForTest should be > 1L
      Path.of(SumSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  it should "keep an all-null group null and drop a null key" in {
    val batches = Vector(
      BatchCodec.nullableIntPairs(
        Seq((None, Some(4)), (Some(7), None), (Some(8), Some(1)), (Some(0), None)),
      ),
      BatchCodec.nullableIntPairs(Seq((Some(7), None), (Some(8), Some(2)))),
    )
    val memory = sum(batches)

    withConf(1L) { dir =>
      val spilled = sum(batches)
      val grouped = groups(spilled)

      grouped shouldBe groups(memory)
      grouped shouldBe Map(0 -> None, 7 -> None, 8 -> Some(3L))
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "negative keys" should "land in a floorMod bucket after a spill" in {
    val batches = Vector(
      BatchCodec.intPairs(Seq((Int.MinValue, 2), (-1, 5), (-4, 1), (3, 9))),
      BatchCodec.intPairs(Seq((Int.MinValue, 3), (-1, 7), (0, 4), (-4, 2))),
    )
    val memory = sum(batches, partitions = 4, parallelism = 4)

    withConf(1L) { dir =>
      val spilled = sum(batches, partitions = 4, parallelism = 4)

      spilled.length shouldBe 4
      groups(spilled) shouldBe groups(memory)
      groups(spilled) shouldBe Map(
        Int.MinValue -> Some(5L),
        -1 -> Some(12L),
        -4 -> Some(3L),
        3 -> Some(9L),
        0 -> Some(4L),
      )
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "return one empty batch per partition and write nothing" in {
    withConf(1L) { dir =>
      val parts = sum(Vector.empty, partitions = 4)

      parts.length shouldBe 4
      parts.map(_.length) shouldBe Seq(0, 0, 0, 0)
      SumSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill files" should "be gone after a successful merge and after a failed one" in {
    val batches = twoPasses

    withConf(1L) { dir =>
      val spilled = sum(batches)

      groups(spilled).size shouldBe 32
      SumSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }

    withConf(1L) { dir =>
      SumSpill.armMergeFailure()
      val caught = intercept[IllegalStateException] {
        val _ = sum(batches)
      }

      caught.getMessage shouldBe SumSpill.MergeFailed
      SumSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a spilled sum" should "agree with the in-memory sum per key and in total" in {
    val batches = twoPasses
    val memory = sum(batches)
    val total = pairRows.iterator.map(_._2.toLong).sum

    withConf(1L) { _ =>
      val spilled = sum(batches)

      groups(spilled) shouldBe groups(memory)
      ColumnReduce.sumInt64(spilled, ordinal = 1) shouldBe ColumnReduce.sumInt64(memory, ordinal = 1)
      ColumnReduce.sumInt64(spilled, ordinal = 1) shouldBe total
    }
  }

  /** 32 keys, each contributed by both passes, so the one-partition bucket has 64 rows. */
  private val twoPassRows = 64

  private def pairRows: Seq[(Int, Int)] =
    (0 until 32).map(key => key -> (key + 1)) ++ (0 until 32).map(key => key -> 1)

  private def twoPasses: Vector[ColumnBatch] =
    val (left, right) = pairRows.splitAt(32)
    Vector(BatchCodec.intPairs(left), BatchCodec.intPairs(right))

  private def sum(
      batches: Vector[ColumnBatch],
      partitions: Int = 1,
      parallelism: Int = 2,
      batchSize: Int = 1,
  ): Vector[ColumnBatch] =
    val result = ColumnPipeline.sumByKey(
      batches,
      keyOrdinal = 0,
      valueOrdinal = 1,
      numPartitions = partitions,
      parallelism = parallelism,
      batchSize = batchSize,
    )
    result.length shouldBe partitions
    result

  private def groups(batches: Vector[ColumnBatch]): Map[Int, Option[Long]] =
    val pairs = batches.flatMap { batch =>
      val keys = int32(batch, 0)
      val values = int64(batch, 1)
      (0 until batch.length).map { row =>
        require(keys.isValid(row))
        val summed = if (values.isValid(row)) Some(values.values(row)) else None
        keys.values(row) -> summed
      }
    }
    pairs.map(_._1) shouldBe pairs.map(_._1).distinct
    pairs.toMap

  private def withConf(budget: Long)(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-sum-spill")
    SumSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = true,
        spillBudgetBytes = budget,
        spillDirectory = dir.toString,
        spillMaxFiles = SpillStore.DefaultMaxFiles,
        spillMaxBytes = SpillStore.DefaultMaxBytes,
      ),
    )
    try body(dir)
    finally
      ScarletConf.set(originalConf)
      SumSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")

  "spill disabled" should "ignore a tiny budget and write no files" in {
    val batches = twoPasses
    val memory = sum(batches)
    val dir = Files.createTempDirectory("scarlet-sum-off")
    SumSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = false,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
      ),
    )
    try
      groups(sum(batches)) shouldBe groups(memory)
      SumSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      SumSpill.resetForTest()
      SpillStore.listForTest(dir).foreach(path => Files.deleteIfExists(path))
      val _ = Files.deleteIfExists(dir)
  }

  "a bucket with more morsels than the file cap" should "spill one file at a time" in {
    val rows = 4000
    val keys = Int32Column.of(Array.tabulate(rows)(row => row % 64), Validity.allValid(rows), rows)
    val values = Int64Column.of(Array.tabulate(rows)(row => row.toLong), Validity.allValid(rows), rows)
    val bucket = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64),
      Vector(keys, values),
      rows,
    )
    val dir = Files.createTempDirectory("scarlet-sum-cap")
    SumSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = true,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
        spillMaxFiles = 4,
      ),
    )
    try
      val summed = SumSpill.finish(bucket, parallelism = 1, batchSize = 16, ScarletConf.get)
      groups(Vector(summed)).size shouldBe 64
      SumSpill.filesWrittenForTest should be > 4L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      SumSpill.resetForTest()
      SpillStore.listForTest(dir).foreach(path => Files.deleteIfExists(path))
      val _ = Files.deleteIfExists(dir)
  }

  "a spill write that exceeds the byte cap" should "leave no file behind" in {
    val dir = Files.createTempDirectory("scarlet-sum-cap-bytes")
    SumSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = true,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
        spillMaxBytes = 16L,
      ),
    )
    try
      an[IllegalStateException] should be thrownBy sum(twoPasses)
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      SumSpill.resetForTest()
      SpillStore.listForTest(dir).foreach(path => Files.deleteIfExists(path))
      val _ = Files.deleteIfExists(dir)
  }
