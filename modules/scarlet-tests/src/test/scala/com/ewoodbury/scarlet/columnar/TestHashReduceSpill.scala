package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.ScarletConf

class TestHashReduceSpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val plus: HashReduce.IntBinOp = (left, right) => left + right

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    HashReduceSpill.resetForTest()

  "retained bytes" should "be zero for an empty bucket" in {
    HashReduceSpill.retainedBytes(0) shouldBe 0L
    HashReduceSpill.retainedBytes(-1) shouldBe 0L
    val rows = 3
    val slots = ColumnHash.tableSize(rows).toLong
    val expected = rows.toLong * (4L + 4L + 1L) + slots * (4L + 4L + 1L + 4L)
    HashReduceSpill.retainedBytes(rows) shouldBe expected
  }

  "a bucket at the budget" should "reduce in memory and write no spill files" in {
    val batches = twoPasses
    val memory = reduce(batches)
    // Two partials of 32 groups, one partition, so the exchanged bucket is 64 rows.
    val budget = HashReduceSpill.retainedBytes(twoPassRows)

    withConf(budget) { dir =>
      val reduced = reduce(batches)

      groups(reduced) shouldBe groups(memory)
      payload(reduced) shouldBe payload(memory)
      reduced.length shouldBe 1
      HashReduceSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a bucket over a tiny budget" should "spill and match the in-memory group map" in {
    val batches = twoPasses
    val memory = reduce(batches)

    withConf(1L) { dir =>
      val spilled = reduce(batches)

      groups(spilled) shouldBe groups(memory)
      groups(spilled) shouldBe (0 until 32).map(key => key -> (key + 2)).toMap
      HashReduceSpill.filesWrittenForTest should be > 0L
      Path.of(HashReduceSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  it should "drop a null key and create no group for a null value" in {
    val batches = Vector(
      BatchCodec.nullableIntPairs(
        Seq((None, Some(4)), (Some(7), None), (Some(8), Some(1)), (Some(0), None)),
      ),
      BatchCodec.nullableIntPairs(Seq((Some(7), None), (Some(8), Some(2)))),
    )
    val memory = reduce(batches)

    withConf(1L) { dir =>
      val spilled = reduce(batches)

      groups(spilled) shouldBe groups(memory)
      groups(spilled) shouldBe Map(8 -> 3)
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "negative keys" should "land in a floorMod bucket after a spill" in {
    val batches = Vector(
      BatchCodec.intPairs(Seq((Int.MinValue, 2), (-1, 5), (-4, 1), (3, 9))),
      BatchCodec.intPairs(Seq((Int.MinValue, 3), (-1, 7), (0, 4), (-4, 2))),
    )
    val memory = reduce(batches, partitions = 4, parallelism = 4)

    withConf(1L) { dir =>
      val spilled = reduce(batches, partitions = 4, parallelism = 4)

      spilled.length shouldBe 4
      groups(spilled) shouldBe groups(memory)
      groups(spilled) shouldBe Map(
        Int.MinValue -> 5,
        -1 -> 12,
        -4 -> 3,
        3 -> 9,
        0 -> 4,
      )
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "return one empty batch per partition and write nothing" in {
    withConf(1L) { dir =>
      val parts = reduce(Vector.empty, partitions = 4)

      parts.length shouldBe 4
      parts.map(_.length) shouldBe Seq(0, 0, 0, 0)
      HashReduceSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill files" should "be gone after a successful merge and after a failed one" in {
    val batches = twoPasses

    withConf(1L) { dir =>
      val spilled = reduce(batches)

      groups(spilled).size shouldBe 32
      HashReduceSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }

    withConf(1L) { dir =>
      HashReduceSpill.armMergeFailure()
      val caught = intercept[IllegalStateException] {
        val _ = reduce(batches)
      }

      caught.getMessage shouldBe HashReduceSpill.MergeFailed
      HashReduceSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill disabled" should "ignore a tiny budget and match the in-memory bytes" in {
    val batches = twoPasses
    val memory = reduce(batches)
    val dir = Files.createTempDirectory("scarlet-reduce-off")
    HashReduceSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = false,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
      ),
    )
    try
      payload(reduce(batches)) shouldBe payload(memory)
      HashReduceSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      HashReduceSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
  }

  "a bucket with more morsels than the file cap" should "spill one file at a time" in {
    val rows = 4000
    val keys = Int32Column.of(Array.tabulate(rows)(row => row % 64), Validity.allValid(rows), rows)
    val values = Int32Column.of(Array.tabulate(rows)(row => row), Validity.allValid(rows), rows)
    val bucket = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(keys, values),
      rows,
    )
    val cap = 4
    val dir = Files.createTempDirectory("scarlet-reduce-cap")
    HashReduceSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = true,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
        spillMaxFiles = cap,
      ),
    )
    try
      val reduced = HashReduceSpill.finish(bucket, parallelism = 1, batchSize = 16, ScarletConf.get, plus)
      groups(Vector(reduced)).size shouldBe 64
      HashReduceSpill.filesWrittenForTest should be > cap.toLong
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      HashReduceSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
  }

  "a spill write that exceeds the byte cap" should "leave no file behind" in {
    val dir = Files.createTempDirectory("scarlet-reduce-cap-bytes")
    HashReduceSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = true,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
        spillMaxBytes = 16L,
      ),
    )
    try
      an[IllegalStateException] should be thrownBy reduce(twoPasses)
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      HashReduceSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
  }

  /** 32 keys, each contributed by both passes, so the one-partition bucket has 64 rows. */
  private val twoPassRows = 64

  private def pairRows: Seq[(Int, Int)] =
    (0 until 32).map(key => key -> (key + 1)) ++ (0 until 32).map(key => key -> 1)

  private def twoPasses: Vector[ColumnBatch] =
    val (left, right) = pairRows.splitAt(32)
    Vector(BatchCodec.intPairs(left), BatchCodec.intPairs(right))

  private def reduce(
      batches: Vector[ColumnBatch],
      partitions: Int = 1,
      parallelism: Int = 2,
      batchSize: Int = 1,
  ): Vector[ColumnBatch] =
    val result = HashReduce.byKey(
      batches,
      numPartitions = partitions,
      parallelism = parallelism,
      batchSize = batchSize,
      plus,
    )
    result.length shouldBe partitions
    result

  private def groups(batches: Vector[ColumnBatch]): Map[Int, Int] =
    val pairs = batches.flatMap { batch =>
      val keys = int32(batch, 0)
      val values = int32(batch, 1)
      (0 until batch.length).map { row =>
        keys.validity.isValid(row) shouldBe true
        values.validity.isValid(row) shouldBe true
        keys.values(row) -> values.values(row)
      }
    }
    pairs.map(_._1) shouldBe pairs.map(_._1).distinct
    pairs.toMap

  /** Key values, value values, and both validity-word arrays, in batch order. */
  private def payload(batches: Vector[ColumnBatch]): Seq[(Seq[Int], Seq[Int], Seq[Long], Seq[Long])] =
    batches.map { batch =>
      val keys = int32(batch, 0)
      val values = int32(batch, 1)
      (
        keys.values.iterator.take(keys.length).toSeq,
        values.values.iterator.take(values.length).toSeq,
        keys.validity.words.toSeq,
        values.validity.words.toSeq,
      )
    }

  private def withConf(budget: Long)(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-reduce-spill")
    HashReduceSpill.resetForTest()
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
      HashReduceSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")
