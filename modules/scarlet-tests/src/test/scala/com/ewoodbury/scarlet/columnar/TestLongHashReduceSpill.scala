package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.ScarletConf

class TestLongHashReduceSpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val plus: LongHashReduce.LongBinOp = (left, right) => left + right

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    LongHashReduceSpill.resetForTest()

  "retained bytes" should "be zero for an empty bucket" in {
    LongHashReduceSpill.retainedBytes(0) shouldBe 0L
    LongHashReduceSpill.retainedBytes(-1) shouldBe 0L
    val rows = 3
    val slots = ColumnHash.tableSize(rows).toLong
    val expected = rows.toLong * (8L + 8L + 1L) + slots * (8L + 8L + 1L + 4L)
    LongHashReduceSpill.retainedBytes(rows) shouldBe expected
  }

  "a bucket at the budget" should "reduce in memory and write no spill files" in {
    val batches = twoPasses
    val memory = reduce(batches)
    // Two partials of 32 groups, one partition, so the exchanged bucket is 64 rows.
    val budget = LongHashReduceSpill.retainedBytes(twoPassRows)

    withConf(budget) { dir =>
      val reduced = reduce(batches)

      groups(reduced) shouldBe groups(memory)
      payload(reduced) shouldBe payload(memory)
      reduced.length shouldBe 1
      LongHashReduceSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a bucket over a tiny budget" should "spill and match the in-memory group map" in {
    val batches = twoPasses
    val memory = reduce(batches)

    withConf(1L) { dir =>
      val spilled = reduce(batches)

      groups(spilled) shouldBe groups(memory)
      groups(spilled) shouldBe (0 until 32).map(key => key.toLong -> (key.toLong + 2L)).toMap
      LongHashReduceSpill.filesWrittenForTest should be > 0L
      Path.of(LongHashReduceSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "long null rules" should "keep one null, unbox a second null, keep a present value, and drop a null key" in {
    // The partial combine runs per batch; the spill fold re-reduces those partials, so both
    // sides of the path see the null rules.
    val batches = Vector(
      nullableLongPairs(
        Seq(
          (Some(2L), None),
          (Some(5L), Some(9L)),
          (None, Some(4L)),
          (Some(4L), None),
        ),
      ),
      nullableLongPairs(Seq((Some(5L), None), (Some(4L), None))),
    )
    val memory = reduce(batches)

    withConf(1L) { dir =>
      val spilled = reduce(batches)

      optionalGroups(spilled) shouldBe optionalGroups(memory)
      optionalGroups(spilled) shouldBe Map(
        2L -> None,
        4L -> Some(0L),
        5L -> Some(9L),
      )
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "negative keys" should "land in a floorMod bucket after a spill" in {
    val batches = Vector(
      longPairs(Seq((Long.MinValue, 2L), (-1L, 5L), (-4L, 1L), (3L, 9L))),
      longPairs(Seq((Long.MinValue, 3L), (-1L, 7L), (0L, 4L), (-4L, 2L))),
    )
    val memory = reduce(batches, partitions = 4, parallelism = 4)

    withConf(1L) { dir =>
      val spilled = reduce(batches, partitions = 4, parallelism = 4)

      spilled.length shouldBe 4
      groups(spilled) shouldBe groups(memory)
      groups(spilled) shouldBe Map(
        Long.MinValue -> 5L,
        -1L -> 12L,
        -4L -> 3L,
        3L -> 9L,
        0L -> 4L,
      )
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "return one empty batch per partition and write nothing" in {
    withConf(1L) { dir =>
      val parts = reduce(Vector.empty, partitions = 4)

      parts.length shouldBe 4
      parts.map(_.length) shouldBe Seq(0, 0, 0, 0)
      LongHashReduceSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill files" should "be gone after a successful merge and after a failed one" in {
    val batches = twoPasses

    withConf(1L) { dir =>
      val spilled = reduce(batches)

      groups(spilled).size shouldBe 32
      LongHashReduceSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }

    withConf(1L) { dir =>
      LongHashReduceSpill.armMergeFailure()
      val caught = intercept[IllegalStateException] {
        val _ = reduce(batches)
      }

      caught.getMessage shouldBe LongHashReduceSpill.MergeFailed
      LongHashReduceSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill disabled" should "ignore a tiny budget and match the in-memory bytes" in {
    val batches = twoPasses
    val memory = reduce(batches)
    val dir = Files.createTempDirectory("scarlet-long-reduce-off")
    LongHashReduceSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = false,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
      ),
    )
    try
      payload(reduce(batches)) shouldBe payload(memory)
      LongHashReduceSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      LongHashReduceSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
  }

  "a bucket with more morsels than the file cap" should "spill one file at a time" in {
    val rows = 4000
    val keys = Int64Column.of(Array.tabulate(rows)(row => (row % 64).toLong), Validity.allValid(rows), rows)
    val values = Int64Column.of(Array.tabulate(rows)(row => row.toLong), Validity.allValid(rows), rows)
    val bucket = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(keys, values),
      rows,
    )
    val cap = 4
    val dir = Files.createTempDirectory("scarlet-long-reduce-cap")
    LongHashReduceSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = true,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
        spillMaxFiles = cap,
      ),
    )
    try
      val reduced = LongHashReduceSpill.finish(
        bucket,
        parallelism = 1,
        batchSize = 16,
        ScarletConf.get,
        plus,
      )
      groups(Vector(reduced)).size shouldBe 64
      LongHashReduceSpill.filesWrittenForTest should be > cap.toLong
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      LongHashReduceSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
  }

  "a spill write that exceeds the byte cap" should "leave no file behind" in {
    val dir = Files.createTempDirectory("scarlet-long-reduce-cap-bytes")
    LongHashReduceSpill.resetForTest()
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
      LongHashReduceSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
  }

  /** 32 keys, each contributed by both passes, so the one-partition bucket has 64 rows. */
  private val twoPassRows = 64

  private def pairRows: Seq[(Long, Long)] =
    (0 until 32).map(key => key.toLong -> (key.toLong + 1L)) ++ (0 until 32).map(key => key.toLong -> 1L)

  private def twoPasses: Vector[ColumnBatch] =
    val (left, right) = pairRows.splitAt(32)
    Vector(longPairs(left), longPairs(right))

  private def longPairs(rows: Seq[(Long, Long)]): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(Int64Column(rows.map(_._1)), Int64Column(rows.map(_._2))),
      rows.length,
    )

  private def nullableLongPairs(rows: Seq[(Option[Long], Option[Long])]): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(Int64Column.fromNullable(rows.map(_._1)), Int64Column.fromNullable(rows.map(_._2))),
      rows.length,
    )

  private def reduce(
      batches: Vector[ColumnBatch],
      partitions: Int = 1,
      parallelism: Int = 2,
      batchSize: Int = 1,
  ): Vector[ColumnBatch] =
    val result = LongHashReduce.byKey(
      batches,
      numPartitions = partitions,
      parallelism = parallelism,
      batchSize = batchSize,
      plus,
    )
    result.length shouldBe partitions
    result

  private def groups(batches: Vector[ColumnBatch]): Map[Long, Long] =
    val pairs = batches.flatMap { batch =>
      val keys = int64(batch, 0)
      val values = int64(batch, 1)
      (0 until batch.length).map { row =>
        keys.isValid(row) shouldBe true
        values.isValid(row) shouldBe true
        keys.values(row) -> values.values(row)
      }
    }
    pairs.map(_._1) shouldBe pairs.map(_._1).distinct
    pairs.toMap

  private def optionalGroups(batches: Vector[ColumnBatch]): Map[Long, Option[Long]] =
    val pairs = batches.flatMap { batch =>
      val keys = int64(batch, 0)
      val values = int64(batch, 1)
      (0 until batch.length).map { row =>
        keys.isValid(row) shouldBe true
        val value = if (values.isValid(row)) Some(values.values(row)) else None
        keys.values(row) -> value
      }
    }
    pairs.map(_._1) shouldBe pairs.map(_._1).distinct
    pairs.toMap

  /** Key values, value values, and both validity-word arrays, in batch order. */
  private def payload(
      batches: Vector[ColumnBatch],
  ): Seq[(Seq[Long], Seq[Long], Seq[Long], Seq[Long])] =
    batches.map { batch =>
      val keys = int64(batch, 0)
      val values = int64(batch, 1)
      (
        keys.values.iterator.take(keys.length).toSeq,
        values.values.iterator.take(values.length).toSeq,
        keys.validity.words.toSeq,
        values.validity.words.toSeq,
      )
    }

  private def withConf(budget: Long)(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-long-reduce-spill")
    LongHashReduceSpill.resetForTest()
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
      LongHashReduceSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")
