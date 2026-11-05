package com.ewoodbury.scarlet.columnar

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.ScarletConf

class TestStringHashReduceSpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val plus: (Int, Int) => Int = (left, right) => left + right
  private val accent = "é"
  private val decomposed = "e\u0301"
  private val emoji = "😀"

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    StringHashReduceSpill.resetForTest()

  "retained bytes" should "be zero for an empty bucket and charge the dictionary once" in {
    StringHashReduceSpill.retainedBytes(0, 0, 0L) shouldBe 0L
    StringHashReduceSpill.retainedBytes(0, 0, 12L) shouldBe 0L
    StringHashReduceSpill.retainedBytes(-1, 0, 12L) shouldBe 0L
    val rows = 3
    val entries = 2
    val dictionary = 11L
    val slots = ColumnHash.tableSize(rows).toLong
    val expected =
      rows.toLong * (4L + 4L + 1L) + entries.toLong * 8L + dictionary +
        slots * (4L + 4L + 1L + 4L)
    StringHashReduceSpill.retainedBytes(rows, entries, dictionary) shouldBe expected

    val batch = BatchCodec.stringIntPairs(Seq(accent -> 1, accent -> 2, emoji -> 3))
    val once = utf8Length(accent) + utf8Length(emoji)
    StringHashReduceSpill.dictionaryBytes(batch) shouldBe once
    utf8Length(accent) shouldBe 2L
    utf8Length(emoji) shouldBe 4L
    utf8Length(decomposed) shouldBe 3L
  }

  "a bucket at the budget" should "reduce in memory and write no spill files" in {
    val batches = twoPasses
    val memory = reduce(batches)
    // Two partials of 32 groups, one partition, so the exchanged bucket is 64 rows. Both passes
    // intern the same strings in the same order, and the exchange keeps that dictionary.
    utf8(batches(0)).dictionary.toSeq shouldBe utf8(batches(1)).dictionary.toSeq
    val budget = StringHashReduceSpill.retainedBytes(
      twoPassRows,
      utf8(batches.head).dictionary.length,
      dictionaryBytes(batches.head),
    )

    withConf(budget) { dir =>
      val reduced = reduce(batches)

      groups(reduced) shouldBe groups(memory)
      payload(reduced) shouldBe payload(memory)
      reduced.length shouldBe 1
      StringHashReduceSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a bucket over a tiny budget" should "spill and match the in-memory group map" in {
    val batches = twoPasses
    val memory = reduce(batches)

    withConf(1L) { dir =>
      val spilled = reduce(batches)

      groups(spilled) shouldBe groups(memory)
      groups(spilled) shouldBe (0 until 32).map(key => key.toString -> (key + 2)).toMap
      StringHashReduceSpill.filesWrittenForTest should be > 0L
      Path.of(StringHashReduceSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "string null rules" should "keep an empty key, drop a null key, and accept two dictionaries" in {
    val left = BatchCodec.stringIntPairs(Seq("" -> 1, "b" -> 2, "a" -> 3, nullKey(9)))
    val right = BatchCodec.stringIntPairs(Seq("a" -> 4, "" -> 5, "b" -> 6))
    utf8(left).dictionary.toSeq should not be utf8(right).dictionary.toSeq
    utf8(left).dictionary.indexOf("") should not be utf8(right).dictionary.indexOf("")
    val batches = Vector(left, right)
    val memory = reduce(batches, partitions = 4, parallelism = 4)

    withConf(1L) { dir =>
      val spilled = reduce(batches, partitions = 4, parallelism = 4)

      spilled.length shouldBe 4
      groups(spilled) shouldBe groups(memory)
      groups(spilled) shouldBe Map("" -> 6, "a" -> 7, "b" -> 8)
      StringHashReduceSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "multibyte keys" should "keep an accent, its decomposition, and an emoji distinct" in {
    val batches = Vector(
      BatchCodec.stringIntPairs(Seq(accent -> 2, decomposed -> 3, emoji -> 4)),
      BatchCodec.stringIntPairs(Seq(emoji -> 7, decomposed -> 8, accent -> 1)),
    )
    val memory = reduce(batches)

    withConf(1L) { dir =>
      val spilled = reduce(batches)

      groups(spilled) shouldBe groups(memory)
      groups(spilled) shouldBe Map(accent -> 3, decomposed -> 11, emoji -> 11)
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "return one empty batch per partition and write nothing" in {
    withConf(1L) { dir =>
      val parts = reduce(Vector.empty, partitions = 4)

      parts.length shouldBe 4
      parts.map(_.length) shouldBe Seq(0, 0, 0, 0)
      StringHashReduceSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill files" should "be gone after a successful merge and after a failed one" in {
    val batches = twoPasses

    withConf(1L) { dir =>
      val spilled = reduce(batches)

      groups(spilled).size shouldBe 32
      StringHashReduceSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }

    withConf(1L) { dir =>
      StringHashReduceSpill.armMergeFailure()
      val caught = intercept[IllegalStateException] {
        val _ = reduce(batches)
      }

      caught.getMessage shouldBe StringHashReduceSpill.MergeFailed
      StringHashReduceSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill disabled" should "ignore a tiny budget and match the in-memory bytes" in {
    val batches = twoPasses
    val memory = reduce(batches)
    val dir = Files.createTempDirectory("scarlet-string-reduce-off")
    StringHashReduceSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = false,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
      ),
    )
    try
      payload(reduce(batches)) shouldBe payload(memory)
      StringHashReduceSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      StringHashReduceSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
  }

  "a bucket with more morsels than the file cap" should "spill one file at a time" in {
    val rows = 4000
    val dictionary = Array.tabulate(64)(slot => "k" + slot.toString)
    val codes = Array.tabulate(rows)(row => row % 64)
    val values = Array.tabulate(rows)(identity)
    val bucket = new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(
        DictUtf8Column.of(dictionary, codes, Validity.allValid(rows), rows),
        Int32Column.of(values, Validity.allValid(rows), rows),
      ),
      rows,
    )
    val cap = 4
    val dir = Files.createTempDirectory("scarlet-string-reduce-cap")
    StringHashReduceSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = true,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
        spillMaxFiles = cap,
      ),
    )
    try
      val reduced = StringHashReduceSpill.finish(
        bucket,
        parallelism = 1,
        batchSize = 16,
        ScarletConf.get,
        plus,
      )
      groups(Vector(reduced)).size shouldBe 64
      StringHashReduceSpill.filesWrittenForTest should be > cap.toLong
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      StringHashReduceSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
  }

  "a spill write that exceeds the byte cap" should "leave no file behind" in {
    val dir = Files.createTempDirectory("scarlet-string-reduce-cap-bytes")
    StringHashReduceSpill.resetForTest()
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
      StringHashReduceSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
  }

  /** 32 keys, each contributed by both passes, so the one-partition bucket has 64 rows. */
  private val twoPassRows = 64

  private def pairRows: Seq[(String, Int)] =
    (0 until 32).map(key => key.toString -> (key + 1)) ++ (0 until 32).map(key => key.toString -> 1)

  private def twoPasses: Vector[ColumnBatch] =
    val (left, right) = pairRows.splitAt(32)
    Vector(BatchCodec.stringIntPairs(left), BatchCodec.stringIntPairs(right))

  private def reduce(
      batches: Vector[ColumnBatch],
      partitions: Int = 1,
      parallelism: Int = 2,
      batchSize: Int = 1,
  ): Vector[ColumnBatch] =
    val result = StringHashReduce.byKey(
      batches,
      numPartitions = partitions,
      parallelism = parallelism,
      batchSize = batchSize,
      plus,
    )
    result.length shouldBe partitions
    result

  private def groups(batches: Vector[ColumnBatch]): Map[String, Int] =
    val pairs = batches.flatMap { batch =>
      val keys = utf8(batch)
      val values = int32(batch)
      (0 until batch.length).map { row =>
        keys.isValid(row) shouldBe true
        values.isValid(row) shouldBe true
        keys.dictionary(keys.codes(row)) -> values.values(row)
      }
    }
    pairs.map(_._1) shouldBe pairs.map(_._1).distinct
    pairs.toMap

  /** Dictionary strings, codes, values, and both validity-word arrays, in batch order. */
  private def payload(
      batches: Vector[ColumnBatch],
  ): Seq[(Seq[String], Seq[Int], Seq[Int], Seq[Long], Seq[Long])] =
    batches.map { batch =>
      val keys = utf8(batch)
      val values = int32(batch)
      (
        keys.dictionary.toSeq,
        keys.codes.iterator.take(keys.length).toSeq,
        values.values.iterator.take(values.length).toSeq,
        keys.validity.words.toSeq,
        values.validity.words.toSeq,
      )
    }

  private def withConf(budget: Long)(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-string-reduce-spill")
    StringHashReduceSpill.resetForTest()
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
      StringHashReduceSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)

  private def dictionaryBytes(batch: ColumnBatch): Long =
    utf8(batch).dictionary.iterator.map(entry => utf8Length(entry)).sum

  private def utf8Length(text: String): Long =
    text.getBytes(StandardCharsets.UTF_8).length.toLong

  private def utf8(batch: ColumnBatch): DictUtf8Column =
    batch.columns.headOption match
      case Some(column: DictUtf8Column) => column
      case other => fail(s"expected Utf8Dict at 0, got $other")

  private def int32(batch: ColumnBatch): Int32Column =
    batch.columns.lift(1) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at 1, got $other")

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value
