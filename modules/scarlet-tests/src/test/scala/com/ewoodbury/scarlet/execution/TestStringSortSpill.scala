package com.ewoodbury.scarlet.execution

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Spilled rank split of a string-pair sort. The in-memory path is [[StringRankSplit]]. Partition
 * boundaries are that split. A counter assertion is what shows the spill ran. The int and long
 * counters stay at zero.
 *
 * A present key orders by `String.compareTo` (UTF-16 code units). A null key orders as `""` and
 * stays null. `""` stays present. A null value stays null.
 */
class TestStringSortSpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val accent = "é"
  private val decomposed = "e\u0301"
  private val emoji = "😀"

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    StringRankSplitRuns.resetForTest()
    RankSplitRuns.resetForTest()
    RankSplitLongRuns.resetForTest()
    ColumnMetrics.restore(NoOpSink)
    ()

  "the trigger" should "charge each dictionary's UTF-8 bytes and stay off at the budget" in {
    val mixed = BatchCodec.stringIntPairs(Seq(accent -> 1, emoji -> 2, "" -> 3))
    val onlyNull = stringBatch(Seq(None -> 1))
    val empty = BatchCodec.stringIntPairs(Seq.empty[(String, Int)])
    val emptyDictionary = new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(
        DictUtf8Column.of(Array("kept"), Array.emptyIntArray, Validity.allValid(0), 0),
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
      ),
      0,
    )

    StringRankSplitRuns.retainedStringBytes(Vector.empty) shouldBe 0L
    StringRankSplitRuns.retainedStringBytes(Vector(empty)) shouldBe 0L
    StringRankSplitRuns.retainedStringBytes(Vector(emptyDictionary)) shouldBe 0L
    StringRankSplitRuns.retainedStringBytes(Vector(mixed)) shouldBe expectedCharge(mixed)
    StringRankSplitRuns.retainedStringBytes(Vector(onlyNull)) shouldBe 1L * (8L + 4L + 4L + 1L) +
      StringRankSplitRuns.HeaderAllowance
    utf8(onlyNull).dictionary.toSeq shouldBe Seq.empty[String]
    StringRankSplitRuns.retainedStringBytes(Vector(mixed, onlyNull, empty)) shouldBe
      expectedCharge(mixed) + expectedCharge(onlyNull)

    val batches = Vector(mixed, onlyNull)
    val estimate = StringRankSplitRuns.retainedStringBytes(batches)
    estimate shouldBe expectedCharge(mixed) + expectedCharge(onlyNull)
    StringRankSplitRuns.exceeds(batches, confAt(estimate, enabled = false)) shouldBe false
    StringRankSplitRuns.exceeds(batches, confAt(estimate, enabled = true)) shouldBe false
    StringRankSplitRuns.exceeds(batches, confAt(estimate - 1L, enabled = true)) shouldBe true
    StringRankSplitRuns.exceeds(Vector(empty, emptyDictionary), confAt(0L, enabled = true)) shouldBe
      false
  }

  "a split at the budget" should "match StringRankSplit and write no files" in {
    val batches = Vector(
      BatchCodec.stringIntPairs(Seq("b" -> 1, "a" -> 2)),
      BatchCodec.stringIntPairs(Seq(accent -> 3, "" -> 4)),
    )
    val estimate = StringRankSplitRuns.retainedStringBytes(batches)
    val memory = StringRankSplit.sortAndSplit(batches, partitions = 3, parallelism = 2)

    withConf(estimate, batchSize = 1, fanIn = 2, shufflePartitions = 3) { dir =>
      val spilled =
        StringRankSplitRuns.sortAndSplit(batches, partitions = 3, parallelism = 2, ScarletConf.get)

      identical(spilled, memory)
      StringRankSplitRuns.filesWrittenForTest shouldBe 0L
      StringRankSplitRuns.mergePassesForTest shouldBe 0L
      StringRankSplitRuns.lastDirectoryForTest shouldBe ""
      RankSplitRuns.filesWrittenForTest shouldBe 0L
      RankSplitLongRuns.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill disabled" should "match StringRankSplit and the row path and write no files" in {
    val source = keyed(Seq("b" -> 2, "a" -> 1), Seq(emoji -> 3, "" -> 4))

    withConf(0L, enabled = false, shufflePartitions = 4) { dir =>
      val input = sourcePairs(source.plan)
      val plan = source.sortByStringKey.plan
      val sorted = sortedParts(plan)
      val memory = StringRankSplit.sortAndSplit(input, partitions = 4, parallelism = 1)

      identical(sorted, memory)
      collect(source.sortByStringKey) shouldBe rowOf(source.sortByStringKey)
      StringRankSplitRuns.filesWrittenForTest shouldBe 0L
      StringRankSplitRuns.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("SortByStringKey(Scan)")
    }
  }

  "a tiny budget" should "spill, match the in-memory split and the row path, and leave no files" in {
    val source = keyed(Seq("b" -> 2, "a" -> 1), Seq("c" -> 3))

    withConf(0L, batchSize = 2, fanIn = 8, shufflePartitions = 4) { dir =>
      val input = sourcePairs(source.plan)
      val plan = source.sortByStringKey.plan
      val spilled = sortedParts(plan)
      val memory = StringRankSplit.sortAndSplit(input, partitions = 4, parallelism = 1)

      input.map(_.length) shouldBe Vector(2, 1)
      StringRankSplitRuns.retainedStringBytes(input) should be > 0L
      identical(spilled, memory)
      // Two source partitions, one merge. Three files, one pass. collect spills again.
      StringRankSplitRuns.filesWrittenForTest should be > 0L
      StringRankSplitRuns.filesWrittenForTest shouldBe 3L
      StringRankSplitRuns.mergePassesForTest shouldBe 1L
      Path.of(StringRankSplitRuns.lastDirectoryForTest) shouldBe dir
      RankSplitRuns.filesWrittenForTest shouldBe 0L
      RankSplitLongRuns.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("SortByStringKey(Scan)")

      StringRankSplitRuns.resetForTest()
      collect(source.sortByStringKey) shouldBe rowOf(source.sortByStringKey)
      StringRankSplitRuns.filesWrittenForTest shouldBe 3L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "more runs than the fan-in" should "take several passes and keep duplicate keys across the cut" in {
    val empty = BatchCodec.stringIntPairs(Seq.empty[(String, Int)])
    val batches = Vector(
      BatchCodec.stringIntPairs(Seq("a" -> 10)),
      empty,
      BatchCodec.stringIntPairs(Seq("a" -> 11)),
      BatchCodec.stringIntPairs(Seq("a" -> 12)),
      BatchCodec.stringIntPairs(Seq("a" -> 13)),
      BatchCodec.stringIntPairs(Seq("a" -> 14)),
    )
    val memory = StringRankSplit.sortAndSplit(batches, partitions = 2, parallelism = 1)

    withConf(0L, batchSize = 1, fanIn = 2, shufflePartitions = 2) { dir =>
      val serial =
        StringRankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)

      identical(serial, memory)
      StringRankSplitRuns.mergePassesForTest should be > 1L
      StringRankSplitRuns.mergePassesForTest shouldBe 3L
      StringRankSplitRuns.filesWrittenForTest shouldBe 9L
      cells(at(serial, 0)) shouldBe Seq(
        (Some("a"), Some(10)),
        (Some("a"), Some(11)),
        (Some("a"), Some(12)),
      )
      cells(at(serial, 1)) shouldBe Seq((Some("a"), Some(13)), (Some("a"), Some(14)))

      StringRankSplitRuns.resetForTest()
      val parallel =
        StringRankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 3, ScarletConf.get)

      identical(parallel, memory)
      StringRankSplitRuns.mergePassesForTest should be > 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "null keys, empty strings, and two dictionaries" should "keep order through a spill" in {
    val left = Seq(
      "b" -> 1,
      "" -> 2,
      nullKey(3),
      accent -> 4,
      "b" -> 5,
      emoji -> 6,
    )
    val right = Seq(
      decomposed -> 7,
      "" -> 8,
      "a" -> 9,
      nullKey(10),
      "a" -> 11,
    )
    val leftBatch = BatchCodec.stringIntPairs(left)
    val rightBatch = BatchCodec.stringIntPairs(right)
    utf8(leftBatch).dictionary.toList shouldNot equal(utf8(rightBatch).dictionary.toList)
    val batches = Vector(leftBatch, rightBatch)
    val memory = StringRankSplit.sortAndSplit(batches, partitions = 2, parallelism = 1)
    val source = keyed(left, right)

    withConf(0L, batchSize = 1, fanIn = 2, shufflePartitions = 2) { dir =>
      val spilled =
        StringRankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)

      identical(spilled, memory)
      val ordered = spilled.flatMap(batch => BatchCodec.decodeStringPairs(batch))
      ordered.map(_._2) shouldBe Seq(2, 3, 8, 10, 9, 11, 1, 5, 7, 4, 6)
      ordered.map(pair => effective(pair._1)) shouldBe Seq(
        "",
        "",
        "",
        "",
        "a",
        "a",
        "b",
        "b",
        decomposed,
        accent,
        emoji,
      )
      ordered.count(pair => isNullKey(pair._1)) shouldBe 2
      ordered.count(pair => isEmptyKey(pair._1)) shouldBe 2
      collect(source.sortByStringKey) shouldBe rowOf(source.sortByStringKey)
      collect(source.sortByStringKey) shouldBe ordered
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a null value and a dirty null key" should "survive a spill without reading the null payload" in {
    val nullable = stringBatch(Seq(Some("b") -> 1, None -> 2, Some("") -> 3), valueAt = Set(0, 2))
    val dirty = new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(
        DictUtf8Column.of(Array("zzz", "a"), Array(0, 1), Validity.pack(2, row => row == 1), 2),
        Int32Column.of(Array(10, 20), Validity.allValid(2), 2),
      ),
      2,
    )
    val memory = StringRankSplit.sortAndSplit(Vector(nullable, dirty), partitions = 2, parallelism = 1)

    withConf(0L, batchSize = 1, fanIn = 2) { dir =>
      val spilled = StringRankSplitRuns.sortAndSplit(
        Vector(nullable, dirty),
        partitions = 2,
        parallelism = 1,
        ScarletConf.get,
      )

      identical(spilled, memory)
      cells(at(spilled, 0)) shouldBe Seq(
        (None, None),
        (Some(""), Some(3)),
        (None, Some(10)),
      )
      cells(at(spilled, 1)) shouldBe Seq((Some("a"), Some(20)), (Some("b"), Some(1)))
      utf8(at(spilled, 0)).dictionary.toList shouldBe Seq("")
      utf8(at(spilled, 1)).dictionary.toList shouldBe Seq("a", "b")
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "write nothing and return fresh empty batches" in {
    val empty = BatchCodec.stringIntPairs(Seq.empty[(String, Int)])
    val batches = Vector(empty, empty)
    val memory = StringRankSplit.sortAndSplit(batches, partitions = 3, parallelism = 1)

    withConf(-1L, batchSize = 1) { dir =>
      val spilled =
        StringRankSplitRuns.sortAndSplit(batches, partitions = 3, parallelism = 1, ScarletConf.get)
      val absent =
        StringRankSplitRuns.sortAndSplit(Vector.empty, partitions = 3, parallelism = 1, ScarletConf.get)

      identical(spilled, memory)
      spilled.map(_.length) shouldBe Vector(0, 0, 0)
      absent.map(_.length) shouldBe Vector(0, 0, 0)
      at(spilled, 0) shouldNot be theSameInstanceAs at(spilled, 1)
      at(absent, 0) shouldNot be theSameInstanceAs at(absent, 1)
      StringRankSplitRuns.filesWrittenForTest shouldBe 0L
      StringRankSplitRuns.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "take" should "match the row path across a partition boundary" in {
    val source = keyed(Seq("d" -> 4, "b" -> 2, "a" -> 1), Seq("c" -> 3, "a" -> 5, "b" -> 6))
    val plan = source.sortByStringKey.plan
    val expected = rowOf(source.sortByStringKey)

    withConf(0L, batchSize = 1, fanIn = 8, shufflePartitions = 2) { dir =>
      val spilled = sortedParts(plan)

      spilled.map(_.length) shouldBe Vector(3, 3)
      expected.take(4) shouldBe Seq("a" -> 1, "a" -> 5, "b" -> 2, "b" -> 6)
      StringRankSplitRuns.resetForTest()
      ColumnMetrics.withSink(new CountingSink) {
        val taken = ColumnarPlanner.tryTake(plan, 4)
        taken shouldBe Some(expected.take(4))
        ColumnMetrics.decodedValueCount shouldBe 4L
      }
      source.sortByStringKey.take(4) shouldBe expected.take(4)
      StringRankSplitRuns.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a write failure" should "delete the run it already published" in {
    val batches = Vector(
      BatchCodec.stringIntPairs(Seq("b" -> 1, "a" -> 2)),
      BatchCodec.stringIntPairs(Seq("c" -> 3)),
    )

    withConf(0L, batchSize = 1, fanIn = 2) { dir =>
      StringRankSplitRuns.armWriteFailure()
      val caught = intercept[IllegalStateException] {
        val _ =
          StringRankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage shouldBe StringRankSplitRuns.WriteFailed
      StringRankSplitRuns.filesWrittenForTest shouldBe 1L
      Path.of(StringRankSplitRuns.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a merge publish failure" should "delete the runs it already published" in {
    val batches = Vector.tabulate(5)(index => BatchCodec.stringIntPairs(Seq(s"k$index" -> index)))

    withConf(0L, batchSize = 1, fanIn = 2) { dir =>
      StringRankSplitRuns.armWriteFailureAfter(5)
      val caught = intercept[IllegalStateException] {
        val _ =
          StringRankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage shouldBe StringRankSplitRuns.WriteFailed
      StringRankSplitRuns.filesWrittenForTest shouldBe 6L
      StringRankSplitRuns.mergePassesForTest shouldBe 0L
      Path.of(StringRankSplitRuns.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a merge failure" should "delete the intermediate runs" in {
    val batches = Vector.tabulate(5)(index => BatchCodec.stringIntPairs(Seq(s"k$index" -> index)))

    withConf(0L, batchSize = 1, fanIn = 2) { dir =>
      StringRankSplitRuns.armPassFailure(2)
      val caught = intercept[IllegalStateException] {
        val _ =
          StringRankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage shouldBe StringRankSplitRuns.MergeFailed
      StringRankSplitRuns.filesWrittenForTest shouldBe 7L
      StringRankSplitRuns.mergePassesForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "bad arguments" should "be rejected before a file is written" in {
    val rows = Vector(BatchCodec.stringIntPairs(Seq("a" -> 1)))
    val width = intercept[IllegalArgumentException] {
      val _ = StringRankSplitRuns.sortAndSplit(rows, 0, 1, confAt(0L, enabled = true))
    }
    val workers = intercept[IllegalArgumentException] {
      val _ = StringRankSplitRuns.sortAndSplit(rows, 1, 0, confAt(0L, enabled = true))
    }

    width.getMessage should include("numPartitions must be positive, got 0")
    workers.getMessage should include("parallelism must be positive, got 0")

    withConf(0L, batchSize = 1, fanIn = 1) { dir =>
      val caught = intercept[IllegalArgumentException] {
        val _ =
          StringRankSplitRuns.sortAndSplit(rows, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage should include("spillFanIn must be >= 2, got 1")
      StringRankSplitRuns.filesWrittenForTest shouldBe 0L
      StringRankSplitRuns.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a store that fails to open" should "not record a directory" in {
    val rows = Vector(BatchCodec.stringIntPairs(Seq("a" -> 1)))

    withConf(0L, batchSize = 1, fanIn = 2, maxFiles = 0) { dir =>
      val caught = intercept[IllegalArgumentException] {
        val _ =
          StringRankSplitRuns.sortAndSplit(rows, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage should include("maxFiles must be > 0")
      StringRankSplitRuns.lastDirectoryForTest shouldBe ""
      StringRankSplitRuns.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "sortAndSplitSelected spill" should "match the in-memory split and write the survivor index" in {
    val batch = stringBatch(
      Seq(Some("b") -> 1, None -> 2, Some("") -> 3, Some("a") -> 4),
      valueAt = Set(0, 1, 3),
    )
    val dropped = stringBatch(Seq(Some("z") -> 9))
    // Drop "b" and the second batch. A null key orders as "" and stays null. "" stays present.
    // The empty-string survivor keeps a null value.
    val selected = Vector(Selected(batch, Array(1, 2, 3), 3), Selected(dropped, Array(0), 0))
    val memory = StringRankSplit.sortAndSplitSelected(selected, partitions = 2, parallelism = 1)

    withConf(0L, batchSize = 2, fanIn = 2, shufflePartitions = 2) { dir =>
      val spilled = StringRankSplitRuns.sortAndSplitSelected(
        selected,
        partitions = 2,
        parallelism = 2,
        ScarletConf.get,
      )
      val flat = spilled.flatMap(one => cells(one))

      identical(spilled, memory)
      flat.exists { case (None, Some(2)) => true; case _ => false } shouldBe true
      flat.exists { case (Some(""), None) => true; case _ => false } shouldBe true
      flat.exists { case (Some("b"), Some(1)) => true; case _ => false } shouldBe false
      flat.exists { case (Some("z"), Some(9)) => true; case _ => false } shouldBe false
      StringRankSplitRuns.filesWrittenForTest shouldBe 1L
      StringRankSplitRuns.mergePassesForTest shouldBe 0L
      RankSplitRuns.filesWrittenForTest shouldBe 0L
      RankSplitLongRuns.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }

    withConf(0L, enabled = false, shufflePartitions = 2) { dir =>
      val stayed = StringRankSplitRuns.sortAndSplitSelected(
        selected,
        partitions = 2,
        parallelism = 1,
        ScarletConf.get,
      )

      identical(stayed, memory)
      StringRankSplitRuns.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a selected charge" should "bill survivor rows and the dictionary the sort keeps" in {
    val batch = BatchCodec.stringIntPairs(Seq("aaaa" -> 1, "b" -> 2))
    val selected = Vector(Selected(batch, Array(1), 1))
    val dictionary = utf8(batch).dictionary
    val full = dictionary.iterator.map(utf8Bytes).sum
    val live = dictionary.iterator.filter(_ != "aaaa").map(utf8Bytes).sum
    val perRow = 8L + 4L + 4L + 1L
    val kept = perRow + full + StringRankSplitRuns.HeaderAllowance
    val survivorDict = perRow + live + StringRankSplitRuns.HeaderAllowance
    val memory = StringRankSplit.sortAndSplitSelected(selected, partitions = 1, parallelism = 1)

    dictionary.toSeq should contain("aaaa")
    full should be > live
    StringRankSplitRuns.retainedStringBytes(Vector(batch)) should be > kept

    withConf(survivorDict, batchSize = 8, fanIn = 2) { _ =>
      val spilled = StringRankSplitRuns.sortAndSplitSelected(
        selected,
        partitions = 1,
        parallelism = 1,
        ScarletConf.get,
      )

      identical(spilled, memory)
      StringRankSplitRuns.filesWrittenForTest should be > 0L
    }
    withConf(kept, batchSize = 8, fanIn = 2) { dir =>
      val stayed = StringRankSplitRuns.sortAndSplitSelected(
        selected,
        partitions = 1,
        parallelism = 1,
        ScarletConf.get,
      )

      identical(stayed, memory)
      StringRankSplitRuns.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
    withConf(0L, enabled = false) { dir =>
      val off = StringRankSplitRuns.sortAndSplitSelected(
        selected,
        partitions = 1,
        parallelism = 1,
        ScarletConf.get,
      )

      identical(off, memory)
      StringRankSplitRuns.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  private def identical(actual: Vector[ColumnBatch], expected: Vector[ColumnBatch]): Unit =
    actual.length shouldBe expected.length
    actual.zip(expected).foreach { (left, right) =>
      left.length shouldBe right.length
      left.schema shouldBe right.schema
      val leftKeys = utf8(left)
      val rightKeys = utf8(right)
      leftKeys.dictionary.toSeq shouldBe rightKeys.dictionary.toSeq
      leftKeys.codes.take(left.length).toSeq shouldBe rightKeys.codes.take(right.length).toSeq
      presences(leftKeys, left.length) shouldBe presences(rightKeys, right.length)
      val leftValues = int32(left)
      val rightValues = int32(right)
      leftValues.values.take(left.length).toSeq shouldBe rightValues.values.take(right.length).toSeq
      presences(leftValues, left.length) shouldBe presences(rightValues, right.length)
    }

  private def presences(column: Column, rows: Int): Seq[Boolean] =
    (0 until rows).map(column.isValid)

  private def cells(batch: ColumnBatch): Seq[(Option[String], Option[Int])] =
    val keys = utf8(batch)
    val values = int32(batch)
    (0 until batch.length).map { row =>
      val key = if (keys.isValid(row)) Some(keys.dictionary(keys.codes(row))) else None
      val value = if (values.isValid(row)) Some(values.values(row)) else None
      (key, value)
    }

  private def expectedCharge(batch: ColumnBatch): Long =
    if (batch.length == 0) then 0L
    else
      val payload = utf8(batch).dictionary.iterator.map(utf8Bytes).sum
      batch.length.toLong * (8L + 4L + 4L + 1L) + payload + StringRankSplitRuns.HeaderAllowance

  private def utf8Bytes(text: String): Long =
    text.getBytes(StandardCharsets.UTF_8).length.toLong

  private def stringBatch(
      rows: Seq[(Option[String], Int)],
      valueAt: Set[Int] = Set.empty,
  ): ColumnBatch =
    val presentValues = if (valueAt.isEmpty) then (0 until rows.length).toSet else valueAt
    val keys = DictUtf8Column.fromNullable(rows.map(_._1))
    val values = Int32Column.of(
      rows.map(_._2).toArray,
      Validity.pack(rows.length, presentValues.contains),
      rows.length,
    )
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(keys, values),
      rows.length,
    )

  private def keyed(
      left: Seq[(String, Int)],
      right: Seq[(String, Int)],
  ): DistCollection[(String, Int)] =
    DistCollection(Plan.Source(Seq(Partition(left), Partition(right))))

  private def sourcePairs(plan: Plan[(String, Int)]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered source"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.StringPairs(parts) => parts
      case other => fail(s"expected string pairs, got $other")

  private def sortedParts(plan: Plan[(String, Int)]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered sort"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.StringPairs(parts) => parts
      case other => fail(s"expected string pairs, got $other")

  private def at(values: Vector[ColumnBatch], index: Int): ColumnBatch =
    values.lift(index).getOrElse(fail(s"missing index $index"))

  private def utf8(batch: ColumnBatch): DictUtf8Column =
    batch.columns.lift(0) match
      case Some(column: DictUtf8Column) => column
      case other => fail(s"expected Utf8Dict, got $other")

  private def int32(batch: ColumnBatch): Int32Column =
    batch.columns.lift(1) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32, got $other")

  private def collect[A](collection: DistCollection[A]): List[A] =
    ScarletRuntime.get.shuffle.clear()
    collection.collect().toList

  private def rowOf[A](collection: DistCollection[A]): List[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      collection.collect().toList
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def effective(key: String): String = key match
    case null => ""
    case text => text

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def isNullKey(key: String): Boolean = key match
    case null => true
    case _ => false

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def isEmptyKey(key: String): Boolean = key match
    case null => false
    case text => text.isEmpty

  private def confAt(budget: Long, enabled: Boolean): ScarletConf =
    originalConf.copy(spillEnabled = enabled, spillBudgetBytes = budget)

  private def withConf(
      budget: Long,
      batchSize: Int = 8,
      fanIn: Int = 8,
      enabled: Boolean = true,
      shufflePartitions: Int = 4,
      maxFiles: Int = SpillStore.DefaultMaxFiles,
  )(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-rank-split-string")
    StringRankSplitRuns.resetForTest()
    RankSplitRuns.resetForTest()
    RankSplitLongRuns.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = shufflePartitions,
        threadPoolSize = 1,
        spillEnabled = enabled,
        spillBudgetBytes = budget,
        spillDirectory = dir.toString,
        spillMaxFiles = maxFiles,
        spillMaxBytes = SpillStore.DefaultMaxBytes,
        spillFanIn = fanIn,
      ),
    )
    try body(dir)
    finally
      ScarletConf.set(originalConf)
      StringRankSplitRuns.resetForTest()
      RankSplitRuns.resetForTest()
      RankSplitLongRuns.resetForTest()
      deleteTree(dir)

  private def deleteTree(dir: Path): Unit =
    SpillStore.listForTest(dir).foreach { path =>
      val _ = Files.deleteIfExists(path)
    }
    val _ = Files.deleteIfExists(dir)
