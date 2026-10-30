package com.ewoodbury.scarlet.execution

import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Spilled rank split of a long-pair sort. The in-memory path is [[RankSplitLong]]. Partition
 * boundaries are that split, not the row executor's. A counter assertion is what shows the spill
 * ran. The int counters stay at zero.
 */
class TestRankSplitLongRuns extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    RankSplitLongRuns.resetForTest()
    RankSplitRuns.resetForTest()
    ColumnMetrics.restore(NoOpSink)
    ()

  "the trigger" should "charge each non-empty long batch and stay off at the budget" in {
    val one = BatchCodec.longPairs(Seq((1L, 1L)))
    val two = BatchCodec.longPairs(Seq((2L, 2L), (3L, 3L)))
    val empty = BatchCodec.longPairs(Seq.empty[(Long, Long)])
    val charged = RankSplitLongRuns.retainedLongBytes(Vector(one))

    RankSplitLongRuns.retainedLongBytes(Vector.empty) shouldBe 0L
    RankSplitLongRuns.retainedLongBytes(Vector(empty)) shouldBe 0L
    RankSplitLongRuns.retainedLongBytes(Vector(one)) shouldBe charged
    RankSplitLongRuns.retainedLongBytes(Vector(one, two, empty)) shouldBe
      charged + RankSplitLongRuns.retainedLongBytes(Vector(two))

    val batches = Vector(one, two)
    val estimate = RankSplitLongRuns.retainedLongBytes(batches)
    RankSplitLongRuns.exceeds(batches, confAt(estimate, enabled = false)) shouldBe false
    RankSplitLongRuns.exceeds(batches, confAt(estimate, enabled = true)) shouldBe false
    RankSplitLongRuns.exceeds(batches, confAt(estimate - 1L, enabled = true)) shouldBe true
    RankSplitLongRuns.exceeds(Vector(empty, empty), confAt(0L, enabled = true)) shouldBe false
  }

  "a split at the budget" should "match RankSplitLong and write no files" in {
    val batches = Vector(
      BatchCodec.longPairs(Seq((3L, 1L), (1L, 2L))),
      BatchCodec.longPairs(Seq((2L, 3L), (0L, 4L))),
    )
    val estimate = RankSplitLongRuns.retainedLongBytes(batches)
    val memory = RankSplitLong.sortAndSplit(batches, partitions = 3, parallelism = 2)

    withConf(estimate, batchSize = 1, fanIn = 2, shufflePartitions = 3) { dir =>
      val spilled =
        RankSplitLongRuns.sortAndSplit(batches, partitions = 3, parallelism = 2, ScarletConf.get)

      identical(spilled, memory)
      RankSplitLongRuns.filesWrittenForTest shouldBe 0L
      RankSplitLongRuns.mergePassesForTest shouldBe 0L
      RankSplitLongRuns.lastDirectoryForTest shouldBe ""
      RankSplitRuns.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill disabled" should "match today and write no files even when the budget is zero" in {
    val source = DistCollection.ofLongPairs(Array(4L, 1L, 3L, 2L), Array(40L, 10L, 30L, 20L), 2)

    withConf(0L, enabled = false, shufflePartitions = 4) { dir =>
      val input = sourcePairs(source.plan)
      val plan = source.sortByLongKey.plan
      val sorted = sortedParts(plan)
      val memory = RankSplitLong.sortAndSplit(input, partitions = 4, parallelism = 1)

      identical(sorted, memory)
      collect(source.sortByLongKey) shouldBe rowCollect(source.sortByLongKey)
      RankSplitLongRuns.filesWrittenForTest shouldBe 0L
      RankSplitLongRuns.lastDirectoryForTest shouldBe ""
      RankSplitRuns.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("SortByLongKey(Scan)")
    }
  }

  "a tiny budget" should "spill a small input, keep the boundaries, and decode only the prefix" in {
    val source = DistCollection.ofLongPairs(Array(2L, 0L, 1L), Array(20L, 0L, 10L), 2)
    // Two source partitions, one merge. Three files, one pass. Output widths are 1, 1, 1, 0.
    withConf(0L, batchSize = 2, fanIn = 8, shufflePartitions = 4) { dir =>
      val input = sourcePairs(source.plan)
      val plan = source.sortByLongKey.plan
      val spilled = sortedParts(plan)
      val memory = RankSplitLong.sortAndSplit(input, partitions = 4, parallelism = 1)

      input.map(_.length) shouldBe Vector(2, 1)
      RankSplitLongRuns.retainedLongBytes(input) should be > 0L
      identical(spilled, memory)
      spilled.map(_.length) shouldBe Vector(1, 1, 1, 0)
      RankSplitLongRuns.filesWrittenForTest shouldBe 3L
      RankSplitLongRuns.mergePassesForTest shouldBe 1L
      Path.of(RankSplitLongRuns.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("SortByLongKey(Scan)")
      RankSplitRuns.filesWrittenForTest shouldBe 0L

      RankSplitLongRuns.resetForTest()
      collect(source.sortByLongKey) shouldBe rowCollect(source.sortByLongKey)
      RankSplitLongRuns.filesWrittenForTest shouldBe 3L

      RankSplitLongRuns.resetForTest()
      ColumnMetrics.withSink(new CountingSink) {
        val taken = ColumnarPlanner.tryTake(plan, 1)
        taken shouldBe Some(Seq(0L -> 0L))
        ColumnMetrics.decodedValueCount shouldBe 1L
      }
      source.sortByLongKey.take(1) shouldBe List(0L -> 0L)
      // tryTake and take each spill the two runs and their merge.
      RankSplitLongRuns.filesWrittenForTest shouldBe 6L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "more runs than the fan-in" should "take several passes and keep duplicate keys across the cut" in {
    val empty = BatchCodec.longPairs(Seq.empty[(Long, Long)])
    // Five non-empty runs and one empty batch. fanIn 2 reduces 5, then 3, then 2: three passes.
    // Initial files 5, plus 2, plus 1, plus 1. The empty batch is not a file.
    val batches = Vector(
      BatchCodec.longPairs(Seq((1L, 10L))),
      empty,
      BatchCodec.longPairs(Seq((1L, 11L))),
      BatchCodec.longPairs(Seq((1L, 12L))),
      BatchCodec.longPairs(Seq((1L, 13L))),
      BatchCodec.longPairs(Seq((1L, 14L))),
    )
    val memory = RankSplitLong.sortAndSplit(batches, partitions = 2, parallelism = 1)

    withConf(0L, batchSize = 1, fanIn = 2, shufflePartitions = 2) { dir =>
      val serial =
        RankSplitLongRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)

      identical(serial, memory)
      cells(serial.lift(0).getOrElse(fail("missing partition 0"))) shouldBe Seq(
        (Some(1L), Some(10L)),
        (Some(1L), Some(11L)),
        (Some(1L), Some(12L)),
      )
      cells(serial.lift(1).getOrElse(fail("missing partition 1"))) shouldBe Seq(
        (Some(1L), Some(13L)),
        (Some(1L), Some(14L)),
      )
      RankSplitLongRuns.filesWrittenForTest shouldBe 9L
      RankSplitLongRuns.mergePassesForTest shouldBe 3L

      RankSplitLongRuns.resetForTest()
      val parallel =
        RankSplitLongRuns.sortAndSplit(batches, partitions = 2, parallelism = 3, ScarletConf.get)

      identical(parallel, memory)
      RankSplitLongRuns.filesWrittenForTest shouldBe 9L
      RankSplitLongRuns.mergePassesForTest shouldBe 3L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "null keys and null values" should "survive a spill, including Long.MinValue and a duplicate across the cut" in {
    val batches = Vector(
      longBatch(Seq((Some(1L), Some(10L)), (None, Some(1L)))),
      longBatch(Seq((Some(1L), None))),
      longBatch(Seq((Some(0L), Some(2L)))),
      longBatch(Seq((Some(1L), Some(11L)))),
      longBatch(Seq((Some(Long.MinValue), Some(7L)), (Some(Long.MaxValue), None))),
    )
    val memory = RankSplitLong.sortAndSplit(batches, partitions = 2, parallelism = 1)

    withConf(0L, batchSize = 1, fanIn = 2, shufflePartitions = 2) { dir =>
      val spilled =
        RankSplitLongRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)

      identical(spilled, memory)
      cells(spilled.lift(0).getOrElse(fail("missing partition 0"))) shouldBe Seq(
        (Some(Long.MinValue), Some(7L)),
        (None, Some(1L)),
        (Some(0L), Some(2L)),
        (Some(1L), Some(10L)),
      )
      cells(spilled.lift(1).getOrElse(fail("missing partition 1"))) shouldBe Seq(
        (Some(1L), None),
        (Some(1L), Some(11L)),
        (Some(Long.MaxValue), None),
      )
      // Five runs, fanIn 2: passes 5 -> 3 -> 2 -> 1, and 5 + 2 + 1 + 1 files.
      RankSplitLongRuns.filesWrittenForTest shouldBe 9L
      RankSplitLongRuns.mergePassesForTest shouldBe 3L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  it should "order a null key as zero when its payload is not zero" in {
    val keys = Int64Column.of(Array(7L, 1L), Validity.pack(2, row => row == 1), 2)
    val values = Int64Column.of(Array(10L, 20L), Validity.allValid(2), 2)
    val first = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(keys, values),
      2,
    )
    val dirty = Int64Column.of(Array(0L, 9L), Validity.pack(2, row => row == 0), 2)
    val secondValues = Int64Column.of(Array(30L, 0L), Validity.pack(2, row => row == 0), 2)
    val secondBatch = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(dirty, secondValues),
      2,
    )
    val batches = Vector(first, secondBatch)
    val memory = RankSplitLong.sortAndSplit(batches, partitions = 2, parallelism = 1)

    withConf(0L, batchSize = 1, fanIn = 2) { dir =>
      val spilled =
        RankSplitLongRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)

      identical(spilled, memory)
      cells(spilled.lift(0).getOrElse(fail("missing partition 0"))) shouldBe Seq(
        (None, Some(10L)),
        (Some(0L), Some(30L)),
      )
      cells(spilled.lift(1).getOrElse(fail("missing partition 1"))) shouldBe Seq(
        (None, None),
        (Some(1L), Some(20L)),
      )
      RankSplitLongRuns.filesWrittenForTest shouldBe 3L
      RankSplitLongRuns.mergePassesForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "one sorted run" should "write a single file and still split a duplicate key" in {
    val batches = Vector(BatchCodec.longPairs(Seq((1L, 10L), (1L, 11L), (1L, 12L), (2L, 20L))))
    val memory = RankSplitLong.sortAndSplit(batches, partitions = 2, parallelism = 1)
    val gapped = Vector(BatchCodec.longPairs(Seq((5L, 1L), (1L, 2L))))
    val gappedMemory = RankSplitLong.sortAndSplit(gapped, partitions = 5, parallelism = 1)

    withConf(0L, batchSize = 1, fanIn = 2, shufflePartitions = 2) { dir =>
      val spilled =
        RankSplitLongRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)

      identical(spilled, memory)
      cells(spilled.lift(0).getOrElse(fail("missing partition 0"))) shouldBe Seq(
        (Some(1L), Some(10L)),
        (Some(1L), Some(11L)),
      )
      cells(spilled.lift(1).getOrElse(fail("missing partition 1"))) shouldBe Seq(
        (Some(1L), Some(12L)),
        (Some(2L), Some(20L)),
      )
      RankSplitLongRuns.filesWrittenForTest shouldBe 1L
      RankSplitLongRuns.mergePassesForTest shouldBe 0L

      // Two rows across five outputs leaves an empty partition between the two occupied ones.
      RankSplitLongRuns.resetForTest()
      val wide =
        RankSplitLongRuns.sortAndSplit(gapped, partitions = 5, parallelism = 1, ScarletConf.get)

      identical(wide, gappedMemory)
      wide.map(_.length) shouldBe Vector(1, 0, 1, 0, 0)
      RankSplitLongRuns.filesWrittenForTest shouldBe 1L
      RankSplitLongRuns.mergePassesForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "write nothing when the budget is negative" in {
    val empty = BatchCodec.longPairs(Seq.empty[(Long, Long)])
    val batches = Vector(empty, empty)
    val memory = RankSplitLong.sortAndSplit(batches, partitions = 3, parallelism = 1)

    withConf(-1L, batchSize = 1) { dir =>
      val spilled =
        RankSplitLongRuns.sortAndSplit(batches, partitions = 3, parallelism = 1, ScarletConf.get)

      identical(spilled, memory)
      spilled.map(_.length) shouldBe Vector(0, 0, 0)
      RankSplitLongRuns.filesWrittenForTest shouldBe 0L
      RankSplitLongRuns.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "sortByLongKey over nulls" should "spill, match the in-memory split, and decode a short prefix" in {
    val rows = Seq(
      longPair(boxedLong(1L), boxedLong(10L)),
      longPair(noLong, boxedLong(1L)),
      longPair(boxedLong(0L), boxedLong(2L)),
      longPair(boxedLong(1L), noLong),
      longPair(boxedLong(1L), boxedLong(11L)),
    )
    val source = DistCollection(rows, 5)
    val plan = source.sortByLongKey.plan

    val memoryParts = withMemory(plan, shufflePartitions = 2)
    withConf(0L, batchSize = 1, fanIn = 2, shufflePartitions = 2) { dir =>
      val spilled = sortedParts(plan)

      identical(spilled, memoryParts)
      cells(spilled.lift(0).getOrElse(fail("missing partition 0"))) shouldBe Seq(
        (None, Some(1L)),
        (Some(0L), Some(2L)),
        (Some(1L), Some(10L)),
      )
      cells(spilled.lift(1).getOrElse(fail("missing partition 1"))) shouldBe Seq(
        (Some(1L), None),
        (Some(1L), Some(11L)),
      )
      RankSplitLongRuns.filesWrittenForTest shouldBe 9L
      RankSplitLongRuns.mergePassesForTest shouldBe 3L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]

      RankSplitLongRuns.resetForTest()
      val off = withMemoryTake(plan, 4)
      ColumnMetrics.withSink(new CountingSink) {
        val taken = ColumnarPlanner.tryTake(plan, 4)
        taken shouldBe Some(off)
        val prefix = taken.getOrElse(fail("missing prefix"))
        prefix.exists(pair => !present(pair, 0)) shouldBe true
        prefix.exists(pair => !present(pair, 1)) shouldBe true
        ColumnMetrics.decodedValueCount shouldBe 4L
      }
      RankSplitLongRuns.filesWrittenForTest shouldBe 9L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a write failure" should "delete the run it already published" in {
    val batches = Vector(
      BatchCodec.longPairs(Seq((1L, 1L), (3L, 3L))),
      BatchCodec.longPairs(Seq((2L, 2L))),
    )

    withConf(0L, batchSize = 1, fanIn = 2) { dir =>
      RankSplitLongRuns.armWriteFailure()
      val caught = intercept[IllegalStateException] {
        val _ =
          RankSplitLongRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage shouldBe RankSplitLongRuns.WriteFailed
      RankSplitLongRuns.filesWrittenForTest shouldBe 1L
      Path.of(RankSplitLongRuns.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a merge publish failure" should "delete the runs it already published" in {
    val batches =
      Vector.tabulate(5)(index => BatchCodec.longPairs(Seq((index.toLong, index.toLong + 1L))))

    withConf(0L, batchSize = 1, fanIn = 2) { dir =>
      // Five initial runs publish. The next publish is the first merged run.
      RankSplitLongRuns.armWriteFailureAfter(5)
      val caught = intercept[IllegalStateException] {
        val _ =
          RankSplitLongRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage shouldBe RankSplitLongRuns.WriteFailed
      RankSplitLongRuns.filesWrittenForTest shouldBe 6L
      RankSplitLongRuns.mergePassesForTest shouldBe 0L
      Path.of(RankSplitLongRuns.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a merge failure" should "delete the intermediate runs" in {
    val batches =
      Vector.tabulate(5)(index => BatchCodec.longPairs(Seq((index.toLong, index.toLong + 1L))))

    withConf(0L, batchSize = 1, fanIn = 2) { dir =>
      RankSplitLongRuns.armPassFailure(2)
      val caught = intercept[IllegalStateException] {
        val _ =
          RankSplitLongRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage shouldBe RankSplitLongRuns.MergeFailed
      /* Five initial runs plus two from the first pass. The second pass throws before it
       * publishes. */
      RankSplitLongRuns.filesWrittenForTest shouldBe 7L
      RankSplitLongRuns.mergePassesForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "bad arguments" should "be rejected before a file is written" in {
    val empty = BatchCodec.longPairs(Seq.empty[(Long, Long)])
    val rows = Vector(BatchCodec.longPairs(Seq((1L, 1L))))
    val missing = intercept[IllegalArgumentException] {
      val _ = RankSplitLongRuns.sortAndSplit(Vector.empty, 2, 1, confAt(0L, enabled = true))
    }
    val width = intercept[IllegalArgumentException] {
      val _ = RankSplitLongRuns.sortAndSplit(Vector(empty), 0, 1, confAt(0L, enabled = true))
    }
    val workers = intercept[IllegalArgumentException] {
      val _ = RankSplitLongRuns.sortAndSplit(Vector(empty), 1, 0, confAt(0L, enabled = true))
    }

    missing.getMessage should include("sortAndSplit needs at least one batch")
    width.getMessage should include("numPartitions must be positive, got 0")
    workers.getMessage should include("parallelism must be positive, got 0")

    withConf(0L, batchSize = 1, fanIn = 1) { dir =>
      val caught = intercept[IllegalArgumentException] {
        val _ =
          RankSplitLongRuns.sortAndSplit(rows, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage should include("spillFanIn must be >= 2, got 1")
      RankSplitLongRuns.filesWrittenForTest shouldBe 0L
      RankSplitLongRuns.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a store that fails to open" should "not record a directory" in {
    val rows = Vector(BatchCodec.longPairs(Seq((1L, 1L))))

    withConf(0L, batchSize = 1, fanIn = 2, maxFiles = 0) { dir =>
      val caught = intercept[IllegalArgumentException] {
        val _ =
          RankSplitLongRuns.sortAndSplit(rows, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage should include("maxFiles must be > 0")
      RankSplitLongRuns.lastDirectoryForTest shouldBe ""
      RankSplitLongRuns.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  private def identical(actual: Vector[ColumnBatch], expected: Vector[ColumnBatch]): Unit =
    actual.length shouldBe expected.length
    actual.zip(expected).foreach { (left, right) =>
      left.length shouldBe right.length
      left.schema shouldBe right.schema
      columnImage(left, 0) shouldBe columnImage(right, 0)
      columnImage(left, 1) shouldBe columnImage(right, 1)
    }

  private def columnImage(batch: ColumnBatch, ordinal: Int): (Seq[Long], Seq[Boolean]) =
    val column = int64(batch, ordinal)
    val values = (0 until batch.length).map(row => column.values(row))
    val valid = (0 until batch.length).map(row => column.isValid(row))
    (values, valid)

  private def cells(batch: ColumnBatch): Seq[(Option[Long], Option[Long])] =
    val keys = int64(batch, 0)
    val values = int64(batch, 1)
    (0 until batch.length).map { row =>
      val key = if (keys.isValid(row)) then Some(keys.values(row)) else None
      val value = if (values.isValid(row)) then Some(values.values(row)) else None
      (key, value)
    }

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")

  private def longBatch(rows: Seq[(Option[Long], Option[Long])]): ColumnBatch =
    BatchCodec.longPairs(
      rows.map { (key, value) =>
        longPair(key.fold(noLong)(boxedLong), value.fold(noLong)(boxedLong))
      },
    )

  private def sourcePairs(plan: Plan[(Long, Long)]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered source"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.LongPairs(parts) => parts
      case other => fail(s"expected long pairs, got $other")

  private def sortedParts(plan: Plan[(Long, Long)]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered sort"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.LongPairs(parts) => parts
      case other => fail(s"expected long pairs, got $other")

  private def withMemory(plan: Plan[(Long, Long)], shufflePartitions: Int): Vector[ColumnBatch] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(
        saved.copy(
          columnarExecution = true,
          spillEnabled = false,
          defaultShufflePartitions = shufflePartitions,
          threadPoolSize = 1,
        ),
      )
      sortedParts(plan)
    finally ScarletConf.set(saved)

  private def withMemoryTake(plan: Plan[(Long, Long)], count: Int): Seq[(Long, Long)] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(spillEnabled = false))
      RankSplitLongRuns.resetForTest()
      val taken = ColumnarPlanner.tryTake(plan, count).getOrElse(fail("missing in-memory prefix"))
      RankSplitLongRuns.filesWrittenForTest shouldBe 0L
      taken
    finally
      RankSplitLongRuns.resetForTest()
      ScarletConf.set(saved)

  private def collect[A](collection: DistCollection[A]): Seq[A] =
    ScarletRuntime.get.shuffle.clear()
    collection.collect().toSeq

  private def rowCollect[A](collection: DistCollection[A]): Seq[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      collection.collect().toSeq
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def present(pair: (Long, Long), ordinal: Int): Boolean =
    pair.productElement(ordinal) match
      case _: java.lang.Long => true
      case _ => false

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def longPair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noLong: java.lang.Long = null

  private def boxedLong(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  private def confAt(budget: Long, enabled: Boolean): ScarletConf =
    originalConf.copy(spillEnabled = enabled, spillBudgetBytes = budget)

  private[execution] def withConf(
      budget: Long,
      batchSize: Int = 8,
      fanIn: Int = 8,
      enabled: Boolean = true,
      shufflePartitions: Int = 4,
      maxFiles: Int = SpillStore.DefaultMaxFiles,
  )(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-rank-split-long")
    RankSplitLongRuns.resetForTest()
    RankSplitRuns.resetForTest()
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
      RankSplitLongRuns.resetForTest()
      deleteTree(dir)

  private def deleteTree(dir: Path): Unit =
    SpillStore.listForTest(dir).foreach { path =>
      val _ = Files.deleteIfExists(path)
    }
    val _ = Files.deleteIfExists(dir)
