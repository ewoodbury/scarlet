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
 * Spilled rank split of an int-pair sort. The in-memory path is [[RankSplit]]. Partition boundaries
 * are that split, not the row executor's. A counter assertion is what shows the spill ran.
 */
class TestRankSplitRuns extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    RankSplitRuns.resetForTest()
    ColumnMetrics.restore(NoOpSink)
    ()

  "the trigger" should "charge each non-empty batch and stay off at the budget" in {
    val one = BatchCodec.intPairs(Seq((1, 1)))
    val two = BatchCodec.intPairs(Seq((2, 2), (3, 3)))
    val empty = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val charged = 1L * (4L + 4L + 1L) + RankSplitRuns.HeaderAllowance

    RankSplitRuns.retainedBytes(Vector.empty) shouldBe 0L
    RankSplitRuns.retainedBytes(Vector(empty)) shouldBe 0L
    RankSplitRuns.retainedBytes(Vector(one)) shouldBe charged
    RankSplitRuns.retainedBytes(Vector(one, two, empty)) shouldBe
      charged + (2L * (4L + 4L + 1L) + RankSplitRuns.HeaderAllowance)

    val batches = Vector(one, two)
    val estimate = RankSplitRuns.retainedBytes(batches)
    RankSplitRuns.exceeds(batches, confAt(estimate, enabled = false)) shouldBe false
    RankSplitRuns.exceeds(batches, confAt(estimate, enabled = true)) shouldBe false
    RankSplitRuns.exceeds(batches, confAt(estimate - 1L, enabled = true)) shouldBe true
    RankSplitRuns.exceeds(Vector(empty, empty), confAt(0L, enabled = true)) shouldBe false
  }

  "a split at the budget" should "match RankSplit and write no files" in {
    val batches = Vector(
      BatchCodec.intPairs(Seq((3, 1), (1, 2))),
      BatchCodec.intPairs(Seq((2, 3), (0, 4))),
    )
    val estimate = RankSplitRuns.retainedBytes(batches)
    val memory = RankSplit.sortAndSplit(batches, partitions = 3, parallelism = 2)

    withConf(estimate, batchSize = 1, fanIn = 2, shufflePartitions = 3) { dir =>
      val spilled = RankSplitRuns.sortAndSplit(batches, partitions = 3, parallelism = 2, ScarletConf.get)

      identical(spilled, memory)
      RankSplitRuns.filesWrittenForTest shouldBe 0L
      RankSplitRuns.mergePassesForTest shouldBe 0L
      RankSplitRuns.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill disabled" should "match today and write no files even when the budget is zero" in {
    val source = DistCollection.ofIntPairs(Array(4, 1, 3, 2), Array(40, 10, 30, 20), 2)

    withConf(0L, enabled = false, shufflePartitions = 4) { dir =>
      val input = sourcePairs(source.plan)
      val sorted = sortedParts(source.sortByIntKey.plan)
      val memory = RankSplit.sortAndSplit(input, partitions = 4, parallelism = 1)

      identical(sorted, memory)
      collect(source.sortByIntKey) shouldBe rowCollect(source.sortByIntKey)
      RankSplitRuns.filesWrittenForTest shouldBe 0L
      RankSplitRuns.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      ColumnarPlanner.lower(source.sortByIntKey.plan).map(_.shape) shouldBe Some(
        "SortByIntKey(Scan)",
      )
    }
  }

  "a tiny budget" should "spill a small input, keep the boundaries, and decode only the prefix" in {
    val source = DistCollection.ofIntPairs(Array(2, 0, 1), Array(20, 0, 10), 2)
    // Two source partitions, one merge. Three files, one pass. Output widths are 1, 1, 1, 0.
    withConf(0L, batchSize = 2, fanIn = 8, shufflePartitions = 4) { dir =>
      val input = sourcePairs(source.plan)
      val plan = source.sortByIntKey.plan
      val spilled = sortedParts(plan)
      val memory = RankSplit.sortAndSplit(input, partitions = 4, parallelism = 1)

      input.map(_.length) shouldBe Vector(2, 1)
      RankSplitRuns.retainedBytes(input) should be > 0L
      identical(spilled, memory)
      spilled.map(_.length) shouldBe Vector(1, 1, 1, 0)
      RankSplitRuns.filesWrittenForTest shouldBe 3L
      RankSplitRuns.mergePassesForTest shouldBe 1L
      Path.of(RankSplitRuns.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some("SortByIntKey(Scan)")

      RankSplitRuns.resetForTest()
      collect(source.sortByIntKey) shouldBe rowCollect(source.sortByIntKey)
      RankSplitRuns.filesWrittenForTest shouldBe 3L

      RankSplitRuns.resetForTest()
      ColumnMetrics.withSink(new CountingSink) {
        val taken = ColumnarPlanner.tryTake(plan, 1)
        taken shouldBe Some(Seq(0 -> 0))
        ColumnMetrics.decodedValueCount shouldBe 1L
      }
      source.sortByIntKey.take(1) shouldBe List(0 -> 0)
      // tryTake and take each spill the two runs and their merge.
      RankSplitRuns.filesWrittenForTest shouldBe 6L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "more runs than the fan-in" should "take several passes and keep duplicate keys across the cut" in {
    val empty = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    // Five non-empty runs and one empty batch. fanIn 2 reduces 5, then 3, then 2: three passes.
    // Initial files 5, plus 2, plus 1, plus 1. The empty batch is not a file.
    val batches = Vector(
      BatchCodec.intPairs(Seq((1, 10))),
      empty,
      BatchCodec.intPairs(Seq((1, 11))),
      BatchCodec.intPairs(Seq((1, 12))),
      BatchCodec.intPairs(Seq((1, 13))),
      BatchCodec.intPairs(Seq((1, 14))),
    )
    val memory = RankSplit.sortAndSplit(batches, partitions = 2, parallelism = 1)

    withConf(0L, batchSize = 1, fanIn = 2, shufflePartitions = 2) { dir =>
      val serial =
        RankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)

      identical(serial, memory)
      cells(serial.lift(0).getOrElse(fail("missing partition 0"))) shouldBe Seq(
        (Some(1), Some(10)),
        (Some(1), Some(11)),
        (Some(1), Some(12)),
      )
      cells(serial.lift(1).getOrElse(fail("missing partition 1"))) shouldBe Seq(
        (Some(1), Some(13)),
        (Some(1), Some(14)),
      )
      RankSplitRuns.filesWrittenForTest shouldBe 9L
      RankSplitRuns.mergePassesForTest shouldBe 3L

      RankSplitRuns.resetForTest()
      val parallel =
        RankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 3, ScarletConf.get)

      identical(parallel, memory)
      RankSplitRuns.filesWrittenForTest shouldBe 9L
      RankSplitRuns.mergePassesForTest shouldBe 3L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "null keys and null values" should "survive a spill, including a duplicate key across the cut" in {
    val batches = Vector(
      BatchCodec.nullableIntPairs(Seq((Some(1), Some(10)), (None, Some(1)))),
      BatchCodec.nullableIntPairs(Seq((Some(1), None))),
      BatchCodec.nullableIntPairs(Seq((Some(0), Some(2)))),
      BatchCodec.nullableIntPairs(Seq((Some(1), Some(11)))),
      BatchCodec.nullableIntPairs(Seq((Some(Int.MinValue), Some(7)), (Some(Int.MaxValue), None))),
    )
    val memory = RankSplit.sortAndSplit(batches, partitions = 2, parallelism = 1)

    withConf(0L, batchSize = 1, fanIn = 2, shufflePartitions = 2) { dir =>
      val spilled = RankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)

      identical(spilled, memory)
      cells(spilled.lift(0).getOrElse(fail("missing partition 0"))) shouldBe Seq(
        (Some(Int.MinValue), Some(7)),
        (None, Some(1)),
        (Some(0), Some(2)),
        (Some(1), Some(10)),
      )
      cells(spilled.lift(1).getOrElse(fail("missing partition 1"))) shouldBe Seq(
        (Some(1), None),
        (Some(1), Some(11)),
        (Some(Int.MaxValue), None),
      )
      // Five runs, fanIn 2: passes 5 -> 3 -> 2 -> 1, and 5 + 2 + 1 + 1 files.
      RankSplitRuns.filesWrittenForTest shouldBe 9L
      RankSplitRuns.mergePassesForTest shouldBe 3L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  it should "order a null key as zero when its payload is not zero" in {
    val keys = Int32Column.of(Array(7, 1), Validity.pack(2, row => row == 1), 2)
    val values = Int32Column.of(Array(10, 20), Validity.allValid(2), 2)
    val first = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(keys, values),
      2,
    )
    val dirty = Int32Column.of(Array(0, 9), Validity.pack(2, row => row == 0), 2)
    val secondValues = Int32Column.of(Array(30, 0), Validity.pack(2, row => row == 0), 2)
    val secondBatch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(dirty, secondValues),
      2,
    )
    val batches = Vector(first, secondBatch)
    val memory = RankSplit.sortAndSplit(batches, partitions = 2, parallelism = 1)

    withConf(0L, batchSize = 1, fanIn = 2) { dir =>
      val spilled = RankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)

      identical(spilled, memory)
      cells(spilled.lift(0).getOrElse(fail("missing partition 0"))) shouldBe Seq(
        (None, Some(10)),
        (Some(0), Some(30)),
      )
      cells(spilled.lift(1).getOrElse(fail("missing partition 1"))) shouldBe Seq(
        (None, None),
        (Some(1), Some(20)),
      )
      RankSplitRuns.filesWrittenForTest shouldBe 3L
      RankSplitRuns.mergePassesForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "one sorted run" should "write a single file and still split a duplicate key" in {
    val batches = Vector(BatchCodec.intPairs(Seq((1, 10), (1, 11), (1, 12), (2, 20))))
    val memory = RankSplit.sortAndSplit(batches, partitions = 2, parallelism = 1)
    val gapped = Vector(BatchCodec.intPairs(Seq((5, 1), (1, 2))))
    val gappedMemory = RankSplit.sortAndSplit(gapped, partitions = 5, parallelism = 1)

    withConf(0L, batchSize = 1, fanIn = 2, shufflePartitions = 2) { dir =>
      val spilled =
        RankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)

      identical(spilled, memory)
      cells(spilled.lift(0).getOrElse(fail("missing partition 0"))) shouldBe Seq(
        (Some(1), Some(10)),
        (Some(1), Some(11)),
      )
      cells(spilled.lift(1).getOrElse(fail("missing partition 1"))) shouldBe Seq(
        (Some(1), Some(12)),
        (Some(2), Some(20)),
      )
      RankSplitRuns.filesWrittenForTest shouldBe 1L
      RankSplitRuns.mergePassesForTest shouldBe 0L

      // Two rows across five outputs leaves an empty partition between the two occupied ones.
      RankSplitRuns.resetForTest()
      val wide =
        RankSplitRuns.sortAndSplit(gapped, partitions = 5, parallelism = 1, ScarletConf.get)

      identical(wide, gappedMemory)
      wide.map(_.length) shouldBe Vector(1, 0, 1, 0, 0)
      RankSplitRuns.filesWrittenForTest shouldBe 1L
      RankSplitRuns.mergePassesForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "write nothing when the budget is negative" in {
    val empty = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val batches = Vector(empty, empty)
    val memory = RankSplit.sortAndSplit(batches, partitions = 3, parallelism = 1)

    withConf(-1L, batchSize = 1) { dir =>
      val spilled = RankSplitRuns.sortAndSplit(batches, partitions = 3, parallelism = 1, ScarletConf.get)

      identical(spilled, memory)
      spilled.map(_.length) shouldBe Vector(0, 0, 0)
      RankSplitRuns.filesWrittenForTest shouldBe 0L
      RankSplitRuns.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "sortByIntKey over nulls" should "spill, match the in-memory split, and decode a short prefix" in {
    val rows = Seq(
      intPair(boxedInt(1), boxedInt(10)),
      intPair(noInt, boxedInt(1)),
      intPair(boxedInt(0), boxedInt(2)),
      intPair(boxedInt(1), noInt),
      intPair(boxedInt(1), boxedInt(11)),
    )
    val source = DistCollection(rows, 5)
    val plan = source.sortByIntKey.plan

    val memoryParts = withMemory(plan, shufflePartitions = 2)
    withConf(0L, batchSize = 1, fanIn = 2, shufflePartitions = 2) { dir =>
      val spilled = sortedParts(plan)

      identical(spilled, memoryParts)
      cells(spilled.lift(0).getOrElse(fail("missing partition 0"))) shouldBe Seq(
        (None, Some(1)),
        (Some(0), Some(2)),
        (Some(1), Some(10)),
      )
      cells(spilled.lift(1).getOrElse(fail("missing partition 1"))) shouldBe Seq(
        (Some(1), None),
        (Some(1), Some(11)),
      )
      RankSplitRuns.filesWrittenForTest shouldBe 9L
      RankSplitRuns.mergePassesForTest shouldBe 3L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]

      RankSplitRuns.resetForTest()
      val off = withMemoryTake(plan, 4)
      ColumnMetrics.withSink(new CountingSink) {
        val taken = ColumnarPlanner.tryTake(plan, 4)
        taken shouldBe Some(off)
        val prefix = taken.getOrElse(fail("missing prefix"))
        prefix.exists(pair => !present(pair, 0)) shouldBe true
        prefix.exists(pair => !present(pair, 1)) shouldBe true
        ColumnMetrics.decodedValueCount shouldBe 4L
      }
      RankSplitRuns.filesWrittenForTest shouldBe 9L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a write failure" should "delete the run it already published" in {
    val batches = Vector(
      BatchCodec.intPairs(Seq((1, 1), (3, 3))),
      BatchCodec.intPairs(Seq((2, 2))),
    )

    withConf(0L, batchSize = 1, fanIn = 2) { dir =>
      RankSplitRuns.armWriteFailure()
      val caught = intercept[IllegalStateException] {
        val _ = RankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage shouldBe RankSplitRuns.WriteFailed
      RankSplitRuns.filesWrittenForTest shouldBe 1L
      Path.of(RankSplitRuns.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a merge publish failure" should "delete the runs it already published" in {
    val batches = Vector.tabulate(5)(index => BatchCodec.intPairs(Seq((index, index + 1))))

    withConf(0L, batchSize = 1, fanIn = 2) { dir =>
      // Five initial runs publish. The next publish is the first merged run.
      RankSplitRuns.armWriteFailureAfter(5)
      val caught = intercept[IllegalStateException] {
        val _ = RankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage shouldBe RankSplitRuns.WriteFailed
      RankSplitRuns.filesWrittenForTest shouldBe 6L
      RankSplitRuns.mergePassesForTest shouldBe 0L
      Path.of(RankSplitRuns.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a merge failure" should "delete the intermediate runs" in {
    val batches = Vector.tabulate(5)(index => BatchCodec.intPairs(Seq((index, index + 1))))

    withConf(0L, batchSize = 1, fanIn = 2) { dir =>
      RankSplitRuns.armPassFailure(2)
      val caught = intercept[IllegalStateException] {
        val _ = RankSplitRuns.sortAndSplit(batches, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage shouldBe RankSplitRuns.MergeFailed
      // Five initial runs plus two from the first pass. The second pass throws before it publishes.
      RankSplitRuns.filesWrittenForTest shouldBe 7L
      RankSplitRuns.mergePassesForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "bad arguments" should "be rejected before a file is written" in {
    val empty = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val rows = Vector(BatchCodec.intPairs(Seq((1, 1))))
    val missing = intercept[IllegalArgumentException] {
      val _ = RankSplitRuns.sortAndSplit(Vector.empty, 2, 1, confAt(0L, enabled = true))
    }
    val width = intercept[IllegalArgumentException] {
      val _ = RankSplitRuns.sortAndSplit(Vector(empty), 0, 1, confAt(0L, enabled = true))
    }
    val workers = intercept[IllegalArgumentException] {
      val _ = RankSplitRuns.sortAndSplit(Vector(empty), 1, 0, confAt(0L, enabled = true))
    }

    missing.getMessage should include("sortAndSplit needs at least one batch")
    width.getMessage should include("numPartitions must be positive, got 0")
    workers.getMessage should include("parallelism must be positive, got 0")

    withConf(0L, batchSize = 1, fanIn = 1) { dir =>
      val caught = intercept[IllegalArgumentException] {
        val _ = RankSplitRuns.sortAndSplit(rows, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage should include("spillFanIn must be >= 2, got 1")
      RankSplitRuns.filesWrittenForTest shouldBe 0L
      RankSplitRuns.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a store that fails to open" should "not record a directory" in {
    val rows = Vector(BatchCodec.intPairs(Seq((1, 1))))

    withConf(0L, batchSize = 1, fanIn = 2, maxFiles = 0) { dir =>
      val caught = intercept[IllegalArgumentException] {
        val _ = RankSplitRuns.sortAndSplit(rows, partitions = 2, parallelism = 1, ScarletConf.get)
      }

      caught.getMessage should include("maxFiles must be > 0")
      RankSplitRuns.lastDirectoryForTest shouldBe ""
      RankSplitRuns.filesWrittenForTest shouldBe 0L
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

  private def columnImage(batch: ColumnBatch, ordinal: Int): (Seq[Int], Seq[Boolean]) =
    val column = int32(batch, ordinal)
    val values = (0 until batch.length).map(row => column.values(row))
    val valid = (0 until batch.length).map(row => column.isValid(row))
    (values, valid)

  private def cells(batch: ColumnBatch): Seq[(Option[Int], Option[Int])] =
    val keys = int32(batch, 0)
    val values = int32(batch, 1)
    (0 until batch.length).map { row =>
      val key = if (keys.isValid(row)) then Some(keys.values(row)) else None
      val value = if (values.isValid(row)) then Some(values.values(row)) else None
      (key, value)
    }

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def sourcePairs(plan: Plan[(Int, Int)]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered source"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.Pairs(parts) => parts
      case other => fail(s"expected int pairs, got $other")

  private def sortedParts(plan: Plan[(Int, Int)]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered sort"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.Pairs(parts) => parts
      case other => fail(s"expected int pairs, got $other")

  private def withMemory(plan: Plan[(Int, Int)], shufflePartitions: Int): Vector[ColumnBatch] =
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

  private def withMemoryTake(plan: Plan[(Int, Int)], count: Int): Seq[(Int, Int)] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(spillEnabled = false))
      RankSplitRuns.resetForTest()
      val taken = ColumnarPlanner.tryTake(plan, count).getOrElse(fail("missing in-memory prefix"))
      RankSplitRuns.filesWrittenForTest shouldBe 0L
      taken
    finally
      RankSplitRuns.resetForTest()
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

  private def present(pair: (Int, Int), ordinal: Int): Boolean =
    pair.productElement(ordinal) match
      case _: java.lang.Integer => true
      case _ => false

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noInt: java.lang.Integer = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)

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
    val dir = Files.createTempDirectory("scarlet-rank-split")
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
      RankSplitRuns.resetForTest()
      deleteTree(dir)

  private def deleteTree(dir: Path): Unit =
    SpillStore.listForTest(dir).foreach { path =>
      val _ = Files.deleteIfExists(path)
    }
    val _ = Files.deleteIfExists(dir)
