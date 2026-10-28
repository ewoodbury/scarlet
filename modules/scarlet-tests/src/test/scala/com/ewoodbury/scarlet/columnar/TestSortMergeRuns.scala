package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/** Spilled sorted runs for one sort-merge partition, against stage 1 and the row path. */
class TestSortMergeRuns extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    SortMergeRuns.resetForTest()
    ()

  "a partition at the budget" should "match sort-merge in memory and write no files" in {
    val left = Seq((3, 30), (1, 10), (1, 11), (2, 20), (-4, 7), (8, 8))
    val right = Seq((1, 100), (1, 101), (0, 1), (3, 33), (-4, 9), (6, 6))
    val leftBatch = BatchCodec.intPairs(left)
    val rightBatch = BatchCodec.intPairs(right)
    val estimate =
      SortMergeRuns.retainedBytes(left.size) + SortMergeRuns.retainedBytes(right.size)

    SortMergeRuns.retainedBytes(0) shouldBe 0L
    SortMergeRuns.retainedBytes(4) shouldBe 4L * (4L + 4L + 1L) + SortMergeRuns.HeaderAllowance
    SortMergeRuns.exceeds(leftBatch, rightBatch, confAt(estimate, enabled = false)) shouldBe false
    SortMergeRuns.exceeds(leftBatch, rightBatch, confAt(estimate, enabled = true)) shouldBe false
    SortMergeRuns.exceeds(
      leftBatch,
      rightBatch,
      confAt(estimate - 1L, enabled = true),
    ) shouldBe true

    withConf(estimate, batchSize = 2) { dir =>
      val staged = triples(join(left, right, batchSize = 2))
      val written = SortMergeRuns.filesWrittenForTest

      staged shouldBe triples(memory(left, right))
      written shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a partition over the budget" should "match the row path and leave no files" in {
    val left = Seq((1, 10), (3, 30), (5, 50), (7, 70), (9, 90), (2, 20))
    val right = Seq((1, 11), (4, 40), (5, 51), (8, 80), (9, 91), (0, 1))
    val budget = SortMergeRuns.retainedBytes(1) + SortMergeRuns.retainedBytes(1)
    val partition =
      SortMergeRuns.retainedBytes(left.size) + SortMergeRuns.retainedBytes(right.size)
    partition should be > budget

    withConf(budget, batchSize = 2) { dir =>
      val spilled = triples(join(left, right, batchSize = 2))

      spilled should contain theSameElementsAs rowPairs(left, right)
      SortMergeRuns.filesWrittenForTest shouldBe 6L
      Path.of(SortMergeRuns.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a duplicate key" should "cross every run on both sides" in {
    val key = 7
    val leftCopies = 5
    val rightCopies = 4
    val left = (0 until leftCopies).map(row => key -> row) :+ (8 -> 80)
    val right = (0 until rightCopies).map(row => key -> (10 + row)) :+ (8 -> 81)
    val partition =
      SortMergeRuns.retainedBytes(left.size) + SortMergeRuns.retainedBytes(right.size)
    val group = SortMergeRuns.retainedBytes(leftCopies) + SortMergeRuns.retainedBytes(rightCopies)
    partition should be > group
    val budget = partition - 1L
    group should be <= budget

    withConf(budget, batchSize = 2) { dir =>
      val spilled = triples(join(left, right, batchSize = 2))

      spilled should contain theSameElementsAs rowPairs(left, right)
      spilled.count { case (found, _) => found == key } shouldBe leftCopies * rightCopies
      spilled.count { case (found, _) => found == 8 } shouldBe 1
      SortMergeRuns.filesWrittenForTest shouldBe 6L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "null keys" should "produce no rows from either side" in {
    val left = Seq(
      (None, Some(1)),
      (Some(0), Some(2)),
      (Some(1), None),
      (Some(2), Some(4)),
      (Some(3), Some(5)),
      (Some(4), Some(6)),
    )
    val right = Seq(
      (None, Some(9)),
      (Some(0), Some(8)),
      (Some(1), Some(7)),
      (Some(2), Some(6)),
      (Some(5), Some(5)),
      (Some(4), Some(4)),
    )
    val presentLeft = left.count(_._1.isDefined)
    val presentRight = right.count(_._1.isDefined)
    val budget = SortMergeRuns.retainedBytes(1) + SortMergeRuns.retainedBytes(1)
    val partition =
      SortMergeRuns.retainedBytes(presentLeft) + SortMergeRuns.retainedBytes(presentRight)
    partition should be > budget

    withConf(budget, batchSize = 2) { dir =>
      val spilled = optional(joinNullable(left, right, batchSize = 2))

      spilled should contain theSameElementsAs optional(Vector(memoryNullable(left, right)))
      spilled.map(_._2) should not contain Some(1)
      spilled.map(_._3) should not contain Some(9)
      spilled should contain((1, None, Some(7)))
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }

    val nullLeft = Seq((None, Some(1)), (None, Some(0)), (None, Some(4)))
    val nullRight = Seq(
      (Some(0), Some(8)),
      (Some(1), Some(7)),
      (Some(2), Some(6)),
      (Some(3), Some(5)),
    )
    val nullBudget = SortMergeRuns.retainedBytes(nullRight.size) - 1L

    withConf(nullBudget, batchSize = 2) { dir =>
      val spilled = optional(joinNullable(nullLeft, nullRight, batchSize = 2))

      spilled shouldBe Seq.empty[(Int, Option[Int], Option[Int])]
      SortMergeRuns.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "extreme keys" should "match across runs" in {
    val left = Seq((Int.MinValue, 1), (Int.MaxValue, 2), (Int.MinValue, 3), (0, 4))
    val right = Seq((Int.MaxValue, 5), (Int.MinValue, 6), (Int.MaxValue, 7), (-1, 8))
    val partition =
      SortMergeRuns.retainedBytes(left.size) + SortMergeRuns.retainedBytes(right.size)
    val group = SortMergeRuns.retainedBytes(2) + SortMergeRuns.retainedBytes(1)
    partition should be > group
    val budget = partition - 1L
    group should be <= budget

    withConf(budget, batchSize = 1) { dir =>
      val spilled = triples(join(left, right, batchSize = 1))

      spilled should contain theSameElementsAs rowPairs(left, right)
      spilled.count { case (key, _) => key == Int.MinValue } shouldBe 2
      spilled.count { case (key, _) => key == Int.MaxValue } shouldBe 2
      SortMergeRuns.filesWrittenForTest shouldBe 8L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a single group over the budget" should "throw and leave no files" in {
    val key = Int.MinValue
    val rows = 8
    val left = (0 until rows).map(row => key -> row)
    val right = (0 until rows).map(row => key -> (row + 100))
    val estimate = SortMergeRuns.retainedBytes(rows) + SortMergeRuns.retainedBytes(rows)
    val budget = estimate - 1L

    withConf(budget, batchSize = 3) { dir =>
      val caught = intercept[IllegalStateException] {
        val _ = join(left, right, batchSize = 3)
      }

      caught.getMessage shouldBe
        s"sort-merge group for key $key retains about $estimate bytes, " +
        s"above the budget of $budget bytes"
      SortMergeRuns.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a merge failure" should "delete the spill files before it returns" in {
    val left = (0 until 6).map(row => row -> (row + 1))
    val right = (0 until 6).map(row => row -> (row + 10))
    val budget =
      SortMergeRuns.retainedBytes(left.size) + SortMergeRuns.retainedBytes(right.size) - 1L
    val group = SortMergeRuns.retainedBytes(1) + SortMergeRuns.retainedBytes(1)
    group should be <= budget

    withConf(budget, batchSize = 2) { dir =>
      SortMergeRuns.armMergeFailure()
      val caught = intercept[IllegalStateException] {
        val _ = join(left, right, batchSize = 2)
      }

      caught.getMessage shouldBe SortMergeRuns.MergeFailed
      SortMergeRuns.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  private def join(
      left: Seq[(Int, Int)],
      right: Seq[(Int, Int)],
      batchSize: Int,
  ): Vector[ColumnBatch] =
    ColumnPipeline.sortMergeJoin(
      Vector(BatchCodec.intPairs(left)),
      Vector(BatchCodec.intPairs(right)),
      numPartitions = 1,
      parallelism = 1,
      batchSize = batchSize,
    )

  private def joinNullable(
      left: Seq[(Option[Int], Option[Int])],
      right: Seq[(Option[Int], Option[Int])],
      batchSize: Int,
  ): Vector[ColumnBatch] =
    ColumnPipeline.sortMergeJoin(
      Vector(BatchCodec.nullableIntPairs(left)),
      Vector(BatchCodec.nullableIntPairs(right)),
      numPartitions = 1,
      parallelism = 1,
      batchSize = batchSize,
    )

  private def memory(left: Seq[(Int, Int)], right: Seq[(Int, Int)]): Vector[ColumnBatch] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(spillEnabled = false))
      SortMergeRuns.resetForTest()
      join(left, right, batchSize = 2)
    finally
      ScarletConf.set(saved)
      SortMergeRuns.resetForTest()

  /** In-memory merge of the same pairs, including a null key and a null value. */
  private def memoryNullable(
      left: Seq[(Option[Int], Option[Int])],
      right: Seq[(Option[Int], Option[Int])],
  ): ColumnBatch =
    SortMerge.join(BatchCodec.nullableIntPairs(left), BatchCodec.nullableIntPairs(right))

  private def rowPairs(left: Seq[(Int, Int)], right: Seq[(Int, Int)]): Seq[(Int, (Int, Int))] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      DistCollection(left, 1).sortMergeJoin(DistCollection(right, 1)).collect().toSeq
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def triples(batches: Vector[ColumnBatch]): Seq[(Int, (Int, Int))] =
    batches.flatMap(batch => BatchCodec.decodeIntTriples(batch))

  private def optional(batches: Vector[ColumnBatch]): Seq[(Int, Option[Int], Option[Int])] =
    batches.flatMap { batch =>
      val keys = int32(batch, 0)
      val left = int32(batch, 1)
      val right = int32(batch, 2)
      (0 until batch.length).map { row =>
        require(keys.isValid(row), "inner join emitted a null key")
        (keys.values(row), cell(left, row), cell(right, row))
      }
    }

  private def cell(column: Int32Column, row: Int): Option[Int] =
    if (column.isValid(row)) then Some(column.values(row)) else None

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def confAt(budget: Long, enabled: Boolean): ScarletConf =
    originalConf.copy(spillEnabled = enabled, spillBudgetBytes = budget)

  private def withConf(budget: Long, batchSize: Int)(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-sort-merge")
    SortMergeRuns.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = 1,
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
      SortMergeRuns.resetForTest()
      deleteTree(dir)

  private def deleteTree(dir: Path): Unit =
    SpillStore.listForTest(dir).foreach { path =>
      val _ = Files.deleteIfExists(path)
    }
    val _ = Files.deleteIfExists(dir)
