package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.ScarletConf

class TestJoinSpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    JoinSpill.resetForTest()

  "a build at the budget" should "match the in-memory join and write no files" in {
    val build = Vector(BatchCodec.intPairs(Seq((1, 10), (2, 20))))
    val probe = Vector(
      BatchCodec.intPairs(Seq((1, 100), (2, 200))),
      BatchCodec.intPairs(Seq((1, 101))),
    )
    val memory = join(probe, build)
    val budget = HashJoin.retainedBuildBytes(buildRows(build))

    withConf(budget) { dir =>
      val same = join(probe, build)

      same.map(_.length) shouldBe memory.map(_.length)
      triples(same) shouldBe triples(memory)
      JoinSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill disabled" should "ignore a budget the build exceeds and write no files" in {
    val build = Vector(BatchCodec.intPairs(Seq((1, 10), (2, 20), (3, 30))))
    val probe = Vector(BatchCodec.intPairs(Seq((1, 100), (3, 300))))
    val memory = join(probe, build)
    val dir = Files.createTempDirectory("scarlet-join-off")
    JoinSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = false,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
      ),
    )
    try
      val same = join(probe, build)

      triples(same) shouldBe triples(memory)
      JoinSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      JoinSpill.resetForTest()
      deleteTree(dir)
  }

  "a build over the budget" should "match the in-memory join and leave no files" in {
    val build = Vector(
      BatchCodec.nullableIntPairs(
        Seq((Some(0), Some(10)), (Some(1), Some(11)), (Some(1), None)),
      ),
      BatchCodec.nullableIntPairs(Seq((Some(2), Some(12)), (Some(3), Some(13)))),
    )
    val probe = Vector(
      BatchCodec.intPairs(Seq((0, 100), (1, 101))),
      BatchCodec.intPairs(Seq((2, 200), (3, 201), (1, 102))),
    )
    val memory = join(probe, build)
    val rows = buildRows(build)
    val budget = HashJoin.retainedBuildBytes(rows) - 1L
    // Two partitions, and the heavier one holds three of the five rows. That table still fits.
    HashJoin.retainedBuildBytes(3) should be <= budget

    withConf(budget, batchSize = 1, broadcastBudget = 1L) { dir =>
      val spilled = join(probe, build)

      triples(spilled).sorted shouldBe triples(memory).sorted
      triples(spilled).count { case (key, _, _) => key == 1 } shouldBe 4
      // Five build rows, two partitions, one file each. Morsels share a file.
      JoinSpill.filesWrittenForTest shouldBe 2L
      Path.of(JoinSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "negative keys" should "keep the cartesian product of one partition" in {
    val build = Vector(
      BatchCodec.intPairs(Seq((Int.MinValue, 1), (Int.MinValue, 2), (-1, 4), (-1, 5))),
    )
    val probe = Vector(
      BatchCodec.intPairs(Seq((Int.MinValue, 10), (Int.MinValue, 11), (Int.MinValue, 12))),
      BatchCodec.intPairs(Seq((-1, 20), (-1, 21))),
    )
    val memory = join(probe, build)
    val budget = HashJoin.retainedBuildBytes(buildRows(build)) - 1L
    HashJoin.retainedBuildBytes(2) should be <= budget

    withConf(budget, batchSize = 1, broadcastBudget = 1L) { dir =>
      val spilled = join(probe, build)
      val matches = triples(spilled).sorted

      matches shouldBe triples(memory).sorted
      matches.count { case (key, _, _) => key == Int.MinValue } shouldBe 6
      matches.count { case (key, _, _) => key == -1 } shouldBe 4
      JoinSpill.filesWrittenForTest shouldBe 2L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "null keys" should "produce no rows from either side" in {
    val build = Vector(
      BatchCodec.nullableIntPairs(
        Seq(
          (None, Some(1)),
          (Some(1), Some(8)),
          (Some(2), Some(9)),
          (Some(3), Some(10)),
          (Some(4), Some(11)),
        ),
      ),
    )
    val probe = Vector(
      BatchCodec.nullableIntPairs(
        Seq(
          (None, Some(99)),
          (Some(0), Some(50)),
          (Some(1), Some(101)),
          (Some(2), Some(102)),
          (Some(3), Some(103)),
          (Some(4), Some(104)),
        ),
      ),
    )
    val memory = join(probe, build)
    val budget = HashJoin.retainedBuildBytes(buildRows(build)) - 1L
    HashJoin.retainedBuildBytes(2) should be <= budget

    withConf(budget, broadcastBudget = 1L) { dir =>
      val spilled = join(probe, build)
      val matches = triples(spilled)

      matches.sorted shouldBe triples(memory).sorted
      matches.map { case (key, _, _) => key } should not contain 0
      JoinSpill.filesWrittenForTest shouldBe 2L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }

    val nullBuild = Vector(
      BatchCodec.nullableIntPairs(Seq((None, Some(1)), (None, Some(2)))),
    )
    val nullProbe = Vector(
      BatchCodec.nullableIntPairs(Seq((None, Some(9)), (Some(0), Some(4)))),
    )
    val nullBudget = HashJoin.retainedBuildBytes(buildRows(nullBuild)) - 1L

    withConf(nullBudget, broadcastBudget = 1L) { dir =>
      val spilled = join(nullProbe, nullBuild)

      triples(spilled) shouldBe empty
      Path.of(JoinSpill.lastDirectoryForTest) shouldBe dir
      JoinSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a single hot key" should "throw the budget refusal and leave no files" in {
    val build = Vector(
      BatchCodec.intPairs(Seq((Int.MinValue, 1), (Int.MinValue, 2))),
      BatchCodec.intPairs(Seq((Int.MinValue, 3), (Int.MinValue, 4))),
    )
    val probe = Vector(BatchCodec.intPairs(Seq((Int.MinValue, 10), (Int.MinValue, 11))))
    val rows = buildRows(build)
    val budget = HashJoin.retainedBuildBytes(1)
    val estimate = HashJoin.retainedBuildBytes(rows)
    estimate should be > budget

    withConf(budget, batchSize = 1, broadcastBudget = 1L) { dir =>
      val caught = intercept[IllegalStateException] {
        val _ = join(probe, build)
      }

      caught.getMessage shouldBe
        s"broadcast build table retains about $estimate bytes for $rows rows, " +
        s"above the budget of $budget bytes; one partition still exceeds the budget"
      JoinSpill.filesWrittenForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a probe failure" should "delete the spill files before it returns" in {
    val build = Vector(
      BatchCodec.intPairs(Seq((0, 10), (1, 11), (2, 12), (3, 13))),
    )
    val probe = Vector(BatchCodec.intPairs(Seq((0, 100), (1, 101), (2, 102), (3, 103))))
    val budget = HashJoin.retainedBuildBytes(buildRows(build)) - 1L
    HashJoin.retainedBuildBytes(2) should be <= budget

    withConf(budget, broadcastBudget = 1L) { dir =>
      JoinSpill.armProbeFailure()
      val caught = intercept[IllegalStateException] {
        val _ = join(probe, build)
      }

      caught.getMessage shouldBe JoinSpill.ProbeFailed
      JoinSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  private def join(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
  ): Vector[ColumnBatch] =
    ColumnPipeline.broadcastJoin(probe, build, parallelism = 2, buildBudgetBytes = broadcastBudget)

  private def broadcastBudget: Long = ScarletConf.get.broadcastBuildBudgetBytes

  private def buildRows(build: Vector[ColumnBatch]): Int =
    val rows = build.foldLeft(0L)((total, batch) => total + batch.length.toLong)
    require(rows <= Int.MaxValue.toLong, s"build side has $rows rows")
    rows.toInt

  private def triples(batches: Vector[ColumnBatch]): Seq[(Int, Option[Int], Option[Int])] =
    batches.flatMap { batch =>
      val keys = int32(batch, 0)
      val probe = int32(batch, 1)
      val build = int32(batch, 2)
      (0 until batch.length).map { row =>
        require(keys.isValid(row), "inner join emitted a null key")
        (keys.values(row), cell(probe, row), cell(build, row))
      }
    }

  private def cell(column: Int32Column, row: Int): Option[Int] =
    if (column.isValid(row)) then Some(column.values(row)) else None

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def withConf(
      budget: Long,
      batchSize: Int = originalConf.columnarBatchSize,
      broadcastBudget: Long = originalConf.broadcastBuildBudgetBytes,
  )(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-join-spill")
    JoinSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = true,
        spillBudgetBytes = budget,
        spillDirectory = dir.toString,
        spillMaxFiles = SpillStore.DefaultMaxFiles,
        spillMaxBytes = SpillStore.DefaultMaxBytes,
        columnarBatchSize = batchSize,
        broadcastBuildBudgetBytes = broadcastBudget,
      ),
    )
    try body(dir)
    finally
      ScarletConf.set(originalConf)
      JoinSpill.resetForTest()
      deleteTree(dir)

  private def deleteTree(dir: Path): Unit =
    SpillStore.listForTest(dir).foreach { path =>
      val _ = Files.deleteIfExists(path)
    }
    val _ = Files.deleteIfExists(dir)
