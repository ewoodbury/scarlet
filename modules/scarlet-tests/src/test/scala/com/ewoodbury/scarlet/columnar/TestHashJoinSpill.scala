package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.columnar.ColumnPipeline.JoinInput
import com.ewoodbury.scarlet.core.ScarletConf

class TestHashJoinSpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val width = 4
  private val half = 32

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    HashJoinSpill.resetForTest()

  "a skewed bucket" should "spill two pieces and match the in-memory join" in {
    val build = pairs(rowsOf(0, half, identity) ++ rowsOf(4, half, row => 1000 + row))
    val probe = pairs(Seq((0, 7), (4, 8)))
    val memory = join(build, probe)
    val budget = HashJoin.retainedBuildBytes(half)
    HashJoin.retainedBuildBytes(half * 2) should be > budget
    HashJoin.retainedBuildBytes(half) should be <= budget

    withConf(budget, batchSize = 8) { dir =>
      SpillStore.splitCount(half * 2, HashJoin.retainedBuildBytes(half * 2), ScarletConf.get) shouldBe 2
      val spilled = join(build, probe)

      spilled.length shouldBe width
      spilled.map(_.length) shouldBe memory.map(_.length)
      ordered(triples(spilled)) shouldBe ordered(triples(memory))
      // Four morsels per piece share one file. A second floorMod would keep both keys together.
      HashJoinSpill.filesWrittenForTest shouldBe 2L
      HashJoinSpill.openTablesForTest shouldBe 1
      Path.of(HashJoinSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "two hot buckets" should "finish under a file cap that cannot hold both splits" in {
    // Key 5 shares bucket 1 with key 1 and separates at two pieces. Key 9 does not:
    // floorDiv(1, 4) and floorDiv(9, 4) are both even, so a two-piece split keeps them together.
    val build = pairs(
      rowsOf(0, half, identity) ++
        rowsOf(4, half, row => 1000 + row) ++
        rowsOf(1, half, row => 2000 + row) ++
        rowsOf(5, half, row => 3000 + row),
    )
    val probe = pairs(Seq((0, 7), (4, 8), (1, 9), (5, 10)))
    val memory = join(build, probe)
    val budget = HashJoin.retainedBuildBytes(half)

    withConf(budget, maxFiles = 3) { dir =>
      val spilled = join(build, probe)

      ordered(triples(spilled)) shouldBe ordered(triples(memory))
      HashJoinSpill.filesWrittenForTest shouldBe 4L
      HashJoinSpill.openTablesForTest shouldBe 1
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a bucket just under the budget" should "match the in-memory join and write no files" in {
    val rows = half - 1
    val build = pairs(rowsOf(0, rows, identity))
    val probe = pairs(Seq((0, 7)))
    val memory = join(build, probe)
    val budget = HashJoin.retainedBuildBytes(half)
    HashJoin.retainedBuildBytes(rows) should be < budget

    withConf(budget) { dir =>
      val same = join(build, probe)

      same.map(_.length) shouldBe memory.map(_.length)
      triples(same) shouldBe triples(memory)
      HashJoinSpill.filesWrittenForTest shouldBe 0L
      HashJoinSpill.openTablesForTest shouldBe 0
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill disabled" should "ignore a budget the build exceeds and write no files" in {
    val build = pairs(rowsOf(0, half, identity) ++ rowsOf(4, half, row => 1000 + row))
    val probe = pairs(Seq((0, 7), (4, 8)))
    val memory = join(build, probe)
    val dir = Files.createTempDirectory("scarlet-hash-join-off")
    HashJoinSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = false,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
      ),
    )
    try
      val same = join(build, probe)

      triples(same) shouldBe triples(memory)
      HashJoinSpill.filesWrittenForTest shouldBe 0L
      HashJoinSpill.openTablesForTest shouldBe 0
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      HashJoinSpill.resetForTest()
      deleteTree(dir)
  }

  "a single hot key" should "throw the budget refusal and leave no files" in {
    val rows = 8
    val build = pairs(rowsOf(0, rows, identity))
    val probe = pairs(Seq((0, 7)))
    val budget = HashJoin.retainedBuildBytes(1)
    val estimate = HashJoin.retainedBuildBytes(rows)
    estimate should be > budget

    withConf(budget) { dir =>
      val caught = intercept[IllegalStateException] {
        val _ = join(build, probe)
      }

      caught.getMessage shouldBe
        s"shuffle-hash build piece retains about $estimate bytes for $rows rows, " +
        s"above the budget of $budget bytes"
      caught.getMessage should not include "broadcast"
      HashJoinSpill.filesWrittenForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "duplicate keys" should "emit the full cartesian product" in {
    val copies = 4
    val partners = 3
    val build = pairs(rowsOf(0, copies, row => row + 1) ++ rowsOf(4, copies, row => row + 11))
    val probe = pairs(rowsOf(0, partners, row => row + 101) ++ rowsOf(4, partners, row => row + 201))
    val memory = join(build, probe)
    val budget = HashJoin.retainedBuildBytes(copies)
    HashJoin.retainedBuildBytes(copies * 2) should be > budget

    withConf(budget) { dir =>
      val spilled = join(build, probe)
      val matches = ordered(triples(spilled))

      matches shouldBe ordered(triples(memory))
      matches.count { case (key, _, _) => key == 0 } shouldBe copies * partners
      matches.count { case (key, _, _) => key == 4 } shouldBe copies * partners
      HashJoinSpill.filesWrittenForTest shouldBe 2L
      HashJoinSpill.openTablesForTest shouldBe 1
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "null keys" should "be dropped and never match key 0" in {
    val buildRows =
      (0 until half).map(row => (Some(0), Some(row))) ++
        (0 until half).map(row => (Some(4), Some(1000 + row))) ++
        Seq((None, Some(777)), (None, Some(0)))
    val probeRows = Seq(
      (Some(0), Some(7)),
      (Some(0), Some(9)),
      (Some(4), Some(8)),
      (None, Some(888)),
    )
    val build = Vector(BatchCodec.nullableIntPairs(buildRows))
    val probe = Vector(BatchCodec.nullableIntPairs(probeRows))
    val memory = join(build, probe)
    val budget = HashJoin.retainedBuildBytes(half)
    val expected =
      (0 until half).flatMap { row =>
        Seq((0, Some(row), Some(7)), (0, Some(row), Some(9)))
      } ++ (0 until half).map { row =>
        (4, Some(1000 + row), Some(8))
      }

    withConf(budget) { dir =>
      val spilled = join(build, probe)
      val matches = ordered(triples(spilled))

      matches shouldBe ordered(expected)
      matches shouldBe ordered(triples(memory))
      matches.map { case (_, left, _) => left } should not contain Some(777)
      matches.map { case (_, _, right) => right } should not contain Some(888)
      HashJoinSpill.filesWrittenForTest shouldBe 2L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty probe bucket" should "write no file when its build is over budget" in {
    val build = pairs(rowsOf(0, half * 2, identity))
    val probe = pairs(Seq((1, 7)))
    val memory = join(build, probe)
    val budget = HashJoin.retainedBuildBytes(half)
    HashJoin.retainedBuildBytes(half * 2) should be > budget

    withConf(budget) { dir =>
      val spilled = join(build, probe)

      spilled.length shouldBe width
      spilled.map(_.length) shouldBe Seq(0, 0, 0, 0)
      triples(spilled) shouldBe triples(memory)
      HashJoinSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a selected build" should "spill the hot bucket and match the row oracle" in {
    val keys = Array.concat(Array.fill(half)(0), Array.fill(half)(4), Array(0, 99))
    val values = Array.concat(
      Array.tabulate(half)(identity),
      Array.tabulate(half)(row => 1000 + row),
      Array(-1, 5),
    )
    val batch = BatchCodec.intPairs(keys, values)
    val selected = Selected(batch, Array.tabulate(half * 2)(identity), half * 2)
    val probe = BatchCodec.intPairs(Seq((0, 50), (4, 60), (99, 6)))
    val budget = HashJoin.retainedBuildBytes(half)
    val expected =
      (0 until half).map(row => (0, Some(row), Some(50))) ++
        (0 until half).map(row => (4, Some(1000 + row), Some(60)))

    withConf(budget) { dir =>
      val spilled = ColumnPipeline.innerJoin(
        JoinInput.Selected(Vector(selected)),
        JoinInput.Dense(Vector(probe)),
        numPartitions = width,
        parallelism = 2,
      )

      spilled.length shouldBe width
      ordered(triples(spilled)) shouldBe ordered(expected)
      HashJoinSpill.filesWrittenForTest shouldBe 2L
      HashJoinSpill.openTablesForTest shouldBe 1
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "negative keys" should "split on the parent quotient" in {
    val build = pairs(rowsOf(-1, half, identity) ++ rowsOf(3, half, row => 1000 + row))
    val probe = pairs(Seq((-1, 7), (3, 8)))
    val memory = join(build, probe)
    val budget = HashJoin.retainedBuildBytes(half)

    withConf(budget) { dir =>
      val spilled = join(build, probe)

      spilled.map(_.length) shouldBe memory.map(_.length)
      ordered(triples(spilled)) shouldBe ordered(triples(memory))
      HashJoinSpill.filesWrittenForTest shouldBe 2L
      HashJoinSpill.openTablesForTest shouldBe 1
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a probe failure" should "delete the spill files before it returns" in {
    val build = pairs(
      rowsOf(0, half, identity) ++
        rowsOf(4, half, row => 1000 + row) ++
        rowsOf(1, half, row => 2000 + row) ++
        rowsOf(5, half, row => 3000 + row),
    )
    val probe = pairs(Seq((0, 7), (4, 8), (1, 9), (5, 10)))
    val budget = HashJoin.retainedBuildBytes(half)

    withConf(budget) { dir =>
      HashJoinSpill.armProbeFailure()
      val caught = intercept[IllegalStateException] {
        val _ = join(build, probe)
      }

      caught.getMessage shouldBe HashJoinSpill.ProbeFailed
      HashJoinSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  private def pairs(rows: Seq[(Int, Int)]): Vector[ColumnBatch] =
    Vector(BatchCodec.intPairs(rows))

  private def rowsOf(key: Int, count: Int, valueAt: Int => Int): Seq[(Int, Int)] =
    Seq.tabulate(count)(row => (key, valueAt(row)))

  private def join(build: Vector[ColumnBatch], probe: Vector[ColumnBatch]): Vector[ColumnBatch] =
    ColumnPipeline.innerJoin(
      build,
      probe,
      numPartitions = width,
      parallelism = 2,
      batchSize = ScarletConf.get.columnarBatchSize,
    )

  private def ordered(
      rows: Seq[(Int, Option[Int], Option[Int])],
  ): Seq[(Int, Option[Int], Option[Int])] =
    rows.sortBy { case (key, left, right) => (key, left, right) }

  private def triples(batches: Vector[ColumnBatch]): Seq[(Int, Option[Int], Option[Int])] =
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

  private def withConf(
      budget: Long,
      batchSize: Int = originalConf.columnarBatchSize,
      maxFiles: Int = SpillStore.DefaultMaxFiles,
  )(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-hash-join-spill")
    HashJoinSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = true,
        spillBudgetBytes = budget,
        spillDirectory = dir.toString,
        spillMaxFiles = maxFiles,
        spillMaxBytes = SpillStore.DefaultMaxBytes,
        columnarBatchSize = batchSize,
      ),
    )
    try body(dir)
    finally
      ScarletConf.set(originalConf)
      HashJoinSpill.resetForTest()
      deleteTree(dir)

  private def deleteTree(dir: Path): Unit =
    SpillStore.listForTest(dir).foreach { path =>
      val _ = Files.deleteIfExists(path)
    }
    val _ = Files.deleteIfExists(dir)
