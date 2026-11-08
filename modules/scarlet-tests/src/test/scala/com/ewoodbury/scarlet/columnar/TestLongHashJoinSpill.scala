package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.execution.ColumnarPlanner
import com.ewoodbury.scarlet.runtime.ScarletRuntime

class TestLongHashJoinSpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val width = 4
  private val half = 32
  // `Long.hashCode` of this key is -1, so it shares a parent bucket with key 3 and splits on the
  // quotient. A raw `floorMod` of the key would not.
  private val negativeHashKey = 4294967295L

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    LongHashJoinSpill.resetForTest()
    ScarletRuntime.get.shuffle.clear()

  "a skewed bucket" should "spill two long-pair pieces and match the in-memory join" in {
    val buildRows =
      rowsOf(0L, half, row => if (row == 0) then 1L << 40 else row.toLong) ++
        rowsOf(4L, half, row => 1000L + row.toLong)
    val probeRows = Seq((0L, 7L), (4L, 8L))
    val build = pairs(buildRows)
    val probe = pairs(probeRows)
    val memory = join(build, probe)
    val fromEngine = rowTriples(buildRows, probeRows)
    val budget = LongHashJoin.retainedBuildBytes(half)
    LongHashJoin.retainedBuildBytes(half * 2) should be > budget
    LongHashJoin.retainedBuildBytes(half) should be <= budget
    Math.floorMod(java.lang.Long.hashCode(0L), width) shouldBe
      Math.floorMod(java.lang.Long.hashCode(4L), width)

    withConf(budget, batchSize = 8) { dir =>
      val splits = SpillStore.splitCount(
        half * 2,
        LongHashJoin.retainedBuildBytes(half * 2),
        ScarletConf.get,
      )
      splits shouldBe 2
      val spilled = join(build, probe)

      spilled.length shouldBe width
      spilled.map(_.length) shouldBe memory.map(_.length)
      val matches = ordered(triples(spilled))
      matches shouldBe ordered(triples(memory))
      matches shouldBe ordered(fromEngine)
      matches.exists { case (_, left, _) => left.contains(1L << 40) } shouldBe true
      // Four morsels per piece share one file. A second floorMod of the hash would keep both keys
      // together.
      LongHashJoinSpill.filesWrittenForTest shouldBe 2L
      LongHashJoinSpill.openTablesForTest shouldBe 1
      Path.of(LongHashJoinSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "two hot buckets" should "finish under a file cap that cannot hold both splits" in {
    // Key 5 shares bucket 1 with key 1 and separates at two pieces. Key 9 does not:
    // floorDiv(hash(1), 4) and floorDiv(hash(9), 4) are both even, so a two-piece split keeps
    // them together. For these small keys the hash code is the key.
    val build = pairs(
      rowsOf(0L, half, _.toLong) ++
        rowsOf(4L, half, row => 1000L + row.toLong) ++
        rowsOf(1L, half, row => 2000L + row.toLong) ++
        rowsOf(5L, half, row => 3000L + row.toLong),
    )
    val probe = pairs(Seq((0L, 7L), (4L, 8L), (1L, 9L), (5L, 10L)))
    val memory = join(build, probe)
    val budget = LongHashJoin.retainedBuildBytes(half)

    withConf(budget, maxFiles = 3) { dir =>
      val spilled = join(build, probe)

      ordered(triples(spilled)) shouldBe ordered(triples(memory))
      LongHashJoinSpill.filesWrittenForTest shouldBe 4L
      LongHashJoinSpill.openTablesForTest shouldBe 1
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a bucket at the budget" should "match the in-memory join and write no files" in {
    val rows = half
    val build = pairs(rowsOf(0L, rows, _.toLong))
    val probe = pairs(Seq((0L, 7L)))
    val memory = join(build, probe)
    val budget = LongHashJoin.retainedBuildBytes(rows)
    LongHashJoin.retainedBuildBytes(rows) shouldBe budget

    withConf(budget) { dir =>
      val same = join(build, probe)

      same.map(_.length) shouldBe memory.map(_.length)
      triples(same) shouldBe triples(memory)
      LongHashJoinSpill.filesWrittenForTest shouldBe 0L
      LongHashJoinSpill.openTablesForTest shouldBe 0
      LongHashJoinSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill disabled" should "ignore a budget the build exceeds and write no files" in {
    val build = pairs(rowsOf(0L, half, _.toLong) ++ rowsOf(4L, half, row => 1000L + row.toLong))
    val probe = pairs(Seq((0L, 7L), (4L, 8L)))
    val memory = join(build, probe)
    val dir = Files.createTempDirectory("scarlet-long-hash-join-off")
    LongHashJoinSpill.resetForTest()
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
      LongHashJoinSpill.filesWrittenForTest shouldBe 0L
      LongHashJoinSpill.openTablesForTest shouldBe 0
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      LongHashJoinSpill.resetForTest()
      deleteTree(dir)
  }

  "a single hot key" should "throw the budget refusal and leave no files" in {
    val rows = 8
    val build = pairs(rowsOf(0L, rows, _.toLong))
    val probe = pairs(Seq((0L, 7L)))
    val budget = LongHashJoin.retainedBuildBytes(1)
    val estimate = LongHashJoin.retainedBuildBytes(rows)
    estimate should be > budget

    withConf(budget) { dir =>
      val caught = intercept[IllegalStateException] {
        val _ = join(build, probe)
      }

      caught.getMessage shouldBe
        s"shuffle-hash build piece retains about $estimate bytes for $rows rows, " +
        s"above the budget of $budget bytes"
      caught.getMessage should not include "broadcast"
      LongHashJoinSpill.filesWrittenForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "duplicate keys" should "emit the full cartesian product" in {
    val copies = 4
    val partners = 3
    val build = pairs(
      rowsOf(0L, copies, row => row.toLong + 1L) ++ rowsOf(4L, copies, row => row.toLong + 11L),
    )
    val probe = pairs(
      rowsOf(0L, partners, row => row.toLong + 101L) ++
        rowsOf(4L, partners, row => row.toLong + 201L),
    )
    val memory = join(build, probe)
    val budget = LongHashJoin.retainedBuildBytes(copies)
    LongHashJoin.retainedBuildBytes(copies * 2) should be > budget

    withConf(budget) { dir =>
      val spilled = join(build, probe)
      val matches = ordered(triples(spilled))

      matches shouldBe ordered(triples(memory))
      matches.count { case (key, _, _) => key == 0L } shouldBe copies * partners
      matches.count { case (key, _, _) => key == 4L } shouldBe copies * partners
      LongHashJoinSpill.filesWrittenForTest shouldBe 2L
      LongHashJoinSpill.openTablesForTest shouldBe 1
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "null keys" should "be dropped and never match key 0" in {
    val buildRows =
      (0 until half).map(row => (Some(0L), Some(row.toLong))) ++
        (0 until half).map(row => (Some(4L), Some(1000L + row.toLong))) ++
        Seq((None, Some(777L)), (None, Some(0L)))
    val probeRows = Seq(
      (Some(0L), Some(7L)),
      (Some(0L), Some(9L)),
      (Some(4L), Some(8L)),
      (None, Some(888L)),
    )
    val build = nullablePairs(buildRows)
    val probe = nullablePairs(probeRows)
    val memory = join(build, probe)
    val budget = LongHashJoin.retainedBuildBytes(half)
    val expected =
      (0 until half).flatMap { row =>
        Seq((0L, Some(row.toLong), Some(7L)), (0L, Some(row.toLong), Some(9L)))
      } ++ (0 until half).map { row =>
        (4L, Some(1000L + row.toLong), Some(8L))
      }

    withConf(budget) { dir =>
      val spilled = join(build, probe)
      val matches = ordered(triples(spilled))

      matches shouldBe ordered(expected)
      matches shouldBe ordered(triples(memory))
      matches.map { case (_, left, _) => left } should not contain Some(777L)
      matches.map { case (_, _, right) => right } should not contain Some(888L)
      LongHashJoinSpill.filesWrittenForTest shouldBe 2L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a null value" should "stay on the row path and write no spill file" in {
    val present =
      rowsOf(0L, half, _.toLong) ++ rowsOf(4L, half, row => 1000L + row.toLong)
    val budget = LongHashJoin.retainedBuildBytes(half)
    LongHashJoin.retainedBuildBytes(present.length + 1) should be > budget

    withConf(budget) { dir =>
      val left = DistCollection(
        present.map { case (key, value) => pair(longRef(key), longRef(value)) } :+
          pair(longRef(0L), absent),
        width,
      )
      val right = DistCollection(
        Seq(pair(longRef(0L), longRef(7L)), pair(longRef(4L), longRef(8L))),
        width,
      )
      val joined = left.join(right)
      ColumnarPlanner.lower(joined.plan).map(_.shape) shouldBe Some(
        "LongHashJoin(Exchange(Scan), Exchange(Scan))",
      )
      ColumnarPlanner.tryRun(joined.plan).isEmpty shouldBe true
      val collected = joined.collect().toList
      collected should contain theSameElementsAs rowOf(joined)
      collected should contain ((0L, pair(absent, longRef(7L))))
      LongHashJoinSpill.filesWrittenForTest shouldBe 0L
      LongHashJoinSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty probe bucket" should "write no file when its build is over budget" in {
    val build = pairs(rowsOf(0L, half * 2, _.toLong))
    val probe = pairs(Seq((1L, 7L)))
    val memory = join(build, probe)
    val budget = LongHashJoin.retainedBuildBytes(half)
    LongHashJoin.retainedBuildBytes(half * 2) should be > budget

    withConf(budget) { dir =>
      val spilled = join(build, probe)

      spilled.length shouldBe width
      spilled.map(_.length) shouldBe Seq(0, 0, 0, 0)
      triples(spilled) shouldBe triples(memory)
      LongHashJoinSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "negative hash codes" should "split on the parent quotient" in {
    java.lang.Long.hashCode(negativeHashKey) shouldBe -1
    java.lang.Long.hashCode(3L) shouldBe 3
    Math.floorMod(-1, width) shouldBe Math.floorMod(3, width)
    val negativePiece = Math.floorMod(Math.floorDiv(-1, width), 2)
    val otherPiece = Math.floorMod(Math.floorDiv(3, width), 2)
    negativePiece should not equal otherPiece

    val build = pairs(
      rowsOf(negativeHashKey, half, _.toLong) ++ rowsOf(3L, half, row => 1000L + row.toLong),
    )
    val probe = pairs(Seq((negativeHashKey, 7L), (3L, 8L)))
    val memory = join(build, probe)
    val budget = LongHashJoin.retainedBuildBytes(half)

    withConf(budget) { dir =>
      val spilled = join(build, probe)

      spilled.map(_.length) shouldBe memory.map(_.length)
      ordered(triples(spilled)) shouldBe ordered(triples(memory))
      LongHashJoinSpill.filesWrittenForTest shouldBe 2L
      LongHashJoinSpill.openTablesForTest shouldBe 1
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a probe failure" should "delete the spill files before it returns" in {
    val build = pairs(
      rowsOf(0L, half, _.toLong) ++
        rowsOf(4L, half, row => 1000L + row.toLong) ++
        rowsOf(1L, half, row => 2000L + row.toLong) ++
        rowsOf(5L, half, row => 3000L + row.toLong),
    )
    val probe = pairs(Seq((0L, 7L), (4L, 8L), (1L, 9L), (5L, 10L)))
    val budget = LongHashJoin.retainedBuildBytes(half)

    withConf(budget) { dir =>
      LongHashJoinSpill.armProbeFailure()
      val caught = intercept[IllegalStateException] {
        val _ = join(build, probe)
      }

      caught.getMessage shouldBe LongHashJoinSpill.ProbeFailed
      LongHashJoinSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  private def pairs(rows: Seq[(Long, Long)]): Vector[ColumnBatch] =
    Vector(BatchCodec.longPairs(rows))

  private def nullablePairs(rows: Seq[(Option[Long], Option[Long])]): Vector[ColumnBatch] =
    Vector(
      new ColumnBatch(
        Vector(LogicalType.Int64, LogicalType.Int64),
        Vector(
          Int64Column.fromNullable(rows.map(_._1)),
          Int64Column.fromNullable(rows.map(_._2)),
        ),
        rows.size,
      ),
    )

  private def rowsOf(key: Long, count: Int, valueAt: Int => Long): Seq[(Long, Long)] =
    Seq.tabulate(count)(row => (key, valueAt(row)))

  private def join(build: Vector[ColumnBatch], probe: Vector[ColumnBatch]): Vector[ColumnBatch] =
    ColumnPipeline.longInnerJoin(
      build,
      probe,
      numPartitions = width,
      parallelism = 2,
      batchSize = ScarletConf.get.columnarBatchSize,
    )

  private def ordered(
      rows: Seq[(Long, Option[Long], Option[Long])],
  ): Seq[(Long, Option[Long], Option[Long])] =
    rows.sortBy { case (key, left, right) => (key, left, right) }

  private def triples(batches: Vector[ColumnBatch]): Seq[(Long, Option[Long], Option[Long])] =
    batches.flatMap { batch =>
      val keys = int64(batch, 0)
      val left = int64(batch, 1)
      val right = int64(batch, 2)
      (0 until batch.length).map { row =>
        require(keys.isValid(row), "inner join emitted a null key")
        (keys.values(row), cell(left, row), cell(right, row))
      }
    }

  private def cell(column: Int64Column, row: Int): Option[Long] =
    if (column.isValid(row)) then Some(column.values(row)) else None

  private def int64(batch: ColumnBatch, ordinal: Int): Int64Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int64Column) => column
      case other => fail(s"expected Int64 at $ordinal, got $other")

  private def rowTriples(
      build: Seq[(Long, Long)],
      probe: Seq[(Long, Long)],
  ): Seq[(Long, Option[Long], Option[Long])] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      DistCollection(build, width)
        .join(DistCollection(probe, width))
        .collect()
        .map { case (key, (left, right)) => (key, Some(left), Some(right)) }
        .toSeq
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def rowOf[A](joined: DistCollection[A]): List[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      joined.collect().toList
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def pair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absent: java.lang.Long = null

  private def longRef(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  private def withConf(
      budget: Long,
      batchSize: Int = originalConf.columnarBatchSize,
      maxFiles: Int = SpillStore.DefaultMaxFiles,
  )(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-long-hash-join-spill")
    LongHashJoinSpill.resetForTest()
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
      LongHashJoinSpill.resetForTest()
      deleteTree(dir)

  private def deleteTree(dir: Path): Unit =
    SpillStore.listForTest(dir).foreach { path =>
      val _ = Files.deleteIfExists(path)
    }
    val _ = Files.deleteIfExists(dir)
