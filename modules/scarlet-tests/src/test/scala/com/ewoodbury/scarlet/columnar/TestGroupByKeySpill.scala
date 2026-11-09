package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.execution.{ColumnarPlanner, ExecutionModuleSupport}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Int `groupByKey` spill. Groups are compared with the in-memory kernel. Spill off matches that
 * kernel and writes nothing. Spill on slices an over-budget bucket in input order.
 */
class TestGroupByKeySpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val morselRows = 4
  private val spillingRows = 9

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    GroupByKeySpill.resetForTest()

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ScarletRuntime.get.shuffle.clear()
    GroupByKeySpill.resetForTest()
    super.afterEach()

  "spill off" should "match the in-memory groups at a tiny budget and write no file" in {
    val batch = distinct(spillingRows)
    val memory = rowsOf(inMemory(Vector(batch)))
    withConf(enabled = false, budget = 1L) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memory
      GroupByKeySpill.filesWrittenForTest shouldBe 0L
      GroupByKeySpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "publish one empty batch per partition at budget 0 and write nothing" in {
    List(false, true).foreach { enabled =>
      withConf(enabled, budget = 0L) { dir =>
        val missing = GroupByKey.int32(Vector.empty, partitions = 4, parallelism = 2)
        val emptyBatch = BatchCodec.intPairs(Seq.empty[(Int, Int)])
        val present = GroupByKey.int32(Vector(emptyBatch), partitions = 3, parallelism = 1)
        missing.length shouldBe 4
        present.length shouldBe 3
        (missing ++ present).foreach { grouped =>
          grouped.groups shouldBe 0
          grouped.offsets.toSeq shouldBe Seq(0)
        }
        GroupByKeySpill.filesWrittenForTest shouldBe 0L
        SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      }
    }
  }

  "a bucket at the budget" should "write nothing" in {
    val rows = 8
    val batch = distinct(rows)
    val memoryRows = rowsOf(inMemory(Vector(batch)))
    GroupByKeySpill.inputRowBytes(rows.toLong) shouldBe GroupByKeySpill.inputRowBytes(
      morselRows.toLong,
    )
    withConf(enabled = true, budget = GroupByKeySpill.inputRowBytes(rows.toLong)) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memoryRows
      GroupByKeySpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "distinct keys" should "spill over the budget and keep first-seen order" in {
    val batch = distinct(spillingRows)
    val memoryRows = rowsOf(inMemory(Vector(batch)))
    memoryRows.map(_._1) shouldBe (0 until spillingRows)
    withConf(enabled = true, budget = morselBudget) { dir =>
      val spilled = group(Vector(batch))
      rowsOf(spilled) shouldBe memoryRows
      spilled.headOption.getOrElse(fail("missing partition")).valuesFor(0) shouldBe Seq(0)
      GroupByKeySpill.filesWrittenForTest should be > 1L
      Path.of(GroupByKeySpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "one hot key" should "append values in input order across morsels" in {
    val values = 0 until spillingRows
    val batch = BatchCodec.intPairs(values.map(value => (7, value)))
    val memoryRows = rowsOf(inMemory(Vector(batch)))
    memoryRows shouldBe Seq((7, values))
    withConf(enabled = true, budget = morselBudget) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memoryRows
      GroupByKeySpill.filesWrittenForTest should be > 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill on at budget 1" should "refuse a morsel and leave no file" in {
    val batch = BatchCodec.intPairs(Seq(4 -> 1))
    withConf(enabled = true, budget = 1L, batchSize = 8) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy group(Vector(batch))
      refused.rows shouldBe 1L
      refused.bytes shouldBe GroupByKeySpill.inputRowBytes(1L)
      refused.bytes should be > refused.budget
      refused.budget shouldBe 1L
      GroupByKeySpill.filesWrittenForTest shouldBe 0L
      Path.of(GroupByKeySpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a null key" should "be dropped when the bucket spills" in {
    val keys = Int32Column.of(
      Array(1, 2, 1, 3, 4, 5, 6, 7, 8, 1),
      Validity.pack(10, row => row != 1),
      10,
    )
    val values = Int32Column.of(Array(10, 99, 11, 30, 40, 50, 60, 70, 80, 12), Validity.allValid(10), 10)
    val batch = new ColumnBatch(Vector(LogicalType.Int32, LogicalType.Int32), Vector(keys, values), 10)
    val memoryRows = rowsOf(inMemory(Vector(batch)))
    memoryRows.flatMap(_._2) should not contain 99
    withConf(enabled = true, budget = morselBudget) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memoryRows
      GroupByKeySpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a clear value bit" should "throw before a file is opened" in {
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(Array(1, 0), Validity.pack(2, row => row != 1), 2),
        Int32Column.of(Array(10, 0), Validity.pack(2, row => row != 1), 2),
      ),
      2,
    )
    withConf(enabled = true, budget = 1L) { dir =>
      val refused = the[IllegalArgumentException] thrownBy group(Vector(batch))
      refused.getMessage should include("clear value bit")
      GroupByKeySpill.filesWrittenForTest shouldBe 0L
      GroupByKeySpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "empty buckets" should "stay empty when another bucket spills" in {
    val values = 0 until spillingRows
    val batch = BatchCodec.intPairs(values.map(value => (Int.MinValue, value)))
    Math.floorMod(Int.MinValue, 4) shouldBe 0
    withConf(enabled = true, budget = morselBudget) { dir =>
      val parts = GroupByKey.int32(Vector(batch), partitions = 4, parallelism = 2)
      parts.length shouldBe 4
      rowsOf(Vector(parts(0))) shouldBe Seq((Int.MinValue, values))
      parts.tail.foreach(_.groups shouldBe 0)
      GroupByKeySpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a survivor index" should "spill kept rows and omit dropped rows" in {
    val pairs = (0 until spillingRows).flatMap(key => Seq((key, key), (99, -1)))
    val batch = BatchCodec.intPairs(pairs)
    val selected = Selected(batch, Array.tabulate(spillingRows)(key => key * 2), spillingRows)
    val memoryRows = rowsOf(memorySelected(Vector(selected)))
    memoryRows.map(_._1) should not contain 99
    withConf(enabled = true, budget = morselBudget) { dir =>
      val spilled = GroupByKey.int32Selected(Vector(selected), partitions = 1, parallelism = 1)
      rowsOf(spilled) shouldBe memoryRows
      GroupByKeySpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "groupByKey" should "match the row path through a spilled collect and keep its shape" in {
    val keys = Array.tabulate(spillingRows)(identity)
    val values = Array.tabulate(spillingRows)(key => key * 2)
    withConf(enabled = true, budget = morselBudget, partitions = 1) { dir =>
      val source = DistCollection.ofIntPairs(keys, values, 1)
      val grouped = source.groupByKey
      val (found, batches) = ColumnMetrics.withSink(new CountingSink) {
        val collected = valuesByKey(grouped.collect())
        collected -> ColumnMetrics.batchCount
      }
      ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
      batches should be > 0L
      found shouldBe valuesByKey(row(grouped.collect()))
      found shouldBe (0 until spillingRows).map(key => key -> Seq(key * 2)).toMap
      GroupByKeySpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  private def morselBudget: Long = GroupByKeySpill.inputRowBytes(morselRows.toLong)

  private def wideBudget(rows: Int): Long = GroupByKeySpill.inputRowBytes(rows.toLong)

  private def distinct(rows: Int): ColumnBatch =
    BatchCodec.intPairs((0 until rows).map(key => (key, key)))

  private def inMemory(batches: Vector[ColumnBatch]): Vector[GroupedIntBatch] =
    withConf(enabled = false, budget = wideBudget(64)) { _ =>
      group(batches)
    }

  private def memorySelected(selected: Vector[Selected]): Vector[GroupedIntBatch] =
    withConf(enabled = false, budget = wideBudget(64)) { _ =>
      GroupByKey.int32Selected(selected, partitions = 1, parallelism = 1)
    }

  /** Uses the configuration the caller installed. Does not open its own store directory. */
  private def group(batches: Vector[ColumnBatch]): Vector[GroupedIntBatch] =
    val result = GroupByKey.int32(batches, partitions = 1, parallelism = 2)
    result.length shouldBe 1
    result

  private def rowsOf(parts: Vector[GroupedIntBatch]): Seq[(Int, Seq[Int])] =
    parts.flatMap(_.rows.map { case (key, values) => (key, values.toSeq) })

  private def valuesByKey(rows: Iterable[(Int, Iterable[Int])]): Map[Int, Seq[Int]] =
    rows.map { case (key, values) => key -> values.toSeq }.toMap

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def withConf[A](
      enabled: Boolean,
      budget: Long,
      batchSize: Int = morselRows,
      partitions: Int = 4,
  )(body: Path => A): A =
    val dir = Files.createTempDirectory("scarlet-group-spill")
    GroupByKeySpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = enabled,
        spillBudgetBytes = budget,
        spillDirectory = dir.toString,
        columnarBatchSize = batchSize,
        columnarExecution = true,
        defaultShufflePartitions = partitions,
        threadPoolSize = 2,
        spillMaxFiles = SpillStore.DefaultMaxFiles,
        spillMaxBytes = SpillStore.DefaultMaxBytes,
      ),
    )
    try body(dir)
    finally
      ScarletConf.set(originalConf)
      GroupByKeySpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
