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
 * Long `groupByKey` spill. The estimate charges 8-byte keys. There is no selected kernel. A
 * filter is covered through [[DistCollection]].
 */
class TestLongGroupByKeySpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val morselRows = 4
  private val spillingRows = 9

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    LongGroupByKeySpill.resetForTest()

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ScarletRuntime.get.shuffle.clear()
    LongGroupByKeySpill.resetForTest()
    super.afterEach()

  "the long estimate" should "charge more than an int group table" in {
    LongGroupByKeySpill.inputRowBytes(1L) should be > GroupByKeySpill.inputRowBytes(1L)
    LongGroupByKeySpill.inputRowBytes(0L) shouldBe 0L
  }

  "spill off" should "match the in-memory groups at a tiny budget and write no file" in {
    val batch = distinct(spillingRows)
    val memory = rowsOf(inMemory(Vector(batch)))
    withConf(enabled = false, budget = 1L) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memory
      LongGroupByKeySpill.filesWrittenForTest shouldBe 0L
      LongGroupByKeySpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "publish one empty batch per partition at budget 0 and write nothing" in {
    List(false, true).foreach { enabled =>
      withConf(enabled, budget = 0L) { dir =>
        val missing = GroupByKey.int64(Vector.empty, partitions = 4, parallelism = 2)
        val present =
          GroupByKey.int64(Vector(ColumnBatches.emptyLongPairs), partitions = 3, parallelism = 1)
        missing.length shouldBe 4
        present.length shouldBe 3
        (missing ++ present).foreach { grouped =>
          grouped.groups shouldBe 0
          grouped.offsets.toSeq shouldBe Seq(0)
        }
        LongGroupByKeySpill.filesWrittenForTest shouldBe 0L
        SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      }
    }
  }

  "a bucket at the budget" should "write nothing" in {
    val rows = 8
    val batch = distinct(rows)
    val memoryRows = rowsOf(inMemory(Vector(batch)))
    val budget = LongGroupByKeySpill.inputRowBytes(rows.toLong)
    budget shouldBe LongGroupByKeySpill.inputRowBytes(morselRows.toLong)
    withConf(enabled = true, budget = budget) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memoryRows
      LongGroupByKeySpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "distinct keys" should "spill and keep 1 distinct from 1 << 32" in {
    val keys = Array(1L, 1L << 32, Long.MinValue, 2L, 3L, 4L, 5L, 6L, 7L)
    val batch = BatchCodec.longPairs(keys.zipWithIndex.map { case (key, index) => (key, index.toLong) })
    val memoryRows = rowsOf(inMemory(Vector(batch)))
    memoryRows.map(_._1) shouldBe keys.toSeq
    withConf(enabled = true, budget = morselBudget) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memoryRows
      LongGroupByKeySpill.filesWrittenForTest should be > 1L
      Path.of(LongGroupByKeySpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "one hot key" should "append values in input order across morsels" in {
    val values = 0 until spillingRows
    val batch = BatchCodec.longPairs(values.map(value => (7L, value.toLong)))
    val memoryRows = rowsOf(inMemory(Vector(batch)))
    memoryRows shouldBe Seq((7L, values.map(_.toLong)))
    withConf(enabled = true, budget = morselBudget) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memoryRows
      LongGroupByKeySpill.filesWrittenForTest should be > 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill on at budget 1" should "refuse a morsel and leave no file" in {
    val batch = BatchCodec.longPairs(Seq(4L -> 1L))
    withConf(enabled = true, budget = 1L, batchSize = 8) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy group(Vector(batch))
      refused.rows shouldBe 1L
      refused.bytes shouldBe LongGroupByKeySpill.inputRowBytes(1L)
      refused.bytes should be > refused.budget
      refused.budget shouldBe 1L
      LongGroupByKeySpill.filesWrittenForTest shouldBe 0L
      Path.of(LongGroupByKeySpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a null key" should "be dropped when the bucket spills" in {
    val keyValues = Array(1L, 2L, 1L, 3L, 4L, 5L, 6L, 7L, 8L, 1L)
    val valueValues = Array(10L, 99L, 11L, 30L, 40L, 50L, 60L, 70L, 80L, 12L)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.of(keyValues, Validity.pack(10, row => row != 1), 10),
        Int64Column.of(valueValues, Validity.allValid(10), 10),
      ),
      10,
    )
    val memoryRows = rowsOf(inMemory(Vector(batch)))
    memoryRows.flatMap(_._2) should not contain 99L
    withConf(enabled = true, budget = morselBudget) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memoryRows
      LongGroupByKeySpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a clear value bit" should "throw before a file is opened" in {
    val batch = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.of(Array(1L, 0L), Validity.pack(2, row => row != 1), 2),
        Int64Column.of(Array(10L, 0L), Validity.pack(2, row => row != 1), 2),
      ),
      2,
    )
    withConf(enabled = true, budget = 1L) { dir =>
      val refused = the[IllegalArgumentException] thrownBy group(Vector(batch))
      refused.getMessage should include("clear value bit")
      LongGroupByKeySpill.filesWrittenForTest shouldBe 0L
      LongGroupByKeySpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "empty buckets" should "stay empty when another bucket spills" in {
    val values = 0 until spillingRows
    val batch = BatchCodec.longPairs(values.map(value => (Long.MinValue, value.toLong)))
    val slot = Math.floorMod(java.lang.Long.hashCode(Long.MinValue), 4)
    withConf(enabled = true, budget = morselBudget) { dir =>
      val parts = GroupByKey.int64(Vector(batch), partitions = 4, parallelism = 2)
      parts.length shouldBe 4
      rowsOf(Vector(parts(slot))) shouldBe Seq((Long.MinValue, values.map(_.toLong)))
      parts.zipWithIndex.foreach { (grouped, index) =>
        if (index != slot) then grouped.groups shouldBe 0
      }
      LongGroupByKeySpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a filtered collect" should "match the row path and spill the kept rows" in {
    val keys = Array(1L, 1L << 32, Long.MinValue, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)
    val values = Array.tabulate(keys.length)(_.toLong)
    withConf(enabled = true, budget = morselBudget, partitions = 1) { dir =>
      val source = DistCollection.ofLongPairs(keys, values, 1)
      val filtered = source
        .filterKeys((key: Long) => key != Long.MinValue)
        .filterValues((value: Long) => value != (keys.length - 1).toLong)
        .groupByKey
      val (found, batches) = ColumnMetrics.withSink(new CountingSink) {
        val collected = valuesByKey(filtered.collect())
        collected -> ColumnMetrics.batchCount
      }
      val shape = ColumnarPlanner.lower(filtered.plan).map(_.shape).getOrElse(fail("row path"))
      shape shouldBe "GroupByKey(Exchange(FilterValue(FilterKey(Scan))))"
      batches should be > 0L
      found shouldBe valuesByKey(row(filtered.collect()))
      found.contains(Long.MinValue) shouldBe false
      found.contains(1L) shouldBe true
      found.contains(1L << 32) shouldBe true
      found(1L) should not be found(1L << 32)
      LongGroupByKeySpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  private def morselBudget: Long = LongGroupByKeySpill.inputRowBytes(morselRows.toLong)

  private def distinct(rows: Int): ColumnBatch =
    BatchCodec.longPairs((0 until rows).map(key => (key.toLong, key.toLong)))

  private def inMemory(batches: Vector[ColumnBatch]): Vector[GroupedLongBatch] =
    withConf(enabled = false, budget = LongGroupByKeySpill.inputRowBytes(64L)) { _ =>
      group(batches)
    }

  private def group(batches: Vector[ColumnBatch]): Vector[GroupedLongBatch] =
    val result = GroupByKey.int64(batches, partitions = 1, parallelism = 2)
    result.length shouldBe 1
    result

  private def rowsOf(parts: Vector[GroupedLongBatch]): Seq[(Long, Seq[Long])] =
    parts.flatMap(_.rows.map { case (key, values) => (key, values.toSeq) })

  private def valuesByKey(rows: Iterable[(Long, Iterable[Long])]): Map[Long, Seq[Long]] =
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
    val dir = Files.createTempDirectory("scarlet-long-group-spill")
    LongGroupByKeySpill.resetForTest()
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
      LongGroupByKeySpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
