package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.{ColumnarPlanner, ExecutionModuleSupport}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * String `groupByKey` spill. The estimate charges dictionary bytes, including an entry no row
 * uses. Groups merge by string equality.
 */
class TestStringGroupByKeySpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val morselRows = 4
  private val accent = "é"
  private val decomposed = "e\u0301"

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    StringGroupByKeySpill.resetForTest()

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ScarletRuntime.get.shuffle.clear()
    StringGroupByKeySpill.resetForTest()
    super.afterEach()

  "the string estimate" should "charge utf-8 bytes and an unused dictionary entry" in {
    val short = textBatch("a")
    val longer = textBatch("éé")
    StringGroupByKeySpill.inputRowBytes(longer) should be > StringGroupByKeySpill.inputRowBytes(
      short,
    )
    val plain = repeated("a", 4)
    val padded = unusedEntry(plain)
    StringGroupByKeySpill.inputRowBytes(padded) should be > StringGroupByKeySpill.inputRowBytes(
      plain,
    )
    StringGroupByKeySpill.inputRowBytes(ColumnBatches.emptyStringPairs) shouldBe 0L
  }

  "spill off" should "match the in-memory groups at a tiny budget and write no file" in {
    val batch = distinct
    val memory = rowsOf(inMemory(Vector(batch)))
    withConf(enabled = false, budget = 1L) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memory
      StringGroupByKeySpill.filesWrittenForTest shouldBe 0L
      StringGroupByKeySpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "publish one empty batch per partition at budget 0 and write nothing" in {
    List(false, true).foreach { enabled =>
      withConf(enabled, budget = 0L) { dir =>
        val missing = GroupByKey.strings(Vector.empty, partitions = 4, parallelism = 2)
        val present =
          GroupByKey.strings(Vector(ColumnBatches.emptyStringPairs), partitions = 3, parallelism = 1)
        missing.length shouldBe 4
        present.length shouldBe 3
        (missing ++ present).foreach { grouped =>
          grouped.groups shouldBe 0
          grouped.offsets.toSeq shouldBe Seq(0)
        }
        StringGroupByKeySpill.filesWrittenForTest shouldBe 0L
        SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      }
    }
  }

  "a bucket at the budget" should "write nothing" in {
    val batch = repeated("a", 4)
    val memoryRows = rowsOf(inMemory(Vector(batch)))
    val budget = StringGroupByKeySpill.inputRowBytes(batch)
    withConf(enabled = true, budget = budget) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memoryRows
      StringGroupByKeySpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an unused dictionary entry" should "push the bucket over the budget and be dropped from the morsel" in {
    val plain = repeated("a", 4)
    val padded = unusedEntry(plain)
    val budget = StringGroupByKeySpill.inputRowBytes(plain)
    StringGroupByKeySpill.inputRowBytes(padded) should be > budget
    withConf(enabled = true, budget = budget) { dir =>
      rowsOf(group(Vector(padded))) shouldBe Seq(("a", Seq(0, 1, 2, 3)))
      StringGroupByKeySpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "distinct keys" should "spill, keep an empty string, and keep both accent forms" in {
    val batch = distinct
    val memoryRows = rowsOf(inMemory(Vector(batch)))
    accent should not be decomposed
    memoryRows.map(_._1) should contain allOf ("", accent, decomposed)
    val budget = morselBudget(batch)
    StringGroupByKeySpill.inputRowBytes(batch) should be > budget
    withConf(enabled = true, budget = budget) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memoryRows
      StringGroupByKeySpill.filesWrittenForTest should be > 1L
      Path.of(StringGroupByKeySpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill on at budget 1" should "refuse a compacted morsel and leave no file" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1))
    val compact = ColumnBatches.compactDictionaries(batch)
    withConf(enabled = true, budget = 1L, batchSize = 8) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy group(Vector(batch))
      refused.rows shouldBe 1L
      refused.bytes shouldBe StringGroupByKeySpill.inputRowBytes(compact)
      refused.bytes should be > refused.budget
      refused.budget shouldBe 1L
      StringGroupByKeySpill.filesWrittenForTest shouldBe 0L
      Path.of(StringGroupByKeySpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a null key" should "be dropped when the bucket spills" in {
    val rows = Seq(nullText(99)) ++ (0 until 9).map(index => s"k$index" -> index)
    val batch = BatchCodec.stringIntPairs(rows)
    val memoryRows = rowsOf(inMemory(Vector(batch)))
    memoryRows.flatMap(_._2) should not contain 99
    val budget = morselBudget(batch)
    withConf(enabled = true, budget = budget) { dir =>
      rowsOf(group(Vector(batch))) shouldBe memoryRows
      StringGroupByKeySpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a null value" should "decline before a file is opened" in {
    val source = DistCollection(
      Plan.Source(Seq(Partition(Seq("red" -> 1, erasedNullValue("blue"))))),
    )
    val grouped = source.groupByKey
    withConf(enabled = true, budget = 1L) { dir =>
      ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some(
        "GroupByKey(Exchange(Scan))",
      )
      ColumnarPlanner.tryRun(grouped.plan) shouldBe None
      StringGroupByKeySpill.filesWrittenForTest shouldBe 0L
      StringGroupByKeySpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a survivor index" should "spill kept rows and omit dropped rows" in {
    val pairs = (0 until 9).flatMap(key => Seq((s"k$key", key), ("drop", -1)))
    val batch = BatchCodec.stringIntPairs(pairs)
    val selected = Selected(batch, Array.tabulate(9)(key => key * 2), 9)
    val memoryRows = rowsOf(inMemorySelected(Vector(selected)))
    memoryRows.map(_._1) should not contain "drop"
    val kept = BatchCodec.stringIntPairs((0 until 9).map(key => s"k$key" -> key))
    val budget = morselBudget(kept)
    withConf(enabled = true, budget = budget) { dir =>
      val spilled = GroupByKey.stringsSelected(Vector(selected), partitions = 1, parallelism = 1)
      rowsOf(spilled) shouldBe memoryRows
      StringGroupByKeySpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a filtered collect" should "match the row path and spill the kept rows" in {
    val rows = (0 until 12).map { index =>
      val text = if (index < 10) then "0" + index.toString else index.toString
      (text, index)
    }
    val kept = BatchCodec.stringIntPairs(rows.filter { case (key, value) =>
      key != "00" && value != 11
    })
    val budget = morselBudget(kept)
    StringGroupByKeySpill.inputRowBytes(kept) should be > budget
    withConf(enabled = true, budget = budget, partitions = 1) { dir =>
      val source = DistCollection(rows, 1)
      val filtered = source
        .filterKeys((key: String) => key != "00")
        .filterValues((value: Int) => value != 11)
        .groupByKey
      val (found, batches) = ColumnMetrics.withSink(new CountingSink) {
        val collected = valuesByKey(filtered.collect())
        collected -> ColumnMetrics.batchCount
      }
      val shape = ColumnarPlanner.lower(filtered.plan).map(_.shape).getOrElse(fail("row path"))
      shape shouldBe "GroupByKey(Exchange(FilterValue(FilterKey(Scan))))"
      batches should be > 0L
      found shouldBe valuesByKey(row(filtered.collect()))
      found.contains("00") shouldBe false
      found.values.flatten.toSeq should not contain 11
      StringGroupByKeySpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  private def distinct: ColumnBatch =
    val rows = Seq("" -> 1, accent -> 2, decomposed -> 3) ++ (0 until 6).map { index =>
      s"k$index" -> (index + 4)
    }
    BatchCodec.stringIntPairs(rows)

  /** Largest compacted morsel of `batch`. A spill of this batch fits one morsel and not the bucket. */
  private def morselBudget(batch: ColumnBatch): Long =
    ColumnBatches
      .slice(batch, morselRows)
      .map(piece => StringGroupByKeySpill.inputRowBytes(ColumnBatches.compactDictionaries(piece)))
      .max

  private def repeated(text: String, rows: Int): ColumnBatch =
    BatchCodec.stringIntPairs((0 until rows).map(index => text -> index))

  private def textBatch(text: String): ColumnBatch =
    val keys = DictUtf8Column.of(Array(text), Array(0), Validity.allValid(1), 1)
    val values = Int32Column.of(Array(1), Validity.allValid(1), 1)
    new ColumnBatch(Vector(LogicalType.Utf8Dict, LogicalType.Int32), Vector(keys, values), 1)

  private def unusedEntry(batch: ColumnBatch): ColumnBatch =
    val keys = batch.columns.head match
      case utf8: DictUtf8Column =>
        DictUtf8Column.of(
          utf8.dictionary :+ "unused-dictionary-entry",
          utf8.codes,
          utf8.validity,
          utf8.length,
        )
      case other => fail(s"expected a dictionary, found $other")
    new ColumnBatch(batch.schema, Vector(keys, batch.columns(1)), batch.length)

  private def inMemory(batches: Vector[ColumnBatch]): Vector[GroupedStringBatch] =
    val budget = batches.map(StringGroupByKeySpill.inputRowBytes).maxOption.getOrElse(0L)
    withConf(enabled = false, budget = budget) { _ =>
      group(batches)
    }

  private def inMemorySelected(selected: Vector[Selected]): Vector[GroupedStringBatch] =
    withConf(enabled = false, budget = 1L) { _ =>
      GroupByKey.stringsSelected(selected, partitions = 1, parallelism = 1)
    }

  private def group(batches: Vector[ColumnBatch]): Vector[GroupedStringBatch] =
    val result = GroupByKey.strings(batches, partitions = 1, parallelism = 2)
    result.length shouldBe 1
    result

  private def rowsOf(parts: Vector[GroupedStringBatch]): Seq[(String, Seq[Int])] =
    parts.flatMap(_.rows.map { case (key, values) => (key, values.toSeq) })

  private def valuesByKey(rows: Iterable[(String, Iterable[Int])]): Map[String, Seq[Int]] =
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

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullText(value: Int): (String, Int) = (null: String) -> value

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erasedNullValue(key: String): (String, Int) =
    Tuple2[String, Integer](key, null).asInstanceOf[(String, Int)]

  private def withConf[A](
      enabled: Boolean,
      budget: Long,
      batchSize: Int = morselRows,
      partitions: Int = 4,
  )(body: Path => A): A =
    val dir = Files.createTempDirectory("scarlet-string-group-spill")
    StringGroupByKeySpill.resetForTest()
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
      StringGroupByKeySpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
