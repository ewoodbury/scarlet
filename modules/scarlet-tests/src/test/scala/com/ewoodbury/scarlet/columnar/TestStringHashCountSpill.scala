package com.ewoodbury.scarlet.columnar

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * String `countByKey` spill. Counts are compared as decoded pairs. A budget that holds eight rows
 * of a nine-entry dictionary, and not nine, is the boundary: a hot-key piece is sliced under it,
 * and a nine-key fold is not.
 */
class TestStringHashCountSpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  /** Rows whose estimate fits in [[tightBudget]] when the dictionary has nine one-byte entries. */
  private val fittingRows = 8

  /** Partial rows that force a post-exchange bucket over [[tightBudget]]. */
  private val spillingRows = 9

  private val hotKey = "k"
  private val hotCopies = 4
  private val keptKey = "k"
  private val droppedKey = "d"

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    StringHashCountSpill.resetForTest()

  "the estimate" should "stay zero for no rows and charge dictionary bytes" in {
    StringHashCount.inputRowBytes(0L, 4L, 12L) shouldBe 0L
    StringHashCount.inputRowBytes(-1L, 4L, 12L) shouldBe 0L
    val rows = 3
    val entries = 2
    val text = 11L
    val slots = ColumnHash.tableSize(rows).toLong
    val expected =
      rows.toLong * (4L + 1L) + entries.toLong * 8L + text + slots * (4L + 4L + 1L + 4L)
    StringHashCount.inputRowBytes(rows.toLong, entries.toLong, text) shouldBe expected

    val accent = "é"
    val emoji = "😀"
    val batch = BatchCodec.stringIntPairs(Seq(accent -> 1, accent -> 2, emoji -> 3))
    val accentBytes = accent.getBytes(StandardCharsets.UTF_8).length.toLong
    val emojiBytes = emoji.getBytes(StandardCharsets.UTF_8).length.toLong
    accentBytes shouldBe 2L
    emojiBytes shouldBe 4L
    StringHashCount.dictionaryEntries(batch) shouldBe 2
    StringHashCount.dictionaryBytes(batch) shouldBe accentBytes + emojiBytes

    val unused = "unused"
    val withSpare = new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(
        DictUtf8Column.of(Array("a", unused), Array(0, 0), Validity.allValid(2), 2),
        Int32Column.of(Array(1, 1), Validity.allValid(2), 2),
      ),
      2,
    )
    StringHashCount.dictionaryEntries(withSpare) shouldBe 2
    StringHashCount.dictionaryBytes(withSpare) shouldBe
      1L + unused.getBytes(StandardCharsets.UTF_8).length.toLong
  }

  "spill off" should "refuse an input whose estimate exceeds the budget and write no file" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 4, "a" -> 5, "b" -> 6))
    val rows = batch.length.toLong
    val bytes = inputCharge(Vector(batch), rows)
    val budget = bytes - 1L
    withConf(enabled = false, budget = budget) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy count(Vector(batch))
      refused.rows shouldBe rows
      refused.bytes shouldBe bytes
      refused.bytes should be > budget
      refused.budget shouldBe budget
      StringHashCountSpill.filesWrittenForTest shouldBe 0L
      StringHashCountSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill on at budget 1" should "refuse a non-empty input and leave no file" in {
    withConf(enabled = true, budget = 1L) { dir =>
      val dense = the[ClosedKernelRefusal] thrownBy {
        count(Vector(BatchCodec.stringIntPairs(Seq("a" -> 1))))
      }
      dense.rows shouldBe 1L
      dense.budget shouldBe 1L
      dense.bytes should be > dense.budget

      val selected = Selected(BatchCodec.stringIntPairs(Seq("a" -> 1, "b" -> 2)), Array(1), 1)
      val indexed = the[ClosedKernelRefusal] thrownBy countSelected(Vector(selected))
      indexed.rows shouldBe 1L
      indexed.budget shouldBe 1L

      StringHashCountSpill.filesWrittenForTest shouldBe 0L
      StringHashCountSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "succeed at budget 0 and write nothing" in {
    List(false, true).foreach { enabled =>
      withConf(enabled, budget = 0L) { dir =>
        val dense = StringHashCount.byKey(
          Vector.empty[ColumnBatch],
          numPartitions = 4,
          parallelism = 1,
          batchSize = 8,
        )
        dense.length shouldBe 4
        dense.map(_.length) shouldBe Seq(0, 0, 0, 0)
        fresh(dense)
        val selected = StringHashCount.byKeySelected(
          Vector.empty[Selected],
          numPartitions = 4,
          parallelism = 1,
          batchSize = 8,
        )
        selected.length shouldBe 4
        selected.map(_.length) shouldBe Seq(0, 0, 0, 0)
        fresh(selected)
        dense.head should not be theSameInstanceAs(selected.head)
        StringHashCountSpill.filesWrittenForTest shouldBe 0L
        SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      }
    }
  }

  "one hot key" should "spill when the input exceeds the budget and match the in-memory count" in {
    val input = hotInput
    val expected = Map(hotKey -> (spillingRows * hotCopies))
    val (rows, wide) = charged(input)
    val memory = withConf(enabled = true, budget = wide) { dir =>
      val counted = counts(count(input))
      StringHashCountSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      counted
    }
    memory shouldBe expected
    rows shouldBe hotRows.toLong

    withConf(enabled = true, budget = tightBudget) { dir =>
      val spilled = counts(count(input))
      spilled shouldBe memory
      StringHashCountSpill.filesWrittenForTest should be > 0L
      Path.of(StringHashCountSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  it should "refuse that hot key when spill is off at the tight budget" in {
    val input = hotInput
    val (rows, bytes) = charged(input)
    withConf(enabled = false, budget = tightBudget) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy count(input)
      refused.rows shouldBe rows
      refused.rows shouldBe hotRows.toLong
      refused.bytes shouldBe bytes
      refused.budget shouldBe tightBudget
      StringHashCountSpill.filesWrittenForTest shouldBe 0L
      StringHashCountSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "distinct keys" should "refuse a fold that still does not fit and leave no file" in {
    val input = Vector.tabulate(spillingRows) { key =>
      BatchCodec.stringIntPairs(Seq((('a' + key).toChar.toString, 0)))
    }
    val (rows, wide) = charged(input)
    val memory = withConf(enabled = true, budget = wide) { dir =>
      val counted = counts(count(input))
      StringHashCountSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      counted
    }
    memory shouldBe (0 until spillingRows).map(key => ('a' + key).toChar.toString -> 1).toMap
    rows shouldBe spillingRows.toLong

    withConf(enabled = true, budget = tightBudget) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy count(input)
      refused.rows shouldBe spillingRows.toLong
      refused.bytes shouldBe StringHashCount.inputRowBytes(
        spillingRows.toLong,
        spillingRows.toLong,
        spillingRows.toLong,
      )
      refused.budget shouldBe tightBudget
      StringHashCountSpill.filesWrittenForTest should be > 0L
      Path.of(StringHashCountSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a survivor index" should "spill kept rows and ignore dropped rows" in {
    val input = survivorInput
    val expected = Map(keptKey -> (spillingRows * 2))
    val (rows, wide) = chargedSelected(input)
    val memory = withConf(enabled = true, budget = wide) { dir =>
      val counted = counts(countSelected(input))
      StringHashCountSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      counted
    }
    memory shouldBe expected
    memory.contains(droppedKey) shouldBe false
    rows shouldBe survivorRows.toLong

    withConf(enabled = true, budget = tightBudget) { dir =>
      val spilled = counts(countSelected(input))
      spilled shouldBe memory
      StringHashCountSpill.filesWrittenForTest should be > 0L
      Path.of(StringHashCountSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }

    withConf(enabled = false, budget = tightBudget) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy countSelected(input)
      val (survivorCount, bytes) = chargedSelected(input)
      refused.rows shouldBe survivorCount
      refused.bytes shouldBe bytes
      refused.budget shouldBe tightBudget
      StringHashCountSpill.filesWrittenForTest shouldBe 0L
      StringHashCountSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  /** Eight rows carrying the nine one-byte keys. Nine rows of that dictionary do not fit. */
  private def tightBudget: Long =
    StringHashCount.inputRowBytes(fittingRows.toLong, spillingRows.toLong, spillingRows.toLong)

  private def hotRows: Int = spillingRows * hotCopies

  private def survivorRows: Int = spillingRows * 2

  private def hotInput: Vector[ColumnBatch] =
    Vector.fill(spillingRows)(BatchCodec.stringIntPairs(Seq.fill(hotCopies)(hotKey -> 1)))

  private def survivorInput: Vector[Selected] =
    Vector.fill(spillingRows) {
      val batch = BatchCodec.stringIntPairs(
        Seq(keptKey -> 1, droppedKey -> 2, keptKey -> 3, droppedKey -> 4),
      )
      Selected(batch, Array(0, 2), 2)
    }

  private def count(
      batches: Vector[ColumnBatch],
      partitions: Int = 1,
      parallelism: Int = 2,
      batchSize: Int = fittingRows,
  ): Vector[ColumnBatch] =
    val result = StringHashCount.byKey(batches, partitions, parallelism, batchSize)
    result.length shouldBe partitions
    result

  private def countSelected(
      selected: Vector[Selected],
      batchSize: Int = fittingRows,
  ): Vector[ColumnBatch] =
    val result = StringHashCount.byKeySelected(
      selected,
      numPartitions = 1,
      parallelism = 2,
      batchSize = batchSize,
    )
    result.length shouldBe 1
    result

  private def counts(batches: Vector[ColumnBatch]): Map[String, Int] =
    val pairs = batches.flatMap(batch => BatchCodec.decodeStringPairs(batch))
    pairs.map(_._1) shouldBe pairs.map(_._1).distinct
    pairs.toMap

  private def charged(batches: Vector[ColumnBatch]): (Long, Long) =
    val rows = batches.iterator.map(_.length.toLong).sum
    (rows, inputCharge(batches, rows))

  private def chargedSelected(selected: Vector[Selected]): (Long, Long) =
    val rows = selected.iterator.map(_.count.toLong).sum
    (rows, inputCharge(selected.map(_.batch), rows))

  private def inputCharge(batches: Iterable[ColumnBatch], rows: Long): Long =
    val entries =
      batches.iterator.map(batch => StringHashCount.dictionaryEntries(batch).toLong).sum
    val text = batches.iterator.map(batch => StringHashCount.dictionaryBytes(batch)).sum
    StringHashCount.inputRowBytes(rows, entries, text)

  private def fresh(batches: Vector[ColumnBatch]): Unit =
    batches.indices.tail.foreach { index =>
      batches(index) should not be theSameInstanceAs(batches(index - 1))
    }

  private def withConf[A](enabled: Boolean, budget: Long)(body: Path => A): A =
    val dir = Files.createTempDirectory("scarlet-string-count-spill")
    StringHashCountSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = enabled,
        spillBudgetBytes = budget,
        spillDirectory = dir.toString,
        spillMaxFiles = SpillStore.DefaultMaxFiles,
        spillMaxBytes = SpillStore.DefaultMaxBytes,
      ),
    )
    try body(dir)
    finally
      ScarletConf.set(originalConf)
      StringHashCountSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
