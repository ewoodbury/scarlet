package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Long `countByKey` spill. Counts are compared as maps. A budget that holds eight rows and not
 * nine is the boundary: a hot-key piece is sliced under it, and a nine-key fold is not.
 */
class TestLongHashCountSpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  /** Rows whose estimate fits in [[tightBudget]]. One more row does not. */
  private val fittingRows = 8

  /** Partial rows that force a post-exchange bucket over [[tightBudget]]. */
  private val spillingRows = 9

  private val hotKey = Long.MinValue
  private val hotCopies = 4
  private val keptKey = 1L
  private val droppedKey = 99L

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    LongHashCountSpill.resetForTest()

  "spill off" should "refuse an input whose estimate exceeds the budget and write no file" in {
    val batch = BatchCodec.longPairs(Seq(1L -> 4L, 1L -> 5L, 2L -> 6L))
    val rows = batch.length.toLong
    val budget = LongHashCount.inputRowBytes(rows) - 1L
    withConf(enabled = false, budget = budget) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy count(Vector(batch))
      refused.rows shouldBe rows
      refused.bytes shouldBe LongHashCount.inputRowBytes(rows)
      refused.bytes should be > budget
      refused.budget shouldBe budget
      LongHashCountSpill.filesWrittenForTest shouldBe 0L
      LongHashCountSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill on at budget 1" should "refuse a non-empty input and leave no file" in {
    withConf(enabled = true, budget = 1L) { dir =>
      val dense = the[ClosedKernelRefusal] thrownBy {
        count(Vector(BatchCodec.longPairs(Seq(4L -> 1L))))
      }
      dense.rows shouldBe 1L
      dense.budget shouldBe 1L
      dense.bytes should be > dense.budget

      val selected = Selected(BatchCodec.longPairs(Seq(4L -> 1L, 5L -> 2L)), Array(1), 1)
      val indexed = the[ClosedKernelRefusal] thrownBy countSelected(Vector(selected))
      indexed.rows shouldBe 1L
      indexed.budget shouldBe 1L

      LongHashCountSpill.filesWrittenForTest shouldBe 0L
      LongHashCountSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "succeed at budget 0 and write nothing" in {
    List(false, true).foreach { enabled =>
      withConf(enabled, budget = 0L) { dir =>
        val dense = LongHashCount.byKey(
          Vector.empty[ColumnBatch],
          numPartitions = 4,
          parallelism = 1,
          batchSize = 8,
        )
        dense.length shouldBe 4
        dense.map(_.length) shouldBe Seq(0, 0, 0, 0)
        val selected = LongHashCount.byKeySelected(
          Vector.empty[Selected],
          numPartitions = 4,
          parallelism = 1,
          batchSize = 8,
        )
        selected.length shouldBe 4
        selected.map(_.length) shouldBe Seq(0, 0, 0, 0)
        LongHashCountSpill.filesWrittenForTest shouldBe 0L
        SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      }
    }
  }

  "one hot key" should "spill when the input exceeds the budget and match the in-memory count" in {
    val input = hotInput
    val expected = Map(hotKey -> (spillingRows * hotCopies))
    val wide = LongHashCount.inputRowBytes(hotRows.toLong)
    val memory = withConf(enabled = true, budget = wide) { dir =>
      val counted = counts(count(input))
      LongHashCountSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
      counted
    }
    memory shouldBe expected

    withConf(enabled = true, budget = tightBudget) { dir =>
      val spilled = counts(count(input))
      spilled shouldBe memory
      LongHashCountSpill.filesWrittenForTest should be > 0L
      Path.of(LongHashCountSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  it should "refuse that hot key when spill is off at the tight budget" in {
    withConf(enabled = false, budget = tightBudget) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy count(hotInput)
      refused.rows shouldBe hotRows.toLong
      refused.bytes shouldBe LongHashCount.inputRowBytes(hotRows.toLong)
      refused.budget shouldBe tightBudget
      LongHashCountSpill.filesWrittenForTest shouldBe 0L
      LongHashCountSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "distinct keys" should "refuse a fold that still does not fit and leave no file" in {
    val input = Vector.tabulate(spillingRows)(key => BatchCodec.longPairs(Seq(key.toLong -> 0L)))
    val memory =
      withConf(enabled = true, budget = LongHashCount.inputRowBytes(spillingRows.toLong)) { dir =>
        val counted = counts(count(input))
        LongHashCountSpill.filesWrittenForTest shouldBe 0L
        SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
        counted
      }
    memory shouldBe (0 until spillingRows).map(key => key.toLong -> 1).toMap

    withConf(enabled = true, budget = tightBudget) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy count(input)
      refused.rows shouldBe spillingRows.toLong
      refused.bytes shouldBe LongHashCount.inputRowBytes(spillingRows.toLong)
      refused.budget shouldBe tightBudget
      LongHashCountSpill.filesWrittenForTest should be > 0L
      Path.of(LongHashCountSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a survivor index" should "spill kept rows and ignore dropped rows" in {
    val input = survivorInput
    val expected = Map(keptKey -> (spillingRows * 2))
    val memory =
      withConf(enabled = true, budget = LongHashCount.inputRowBytes(survivorRows.toLong)) { dir =>
        val counted = counts(countSelected(input))
        LongHashCountSpill.filesWrittenForTest shouldBe 0L
        SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
        counted
      }
    memory shouldBe expected
    memory.contains(droppedKey) shouldBe false

    withConf(enabled = true, budget = tightBudget) { dir =>
      val spilled = counts(countSelected(input))
      spilled shouldBe memory
      LongHashCountSpill.filesWrittenForTest should be > 0L
      Path.of(LongHashCountSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }

    withConf(enabled = false, budget = tightBudget) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy countSelected(input)
      refused.rows shouldBe survivorRows.toLong
      refused.bytes shouldBe LongHashCount.inputRowBytes(survivorRows.toLong)
      refused.budget shouldBe tightBudget
      LongHashCountSpill.filesWrittenForTest shouldBe 0L
      LongHashCountSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  private def tightBudget: Long = LongHashCount.inputRowBytes(fittingRows.toLong)

  private def hotRows: Int = spillingRows * hotCopies

  private def survivorRows: Int = spillingRows * 2

  private def hotInput: Vector[ColumnBatch] =
    Vector.fill(spillingRows)(BatchCodec.longPairs(Seq.fill(hotCopies)(hotKey -> 1L)))

  private def survivorInput: Vector[Selected] =
    Vector.fill(spillingRows) {
      val batch = BatchCodec.longPairs(
        Seq(keptKey -> 1L, droppedKey -> 2L, keptKey -> 3L, droppedKey -> 4L),
      )
      Selected(batch, Array(0, 2), 2)
    }

  private def count(
      batches: Vector[ColumnBatch],
      partitions: Int = 1,
      parallelism: Int = 2,
      batchSize: Int = fittingRows,
  ): Vector[ColumnBatch] =
    val result = LongHashCount.byKey(batches, partitions, parallelism, batchSize)
    result.length shouldBe partitions
    result

  private def countSelected(
      selected: Vector[Selected],
      batchSize: Int = fittingRows,
  ): Vector[ColumnBatch] =
    val result = LongHashCount.byKeySelected(
      selected,
      numPartitions = 1,
      parallelism = 2,
      batchSize = batchSize,
    )
    result.length shouldBe 1
    result

  private def counts(batches: Vector[ColumnBatch]): Map[Long, Int] =
    val pairs = batches.flatMap(batch => BatchCodec.decodeLongCounts(batch))
    pairs.map(_._1) shouldBe pairs.map(_._1).distinct
    pairs.toMap

  private def withConf[A](enabled: Boolean, budget: Long)(body: Path => A): A =
    val dir = Files.createTempDirectory("scarlet-long-count-spill")
    LongHashCountSpill.resetForTest()
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
      LongHashCountSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
