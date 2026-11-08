package com.ewoodbury.scarlet.columnar

import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Int `countByKey` spill. Counts are compared as maps. A budget that holds eight rows and not nine
 * is the boundary: a hot-key piece is sliced under it, and a nine-key fold is not.
 */
class TestHashCountSpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  /** Rows whose estimate fits in [[tightBudget]]. One more row does not. */
  private val fittingRows = 8

  /** Partial rows that force a post-exchange bucket over [[tightBudget]]. */
  private val spillingRows = 9

  private val hotKey = Int.MinValue
  private val hotCopies = 4
  private val keptKey = 1
  private val droppedKey = 99

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    HashCountSpill.resetForTest()

  "spill off" should "refuse an input whose estimate exceeds the budget and write no file" in {
    val batch = BatchCodec.intPairs(Seq(1 -> 4, 1 -> 5, 2 -> 6))
    val rows = batch.length.toLong
    val budget = HashCount.inputRowBytes(rows) - 1L
    withConf(enabled = false, budget = budget) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy count(Vector(batch))
      refused.rows shouldBe rows
      refused.bytes shouldBe HashCount.inputRowBytes(rows)
      refused.bytes should be > budget
      refused.budget shouldBe budget
      HashCountSpill.filesWrittenForTest shouldBe 0L
      HashCountSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty
    }
  }

  "spill on at budget 1" should "refuse a non-empty input and leave no file" in {
    withConf(enabled = true, budget = 1L) { dir =>
      val dense = the[ClosedKernelRefusal] thrownBy {
        count(Vector(BatchCodec.intPairs(Seq(4 -> 1))))
      }
      dense.rows shouldBe 1L
      dense.budget shouldBe 1L
      dense.bytes should be > dense.budget

      val selected = Selected(BatchCodec.intPairs(Seq(4 -> 1, 5 -> 2)), Array(1), 1)
      val indexed = the[ClosedKernelRefusal] thrownBy countSelected(Vector(selected))
      indexed.rows shouldBe 1L
      indexed.budget shouldBe 1L

      HashCountSpill.filesWrittenForTest shouldBe 0L
      HashCountSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty
    }
  }

  "an empty input" should "succeed at budget 0 and write nothing" in {
    List(false, true).foreach { enabled =>
      withConf(enabled, budget = 0L) { dir =>
        val dense = HashCount.byKey(
          Vector.empty,
          numPartitions = 4,
          parallelism = 1,
          batchSize = 8,
        )
        dense.length shouldBe 4
        dense.map(_.length) shouldBe Seq(0, 0, 0, 0)
        val selected =
          HashCount.byKeySelected(Vector.empty, numPartitions = 4, parallelism = 1, batchSize = 8)
        selected.length shouldBe 4
        selected.map(_.length) shouldBe Seq(0, 0, 0, 0)
        HashCountSpill.filesWrittenForTest shouldBe 0L
        SpillStore.listForTest(dir) shouldBe Vector.empty
      }
    }
  }

  "one hot key" should "spill when the input exceeds the budget and match the in-memory count" in {
    val input = hotInput
    val expected = Map(hotKey -> (spillingRows * hotCopies))
    val wide = HashCount.inputRowBytes(hotRows.toLong)
    val memory = withConf(enabled = true, budget = wide) { dir =>
      val counted = counts(count(input))
      HashCountSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty
      counted
    }
    memory shouldBe expected

    withConf(enabled = true, budget = tightBudget) { dir =>
      val spilled = counts(count(input))
      spilled shouldBe memory
      HashCountSpill.filesWrittenForTest should be > 0L
      Path.of(HashCountSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty
    }
  }

  it should "refuse that hot key when spill is off at the tight budget" in {
    withConf(enabled = false, budget = tightBudget) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy count(hotInput)
      refused.rows shouldBe hotRows.toLong
      refused.bytes shouldBe HashCount.inputRowBytes(hotRows.toLong)
      refused.budget shouldBe tightBudget
      HashCountSpill.filesWrittenForTest shouldBe 0L
      HashCountSpill.lastDirectoryForTest shouldBe ""
      SpillStore.listForTest(dir) shouldBe Vector.empty
    }
  }

  "distinct keys" should "refuse a fold that still does not fit and leave no file" in {
    val input = Vector.tabulate(spillingRows)(key => BatchCodec.intPairs(Seq(key -> 0)))
    val memory = withConf(enabled = true, budget = HashCount.inputRowBytes(spillingRows.toLong)) {
      dir =>
        val counted = counts(count(input))
        HashCountSpill.filesWrittenForTest shouldBe 0L
        SpillStore.listForTest(dir) shouldBe Vector.empty
        counted
    }
    memory shouldBe (0 until spillingRows).map(key => key -> 1).toMap

    withConf(enabled = true, budget = tightBudget) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy count(input)
      refused.rows shouldBe spillingRows.toLong
      refused.bytes shouldBe HashCount.inputRowBytes(spillingRows.toLong)
      refused.budget shouldBe tightBudget
      HashCountSpill.filesWrittenForTest should be > 0L
      Path.of(HashCountSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty
    }
  }

  "a survivor index" should "spill kept rows and ignore dropped rows" in {
    val input = survivorInput
    val expected = Map(keptKey -> (spillingRows * 2))
    val memory =
      withConf(enabled = true, budget = HashCount.inputRowBytes(survivorRows.toLong)) { dir =>
        val counted = counts(countSelected(input))
        HashCountSpill.filesWrittenForTest shouldBe 0L
        SpillStore.listForTest(dir) shouldBe Vector.empty
        counted
      }
    memory shouldBe expected
    memory.contains(droppedKey) shouldBe false

    withConf(enabled = true, budget = tightBudget) { dir =>
      val spilled = counts(countSelected(input))
      spilled shouldBe memory
      HashCountSpill.filesWrittenForTest should be > 0L
      Path.of(HashCountSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty
    }

    withConf(enabled = false, budget = tightBudget) { dir =>
      val refused = the[ClosedKernelRefusal] thrownBy countSelected(input)
      refused.rows shouldBe survivorRows.toLong
      refused.bytes shouldBe HashCount.inputRowBytes(survivorRows.toLong)
      refused.budget shouldBe tightBudget
      HashCountSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty
    }
  }

  private def tightBudget: Long = HashCount.inputRowBytes(fittingRows.toLong)

  private def hotRows: Int = spillingRows * hotCopies

  private def survivorRows: Int = spillingRows * 2

  private def hotInput: Vector[ColumnBatch] =
    Vector.fill(spillingRows)(BatchCodec.intPairs(Seq.fill(hotCopies)(hotKey -> 1)))

  private def survivorInput: Vector[Selected] =
    Vector.fill(spillingRows) {
      val batch = BatchCodec.intPairs(
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
    val result = HashCount.byKey(batches, partitions, parallelism, batchSize)
    result.length shouldBe partitions
    result

  private def countSelected(
      selected: Vector[Selected],
      batchSize: Int = fittingRows,
  ): Vector[ColumnBatch] =
    val result = HashCount.byKeySelected(
      selected,
      numPartitions = 1,
      parallelism = 2,
      batchSize = batchSize,
    )
    result.length shouldBe 1
    result

  private def counts(batches: Vector[ColumnBatch]): Map[Int, Int] =
    val pairs = batches.flatMap(batch => BatchCodec.decodeIntPairs(batch))
    pairs.map(_._1) shouldBe pairs.map(_._1).distinct
    pairs.toMap

  private def withConf[A](enabled: Boolean, budget: Long)(body: Path => A): A =
    val dir = Files.createTempDirectory("scarlet-count-spill")
    HashCountSpill.resetForTest()
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
      HashCountSpill.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)
