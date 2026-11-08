package com.ewoodbury.scarlet.columnar

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.columnar.StringHashJoin.StringJoinInput
import com.ewoodbury.scarlet.core.ScarletConf

class TestStringHashJoinSpill extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val width = 4
  private val half = 32
  private val accent = "é"
  private val decomposed = "e\u0301"

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    StringHashJoinSpill.resetForTest()

  "retained bytes" should "be zero for an empty bucket and charge the dictionary once" in {
    StringHashJoinSpill.retainedBytes(0, 0, 0L) shouldBe 0L
    StringHashJoinSpill.retainedBytes(0, 4, 12L) shouldBe 0L
    StringHashJoinSpill.retainedBytes(-1, 1, 12L) shouldBe 0L
    val rows = 3
    val entries = 2
    val dictionary = 11L
    val mapEntries = math.min(rows, entries).toLong
    val expected =
      rows.toLong * (4L + 4L + 1L) +
        entries.toLong * 8L +
        dictionary +
        rows.toLong * (4L + 4L) +
        mapEntries * (8L + 4L)
    StringHashJoinSpill.retainedBytes(rows, entries, dictionary) shouldBe expected

    val batch = BatchCodec.stringIntPairs(Seq(accent -> 1, accent -> 2, "😀" -> 3))
    StringHashJoinSpill.dictionaryEntries(batch) shouldBe 2
    StringHashJoinSpill.dictionaryBytes(batch) shouldBe utf8Length(accent) + utf8Length("😀")
    utf8Length(accent) shouldBe 2L
    utf8Length(decomposed) shouldBe 3L
    utf8Length("") shouldBe 0L
  }

  "a bucket at the budget" should "match the in-memory join and write no files" in {
    val build = pairs(
      rowsOf("", 8, identity) ++
        rowsOf(accent, 8, row => 100 + row) ++
        rowsOf(decomposed, 1, _ => 7),
    )
    val probe = pairs(Seq("" -> 1, accent -> 2, decomposed -> 3))
    val memory = join(build, probe, width)
    val buckets = HashExchange.partitionAllUtf8(build, keyOrdinal = 0, width, parallelism = 1)
    val budget = buckets.map(estimate).max
    buckets.map(estimate).foreach(bytes => bytes should be <= budget)
    buckets.map(estimate) should contain(budget)

    withConf(budget) { dir =>
      val same = join(build, probe, width)

      same.length shouldBe width
      pairsOf(same) shouldBe pairsOf(memory)
      pairsOf(same).map(_._1) should contain allOf ("", accent, decomposed)
      StringHashJoinSpill.filesWrittenForTest shouldBe 0L
      StringHashJoinSpill.openTablesForTest shouldBe 0
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an over-budget bucket" should "spill both parities and keep empty, accent, and decomposed keys" in {
    accent should not be decomposed
    Math.floorMod("".hashCode, 2) shouldBe Math.floorMod(decomposed.hashCode, 2)
    Math.floorMod("".hashCode, 2) should not be Math.floorMod(accent.hashCode, 2)
    val build = pairs(
      rowsOf("", half, identity) ++
        rowsOf(accent, half, row => 1000 + row) ++
        rowsOf(decomposed, 1, _ => 7),
    )
    val probe = pairs(Seq("" -> 10, "" -> 11, accent -> 20, decomposed -> 30))
    val memory = join(build, probe, partitions = 1)
    val batch = build.head
    val entries = StringHashJoinSpill.dictionaryEntries(batch)
    val dictionary = StringHashJoinSpill.dictionaryBytes(batch)
    entries shouldBe 3
    dictionary shouldBe utf8Length("") + utf8Length(accent) + utf8Length(decomposed)
    val budget = StringHashJoinSpill.retainedBytes(half + 1, entries, dictionary)
    val parent = StringHashJoinSpill.retainedBytes(half * 2 + 1, entries, dictionary)
    parent should be > budget

    withConf(budget, batchSize = 8) { dir =>
      SpillStore.splitCount(half * 2 + 1, parent, ScarletConf.get) shouldBe 2
      val spilled = join(build, probe, partitions = 1)
      val matches = ordered(pairsOf(spilled))

      spilled.length shouldBe 1
      matches shouldBe ordered(pairsOf(memory))
      matches.count(_._1 == "") shouldBe half * 2
      matches.count(_._1 == accent) shouldBe half
      matches.count(_._1 == decomposed) shouldBe 1
      matches.collect { case (`accent`, left, _) => left } should not contain 7
      // Five morsels of the larger piece share one file. The probe is not written.
      StringHashJoinSpill.filesWrittenForTest shouldBe 2L
      StringHashJoinSpill.openTablesForTest shouldBe 1
      Path.of(StringHashJoinSpill.lastDirectoryForTest) shouldBe dir
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "spill disabled" should "ignore a budget the build exceeds and write no files" in {
    val build = pairs(rowsOf("", half, identity) ++ rowsOf(accent, half, row => 1000 + row))
    val probe = pairs(Seq("" -> 10, accent -> 20))
    val memory = join(build, probe, partitions = 1)
    val dir = Files.createTempDirectory("scarlet-string-join-off")
    StringHashJoinSpill.resetForTest()
    ScarletConf.set(
      originalConf.copy(
        spillEnabled = false,
        spillBudgetBytes = 1L,
        spillDirectory = dir.toString,
      ),
    )
    try
      val same = join(build, probe, partitions = 1)

      pairsOf(same) shouldBe pairsOf(memory)
      StringHashJoinSpill.filesWrittenForTest shouldBe 0L
      StringHashJoinSpill.openTablesForTest shouldBe 0
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    finally
      ScarletConf.set(originalConf)
      StringHashJoinSpill.resetForTest()
      deleteTree(dir)
  }

  "a single hot key" should "throw the budget refusal and leave no files" in {
    val rows = 8
    val build = pairs(rowsOf("hot", rows, identity))
    val probe = pairs(Seq("hot" -> 7))
    val batch = build.head
    val entries = StringHashJoinSpill.dictionaryEntries(batch)
    val dictionary = StringHashJoinSpill.dictionaryBytes(batch)
    val budget = StringHashJoinSpill.retainedBytes(1, entries, dictionary)
    val estimate = StringHashJoinSpill.retainedBytes(rows, entries, dictionary)
    estimate should be > budget

    withConf(budget) { dir =>
      val caught = intercept[IllegalStateException] {
        val _ = join(build, probe, partitions = 1)
      }

      caught.getMessage shouldBe
        s"string shuffle-hash build piece retains about $estimate bytes for $rows rows, " +
        s"above the budget of $budget bytes"
      caught.getMessage should not include "broadcast"
      StringHashJoinSpill.filesWrittenForTest shouldBe 1L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "null keys" should "be dropped while an empty key survives the spill" in {
    val buildRows =
      rowsOf("", half, identity) ++
        rowsOf(accent, half, row => 1000 + row) ++
        Seq(nullKey(777), nullKey(0))
    val probeRows = Seq("" -> 10, accent -> 20, nullKey(888))
    val build = pairs(buildRows)
    val probe = pairs(probeRows)
    val memory = join(build, probe, partitions = 1)
    val batch = build.head
    val entries = StringHashJoinSpill.dictionaryEntries(batch)
    val dictionary = StringHashJoinSpill.dictionaryBytes(batch)
    entries shouldBe 2
    val budget = StringHashJoinSpill.retainedBytes(half, entries, dictionary)
    StringHashJoinSpill.retainedBytes(half * 2, entries, dictionary) should be > budget

    withConf(budget) { dir =>
      val matches = ordered(pairsOf(join(build, probe, partitions = 1)))

      matches shouldBe ordered(pairsOf(memory))
      matches.map(_._1) should contain("")
      matches.map(_._2) should not contain 777
      matches.map(_._3) should not contain 888
      StringHashJoinSpill.filesWrittenForTest shouldBe 2L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty probe bucket" should "write no file when its build is over budget" in {
    Math.floorMod("".hashCode, width) should not be Math.floorMod("b".hashCode, width)
    val build = pairs(rowsOf("", half * 2, identity))
    val probe = pairs(Seq("b" -> 7))
    val memory = join(build, probe, width)
    val batch = build.head
    val budget = StringHashJoinSpill.retainedBytes(
      half,
      StringHashJoinSpill.dictionaryEntries(batch),
      StringHashJoinSpill.dictionaryBytes(batch),
    )
    StringHashJoinSpill.retainedBytes(half * 2, 1, 0L) should be > budget

    withConf(budget) { dir =>
      val spilled = join(build, probe, width)

      spilled.length shouldBe width
      spilled.map(_.length) shouldBe Seq(0, 0, 0, 0)
      pairsOf(spilled) shouldBe pairsOf(memory)
      StringHashJoinSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a two-filter selection" should "spill only the survivor rows" in {
    Math.floorMod("a".hashCode, 2) should not be Math.floorMod("f".hashCode, 2)
    val group = 16
    val survivors = group * 2
    val buildRows = (0 until survivors).flatMap { index =>
      val key = if (index < group) "a" else "f"
      val value = if (index < group) index else 1000 + index
      Seq(key -> value, "drop" -> (9000 + index))
    }
    val batch = BatchCodec.stringIntPairs(buildRows)
    val selected = Selected(batch, Array.tabulate(survivors)(index => index * 2), survivors)
    val probe = Vector(BatchCodec.stringIntPairs(Seq("a" -> 50, "f" -> 60, "drop" -> 70)))
    val memory = joinSelected(selected, probe)
    val entries = StringHashJoinSpill.dictionaryEntries(batch)
    val dictionary = StringHashJoinSpill.dictionaryBytes(batch)
    entries shouldBe 3
    val budget = StringHashJoinSpill.retainedBytes(group, entries, dictionary)
    StringHashJoinSpill.retainedBytes(survivors, entries, dictionary) should be > budget
    val expected =
      (0 until group).map(index => ("a", index, 50)) ++
        (group until survivors).map(index => ("f", 1000 + index, 60))

    withConf(budget) { dir =>
      val spilled = joinSelected(selected, probe)
      val matches = ordered(pairsOf(spilled))

      spilled.length shouldBe 1
      matches shouldBe ordered(pairsOf(memory))
      matches shouldBe ordered(expected)
      matches.map(_._1) should not contain "drop"
      StringHashJoinSpill.filesWrittenForTest shouldBe 2L
      StringHashJoinSpill.openTablesForTest shouldBe 1
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "two hot buckets" should "finish under a file cap that cannot hold both splits" in {
    // Width 3 does not fix parity, so each bucket's two keys separate when the split is 2.
    Math.floorMod("".hashCode, 3) shouldBe Math.floorMod("c".hashCode, 3)
    Math.floorMod("".hashCode, 2) should not be Math.floorMod("c".hashCode, 2)
    Math.floorMod("d".hashCode, 3) shouldBe Math.floorMod("a".hashCode, 3)
    Math.floorMod("d".hashCode, 2) should not be Math.floorMod("a".hashCode, 2)
    val build = pairs(
      rowsOf("", half, identity) ++
        rowsOf("c", half, row => 1000 + row) ++
        rowsOf("d", half, row => 2000 + row) ++
        rowsOf("a", half, row => 3000 + row),
    )
    val probe = pairs(Seq("" -> 1, "c" -> 2, "d" -> 3, "a" -> 4))
    val memory = join(build, probe, partitions = 3)
    val batch = build.head
    val entries = StringHashJoinSpill.dictionaryEntries(batch)
    val dictionary = StringHashJoinSpill.dictionaryBytes(batch)
    val budget = StringHashJoinSpill.retainedBytes(half, entries, dictionary)
    val parent = StringHashJoinSpill.retainedBytes(half * 2, entries, dictionary)
    parent should be > budget

    withConf(budget, maxFiles = 3) { dir =>
      SpillStore.splitCount(half * 2, parent, ScarletConf.get) shouldBe 2
      val spilled = join(build, probe, partitions = 3)

      spilled.length shouldBe 3
      ordered(pairsOf(spilled)) shouldBe ordered(pairsOf(memory))
      StringHashJoinSpill.filesWrittenForTest shouldBe 4L
      StringHashJoinSpill.openTablesForTest shouldBe 1
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "an empty input" should "return one empty batch per partition and write nothing" in {
    withConf(1L) { dir =>
      val parts = join(Vector.empty, Vector.empty, width)

      parts.length shouldBe width
      parts.map(_.length) shouldBe Seq(0, 0, 0, 0)
      StringHashJoinSpill.filesWrittenForTest shouldBe 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  "a probe failure" should "delete the spill files before it returns" in {
    val build = pairs(
      rowsOf("", half, identity) ++
        rowsOf("c", half, row => 1000 + row) ++
        rowsOf("d", half, row => 2000 + row) ++
        rowsOf("a", half, row => 3000 + row),
    )
    val probe = pairs(Seq("" -> 1, "c" -> 2, "d" -> 3, "a" -> 4))
    val batch = build.head
    val budget = StringHashJoinSpill.retainedBytes(
      half,
      StringHashJoinSpill.dictionaryEntries(batch),
      StringHashJoinSpill.dictionaryBytes(batch),
    )

    withConf(budget) { dir =>
      StringHashJoinSpill.armProbeFailure()
      val caught = intercept[IllegalStateException] {
        val _ = join(build, probe, partitions = 3)
      }

      caught.getMessage shouldBe StringHashJoinSpill.ProbeFailed
      StringHashJoinSpill.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]
    }
  }

  private def pairs(rows: Seq[(String, Int)]): Vector[ColumnBatch] =
    Vector(BatchCodec.stringIntPairs(rows))

  private def rowsOf(key: String, count: Int, valueAt: Int => Int): Seq[(String, Int)] =
    Seq.tabulate(count)(row => (key, valueAt(row)))

  private def join(
      build: Vector[ColumnBatch],
      probe: Vector[ColumnBatch],
      partitions: Int,
  ): Vector[ColumnBatch] =
    StringHashJoin.inner(build, probe, partitions, parallelism = 2)

  private def joinSelected(selected: Selected, probe: Vector[ColumnBatch]): Vector[ColumnBatch] =
    StringHashJoin.inner(
      StringJoinInput.Selected(Vector(selected)),
      StringJoinInput.Dense(probe),
      numPartitions = 1,
      parallelism = 2,
    )

  private def estimate(bucket: ColumnBatch): Long =
    StringHashJoinSpill.retainedBytes(
      bucket.length,
      StringHashJoinSpill.dictionaryEntries(bucket),
      StringHashJoinSpill.dictionaryBytes(bucket),
    )

  private def ordered(rows: Seq[(String, Int, Int)]): Seq[(String, Int, Int)] =
    rows.sortBy { case (key, left, right) => (key, left, right) }

  private def pairsOf(batches: Vector[ColumnBatch]): Seq[(String, Int, Int)] =
    batches.flatMap(batch => BatchCodec.decodeStringJoin(batch)).map { case (key, (left, right)) =>
      (key, left, right)
    }

  private def withConf(
      budget: Long,
      batchSize: Int = originalConf.columnarBatchSize,
      maxFiles: Int = SpillStore.DefaultMaxFiles,
  )(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-string-join-spill")
    StringHashJoinSpill.resetForTest()
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
      StringHashJoinSpill.resetForTest()
      deleteTree(dir)

  private def deleteTree(dir: Path): Unit =
    SpillStore.listForTest(dir).foreach { path =>
      val _ = Files.deleteIfExists(path)
    }
    val _ = Files.deleteIfExists(dir)

  private def utf8Length(text: String): Long =
    text.getBytes(StandardCharsets.UTF_8).length.toLong

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value
