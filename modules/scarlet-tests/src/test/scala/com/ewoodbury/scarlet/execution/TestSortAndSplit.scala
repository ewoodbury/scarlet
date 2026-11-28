package com.ewoodbury.scarlet.execution
import java.nio.file.Path

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.support.Check.shouldBeEmpty

/** Maps that stay off an int key sort, and the selected split. */
class TestSortAndSplit extends SelectionSortSupport:
  "other maps" should "stay off sortByIntKey" in {
    val source = pairs
    val values = source.mapValues((value: Int) => value + 1).sortByIntKey
    val twice = source
      .map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
      .map((pair: (Int, Int)) => (pair._1, pair._2 + 1))
    val above = source
      .map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
      .filter((pair: (Int, Int)) => pair._2 > 0)
      .sortByIntKey
    val twiceDesc = twice.sortByIntValueDesc
    val aboveMap = source
      .mapValues((value: Int) => value + 1)
      .filterValues((value: Int) => value > 0)
      .sortByIntValueDesc
    val twiceMapped = source
      .mapValues((value: Int) => value + 1)
      .mapValues((value: Int) => value + 1)
      .sortByIntValueDesc
    val onProject = source
      .map((pair: (Int, Int)) => (pair._1, pair._2))
      .mapValues((value: Int) => value + 1)
      .sortByIntValueDesc
    val second = source
      .map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
      .filterKeys((key: Int) => key > 2)
      .filterValues((value: Int) => value > 0)
      .sortByIntKey
    val secondAbove = source
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 0)
      .sortByIntKey
    val secondKey = source
      .mapIntKey((key: Int) => key + 1)
      .mapIntKey((key: Int) => key + 1)
      .sortByIntKey
    val rowAboveKey = source
      .mapIntKey((key: Int) => key + 1)
      .filter((pair: (Int, Int)) => pair._2 > 0)
      .sortByIntKey
    val secondSlot = source
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 0)
      .sortByIntValueDesc
    val rowAboveMap = source
      .mapIntKey((key: Int) => key + 1)
      .filter((pair: (Int, Int)) => pair._2 > 0)
      .sortByIntValueDesc
    val slotAboveSecond = source
      .mapIntKey((key: Int) => key + 1)
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 0)
      .sortByIntValueDesc
    val secondMap = source
      .mapIntKey((key: Int) => key + 1)
      .mapIntKey((key: Int) => key + 1)
      .sortByIntValueDesc
    val projectUnderMap = source
      .map((pair: (Int, Int)) => (pair._1, pair._2))
      .mapIntKey((key: Int) => key + 1)
      .sortByIntValueDesc
    val rowAboveProject = source
      .map((pair: (Int, Int)) => (pair._1, pair._2))
      .filter((pair: (Int, Int)) => pair._2 > 0)
      .sortByIntValueDesc
    val secondAboveProject = source
      .map((pair: (Int, Int)) => (pair._1, pair._2))
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 0)
      .sortByIntValueDesc
    val longs = DistCollection
      .ofLongPairs(Array(3L, 1L), Array(30L, 10L), 1)
      .map((pair: (Long, Long)) => (pair._1.toInt, pair._2.toInt))
    val strings =
      DistCollection(Seq("b" -> 2, "a" -> 1), 1).map((pair: (String, Int)) => (pair._2, pair._2))
    val bad = source.map((pair: (Int, Int)) => stringKey(pair)).sortByIntKey

    ColumnarPlanner.lower(values.plan).shouldBeEmpty
    ColumnarPlanner.lower(twice.plan).map(_.shape) shouldBe Some("Project(Project(Scan))")
    ColumnarPlanner.lower(twice.sortByIntKey.plan).shouldBeEmpty
    ColumnarPlanner.lower(above.plan).shouldBeEmpty
    ColumnarPlanner.lower(twiceDesc.plan).shouldBeEmpty
    ColumnarPlanner.lower(aboveMap.plan).shouldBeEmpty
    ColumnarPlanner.lower(twiceMapped.plan).shouldBeEmpty
    ColumnarPlanner.lower(onProject.plan).shouldBeEmpty
    ColumnarPlanner.lower(second.plan).shouldBeEmpty
    ColumnarPlanner.lower(secondAbove.plan).shouldBeEmpty
    ColumnarPlanner.lower(secondKey.plan).shouldBeEmpty
    ColumnarPlanner.lower(rowAboveKey.plan).shouldBeEmpty
    ColumnarPlanner.lower(secondSlot.plan).shouldBeEmpty
    ColumnarPlanner.lower(rowAboveMap.plan).shouldBeEmpty
    ColumnarPlanner.lower(slotAboveSecond.plan).shouldBeEmpty
    ColumnarPlanner.lower(secondMap.plan).shouldBeEmpty
    ColumnarPlanner.lower(projectUnderMap.plan).shouldBeEmpty
    ColumnarPlanner.lower(rowAboveProject.plan).shouldBeEmpty
    ColumnarPlanner.lower(secondAboveProject.plan).shouldBeEmpty
    ColumnarPlanner.lower(longs.plan).map(_.shape) shouldBe Some("Project(Scan)")
    ColumnarPlanner.lower(longs.sortByIntKey.plan).shouldBeEmpty
    ColumnarPlanner.lower(longs.sortByIntValueDesc.plan).shouldBeEmpty
    ColumnarPlanner.lower(strings.plan).map(_.shape) shouldBe Some("Project(Scan)")
    ColumnarPlanner.lower(strings.sortByIntKey.plan).shouldBeEmpty
    ColumnarPlanner.lower(strings.sortByIntValueDesc.plan).shouldBeEmpty
    shape(bad) shouldBe "SortByIntKey(Project(Scan))"
    val lowered = ColumnarPlanner.lower(bad.plan).getOrElse(fail("expected a project sort"))
    a[ColumnarFallback] should be thrownBy {
      val _ = ColumnarInterpreter.interpret(bad.plan, lowered)
    }
    ColumnarPlanner.tryRun(bad.plan).shouldBeEmpty
  }

  "sortAndSplitSelected" should "match sortAndSplit of the materialised prefix" in {
    val batch = BatchCodec.intPairs(Seq(4 -> 1, 1 -> 2, 4 -> 3, 2 -> 4, 1 -> 5))
    // Rows 0, 2, and 3. The key-1 row at index 1 is dropped.
    val selected = Selected(batch, Array(0, 2, 3), 3)
    val parts = RankSplit.sortAndSplitSelected(Vector(selected), partitions = 2, parallelism = 1)
    val dense = RankSplit.sortAndSplit(
      Vector(Selection.materialize(selected)),
      partitions = 2,
      parallelism = 1,
    )

    identical(parts, dense)
    val flat = parts.flatMap(batch => cells(batch))
    flat shouldBe Seq((Some(2), Some(4)), (Some(4), Some(1)), (Some(4), Some(3)))
  }

  it should "order a dirty null key as zero and keep the null" in {
    val keys = Int32Column.of(Array(7, 1, 4), Validity.pack(3, row => row != 0), 3)
    val values = Int32Column.of(Array(10, 20, 30), Validity.pack(3, row => row != 2), 3)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(keys, values),
      3,
    )
    val selected = Selected(batch, Array(0, 1, 2), 3)
    val parts = RankSplit.sortAndSplitSelected(Vector(selected), partitions = 1, parallelism = 1)
    val sorted = parts.headOption.getOrElse(fail("expected one partition"))

    cells(sorted) shouldBe Seq((None, Some(10)), (Some(1), Some(20)), (Some(4), None))
    val dense = RankSplit.sortAndSplit(
      Vector(Selection.materialize(selected)),
      partitions = 1,
      parallelism = 1,
    )
    identical(parts, dense)
  }

  it should "keep one hot key in survivor order and ignore a dropped row" in {
    val batch = BatchCodec.intPairs(Seq(7 -> 1, 7 -> 2, 7 -> 3, 7 -> 4))
    val selected = Selected(batch, Array(0, 2), 2)
    val parts = RankSplit.sortAndSplitSelected(Vector(selected), partitions = 1, parallelism = 1)

    parts.flatMap(one => cells(one)) shouldBe Seq((Some(7), Some(1)), (Some(7), Some(3)))
  }

  it should "publish empty int-pair batches for an empty selection" in {
    val batch = BatchCodec.intPairs(Seq(1 -> 10, 2 -> 20))
    val empty = Selected(batch, Array(0, 1), 0)
    val parts =
      RankSplit.sortAndSplitSelected(Vector(empty, empty), partitions = 3, parallelism = 1)

    parts.length shouldBe 3
    parts.foreach { one =>
      one.length shouldBe 0
      one.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int32)
    }
  }

  it should "sort a live prefix beside an empty selection" in {
    val live = BatchCodec.intPairs(Seq(3 -> 30, 1 -> 10))
    val dead = BatchCodec.intPairs(Seq(9 -> 90))
    val selected = Vector(Selected(live, Array(0, 1), 2), Selected(dead, Array(0), 0))
    val serial = RankSplit.sortAndSplitSelected(selected, partitions = 2, parallelism = 1)
    val parallel = RankSplit.sortAndSplitSelected(selected, partitions = 2, parallelism = 2)

    identical(parallel, serial)
    serial.flatMap(batch => cells(batch)) shouldBe Seq((Some(1), Some(10)), (Some(3), Some(30)))
  }

  it should "match the dense split when every row is selected" in {
    val left = BatchCodec.intPairs(Seq(5 -> 50, 1 -> 10))
    val right = BatchCodec.intPairs(Seq(1 -> 11, 0 -> 0))
    val selected = Vector(allRows(left), allRows(right))
    val viaSelection = RankSplit.sortAndSplitSelected(selected, partitions = 2, parallelism = 2)
    val viaDense = RankSplit.sortAndSplit(Vector(left, right), partitions = 2, parallelism = 1)

    identical(viaSelection, viaDense)
  }

  it should "reject an empty vector, a non-positive width, and a non-positive parallelism" in {
    val empty = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val selected = Vector(Selected(empty, Array.empty[Int], 0))
    val missing = the[IllegalArgumentException] thrownBy {
      RankSplit.sortAndSplitSelected(Vector.empty, partitions = 2, parallelism = 1)
    }
    val width = the[IllegalArgumentException] thrownBy {
      RankSplit.sortAndSplitSelected(selected, partitions = 0, parallelism = 1)
    }
    val workers = the[IllegalArgumentException] thrownBy {
      RankSplit.sortAndSplitSelected(selected, partitions = 1, parallelism = 0)
    }

    missing.getMessage should include("sortAndSplitSelected needs at least one batch")
    width.getMessage should include("numPartitions must be positive")
    workers.getMessage should include("parallelism must be positive")
  }

  "sortAndSplitSelected spill" should "match the in-memory split and write the survivor index" in {
    val batch = BatchCodec.intPairs(Seq(4 -> 1, 1 -> 2, 3 -> 3, 1 -> 4))
    val selected = Vector(Selected(batch, Array(0, 1, 3), 3))
    val memory = RankSplit.sortAndSplitSelected(selected, partitions = 2, parallelism = 1)
    val nullKeys = Int32Column.of(Array(4, 1, 3, 1), Validity.pack(4, row => row != 1), 4)
    val nullValues = Int32Column.of(Array(1, 2, 3, 4), Validity.pack(4, row => row != 3), 4)
    val nullBatch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(nullKeys, nullValues),
      4,
    )
    // Drop the present key 3. The null key orders as zero and stays null.
    val nullSelected = Vector(Selected(nullBatch, Array(0, 1, 3), 3))
    val nullMemory = RankSplit.sortAndSplitSelected(nullSelected, partitions = 2, parallelism = 1)

    withSpill(budget = 1L) { dir =>
      val spilled = RankSplitRuns.sortAndSplitSelected(
        selected,
        partitions = 2,
        parallelism = 2,
        ScarletConf.get,
      )
      identical(spilled, memory)
      RankSplitRuns.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]

      RankSplitRuns.resetForTest()
      val nullSpilled = RankSplitRuns.sortAndSplitSelected(
        nullSelected,
        partitions = 2,
        parallelism = 1,
        ScarletConf.get,
      )
      identical(nullSpilled, nullMemory)
      val nullFlat = nullSpilled.flatMap(one => cells(one))
      nullFlat should contain((None, Some(2)))
      nullFlat should contain((Some(1), None))
      nullFlat.exists { case (Some(3), Some(3)) => true; case _ => false } shouldBe false
      RankSplitRuns.filesWrittenForTest should be > 0L
    }
    RankSplitRuns.resetForTest()
    ScarletConf.set(ScarletConf.get.copy(spillEnabled = false, spillBudgetBytes = 0L))
    val stayed = RankSplitRuns.sortAndSplitSelected(
      selected,
      partitions = 2,
      parallelism = 1,
      ScarletConf.get,
    )
    identical(stayed, memory)
    RankSplitRuns.filesWrittenForTest shouldBe 0L
  }

  it should "match the in-memory split when the prefix fits the budget" in {
    val batch = BatchCodec.intPairs(Seq(4 -> 1, 1 -> 2, 3 -> 3, 1 -> 4))
    val selected = Vector(Selected(batch, Array(0, 1, 3), 3))
    val memory = RankSplit.sortAndSplitSelected(selected, partitions = 2, parallelism = 1)
    val nullKeys = Int32Column.of(Array(4, 1, 3, 1), Validity.pack(4, row => row != 1), 4)
    val nullValues = Int32Column.of(Array(1, 2, 3, 4), Validity.pack(4, row => row != 3), 4)
    val nullBatch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(nullKeys, nullValues),
      4,
    )
    val nullSelected = Vector(Selected(nullBatch, Array(0, 1, 3), 3))
    val nullMemory = RankSplit.sortAndSplitSelected(nullSelected, partitions = 2, parallelism = 1)

    withSpill(budget = 64L * 1024L * 1024L) { _ =>
      val stayed = RankSplitRuns.sortAndSplitSelected(
        selected,
        partitions = 2,
        parallelism = 2,
        ScarletConf.get,
      )
      identical(stayed, memory)
      RankSplitRuns.filesWrittenForTest shouldBe 0L

      RankSplitRuns.resetForTest()
      val nullStayed = RankSplitRuns.sortAndSplitSelected(
        nullSelected,
        partitions = 2,
        parallelism = 1,
        ScarletConf.get,
      )
      identical(nullStayed, nullMemory)
      RankSplitRuns.filesWrittenForTest shouldBe 0L
    }
  }
