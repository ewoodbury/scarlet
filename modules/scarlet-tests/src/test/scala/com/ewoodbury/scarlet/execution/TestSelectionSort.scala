package com.ewoodbury.scarlet.execution

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Int `sortByIntKey` over a filter selection.
 *
 * A two-filter stack keeps the survivor index. The rank split sorts those rows and still publishes
 * a compacted sorted batch. A null key orders as zero and stays null. Ties keep input order. A
 * single filter, and a scan, still compact or sort on the dense path. Over a tiny spill budget the
 * selection is materialised and the dense spill writer runs.
 */
class TestSelectionSort extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val keys = Array(1, -1, 2, 3, 4, 0, 5, 6)
  private val values = Array(10, 20, 30, 40, 50, 5, 60, 70)
  private val kept = Seq(2 -> 30, 3 -> 40, 4 -> 50, 5 -> 60, 6 -> 70)
  private val stackShape = "SortByIntKey(FilterValue(FilterKey(Scan)))"

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 2,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
      ),
    )
    ScarletRuntime.get.shuffle.clear()
    RankSplitRuns.resetForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    RankSplitRuns.resetForTest()
    ScarletConf.set(originalConf)
    ()

  "a two-filter sort" should "match the row path, the dense sort, and skip the compact" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val source = pairs
    val filtered = source
      .filterKeys { (key: Int) =>
        val _ = keyCalls.incrementAndGet()
        key > 0
      }
      .filterValues { (value: Int) =>
        val _ = valueCalls.incrementAndGet()
        value > 15
      }
    val sorted = filtered.sortByIntKey

    shape(sorted) shouldBe stackShape
    val found = collect(sorted)
    // One pass: the key predicate sees every row, and the value predicate sees the six positive
    // keys. A second interpret would double both counts.
    keyCalls.get() shouldBe keys.length
    valueCalls.get() shouldBe 6
    found shouldBe kept
    found shouldBe row(sorted.collect().toSeq)
    found shouldBe decoded(denseSorted(filtered))
    assertFewerBatches(sorted, filtered, source)
  }

  it should "keep duplicate keys in input order, including across partitions" in {
    val source = DistCollection.ofIntPairs(
      Array(1, 2, 1, 2, 1, 2, 1, 2),
      Array(10, 20, 30, 40, 50, 60, 70, 80),
      2,
    )
    val sorted = keepAll(source).sortByIntKey
    val found = collect(sorted)

    shape(sorted) shouldBe stackShape
    found shouldBe Seq(1 -> 10, 1 -> 30, 1 -> 50, 1 -> 70, 2 -> 20, 2 -> 40, 2 -> 60, 2 -> 80)
    found shouldBe row(sorted.collect().toSeq)
    found shouldBe decoded(denseSorted(keepAll(source)))
  }

  it should "keep a null key that passes a value filter and order it as zero" in {
    val rows = Seq(
      intPair(boxedInt(2), boxedInt(20)),
      intPair(noInt, boxedInt(5)),
      intPair(boxedInt(-4), boxedInt(1)),
      intPair(boxedInt(0), boxedInt(8)),
      intPair(noInt, boxedInt(3)),
      intPair(boxedInt(1), noInt),
    )
    val sorted = DistCollection(rows, 2)
      .filterValues((value: Int) => value > 0)
      .filterValues((value: Int) => value < 100)
      .sortByIntKey
    val found = collect(sorted)

    shape(sorted) shouldBe "SortByIntKey(FilterValue(FilterValue(Scan)))"
    found shouldBe row(sorted.collect().toSeq)
    found shouldBe Seq(
      intPair(boxedInt(-4), boxedInt(1)),
      intPair(noInt, boxedInt(5)),
      intPair(boxedInt(0), boxedInt(8)),
      intPair(noInt, boxedInt(3)),
      intPair(boxedInt(2), boxedInt(20)),
    )
    val parts = sortedParts(sorted.plan)
    val flat = parts.flatMap(batch => cells(batch))
    flat shouldBe Seq(
      (Some(-4), Some(1)),
      (None, Some(5)),
      (Some(0), Some(8)),
      (None, Some(3)),
      (Some(2), Some(20)),
    )
  }

  it should "keep a null value on a selected row and drop a null key" in {
    val rows = Seq(
      intPair(boxedInt(1), boxedInt(10)),
      intPair(noInt, boxedInt(99)),
      intPair(boxedInt(0), noInt),
      intPair(boxedInt(2), boxedInt(20)),
    )
    val sorted = DistCollection(rows, 2)
      .filterKeys((key: Int) => key > -1)
      .filterKeys((key: Int) => key < 10)
      .sortByIntKey
    val found = collect(sorted)

    shape(sorted) shouldBe "SortByIntKey(FilterKey(FilterKey(Scan)))"
    // A null Int unboxes to 0 on the row path, so both key predicates keep that row. The columnar
    // key filter drops a null key and does not call the predicate. The null value stays.
    found shouldBe Seq(
      intPair(boxedInt(0), noInt),
      intPair(boxedInt(1), boxedInt(10)),
      intPair(boxedInt(2), boxedInt(20)),
    )
    row(sorted.collect().toSeq) shouldBe Seq(
      intPair(noInt, boxedInt(99)),
      intPair(boxedInt(0), noInt),
      intPair(boxedInt(1), boxedInt(10)),
      intPair(boxedInt(2), boxedInt(20)),
    )
    val flat = sortedParts(sorted.plan).flatMap(batch => cells(batch))
    flat shouldBe Seq((Some(0), None), (Some(1), Some(10)), (Some(2), Some(20)))
  }

  it should "drop a null key under a single filter and keep it on the row path" in {
    val rows = Seq(
      intPair(boxedInt(1), boxedInt(10)),
      intPair(noInt, boxedInt(99)),
      intPair(boxedInt(2), boxedInt(20)),
    )
    val sorted = DistCollection(rows, 2)
      .filterKeys((key: Int) => key > -1)
      .sortByIntKey
    val found = collect(sorted)

    shape(sorted) shouldBe "SortByIntKey(FilterKey(Scan))"
    found shouldBe Seq(intPair(boxedInt(1), boxedInt(10)), intPair(boxedInt(2), boxedInt(20)))
    row(sorted.collect().toSeq) shouldBe Seq(
      intPair(noInt, boxedInt(99)),
      intPair(boxedInt(1), boxedInt(10)),
      intPair(boxedInt(2), boxedInt(20)),
    )
  }

  it should "sort one hot key in input order" in {
    val rows = 8
    val source = DistCollection.ofIntPairs(Array.fill(rows)(7), Array.tabulate(rows)(_ + 1), 2)
    val sorted = source
      .filterKeys((key: Int) => key == 7)
      .filterValues((value: Int) => value > 0)
      .sortByIntKey
    val found = collect(sorted)

    shape(sorted) shouldBe stackShape
    found shouldBe (1 to rows).map(value => 7 -> value)
    found shouldBe row(sorted.collect().toSeq)
  }

  it should "keep every row when both filters pass" in {
    val source = pairs
    val filtered = keepAll(source)
    val sorted = filtered.sortByIntKey
    val found = collect(sorted)
    val plain = collect(source.sortByIntKey)

    shape(sorted) shouldBe stackShape
    found shouldBe plain
    found shouldBe row(sorted.collect().toSeq)
    found shouldBe decoded(denseSorted(filtered))
  }

  it should "publish empty output partitions when both filters reject every row" in {
    val sorted = pairs
      .filterKeys((_: Int) => false)
      .filterValues((_: Int) => true)
      .sortByIntKey
    val parts = sortedParts(sorted.plan)

    shape(sorted) shouldBe stackShape
    collect(sorted) shouldBe empty
    collect(sorted) shouldBe row(sorted.collect().toSeq)
    parts.length shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach { batch =>
      batch.length shouldBe 0
      batch.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int32)
    }
  }

  it should "publish empty output partitions for an empty source" in {
    val sorted = DistCollection
      .ofIntPairs(Array.empty[Int], Array.empty[Int], 2)
      .filterKeys((_: Int) => true)
      .filterValues((_: Int) => true)
      .sortByIntKey
    val parts = sortedParts(sorted.plan)

    shape(sorted) shouldBe stackShape
    collect(sorted) shouldBe empty
    parts.length shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(_.length shouldBe 0)
  }

  it should "match the in-memory selection sort and the row path when spill is on" in {
    val source = pairs
    val sorted = source
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 15)
      .sortByIntKey
    val inMemory = collect(sorted)

    withSpill(budget = 1L) { _ =>
      val spilled = collect(sorted)
      spilled shouldBe inMemory
      spilled shouldBe kept
      spilled shouldBe row(sorted.collect().toSeq)
      RankSplitRuns.filesWrittenForTest should be > 0L
    }
  }

  "a single filter" should "compact and take the dense path" in {
    val source = pairs
    val filtered = source.filterKeys((key: Int) => key > 0)
    val sorted = filtered.sortByIntKey

    shape(sorted) shouldBe "SortByIntKey(FilterKey(Scan))"
    val found = collect(sorted)
    found shouldBe Seq(1 -> 10, 2 -> 30, 3 -> 40, 4 -> 50, 5 -> 60, 6 -> 70)
    found shouldBe row(sorted.collect().toSeq)
    assertSameBatches(sorted, filtered, source)
  }

  "a scan" should "take the dense path, and a map under the sort should stay on the row path" in {
    val source = pairs
    val sorted = source.sortByIntKey
    val mapped = source.map((pair: (Int, Int)) => (pair._1, pair._2)).sortByIntKey

    shape(sorted) shouldBe "SortByIntKey(Scan)"
    collect(sorted) shouldBe row(sorted.collect().toSeq)
    val direct = published {
      val scan = ColumnarPlanner.lower(source.plan).getOrElse(fail("expected a scan"))
      val batches = ColumnarInterpreter.pairsOf(ColumnarInterpreter.interpret(source.plan, scan))
      RankSplitRuns.sortAndSplit(
        batches,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ScarletConf.get,
      )
    }
    published(sorted.collect()) shouldBe direct
    ColumnarPlanner.lower(mapped.plan) shouldBe Option.empty[PhysicalOp]
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

  "sortAndSplitSelected spill" should "materialise, match the in-memory split, and write runs" in {
    val batch = BatchCodec.intPairs(Seq(4 -> 1, 1 -> 2, 3 -> 3, 1 -> 4))
    val selected = Vector(Selected(batch, Array(0, 1, 3), 3))
    val memory = RankSplit.sortAndSplitSelected(selected, partitions = 2, parallelism = 1)

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

  private def pairs: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(keys, values, 2)

  private def keepAll(source: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    source.filterKeys((_: Int) => true).filterValues((_: Int) => true)

  private def shape(data: DistCollection[_]): String =
    ColumnarPlanner.lower(data.plan).map(_.shape).getOrElse(fail("expected a columnar plan"))

  private def assertFewerBatches(
      sorted: DistCollection[(Int, Int)],
      filtered: DistCollection[(Int, Int)],
      source: DistCollection[(Int, Int)],
  ): Unit =
    val (compactPublishes, dense, selectedCount) = batchGap(sorted, filtered, source)
    compactPublishes should be > 0L
    (dense - selectedCount) shouldBe compactPublishes

  /** A single filter still compacts, so the selection collect publishes the dense total. */
  private def assertSameBatches(
      sorted: DistCollection[(Int, Int)],
      filtered: DistCollection[(Int, Int)],
      source: DistCollection[(Int, Int)],
  ): Unit =
    val (compactPublishes, dense, selectedCount) = batchGap(sorted, filtered, source)
    compactPublishes should be > 0L
    selectedCount shouldBe dense

  /**
   * `dense` compacts the filter, then rank-splits. `selectedCount` is the sort collect. The two
   * differ by the compact when the sort itself consumes the survivor index.
   */
  private def batchGap(
      sorted: DistCollection[(Int, Int)],
      filtered: DistCollection[(Int, Int)],
      source: DistCollection[(Int, Int)],
  ): (Long, Long, Long) =
    val encode = published {
      val scan = ColumnarPlanner.lower(source.plan).getOrElse(fail("expected a scan"))
      ColumnarInterpreter.interpret(source.plan, scan)
    }
    val selectedCount = published {
      ScarletRuntime.get.shuffle.clear()
      sorted.collect()
    }
    val (afterFilter, dense) = ColumnMetrics.withSink(new CountingSink) {
      val op = ColumnarPlanner.lower(filtered.plan).getOrElse(fail("expected a filter"))
      val columns = ColumnarInterpreter.interpret(filtered.plan, op)
      val mid = ColumnMetrics.batchCount
      val _ = RankSplitRuns.sortAndSplit(
        ColumnarInterpreter.pairsOf(columns),
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ScarletConf.get,
      )
      (mid, ColumnMetrics.batchCount)
    }
    (afterFilter - encode, dense, selectedCount)

  private def denseSorted(filtered: DistCollection[(Int, Int)]): Vector[ColumnBatch] =
    val op = ColumnarPlanner.lower(filtered.plan).getOrElse(fail("expected a filter"))
    val batches = ColumnarInterpreter.pairsOf(ColumnarInterpreter.interpret(filtered.plan, op))
    RankSplitRuns.sortAndSplit(
      batches,
      ColumnarPlanner.shuffleWidth,
      ColumnarPlanner.parallelism,
      ScarletConf.get,
    )

  private def sortedParts(plan: Plan[(Int, Int)]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered sort"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.Pairs(parts) => parts
      case other => fail(s"expected int pairs, got $other")

  private def identical(actual: Vector[ColumnBatch], expected: Vector[ColumnBatch]): Unit =
    actual.length shouldBe expected.length
    actual.zip(expected).foreach { (left, right) =>
      left.length shouldBe right.length
      left.schema shouldBe right.schema
      columnImage(left, 0) shouldBe columnImage(right, 0)
      columnImage(left, 1) shouldBe columnImage(right, 1)
    }

  private def columnImage(batch: ColumnBatch, ordinal: Int): (Seq[Int], Seq[Boolean]) =
    val column = int32(batch, ordinal)
    val stored = (0 until batch.length).map(row => column.values(row))
    val valid = (0 until batch.length).map(row => column.isValid(row))
    (stored, valid)

  private def cells(batch: ColumnBatch): Seq[(Option[Int], Option[Int])] =
    val keyColumn = int32(batch, 0)
    val valueColumn = int32(batch, 1)
    (0 until batch.length).map { row =>
      val key = if (keyColumn.isValid(row)) then Some(keyColumn.values(row)) else None
      val value = if (valueColumn.isValid(row)) then Some(valueColumn.values(row)) else None
      (key, value)
    }

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def allRows(batch: ColumnBatch): Selected =
    Selected(batch, Array.tabulate(batch.length)(row => row), batch.length)

  private def decoded(parts: Vector[ColumnBatch]): Seq[(Int, Int)] =
    parts.flatMap(batch => BatchCodec.decodeIntPairs(batch))

  private def published[A](body: => A): Long =
    ColumnMetrics.withSink(new CountingSink) {
      val _ = body
      ColumnMetrics.batchCount
    }

  private def collect(data: DistCollection[(Int, Int)]): Seq[(Int, Int)] =
    ScarletRuntime.get.shuffle.clear()
    data.collect().toSeq

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def withSpill(budget: Long)(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-selection-sort")
    val saved = ScarletConf.get
    RankSplitRuns.resetForTest()
    ScarletConf.set(
      saved.copy(
        spillEnabled = true,
        spillBudgetBytes = budget,
        spillDirectory = dir.toString,
        spillFanIn = 2,
      ),
    )
    try body(dir)
    finally
      ScarletConf.set(saved)
      RankSplitRuns.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noInt: java.lang.Integer = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)
