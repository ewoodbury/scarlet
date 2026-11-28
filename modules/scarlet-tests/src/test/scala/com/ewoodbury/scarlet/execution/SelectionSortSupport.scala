package com.ewoodbury.scarlet.execution
import java.nio.file.{Files, Path}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/** Fixture for the int selection-sort specs. */
trait SelectionSortSupport extends AnyFlatSpec with Matchers with BeforeAndAfterEach:
  protected val originalConf = ScarletConf.get
  protected val keys: Array[Int] = Array(1, -1, 2, 3, 4, 0, 5, 6)
  protected val values: Array[Int] = Array(10, 20, 30, 40, 50, 5, 60, 70)
  protected val kept: Seq[(Int, Int)] = Seq(2 -> 30, 3 -> 40, 4 -> 50, 5 -> 60, 6 -> 70)
  protected val stackShape = "SortByIntKey(FilterValue(FilterKey(Scan)))"

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

  protected def pairs: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(keys, values, 2)

  protected def keepAll(source: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    source.filterKeys((_: Int) => true).filterValues((_: Int) => true)

  protected def shape(data: DistCollection[_]): String =
    ColumnarPlanner.lower(data.plan).map(_.shape).getOrElse(fail("expected a columnar plan"))

  protected def assertFewerBatches(
      sorted: DistCollection[(Int, Int)],
      filtered: DistCollection[(Int, Int)],
      source: DistCollection[(Int, Int)],
  ): Unit =
    val (compactPublishes, dense, selectedCount) = batchGap(sorted, filtered, source)
    compactPublishes should be > 0L
    (dense - selectedCount) shouldBe compactPublishes

  /**
   * `dense` compacts the filter, then rank-splits. `selectedCount` is the sort collect. The two
   * differ by the compact when the sort itself consumes the survivor index.
   */
  protected def batchGap(
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

  protected def denseSorted(filtered: DistCollection[(Int, Int)]): Vector[ColumnBatch] =
    val op = ColumnarPlanner.lower(filtered.plan).getOrElse(fail("expected a filter"))
    val batches = ColumnarInterpreter.pairsOf(ColumnarInterpreter.interpret(filtered.plan, op))
    RankSplitRuns.sortAndSplit(
      batches,
      ColumnarPlanner.shuffleWidth,
      ColumnarPlanner.parallelism,
      ScarletConf.get,
    )

  protected def sortedParts(plan: Plan[(Int, Int)]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered sort"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.Pairs(parts) => parts
      case other => fail(s"expected int pairs, got $other")

  protected def identical(actual: Vector[ColumnBatch], expected: Vector[ColumnBatch]): Unit =
    actual.length shouldBe expected.length
    actual.zip(expected).foreach { (left, right) =>
      left.length shouldBe right.length
      left.schema shouldBe right.schema
      columnImage(left, 0) shouldBe columnImage(right, 0)
      columnImage(left, 1) shouldBe columnImage(right, 1)
    }

  protected def columnImage(batch: ColumnBatch, ordinal: Int): (Seq[Int], Seq[Boolean]) =
    val column = int32(batch, ordinal)
    val stored = (0 until batch.length).map(row => column.values(row))
    val valid = (0 until batch.length).map(row => column.isValid(row))
    (stored, valid)

  protected def cells(batch: ColumnBatch): Seq[(Option[Int], Option[Int])] =
    val keyColumn = int32(batch, 0)
    val valueColumn = int32(batch, 1)
    (0 until batch.length).map { row =>
      val key = if (keyColumn.isValid(row)) then Some(keyColumn.values(row)) else None
      val value = if (valueColumn.isValid(row)) then Some(valueColumn.values(row)) else None
      (key, value)
    }

  protected def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  protected def allRows(batch: ColumnBatch): Selected =
    Selected(batch, Array.tabulate(batch.length)(row => row), batch.length)

  protected def decoded(parts: Vector[ColumnBatch]): Seq[(Int, Int)] =
    parts.flatMap(batch => BatchCodec.decodeIntPairs(batch))

  protected def published[A](body: => A): Long =
    ColumnMetrics.withSink(new CountingSink) {
      val _ = body
      ColumnMetrics.batchCount
    }

  protected def collect(data: DistCollection[(Int, Int)]): Seq[(Int, Int)] =
    ScarletRuntime.get.shuffle.clear()
    data.collect().toSeq

  protected def collectAt(data: DistCollection[(Int, Int)], batch: Int): Seq[(Int, Int)] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarBatchSize = batch))
      collect(data)
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  protected def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  protected def withSpill(budget: Long)(body: Path => Unit): Unit =
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
  protected def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  /** Copies both cells, including a null. `productElement` does not unbox. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  protected def copyPair(pair: (Int, Int)): (Int, Int) =
    val product = pair.asInstanceOf[Product]
    Tuple2(product.productElement(0), product.productElement(1)).asInstanceOf[(Int, Int)]

  /** A string key in an int-pair slot. The pair project declines the cell. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  protected def stringKey(pair: (Int, Int)): (Int, Int) =
    Tuple2(pair._1.toString, java.lang.Integer.valueOf(pair._2)).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
  protected def shown(rows: Seq[(Int, Int)]): Seq[String] =
    rows.map { row =>
      val product = row.asInstanceOf[Product]
      String.valueOf(product.productElement(0)) + ":" + String.valueOf(product.productElement(1))
    }

  protected def effectiveKeys(rows: Seq[(Int, Int)]): Seq[Int] =
    rows.map(pair => if (present(pair, 0)) storedInt(pair, 0) else 0)

  protected def present(pair: (Int, Int), ordinal: Int): Boolean =
    pair.productElement(ordinal) match
      case _: java.lang.Integer => true
      case _ => false

  protected def storedInt(pair: (Int, Int), ordinal: Int): Int =
    pair.productElement(ordinal) match
      case key: java.lang.Integer => key.intValue()
      case other => fail(s"expected an int component, found $other")

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  protected def noInt: java.lang.Integer = null

  protected def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)
