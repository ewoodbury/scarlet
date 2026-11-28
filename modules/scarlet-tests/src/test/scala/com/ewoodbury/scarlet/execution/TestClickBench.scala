package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * ClickBench slice on generated columns. A checksum that matches only the row path fails here.
 */
class TestClickBench extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8192,
        threadPoolSize = 2,
        defaultShufflePartitions = ClickBenchSlice.partitions,
        spillEnabled = false,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "the note" should "name every ClickBench query" in {
    val lines = ClickBenchSlice.note
    lines.length shouldBe 43
    lines.zipWithIndex.foreach { (line, number) =>
      line.startsWith(s"$number ") shouldBe true
    }
  }

  "query 0" should "count every generated row on the columnar path" in {
    val planned = ClickBenchSlice.scan()
    ColumnarPlanner.lower(planned.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.Columns)
    ColumnarPlanner.tryCount(planned.plan) shouldBe Some(ClickBenchSlice.countAll)
    sameCount(planned, ClickBenchSlice.countAll)
  }

  "query 1" should "drop a zero AdvEngineID" in {
    val planned = ClickBenchSlice.filtered()
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideFilter0(Scan)")
    ColumnarPlanner.tryCount(planned.plan) shouldBe Some(ClickBenchSlice.countAdv)
    sameCount(planned, ClickBenchSlice.countAdv)
  }

  "query 2" should "sum the cast int columns" in {
    val adv = ClickBenchSlice.advAsLong()
    val width = ClickBenchSlice.widthAsLong()
    ColumnarPlanner.lower(adv.plan).map(_.shape) shouldBe Some("WideCompute(Scan)")
    WideSum.longColumn(adv.plan, 3) shouldBe Some(ClickBenchSlice.sumAdv)
    WideSum.longColumn(width.plan, 3) shouldBe Some(ClickBenchSlice.sumWidth)
    sameSum(adv, 3, ClickBenchSlice.sumAdv)
    sameSum(width, 3, ClickBenchSlice.sumWidth)
    sameCount(ClickBenchSlice.scan(), ClickBenchSlice.countAll)
  }

  "query 6" should "min and max ResolutionWidth" in {
    val planned = ClickBenchSlice.scan()
    WideExtremum.minInt(planned.plan, 1) shouldBe Some(ClickBenchSlice.minWidth)
    WideExtremum.maxInt(planned.plan, 1) shouldBe Some(ClickBenchSlice.maxWidth)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    planned.minIntColumn(1) shouldBe ClickBenchSlice.minWidth
    planned.maxIntColumn(1) shouldBe ClickBenchSlice.maxWidth
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    planned.minIntColumn(1) shouldBe ClickBenchSlice.minWidth
    planned.maxIntColumn(1) shouldBe ClickBenchSlice.maxWidth
  }

  "query 3" should "sum UserID" in {
    val planned = ClickBenchSlice.scan()
    WideSum.longColumn(planned.plan, 2) shouldBe Some(ClickBenchSlice.sumUser)
    sameSum(planned, 2, ClickBenchSlice.sumUser)
  }

  "query 4" should "count distinct UserID" in {
    val planned = ClickBenchSlice.scan()
    WideDistinctCount.count(planned.plan, 2) shouldBe Some(ClickBenchSlice.distinctUsers)
    sameDistinct(planned, 2, ClickBenchSlice.distinctUsers)
  }

  "query 5" should "count a repeating phrase" in {
    val planned = ClickBenchSlice.phrases()
    WideDistinctCount.count(planned.plan, 3) shouldBe Some(ClickBenchSlice.distinctPhrases)
    sameDistinct(planned, 3, ClickBenchSlice.distinctPhrases)
  }

  "query 8" should "count distinct widths inside each AdvEngineID" in {
    val planned = ClickBenchSlice.groupedDistinct()
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideGroup(Scan)")
    ColumnarPlanner.tryCount(planned.plan) shouldBe Some(ClickBenchSlice.distinctGroups)
    val columnar = collect(planned)
    columnar.length shouldBe 8
    cellsOf(columnar, 0) shouldBe Vector(
      Cell.Int32(0),
      Cell.Int64(512L),
      Cell.Int64(458128L),
      Cell.Int32(800),
      Cell.Int32(992),
      Cell.Int64(ClickBenchSlice.distinctWidths),
    )
    ClickBenchSlice.distinctChecksum(columnar) shouldBe ClickBenchSlice.distinctMix
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = planned.collect()
    ClickBenchSlice.distinctChecksum(rows) shouldBe ClickBenchSlice.distinctMix
  }

  "query 7" should "group AdvEngineID without building lists" in {
    val planned = ClickBenchSlice.grouped()
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some(
      "WideGroup(WideFilter0(Scan))",
    )
    ColumnarPlanner.tryCount(planned.plan) shouldBe Some(ClickBenchSlice.groups)
    val columnar = collect(planned)
    columnar.length shouldBe 7
    cellsOf(columnar, 1) shouldBe Vector(
      Cell.Int32(1),
      Cell.Int64(512L),
      Cell.Int64(512L),
      Cell.Int32(1),
      Cell.Int32(1),
    )
    cellsOf(columnar, 7) shouldBe Vector(
      Cell.Int32(7),
      Cell.Int64(512L),
      Cell.Int64(512L * 7L),
      Cell.Int32(7),
      Cell.Int32(7),
    )
    ClickBenchSlice.groupedChecksum(columnar) shouldBe ClickBenchSlice.groupedMix
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = planned.collect()
    ClickBenchSlice.groupedChecksum(rows) shouldBe ClickBenchSlice.groupedMix
  }

  "the slice" should "agree with the row path" in {
    ClickBenchSlice.failures() shouldBe Nil
  }

  private def sameCount(planned: DistCollection[WideRow], expected: Long): Unit =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = planned.count()
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = planned.count()
    columnar shouldBe expected
    rows shouldBe expected

  private def sameDistinct(planned: DistCollection[WideRow], ordinal: Int, expected: Long): Unit =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = planned.countDistinctColumn(ordinal)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = planned.countDistinctColumn(ordinal)
    columnar shouldBe expected
    rows shouldBe expected

  private def sameSum(planned: DistCollection[WideRow], ordinal: Int, expected: Long): Unit =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = planned.sumLongColumn(ordinal)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = planned.sumLongColumn(ordinal)
    columnar shouldBe expected
    rows shouldBe expected

  private def collect(planned: DistCollection[WideRow]): Seq[WideRow] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    planned.collect().toSeq

  private def cellsOf(rows: Seq[WideRow], key: Int): Vector[Cell] =
    rows
      .collectFirst {
        case row if row.cells.headOption.contains(Cell.Int32(key)) => row.cells
      }
      .getOrElse(fail(s"missing group $key"))
