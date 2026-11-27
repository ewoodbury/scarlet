package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * TPC-H queries 6, 1, and 3 on generated rows. A checksum that matches only the row path fails.
 */
class TestTpch extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8192,
        threadPoolSize = 2,
        defaultShufflePartitions = TpchSlice.partitions,
        spillEnabled = false,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "the scale" should "be generated and named" in {
    TpchSlice.lineitemScale shouldBe "rows-4096"
    TpchSlice.ordersScale shouldBe "orders-256"
    TpchSlice.lineitemRows shouldBe 4096
    TpchSlice.orderRows shouldBe 256
  }

  "the note" should "name every TPC-H query" in {
    val lines = TpchSlice.note
    lines.length shouldBe 22
    lines.zipWithIndex.foreach { (line, offset) =>
      line.startsWith(s"${offset + 1} ") shouldBe true
    }
  }

  "query 6" should "sum the product on the columnar path" in {
    val planned = TpchSlice.revenuePlan()
    val shape =
      "WideCompute(WideFilter2(WideFilter1(WideFilter1(WideFilter0(WideFilter0(Scan))))))"
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some(shape)
    ColumnarPlanner.tryCount(planned.plan) shouldBe Some(TpchSlice.revenueRows)
    WideSum.longColumn(planned.plan, 4) shouldBe Some(TpchSlice.revenue)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    planned.sumLongColumn(4) shouldBe TpchSlice.revenue
  }

  "query 1" should "group one column after a long date filter" in {
    val planned = TpchSlice.pricingPlan()
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideGroup(WideFilter0(Scan))")
    ColumnarPlanner.tryCount(planned.plan) shouldBe Some(TpchSlice.pricingGroups)
    val columnar = planned.collect().toSeq
    cellsOf(columnar, 0) shouldBe Vector(
      Cell.Int32(0),
      Cell.Int64(1001L),
      Cell.Int64(25501L),
      Cell.Int32(1),
      Cell.Int32(50),
    )
    TpchSlice.pricingChecksum(columnar) shouldBe TpchSlice.pricingMix
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    TpchSlice.pricingChecksum(planned.collect()) shouldBe TpchSlice.pricingMix
  }

  "query 3" should "join and then group on the columnar path" in {
    val planned = TpchSlice.shippingPlan()
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some(
      "WideGroup(WideHashJoin(WideFilter1(Scan),WideCompute(WideFilter1(Scan))))",
    )
    ColumnarPlanner.tryCount(planned.plan) shouldBe Some(TpchSlice.shippingGroups)
    val columnar = planned.collect().toSeq
    columnar.length shouldBe 128
    cellsOf(columnar, 0) shouldBe Vector(
      Cell.Int32(0),
      Cell.Int64(1L),
      Cell.Int64(30L),
      Cell.Int64(30L),
      Cell.Int64(30L),
    )
    TpchSlice.shippingChecksum(columnar) shouldBe TpchSlice.shippingMix
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    TpchSlice.shippingChecksum(planned.collect()) shouldBe TpchSlice.shippingMix
  }

  "the slice" should "agree with the row path" in {
    TpchSlice.failures() shouldBe Nil
  }

  private def cellsOf(rows: Seq[WideRow], key: Int): Vector[Cell] =
    rows
      .collectFirst {
        case row if row.cells.headOption.contains(Cell.Int32(key)) => row.cells
      }
      .getOrElse(fail(s"missing group $key"))
