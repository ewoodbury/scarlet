package com.ewoodbury.scarlet.execution
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.support.Check
import com.ewoodbury.scarlet.support.Check.shouldBeEmpty

/**
 * TPC-H Q6 shape: filter long columns, multiply two of them, and sum the product.
 *
 * A null in either multiplied column contributes nothing. The plan is not a ledger row.
 */
class TestWideQuery extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val rows = 4096

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8192,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
        spillEnabled = false,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "a wide product" should "sum the same longs on both paths" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.nullableLongs("price", Array(10L, 0L, 4L, 2L), Array(true, false, true, true)),
          WideInput
            .nullableLongs("discount", Array(2L, 3L, 0L, 5L), Array(true, true, false, true)),
        ),
        2,
      )
      .compute(ColumnExpr.Mul(0, 1), "revenue")
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideCompute(Scan)")
    same(planned, 2, 30L)
    val kept = List(
      WideRow(
        Vector(field("price"), field("discount"), field("revenue")),
        Vector(Cell.Int64(10L), Cell.Int64(2L), Cell.Int64(20L)),
      ),
      WideRow(
        Vector(field("price"), field("discount"), field("revenue")),
        Vector(Cell.Int64(2L), Cell.Int64(5L), Cell.Int64(10L)),
      ),
    )
    collect(planned).filter(row => !Check.same(Check.last(row.cells), Cell.Null)) shouldBe kept
  }

  it should "match a row fold of the Q6 filters" in {
    val price = new Array[Long](rows)
    val pricePresent = new Array[Boolean](rows)
    val discount = new Array[Long](rows)
    val discountPresent = new Array[Boolean](rows)
    val quantity = new Array[Long](rows)
    val ship = new Array[Long](rows)
    val expected = (0 until rows).foldLeft(0L) { (sum, index) =>
      price(index) = (index * 17L) % 1000L
      pricePresent(index) = index % 17 != 0
      discount(index) = (index % 11).toLong
      discountPresent(index) = index % 19 != 0
      quantity(index) = (index % 40).toLong
      ship(index) = 50L + (index % 200).toLong
      val shipOk = ship(index) >= 100L && ship(index) < 200L
      val discountOk = discountPresent(index) && discount(index) >= 5L && discount(index) <= 7L
      val quantityOk = quantity(index) < 24L
      if (shipOk && discountOk && quantityOk && pricePresent(index)) then
        sum + price(index) * discount(index)
      else sum
    }
    val planned = lineitem(price, pricePresent, discount, discountPresent, quantity, ship)
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some(
      "WideCompute(WideFilter2(WideFilter1(WideFilter1(WideFilter0(WideFilter0(Scan))))))",
    )
    same(planned, 4, expected)
  }

  it should "reject a string factor before the job starts" in {
    val source = DistCollection.ofColumns(
      Vector(
        WideInput.longs("k", Array(1L, 2L)),
        WideInput.strings("s", Array("a", "b")),
      ),
      1,
    )
    an[IllegalArgumentException] should be thrownBy {
      source.compute(ColumnExpr.Mul(0, 1), "bad")
    }
    val planned = DistCollection(Plan.WideComputeOp(source.plan, ColumnExpr.Mul(0, 1), "bad"))
    ColumnarPlanner.lower(planned.plan).shouldBeEmpty
  }

  private def lineitem(
      price: Array[Long],
      pricePresent: Array[Boolean],
      discount: Array[Long],
      discountPresent: Array[Boolean],
      quantity: Array[Long],
      ship: Array[Long],
  ): DistCollection[WideRow] =
    DistCollection
      .ofColumns(
        Vector(
          WideInput.longs("ship", ship),
          WideInput.nullableLongs("discount", discount, discountPresent),
          WideInput.longs("quantity", quantity),
          WideInput.nullableLongs("price", price, pricePresent),
        ),
        4,
      )
      .filterColumn(0, ColumnCompare(CompareOp.Ge, ColumnLiteral.Int64(100L)))
      .filterColumn(0, ColumnCompare(CompareOp.Lt, ColumnLiteral.Int64(200L)))
      .filterColumn(1, ColumnCompare(CompareOp.Ge, ColumnLiteral.Int64(5L)))
      .filterColumn(1, ColumnCompare(CompareOp.Le, ColumnLiteral.Int64(7L)))
      .filterColumn(2, ColumnCompare(CompareOp.Lt, ColumnLiteral.Int64(24L)))
      .compute(ColumnExpr.Mul(3, 1), "revenue")

  private def same(planned: DistCollection[WideRow], ordinal: Int, expected: Long): Unit =
    val columnar = planned.sumLongColumn(ordinal)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = planned.sumLongColumn(ordinal)
    columnar shouldBe expected
    rows shouldBe expected

  private def collect(planned: DistCollection[WideRow]): List[WideRow] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = planned.collect().toList
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = planned.collect().toList
    columnar shouldBe rows
    columnar

  private def field(name: String): ColumnField = ColumnField(name, ColumnKind.Int64)
