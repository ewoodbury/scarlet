package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{WideBatch, WideExpr}
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Decimal columns are scaled longs. Add, multiply, a literal compare, and an ungrouped sum match a
 * row fold. A 19th digit refuses. A null input stays null.
 */
class TestDecimal extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

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

  "decimal arithmetic" should "add different scales and keep a null" in {
    val table = WideColumns(
      Vector(
        WideInput.nullableDecimals("a", 4, 2, Array(150L, 0L), Array(true, true)),
        WideInput.nullableDecimals("b", 4, 3, Array(125L, 0L), Array(true, false)),
      ),
    )
    val summed = agree(table, ColumnExpr.Add(0, 1), "sum")
    summed.head.fields.last.kind shouldBe ColumnKind.Decimal(6, 3)
    summed.map(_.cells.last) shouldBe Vector(Cell.Decimal(6, 3, 1625L), Cell.Null)
  }

  it should "subtract after aligning scales" in {
    val table = WideColumns(
      Vector(
        WideInput.decimals("a", 4, 2, Array(500L)),
        WideInput.decimals("b", 4, 3, Array(1250L)),
      ),
    )
    val difference = agree(table, ColumnExpr.Sub(0, 1), "diff")
    difference.head.fields.last.kind shouldBe ColumnKind.Decimal(6, 3)
    difference.map(_.cells.last) shouldBe Vector(Cell.Decimal(6, 3, 3750L))
  }

  it should "multiply and widen precision and scale" in {
    val table = WideColumns(
      Vector(
        WideInput.decimals("a", 2, 1, Array(15L)),
        WideInput.decimals("b", 2, 1, Array(25L)),
      ),
    )
    val product = agree(table, ColumnExpr.Mul(0, 1), "product")
    product.head.fields.last.kind shouldBe ColumnKind.Decimal(4, 2)
    product.map(_.cells.last) shouldBe Vector(Cell.Decimal(4, 2, 375L))
  }

  it should "compare a column with a literal after aligning scales" in {
    val table = WideColumns(
      Vector(WideInput.decimals("a", 3, 2, Array(150L, 160L))),
    ).compute(ColumnExpr.Lit(ColumnLiteral.Decimal(2, 1, 15L)), "mark")
    val compared = agree(table, ColumnExpr.Compare(CompareOp.Eq, 0, 1), "eq")
    compared.map(_.cells.last) shouldBe Vector(Cell.Bool(true), Cell.Bool(false))
  }

  it should "cast between a long and a decimal toward zero" in {
    val ints = WideColumns(Vector(WideInput.ints("n", Array(5, -1))))
    val widened = agree(ints, ColumnExpr.Cast(0, ColumnKind.Decimal(4, 2)), "money")
    widened.map(_.cells.last) shouldBe Vector(
      Cell.Decimal(4, 2, 500L),
      Cell.Decimal(4, 2, -100L),
    )
    val money = WideColumns(Vector(WideInput.decimals("m", 4, 2, Array(150L, -150L, -99L))))
    val narrowed = agree(money, ColumnExpr.Cast(0, ColumnKind.Int64), "whole")
    narrowed.map(_.cells.last) shouldBe Vector(Cell.Int64(1L), Cell.Int64(-1L), Cell.Int64(0L))
  }

  it should "match a row fold on a few thousand rows" in {
    val left = Array.tabulate(rows)(row => (row % 1000).toLong)
    val right = Array.tabulate(rows)(row => (row % 50).toLong)
    val present = Array.tabulate(rows)(row => row != 7)
    val table = WideColumns(
      Vector(
        WideInput.nullableDecimals("a", 4, 0, left, present),
        WideInput.decimals("b", 2, 0, right),
      ),
    )
    val summed = agree(table, ColumnExpr.Add(0, 1), "sum")
    summed.head.fields.last.kind shouldBe ColumnKind.Decimal(5, 0)
    val expected = Vector.tabulate(rows) { row =>
      if (present(row)) then Cell.Decimal(5, 0, left(row) + right(row)) else Cell.Null
    }
    summed.map(_.cells.last) shouldBe expected
    val product = agree(table, ColumnExpr.Mul(0, 1), "product")
    product.head.fields.last.kind shouldBe ColumnKind.Decimal(6, 0)
    product.map(_.cells.last) shouldBe Vector.tabulate(rows) { row =>
      if (present(row)) then Cell.Decimal(6, 0, left(row) * right(row)) else Cell.Null
    }
  }

  it should "keep a decimal payload through an int join" in {
    val left = DistCollection.ofColumns(
      Vector(
        WideInput.ints("k", Array(1, 2)),
        WideInput.decimals("m", 4, 2, Array(150L, 250L)),
      ),
      2,
    )
    val right = DistCollection.ofColumns(
      Vector(
        WideInput.ints("k", Array(2, 1)),
        WideInput.ints("q", Array(7, 8)),
      ),
      2,
    )
    val joined = collect(left.hashJoinColumns(right, 0, 0))
    joined.map(_.cells).sortBy {
      case Vector(Cell.Int32(key), _, _) => key
      case other => throw new IllegalArgumentException(s"joined row $other")
    } shouldBe List(
      Vector(Cell.Int32(1), Cell.Decimal(4, 2, 150L), Cell.Int32(8)),
      Vector(Cell.Int32(2), Cell.Decimal(4, 2, 250L), Cell.Int32(7)),
    )
  }

  it should "filter a decimal column against a literal" in {
    val input = WideInput.nullableDecimals(
      "m",
      4,
      2,
      Array(150L, 0L, 250L),
      Array(true, false, true),
    )
    val planned = DistCollection
      .ofColumns(Vector(input), 1)
      .filterColumn(0, ColumnCompare(CompareOp.Gt, ColumnLiteral.Decimal(4, 2, 200L)))
    val kept = collect(planned)
    kept.map(_.cells.head) shouldBe List(Cell.Decimal(4, 2, 250L))
  }

  "an ungrouped decimal sum" should "match a row fold and skip nulls" in {
    val values = Array.tabulate(rows)(row => (row % 1000).toLong)
    val present = Array.tabulate(rows)(row => row != 7)
    val expected = values.indices.foldLeft(0L) { (total, row) =>
      if (present(row)) then total + values(row) else total
    }
    val planned = DistCollection.ofColumns(
      Vector(WideInput.nullableDecimals("a", 4, 0, values, present)),
      4,
    )
    sameSum(planned, 0, expected)
  }

  it should "return zero for an empty column" in {
    val planned = DistCollection.ofColumns(
      Vector(WideInput.decimals("a", 4, 2, Array.emptyLongArray)),
      1,
    )
    sameSum(planned, 0, 0L)
  }

  it should "refuse a total that needs a 19th digit" in {
    val values = Array(600000000000000000L, 600000000000000000L)
    val planned = DistCollection.ofColumns(
      Vector(WideInput.decimals("a", 18, 0, values)),
      1,
    )
    val columnar = the[IllegalArgumentException] thrownBy planned.sumDecimalColumn(0)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rowsThrown = the[IllegalArgumentException] thrownBy planned.sumDecimalColumn(0)
    columnar.getMessage shouldBe "decimal value does not fit precision 18"
    rowsThrown.getMessage shouldBe "decimal value does not fit precision 18"
  }

  "a decimal that does not fit" should "refuse a 19th digit and a result past 18" in {
    val stored = the[IllegalArgumentException] thrownBy {
      WideInput.decimals("a", 18, 0, Array(DecimalRules.pow10(18)))
    }
    stored.getMessage shouldBe "decimal value does not fit precision 18"
    val kind = the[IllegalArgumentException] thrownBy {
      DecimalRules.add(1L, 18, 0, 1L, 18, 0)
    }
    kind.getMessage shouldBe "decimal precision exceeds 18"
    val table = WideColumns(
      Vector(
        WideInput.decimals("a", 18, 0, Array(1L)),
        WideInput.decimals("b", 18, 0, Array(1L)),
      ),
    )
    val rowThrown = the[IllegalArgumentException] thrownBy {
      table.compute(ColumnExpr.Add(0, 1), "sum")
    }
    val batchThrown = the[IllegalArgumentException] thrownBy {
      WideExpr.append(WideBatch.encode(table), table.fields, ColumnExpr.Add(0, 1), "sum")
    }
    rowThrown.getMessage should include("does not apply")
    batchThrown.getMessage should include("does not apply")
  }

  private def agree(table: WideColumns, expr: ColumnExpr, name: String): Vector[WideRow] =
    val computed = table.compute(expr, name).rows
    val (fields, batch) = WideExpr.append(WideBatch.encode(table), table.fields, expr, name)
    WideBatch.decode(batch, fields) shouldBe computed
    computed

  private def sameSum(planned: DistCollection[WideRow], ordinal: Int, expected: Long): Unit =
    val columnar = planned.sumDecimalColumn(ordinal)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val folded = planned.sumDecimalColumn(ordinal)
    columnar shouldBe expected
    folded shouldBe expected

  private def collect(planned: DistCollection[WideRow]): List[WideRow] =
    val columnar = planned.collect().toList
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val folded = planned.collect().toList
    columnar shouldBe folded
    columnar
