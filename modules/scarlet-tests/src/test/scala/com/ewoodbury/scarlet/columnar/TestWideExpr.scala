package com.ewoodbury.scarlet.columnar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.support.Check

/** Row fold and batch evaluation of one wide expression. */
class TestWideExpr extends AnyFlatSpec with Matchers:

  "a wide expression" should "multiply longs and keep a null factor null" in {
    val rows = agree(
      WideColumns(
        Vector(
          WideInput.nullableLongs("price", Array(10L, 0L, 4L), Array(true, false, true)),
          WideInput.longs("discount", Array(2L, 3L, 5L)),
        ),
      ),
      ColumnExpr.Mul(0, 1),
      "revenue",
    )
    rows.map(row => Check.last(row.cells)) shouldBe Vector(
      Cell.Int64(20L),
      Cell.Null,
      Cell.Int64(20L),
    )
  }

  it should "add an int and a long as a long" in {
    val rows = agree(
      WideColumns(
        Vector(
          WideInput.ints("n", Array(Int.MaxValue)),
          WideInput.longs("k", Array(1L)),
        ),
      ),
      ColumnExpr.Add(0, 1),
      "sum",
    )
    rows.map(row => Check.last(row.cells)) shouldBe Vector(Cell.Int64(Int.MaxValue.toLong + 1L))
  }

  it should "add a long and a float as a float" in {
    val rows = agree(
      WideColumns(
        Vector(
          WideInput.longs("k", Array(1L)),
          WideInput.floats("f", Array(0.5f)),
        ),
      ),
      ColumnExpr.Add(0, 1),
      "sum",
    )
    Check.last(Check.first(rows).fields).kind shouldBe ColumnKind.Float32
    rows.map(row => Check.last(row.cells)) shouldBe Vector(Cell.Float32(1.5f))
  }

  it should "compare an int with a long" in {
    val rows = agree(
      WideColumns(
        Vector(
          WideInput.nullableInts("n", Array(1, 0, 4), Array(true, false, true)),
          WideInput.longs("k", Array(1L, 2L, 3L)),
        ),
      ),
      ColumnExpr.Compare(CompareOp.Gt, 0, 1),
      "keep",
    )
    rows.map(row => Check.last(row.cells)) shouldBe Vector(
      Cell.Bool(false),
      Cell.Null,
      Cell.Bool(true),
    )
  }

  it should "keep null out of bool and, or, and not" in {
    val table = WideColumns(
      Vector(
        WideInput.nullableBools("a", Array(true, false, true), Array(true, true, false)),
        WideInput.bools("b", Array(false, true, true)),
      ),
    )
    agree(table, ColumnExpr.Or(0, 1), "or").map(row => Check.last(row.cells)) shouldBe Vector(
      Cell.Bool(true),
      Cell.Bool(true),
      Cell.Null,
    )
    agree(table, ColumnExpr.And(0, 1), "and").map(row => Check.last(row.cells)) shouldBe Vector(
      Cell.Bool(false),
      Cell.Bool(false),
      Cell.Null,
    )
    agree(table, ColumnExpr.Not(0), "not").map(row => Check.last(row.cells)) shouldBe Vector(
      Cell.Bool(false),
      Cell.Bool(true),
      Cell.Null,
    )
  }

  it should "cast with the same conversions on both paths" in {
    val table = WideColumns(
      Vector(
        WideInput.longs("k", Array(5L)),
        WideInput.doubles("x", Array(1.9)),
      ),
    )
    agree(table, ColumnExpr.Cast(0, ColumnKind.Int32), "n")
      .map(row => Check.last(row.cells)) shouldBe Vector(
      Cell.Int32(5),
    )
    agree(table, ColumnExpr.Cast(1, ColumnKind.Int32), "n")
      .map(row => Check.last(row.cells)) shouldBe Vector(
      Cell.Int32(1),
    )
  }

  it should "append a literal on both paths" in {
    val base = WideColumns(Vector(WideInput.ints("n", Array(1))))
    agree(base, ColumnExpr.Lit(ColumnLiteral.Int32(11)), "k")
      .map(row => Check.last(row.cells)) shouldBe Vector(
      Cell.Int32(11),
    )
    agree(base, ColumnExpr.Lit(ColumnLiteral.Int64(11L)), "k")
      .map(row => Check.last(row.cells)) shouldBe Vector(
      Cell.Int64(11L),
    )
    agree(base, ColumnExpr.Lit(ColumnLiteral.Float64(1.5)), "k")
      .map(row => Check.last(row.cells)) shouldBe Vector(
      Cell.Float64(1.5),
    )
    agree(base, ColumnExpr.Lit(ColumnLiteral.Bool(false)), "k")
      .map(row => Check.last(row.cells)) shouldBe Vector(
      Cell.Bool(false),
    )
  }

  it should "add a literal to an int on both paths" in {
    val base = WideColumns(Vector(WideInput.ints("n", Array(1, 2))))
    val lit = ColumnExpr.Lit(ColumnLiteral.Int32(11))
    val rowLit = base.compute(lit, "eleven")
    val (fields, batch) = WideExpr.append(WideBatch.encode(base), base.fields, lit, "eleven")
    WideBatch.decode(batch, fields) shouldBe rowLit.rows
    val summed = rowLit.compute(ColumnExpr.Add(0, 1), "sum")
    val (sumFields, sumBatch) = WideExpr.append(batch, fields, ColumnExpr.Add(0, 1), "sum")
    WideBatch.decode(sumBatch, sumFields) shouldBe summed.rows
    summed.rows.map(row => Check.last(row.cells)) shouldBe Vector(Cell.Int32(12), Cell.Int32(13))
  }

  it should "divide integers toward zero" in {
    val rows = agree(
      WideColumns(
        Vector(
          WideInput.ints("n", Array(-5, 5, 7)),
          WideInput.ints("d", Array(2, -2, 2)),
        ),
      ),
      ColumnExpr.Div(0, 1),
      "q",
    )
    rows.map(row => Check.last(row.cells)) shouldBe Vector(
      Cell.Int32(-2),
      Cell.Int32(-2),
      Cell.Int32(3),
    )
    val longs = agree(
      WideColumns(
        Vector(
          WideInput.longs("n", Array(-5L)),
          WideInput.longs("d", Array(2L)),
        ),
      ),
      ColumnExpr.Div(0, 1),
      "q",
    )
    longs.map(row => Check.last(row.cells)) shouldBe Vector(Cell.Int64(-2L))
  }

  it should "leave a null divisor null" in {
    val rows = agree(
      WideColumns(
        Vector(
          WideInput.ints("n", Array(5, 5)),
          WideInput.nullableInts("d", Array(0, 2), Array(false, true)),
        ),
      ),
      ColumnExpr.Div(0, 1),
      "q",
    )
    rows.map(row => Check.last(row.cells)) shouldBe Vector(Cell.Null, Cell.Int32(2))
  }

  it should "divide a double" in {
    val rows = agree(
      WideColumns(
        Vector(
          WideInput.doubles("n", Array(1.5)),
          WideInput.doubles("d", Array(0.5)),
        ),
      ),
      ColumnExpr.Div(0, 1),
      "q",
    )
    Check.last(Check.first(rows).fields).kind shouldBe ColumnKind.Float64
    rows.map(row => Check.last(row.cells)) shouldBe Vector(Cell.Float64(3.0))
  }

  it should "refuse a present zero divisor" in {
    val table = WideColumns(
      Vector(
        WideInput.ints("n", Array(8, 5)),
        WideInput.ints("d", Array(2, 0)),
      ),
    )
    val rowThrown = the[IllegalArgumentException] thrownBy {
      table.compute(ColumnExpr.Div(0, 1), "q")
    }
    val batchThrown = the[IllegalArgumentException] thrownBy {
      WideExpr.append(WideBatch.encode(table), table.fields, ColumnExpr.Div(0, 1), "q")
    }
    rowThrown.getMessage shouldBe "divide by zero"
    batchThrown.getMessage shouldBe "divide by zero"
    val negativeZero = WideColumns(
      Vector(
        WideInput.doubles("n", Array(1.0)),
        WideInput.doubles("d", Array(-0.0)),
      ),
    )
    val floatThrown = the[IllegalArgumentException] thrownBy {
      negativeZero.compute(ColumnExpr.Div(0, 1), "q")
    }
    floatThrown.getMessage shouldBe "divide by zero"
  }

  it should "reject a string factor" in {
    val table = WideColumns(
      Vector(
        WideInput.longs("k", Array(1L)),
        WideInput.strings("s", Array("a")),
      ),
    )
    an[IllegalArgumentException] should be thrownBy {
      table.compute(ColumnExpr.Mul(0, 1), "bad")
    }
    an[IllegalArgumentException] should be thrownBy {
      WideExpr.append(WideBatch.encode(table), table.fields, ColumnExpr.Mul(0, 1), "bad")
    }
  }

  private def agree(table: WideColumns, expr: ColumnExpr, name: String): Vector[WideRow] =
    val rows = table.compute(expr, name).rows
    val (fields, batch) = WideExpr.append(WideBatch.encode(table), table.fields, expr, name)
    WideBatch.decode(batch, fields) shouldBe rows
    val kind = Check.last(Check.first(rows).fields).kind
    rows.foreach(row => Check.last(row.fields) shouldBe ColumnField(name, kind))
    rows
