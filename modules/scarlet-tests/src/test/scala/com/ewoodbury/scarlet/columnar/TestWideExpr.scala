package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.*

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
    rows.map(_.cells.last) shouldBe Vector(Cell.Int64(20L), Cell.Null, Cell.Int64(20L))
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
    rows.map(_.cells.last) shouldBe Vector(Cell.Int64(Int.MaxValue.toLong + 1L))
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
    rows.head.fields.last.kind shouldBe ColumnKind.Float32
    rows.map(_.cells.last) shouldBe Vector(Cell.Float32(1.5f))
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
    rows.map(_.cells.last) shouldBe Vector(Cell.Bool(false), Cell.Null, Cell.Bool(true))
  }

  it should "keep null out of bool and, or, and not" in {
    val table = WideColumns(
      Vector(
        WideInput.nullableBools("a", Array(true, false, true), Array(true, true, false)),
        WideInput.bools("b", Array(false, true, true)),
      ),
    )
    agree(table, ColumnExpr.Or(0, 1), "or").map(_.cells.last) shouldBe Vector(
      Cell.Bool(true),
      Cell.Bool(true),
      Cell.Null,
    )
    agree(table, ColumnExpr.And(0, 1), "and").map(_.cells.last) shouldBe Vector(
      Cell.Bool(false),
      Cell.Bool(false),
      Cell.Null,
    )
    agree(table, ColumnExpr.Not(0), "not").map(_.cells.last) shouldBe Vector(
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
    agree(table, ColumnExpr.Cast(0, ColumnKind.Int32), "n").map(_.cells.last) shouldBe Vector(
      Cell.Int32(5),
    )
    agree(table, ColumnExpr.Cast(1, ColumnKind.Int32), "n").map(_.cells.last) shouldBe Vector(
      Cell.Int32(1),
    )
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
    val kind = rows.head.fields.last.kind
    rows.foreach(row => row.fields.last shouldBe ColumnField(name, kind))
    rows
