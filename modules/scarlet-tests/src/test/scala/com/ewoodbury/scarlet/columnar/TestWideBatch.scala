package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.core.{
  Cell,
  ColumnCompare,
  ColumnLiteral,
  CompareOp,
  WideColumns,
  WideInput,
  WideRow,
}

/**
 * Wide-row filter and project against the row fold.
 *
 * A null in the predicate column is dropped. A null in another column stays. Project drops,
 * reorders, or renames and does not compute.
 */
class TestWideBatch extends AnyFlatSpec with Matchers:

  private val greater = ColumnCompare(CompareOp.Gt, ColumnLiteral.Int32(1))

  "a wide row" should "filter an int column and drop another" in {
    val table = sample()
    val filtered = table.filter(0, greater)
    filtered.rows shouldBe Vector(
      row(filtered, Cell.Int32(4), Cell.Null, Cell.Float64(3.5)),
      row(filtered, Cell.Int32(2), Cell.Int64(40L), Cell.Float64(4.5)),
    )
    val projected = filtered.project(Vector((0, "n"), (2, "amount")))
    projected.fields.map(_.name) shouldBe Vector("n", "amount")
    projected.rows shouldBe Vector(
      row(projected, Cell.Int32(4), Cell.Float64(3.5)),
      row(projected, Cell.Int32(2), Cell.Float64(4.5)),
    )
    same(table, 0, greater, Vector((0, "n"), (2, "amount")))
  }

  it should "keep a null in a column the filter does not read" in {
    same(sample(), 0, greater, Vector((0, "n"), (1, "k")))
  }

  it should "compare a long and a double" in {
    val table = sample()
    same(
      table,
      1,
      ColumnCompare(CompareOp.Gt, ColumnLiteral.Int64(30L)),
      Vector((0, "n"), (1, "k")),
    )
    same(
      table,
      2,
      ColumnCompare(CompareOp.Ge, ColumnLiteral.Float64(3.5)),
      Vector((2, "x"), (0, "n")),
    )
  }

  it should "compare a bool and round-trip the other kinds" in {
    val flags = WideColumns(
      Vector(
        WideInput.nullableBools("b", Array(true, false, true), Array(true, false, true)),
        WideInput.nullableStrings("s", Array("a", "b", "c"), Array(true, true, false)),
        WideInput.nullableFloats("f", Array(1.0f, 2.0f, 3.0f), Array(true, false, true)),
      ),
    )
    roundTrip(flags)
    same(
      flags,
      0,
      ColumnCompare(CompareOp.Eq, ColumnLiteral.Bool(true)),
      Vector((0, "b"), (1, "s")),
    )
  }

  it should "drop every row when nothing passes" in {
    val table = sample()
    val compare = ColumnCompare(CompareOp.Gt, ColumnLiteral.Int32(100))
    val filtered = table.filter(0, compare)
    filtered.length shouldBe 0
    val decoded = WideBatch.decode(
      WideBatch.filter(WideBatch.encode(table), 0, compare),
      filtered.fields,
    )
    decoded shouldBe Vector.empty[WideRow]
  }

  it should "refuse a compare the column does not support" in {
    val table = sample()
    an[IllegalArgumentException] should be thrownBy {
      table.filter(2, greater)
    }
    an[IllegalArgumentException] should be thrownBy {
      WideBatch.filter(WideBatch.encode(table), 2, greater)
    }
  }

  private def same(
      table: WideColumns,
      ordinal: Int,
      compare: ColumnCompare,
      spec: Vector[(Int, String)],
  ): Unit =
    val expected = table.filter(ordinal, compare).project(spec)
    val batch = WideBatch.project(
      WideBatch.filter(WideBatch.encode(table), ordinal, compare),
      spec.map(_._1),
    )
    WideBatch.decode(batch, expected.fields) shouldBe expected.rows

  private def roundTrip(table: WideColumns): Unit =
    WideBatch.decode(WideBatch.encode(table), table.fields) shouldBe table.rows

  private def sample(): WideColumns =
    WideColumns(
      Vector(
        WideInput.nullableInts("n", Array(1, 0, 4, 2, 0), Array(true, false, true, true, true)),
        WideInput.nullableLongs(
          "k",
          Array(10L, 20L, 30L, 40L, 50L),
          Array(true, true, false, true, true),
        ),
        WideInput.doubles("x", Array(1.5, 2.5, 3.5, 4.5, 5.5)),
      ),
    )

  private def row(table: WideColumns, cells: Cell*): WideRow =
    val values = cells.toVector
    require(values.size == table.fields.size, s"${values.size} cells for ${table.fields.size}")
    WideRow(table.fields, values)
