package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.support.Check

/**
 * String order, `LIKE`, and length on wide rows.
 *
 * Dictionary codes are per batch, so a min taken from codes would keep the first string. The batch
 * path and the row path both compare JVM strings. `"é"` and `"e\u0301"` stay distinct.
 */
class TestStringText extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 1,
        threadPoolSize = 2,
        defaultShufflePartitions = 2,
        spillEnabled = false,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "a string min" should "order strings that share a dictionary code" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.strings("k", Array("k", "k")),
          WideInput.strings("u", Array("b", "a")),
        ),
        2,
      )
      .groupColumns(0, 1)
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideGroup(Scan)")
    cells(both(planned)) shouldBe cellsOf(
      List(Vector(Cell.Utf8("k"), Cell.Int64(2L), Cell.Utf8("a"), Cell.Utf8("b"))),
    )
  }

  it should "min an int key across batches" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("k", Array(1, 1, 2)),
          WideInput.strings("u", Array("b", "a", "c")),
        ),
        2,
      )
      .groupColumns(0, 1)
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(Cell.Int32(1), Cell.Int64(2L), Cell.Utf8("a"), Cell.Utf8("b")),
        Vector(Cell.Int32(2), Cell.Int64(1L), Cell.Utf8("c"), Cell.Utf8("c")),
      ),
    )
  }

  it should "min a two-key group by the string" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("k", Array(1, 1)),
          WideInput.strings("s", Array("p", "p")),
          WideInput.strings("u", Array("b", "a")),
        ),
        2,
      )
      .groupColumns(0, 1, 2)
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Int32(1),
          Cell.Utf8("p"),
          Cell.Int64(2L),
          Cell.Utf8("a"),
          Cell.Utf8("b"),
        ),
      ),
    )
  }

  it should "emit null min and max when every measure is null" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.strings("k", Array("k", "k")),
          WideInput.nullableStrings("u", Array("b", "a"), Array(false, false)),
        ),
        2,
      )
      .groupColumns(0, 1)
    cells(both(planned)) shouldBe cellsOf(
      List(Vector(Cell.Utf8("k"), Cell.Int64(2L), Cell.Null, Cell.Null)),
    )
  }

  it should "keep one present string beside a null measure" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.strings("k", Array("k", "k")),
          WideInput.nullableStrings("u", Array("a", "b"), Array(true, false)),
        ),
        2,
      )
      .groupColumns(0, 1)
    cells(both(planned)) shouldBe cellsOf(
      List(Vector(Cell.Utf8("k"), Cell.Int64(2L), Cell.Utf8("a"), Cell.Utf8("a"))),
    )
  }

  "length" should "count UTF-16 code units and keep a null" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.nullableStrings(
            "s",
            Array("é", "e\u0301", "", "x"),
            Array(true, true, true, false),
          ),
        ),
        2,
      )
      .compute(ColumnExpr.Len(0), "n")
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideCompute(Scan)")
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(Cell.Utf8("é"), Cell.Int32(1)),
        Vector(Cell.Utf8("e\u0301"), Cell.Int32(2)),
        Vector(Cell.Utf8(""), Cell.Int32(0)),
        Vector(Cell.Null, Cell.Null),
      ),
    )
  }

  "like" should "match wildcards and drop a null" in {
    val planned = texts.filterColumn(1, ColumnCompare(CompareOp.Like, ColumnLiteral.Utf8("%")))
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideFilter1(Scan)")
    idsOf(planned) shouldBe Vector(1, 2, 3, 4, 5, 6, 7)
    idsOf(texts.filterColumn(1, ColumnCompare(CompareOp.Like, ColumnLiteral.Utf8("_")))) shouldBe
      Vector(6)
    idsOf(texts.filterColumn(1, ColumnCompare(CompareOp.Like, ColumnLiteral.Utf8("a%c")))) shouldBe
      Vector(2, 3)
    idsOf(
      texts.filterColumn(1, ColumnCompare(CompareOp.Like, ColumnLiteral.Utf8("%google%"))),
    ) shouldBe Vector(4)
    idsOf(texts.filterColumn(1, ColumnCompare(CompareOp.Like, ColumnLiteral.Utf8("é")))) shouldBe
      Vector(6)
  }

  "not like" should "drop a null and a match" in {
    idsOf(
      texts.filterColumn(1, ColumnCompare(CompareOp.Unlike, ColumnLiteral.Utf8("%google%"))),
    ) shouldBe Vector(1, 2, 3, 5, 6, 7)
    idsOf(texts.filterColumn(1, ColumnCompare(CompareOp.Unlike, ColumnLiteral.Utf8("%")))) shouldBe
      Vector.empty[Int]
  }

  private val texts: DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.ints("id", Array(1, 2, 3, 4, 5, 6, 7, 8)),
        WideInput.nullableStrings(
          "s",
          Array("", "ac", "axc", "agoogle", "Google", "é", "e\u0301", "x"),
          Array(true, true, true, true, true, true, true, false),
        ),
      ),
      2,
    )

  private def idsOf(planned: DistCollection[WideRow]): Vector[Int] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = ids(planned)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = ids(planned)
    columnar shouldBe rows
    columnar

  private def ids(planned: DistCollection[WideRow]): Vector[Int] =
    planned
      .collect()
      .map(row => Check.first(row.cells))
      .collect { case Cell.Int32(value) => value }
      .toVector

  private def both(planned: DistCollection[WideRow]): List[WideRow] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = planned.collect().toList
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = planned.collect().toList
    cells(columnar) shouldBe cells(rows)
    columnar

  private def cells(rows: Seq[WideRow]): List[Vector[Cell]] =
    rows.map(_.cells).toList.sortBy(_.toString)

  private def cellsOf(rows: List[Vector[Cell]]): List[Vector[Cell]] =
    rows.sortBy(_.toString)
