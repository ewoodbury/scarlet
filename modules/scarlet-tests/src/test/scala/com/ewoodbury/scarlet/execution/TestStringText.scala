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

  it should "drop a null string key" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.nullableStrings("k", Array("k", "z", "k"), Array(true, false, true)),
          WideInput.strings("u", Array("b", "z", "a")),
        ),
        2,
      )
      .groupColumns(0, 1)
    cells(both(planned)) shouldBe cellsOf(
      List(Vector(Cell.Utf8("k"), Cell.Int64(2L), Cell.Utf8("a"), Cell.Utf8("b"))),
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

  it should "append the min of a second string" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.strings("k", Array("a", "a", "a", "b")),
          WideInput
            .nullableStrings("u", Array("z", "a", "m", "q"), Array(true, true, false, true)),
          WideInput.nullableStrings(
            "t",
            Array("é", "e\u0301", "m", "z"),
            Array(true, true, false, false),
          ),
          WideInput.longs("id", Array(1L, 2L, 1L, 9L)),
        ),
        2,
      )
      .groupStringMin(0, 1, 2, 3)
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideGroup(Scan)")
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Utf8("a"),
          Cell.Int64(3L),
          Cell.Utf8("a"),
          Cell.Utf8("z"),
          Cell.Utf8("e\u0301"),
          Cell.Int64(2L),
        ),
        Vector(
          Cell.Utf8("b"),
          Cell.Int64(1L),
          Cell.Utf8("q"),
          Cell.Utf8("q"),
          Cell.Null,
          Cell.Int64(1L),
        ),
      ),
    )
  }

  it should "keep the second min when the table grows" in {
    val count = 12
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.strings("k", Array.tabulate(count)(i => "k" + i.toString)),
          WideInput.strings("u", Array.tabulate(count)(i => "u" + i.toString)),
          WideInput.strings("t", Array.tabulate(count)(i => "t" + i.toString)),
        ),
        2,
      )
      .groupStringMin(0, 1, 2)
    cells(both(planned)) shouldBe cellsOf(
      List.tabulate(count) { i =>
        Vector(
          Cell.Utf8("k" + i.toString),
          Cell.Int64(1L),
          Cell.Utf8("u" + i.toString),
          Cell.Utf8("u" + i.toString),
          Cell.Utf8("t" + i.toString),
        )
      },
    )
  }

  it should "refuse a second string min of a number" in {
    val planned = DistCollection.ofColumns(
      Vector(
        WideInput.strings("k", Array("a")),
        WideInput.ints("q", Array(1)),
        WideInput.strings("t", Array("b")),
      ),
      2,
    )
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = planned.groupStringMin(0, 1, 2)
    }
    thrown.getMessage should include("string measure")
    val same = the[IllegalArgumentException] thrownBy {
      val _ = planned.groupStringMin(0, 2, 2)
    }
    same.getMessage should include("another column")
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

  "a host replace" should "keep the host and leave another string" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.nullableStrings(
            "s",
            Array(
              "https://www.example.com/a",
              "http://example.com/b",
              "https://example.com/c",
              "bare",
              "http://example.com",
              "https://www./a",
              "https://www./",
              "http://www./x",
              "https://www.//a",
              "https://example.com/a\nb",
              "https://www.example.com/a\nb",
              "https://example.com/a\n",
              "x",
            ),
            Array(true, true, true, true, true, true, true, true, true, true, true, true, false),
          ),
        ),
        2,
      )
      .compute(ColumnExpr.Host(0), "k")
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideCompute(Scan)")
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(Cell.Utf8("https://www.example.com/a"), Cell.Utf8("example.com")),
        Vector(Cell.Utf8("http://example.com/b"), Cell.Utf8("example.com")),
        Vector(Cell.Utf8("https://example.com/c"), Cell.Utf8("example.com")),
        Vector(Cell.Utf8("bare"), Cell.Utf8("bare")),
        Vector(Cell.Utf8("http://example.com"), Cell.Utf8("http://example.com")),
        Vector(Cell.Utf8("https://www./a"), Cell.Utf8("www.")),
        Vector(Cell.Utf8("https://www./"), Cell.Utf8("www.")),
        Vector(Cell.Utf8("http://www./x"), Cell.Utf8("www.")),
        Vector(Cell.Utf8("https://www.//a"), Cell.Utf8("www.")),
        Vector(Cell.Utf8("https://example.com/a\nb"), Cell.Utf8("https://example.com/a\nb")),
        Vector(
          Cell.Utf8("https://www.example.com/a\nb"),
          Cell.Utf8("https://www.example.com/a\nb"),
        ),
        Vector(Cell.Utf8("https://example.com/a\n"), Cell.Utf8("https://example.com/a\n")),
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

  it should "treat a percent sign in the text as data" in {
    idsOf(
      percents.filterColumn(1, ColumnCompare(CompareOp.Like, ColumnLiteral.Utf8("%a"))),
    ) shouldBe
      Vector(1)
    idsOf(
      percents.filterColumn(1, ColumnCompare(CompareOp.Like, ColumnLiteral.Utf8("%bc"))),
    ) shouldBe Vector(2)
    idsOf(
      percents.filterColumn(1, ColumnCompare(CompareOp.Like, ColumnLiteral.Utf8("%"))),
    ) shouldBe
      Vector(1, 2, 3, 4)
    idsOf(
      percents.filterColumn(1, ColumnCompare(CompareOp.Like, ColumnLiteral.Utf8("a%"))),
    ) shouldBe
      Vector(4)
  }

  "string order" should "agree with compareTo" in {
    val samples = Vector("", "a", "b", "B", "é", "e\u0301")
    samples.foreach { left =>
      samples.foreach { right =>
        TextRule.compare(left, right) shouldBe left.compareTo(right)
      }
    }
    val planned = DistCollection
      .ofColumns(Vector(WideInput.strings("s", samples.toArray)), 2)
      .sortColumn(0, descending = false)
    textsOf(planned) shouldBe samples.sortWith((left, right) => left.compareTo(right) < 0)
    val reversed = planned.sortColumn(0, descending = true)
    textsOf(reversed) shouldBe samples.sortWith((left, right) => left.compareTo(right) > 0)
  }

  "like" should "agree with LikeMatch" in {
    val pattern = "a_c%"
    val rows = Vector("ac", "axc", "abc", "a", "abbc", "é", "")
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("id", rows.indices.map(_ + 1).toArray),
          WideInput.strings("s", rows.toArray),
        ),
        2,
      )
      .filterColumn(1, ColumnCompare(CompareOp.Like, ColumnLiteral.Utf8(pattern)))
    idsOf(planned) shouldBe rows.zipWithIndex.collect {
      case (text, index) if LikeMatch.matches(text, pattern) => index + 1
    }
  }

  "not like" should "drop a null and a match" in {
    idsOf(
      texts.filterColumn(1, ColumnCompare(CompareOp.Unlike, ColumnLiteral.Utf8("%google%"))),
    ) shouldBe Vector(1, 2, 3, 5, 6, 7)
    idsOf(texts.filterColumn(1, ColumnCompare(CompareOp.Unlike, ColumnLiteral.Utf8("%")))) shouldBe
      Vector.empty[Int]
  }

  private val percents: DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.ints("id", Array(1, 2, 3, 4, 5)),
        WideInput.nullableStrings(
          "s",
          Array("%xa", "%abc", "%x", "a%", "x"),
          Array(true, true, true, true, false),
        ),
      ),
      2,
    )

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

  private def textsOf(planned: DistCollection[WideRow]): Vector[String] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = textsIn(planned)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = textsIn(planned)
    columnar shouldBe rows
    columnar

  private def textsIn(planned: DistCollection[WideRow]): Vector[String] =
    planned
      .collect()
      .map(row => Check.first(row.cells))
      .collect { case Cell.Utf8(value) =>
        value
      }
      .toVector

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
