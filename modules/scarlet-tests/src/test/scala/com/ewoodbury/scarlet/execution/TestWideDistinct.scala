package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.support.Check.shouldBeEmpty

/**
 * Exact distinct count. Repeats, nulls, an empty string, and combining characters keep the count
 * from being a copy of `count`. The batch path and the row path match.
 */
class TestWideDistinct extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 64,
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

  "an int column" should "skip nulls and collapse repeats" in {
    val table = DistCollection.ofColumns(
      Vector(
        WideInput.nullableInts(
          "n",
          Array(4, 4, 5, 4, 0),
          Array(true, true, true, true, false),
        ),
      ),
      2,
    )
    bothCounts(table, 0, 2L)
  }

  it should "keep Int.MinValue once" in {
    val table = DistCollection.ofColumns(
      Vector(WideInput.ints("n", Array(Int.MinValue, 1, Int.MinValue))),
      2,
    )
    bothCounts(table, 0, 2L)
  }

  it should "be zero when the column is empty or all null" in {
    val empty = DistCollection.ofColumns(Vector(WideInput.ints("n", Array.emptyIntArray)), 2)
    val absent = DistCollection.ofColumns(
      Vector(WideInput.nullableInts("n", Array(1, 2, 3), Array(false, false, false))),
      2,
    )
    bothCounts(empty, 0, 0L)
    bothCounts(absent, 0, 0L)
  }

  "a long column" should "keep values that share a hash prefix" in {
    val table = DistCollection.ofColumns(
      Vector(WideInput.longs("n", Array(1L, 1L, 1L << 32, 1L))),
      2,
    )
    bothCounts(table, 0, 2L)
  }

  "a string column" should "count an empty string and skip null" in {
    val table = DistCollection.ofColumns(
      Vector(
        WideInput.nullableStrings(
          "s",
          Array("", "", "a", ""),
          Array(true, false, true, true),
        ),
      ),
      2,
    )
    bothCounts(table, 0, 2L)
  }

  it should "keep a composed character apart from its combining form" in {
    val table = DistCollection.ofColumns(
      Vector(WideInput.strings("s", Array("é", "e\u0301", "é"))),
      2,
    )
    bothCounts(table, 0, 2L)
  }

  it should "use the string, not the dictionary code" in {
    val table = DistCollection.ofColumns(
      Vector(WideInput.strings("s", Array("a", "b", "c"))),
      2,
    )
    bothCounts(table, 0, 3L)
  }

  "a group" should "count a column other than the measure" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("k", Array(1, 1, 1, 2)),
          WideInput.ints("q", Array(10, 10, 10, 10)),
          WideInput.ints("d", Array(4, 4, 5, 4)),
        ),
        2,
      )
      .groupDistinct(0, 1, 2)
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideGroup(Scan)")
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Int32(1),
          Cell.Int64(3L),
          Cell.Int64(30L),
          Cell.Int32(10),
          Cell.Int32(10),
          Cell.Int64(2L),
        ),
        Vector(
          Cell.Int32(2),
          Cell.Int64(1L),
          Cell.Int64(10L),
          Cell.Int32(10),
          Cell.Int32(10),
          Cell.Int64(1L),
        ),
      ),
    )
  }

  it should "emit zero when every distinct value is null" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("k", Array(1, 1, 1)),
          WideInput.ints("q", Array(5, 6, 7)),
          WideInput.nullableInts("d", Array(1, 2, 3), Array(false, false, false)),
        ),
        2,
      )
      .groupDistinct(0, 1, 2)
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Int32(1),
          Cell.Int64(3L),
          Cell.Int64(18L),
          Cell.Int32(5),
          Cell.Int32(7),
          Cell.Int64(0L),
        ),
      ),
    )
  }

  it should "drop a null key and ignore that row's value" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.nullableInts("k", Array(0, 1, 1), Array(false, true, true)),
          WideInput.ints("q", Array(1, 2, 3)),
          WideInput.ints("d", Array(9, 4, 4)),
        ),
        2,
      )
      .groupDistinct(0, 1, 2)
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Int32(1),
          Cell.Int64(2L),
          Cell.Int64(5L),
          Cell.Int32(2),
          Cell.Int32(3),
          Cell.Int64(1L),
        ),
      ),
    )
  }

  it should "keep an empty string and a composed character" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.strings("k", Array("g", "g", "g", "g")),
          WideInput.ints("q", Array(1, 1, 1, 1)),
          WideInput.strings("d", Array("", "é", "e\u0301", "")),
        ),
        2,
      )
      .groupDistinct(0, 1, 2)
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Utf8("g"),
          Cell.Int64(4L),
          Cell.Int64(4L),
          Cell.Int32(1),
          Cell.Int32(1),
          Cell.Int64(3L),
        ),
      ),
    )
  }

  it should "union strings across partitions" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.strings("k", Array("g", "g", "g", "g")),
          WideInput.ints("q", Array(1, 2, 3, 4)),
          WideInput.strings("d", Array("a", "b", "b", "a")),
        ),
        2,
      )
      .groupDistinct(0, 1, 2)
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Utf8("g"),
          Cell.Int64(4L),
          Cell.Int64(10L),
          Cell.Int32(1),
          Cell.Int32(4),
          Cell.Int64(2L),
        ),
      ),
    )
  }

  it should "group a string key and a long" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.strings("k", Array("a", "a", "a", "b")),
          WideInput.longs("n", Array(1L, 1L, 2L, 2L)),
          WideInput.ints("q", Array(3, 4, 5, 6)),
        ),
        2,
      )
      .groupDistinct(0, 2, 1)
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Utf8("a"),
          Cell.Int64(3L),
          Cell.Int64(12L),
          Cell.Int32(3),
          Cell.Int32(5),
          Cell.Int64(2L),
        ),
        Vector(
          Cell.Utf8("b"),
          Cell.Int64(1L),
          Cell.Int64(6L),
          Cell.Int32(6),
          Cell.Int32(6),
          Cell.Int64(1L),
        ),
      ),
    )
  }

  it should "count distinct strings beside two keys" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("k", Array(1, 1, 1)),
          WideInput.strings("s", Array("a", "a", "b")),
          WideInput.ints("q", Array(3, 4, 5)),
          WideInput.longs("n", Array(7L, 8L, 7L)),
        ),
        2,
      )
      .groupDistinct(0, 1, 2, 3)
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideGroup(Scan)")
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Int32(1),
          Cell.Utf8("a"),
          Cell.Int64(2L),
          Cell.Int64(7L),
          Cell.Int32(3),
          Cell.Int32(4),
          Cell.Int64(2L),
        ),
        Vector(
          Cell.Int32(1),
          Cell.Utf8("b"),
          Cell.Int64(1L),
          Cell.Int64(5L),
          Cell.Int32(5),
          Cell.Int32(5),
          Cell.Int64(1L),
        ),
      ),
    )
  }

  "an old group" should "keep four aggregate columns" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("k", Array(1, 1)),
          WideInput.ints("q", Array(4, 5)),
        ),
        2,
      )
      .groupColumns(0, 1)
    val rows = both(planned)
    rows.foreach { row =>
      row.cells.size shouldBe 5
      row.fields.map(_.name) shouldBe Vector("k", "count", "sum", "min", "max")
    }
  }

  "a float" should "be refused" in {
    val table = DistCollection.ofColumns(
      Vector(
        WideInput.ints("k", Array(1, 1)),
        WideInput.doubles("q", Array(1.0, 2.0)),
      ),
      2,
    )
    val grouped = the[IllegalArgumentException] thrownBy {
      val _ = table.groupDistinct(0, 1, 1)
    }
    grouped.getMessage should include("distinct of Float64 is not supported")
    val counted = the[IllegalArgumentException] thrownBy {
      val _ = table.countDistinctColumn(1)
    }
    counted.getMessage should include("distinct of Float64 is not supported")
  }

  "a bool" should "be refused" in {
    val table = DistCollection.ofColumns(Vector(WideInput.bools("b", Array(true, false))), 2)
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = table.countDistinctColumn(0)
    }
    thrown.getMessage should include("distinct of Bool is not supported")
  }

  "a decimal" should "be refused" in {
    val table = DistCollection.ofColumns(
      Vector(WideInput.decimals("d", 4, 0, Array(1L, 2L))),
      2,
    )
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = table.countDistinctColumn(0)
    }
    thrown.getMessage should include("distinct of Decimal")
  }

  private def bothCounts(table: DistCollection[WideRow], ordinal: Int, expected: Long): Unit =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    WideDistinctCount.count(table.plan, ordinal) shouldBe Some(expected)
    table.countDistinctColumn(ordinal) shouldBe expected
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    WideDistinctCount.count(table.plan, ordinal).shouldBeEmpty
    table.countDistinctColumn(ordinal) shouldBe expected

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
