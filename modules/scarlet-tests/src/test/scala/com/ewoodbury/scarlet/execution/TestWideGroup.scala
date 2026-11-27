package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Group one int or long column of a wide row.
 *
 * Count, sum, min, and max run in one pass. A null key is dropped. A null measure is counted and
 * skipped by sum, min, and max. The plan is not `GroupByKey`. Spill stays off.
 */
class TestWideGroup extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

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

  "a wide int group" should "sum a second column without GroupByKey" in {
    val planned = table(
      WideInput.nullableInts(
        "k",
        Array(1, 0, 1, 2, 2, 3),
        Array(true, false, true, true, true, true),
      ),
      WideInput.ints("q", Array(10, 99, 1, 4, 5, 7)),
    ).groupColumns(0, 1)
    planned.plan match
      case _: Plan.WideGroupOp => succeed
      case other => fail(s"plan was $other")
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideGroup(Scan)")
    planned.count() shouldBe 3L
    val fields = Vector(
      ColumnField("k", ColumnKind.Int32),
      ColumnField("count", ColumnKind.Int64),
      ColumnField("sum", ColumnKind.Int64),
      ColumnField("min", ColumnKind.Int32),
      ColumnField("max", ColumnKind.Int32),
    )
    val rows = both(planned)
    rows.map(_.fields).distinct shouldBe List(fields)
    cells(rows) shouldBe cellsOf(
      List(
        Vector(Cell.Int32(1), Cell.Int64(2L), Cell.Int64(11L), Cell.Int32(1), Cell.Int32(10)),
        Vector(Cell.Int32(2), Cell.Int64(2L), Cell.Int64(9L), Cell.Int32(4), Cell.Int32(5)),
        Vector(Cell.Int32(3), Cell.Int64(1L), Cell.Int64(7L), Cell.Int32(7), Cell.Int32(7)),
      ),
    )
  }

  it should "count a null measure and skip it in the sum" in {
    val planned = table(
      WideInput.ints("k", Array(1, 1, 1)),
      WideInput.nullableInts("q", Array(5, 0, 0), Array(true, false, false)),
    ).groupColumns(0, 1)
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(Cell.Int32(1), Cell.Int64(3L), Cell.Int64(5L), Cell.Int32(5), Cell.Int32(5)),
      ),
    )
  }

  it should "keep a group whose measures are all null" in {
    val planned = table(
      WideInput.ints("k", Array(4, 4)),
      WideInput.nullableInts("q", Array(0, 0), Array(false, false)),
    ).groupColumns(0, 1)
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(Cell.Int32(4), Cell.Int64(2L), Cell.Null, Cell.Null, Cell.Null),
      ),
    )
  }

  it should "widen an int sum past Int.MaxValue" in {
    val planned = table(
      WideInput.ints("k", Array(1, 1)),
      WideInput.ints("q", Array(Int.MaxValue, 2)),
    ).groupColumns(0, 1)
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Int32(1),
          Cell.Int64(2L),
          Cell.Int64(Int.MaxValue.toLong + 2L),
          Cell.Int32(2),
          Cell.Int32(Int.MaxValue),
        ),
      ),
    )
  }

  it should "merge more groups than the first hash table" in {
    val keys = Array.tabulate(40)(row => row % 20)
    val values = Array.fill(40)(1)
    val planned = table(WideInput.ints("k", keys), WideInput.ints("q", values)).groupColumns(0, 1)
    val expected = (0 until 20).toList.map { key =>
      Vector(Cell.Int32(key), Cell.Int64(2L), Cell.Int64(2L), Cell.Int32(1), Cell.Int32(1))
    }
    cells(both(planned)) shouldBe cellsOf(expected)
  }

  it should "group a long key and wrap a long sum" in {
    val planned = table(
      WideInput.nullableLongs("k", Array(1L, 0L, 1L), Array(true, false, true)),
      WideInput.longs("q", Array(Long.MaxValue, 5L, 1L)),
    ).groupColumns(0, 1)
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Int64(1L),
          Cell.Int64(2L),
          Cell.Int64(Long.MaxValue + 1L),
          Cell.Int64(1L),
          Cell.Int64(Long.MaxValue),
        ),
      ),
    )
  }

  it should "sum a double and a float" in {
    val doubles = table(
      WideInput.ints("k", Array(1, 1)),
      WideInput.doubles("q", Array(1.5, 2.5)),
    ).groupColumns(0, 1)
    cells(both(doubles)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Int32(1),
          Cell.Int64(2L),
          Cell.Float64(4.0),
          Cell.Float64(1.5),
          Cell.Float64(2.5),
        ),
      ),
    )
    val floats = table(
      WideInput.longs("k", Array(8L, 8L)),
      WideInput.nullableFloats("q", Array(1.5f, 0.0f), Array(true, false)),
    ).groupColumns(0, 1)
    cells(both(floats)) shouldBe cellsOf(
      List(
        Vector(
          Cell.Int64(8L),
          Cell.Int64(2L),
          Cell.Float64(1.5),
          Cell.Float32(1.5f),
          Cell.Float32(1.5f),
        ),
      ),
    )
  }

  it should "group the rows a filter keeps" in {
    val planned = table(
      WideInput.ints("k", Array(-1, 1, 2)),
      WideInput.ints("q", Array(9, 3, 4)),
    ).filterColumn(0, ColumnCompare(CompareOp.Gt, ColumnLiteral.Int32(0)))
      .groupColumns(0, 1)
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideGroup(WideFilter0(Scan))")
    cells(both(planned)) shouldBe cellsOf(
      List(
        Vector(Cell.Int32(1), Cell.Int64(1L), Cell.Int64(3L), Cell.Int32(3), Cell.Int32(3)),
        Vector(Cell.Int32(2), Cell.Int64(1L), Cell.Int64(4L), Cell.Int32(4), Cell.Int32(4)),
      ),
    )
  }

  it should "emit nothing for an empty input" in {
    val planned = table(
      WideInput.ints("k", Array.emptyIntArray),
      WideInput.ints("q", Array.emptyIntArray),
    ).groupColumns(0, 1)
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideGroup(Scan)")
    both(planned) shouldBe Nil
  }

  it should "refuse a string key and a bool measure" in {
    val strings = table(WideInput.strings("k", Array("a")), WideInput.ints("q", Array(1)))
      .groupColumns(0, 1)
    ColumnarPlanner.lower(strings.plan) shouldBe None
    an[IllegalArgumentException] should be thrownBy strings.collect()
    val bools = table(WideInput.ints("k", Array(1)), WideInput.bools("q", Array(true)))
      .groupColumns(0, 1)
    ColumnarPlanner.lower(bools.plan) shouldBe None
    an[IllegalArgumentException] should be thrownBy bools.collect()
  }

  private def table(key: WideInput, measure: WideInput): DistCollection[WideRow] =
    DistCollection.ofColumns(Vector(key, measure), 2)

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
