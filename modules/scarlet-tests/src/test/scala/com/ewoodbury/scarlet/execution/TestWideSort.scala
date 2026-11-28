package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*

/**
 * Global sort of one int, long, or string column, then an optional second of those kinds. A null
 * number orders as zero and stays. A null string orders as empty and stays.
 */
class TestWideSort extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8192,
        threadPoolSize = 2,
        defaultShufflePartitions = 2,
        spillEnabled = false,
      ),
    )

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ()

  "a null key" should "order as zero and stay null" in {
    val planned = numbers.sortColumn(0, descending = false)
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideSort(Scan)")
    both(planned) shouldBe List(
      Vector(Cell.Int32(-1), Cell.Int32(2)),
      Vector(Cell.Null, Cell.Int32(0)),
      Vector(Cell.Int32(0), Cell.Int32(3)),
      Vector(Cell.Int32(2), Cell.Int32(1)),
    )
  }

  "a descending null key" should "keep the earlier tie" in {
    both(numbers.sortColumn(0, descending = true)) shouldBe List(
      Vector(Cell.Int32(2), Cell.Int32(1)),
      Vector(Cell.Null, Cell.Int32(0)),
      Vector(Cell.Int32(0), Cell.Int32(3)),
      Vector(Cell.Int32(-1), Cell.Int32(2)),
    )
  }

  "equal keys" should "keep input order when the sort descends" in {
    val rows = ties
    both(rows.sortColumn(0, descending = false)) shouldBe List(
      Vector(Cell.Int32(0), Cell.Int32(1)),
      Vector(Cell.Int32(0), Cell.Int32(3)),
      Vector(Cell.Int32(1), Cell.Int32(0)),
      Vector(Cell.Int32(1), Cell.Int32(2)),
    )
    both(rows.sortColumn(0, descending = true)) shouldBe List(
      Vector(Cell.Int32(1), Cell.Int32(0)),
      Vector(Cell.Int32(1), Cell.Int32(2)),
      Vector(Cell.Int32(0), Cell.Int32(1)),
      Vector(Cell.Int32(0), Cell.Int32(3)),
    )
  }

  "uneven counts" should "sort by the count" in {
    val planned = counts.groupColumns(0, 1).sortColumn(1, descending = true)
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideSort(WideGroup(Scan))")
    both(planned) shouldBe List(
      Vector(Cell.Int32(1), Cell.Int64(3L), Cell.Int64(30L), Cell.Int32(10), Cell.Int32(10)),
      Vector(Cell.Int32(2), Cell.Int64(2L), Cell.Int64(8L), Cell.Int32(4), Cell.Int32(4)),
      Vector(Cell.Int32(3), Cell.Int64(1L), Cell.Int64(7L), Cell.Int32(7), Cell.Int32(7)),
    )
  }

  "a long limit" should "return every row" in {
    val planned = numbers.limitColumns(10)
    both(planned).length shouldBe 4
  }

  "a string column" should "order by the characters" in {
    val planned = DistCollection
      .ofColumns(Vector(WideInput.strings("s", Array("b", "a", "b"))), 1)
      .sortColumn(0, descending = false)
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideSort(Scan)")
    both(planned) shouldBe List(
      Vector(Cell.Utf8("a")),
      Vector(Cell.Utf8("b")),
      Vector(Cell.Utf8("b")),
    )
  }

  "a descending string" should "keep the earlier tie" in {
    val planned = DistCollection
      .ofColumns(Vector(WideInput.strings("s", Array("b", "a", "b"))), 1)
      .sortColumn(0, descending = true)
    both(planned) shouldBe List(
      Vector(Cell.Utf8("b")),
      Vector(Cell.Utf8("b")),
      Vector(Cell.Utf8("a")),
    )
  }

  "a null string beside an empty string" should "tie on empty and keep the cell" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.nullableStrings("s", Array("", "a", "b"), Array(true, false, true)),
        ),
        1,
      )
      .sortColumn(0, descending = false)
    both(planned) shouldBe List(
      Vector(Cell.Utf8("")),
      Vector(Cell.Null),
      Vector(Cell.Utf8("b")),
    )
  }

  "a null string" should "order as empty and stay null" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.nullableStrings("s", Array("b", "a", "c"), Array(true, false, true)),
        ),
        1,
      )
      .sortColumn(0, descending = false)
    both(planned) shouldBe List(
      Vector(Cell.Null),
      Vector(Cell.Utf8("b")),
      Vector(Cell.Utf8("c")),
    )
  }

  "a second int key" should "break the tie and keep input order" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("n", Array(1, 0, 1, 0)),
          WideInput.ints("id", Array(2, 0, 1, 0)),
        ),
        2,
      )
      .sortColumn(0, descending = false, Some(1))
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideSort(Scan)")
    both(planned) shouldBe List(
      Vector(Cell.Int32(0), Cell.Int32(0)),
      Vector(Cell.Int32(0), Cell.Int32(0)),
      Vector(Cell.Int32(1), Cell.Int32(1)),
      Vector(Cell.Int32(1), Cell.Int32(2)),
    )
  }

  "a descending first key" should "leave the second key ascending" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("n", Array(1, 0, 1, 0)),
          WideInput.ints("id", Array(2, 0, 1, 3)),
        ),
        2,
      )
      .sortColumn(0, descending = true, Some(1))
    both(planned) shouldBe List(
      Vector(Cell.Int32(1), Cell.Int32(1)),
      Vector(Cell.Int32(1), Cell.Int32(2)),
      Vector(Cell.Int32(0), Cell.Int32(0)),
      Vector(Cell.Int32(0), Cell.Int32(3)),
    )
  }

  "a long and a string" should "order the string inside one time" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.longs("t", Array(2L, 1L, 1L, 2L)),
          WideInput.strings("s", Array("b", "b", "a", "a")),
        ),
        2,
      )
      .sortColumn(0, descending = false, Some(1))
    both(planned) shouldBe List(
      Vector(Cell.Int64(1L), Cell.Utf8("a")),
      Vector(Cell.Int64(1L), Cell.Utf8("b")),
      Vector(Cell.Int64(2L), Cell.Utf8("a")),
      Vector(Cell.Int64(2L), Cell.Utf8("b")),
    )
  }

  "a null time and a null phrase" should "order as zero and empty and stay" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.nullableLongs("t", Array(0L, 0L, 1L), Array(false, true, true)),
          WideInput.nullableStrings("s", Array("b", "", "a"), Array(true, false, true)),
        ),
        1,
      )
      .sortColumn(0, descending = false, Some(1))
    both(planned) shouldBe List(
      Vector(Cell.Int64(0L), Cell.Null),
      Vector(Cell.Null, Cell.Utf8("b")),
      Vector(Cell.Int64(1L), Cell.Utf8("a")),
    )
  }

  "a float second key" should "be refused" in {
    val rows = DistCollection.ofColumns(
      Vector(
        WideInput.ints("n", Array(1, 0)),
        WideInput.doubles("x", Array(1.0, 0.0)),
      ),
      1,
    )
    an[IllegalArgumentException] should be thrownBy {
      rows.sortColumn(0, descending = false, Some(1))
    }
  }

  "a negative limit" should "be refused" in {
    an[IllegalArgumentException] should be thrownBy numbers.limitColumns(-1)
  }

  "a string payload" should "follow the int key across partitions" in {
    val planned = DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("n", Array(3, 1, 4, 2)),
          WideInput.strings("s", Array("c", "a", "d", "b")),
        ),
        2,
      )
      .sortColumn(0, descending = false)
    both(planned) shouldBe List(
      Vector(Cell.Int32(1), Cell.Utf8("a")),
      Vector(Cell.Int32(2), Cell.Utf8("b")),
      Vector(Cell.Int32(3), Cell.Utf8("c")),
      Vector(Cell.Int32(4), Cell.Utf8("d")),
    )
  }

  private def numbers: DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.nullableInts("n", Array(0, 2, -1, 0), Array(false, true, true, true)),
        WideInput.ints("id", Array(0, 1, 2, 3)),
      ),
      1,
    )

  private def ties: DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.ints("n", Array(1, 0, 1, 0)),
        WideInput.ints("id", Array(0, 1, 2, 3)),
      ),
      1,
    )

  private def counts: DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.ints("k", Array(1, 1, 1, 2, 2, 3)),
        WideInput.ints("m", Array(10, 10, 10, 4, 4, 7)),
      ),
      2,
    )

  private def both(planned: DistCollection[WideRow]): List[Vector[Cell]] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = planned.collect().toList.map(_.cells)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = planned.collect().toList.map(_.cells)
    columnar shouldBe rows
    columnar
