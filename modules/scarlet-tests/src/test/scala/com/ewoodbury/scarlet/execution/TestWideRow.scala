package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{
  Cell,
  ColumnCompare,
  ColumnField,
  ColumnKind,
  ColumnLiteral,
  CompareOp,
  Plan,
  ScarletConf,
  WideInput,
  WideRow,
}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Wide-row scan, filter, and project through the engine.
 *
 * A null in the predicate column is dropped. A null in a kept column stays. A compare the closed
 * set does not name stays on the row path. This shape is not one of the admission ledger rows.
 */
class TestWideRow extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val greater = ColumnCompare(CompareOp.Gt, ColumnLiteral.Int32(1))
  private val fields = Vector(
    ColumnField("n", ColumnKind.Int32),
    ColumnField("k", ColumnKind.Int64),
    ColumnField("x", ColumnKind.Float64),
  )
  private val renamed = Vector(
    ColumnField("n", ColumnKind.Int32),
    ColumnField("amount", ColumnKind.Float64),
  )
  private val kept = Vector(
    ColumnField("n", ColumnKind.Int32),
    ColumnField("k", ColumnKind.Int64),
  )

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

  "a wide row" should "filter an int column and drop another" in {
    val planned = source.filterColumn(0, greater).projectColumns(Vector((0, "n"), (2, "amount")))
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some(
      "WideProject(WideFilter0(Scan))",
    )
    same(
      planned,
      List(
        WideRow(renamed, Vector(Cell.Int32(4), Cell.Float64(3.5))),
        WideRow(renamed, Vector(Cell.Int32(2), Cell.Float64(4.5))),
      ),
    )
  }

  it should "keep a null in a column the filter does not read" in {
    val planned = source.filterColumn(0, greater).projectColumns(Vector((0, "n"), (1, "k")))
    same(
      planned,
      List(
        WideRow(kept, Vector(Cell.Int32(4), Cell.Null)),
        WideRow(kept, Vector(Cell.Int32(2), Cell.Int64(40L))),
      ),
    )
  }

  it should "rerun a compare the set does not name" in {
    val planned = source.filterColumn(2, ColumnCompare(CompareOp.Gt, ColumnLiteral.Int32(1)))
    ColumnarPlanner.lower(planned.plan) shouldBe None
    same(planned, List.empty[WideRow])
  }

  it should "rerun a predicate the set does not name" in {
    val planned = source.filter { row =>
      row.cells.head match
        case Cell.Int32(value) => value > 1
        case _ => false
    }
    ColumnarPlanner.lower(planned.plan) shouldBe None
    planned.plan match
      case _: Plan.FilterOp[?] => succeed
      case other => fail(s"expected a row filter, found $other")
    same(
      planned,
      List(
        WideRow(fields, Vector(Cell.Int32(4), Cell.Null, Cell.Float64(3.5))),
        WideRow(fields, Vector(Cell.Int32(2), Cell.Int64(40L), Cell.Float64(4.5))),
      ),
    )
  }

  private def source: DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(
        WideInput.nullableInts("n", Array(1, 0, 4, 2), Array(true, false, true, true)),
        WideInput.nullableLongs("k", Array(10L, 20L, 0L, 40L), Array(true, true, false, true)),
        WideInput.doubles("x", Array(1.5, 2.5, 3.5, 4.5)),
      ),
      2,
    )

  private def same(planned: DistCollection[WideRow], expected: List[WideRow]): Unit =
    val columnar = planned.collect().toList
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = planned.collect().toList
    columnar shouldBe expected
    rows shouldBe expected
