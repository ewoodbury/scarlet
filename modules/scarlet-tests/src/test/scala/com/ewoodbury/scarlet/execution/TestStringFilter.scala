package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*

/**
 * Equality and inequality of a string column against a literal.
 *
 * A null cell does not pass. A null literal is refused. Ordering stays off the columnar plan.
 */
class TestStringFilter extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 4,
        threadPoolSize = 2,
        defaultShufflePartitions = 2,
        spillEnabled = false,
      ),
    )

  override def afterEach(): Unit =
    ScarletConf.set(originalConf)
    ()

  private val phrases = WideInput.nullableStrings(
    "phrase",
    Array("a", "", "a", "b", ""),
    Array(true, true, false, true, true),
  )
  private val ids = WideInput.ints("id", Array(1, 2, 3, 4, 5))

  private def table: DistCollection[WideRow] =
    DistCollection.ofColumns(Vector(ids, phrases), 2)

  "equality" should "keep the present matches" in {
    val planned = table.filterColumn(1, ColumnCompare(CompareOp.Eq, ColumnLiteral.Utf8("a")))
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideFilter1(Scan)")
    idsOf(planned) shouldBe Vector(1)
  }

  "inequality" should "drop a null and an empty string" in {
    val planned = table.filterColumn(1, ColumnCompare(CompareOp.Ne, ColumnLiteral.Utf8("")))
    ColumnarPlanner.lower(planned.plan).map(_.shape) shouldBe Some("WideFilter1(Scan)")
    idsOf(planned) shouldBe Vector(1, 4)
  }

  "a missing literal" should "keep every present row when not equal" in {
    val planned = table.filterColumn(1, ColumnCompare(CompareOp.Ne, ColumnLiteral.Utf8("z")))
    idsOf(planned) shouldBe Vector(1, 2, 4, 5)
  }

  "a null literal" should "be refused" in {
    val missing: String = null
    val thrown = the[IllegalArgumentException] thrownBy {
      val _ = ColumnCompare(CompareOp.Eq, ColumnLiteral.Utf8(missing))
    }
    thrown.getMessage should include("must not be null")
  }

  "ordering" should "stay off the columnar plan" in {
    val compare = ColumnCompare(CompareOp.Gt, ColumnLiteral.Utf8("a"))
    val planned = table.filterColumn(1, compare)
    ColumnarPlanner.lower(planned.plan) shouldBe None
  }

  private def idsOf(planned: DistCollection[WideRow]): Vector[Int] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = ids(planned)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = ids(planned)
    columnar shouldBe rows
    columnar

  private def ids(planned: DistCollection[WideRow]): Vector[Int] =
    planned.collect().map(_.cells.head).collect { case Cell.Int32(value) => value }.toVector
