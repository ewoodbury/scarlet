package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Ungrouped min and max. A null is skipped. Empty and all-null columns are zero. The batch path
 * and the row path match a fold of the same values.
 */
class TestUngroupedExtremum extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 64,
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

  "an int column" should "skip nulls and keep a negative" in {
    val values = Array(4, -8, 9, 1)
    val present = Array(true, true, false, true)
    val table = ints(values, present)
    WideExtremum.minInt(table.plan, 0) shouldBe Some(-8)
    WideExtremum.maxInt(table.plan, 0) shouldBe Some(4)
    agreeInt(table, -8, 4)
  }

  it should "keep one present neighbor of a null" in {
    val table = ints(Array(4, 9), Array(false, true))
    WideExtremum.minInt(table.plan, 0) shouldBe Some(9)
    WideExtremum.maxInt(table.plan, 0) shouldBe Some(9)
    agreeInt(table, 9, 9)
  }

  it should "match a row fold across several batches" in {
    val count = 3000
    val values = Array.tabulate(count)(index => (index % 50) - 20)
    val present = Array.tabulate(count)(index => index != 7 && index != 8)
    val table = ints(values, present)
    val least = foldInt(values, present, least = true)
    val greatest = foldInt(values, present, least = false)
    WideExtremum.minInt(table.plan, 0) shouldBe Some(least)
    WideExtremum.maxInt(table.plan, 0) shouldBe Some(greatest)
    agreeInt(table, least, greatest)
  }

  it should "be zero when every slot is null or the column is empty" in {
    val empty = ints(Array.emptyIntArray, Array.emptyBooleanArray)
    val absent = ints(Array(1, 2, 3), Array(false, false, false))
    WideExtremum.minInt(empty.plan, 0) shouldBe Some(0)
    WideExtremum.maxInt(empty.plan, 0) shouldBe Some(0)
    WideExtremum.minInt(absent.plan, 0) shouldBe Some(0)
    WideExtremum.maxInt(absent.plan, 0) shouldBe Some(0)
    agreeInt(empty, 0, 0)
    agreeInt(absent, 0, 0)
  }

  it should "read a filtered column" in {
    val table = DistCollection
      .ofColumns(Vector(WideInput.ints("n", Array(1, 2, 3, 4))), 2)
      .filterColumn(0, ColumnCompare(CompareOp.Gt, ColumnLiteral.Int32(2)))
    WideExtremum.minInt(table.plan, 0) shouldBe Some(3)
    WideExtremum.maxInt(table.plan, 0) shouldBe Some(4)
    agreeInt(table, 3, 4)
  }

  "a long column" should "reach both ends of a long" in {
    val count = 3000
    val values = Array.tabulate(count)(index => (index % 50).toLong - 20L)
    values(0) = Long.MinValue
    values(1) = Long.MaxValue
    val present = Array.tabulate(count)(index => index != 7)
    val table = longs(values, present)
    WideExtremum.minLong(table.plan, 0) shouldBe Some(Long.MinValue)
    WideExtremum.maxLong(table.plan, 0) shouldBe Some(Long.MaxValue)
    agreeLong(table, Long.MinValue, Long.MaxValue)
  }

  "a float column" should "skip a null and match the fold" in {
    val count = 3000
    val values = Array.tabulate(count)(index => (index % 50).toFloat - 20.5f)
    val present = Array.tabulate(count)(index => index != 7)
    val table = floats(values, present)
    val least = foldFloat(values, present, least = true)
    val greatest = foldFloat(values, present, least = false)
    WideExtremum.minFloat(table.plan, 0) shouldBe Some(least)
    WideExtremum.maxFloat(table.plan, 0) shouldBe Some(greatest)
    agreeFloat(table, least, greatest)
  }

  "a double column" should "skip a null and match the fold" in {
    val count = 3000
    val values = Array.tabulate(count)(index => (index % 50).toDouble - 20.5)
    val present = Array.tabulate(count)(index => index != 7)
    val table = doubles(values, present)
    val least = foldDouble(values, present, least = true)
    val greatest = foldDouble(values, present, least = false)
    WideExtremum.minDouble(table.plan, 0) shouldBe Some(least)
    WideExtremum.maxDouble(table.plan, 0) shouldBe Some(greatest)
    agreeDouble(table, least, greatest)
  }

  "the wrong kind" should "stay off the int kernel" in {
    val table = DistCollection.ofColumns(Vector(WideInput.longs("n", Array(1L, -2L))), 1)
    WideExtremum.minInt(table.plan, 0) shouldBe None
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    an[IllegalArgumentException] should be thrownBy table.minIntColumn(0)
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    an[IllegalArgumentException] should be thrownBy table.minIntColumn(0)
  }

  private def ints(values: Array[Int], present: Array[Boolean]): DistCollection[WideRow] =
    DistCollection.ofColumns(Vector(WideInput.nullableInts("n", values, present)), 4)

  private def longs(values: Array[Long], present: Array[Boolean]): DistCollection[WideRow] =
    DistCollection.ofColumns(Vector(WideInput.nullableLongs("n", values, present)), 4)

  private def floats(values: Array[Float], present: Array[Boolean]): DistCollection[WideRow] =
    DistCollection.ofColumns(Vector(WideInput.nullableFloats("n", values, present)), 4)

  private def doubles(values: Array[Double], present: Array[Boolean]): DistCollection[WideRow] =
    DistCollection.ofColumns(Vector(WideInput.nullableDoubles("n", values, present)), 4)

  private def agreeInt(table: DistCollection[WideRow], least: Int, greatest: Int): Unit =
    val (minColumnar, minRows) = both(table.minIntColumn(0))
    val (maxColumnar, maxRows) = both(table.maxIntColumn(0))
    minColumnar shouldBe least
    minRows shouldBe least
    maxColumnar shouldBe greatest
    maxRows shouldBe greatest

  private def agreeLong(table: DistCollection[WideRow], least: Long, greatest: Long): Unit =
    val (minColumnar, minRows) = both(table.minLongColumn(0))
    val (maxColumnar, maxRows) = both(table.maxLongColumn(0))
    minColumnar shouldBe least
    minRows shouldBe least
    maxColumnar shouldBe greatest
    maxRows shouldBe greatest

  private def agreeFloat(table: DistCollection[WideRow], least: Float, greatest: Float): Unit =
    val (minColumnar, minRows) = both(table.minFloatColumn(0))
    val (maxColumnar, maxRows) = both(table.maxFloatColumn(0))
    minColumnar shouldBe least
    minRows shouldBe least
    maxColumnar shouldBe greatest
    maxRows shouldBe greatest

  private def agreeDouble(table: DistCollection[WideRow], least: Double, greatest: Double): Unit =
    val (minColumnar, minRows) = both(table.minDoubleColumn(0))
    val (maxColumnar, maxRows) = both(table.maxDoubleColumn(0))
    minColumnar shouldBe least
    minRows shouldBe least
    maxColumnar shouldBe greatest
    maxRows shouldBe greatest

  private def both[A](run: => A): (A, A) =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = run
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = run
    (columnar, rows)

  private def foldInt(values: Array[Int], present: Array[Boolean], least: Boolean): Int =
    values.indices
      .foldLeft((0, false)) { (state, index) =>
        keep(state, present(index), values(index), least)
      }
      ._1

  private def foldFloat(values: Array[Float], present: Array[Boolean], least: Boolean): Float =
    values.indices
      .foldLeft((0.0f, false)) { (state, index) =>
        keep(state, present(index), values(index), least)
      }
      ._1

  private def foldDouble(values: Array[Double], present: Array[Boolean], least: Boolean): Double =
    values.indices
      .foldLeft((0.0, false)) { (state, index) =>
        keep(state, present(index), values(index), least)
      }
      ._1

  private def keep[A](state: (A, Boolean), present: Boolean, value: A, least: Boolean)(using
      ordering: Ordering[A],
  ): (A, Boolean) =
    if (!present) then state
    else
      val (best, seen) = state
      if (!seen) then (value, true)
      else if (least && ordering.lt(value, best)) then (value, true)
      else if (!least && ordering.gt(value, best)) then (value, true)
      else state
