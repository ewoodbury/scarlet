package com.ewoodbury.scarlet.execution
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.support.Check
import com.ewoodbury.scarlet.support.Check.shouldBeEmpty

/**
 * Group by two int or long columns. A null in either key is dropped. The plan is not `GroupByKey`.
 * Both paths match a fold of the same rows.
 */
class TestTwoKeyGroup extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 32,
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

  "two int keys" should "drop a null second key and match a row fold" in {
    val count = 3000
    val left = Array.tabulate(count)(index => index % 40)
    val right = Array.tabulate(count)(index => index % 25)
    val measure = Array.tabulate(count)(index => (index % 7) - 3)
    val leftPresent = Array.fill(count)(true)
    val rightPresent = Array.tabulate(count)(index => index % 17 != 0)
    val measurePresent = Array.tabulate(count)(index => index % 13 != 0)
    val table = ints(left, leftPresent, right, rightPresent, measure, measurePresent)
    table.plan match
      case Plan.WideGroupOp(_, _, _, Some(_), None) =>
        Check.text(table.plan).contains("GroupByKey") shouldBe false
      case other =>
        fail(Check.text(other))
    ColumnarPlanner.lower(table.plan).map(_.shape) shouldBe Some("WideGroup(Scan)")
    val expected = foldInts(left, leftPresent, right, rightPresent, measure, measurePresent)
    both(table) shouldBe expected
  }

  it should "drop a null first key" in {
    val table = ints(
      Array(1, 1, 2),
      Array(true, false, true),
      Array(10, 10, 11),
      Array(true, true, true),
      Array(5, 8, 4),
      Array(true, true, true),
    )
    both(table) shouldBe Vector(
      Vector(
        Cell.Int32(1),
        Cell.Int32(10),
        Cell.Int64(1),
        Cell.Int64(5),
        Cell.Int32(5),
        Cell.Int32(5),
      ),
      Vector(
        Cell.Int32(2),
        Cell.Int32(11),
        Cell.Int64(1),
        Cell.Int64(4),
        Cell.Int32(4),
        Cell.Int32(4),
      ),
    )
  }

  it should "keep a group whose measure is null" in {
    val table = ints(
      Array(1),
      Array(true),
      Array(2),
      Array(true),
      Array(9),
      Array(false),
    )
    both(table) shouldBe Vector(
      Vector(Cell.Int32(1), Cell.Int32(2), Cell.Int64(1), Cell.Null, Cell.Null, Cell.Null),
    )
  }

  "an int and a long key" should "match a row fold" in {
    val table = DistCollection.ofColumns(
      Vector(
        WideInput.ints("a", Array(1, 1, 1, 2)),
        WideInput
          .nullableLongs("b", Array(100L, 100L, 200L, 100L), Array(true, true, true, false)),
        WideInput.ints("m", Array(3, 4, 5, 9)),
      ),
      4,
    )
    val grouped = table.groupColumns(0, 1, 2)
    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("WideGroup(Scan)")
    both(grouped) shouldBe Vector(
      Vector(
        Cell.Int32(1),
        Cell.Int64(100L),
        Cell.Int64(2),
        Cell.Int64(7),
        Cell.Int32(3),
        Cell.Int32(4),
      ),
      Vector(
        Cell.Int32(1),
        Cell.Int64(200L),
        Cell.Int64(1),
        Cell.Int64(5),
        Cell.Int32(5),
        Cell.Int32(5),
      ),
    )
  }

  "two long keys" should "match a row fold" in {
    val table = DistCollection.ofColumns(
      Vector(
        WideInput.longs("a", Array(1L, 1L, -4L)),
        WideInput.longs("b", Array(8L, 9L, 8L)),
        WideInput.longs("m", Array(2L, 3L, -5L)),
      ),
      2,
    )
    both(table.groupColumns(0, 1, 2)) shouldBe Vector(
      Vector(
        Cell.Int64(-4L),
        Cell.Int64(8L),
        Cell.Int64(1),
        Cell.Int64(-5L),
        Cell.Int64(-5L),
        Cell.Int64(-5L),
      ),
      Vector(
        Cell.Int64(1L),
        Cell.Int64(8L),
        Cell.Int64(1),
        Cell.Int64(2L),
        Cell.Int64(2L),
        Cell.Int64(2L),
      ),
      Vector(
        Cell.Int64(1L),
        Cell.Int64(9L),
        Cell.Int64(1),
        Cell.Int64(3L),
        Cell.Int64(3L),
        Cell.Int64(3L),
      ),
    )
  }

  "the same column twice" should "be refused" in {
    val table = DistCollection.ofColumns(Vector(WideInput.ints("a", Array(1, 2))), 1)
    an[IllegalArgumentException] should be thrownBy table.groupColumns(0, 0, 0)
  }

  "a float key" should "stay off the wide group" in {
    val table = DistCollection
      .ofColumns(
        Vector(
          WideInput.ints("a", Array(1, 2)),
          WideInput.floats("b", Array(1.0f, 2.0f)),
          WideInput.ints("m", Array(3, 4)),
        ),
        1,
      )
      .groupColumns(0, 1, 2)
    ColumnarPlanner.lower(table.plan).shouldBeEmpty
    an[IllegalArgumentException] should be thrownBy table.collect()
  }

  private def ints(
      left: Array[Int],
      leftPresent: Array[Boolean],
      right: Array[Int],
      rightPresent: Array[Boolean],
      measure: Array[Int],
      measurePresent: Array[Boolean],
  ): DistCollection[WideRow] =
    DistCollection
      .ofColumns(
        Vector(
          WideInput.nullableInts("a", left, leftPresent),
          WideInput.nullableInts("b", right, rightPresent),
          WideInput.nullableInts("m", measure, measurePresent),
        ),
        4,
      )
      .groupColumns(0, 1, 2)

  private def both(table: DistCollection[WideRow]): Vector[Vector[Cell]] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true))
    val columnar = sorted(table.collect())
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = false))
    val rows = sorted(table.collect())
    columnar shouldBe rows
    columnar

  private def sorted(rows: Iterable[WideRow]): Vector[Vector[Cell]] =
    rows.map(_.cells).toVector.sortWith(before)

  private def before(left: Vector[Cell], right: Vector[Cell]): Boolean =
    (left.headOption, right.headOption) match
      case (Some(Cell.Int32(l)), Some(Cell.Int32(r))) if l != r => l < r
      case (Some(Cell.Int64(l)), Some(Cell.Int64(r))) if l != r => l < r
      case _ =>
        (left.lift(1), right.lift(1)) match
          case (Some(Cell.Int32(l)), Some(Cell.Int32(r))) => l < r
          case (Some(Cell.Int64(l)), Some(Cell.Int64(r))) => l < r
          case _ => false

  private def foldInts(
      left: Array[Int],
      leftPresent: Array[Boolean],
      right: Array[Int],
      rightPresent: Array[Boolean],
      measure: Array[Int],
      measurePresent: Array[Boolean],
  ): Vector[Vector[Cell]] =
    val groups = left.indices.foldLeft(Map.empty[(Int, Int), (Long, Long, Int, Int, Boolean)]) {
      (acc, index) =>
        if (leftPresent(index) && rightPresent(index)) then
          val key = (left(index), right(index))
          val (count, sum, min, max, seen) =
            acc.getOrElse(key, (0L, 0L, 0, 0, false))
          val next =
            if (!measurePresent(index)) then (count + 1L, sum, min, max, seen)
            else if (!seen) then
              (count + 1L, measure(index).toLong, measure(index), measure(index), true)
            else
              (
                count + 1L,
                sum + measure(index).toLong,
                math.min(min, measure(index)),
                math.max(max, measure(index)),
                true,
              )
          acc + (key -> next)
        else acc
    }
    groups.toVector.sortBy((key, _) => key).map {
      case ((key, key2), (count, sum, min, max, seen)) =>
        if (seen) then
          Vector(
            Cell.Int32(key),
            Cell.Int32(key2),
            Cell.Int64(count),
            Cell.Int64(sum),
            Cell.Int32(min),
            Cell.Int32(max),
          )
        else
          Vector(
            Cell.Int32(key),
            Cell.Int32(key2),
            Cell.Int64(count),
            Cell.Null,
            Cell.Null,
            Cell.Null,
          )
    }
