package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Hash outer joins of `mapValues` from `(Int, Int)` to `(Int, Long)`.
 *
 * A null key does not match. The preserved side keeps it, and the other value is absent. A null
 * long stays null and is not a missing partner. An unhinted outer uses the hash node.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Any",
  ),
)
class TestIntLongHashOuter extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private enum LongCell:
    case Missing, NullLong
    case Value(n: Long)

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "outer join" should "match the row join for present longs" in {
    val left = widened(Array(1, 2, 2), Array(10, 20, 30))
    val right = widened(Array(2, 4), Array(5, 6))
    val leftJoin = left.leftOuterJoin(right)
    val rightJoin = left.rightOuterJoin(right)
    val fullJoin = left.fullOuterJoin(right)
    val filtered = left.filterValues((value: Long) => value > 15L).leftOuterJoin(right)

    shape(leftJoin) shouldBe leftShape
    shape(rightJoin) shouldBe rightShape
    shape(fullJoin) shouldBe fullShape
    shape(filtered) shouldBe
      "IntLongHashLeftOuter(Exchange(FilterValue(IntLongMapValue(Scan))), Exchange(IntLongMapValue(Scan)))"
    view(columnar(leftJoin)) shouldBe view(row(leftJoin.collect().toList))
    view(columnar(rightJoin)) shouldBe view(row(rightJoin.collect().toList))
    view(columnar(fullJoin)) shouldBe view(row(fullJoin.collect().toList))
    view(columnar(filtered)) shouldBe view(row(filtered.collect().toList))
  }

  it should "keep a null key on the preserved side" in {
    val left = pairs(
      Seq(cell(intRef(1), intRef(10)), cell(absentInt, intRef(4))),
    ).mapValues((value: Int) => value.toLong)
    val right = pairs(
      Seq(cell(intRef(1), intRef(5)), cell(absentInt, intRef(9))),
    ).mapValues((value: Int) => value.toLong)

    view(columnar(left.leftOuterJoin(right))) shouldBe view(
      row(left.leftOuterJoin(right).collect().toList),
    )
    view(columnar(left.rightOuterJoin(right))) shouldBe view(
      row(left.rightOuterJoin(right).collect().toList),
    )
    view(columnar(left.fullOuterJoin(right))) shouldBe view(
      row(left.fullOuterJoin(right).collect().toList),
    )
  }

  it should "keep a null long and still tell a missing partner apart" in {
    val left = pairs(
      Seq(cell(intRef(2), intRef(8)), cell(intRef(1), absentInt)),
    ).mapValues((value: Int) => value.toLong)
    val right = pairs(
      Seq(cell(intRef(1), intRef(5)), cell(intRef(3), intRef(7))),
    ).mapValues((value: Int) => value.toLong)

    // Row mapValues unboxes a null int to zero, so that collect is not the oracle.
    view(columnar(left.leftOuterJoin(right))) shouldBe Seq(
      (Some(1), LongCell.NullLong, LongCell.Value(5L)),
      (Some(2), LongCell.Value(8L), LongCell.Missing),
    )
    view(columnar(left.rightOuterJoin(right))) shouldBe Seq(
      (Some(1), LongCell.NullLong, LongCell.Value(5L)),
      (Some(3), LongCell.Missing, LongCell.Value(7L)),
    )
    view(columnar(left.fullOuterJoin(right))) shouldBe Seq(
      (Some(1), LongCell.NullLong, LongCell.Value(5L)),
      (Some(2), LongCell.Value(8L), LongCell.Missing),
      (Some(3), LongCell.Missing, LongCell.Value(7L)),
    )
  }

  private val leftShape =
    "IntLongHashLeftOuter(Exchange(IntLongMapValue(Scan)), Exchange(IntLongMapValue(Scan)))"

  private val rightShape =
    "IntLongHashRightOuter(Exchange(IntLongMapValue(Scan)), Exchange(IntLongMapValue(Scan)))"

  private val fullShape =
    "IntLongHashFullOuter(Exchange(IntLongMapValue(Scan)), Exchange(IntLongMapValue(Scan)))"

  private def widened(keys: Array[Int], values: Array[Int]): DistCollection[(Int, Long)] =
    DistCollection.ofIntPairs(keys, values, 2).mapValues((value: Int) => value.toLong)

  private def pairs(rows: Seq[(Int, Int)]): DistCollection[(Int, Int)] =
    DistCollection(rows, 1)

  private def shape(collection: DistCollection[_]): String =
    ColumnarPlanner.lower(collection.plan).map(_.shape).getOrElse("none")

  private def columnar[A](collection: DistCollection[A]): Seq[A] =
    ColumnarPlanner
      .tryRun(collection.plan)
      .getOrElse(fail(s"stayed on the row path: ${collection.plan}"))
      .flatMap(_.data)
      .toList

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def cell(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  private def absentInt: java.lang.Integer = null

  private def intRef(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)

  private def view(rows: Seq[Any]): Seq[(Option[Int], LongCell, LongCell)] =
    rows
      .map { row =>
        val product = row.asInstanceOf[Product]
        val values = product.productElement(1).asInstanceOf[Product]
        (
          optInt(product.productElement(0)),
          longCell(values.productElement(0)),
          longCell(values.productElement(1)),
        )
      }
      .sortBy { cell =>
        (cell._1.getOrElse(Int.MinValue), rank(cell._2), rank(cell._3))
      }

  private def optInt(value: Any): Option[Int] = value match
    case null => None
    case found: Int => Some(found)
    case other => fail(s"unexpected key $other")

  private def longCell(value: Any): LongCell = value match
    case None => LongCell.Missing
    case Some(inner) => boxed(inner)
    case other => boxed(other)

  private def boxed(value: Any): LongCell = value match
    case null => LongCell.NullLong
    case found: Long => LongCell.Value(found)
    case other => fail(s"unexpected long $other")

  private def rank(cell: LongCell): (Int, Long) = cell match
    case LongCell.Missing => (0, 0L)
    case LongCell.NullLong => (1, 0L)
    case LongCell.Value(n) => (2, n)
