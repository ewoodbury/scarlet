package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Inner hash join of `mapValues` from `(Int, Int)` to `(Int, Long)` on both sides.
 *
 * A null key is dropped and does not match. A null long stays null and still matches. Broadcast,
 * sort-merge, and the three outers stay on rows. An int-pair join stays [[PhysicalOp.HashJoin]].
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Any",
  ),
)
class TestIntLongHashJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "join" should "match the row join for present longs and one filter" in {
    val left = widened(Array(1, 2, 4), Array(10, 20, 40))
    val right = widened(Array(2, 4, 9), Array(3, 5, 6))
    val joined = left.join(right)
    val hashed = left.shuffleHashJoin(right)
    val filtered = left.filterValues((value: Long) => value > 15L).join(right)

    shape(joined) shouldBe hashShape
    shape(hashed) shouldBe hashShape
    shape(filtered) shouldBe
      "IntLongHashJoin(Exchange(FilterValue(IntLongMapValue(Scan))), Exchange(IntLongMapValue(Scan)))"
    view(columnar(joined)) shouldBe view(row(joined.collect().toList))
    view(columnar(hashed)) shouldBe view(row(hashed.collect().toList))
    view(columnar(filtered)) shouldBe view(row(filtered.collect().toList))
  }

  it should "keep a null long and still match that key" in {
    val left = pairs(
      Seq(cell(intRef(2), intRef(8)), cell(intRef(1), absentInt)),
    ).mapValues((value: Int) => value.toLong)
    val right = pairs(
      Seq(cell(intRef(1), intRef(5)), cell(intRef(3), intRef(7))),
    ).mapValues((value: Int) => value.toLong)
    val joined = left.join(right)
    val seen = view(columnar(joined))

    shape(joined) shouldBe hashShape
    // Row mapValues unboxes a null int to zero, so that collect is not the oracle.
    seen shouldBe Seq((Some(1), None, Some(5L)))
  }

  it should "drop a null key on both sides" in {
    val left = pairs(
      Seq(cell(intRef(1), intRef(10)), cell(absentInt, intRef(7))),
    ).mapValues((value: Int) => value.toLong)
    val right = pairs(
      Seq(cell(intRef(1), intRef(4)), cell(absentInt, intRef(9))),
    ).mapValues((value: Int) => value.toLong)
    val joined = left.join(right)
    val seen = view(columnar(joined))

    shape(joined) shouldBe hashShape
    seen shouldBe view(row(joined.collect().toList))
    seen shouldBe Seq((Some(1), Some(10L), Some(4L)))
  }

  it should "leave other strategies and int pairs alone" in {
    val left = widened(Array(1, 2), Array(10, 20))
    val right = widened(Array(2, 3), Array(5, 6))
    val ints = DistCollection.ofIntPairs(Array(1, 2), Array(3, 4), 1)
    val other = DistCollection.ofIntPairs(Array(2), Array(5), 1)

    ColumnarPlanner.lower(left.broadcastJoin(right).plan) shouldBe None
    ColumnarPlanner.lower(left.sortMergeJoin(right).plan) shouldBe None
    ColumnarPlanner.lower(left.leftOuterJoin(right).plan) shouldBe None
    ColumnarPlanner.lower(left.rightOuterJoin(right).plan) shouldBe None
    ColumnarPlanner.lower(left.fullOuterJoin(right).plan) shouldBe None
    ColumnarPlanner.lower(left.join(ints).plan) shouldBe None
    shape(ints.join(other)) shouldBe "HashJoin(Exchange(Scan), Exchange(Scan))"
  }

  private val hashShape =
    "IntLongHashJoin(Exchange(IntLongMapValue(Scan)), Exchange(IntLongMapValue(Scan)))"

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

  private def view(rows: Seq[Any]): Seq[(Option[Int], Option[Long], Option[Long])] =
    rows
      .map { row =>
        val product = row.asInstanceOf[Product]
        val values = product.productElement(1).asInstanceOf[Product]
        (
          optInt(product.productElement(0)),
          optLong(values.productElement(0)),
          optLong(values.productElement(1)),
        )
      }
      .sortBy { cell =>
        (
          cell._1.getOrElse(Int.MinValue),
          cell._2.getOrElse(Long.MinValue),
          cell._3.getOrElse(Long.MinValue),
        )
      }

  private def optInt(value: Any): Option[Int] = value match
    case null => None
    case found: Int => Some(found)
    case other => fail(s"unexpected key $other")

  private def optLong(value: Any): Option[Long] = value match
    case null => None
    case found: Long => Some(found)
    case other => fail(s"unexpected long $other")
