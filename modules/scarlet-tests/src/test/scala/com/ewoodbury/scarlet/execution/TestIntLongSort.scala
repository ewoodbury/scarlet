package com.ewoodbury.scarlet.execution
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.support.Check.shouldBeEmpty

/**
 * Key sort and value sort of `mapValues` from `(Int, Int)` to `(Int, Long)`.
 *
 * A null key orders as zero and stays null. A null long stays null, and on the value sort it
 * orders as zero. One filter under the key sort lowers. A second map stays on rows for the key
 * sort. The value sort accepts one `Long => Long` map. A filter above that map stays empty.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Any",
  ),
)
class TestIntLongSort extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "sortByIntLongKey" should "order present keys and keep one filter" in {
    val widened = DistCollection
      .ofIntPairs(Array(3, 1, -2, 8), Array(10, 4, 7, 1), 2)
      .mapValues((value: Int) => value.toLong)
    val sorted = widened.sortByIntLongKey
    val filtered = widened.filterValues((value: Long) => value > 5L).sortByIntLongKey

    shape(sorted) shouldBe "SortByIntLongKey(IntLongMapValue(Scan))"
    shape(filtered) shouldBe "SortByIntLongKey(FilterValue(IntLongMapValue(Scan)))"
    columnar(sorted) shouldBe row(sorted.collect().toList)
    columnar(filtered) shouldBe row(filtered.collect().toList)
  }

  it should "order a null key as zero and leave a null long null" in {
    val sorted = pairs(
      Seq(
        cell(intRef(2), intRef(9)),
        cell(absentInt, intRef(4)),
        cell(intRef(-3), intRef(1)),
        cell(intRef(5), absentInt),
      ),
    ).mapValues((value: Int) => value.toLong).sortByIntLongKey

    shape(sorted) shouldBe "SortByIntLongKey(IntLongMapValue(Scan))"
    cells(columnar(sorted)) shouldBe Seq(
      (Some(-3), Some(1L)),
      (None, Some(4L)),
      (Some(2), Some(9L)),
      (Some(5), None),
    )
  }

  it should "refuse a second map" in {
    val widened = DistCollection
      .ofIntPairs(Array(1, 2), Array(10, 20), 1)
      .mapValues((value: Int) => value.toLong)
    val mapped = widened.mapValues((value: Long) => value + 1L).sortByIntLongKey

    ColumnarPlanner.lower(mapped.plan).shouldBeEmpty
  }

  "sortByIntLongValueDesc" should "order the long, then the int, and match present rows" in {
    val widened = DistCollection
      .ofIntPairs(Array(3, 1, 4, 2), Array(5, 9, 5, 1), 2)
      .mapValues((value: Int) => value.toLong)
    val sorted = widened.sortByIntLongValueDesc
    val mapped = widened.mapValues((value: Long) => value + 1L).sortByIntLongValueDesc
    val above = widened
      .mapValues((value: Long) => value + 1L)
      .filterValues((value: Long) => value > 0L)
      .sortByIntLongValueDesc

    shape(sorted) shouldBe "SortByIntLongValueDesc(IntLongMapValue(Scan))"
    shape(mapped) shouldBe "SortByIntLongValueDesc(MapValue(IntLongMapValue(Scan)))"
    ColumnarPlanner.lower(above.plan).shouldBeEmpty
    columnar(sorted) shouldBe row(sorted.collect().toList)
    columnar(mapped) shouldBe row(mapped.collect().toList)
  }

  it should "order a null long as zero and keep it null" in {
    val sorted = pairs(
      Seq(
        cell(intRef(1), intRef(5)),
        cell(intRef(4), absentInt),
        cell(intRef(2), intRef(-1)),
      ),
    ).mapValues((value: Int) => value.toLong).sortByIntLongValueDesc

    shape(sorted) shouldBe "SortByIntLongValueDesc(IntLongMapValue(Scan))"
    cells(columnar(sorted)) shouldBe Seq(
      (Some(1), Some(5L)),
      (Some(4), None),
      (Some(2), Some(-1L)),
    )
  }

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

  private def cells(rows: Seq[(Int, Long)]): Seq[(Option[Int], Option[Long])] =
    rows.map { row =>
      val product = row.asInstanceOf[Product]
      val key = product.productElement(0) match
        case null => None
        case value: Int => Some(value)
        case other => fail(s"unexpected key $other")
      val value = product.productElement(1) match
        case null => None
        case found: Long => Some(found)
        case other => fail(s"unexpected value $other")
      (key, value)
    }
