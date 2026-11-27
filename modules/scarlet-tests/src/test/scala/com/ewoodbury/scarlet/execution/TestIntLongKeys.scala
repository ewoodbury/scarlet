package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Keys, values, filters, and `Long => Long` `mapValues` of an int value widened to long.
 *
 * Present rows match the row path. A null key or a null long does not: the row path unboxes it.
 * The columnar key slot drops a null key and keeps a null long. The value slot drops a null long
 * and keeps a null key. Neither calls the predicate on the clear bit. A whole-row filter drops a
 * row when either bit is clear. `Long => Int` and `Long => Double` stay empty. A non-adding
 * reduce, and `countByKey` or `groupByKey` of the filter, stay empty.
 */
class TestIntLongKeys extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "keys and values" should "keep a null component of the widening" in {
    val widened = nullPairs.mapValues((value: Int) => value.toLong)
    val keys = columnar(widened.keys)
    val values = columnar(widened.values)

    keys.length shouldBe 3
    intComponent(keys, 0) shouldBe 1
    intComponent(keys, 1) shouldBe 2
    absent(component(keys, 2)) shouldBe true
    longComponent(values, 0) shouldBe 4L
    absent(component(values, 1)) shouldBe true
    longComponent(values, 2) shouldBe 7L
    shape(widened.keys) shouldBe "Key(IntLongMapValue(Scan))"
    shape(widened.values) shouldBe "Value(IntLongMapValue(Scan))"
  }

  "present rows" should "match the row path for keys, values, filters, and a long map" in {
    val source = DistCollection.ofIntPairs(Array(1, 2, 3, 2), Array(10, 1, 30, 8), 1)
    val widened = source.mapValues((value: Int) => value.toLong)
    val keyed = widened.filterKeys((key: Int) => key > 1)
    val valued = widened.filterValues((value: Long) => value > 5L)
    val rowFilter = widened.filter((pair: (Int, Long)) => pair._1 > 1 && pair._2 > 5L)
    val mapped = widened.mapValues((value: Long) => value + 1L)

    columnar(widened.keys) shouldBe row(widened.keys.collect().toList)
    columnar(widened.values) shouldBe row(widened.values.collect().toList)
    columnar(keyed) shouldBe row(keyed.collect().toList)
    columnar(valued) shouldBe row(valued.collect().toList)
    columnar(rowFilter) shouldBe row(rowFilter.collect().toList)
    columnar(mapped) shouldBe row(mapped.collect().toList)
    shape(keyed) shouldBe "FilterKey(IntLongMapValue(Scan))"
    shape(valued) shouldBe "FilterValue(IntLongMapValue(Scan))"
    shape(rowFilter) shouldBe "FilterRow(IntLongMapValue(Scan))"
    shape(mapped) shouldBe "MapValue(IntLongMapValue(Scan))"
  }

  "filterValues" should "drop a null long, keep a null key, and call the predicate twice" in {
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 1))
    val calls = new AtomicInteger
    val filtered =
      nullPairs.mapValues((value: Int) => value.toLong).filterValues { (value: Long) =>
        val _ = calls.incrementAndGet()
        value > 0L
      }
    val got = columnar(filtered)

    got.length shouldBe 2
    intComponentPair(got, 0) shouldBe 1
    longComponentPair(got, 0) shouldBe 4L
    absent(pairComponent(got, 1, 0)) shouldBe true
    longComponentPair(got, 1) shouldBe 7L
    calls.get() shouldBe 2
    shape(filtered) shouldBe "FilterValue(IntLongMapValue(Scan))"
  }

  "filterKeys" should "drop a null key, keep a null long, and call the predicate twice" in {
    val calls = new AtomicInteger
    val filtered = nullPairs.mapValues((value: Int) => value.toLong).filterKeys { (key: Int) =>
      val _ = calls.incrementAndGet()
      key > 0
    }
    val got = columnar(filtered)

    got.length shouldBe 2
    intComponentPair(got, 0) shouldBe 1
    longComponentPair(got, 0) shouldBe 4L
    intComponentPair(got, 1) shouldBe 2
    absent(pairComponent(got, 1, 1)) shouldBe true
    calls.get() shouldBe 2
    shape(filtered) shouldBe "FilterKey(IntLongMapValue(Scan))"
  }

  "a row filter" should "drop a null component without calling the predicate" in {
    val calls = new AtomicInteger
    val filtered =
      nullPairs.mapValues((value: Int) => value.toLong).filter { (pair: (Int, Long)) =>
        val _ = calls.incrementAndGet()
        pair._2 > 0L
      }
    val got = columnar(filtered)

    got.length shouldBe 1
    intComponentPair(got, 0) shouldBe 1
    longComponentPair(got, 0) shouldBe 4L
    calls.get() shouldBe 1
  }

  "mapValues" should "add one to each present long and leave a null long null" in {
    val calls = new AtomicInteger
    val mapped = nullPairs.mapValues((value: Int) => value.toLong).mapValues { (value: Long) =>
      val _ = calls.incrementAndGet()
      value + 1L
    }
    val got = columnar(mapped)

    got.length shouldBe 3
    longComponentPair(got, 0) shouldBe 5L
    absent(pairComponent(got, 1, 1)) shouldBe true
    longComponentPair(got, 2) shouldBe 8L
    calls.get() shouldBe 2
    shape(mapped) shouldBe "MapValue(IntLongMapValue(Scan))"
  }

  "other parents" should "stay empty" in {
    val widened = DistCollection
      .ofIntPairs(Array(1, 2), Array(10, 20), 1)
      .mapValues((value: Int) => value.toLong)
    val filtered = widened.filterValues((value: Long) => value > 0L)

    ColumnarPlanner.lower(widened.mapValues((value: Long) => value.toInt).plan) shouldBe None
    ColumnarPlanner.lower(widened.mapValues((value: Long) => value.toDouble).plan) shouldBe None
    ColumnarPlanner.lower(filtered.countByKey.plan) shouldBe None
    ColumnarPlanner.lower(filtered.groupByKey.plan) shouldBe None
    ColumnarPlanner.lower(
      widened.reduceByKey[Int, Long]((left: Long, _: Long) => left).plan,
    ) shouldBe None
  }

  /** Present pair, null value, null key. One partition, input order. */
  private def nullPairs: DistCollection[(Int, Int)] =
    DistCollection(
      Seq(
        pair(intRef(1), intRef(4)),
        pair(intRef(2), absentInt),
        pair(absentInt, intRef(7)),
      ),
      1,
    )

  private def columnar[A](collection: DistCollection[A]): Seq[A] =
    val ran = ColumnarPlanner
      .tryRun(collection.plan)
      .getOrElse(fail(s"stayed on the row path: ${collection.plan}"))
    ran.flatMap(_.data).toList

  private def shape(collection: DistCollection[_]): String =
    ColumnarPlanner.lower(collection.plan).map(_.shape).getOrElse("none")

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def pair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentInt: java.lang.Integer = null

  private def intRef(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def component[A](values: Seq[A], index: Int): Any =
    values.iterator.drop(index).next().asInstanceOf[AnyRef]

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def pairComponent(values: Seq[(Int, Long)], index: Int, ordinal: Int): Any =
    values.iterator.drop(index).next().asInstanceOf[Product].productElement(ordinal)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def absent(value: Any): Boolean =
    value.asInstanceOf[AnyRef] eq null

  private def intComponent(values: Seq[Int], index: Int): Int = component(values, index) match
    case value: Int => value
    case _ => fail(s"expected a present int at $index")

  private def longComponent(values: Seq[Long], index: Int): Long = component(values, index) match
    case value: Long => value
    case _ => fail(s"expected a present long at $index")

  private def intComponentPair(values: Seq[(Int, Long)], index: Int): Int =
    pairComponent(values, index, 0) match
      case value: Int => value
      case _ => fail(s"expected a present int key at $index")

  private def longComponentPair(values: Seq[(Int, Long)], index: Int): Long =
    pairComponent(values, index, 1) match
      case value: Long => value
      case _ => fail(s"expected a present long value at $index")
