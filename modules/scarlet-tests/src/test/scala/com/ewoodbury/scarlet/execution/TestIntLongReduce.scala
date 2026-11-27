package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.execution.schema.Layout
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Non-adding `reduceByKey` and `countSumByIntLongKey` after `mapValues` to long.
 *
 * Addition stays a sum. Any other `(Long, Long) => Long` on the bare widening is an int-long hash
 * aggregate. A filter above that map does not reduce. A null key is dropped. A null value unboxes
 * as zero once a second value arrives. `countSumByIntLongKey` counts a null value and does not add
 * it. The row path unboxes a null, so those groups are not compared to it.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Var",
  ),
)
class TestIntLongReduce extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "long addition" should "stay a sum and match the row path" in {
    val widened = DistCollection
      .ofIntPairs(Array(1, 2, 1, 3), Array(10, 4, 5, 7), 2)
      .mapValues((value: Int) => value.toLong)
    val added = widened.reduceByKey[Int, Long]((left: Long, right: Long) => left + right)

    shape(added) shouldBe "HashAggregate(Exchange(IntLongMapValue(Scan)))"
    byKey(columnar(added)) shouldBe byKey(row(added.collect().toList))
  }

  "a non-adding reduce" should "run once per extra value and match one partition" in {
    val calls = new AtomicInteger
    val source = DistCollection
      .ofIntPairs(Array(1, 1, 1, 2), Array(10, 20, 30, 5), 1)
      .mapValues((value: Int) => value.toLong)
    val kept = source.reduceByKey[Int, Long] { (left: Long, right: Long) =>
      note(calls, left, right)
      left
    }

    shape(kept) shouldBe "IntLongHashAggregate(Exchange(IntLongMapValue(Scan)))"
    val found = columnar(kept)
    calls.get() shouldBe 2
    byKey(found) shouldBe byKey(row(kept.collect().toList))
  }

  "other reducers" should "stay empty" in {
    val widened = DistCollection
      .ofIntPairs(Array(1, 1), Array(10, 20), 1)
      .mapValues((value: Int) => value.toLong)
    val filtered = widened.filterValues((value: Long) => value > 0L)
    val intFn = ((left: Int, _: Int) => left).asInstanceOf[(Long, Long) => Long]
    val ints = Plan.ReduceByKeyOp(widened.plan, intFn)
    val kept = filtered.reduceByKey[Int, Long]((left: Long, _: Long) => left)

    ColumnarPlanner.lower(ints) shouldBe None
    ColumnarPlanner.lower(kept.plan) shouldBe None
  }

  "null components" should "drop a null key and unbox a null value" in {
    val calls = new AtomicInteger
    val combine = (left: Long, right: Long) => {
      note(calls, left, right)
      left * 10L + right
    }
    val dropped = reduce(Seq(cell(intRef(1), intRef(7)), cell(absentInt, intRef(5))), combine)
    calls.get() shouldBe 0
    dropped.length shouldBe 1
    intComponent(dropped, 0) shouldBe 1
    longComponent(dropped, 0) shouldBe 7L

    // A leading null value stays Unknown, so each group starts with a present pair of another key.
    calls.set(0)
    val onlyNull = reduce(Seq(cell(intRef(2), intRef(3)), cell(intRef(1), absentInt)), combine)
    calls.get() shouldBe 0
    onlyNull.length shouldBe 2
    absent(componentFor(onlyNull, 1, 1)) shouldBe true
    longFor(onlyNull, 2) shouldBe 3L

    calls.set(0)
    val nullThen = reduce(
      Seq(cell(intRef(2), intRef(3)), cell(intRef(1), absentInt), cell(intRef(1), intRef(5))),
      combine,
    )
    calls.get() shouldBe 1
    longFor(nullThen, 1) shouldBe 5L
    longFor(nullThen, 2) shouldBe 3L

    calls.set(0)
    val presentThen = reduce(Seq(cell(intRef(1), intRef(5)), cell(intRef(1), absentInt)), combine)
    calls.get() shouldBe 1
    longFor(presentThen, 1) shouldBe 50L

    calls.set(0)
    val bothNull = reduce(
      Seq(cell(intRef(2), intRef(3)), cell(intRef(1), absentInt), cell(intRef(1), absentInt)),
      combine,
    )
    calls.get() shouldBe 1
    longFor(bothNull, 1) shouldBe 0L
  }

  "countSumByIntLongKey" should "lower the widening, a filter, and a long map" in {
    val widened = DistCollection
      .ofIntPairs(Array(1, 2), Array(10, 20), 1)
      .mapValues((value: Int) => value.toLong)
    val filtered = widened.filterValues((value: Long) => value > 0L)
    val mapped = widened.mapValues((value: Long) => value + 1L)

    shape(widened.countSumByIntLongKey) shouldBe
      "IntLongCountSum(Exchange(IntLongMapValue(Scan)))"
    shape(filtered.countSumByIntLongKey) shouldBe
      "IntLongCountSum(Exchange(FilterValue(IntLongMapValue(Scan))))"
    shape(mapped.countSumByIntLongKey) shouldBe
      "IntLongCountSum(Exchange(MapValue(IntLongMapValue(Scan))))"
  }

  it should "drop a null key, count a null value, and match present rows" in {
    val calls = new AtomicInteger
    val widen: Int => Long = value => {
      val _ = calls.incrementAndGet()
      value.toLong
    }
    val nullable = pairs(
      Seq(cell(intRef(1), intRef(4)), cell(intRef(1), absentInt), cell(intRef(2), absentInt)),
    ).mapValues(widen)
    calls.set(0)
    val counted = triples(nullable.countSumByIntLongKey)
    calls.get() shouldBe 1
    byTriple(counted) shouldBe Seq((1, 2, 4L), (2, 1, 0L))

    calls.set(0)
    val dropped = pairs(Seq(cell(intRef(1), intRef(4)), cell(absentInt, intRef(9)))).mapValues(
      widen,
    )
    val kept = triples(dropped.countSumByIntLongKey)
    calls.get() shouldBe 2
    byTriple(kept) shouldBe Seq((1, 1, 4L))

    val present = DistCollection
      .ofIntPairs(Array(1, 2, 1, 3, 2), Array(10, 20, 5, 7, 1), 4)
      .mapValues((value: Int) => value.toLong)
      .countSumByIntLongKey
    byTriple(triples(present)) shouldBe byTriple(row(present.collect().toList))
  }

  "other count sums" should "stay off the int-long kernel" in {
    val ints = DistCollection.ofIntPairs(Array(1, 1, 2), Array(10, 20, 5), 1).countSumByKey
    shape(ints) shouldBe "CountSum(Exchange(Scan))"
    val projected = PhysicalOp.MapKey(
      PhysicalOp.Project(PhysicalOp.Scan(PhysicalOp.Kind.Triples, Seq.empty)),
    )
    Layout.known(projected) shouldBe Layout.AssumedIntLong
    CountSumLowering(projected) shouldBe None
  }

  /**
   * The binder calls `(1, 2)` to tell addition from any other function. That probe is not a group.
   */
  private def note(calls: AtomicInteger, left: Long, right: Long): Unit =
    (left, right) match
      case (1L, 2L) => ()
      case _ =>
        val _ = calls.incrementAndGet()

  private def reduce(
      rows: Seq[(Int, Int)],
      op: (Long, Long) => Long,
  ): Seq[(Int, Long)] =
    columnar(pairs(rows).mapValues((value: Int) => value.toLong).reduceByKey[Int, Long](op))

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

  private def triples(collection: DistCollection[(Int, Int, Long)]): Seq[(Int, Int, Long)] =
    columnar(collection)

  private def byKey(rows: Seq[(Int, Long)]): Seq[(Int, Long)] =
    rows.sortBy(_._1)

  private def byTriple(rows: Seq[(Int, Int, Long)]): Seq[(Int, Int, Long)] =
    rows.sortBy(_._1)

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

  private def intComponent(values: Seq[(Int, Long)], index: Int): Int =
    pairComponent(values, index, 0) match
      case value: Int => value
      case _ => fail(s"expected a present int key at $index")

  private def longComponent(values: Seq[(Int, Long)], index: Int): Long =
    pairComponent(values, index, 1) match
      case value: Long => value
      case _ => fail(s"expected a present long value at $index")

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def componentFor(values: Seq[(Int, Long)], key: Int, ordinal: Int): Any =
    val row = values.iterator
      .map(_.asInstanceOf[Product])
      .find { product =>
        product.productElement(0) match
          case found: Int => found == key
          case _ => false
      }
      .getOrElse(fail(s"missing key $key"))
    row.productElement(ordinal)

  private def longFor(values: Seq[(Int, Long)], key: Int): Long =
    componentFor(values, key, 1) match
      case value: Long => value
      case _ => fail(s"expected a present long value for key $key")
