package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.Selection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Columnar `filterKeys` and `filterValues` on `(Long, Long)`.
 *
 * Present rows match the row path. A null key or value does not: the row path unboxes it to zero
 * and may keep the row, and the columnar kernel drops that row without calling the predicate. Read
 * a null cell with `productElement`. A `shouldBe` against a `(Long, Long)` literal unboxes it.
 */
class TestLongPairFilter extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get
  private val aboveIntMax = Int.MaxValue.toLong + 1L

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "filterKeys" should "drop a null key, keep a null value, and call the predicate twice" in {
    val calls = new AtomicInteger
    val filtered = nullable.filterKeys[Long, Long] { key =>
      calls.incrementAndGet()
      key >= 0L
    }
    val got = columnar(filtered)
    val first = pairAt(got, 0)
    val second = pairAt(got, 1)

    // Input order is present zero, null value, null key. Length 2 drops the null key.
    got.length shouldBe 2
    longComponent(first, 0) shouldBe 0L
    longComponent(first, 1) shouldBe 1L
    longComponent(second, 0) shouldBe 2L
    absentRef(component(second, 1)) shouldBe true
    calls.get() shouldBe 2
    assertColumnar(filtered, "FilterKey(Scan)")
  }

  "filterValues" should "drop a null value, keep a null key, and call the predicate twice" in {
    val calls = new AtomicInteger
    val filtered = nullable.filterValues[Long, Long] { value =>
      calls.incrementAndGet()
      value >= 0L
    }
    val got = columnar(filtered)
    val first = pairAt(got, 0)
    val second = pairAt(got, 1)

    // The null value is the missing row. The null key stays.
    got.length shouldBe 2
    longComponent(first, 0) shouldBe 0L
    longComponent(first, 1) shouldBe 1L
    absentRef(component(second, 0)) shouldBe true
    longComponent(second, 1) shouldBe 7L
    calls.get() shouldBe 2
    assertColumnar(filtered, "FilterValue(Scan)")
  }

  "present rows" should "match the row path for both filters" in {
    val source = DistCollection.ofLongPairs(
      Array(-2L, 0L, 5L, aboveIntMax),
      Array(4L, -1L, 0L, 8L),
      1,
    )
    val keyed = source.filterKeys[Long, Long](_ >= 0L)
    val valued = source.filterValues[Long, Long](_ >= 0L)

    columnar(keyed) shouldBe row(keyed.collect().toList)
    columnar(valued) shouldBe row(valued.collect().toList)
    assertColumnar(keyed, "FilterKey(Scan)")
    assertColumnar(valued, "FilterValue(Scan)")
  }

  "two filters" should "chain in either order" in {
    val source = DistCollection.ofLongPairs(
      Array(1L, 2L, -4L, 8L),
      Array(10L, -3L, 9L, 1L),
      1,
    )
    val keyThenValue = source.filterKeys[Long, Long](_ > 0L).filterValues[Long, Long](_ > 0L)
    val valueThenKey = source.filterValues[Long, Long](_ > 0L).filterKeys[Long, Long](_ > 0L)

    columnar(keyThenValue) shouldBe row(keyThenValue.collect().toList)
    columnar(valueThenKey) shouldBe row(valueThenKey.collect().toList)
    assertColumnar(keyThenValue, "FilterValue(FilterKey(Scan))")
    assertColumnar(valueThenKey, "FilterKey(FilterValue(Scan))")
  }

  it should "keep a clear value bit through a two-filter collect" in {
    // Two key filters, so the null value on key 2 survives the index. A decoder that rejects a
    // clear bit leaves this collect. A decoder that stores the null as zero fails the absent check.
    val filtered = nullable
      .filterKeys[Long, Long]((key: Long) => key >= 0L)
      .filterKeys[Long, Long]((key: Long) => key < 10L)
    val op = ColumnarPlanner.lower(filtered.plan).getOrElse(fail("two key filters did not lower"))
    val compiled =
      PredicatePlan.compile(filtered.plan, op).getOrElse(fail("stack did not compile"))
    val steps =
      PredicatePlan.selectionSteps(compiled).getOrElse(fail("long key filters have no steps"))

    steps.length shouldBe 2
    steps.foreach {
      case Selection.Step.Int64(ordinal, _) => ordinal shouldBe 0
      case other => fail(s"expected an Int64 key step, got $other")
    }
    FilterChain.selected(filtered.plan, op, None, false, Set(0, 1)) shouldBe defined

    val got = columnar(filtered)
    got.length shouldBe 2
    val first = pairAt(got, 0)
    val second = pairAt(got, 1)
    longComponent(first, 0) shouldBe 0L
    longComponent(first, 1) shouldBe 1L
    longComponent(second, 0) shouldBe 2L
    absentRef(component(second, 1)) shouldBe true
    val taken = ColumnarPlanner
      .tryTake(filtered.plan, 2)
      .getOrElse(fail("take left the columnar path"))
    taken.length shouldBe 2
    absentRef(component(pairAt(taken, 1), 1)) shouldBe true

    val once = nullable.filterKeys[Long, Long]((key: Long) => key >= 0L)
    val onceOp = ColumnarPlanner.lower(once.plan).getOrElse(fail("one key filter did not lower"))
    FilterChain.selected(once.plan, onceOp, None, false, Set(0, 1)) shouldBe defined
    val onceRows = columnar(once)
    onceRows.length shouldBe 2
    longComponent(pairAt(onceRows, 0), 0) shouldBe 0L
    longComponent(pairAt(onceRows, 0), 1) shouldBe 1L
    longComponent(pairAt(onceRows, 1), 0) shouldBe 2L
    absentRef(component(pairAt(onceRows, 1), 1)) shouldBe true

    val rows = nullable
      .filter((_: (Long, Long)) => true)
      .filter((_: (Long, Long)) => true)
    val rowOp = ColumnarPlanner.lower(rows.plan).getOrElse(fail("row filters did not lower"))
    PredicatePlan.compile(rows.plan, rowOp).flatMap(PredicatePlan.selectionSteps) shouldBe None
    FilterChain.selected(rows.plan, rowOp, None, false, Set(0, 1)) shouldBe None
  }

  "a whole-row filter" should "lower to FilterRow(Scan)" in {
    val source = DistCollection.ofLongPairs(Array(-1L, 2L, 0L), Array(5L, -6L, 7L), 1)
    val filtered = source.filter((pair: (Long, Long)) => pair._1 >= 0L)

    columnar(filtered) shouldBe row(filtered.collect().toList)
    assertColumnar(filtered, "FilterRow(Scan)")
  }

  "filterValues above mapValues" should "lower to FilterValue(MapValue(Scan))" in {
    val source = DistCollection.ofLongPairs(Array(1L, 2L, 3L), Array(10L, -2L, 0L), 1)
    val filtered = source.mapValues[Long, Long, Long](_ + 1L).filterValues[Long, Long](_ > 0L)

    columnar(filtered) shouldBe row(filtered.collect().toList)
    assertColumnar(filtered, "FilterValue(MapValue(Scan))")
  }

  "filterKeys above reduceByKey" should "lower to FilterKey(LongHashAggregate(Exchange(Scan)))" in {
    val source = DistCollection.ofLongPairs(Array(1L, 1L, -3L, 4L), Array(10L, 5L, 7L, 2L), 2)
    val filtered = source.reduceByKey[Long, Long](_ + _).filterKeys[Long, Long](_ > 0L)

    columnar(filtered) should contain theSameElementsAs row(filtered.collect().toList)
    assertColumnar(filtered, "FilterKey(LongHashAggregate(Exchange(Scan)))")
  }

  "filterKeys above a map, a long sort, or a long join" should "stay empty" in {
    val source = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 1)
    val mapped = source.map((pair: (Long, Long)) => (pair._1, pair._2))
    val sorted = source.sortByLongKey
    val joined = source.join(source)

    assertEmpty(mapped.filterKeys[Long, Long](_ > 0L))
    assertEmpty(sorted.filterKeys[Long, Long](_ > 0L))
    assertEmpty(joined.filterKeys[Long, (Long, Long)]((key: Long) => key > 0L))
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some("SortByLongKey(Scan)")
    ColumnarPlanner.lower(joined.plan).map(_.shape) shouldBe Some(
      "LongHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    assertEmpty(mapped)
  }

  "an empty input" should "lower and call nothing" in {
    val empty = DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], 2)
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val keyed = empty.filterKeys[Long, Long] { key =>
      keyCalls.incrementAndGet()
      key >= 0L
    }
    val valued = empty.filterValues[Long, Long] { value =>
      valueCalls.incrementAndGet()
      value >= 0L
    }

    columnar(keyed) shouldBe Seq.empty[(Long, Long)]
    columnar(valued) shouldBe Seq.empty[(Long, Long)]
    keyCalls.get() shouldBe 0
    valueCalls.get() shouldBe 0
    assertColumnar(keyed, "FilterKey(Scan)")
    assertColumnar(valued, "FilterValue(Scan)")
  }

  "a rejecting predicate" should "drop every row" in {
    val source = DistCollection.ofLongPairs(Array(1L, aboveIntMax), Array(10L, 20L), 1)
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val keyed = source.filterKeys[Long, Long] { _ =>
      keyCalls.incrementAndGet()
      false
    }
    val valued = source.filterValues[Long, Long] { _ =>
      valueCalls.incrementAndGet()
      false
    }

    columnar(keyed) shouldBe Seq.empty[(Long, Long)]
    columnar(valued) shouldBe Seq.empty[(Long, Long)]
    keyCalls.get() shouldBe 2
    valueCalls.get() shouldBe 2
    assertColumnar(keyed, "FilterKey(Scan)")
    assertColumnar(valued, "FilterValue(Scan)")
  }

  "an accepting predicate" should "keep every present row" in {
    val source = DistCollection.ofLongPairs(
      Array(-5L, 0L, aboveIntMax),
      Array(1L, 2L, 3L),
      1,
    )
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val keyed = source.filterKeys[Long, Long] { _ =>
      keyCalls.incrementAndGet()
      true
    }
    val valued = source.filterValues[Long, Long] { _ =>
      valueCalls.incrementAndGet()
      true
    }
    val expected = Seq((-5L, 1L), (0L, 2L), (aboveIntMax, 3L))

    columnar(keyed) shouldBe expected
    columnar(valued) shouldBe expected
    keyCalls.get() shouldBe 3
    valueCalls.get() shouldBe 3
    assertColumnar(keyed, "FilterKey(Scan)")
    assertColumnar(valued, "FilterValue(Scan)")
  }

  "a negative key" should "fail a non-negative key predicate" in {
    val source = DistCollection.ofLongPairs(Array(-8L, 0L, 3L), Array(1L, 2L, 4L), 1)
    val filtered = source.filterKeys[Long, Long](_ >= 0L)

    columnar(filtered) shouldBe Seq((0L, 2L), (3L, 4L))
    assertColumnar(filtered, "FilterKey(Scan)")
  }

  "a key above Int.MaxValue" should "pass a Long predicate" in {
    val ceiling = Int.MaxValue.toLong
    val source = DistCollection.ofLongPairs(
      Array(1L, ceiling, aboveIntMax),
      Array(10L, 20L, 30L),
      1,
    )
    val filtered = source.filterKeys[Long, Long]((key: Long) => key > Int.MaxValue)

    columnar(filtered) shouldBe Seq((aboveIntMax, 30L))
    assertColumnar(filtered, "FilterKey(Scan)")
  }

  /** Present zero, a null value, and a null key. One partition, input order. */
  private def nullable: DistCollection[(Long, Long)] =
    DistCollection(
      Seq(
        pair(longRef(0L), longRef(1L)),
        pair(longRef(2L), absent),
        pair(absent, longRef(7L)),
      ),
      1,
    )

  private def columnar[A](collection: DistCollection[A]): Seq[A] =
    val ran = ColumnarPlanner
      .tryRun(collection.plan)
      .getOrElse(fail(s"stayed on the row path: ${collection.plan}"))
    ran.flatMap(_.data).toList

  private def assertColumnar(collection: DistCollection[_], shape: String): Unit =
    ColumnarPlanner.lower(collection.plan).map(_.shape) shouldBe Some(shape)
    ColumnarPlanner.partitioning(collection.plan).map(_.layout) shouldBe Some(
      Layout.Columnar(ScarletConf.get.columnarBatchSize),
    )

  private def assertEmpty(collection: DistCollection[_]): Unit =
    ColumnarPlanner.lower(collection.plan) shouldBe Option.empty[PhysicalOp]

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
  private def pair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absent: java.lang.Long = null

  private def longRef(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def component(row: (Long, Long), ordinal: Int): Any =
    row.productElement(ordinal)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def absentRef(value: Any): Boolean =
    value.asInstanceOf[AnyRef] eq null

  private def longComponent(row: (Long, Long), ordinal: Int): Long = component(row, ordinal) match
    case value: Long => value
    case _ => fail(s"expected a present long at ordinal $ordinal")

  private def pairAt(values: Seq[(Long, Long)], index: Int): (Long, Long) =
    values.iterator.drop(index).next()
