package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.ColumnarFallback
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Row-path oracle for a long pair map, a string pair map, and an int pair map under
 * `sortByIntKey`.
 *
 * A null key is passed to the pair function. `mapIntKey` is not this path. A string key the input
 * dictionary does not hold declines. A map above the sort, and a long or string map under its own
 * sort, stay refused.
 */
class TestPairMapKinds extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val dictionaryMiss =
    "string pair map cannot share the input dictionary: the key is not in it"

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "a long pair map" should "match the row path, including a null key the function rewrites" in {
    val calls = new AtomicInteger
    val mapped = longNullable.map(rewriteLong(calls, nullKey = 7L))
    val columnar = agree(mapped, calls, expectedCalls = 3)
    checksum(columnar) shouldBe checksum(rowCollect(mapped))
    fingerprint(columnar) shouldBe Seq("2:10", "7:4", "9:null")

    val counted = longNullable.map(rewriteLong(calls, nullKey = 7L)).countByKey
    calls.set(0)
    val columnarCounts = counted.collect().toMap
    calls.get() shouldBe 3
    calls.set(0)
    columnarCounts shouldBe rowCollect(counted).toMap
    calls.get() shouldBe 3
    columnarCounts shouldBe Map(2L -> 1, 7L -> 1, 9L -> 1)
    shape(counted) shouldBe "CountByKey(Exchange(Project(Scan)))"
    countChecksum(columnarCounts) shouldBe countChecksum(rowCollect(counted).toMap)
  }

  it should "reduce and read keys of a present-key map" in {
    val source = DistCollection.ofLongPairs(Array(1L, 1L, 2L), Array(10L, 5L, 7L), 2)
    val mapped = source.map((pair: (Long, Long)) => (pair._1 % 10L, pair._2 + 1L))
    val reduced = mapped.reduceByKey[Long, Long](_ + _)
    reduced.collect().toMap shouldBe rowCollect(reduced).toMap
    shape(reduced) shouldBe "LongHashAggregate(Exchange(Project(Scan)))"
    mapped.keys.collect().toSeq shouldBe rowCollect(mapped.keys)
    shape(mapped.keys) shouldBe "Key(Project(Scan))"
  }

  "a string pair map" should "share the dictionary when the key stays and match the row path" in {
    val calls = new AtomicInteger
    val mapped = stringNullable.map(rewriteString(calls))
    val columnar = agree(mapped, calls, expectedCalls = 4)
    checksum(columnar) shouldBe checksum(rowCollect(mapped))
    fingerprint(columnar) shouldBe Seq("b:2", "a:3", "a:4", "b:null")

    val counted = stringNullable.map(rewriteString(calls)).countByKey
    calls.set(0)
    val columnarCounts = counted.collect().toMap
    calls.get() shouldBe 4
    columnarCounts shouldBe rowCollect(counted).toMap
    columnarCounts shouldBe Map("b" -> 2, "a" -> 2)
    shape(counted) shouldBe "CountByKey(Exchange(Project(Scan)))"
  }

  it should "decline a key the dictionary does not hold and collect on the row path" in {
    val source = DistCollection(Seq("a" -> 1, "b" -> 2), 1)
    val invented = source.map((pair: (String, Int)) => (pair._1 + "!", pair._2))
    shape(invented) shouldBe "Project(Scan)"
    ColumnarPlanner.tryRun(invented.plan) shouldBe None
    val op = ColumnarPlanner.lower(invented.plan).getOrElse(fail("expected a project"))
    val thrown = intercept[ColumnarFallback] {
      val _ = ColumnarInterpreter.interpret(invented.plan, op)
    }
    thrown.getMessage shouldBe dictionaryMiss
    invented.collect().toSeq shouldBe rowCollect(invented)
  }

  "sortByIntKey of an int pair map" should "sort the mapped key, including a null key" in {
    val calls = new AtomicInteger
    val mapped = intNullable.map(rewriteIntSort(calls)).sortByIntKey
    val columnar = agree(mapped, calls, expectedCalls = 4)
    checksum(columnar) shouldBe checksum(rowCollect(mapped))
    fingerprint(columnar) shouldBe Seq("null:6", "1:-4", "8:0", "20:2")
    shape(mapped) shouldBe "SortByIntKey(Project(Scan))"

    val source = DistCollection.ofIntPairs(Array(2, 1, 2, 3), Array(20, 10, 20, 5), 2)
    val swapped = source.map((pair: (Int, Int)) => (pair._2, pair._1)).sortByIntKey
    val found = swapped.collect().toSeq
    found shouldBe rowCollect(swapped)
    checksum(found) shouldBe checksum(rowCollect(swapped))
    shape(swapped) shouldBe "SortByIntKey(Project(Scan))"
  }

  it should "sort a filter around the map when every key stays present" in {
    val source = DistCollection.ofIntPairs(Array(3, 1, 2, 4), Array(30, 10, 20, 15), 2)
    val filterThen = source
      .filter((pair: (Int, Int)) => pair._1 > 1)
      .map((pair: (Int, Int)) => (pair._2, pair._1))
      .sortByIntKey
    val mapThen = source
      .map((pair: (Int, Int)) => (pair._2, pair._1))
      .filter((pair: (Int, Int)) => pair._1 > 15)
      .sortByIntKey

    filterThen.collect().toSeq shouldBe rowCollect(filterThen)
    mapThen.collect().toSeq shouldBe rowCollect(mapThen)
    shape(filterThen) shouldBe "SortByIntKey(Project(FilterRow(Scan)))"
    shape(mapThen) shouldBe "SortByIntKey(FilterRow(Project(Scan)))"
  }

  "a map whose sort key cannot be resolved" should "stay on the row path" in {
    val ints = DistCollection.ofIntPairs(Array(3, 1, 2), Array(30, 10, 20), 2)
    val above = ints.sortByIntKey.map((pair: (Int, Int)) => (pair._2, pair._1))
    val values = ints.mapValues((value: Int) => value + 1).sortByIntKey
    val byValue = ints.map((pair: (Int, Int)) => (pair._2, pair._1)).sortByIntValueDesc
    val filtered = ints
      .map((pair: (Int, Int)) => (pair._1, pair._2))
      .sortByIntKey
      .filter((pair: (Int, Int)) => pair._2 > 0)
    val longs = DistCollection.ofLongPairs(Array(2L, 1L), Array(20L, 10L), 1)
    val longSorted = longs.map((pair: (Long, Long)) => (pair._2, pair._1)).sortByLongKey
    val strings = DistCollection(Seq("b" -> 1, "a" -> 2), 1)
    val stringSorted = strings.map((pair: (String, Int)) => (pair._1, pair._2 + 1)).sortByStringKey

    assertRefused(above)
    assertRefused(values)
    assertRefused(byValue)
    assertRefused(filtered)
    assertRefused(longSorted)
    assertRefused(stringSorted)
    above.collect().toSeq shouldBe rowCollect(above)
    longSorted.collect().toSeq shouldBe rowCollect(longSorted)
    stringSorted.collect().toSeq shouldBe rowCollect(stringSorted)
  }

  private def agree[A <: Product](
      collection: DistCollection[A],
      calls: AtomicInteger,
      expectedCalls: Int,
  ): Seq[A] =
    calls.set(0)
    val columnar = collection.collect().toSeq
    calls.get() shouldBe expectedCalls
    checksum(columnar) shouldBe checksum(rowCollect(collection))
    columnar shouldBe rowCollect(collection)
    columnar

  private def assertRefused(collection: DistCollection[_]): Unit =
    ColumnarPlanner.lower(collection.plan) shouldBe None
    ColumnarPlanner.partitioning(collection.plan) shouldBe None

  private def shape(collection: DistCollection[_]): String =
    ColumnarPlanner.lower(collection.plan).map(_.shape).getOrElse("row")

  private def longNullable: DistCollection[(Long, Long)] =
    DistCollection(
      Seq(
        longPair(Long.box(1L), Long.box(10L)),
        longPair(null, Long.box(4L)),
        longPair(Long.box(8L), null),
      ),
      2,
    )

  private def stringNullable: DistCollection[(String, Int)] =
    DistCollection(
      Seq(
        stringPair("b", Int.box(1)),
        stringPair("a", Int.box(2)),
        stringPair(null, Int.box(3)),
        stringPair("b", null),
      ),
      1,
    )

  private def intNullable: DistCollection[(Int, Int)] =
    DistCollection(
      Seq(
        intPair(Int.box(2), Int.box(20)),
        intPair(null, Int.box(5)),
        intPair(Int.box(-4), Int.box(1)),
        intPair(Int.box(0), Int.box(8)),
      ),
      2,
    )

  /** A null key becomes `nullKey`. A present key is incremented. A null value stays null. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def rewriteLong(
      calls: AtomicInteger,
      nullKey: Long,
  ): ((Long, Long)) => (Long, Long) = pair =>
    calls.incrementAndGet()
    val product = pair.asInstanceOf[Product]
    val value = product.productElement(1)
    product.productElement(0) match
      case null => longPair(Long.box(nullKey), value)
      case present: Long => longPair(Long.box(present + 1L), value)
      case other => fail(s"long pair map saw $other")

  /**
   * A null key becomes "a", already in the dictionary. Any other key stays. A present value is
   * incremented.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def rewriteString(calls: AtomicInteger): ((String, Int)) => (String, Int) = pair =>
    calls.incrementAndGet()
    val product = pair.asInstanceOf[Product]
    val key = product.productElement(0) match
      case null => "a"
      case present: String => present
      case other => fail(s"string pair map saw $other")
    val value = product.productElement(1) match
      case null => null
      case present: Int => Int.box(present + 1)
      case other => fail(s"string pair value saw $other")
    stringPair(key, value)

  /**
   * A null key stays null and the value is incremented, so the sort orders that key as zero. A
   * present key is replaced by the value. That order is not the order of the input key.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def rewriteIntSort(calls: AtomicInteger): ((Int, Int)) => (Int, Int) = pair =>
    calls.incrementAndGet()
    val product = pair.asInstanceOf[Product]
    val key = product.productElement(0)
    val value = product.productElement(1)
    key match
      case null =>
        val next = value match
          case present: Int => Int.box(present + 1)
          case _ => null
        intPair(null, next)
      case _: Int => intPair(value, key)
      case other => fail(s"int pair map saw $other")

  private def checksum(rows: Iterable[Product]): Long =
    rows.iterator.foldLeft(0L) { (total, row) =>
      total + component(row, 0) * 31L + component(row, 1)
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def countChecksum(rows: Map[?, Int]): Long =
    rows.toSeq
      .sortBy { case (key, _) => componentOf(key) }
      .foldLeft(0L) { case (total, (key, count)) =>
        total + componentOf(key) * 31L + count.toLong
      }

  private def component(row: Product, ordinal: Int): Long =
    componentOf(row.productElement(ordinal))

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def componentOf(value: Any): Long = value match
    case null => 0L
    case present: Int => present.toLong
    case present: Long => present
    case present: String => present.hashCode.toLong
    case other => fail(s"checksum saw $other")

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def fingerprint(rows: Iterable[Product]): Seq[String] =
    rows.map { row =>
      String.valueOf(row.productElement(0)) + ":" + String.valueOf(row.productElement(1))
    }.toSeq

  private def rowCollect[A](collection: DistCollection[A]): Seq[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      collection.collect().toSeq
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def longPair(key: Any, value: Any): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def stringPair(key: Any, value: Any): (String, Int) =
    Tuple2(key, value).asInstanceOf[(String, Int)]

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def intPair(key: Any, value: Any): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]
