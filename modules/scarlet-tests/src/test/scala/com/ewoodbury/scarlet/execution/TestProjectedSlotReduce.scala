package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * One int slot above an int-pair `Project`, reduced without publishing the pair batch.
 *
 * The adapter runs on every input row, including a null key and a null value. The slot then sees
 * the projected pair. A null projected key is dropped and the key predicate is not called. A null
 * projected value is dropped and does not create a group, and the value predicate is not called.
 * [[HashReduce.byKey]] does not see the projected columns. Collect of the filtered map still
 * writes them.
 *
 * `filterValues(_ >= 20)` on `(k + 1, v)` keeps `(2, 30)`, so the groups are
 * `{2 -> 30, 3 -> 20, 4 -> 40}`.
 */
class TestProjectedSlotReduce extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true, defaultShufflePartitions = 4))
    ScarletRuntime.get.shuffle.clear()
    IntPairProject.resetPublishedForTest()
    HashReduce.resetByKeyForTest()
    MapIntKey.resetPublishedForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  it should "reduce filterKeys above an int pair map without publishing" in {
    val source = pairs()
    val mapped = source.map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
    val reduced = mapped.filterKeys((key: Int) => key > 2).reduceByKey[Int, Int](_ + _)
    shape(reduced) shouldBe "HashAggregate(Exchange(FilterKey(Project(Scan))))"
    val expected = Map(3 -> 20, 4 -> 40)
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      IntPairProject.resetPublishedForTest()
      HashReduce.resetByKeyForTest()
      val sink = new CountingSink
      val found = ColumnMetrics.withSink(sink)(reduced.collect().toMap)
      IntPairProject.publishedForTest shouldBe 0
      HashReduce.byKeyCallsForTest shouldBe 0
      sink.fusedCount should be > 0L
      found
    }
    val summed = atBatch(8192)
    summed shouldBe expected
    atBatch(1) shouldBe summed
    summed shouldBe row(reduced.collect().toMap)

    IntPairProject.resetPublishedForTest()
    val projected = mapped.filterKeys((key: Int) => key > 2).collect()
    IntPairProject.publishedForTest should be > 0
    projected.toList.sorted shouldBe List((3, 20), (4, 40))
  }

  it should "reduce filterValues above an int pair map without publishing" in {
    val source = pairs()
    val mapped = source.map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
    val reduced = mapped.filterValues((value: Int) => value >= 20).reduceByKey[Int, Int](_ + _)
    shape(reduced) shouldBe "HashAggregate(Exchange(FilterValue(Project(Scan))))"
    val expected = Map(2 -> 30, 3 -> 20, 4 -> 40)
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      IntPairProject.resetPublishedForTest()
      HashReduce.resetByKeyForTest()
      val sink = new CountingSink
      val found = ColumnMetrics.withSink(sink)(reduced.collect().toMap)
      IntPairProject.publishedForTest shouldBe 0
      HashReduce.byKeyCallsForTest shouldBe 0
      sink.fusedCount should be > 0L
      found
    }
    val summed = atBatch(8192)
    summed shouldBe expected
    atBatch(1) shouldBe summed
    summed shouldBe row(reduced.collect().toMap)
  }

  it should "pass a null input key to the map and keep the row-path sums" in {
    val calls = new AtomicInteger
    val reduced = nullIntKeySource()
      .map(shiftNullKey(calls, nullKey = 7))
      .filterKeys((key: Int) => key > 0)
      .reduceByKey[Int, Int](_ + _)
    shape(reduced) shouldBe "HashAggregate(Exchange(FilterKey(Project(Scan))))"
    val nullAt = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      IntPairProject.resetPublishedForTest()
      HashReduce.resetByKeyForTest()
      val found = reduced.collect().toMap
      IntPairProject.publishedForTest shouldBe 0
      HashReduce.byKeyCallsForTest shouldBe 0
      (found, calls.get)
    }
    val (sums, seen) = nullAt(8192)
    sums shouldBe Map(2 -> 9, 3 -> 5, 7 -> 17)
    nullAt(1) shouldBe (sums, seen)
    calls.set(0)
    row(reduced.collect().toMap) shouldBe sums
    calls.get shouldBe seen
  }

  it should "drop a null projected value and still call the key predicate" in {
    val calls = new AtomicInteger
    val preds = new AtomicInteger
    val reduced = nullIntValueSource()
      .map(shiftNullKey(calls, nullKey = 7))
      .filterKeys { (key: Int) =>
        val _ = preds.incrementAndGet()
        key > 0
      }
      .reduceByKey[Int, Int](_ + _)
    shape(reduced) shouldBe "HashAggregate(Exchange(FilterKey(Project(Scan))))"
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      preds.set(0)
      IntPairProject.resetPublishedForTest()
      HashReduce.resetByKeyForTest()
      val found = reduced.collect().toMap
      IntPairProject.publishedForTest shouldBe 0
      HashReduce.byKeyCallsForTest shouldBe 0
      (found, calls.get, preds.get)
    }
    val (sums, seen, kept) = atBatch(8192)
    sums shouldBe Map(5 -> 4)
    seen shouldBe 2
    kept shouldBe 2
    atBatch(1) shouldBe (sums, seen, kept)
    // The row path keeps the null value as a group. This arm matches the unpublished project
    // reduce: the value is dropped and key 2 is not created. Both paths still call the map and
    // the key predicate.
    calls.set(0)
    preds.set(0)
    val _ = row(reduced.collect())
    calls.get shouldBe seen
    preds.get shouldBe kept
  }

  it should "drop a null projected key without calling the key predicate" in {
    val calls = new AtomicInteger
    val preds = new AtomicInteger
    val reduced = pairs()
      .map(dropIntKey(calls, dropped = 2))
      .filterKeys { (key: Int) =>
        val _ = preds.incrementAndGet()
        key > 0
      }
      .reduceByKey[Int, Int](_ + _)
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      preds.set(0)
      IntPairProject.resetPublishedForTest()
      HashReduce.resetByKeyForTest()
      val found = reduced.collect().toMap
      IntPairProject.publishedForTest shouldBe 0
      HashReduce.byKeyCallsForTest shouldBe 0
      (found, calls.get, preds.get)
    }
    val (sums, seen, kept) = atBatch(8192)
    sums shouldBe Map(1 -> 40, 3 -> 40)
    seen shouldBe 4
    kept shouldBe 3
    atBatch(1) shouldBe (sums, seen, kept)
  }

  it should "call a value predicate on a null projected key and create no group" in {
    val calls = new AtomicInteger
    val preds = new AtomicInteger
    val reduced = pairs()
      .map(dropIntKey(calls, dropped = 2))
      .filterValues { (value: Int) =>
        val _ = preds.incrementAndGet()
        value >= 20
      }
      .reduceByKey[Int, Int](_ + _)
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      preds.set(0)
      IntPairProject.resetPublishedForTest()
      HashReduce.resetByKeyForTest()
      val found = reduced.collect().toMap
      IntPairProject.publishedForTest shouldBe 0
      HashReduce.byKeyCallsForTest shouldBe 0
      (found, calls.get, preds.get)
    }
    val (sums, seen, kept) = atBatch(8192)
    sums shouldBe Map(1 -> 30, 3 -> 40)
    seen shouldBe 4
    kept shouldBe 4
    atBatch(1) shouldBe (sums, seen, kept)
    calls.set(0)
    preds.set(0)
    row(reduced.collect().toMap) shouldBe sums
    calls.get shouldBe seen
    preds.get shouldBe kept
  }

  it should "skip a null projected value and not call the value predicate" in {
    val calls = new AtomicInteger
    val preds = new AtomicInteger
    val reduced = nullIntValueSource()
      .map(shiftNullKey(calls, nullKey = 7))
      .filterValues { (value: Int) =>
        val _ = preds.incrementAndGet()
        value >= 0
      }
      .reduceByKey[Int, Int](_ + _)
    calls.set(0)
    preds.set(0)
    IntPairProject.resetPublishedForTest()
    HashReduce.resetByKeyForTest()
    val found = reduced.collect().toMap
    calls.get shouldBe 2
    preds.get shouldBe 1
    found shouldBe Map(5 -> 4)
    IntPairProject.publishedForTest shouldBe 0
    HashReduce.byKeyCallsForTest shouldBe 0
  }

  it should "reduce one slot above a filtered int pair map without publishing" in {
    val reduced = pairs()
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .map { case (key, value) => (key % 100, value) }
      .filterKeys((key: Int) => key < 50)
      .reduceByKey[Int, Int](_ + _)
    shape(reduced) shouldBe
      "HashAggregate(Exchange(FilterKey(Project(FilterValue(FilterKey(Scan))))))"
    val found = reduced.collect().toMap
    IntPairProject.publishedForTest shouldBe 0
    HashReduce.byKeyCallsForTest shouldBe 0
    found shouldBe Map(1 -> 40, 2 -> 20)
    found shouldBe row(reduced.collect().toMap)
  }

  it should "keep a second slot, a whole-row filter, and a second map published" in {
    val source = pairs()
    val shifted = source.map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
    val twice = shifted
      .filterKeys((key: Int) => key > 2)
      .filterValues((value: Int) => value >= 20)
      .reduceByKey[Int, Int](_ + _)
    shape(twice) shouldBe "HashAggregate(Exchange(FilterValue(FilterKey(Project(Scan)))))"
    val stacked = collectPublished(twice)
    stacked shouldBe Map(3 -> 20, 4 -> 40)
    stacked shouldBe row(twice.collect().toMap)

    val whole = shifted
      .filter((pair: (Int, Int)) => pair._1 > 2 && pair._2 >= 20)
      .reduceByKey[Int, Int](_ + _)
    shape(whole) shouldBe "HashAggregate(Exchange(FilterRow(Project(Scan))))"
    val rows = collectPublished(whole)
    rows shouldBe Map(3 -> 20, 4 -> 40)
    rows shouldBe row(whole.collect().toMap)

    val nested = source
      .map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
      .map((pair: (Int, Int)) => (pair._1 * 2, pair._2))
      .filterKeys((key: Int) => key > 2)
      .reduceByKey[Int, Int](_ + _)
    shape(nested) shouldBe "HashAggregate(Exchange(FilterKey(Project(Project(Scan)))))"
    val doubled = collectPublished(nested)
    doubled shouldBe Map(4 -> 40, 6 -> 20, 8 -> 40)
    doubled shouldBe row(nested.collect().toMap)
  }

  it should "reduce one slot above mapIntKey without publishing the pair batch" in {
    val source = pairs()
    val mapped = source.mapIntKey((key: Int) => key + 1)
    val reduced = mapped.filterKeys((key: Int) => key > 2).reduceByKey[Int, Int](_ + _)
    shape(reduced) shouldBe "HashAggregate(Exchange(FilterKey(MapKey(Scan))))"
    val expected = Map(3 -> 20, 4 -> 40)
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      MapIntKey.resetPublishedForTest()
      IntPairProject.resetPublishedForTest()
      HashReduce.resetByKeyForTest()
      val found = reduced.collect().toMap
      MapIntKey.publishedForTest shouldBe 0
      IntPairProject.publishedForTest shouldBe 0
      HashReduce.byKeyCallsForTest shouldBe 0
      found
    }
    atBatch(8192) shouldBe expected
    atBatch(1) shouldBe expected
    atBatch(8192) shouldBe row(reduced.collect().toMap)

    MapIntKey.resetPublishedForTest()
    val projected = mapped.filterKeys((key: Int) => key > 2).collect()
    MapIntKey.publishedForTest should be > 0
    projected.toMap shouldBe expected

    val byValue = mapped.filterValues((value: Int) => value >= 20).reduceByKey[Int, Int](_ + _)
    shape(byValue) shouldBe "HashAggregate(Exchange(FilterValue(MapKey(Scan))))"
    val values = Map(2 -> 30, 3 -> 20, 4 -> 40)
    val atValue = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      MapIntKey.resetPublishedForTest()
      HashReduce.resetByKeyForTest()
      val found = byValue.collect().toMap
      MapIntKey.publishedForTest shouldBe 0
      HashReduce.byKeyCallsForTest shouldBe 0
      found
    }
    atValue(8192) shouldBe values
    atValue(1) shouldBe values
    atValue(8192) shouldBe row(byValue.collect().toMap)
  }

  it should "skip a null key above mapIntKey and still test a value predicate" in {
    val mapCalls = new AtomicInteger
    val keyCalls = new AtomicInteger
    val reduced = nullIntKeySource()
      .mapIntKey { key =>
        val _ = mapCalls.incrementAndGet()
        key + 1
      }
      .filterKeys { (key: Int) =>
        val _ = keyCalls.incrementAndGet()
        key > 2
      }
      .reduceByKey[Int, Int](_ + _)
    shape(reduced) shouldBe "HashAggregate(Exchange(FilterKey(MapKey(Scan))))"
    mapCalls.set(0)
    keyCalls.set(0)
    MapIntKey.resetPublishedForTest()
    HashReduce.resetByKeyForTest()
    val found = reduced.collect().toMap
    mapCalls.get shouldBe 4
    keyCalls.get shouldBe 4
    MapIntKey.publishedForTest shouldBe 0
    HashReduce.byKeyCallsForTest shouldBe 0
    found shouldBe Map(3 -> 5)
    mapCalls.set(0)
    keyCalls.set(0)
    row(reduced.collect().toMap) shouldBe found
    mapCalls.get shouldBe 4

    val valueCalls = new AtomicInteger
    val byValue = nullIntKeySource()
      .mapIntKey { key =>
        val _ = mapCalls.incrementAndGet()
        key + 1
      }
      .filterValues { (value: Int) =>
        val _ = valueCalls.incrementAndGet()
        value >= 0
      }
      .reduceByKey[Int, Int](_ + _)
    mapCalls.set(0)
    valueCalls.set(0)
    MapIntKey.resetPublishedForTest()
    HashReduce.resetByKeyForTest()
    val kept = byValue.collect().toMap
    mapCalls.get shouldBe 4
    valueCalls.get shouldBe 6
    MapIntKey.publishedForTest shouldBe 0
    HashReduce.byKeyCallsForTest shouldBe 0
    kept shouldBe Map(2 -> 9, 3 -> 6)
    mapCalls.set(0)
    valueCalls.set(0)
    row(byValue.collect().toMap) shouldBe kept
    mapCalls.get shouldBe 4
    valueCalls.get shouldBe 6

    val nullValues = nullIntValueSource()
      .mapIntKey { key =>
        val _ = mapCalls.incrementAndGet()
        key * 2
      }
      .filterKeys { (key: Int) =>
        val _ = keyCalls.incrementAndGet()
        key > 0
      }
      .reduceByKey[Int, Int](_ + _)
    mapCalls.set(0)
    keyCalls.set(0)
    MapIntKey.resetPublishedForTest()
    HashReduce.resetByKeyForTest()
    val dropped = nullValues.collect().toMap
    mapCalls.get shouldBe 2
    keyCalls.get shouldBe 2
    MapIntKey.publishedForTest shouldBe 0
    HashReduce.byKeyCallsForTest shouldBe 0
    dropped shouldBe Map(8 -> 4)
    /* The row reduce keeps a lone null value. This arm drops it, as the unfiltered map reduce
     * does. */
    mapCalls.set(0)
    keyCalls.set(0)
    val _ = row(nullValues.collect())
    mapCalls.get shouldBe 2
    keyCalls.get shouldBe 2

    val nullValueCalls = new AtomicInteger
    val byNullValue = nullIntValueSource()
      .mapIntKey { key =>
        val _ = mapCalls.incrementAndGet()
        key * 2
      }
      .filterValues { (value: Int) =>
        val _ = nullValueCalls.incrementAndGet()
        value >= 0
      }
      .reduceByKey[Int, Int](_ + _)
    mapCalls.set(0)
    nullValueCalls.set(0)
    MapIntKey.resetPublishedForTest()
    HashReduce.resetByKeyForTest()
    val presentOnly = byNullValue.collect().toMap
    mapCalls.get shouldBe 2
    nullValueCalls.get shouldBe 1
    MapIntKey.publishedForTest shouldBe 0
    HashReduce.byKeyCallsForTest shouldBe 0
    presentOnly shouldBe Map(8 -> 4)
  }

  it should "reduce one slot above a filtered mapIntKey without publishing" in {
    val reduced = pairs()
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 2)
      .reduceByKey[Int, Int](_ + _)
    shape(reduced) shouldBe
      "HashAggregate(Exchange(FilterKey(MapKey(FilterValue(FilterKey(Scan))))))"
    MapIntKey.resetPublishedForTest()
    HashReduce.resetByKeyForTest()
    val found = reduced.collect().toMap
    MapIntKey.publishedForTest shouldBe 0
    HashReduce.byKeyCallsForTest shouldBe 0
    found shouldBe Map(3 -> 20)
    found shouldBe row(reduced.collect().toMap)
  }

  it should "leave a second map above mapIntKey on the published byKey path" in {
    val reduced = pairs()
      .mapIntKey((key: Int) => key + 1)
      .mapIntKey((key: Int) => key * 2)
      .filterKeys((key: Int) => key > 2)
      .reduceByKey[Int, Int](_ + _)
    shape(reduced) shouldBe "HashAggregate(Exchange(FilterKey(MapKey(MapKey(Scan)))))"
    MapIntKey.resetPublishedForTest()
    HashReduce.resetByKeyForTest()
    val found = reduced.collect().toMap
    MapIntKey.publishedForTest should be > 0
    HashReduce.byKeyCallsForTest should be > 0
    found shouldBe Map(4 -> 40, 6 -> 20, 8 -> 40)
    found shouldBe row(reduced.collect().toMap)

    val stacked = pairs()
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 2)
      .filterValues((value: Int) => value >= 20)
      .reduceByKey[Int, Int](_ + _)
    shape(stacked) shouldBe
      "HashAggregate(Exchange(FilterValue(FilterKey(MapKey(Scan)))))"
    stacked.collect().toMap shouldBe row(stacked.collect().toMap)

    val whole = pairs()
      .mapIntKey((key: Int) => key + 1)
      .filter((pair: (Int, Int)) => pair._1 > 2 && pair._2 >= 20)
      .reduceByKey[Int, Int](_ + _)
    ColumnarPlanner.lower(whole.plan) shouldBe None
    whole.collect().toMap shouldBe row(whole.collect().toMap)
  }

  private def pairs(): DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 1)

  /** The arm did not run: the project was published and nothing was fused into the reduce. */
  private def collectPublished(reduced: DistCollection[(Int, Int)]): Map[Int, Int] =
    ScarletRuntime.get.shuffle.clear()
    IntPairProject.resetPublishedForTest()
    HashReduce.resetByKeyForTest()
    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink)(reduced.collect().toMap)
    IntPairProject.publishedForTest should be > 0
    sink.fusedCount shouldBe 0L
    found

  private def shape(collection: DistCollection[_]): String =
    ColumnarPlanner
      .lower(collection.plan)
      .map(_.shape)
      .getOrElse(fail("plan stayed on the row path"))

  /**
   * A null key becomes `nullKey`. A present key is incremented. `productElement` keeps a null
   * value, so both paths see it.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def shiftNullKey(calls: AtomicInteger, nullKey: Int): ((Int, Int)) => (Int, Int) =
    pair => {
      val _ = calls.incrementAndGet()
      val product = pair.asInstanceOf[Product]
      val value = product.productElement(1)
      val key = product.productElement(0) match
        case null => nullKey
        case present: Int => present + 1
        case other => throw new IllegalArgumentException(s"pair map saw $other")
      Tuple2(Int.box(key), value).asInstanceOf[(Int, Int)]
    }

  /** A present key equal to `dropped` becomes null. The value stays. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def dropIntKey(calls: AtomicInteger, dropped: Int): ((Int, Int)) => (Int, Int) =
    pair => {
      val _ = calls.incrementAndGet()
      val key = if (pair._1 == dropped) null else Int.box(pair._1)
      Tuple2(key, Int.box(pair._2)).asInstanceOf[(Int, Int)]
    }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullIntValueSource(): DistCollection[(Int, Int)] =
    def cell(key: Int, value: Any): (Int, Int) =
      Tuple2(Int.box(key), value).asInstanceOf[(Int, Int)]
    DistCollection(Plan.Source(Seq(Partition(Seq(cell(4, 4), cell(1, null))))))

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullIntKeySource(): DistCollection[(Int, Int)] =
    def cell(key: Any, value: Int): (Int, Int) =
      Tuple2(key, Int.box(value)).asInstanceOf[(Int, Int)]
    DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq(cell(1, 4), cell(null, 9), cell(1, 5), cell(2, -1))),
          Partition(Seq(cell(2, 6), cell(null, 8))),
        ),
      ),
    )

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
