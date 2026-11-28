package com.ewoodbury.scarlet.execution
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountSum, CountingSink, IntPairProject}
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.support.Check.shouldBeEmpty

/**
 * One int slot above an int-pair `Project`, counted and summed without a pair batch.
 *
 * The adapter runs on every input row, including a null key and a null value. The slot then sees
 * the projected pair. A null projected key is dropped and the key predicate is not called. A null
 * projected value that a key slot keeps is counted and not added. A value predicate is not called
 * on a null projected value. [[CountSum.byKey]] does not see the projected columns. Collect of the
 * filtered map still writes them.
 *
 * `filterValues(_ >= 20)` on `(k + 1, v)` keeps `(2, 30)`, so the groups are
 * `{2 -> (1, 30), 3 -> (1, 20), 4 -> (1, 40)}`.
 */
class TestProjectedSlotCountSum extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8192,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
        spillEnabled = true,
      ),
    )
    ScarletRuntime.get.shuffle.clear()
    IntPairProject.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    MapIntKey.resetPublishedForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "countSumByKey" should "fold filterKeys above an int pair map without publishing" in {
    val source = pairs()
    val mapped = source.map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
    val counted = mapped.filterKeys((key: Int) => key > 2).countSumByKey
    shape(counted) shouldBe "CountSum(Exchange(FilterKey(Project(Scan))))"
    val expected = Map(3 -> (1, 20L), 4 -> (1, 40L))
    val summed = atBatch(counted, 8192)
    summed shouldBe expected
    atBatch(counted, 1) shouldBe summed
    summed shouldBe row(groups(counted))

    IntPairProject.resetPublishedForTest()
    val projected = mapped.filterKeys((key: Int) => key > 2).collect()
    IntPairProject.publishedForTest should be > 0
    projected.toList.sorted shouldBe List((3, 20), (4, 40))
  }

  it should "fold filterValues above an int pair map without publishing" in {
    val source = pairs()
    val mapped = source.map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
    val counted = mapped.filterValues((value: Int) => value >= 20).countSumByKey
    shape(counted) shouldBe "CountSum(Exchange(FilterValue(Project(Scan))))"
    val expected = Map(2 -> (1, 30L), 3 -> (1, 20L), 4 -> (1, 40L))
    val summed = atBatch(counted, 8192)
    summed shouldBe expected
    atBatch(counted, 1) shouldBe summed
    summed shouldBe row(groups(counted))
  }

  it should "pass a null input key to the map and keep the row-path sums" in {
    val calls = new AtomicInteger
    val counted = nullIntKeySource()
      .map(shiftNullKey(calls, nullKey = 7))
      .filterKeys((key: Int) => key > 0)
      .countSumByKey
    shape(counted) shouldBe "CountSum(Exchange(FilterKey(Project(Scan))))"
    val nullAt = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      IntPairProject.resetPublishedForTest()
      CountSum.resetByKeyForTest()
      val found = groups(counted)
      IntPairProject.publishedForTest shouldBe 0
      CountSum.byKeyCallsForTest shouldBe 0
      (found, calls.get)
    }
    val (sums, seen) = nullAt(8192)
    sums shouldBe Map(2 -> (2, 9L), 3 -> (2, 5L), 7 -> (2, 17L))
    nullAt(1) shouldBe (sums, seen)
    calls.set(0)
    row(groups(counted)) shouldBe sums
    calls.get shouldBe seen
  }

  it should "count a null projected value and still call the key predicate" in {
    val calls = new AtomicInteger
    val preds = new AtomicInteger
    val counted = nullIntValueSource()
      .map(shiftNullKey(calls, nullKey = 7))
      .filterKeys { (key: Int) =>
        val _ = preds.incrementAndGet()
        key > 0
      }
      .countSumByKey
    shape(counted) shouldBe "CountSum(Exchange(FilterKey(Project(Scan))))"
    val measured = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      preds.set(0)
      IntPairProject.resetPublishedForTest()
      CountSum.resetByKeyForTest()
      val found = groups(counted)
      IntPairProject.publishedForTest shouldBe 0
      CountSum.byKeyCallsForTest shouldBe 0
      (found, calls.get, preds.get)
    }
    val (sums, seen, kept) = measured(8192)
    sums shouldBe Map(2 -> (1, 0L), 5 -> (1, 4L))
    seen shouldBe 2
    kept shouldBe 2
    measured(1) shouldBe (sums, seen, kept)
    calls.set(0)
    preds.set(0)
    row(groups(counted)) shouldBe sums
    calls.get shouldBe seen
    preds.get shouldBe kept
  }

  it should "drop a null projected key without calling the key predicate" in {
    val calls = new AtomicInteger
    val preds = new AtomicInteger
    val counted = pairs()
      .map(dropIntKey(calls, dropped = 2))
      .filterKeys { (key: Int) =>
        val _ = preds.incrementAndGet()
        key > 0
      }
      .countSumByKey
    val measured = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      preds.set(0)
      IntPairProject.resetPublishedForTest()
      CountSum.resetByKeyForTest()
      val found = groups(counted)
      IntPairProject.publishedForTest shouldBe 0
      CountSum.byKeyCallsForTest shouldBe 0
      (found, calls.get, preds.get)
    }
    val (sums, seen, kept) = measured(8192)
    sums shouldBe Map(1 -> (2, 40L), 3 -> (1, 40L))
    seen shouldBe 4
    kept shouldBe 3
    measured(1) shouldBe (sums, seen, kept)
  }

  it should "call a value predicate on a null projected key and create no group" in {
    val calls = new AtomicInteger
    val preds = new AtomicInteger
    val counted = pairs()
      .map(dropIntKey(calls, dropped = 2))
      .filterValues { (value: Int) =>
        val _ = preds.incrementAndGet()
        value >= 20
      }
      .countSumByKey
    val measured = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      preds.set(0)
      IntPairProject.resetPublishedForTest()
      CountSum.resetByKeyForTest()
      val found = groups(counted)
      IntPairProject.publishedForTest shouldBe 0
      CountSum.byKeyCallsForTest shouldBe 0
      (found, calls.get, preds.get)
    }
    val (sums, seen, kept) = measured(8192)
    sums shouldBe Map(1 -> (1, 30L), 3 -> (1, 40L))
    seen shouldBe 4
    kept shouldBe 4
    measured(1) shouldBe (sums, seen, kept)
    calls.set(0)
    preds.set(0)
    row(groups(counted)) shouldBe sums
    calls.get shouldBe seen
    preds.get shouldBe kept
  }

  it should "skip a null projected value and not call the value predicate" in {
    val calls = new AtomicInteger
    val preds = new AtomicInteger
    val counted = nullIntValueSource()
      .map(shiftNullKey(calls, nullKey = 7))
      .filterValues { (value: Int) =>
        val _ = preds.incrementAndGet()
        value >= 0
      }
      .countSumByKey
    calls.set(0)
    preds.set(0)
    IntPairProject.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val found = groups(counted)
    calls.get shouldBe 2
    preds.get shouldBe 1
    found shouldBe Map(5 -> (1, 4L))
    IntPairProject.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
  }

  it should "fold one slot above a filtered int pair map without publishing" in {
    val counted = pairs()
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .map { case (key, value) => (key % 100, value) }
      .filterKeys((key: Int) => key < 50)
      .countSumByKey
    shape(counted) shouldBe
      "CountSum(Exchange(FilterKey(Project(FilterValue(FilterKey(Scan))))))"
    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      val rows = groups(counted)
      ColumnMetrics.primitiveEncodeCount shouldBe 0L
      rows
    }
    IntPairProject.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    sink.fusedCount should be > 0L
    found shouldBe Map(1 -> (2, 40L), 2 -> (1, 20L))
    found shouldBe row(groups(counted))
  }

  it should "keep a second slot, a whole-row filter, and a second map published" in {
    val source = pairs()
    val shifted = source.map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
    val twice = shifted
      .filterKeys((key: Int) => key > 2)
      .filterValues((value: Int) => value >= 20)
      .countSumByKey
    shape(twice) shouldBe "CountSum(Exchange(FilterValue(FilterKey(Project(Scan)))))"
    val stacked = collectPublished(twice)
    stacked shouldBe Map(3 -> (1, 20L), 4 -> (1, 40L))
    stacked shouldBe row(groups(twice))

    val whole = shifted
      .filter((pair: (Int, Int)) => pair._1 > 2 && pair._2 >= 20)
      .countSumByKey
    shape(whole) shouldBe "CountSum(Exchange(FilterRow(Project(Scan))))"
    val rows = collectPublished(whole)
    rows shouldBe Map(3 -> (1, 20L), 4 -> (1, 40L))
    rows shouldBe row(groups(whole))

    val nested = source
      .map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
      .map((pair: (Int, Int)) => (pair._1 * 2, pair._2))
      .filterKeys((key: Int) => key > 2)
      .countSumByKey
    shape(nested) shouldBe "CountSum(Exchange(FilterKey(Project(Project(Scan)))))"
    val doubled = collectPublished(nested)
    doubled shouldBe Map(4 -> (2, 40L), 6 -> (1, 20L), 8 -> (1, 40L))
    doubled shouldBe row(groups(nested))
  }

  it should "keep a whole-row filter under the project on the published path" in {
    val counted = pairs()
      .filter((pair: (Int, Int)) => pair._1 % 2 != 0 && pair._2 > 0)
      .map { case (key, value) => (key + 1, value) }
      .filterKeys((key: Int) => key > 2)
      .countSumByKey
    shape(counted) shouldBe "CountSum(Exchange(FilterKey(Project(FilterRow(Scan)))))"
    val found = collectPublished(counted)
    found shouldBe Map(4 -> (1, 40L))
    found shouldBe row(groups(counted))
  }

  it should "fold one slot above mapIntKey without publishing the pair batch" in {
    val source = pairs()
    val mapped = source.mapIntKey((key: Int) => key + 1)
    val counted = mapped.filterKeys((key: Int) => key > 2).countSumByKey
    shape(counted) shouldBe "CountSum(Exchange(FilterKey(MapKey(Scan))))"
    val expected = Map(3 -> (1, 20L), 4 -> (1, 40L))
    val summed = atMapBatch(counted, 8192)
    summed shouldBe expected
    atMapBatch(counted, 1) shouldBe summed
    summed shouldBe row(groups(counted))

    MapIntKey.resetPublishedForTest()
    val projected = mapped.filterKeys((key: Int) => key > 2).collect()
    MapIntKey.publishedForTest should be > 0
    projected.toList.sorted shouldBe List((3, 20), (4, 40))

    val byValue = mapped.filterValues((value: Int) => value >= 20).countSumByKey
    shape(byValue) shouldBe "CountSum(Exchange(FilterValue(MapKey(Scan))))"
    val values = Map(2 -> (1, 30L), 3 -> (1, 20L), 4 -> (1, 40L))
    atMapBatch(byValue, 8192) shouldBe values
    atMapBatch(byValue, 1) shouldBe values
    atMapBatch(byValue, 8192) shouldBe row(groups(byValue))
  }

  it should "skip a null key above mapIntKey and count a null value" in {
    val mapCalls = new AtomicInteger
    val keyCalls = new AtomicInteger
    val counted = nullIntKeySource()
      .mapIntKey { key =>
        val _ = mapCalls.incrementAndGet()
        key + 1
      }
      .filterKeys { (key: Int) =>
        val _ = keyCalls.incrementAndGet()
        key > 2
      }
      .countSumByKey
    shape(counted) shouldBe "CountSum(Exchange(FilterKey(MapKey(Scan))))"
    val measured = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      mapCalls.set(0)
      keyCalls.set(0)
      MapIntKey.resetPublishedForTest()
      CountSum.resetByKeyForTest()
      val found = groups(counted)
      MapIntKey.publishedForTest shouldBe 0
      CountSum.byKeyCallsForTest shouldBe 0
      (found, mapCalls.get, keyCalls.get)
    }
    val (sums, mapped, kept) = measured(8192)
    sums shouldBe Map(3 -> (2, 5L))
    mapped shouldBe 4
    kept shouldBe 4
    measured(1) shouldBe (sums, mapped, kept)
    mapCalls.set(0)
    keyCalls.set(0)
    row(groups(counted)) shouldBe sums
    mapCalls.get shouldBe mapped

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
      .countSumByKey
    mapCalls.set(0)
    valueCalls.set(0)
    MapIntKey.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val values = groups(byValue)
    mapCalls.get shouldBe 4
    valueCalls.get shouldBe 6
    MapIntKey.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    values shouldBe Map(2 -> (2, 9L), 3 -> (1, 6L))
    mapCalls.set(0)
    valueCalls.set(0)
    row(groups(byValue)) shouldBe values
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
      .countSumByKey
    mapCalls.set(0)
    keyCalls.set(0)
    MapIntKey.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val countedNull = groups(nullValues)
    mapCalls.get shouldBe 2
    keyCalls.get shouldBe 2
    MapIntKey.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    countedNull shouldBe Map(2 -> (1, 0L), 8 -> (1, 4L))
    mapCalls.set(0)
    keyCalls.set(0)
    row(groups(nullValues)) shouldBe countedNull
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
      .countSumByKey
    mapCalls.set(0)
    nullValueCalls.set(0)
    MapIntKey.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val presentOnly = groups(byNullValue)
    mapCalls.get shouldBe 2
    nullValueCalls.get shouldBe 1
    MapIntKey.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    presentOnly shouldBe Map(8 -> (1, 4L))
    // The row path unboxes a null value to 0, so `>= 0` keeps it and the key counts. This slot
    // does not call the predicate and does not count that row, same as the project slot above.
  }

  it should "fold one slot above a filtered mapIntKey without publishing" in {
    val counted = pairs()
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 2)
      .countSumByKey
    shape(counted) shouldBe
      "CountSum(Exchange(FilterKey(MapKey(FilterValue(FilterKey(Scan))))))"
    val found = atMapBatch(counted, 8192)
    found shouldBe Map(3 -> (1, 20L))
    atMapBatch(counted, 1) shouldBe found
    found shouldBe row(groups(counted))

    val byValue = pairs()
      .filterValues((value: Int) => value >= 0)
      .filterKeys((key: Int) => key % 3 != 0)
      .mapIntKey((key: Int) => key + 1)
      .filterValues((value: Int) => value >= 20)
      .countSumByKey
    shape(byValue) shouldBe
      "CountSum(Exchange(FilterValue(MapKey(FilterKey(FilterValue(Scan))))))"
    val values = atMapBatch(byValue, 8192)
    values shouldBe Map(2 -> (1, 30L), 3 -> (1, 20L))
    atMapBatch(byValue, 1) shouldBe values
    values shouldBe row(groups(byValue))
  }

  it should "keep a second map and a whole-row filter under mapIntKey published" in {
    val nested = pairs()
      .mapIntKey((key: Int) => key + 1)
      .mapIntKey((key: Int) => key * 2)
      .filterKeys((key: Int) => key > 2)
      .countSumByKey
    shape(nested) shouldBe "CountSum(Exchange(FilterKey(MapKey(MapKey(Scan)))))"
    val doubled = collectMapPublished(nested)
    doubled shouldBe Map(4 -> (2, 40L), 6 -> (1, 20L), 8 -> (1, 40L))
    doubled shouldBe row(groups(nested))

    val whole = pairs()
      .filter((pair: (Int, Int)) => pair._1 % 2 != 0 && pair._2 > 0)
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 2)
      .countSumByKey
    shape(whole) shouldBe "CountSum(Exchange(FilterKey(MapKey(FilterRow(Scan)))))"
    val filtered = collectMapPublished(whole)
    filtered shouldBe Map(4 -> (1, 40L))
    filtered shouldBe row(groups(whole))

    val stacked = pairs()
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 2)
      .filterValues((value: Int) => value >= 20)
      .countSumByKey
    shape(stacked) shouldBe
      "CountSum(Exchange(FilterValue(FilterKey(MapKey(Scan)))))"
    stacked.collect().map { case (key, count, sum) => key -> (count, sum) }.toMap shouldBe
      row(groups(stacked))

    val rowFilter = pairs()
      .mapIntKey((key: Int) => key + 1)
      .filter((pair: (Int, Int)) => pair._1 > 2 && pair._2 >= 20)
      .countSumByKey
    ColumnarPlanner.lower(rowFilter.plan).shouldBeEmpty
    rowFilter.collect().map { case (key, count, sum) => key -> (count, sum) }.toMap shouldBe
      row(groups(rowFilter))
  }

  private def pairs(): DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 2)

  private def atBatch(
      counted: DistCollection[(Int, Int, Long)],
      size: Int,
  ): Map[Int, (Int, Long)] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
    ScarletRuntime.get.shuffle.clear()
    IntPairProject.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink)(groups(counted))
    IntPairProject.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    sink.fusedCount should be > 0L
    found

  /** Owned `mapIntKey` plus one slot. Neither pair batch is published. */
  private def atMapBatch(
      counted: DistCollection[(Int, Int, Long)],
      size: Int,
  ): Map[Int, (Int, Long)] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
    ScarletRuntime.get.shuffle.clear()
    MapIntKey.resetPublishedForTest()
    IntPairProject.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      val rows = groups(counted)
      ColumnMetrics.primitiveEncodeCount shouldBe 0L
      rows
    }
    MapIntKey.publishedForTest shouldBe 0
    IntPairProject.publishedForTest shouldBe 0
    CountSum.byKeyCallsForTest shouldBe 0
    sink.fusedCount should be > 0L
    found

  /** The arm did not run: the key map was published and [[CountSum.byKey]] saw it. */
  private def collectMapPublished(
      counted: DistCollection[(Int, Int, Long)],
  ): Map[Int, (Int, Long)] =
    ScarletRuntime.get.shuffle.clear()
    MapIntKey.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val found = groups(counted)
    MapIntKey.publishedForTest should be > 0
    CountSum.byKeyCallsForTest should be > 0
    found

  /** The arm did not run: the project was published. */
  private def collectPublished(
      counted: DistCollection[(Int, Int, Long)],
  ): Map[Int, (Int, Long)] =
    ScarletRuntime.get.shuffle.clear()
    IntPairProject.resetPublishedForTest()
    CountSum.resetByKeyForTest()
    val found = groups(counted)
    IntPairProject.publishedForTest should be > 0
    found

  private def groups(counted: DistCollection[(Int, Int, Long)]): Map[Int, (Int, Long)] =
    counted.collect().map { case (key, count, sum) => key -> (count, sum) }.toMap

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
