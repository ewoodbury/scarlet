package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{FoldedCount, HashReduce}
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `mapIntKey` is the typed witness for an int key change.
 *
 * A null key stays null and the function is not called. `countByKey` then drops that key. A
 * generic `(Int, Int)` pair `map` lowers as `Project` and calls the function on a null key. A One
 * `filterKeys` or `filterValues` above this key map lowers. The key predicate sees the mapped key
 * and is not called on a null. `keys` and `values` of the map, and of that one slot, lower. The
 * key column is the mapped key. The value column is the original value. A null input key stays
 * null, is not passed to the function or a key predicate, and does not drop the value cell. A
 * value predicate still sees that row. A null value stays a clear bit. A second slot, `mapValues`,
 * a second map above that filter, and a whole-row filter stay empty. `partitionBy` then this map
 * drops the hash, so `countByKey` is one global row per new key.
 */
class TestMapIntKey extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "mapIntKey" should "lower a key change, and a generic pair map should lower beside it" in {
    val mapped = present.mapIntKey(key => key % 100)
    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some("MapKey(Scan)")
    val counted = mapped.countByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(MapKey(Scan)))",
    )
    val opaque = present.map { case (key, value) => (key % 100, value) }.countByKey
    ColumnarPlanner.lower(opaque.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(Project(Scan)))",
    )
    counts(run(columnar = true)(opaque.collect())) shouldBe counts(
      run(columnar = false)(opaque.collect()),
    )
  }

  it should "call a generic pair map on a null key and still count a null value" in {
    val calls = new AtomicInteger
    val mapped = nullable.map(rewriteNullKey(calls, nullKey = 7))
    calls.set(0)
    val columnar = fingerprint(run(columnar = true)(mapped.collect()))
    calls.get() shouldBe 6
    calls.set(0)
    val rows = fingerprint(run(columnar = false)(mapped.collect()))
    calls.get() shouldBe 6
    columnar shouldBe rows
    columnar shouldBe Seq("2:4", "2:5", "3:-1", "3:6", "7:8", "7:9")

    val counted = nullable.map(rewriteNullKey(calls, nullKey = 7)).countByKey
    calls.set(0)
    val columnarCounts = counts(run(columnar = true)(counted.collect()))
    calls.get() shouldBe 6
    calls.set(0)
    val rowCounts = counts(run(columnar = false)(counted.collect()))
    calls.get() shouldBe 6
    columnarCounts shouldBe rowCounts
    columnarCounts shouldBe Map(2 -> 2, 3 -> 2, 7 -> 2)

    val valueCalls = new AtomicInteger
    val values = nullThenValue.map(rewriteNullKey(valueCalls, nullKey = 7)).countByKey
    valueCalls.set(0)
    val columnarValues = counts(run(columnar = true)(values.collect()))
    valueCalls.get() shouldBe 2
    valueCalls.set(0)
    val rowValues = counts(run(columnar = false)(values.collect()))
    valueCalls.get() shouldBe 2
    columnarValues shouldBe rowValues
    columnarValues shouldBe Map(2 -> 1, 5 -> 1)
  }

  it should "keep a null key, skip the function, and match the row path" in {
    val calls = new AtomicInteger
    val mapped = nullable.mapIntKey { key =>
      calls.incrementAndGet()
      key + 1
    }

    calls.set(0)
    val columnar = fingerprint(run(columnar = true)(mapped.collect()))
    calls.get() shouldBe 4
    calls.set(0)
    val rows = fingerprint(run(columnar = false)(mapped.collect()))
    calls.get() shouldBe 4
    columnar shouldBe rows
    columnar shouldBe Seq("2:4", "2:5", "3:-1", "3:6", "null:8", "null:9")
  }

  it should "drop a null key in countByKey without calling the function on it" in {
    val calls = new AtomicInteger
    val counted = nullable.mapIntKey { key =>
      calls.incrementAndGet()
      key + 1
    }.countByKey

    calls.set(0)
    val columnar = counts(run(columnar = true)(counted.collect()))
    calls.get() shouldBe 4
    calls.set(0)
    val rows = counts(run(columnar = false)(counted.collect()))
    calls.get() shouldBe 4
    columnar shouldBe rows
    columnar shouldBe Map(2 -> 2, 3 -> 2)
  }

  it should "preserve a null value and still map the key" in {
    val calls = new AtomicInteger
    val mapped = nullValue.mapIntKey { key =>
      calls.incrementAndGet()
      key * 2
    }
    calls.set(0)
    val columnar = fingerprint(run(columnar = true)(mapped.collect()))
    calls.get() shouldBe 2
    columnar shouldBe fingerprint(run(columnar = false)(mapped.collect()))
    columnar shouldBe Seq("2:null", "8:4")
  }

  it should "agree with the row path on a negative key" in {
    val mapped = signed.mapIntKey(key => key % 100).countByKey
    val columnar = counts(run(columnar = true)(mapped.collect()))
    columnar shouldBe counts(run(columnar = false)(mapped.collect()))
    columnar shouldBe Map(-5 -> 1, 5 -> 1)
  }

  it should "compose two key maps" in {
    val mapped = present.mapIntKey(_ + 1).mapIntKey(_ * 2)
    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some("MapKey(MapKey(Scan))")
    fingerprint(run(columnar = true)(mapped.collect())) shouldBe fingerprint(
      run(columnar = false)(mapped.collect()),
    )
  }

  it should "lower one slot filter above a key map, and keys and values of that map" in {
    val mapped = nullable.mapIntKey(key => key + 1)
    ColumnarPlanner.lower(mapped.filterKeys((key: Int) => key > 0).plan).map(_.shape) shouldBe
      Some("FilterKey(MapKey(Scan))")
    ColumnarPlanner
      .lower(mapped.filterValues((value: Int) => value >= 0).plan)
      .map(_.shape) shouldBe
      Some("FilterValue(MapKey(Scan))")
    ColumnarPlanner.lower(mapped.keys.plan).map(_.shape) shouldBe Some("Key(MapKey(Scan))")
    ColumnarPlanner.lower(mapped.values.plan).map(_.shape) shouldBe Some("Value(MapKey(Scan))")
    ColumnarPlanner.lower(mapped.filterKeys((key: Int) => key > 0).keys.plan).map(_.shape) shouldBe
      Some("Key(FilterKey(MapKey(Scan)))")
    ColumnarPlanner
      .lower(mapped.filterKeys((key: Int) => key > 0).values.plan)
      .map(_.shape) shouldBe
      Some("Value(FilterKey(MapKey(Scan)))")
    ColumnarPlanner
      .lower(mapped.filterValues((value: Int) => value >= 0).keys.plan)
      .map(_.shape) shouldBe
      Some("Key(FilterValue(MapKey(Scan)))")
    ColumnarPlanner
      .lower(mapped.filterValues((value: Int) => value >= 0).values.plan)
      .map(_.shape) shouldBe
      Some("Value(FilterValue(MapKey(Scan)))")
    ColumnarPlanner.lower(mapped.mapValues((value: Int) => value + 1).plan) shouldBe None
    ColumnarPlanner.lower(mapped.filterKeys((key: Int) => key > 0).mapIntKey(_ + 1).plan) shouldBe
      None
    ColumnarPlanner.lower(mapped.filter { case (key, _) => key > 0 }.plan) shouldBe None
    ColumnarPlanner.lower(mapped.filter { case (key, _) => key > 0 }.keys.plan) shouldBe None
    ColumnarPlanner.lower(
      mapped.filterKeys((key: Int) => key > 0).filterValues((value: Int) => value > 0).plan,
    ) shouldBe None
    ColumnarPlanner.lower(
      mapped.filterKeys((key: Int) => key > 0).filterValues((value: Int) => value > 0).keys.plan,
    ) shouldBe None
    ColumnarPlanner.lower(mapped.mapValues((value: Int) => value + 1).keys.plan) shouldBe None
    ColumnarPlanner.lower(
      mapped.filterKeys((key: Int) => key > 0).union(mapped).plan,
    ) shouldBe None
  }

  it should "collect keys and values of one slot above the map" in {
    val pairs = DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 2)
    val filtered = pairs.mapIntKey(_ + 1).filterKeys((key: Int) => key > 2)
    val keyed = filtered.keys
    val valued = filtered.values
    ColumnarPlanner.lower(keyed.plan).map(_.shape) shouldBe Some("Key(FilterKey(MapKey(Scan)))")
    ColumnarPlanner.lower(valued.plan).map(_.shape) shouldBe Some(
      "Value(FilterKey(MapKey(Scan)))",
    )
    ColumnarPlanner.tryCount(keyed.plan) shouldBe Some(2L)
    ColumnarPlanner.tryCount(valued.plan) shouldBe Some(2L)
    val at = (size: Int, column: DistCollection[Int]) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size, columnarExecution = true))
      ScarletRuntime.get.shuffle.clear()
      column.collect().toList
    }
    val wideKeys = at(8192, keyed)
    wideKeys shouldBe List(3, 4)
    at(1, keyed) shouldBe wideKeys
    val wideValues = at(8192, valued)
    wideValues shouldBe List(20, 40)
    at(1, valued) shouldBe wideValues

    val mapCalls = new AtomicInteger
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val mapped = nullable.mapIntKey { key =>
      mapCalls.incrementAndGet()
      key + 1
    }
    val byKey = mapped.filterKeys { (key: Int) =>
      keyCalls.incrementAndGet()
      key > 0
    }
    val byValue = mapped.filterValues { (value: Int) =>
      valueCalls.incrementAndGet()
      value >= 0
    }
    def projected(columnar: Boolean, column: DistCollection[Int]): Seq[String] =
      keyText(run(columnar = columnar)(column.collect()))

    mapCalls.set(0)
    keyCalls.set(0)
    val keyKeys = projected(true, byKey.keys)
    mapCalls.get() shouldBe 4
    keyCalls.get() shouldBe 4
    mapCalls.set(0)
    keyKeys shouldBe projected(false, byKey.keys)
    mapCalls.get() shouldBe 4
    keyKeys shouldBe Seq("2", "2", "3", "3")

    mapCalls.set(0)
    keyCalls.set(0)
    val keyValues = projected(true, byKey.values)
    mapCalls.get() shouldBe 4
    keyCalls.get() shouldBe 4
    mapCalls.set(0)
    keyValues shouldBe projected(false, byKey.values)
    mapCalls.get() shouldBe 4
    keyValues shouldBe Seq("4", "5", "-1", "6")

    mapCalls.set(0)
    valueCalls.set(0)
    val valueKeys = projected(true, byValue.keys)
    mapCalls.get() shouldBe 4
    valueCalls.get() shouldBe 6
    mapCalls.set(0)
    valueKeys shouldBe projected(false, byValue.keys)
    mapCalls.get() shouldBe 4
    valueKeys shouldBe Seq("2", "null", "2", "3", "null")

    mapCalls.set(0)
    valueCalls.set(0)
    val valueValues = projected(true, byValue.values)
    mapCalls.get() shouldBe 4
    valueCalls.get() shouldBe 6
    mapCalls.set(0)
    valueValues shouldBe projected(false, byValue.values)
    mapCalls.get() shouldBe 4
    valueValues shouldBe Seq("4", "9", "5", "6", "8")
    ColumnarPlanner.tryCount(byValue.keys.plan) shouldBe Some(5L)
    run(columnar = false)(byValue.keys.count()) shouldBe 5L

    // A present pair witnesses the scan. The null value is the next cell, so it stays a clear bit.
    val nulls = nullThenValue.mapIntKey(_ * 2).filterKeys((key: Int) => key > 0).values
    ColumnarPlanner.lower(nulls.plan).map(_.shape) shouldBe Some("Value(FilterKey(MapKey(Scan)))")
    val nullText = keyText(run(columnar = true)(nulls.collect()))
    nullText shouldBe keyText(run(columnar = false)(nulls.collect()))
    nullText shouldBe Seq("4", "null")
  }

  it should "collect mapped keys in partition order and keep a null key" in {
    val keyed = DistCollection
      .ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 2)
      .mapIntKey(_ + 1)
      .keys
    ColumnarPlanner.lower(keyed.plan).map(_.shape) shouldBe Some("Key(MapKey(Scan))")
    ColumnarPlanner.tryCount(keyed.plan) shouldBe Some(4L)
    val at = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size, columnarExecution = true))
      ScarletRuntime.get.shuffle.clear()
      keyed.collect().toList
    }
    val wide = at(8192)
    wide shouldBe List(2, 3, 2, 4)
    at(1) shouldBe wide

    val calls = new AtomicInteger
    val nullKeys = nullable.mapIntKey { key =>
      calls.incrementAndGet()
      key + 1
    }.keys
    calls.set(0)
    MapIntKey.resetPublishedForTest()
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 8192, columnarExecution = true))
    val columnar = keyText(run(columnar = true)(nullKeys.collect()))
    calls.get() shouldBe 4
    MapIntKey.publishedForTest should be > 0
    calls.set(0)
    val rows = keyText(run(columnar = false)(nullKeys.collect()))
    calls.get() shouldBe 4
    columnar shouldBe rows
    columnar shouldBe Seq("2", "null", "2", "3", "3", "null")
    ColumnarPlanner.tryCount(nullKeys.plan) shouldBe Some(6L)
    run(columnar = false)(nullKeys.count()) shouldBe 6L
  }

  it should "collect mapped values in partition order and keep a value beside a null key" in {
    val valued = DistCollection
      .ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 2)
      .mapIntKey(_ + 1)
      .values
    ColumnarPlanner.lower(valued.plan).map(_.shape) shouldBe Some("Value(MapKey(Scan))")
    ColumnarPlanner.tryCount(valued.plan) shouldBe Some(4L)
    val at = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size, columnarExecution = true))
      ScarletRuntime.get.shuffle.clear()
      valued.collect().toList
    }
    val wide = at(8192)
    wide shouldBe List(10, 20, 30, 40)
    at(1) shouldBe wide

    val calls = new AtomicInteger
    val nullKeys = nullable.mapIntKey { key =>
      calls.incrementAndGet()
      key + 1
    }.values
    calls.set(0)
    MapIntKey.resetPublishedForTest()
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 8192, columnarExecution = true))
    val columnar = keyText(run(columnar = true)(nullKeys.collect()))
    calls.get() shouldBe 4
    MapIntKey.publishedForTest should be > 0
    calls.set(0)
    val rows = keyText(run(columnar = false)(nullKeys.collect()))
    calls.get() shouldBe 4
    columnar shouldBe rows
    columnar shouldBe Seq("4", "9", "5", "-1", "6", "8")
    ColumnarPlanner.tryCount(nullKeys.plan) shouldBe Some(6L)
    run(columnar = false)(nullKeys.count()) shouldBe 6L

    // A present pair witnesses the scan. The null value is the next cell, so it stays a clear bit.
    val valueCalls = new AtomicInteger
    val nulls = nullThenValue.mapIntKey { key =>
      valueCalls.incrementAndGet()
      key * 2
    }.values
    ColumnarPlanner.lower(nulls.plan).map(_.shape) shouldBe Some("Value(MapKey(Scan))")
    valueCalls.set(0)
    MapIntKey.resetPublishedForTest()
    val columnarNulls = keyText(run(columnar = true)(nulls.collect()))
    valueCalls.get() shouldBe 2
    MapIntKey.publishedForTest should be > 0
    valueCalls.set(0)
    val rowNulls = keyText(run(columnar = false)(nulls.collect()))
    valueCalls.get() shouldBe 2
    columnarNulls shouldBe rowNulls
    columnarNulls shouldBe Seq("4", "null")
    ColumnarPlanner.tryCount(nulls.plan) shouldBe Some(2L)
  }

  it should "count a key filter above the map without publishing the pair batch" in {
    val counted = DistCollection
      .ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 2)
      .mapIntKey(_ + 1)
      .filterKeys((key: Int) => key > 2)
      .countByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(FilterKey(MapKey(Scan))))",
    )
    val at = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size, columnarExecution = true))
      ScarletRuntime.get.shuffle.clear()
      MapIntKey.resetPublishedForTest()
      FoldedCount.resetPassesForTest()
      val found = counts(counted.collect())
      (found, MapIntKey.publishedForTest, FoldedCount.passesForTest)
    }
    val wide = at(8192)
    wide shouldBe (Map(3 -> 1, 4 -> 1), 0, 1)
    at(1) shouldBe wide
    counts(run(columnar = false)(counted.collect())) shouldBe Map(3 -> 1, 4 -> 1)
  }

  it should "skip a null key above the map and still count a null value" in {
    val mapCalls = new AtomicInteger
    val keyCalls = new AtomicInteger
    val mapped = nullable.mapIntKey { key =>
      mapCalls.incrementAndGet()
      key + 1
    }
    val filtered = mapped.filterKeys { (key: Int) =>
      keyCalls.incrementAndGet()
      key > 0
    }
    mapCalls.set(0)
    keyCalls.set(0)
    MapIntKey.resetPublishedForTest()
    val columnar = fingerprint(run(columnar = true)(filtered.collect()))
    mapCalls.get() shouldBe 4
    keyCalls.get() shouldBe 4
    MapIntKey.publishedForTest should be > 0
    columnar shouldBe Seq("2:4", "2:5", "3:-1", "3:6")
    columnar shouldBe fingerprint(run(columnar = false)(filtered.collect()))

    val valueCalls = new AtomicInteger
    val byValue = nullable
      .mapIntKey { key =>
        mapCalls.incrementAndGet()
        key + 1
      }
      .filterValues { (value: Int) =>
        valueCalls.incrementAndGet()
        value >= 0
      }
    mapCalls.set(0)
    valueCalls.set(0)
    val kept = fingerprint(run(columnar = true)(byValue.collect()))
    mapCalls.get() shouldBe 4
    valueCalls.get() shouldBe 6
    kept shouldBe fingerprint(run(columnar = false)(byValue.collect()))
    kept shouldBe Seq("2:4", "2:5", "3:6", "null:8", "null:9")

    val counted = nullable
      .mapIntKey { key =>
        mapCalls.incrementAndGet()
        key + 1
      }
      .filterKeys { (key: Int) =>
        keyCalls.incrementAndGet()
        key > 0
      }
      .countByKey
    mapCalls.set(0)
    keyCalls.set(0)
    MapIntKey.resetPublishedForTest()
    FoldedCount.resetPassesForTest()
    val countsByKey = counts(run(columnar = true)(counted.collect()))
    mapCalls.get() shouldBe 4
    keyCalls.get() shouldBe 4
    MapIntKey.publishedForTest shouldBe 0
    FoldedCount.passesForTest shouldBe 1
    countsByKey shouldBe counts(run(columnar = false)(counted.collect()))
    countsByKey shouldBe Map(2 -> 2, 3 -> 2)

    val nullValues = nullValue.mapIntKey(_ * 2).filterKeys((key: Int) => key > 0).countByKey
    counts(run(columnar = true)(nullValues.collect())) shouldBe Map(2 -> 1, 8 -> 1)
    counts(run(columnar = false)(nullValues.collect())) shouldBe Map(2 -> 1, 8 -> 1)
  }

  it should "shuffle again after partitionBy because the key changed" in {
    val width = 4
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = width))
    val keys = Array(0, 1, 2, 3, 4, 5)
    val values = Array.fill(keys.length)(1)
    val counted = DistCollection
      .ofIntPairs(keys, values, numPartitions = 2)
      .partitionBy(width)
      .mapIntKey(key => key % 2)
      .countByKey
    ColumnarPlanner.lower(counted.plan) shouldBe None

    def check(): Unit =
      val rows = counted.collect()
      rows.size shouldBe 2
      counts(rows) shouldBe Map(0 -> 3, 1 -> 3)

    run(columnar = true)(check())
    run(columnar = false)(check())
  }

  it should "count filtered owned pairs by the mapped key and skip dropped rows" in {
    val calls = new AtomicInteger
    val counted = DistCollection
      .ofIntPairs(Array(1, 2, 3, 4, -5), Array(1, -1, 2, 3, 1), 2)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .mapIntKey { key =>
        calls.incrementAndGet()
        key % 100
      }
      .countByKey
    calls.set(0)
    FoldedCount.resetPassesForTest()
    val columnar = counts(run(columnar = true)(counted.collect()))
    calls.get() shouldBe 3
    FoldedCount.passesForTest shouldBe 1
    calls.set(0)
    val rows = counts(run(columnar = false)(counted.collect()))
    calls.get() shouldBe 3
    columnar shouldBe rows
    columnar shouldBe Map(1 -> 1, 4 -> 1, -5 -> 1)
  }

  it should "drop a null key in reduceByKey and match the row path" in {
    val calls = new AtomicInteger
    val mapped = nullable.mapIntKey { key =>
      calls.incrementAndGet()
      key + 1
    }
    val reduced = mapped.reduceByKey[Int, Int](_ + _)
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "HashAggregate(Exchange(MapKey(Scan)))",
    )
    calls.set(0)
    MapIntKey.resetPublishedForTest()
    HashReduce.resetByKeyForTest()
    val columnar = counts(run(columnar = true)(reduced.collect()))
    calls.get() shouldBe 4
    MapIntKey.publishedForTest shouldBe 0
    HashReduce.byKeyCallsForTest shouldBe 0
    calls.set(0)
    val rows = counts(run(columnar = false)(reduced.collect()))
    calls.get() shouldBe 4
    columnar shouldBe rows
    columnar shouldBe Map(2 -> 9, 3 -> 5)
  }

  it should "reduce a mapped key without publishing the pair batch" in {
    val source = DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 1)
    val mapped = source.mapIntKey(_ + 1)
    val changed = mapped.reduceByKey[Int, Int](_ + _)
    ColumnarPlanner.lower(changed.plan).map(_.shape) shouldBe Some(
      "HashAggregate(Exchange(MapKey(Scan)))",
    )
    val expected = Map(2 -> 40, 3 -> 20, 4 -> 40)
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      MapIntKey.resetPublishedForTest()
      HashReduce.resetByKeyForTest()
      val found = counts(changed.collect())
      MapIntKey.publishedForTest shouldBe 0
      HashReduce.byKeyCallsForTest shouldBe 0
      found
    }
    atBatch(8192) shouldBe expected
    atBatch(1) shouldBe expected
    atBatch(8192) shouldBe counts(run(columnar = false)(changed.collect()))

    MapIntKey.resetPublishedForTest()
    val projected = mapped.collect()
    MapIntKey.publishedForTest should be > 0
    fingerprint(projected) shouldBe Seq("2:10", "2:30", "3:20", "4:40")
  }

  it should "call the key function on a null value and drop that group" in {
    val calls = new AtomicInteger
    val reduced = nullThenValue
      .mapIntKey { key =>
        calls.incrementAndGet()
        key * 2
      }
      .reduceByKey[Int, Int](_ + _)
    calls.set(0)
    MapIntKey.resetPublishedForTest()
    HashReduce.resetByKeyForTest()
    val columnar = counts(run(columnar = true)(reduced.collect()))
    calls.get() shouldBe 2
    MapIntKey.publishedForTest shouldBe 0
    HashReduce.byKeyCallsForTest shouldBe 0
    columnar shouldBe Map(8 -> 4)
    calls.set(0)
    val _ = run(columnar = false)(reduced.collect())
    calls.get() shouldBe 2
  }

  it should "reduce filtered owned pairs by the mapped key and skip dropped rows" in {
    val calls = new AtomicInteger
    val reduced = DistCollection
      .ofIntPairs(Array(1, 2, 3, 4, -5), Array(1, -1, 2, 3, 1), 2)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .mapIntKey { key =>
        calls.incrementAndGet()
        key % 100
      }
      .reduceByKey[Int, Int](_ + _)
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "HashAggregate(Exchange(MapKey(FilterValue(FilterKey(Scan)))))",
    )
    calls.set(0)
    MapIntKey.resetPublishedForTest()
    HashReduce.resetByKeyForTest()
    val columnar = counts(run(columnar = true)(reduced.collect()))
    calls.get() shouldBe 3
    MapIntKey.publishedForTest shouldBe 0
    HashReduce.byKeyCallsForTest shouldBe 0
    calls.set(0)
    val rows = counts(run(columnar = false)(reduced.collect()))
    calls.get() shouldBe 3
    columnar shouldBe rows
    columnar shouldBe Map(1 -> 1, 4 -> 3, -5 -> 1)
  }

  private val present: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 5, 100), Array(3, 8, 0), 2)

  private val signed: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(-5, 105), Array(1, 2), 2)

  /** Present keys 1, 1, 2, 2 and two null keys. Values stay present. */
  private def nullable: DistCollection[(Int, Int)] =
    DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq(cell(1, 4), cell(null, 9), cell(1, 5), cell(2, -1))),
          Partition(Seq(cell(2, 6), cell(null, 8))),
        ),
      ),
    )

  /** A present pair witnesses the scan, then a null value. The key stays present. */
  private def nullThenValue: DistCollection[(Int, Int)] =
    DistCollection(
      Plan.Source(
        Seq(Partition(Seq(cell(4, 4), cell(1, null)))),
      ),
    )

  /**
   * A null key becomes `nullKey`. A present key is incremented. `productElement` keeps a null
   * value, so both paths count it. The row path and the pair project call this once per row.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def rewriteNullKey(calls: AtomicInteger, nullKey: Int): ((Int, Int)) => (Int, Int) =
    pair => {
      calls.incrementAndGet()
      val product = pair.asInstanceOf[Product]
      val value = product.productElement(1)
      product.productElement(0) match
        case null => Tuple2(Int.box(nullKey), value).asInstanceOf[(Int, Int)]
        case present: Int => Tuple2(Int.box(present + 1), value).asInstanceOf[(Int, Int)]
        case other =>
          throw new IllegalArgumentException(s"pair map saw $other")
    }

  /** One null value. The key function still runs. */
  private def nullValue: DistCollection[(Int, Int)] =
    DistCollection(
      Plan.Source(
        Seq(Partition(Seq(cell(1, null), cell(4, 4)))),
      ),
    )

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def cell(key: Any, value: Any): (Int, Int) =
    val boxedKey = key match
      case null => null
      case presentKey: Int => Int.box(presentKey)
      case other => other
    val boxedValue = value match
      case null => null
      case presentValue: Int => Int.box(presentValue)
      case other => other
    Tuple2(boxedKey, boxedValue).asInstanceOf[(Int, Int)]

  private def run[A](columnar: Boolean)(body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = columnar))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  /** A null key stays the string `null`. `String.valueOf` does not unbox it to `0`. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def keyText(rows: Iterable[Any]): Seq[String] =
    rows.toSeq.map { row =>
      val ref = row.asInstanceOf[AnyRef]
      if (ref == null) then "null" else String.valueOf(ref)
    }

  /** `productElement` does not unbox a null int, and tuple equality would. */
  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
  private def fingerprint(rows: Iterable[Any]): Seq[String] =
    rows
      .map { row =>
        val product = row.asInstanceOf[Product]
        String.valueOf(product.productElement(0)) + ":" + String.valueOf(product.productElement(1))
      }
      .toSeq
      .sorted

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def counts(rows: Iterable[Any]): Map[Int, Int] =
    rows.map { row =>
      val product = row.asInstanceOf[Product]
      val key = product.productElement(0) match
        case null =>
          throw new IllegalArgumentException("countByKey published a null key")
        case presentKey => presentKey.asInstanceOf[Int]
      key -> product.productElement(1).asInstanceOf[Int]
    }.toMap
