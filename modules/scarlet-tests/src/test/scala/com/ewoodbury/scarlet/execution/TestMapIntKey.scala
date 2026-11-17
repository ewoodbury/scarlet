package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.FoldedCount
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `mapIntKey` is the typed witness for an int key change.
 *
 * A null key stays null and the function is not called. `countByKey` then drops that key. A
 * generic pair `map` still declines. A filter, `keys`, or `values` above the map declines too: the
 * row path unboxes the null key. `partitionBy` then this map drops the hash, so `countByKey` is
 * one global row per new key.
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

  "mapIntKey" should "lower a key change and leave a generic pair map on the row path" in {
    val mapped = present.mapIntKey(key => key % 100)
    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some("MapKey(Scan)")
    val counted = mapped.countByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(MapKey(Scan)))",
    )
    val opaque = present.map { case (key, value) => (key % 100, value) }.countByKey
    ColumnarPlanner.lower(opaque.plan) shouldBe None
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

  it should "decline a filter, keys, and values above a key map" in {
    val mapped = nullable.mapIntKey(key => key + 1)
    ColumnarPlanner.lower(mapped.filterKeys((key: Int) => key > 0).plan) shouldBe None
    ColumnarPlanner.lower(mapped.filterValues((value: Int) => value >= 0).plan) shouldBe None
    ColumnarPlanner.lower(mapped.keys.plan) shouldBe None
    ColumnarPlanner.lower(mapped.values.plan) shouldBe None
    ColumnarPlanner.tryRun(mapped.filterKeys((key: Int) => key > 0).plan) shouldBe None
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
    val reduced = nullable.mapIntKey(key => key + 1).reduceByKey[Int, Int](_ + _)
    counts(run(columnar = true)(reduced.collect())) shouldBe counts(
      run(columnar = false)(reduced.collect()),
    )
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

  /** `productElement` does not unbox a null int, and tuple equality would. */
  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
  private def fingerprint(rows: Iterable[Any]): Seq[String] =
    rows.map { row =>
      val product = row.asInstanceOf[Product]
      String.valueOf(product.productElement(0)) + ":" + String.valueOf(product.productElement(1))
    }.toSeq.sorted

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
