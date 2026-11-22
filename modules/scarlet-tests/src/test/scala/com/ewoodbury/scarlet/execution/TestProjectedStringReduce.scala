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
 * A string-pair `Project` reduced without publishing the pair batch when one slot sits above it.
 *
 * The adapter runs on every row, including a null key. A null projected key is dropped and the key
 * predicate is not called. A null projected value throws [[ColumnarFallback]], so collect follows
 * the row path. `""` is present. `"é"` and `"e\u0301"` stay distinct. [[StringHashReduce.byKey]]
 * is the published-column path and does not see this slot. A bare project still publishes.
 */
class TestProjectedStringReduce extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

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
    StringPairProject.resetPublishedForTest()
    StringHashReduce.resetByKeyForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  it should "reduce one key slot above a string pair map without publishing" in {
    val source = sample()
    val mapped = source.map((pair: (String, Int)) => (pair._1 + "!", pair._2))
    val reduced = mapped.filterKeys((key: String) => key != "!").reduceByKey[String, Int](_ + _)
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "StringHashAggregate(Exchange(FilterKey(Project(Scan))))",
    )
    val expected = Map("a!" -> 3, "b!" -> 3)
    val summed = atBatch(reduced, 8192)
    summed shouldBe expected
    atBatch(reduced, 1) shouldBe summed
    summed shouldBe row(reduced.collect().toMap)

    StringPairProject.resetPublishedForTest()
    StringHashReduce.resetByKeyForTest()
    val projected = mapped.filterKeys((key: String) => key != "!").collect()
    StringPairProject.publishedForTest should be > 0
    StringHashReduce.byKeyCallsForTest shouldBe 0
    shown(projected) shouldBe Seq("a!:1", "a!:2", "b!:3")

    val byValue = mapped.filterValues((value: Int) => value > 1).reduceByKey[String, Int](_ + _)
    ColumnarPlanner.lower(byValue.plan).map(_.shape) shouldBe Some(
      "StringHashAggregate(Exchange(FilterValue(Project(Scan))))",
    )
    val valueSums = atBatch(byValue, 8192)
    valueSums shouldBe Map("a!" -> 2, "b!" -> 3, "!" -> 4)
    atBatch(byValue, 1) shouldBe valueSums
    valueSums shouldBe row(byValue.collect().toMap)
  }

  it should "pass a null input key, keep an empty key, and keep accents distinct" in {
    val calls = new AtomicInteger
    val reduced = presentNullKeys()
      .map(shiftNullKey(calls))
      .filterKeys((key: String) => key != "!")
      .reduceByKey[String, Int](_ + _)
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "StringHashAggregate(Exchange(FilterKey(Project(Scan))))",
    )
    val nullAt = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      StringPairProject.resetPublishedForTest()
      StringHashReduce.resetByKeyForTest()
      val found = reduced.collect().toMap
      StringPairProject.publishedForTest shouldBe 0
      StringHashReduce.byKeyCallsForTest shouldBe 0
      (found, calls.get)
    }
    val (sums, seen) = nullAt(8192)
    sums shouldBe Map("a!" -> 1, "n" -> 17)
    nullAt(1) shouldBe (sums, seen)
    calls.set(0)
    row(reduced.collect().toMap) shouldBe sums
    calls.get shouldBe seen
    seen shouldBe 4

    val keptEmpty = DistCollection(Seq("" -> 1, "a" -> 2), 1)
      .map((pair: (String, Int)) => (pair._1, pair._2))
      .filterKeys((key: String) => key != "a")
      .reduceByKey[String, Int](_ + _)
    val emptySums = atBatch(keptEmpty, 8192)
    emptySums shouldBe Map("" -> 1)
    atBatch(keptEmpty, 1) shouldBe emptySums
    emptySums shouldBe row(keptEmpty.collect().toMap)

    val composed = "é"
    val decomposed = "e\u0301"
    val accents = DistCollection(Seq(composed -> 3, decomposed -> 4, composed -> 5), 1)
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2))
      .filterKeys((key: String) => key != "!")
      .reduceByKey[String, Int](_ + _)
    val accentSums = Map(composed + "!" -> 8, decomposed + "!" -> 4)
    atBatch(accents, 8192) shouldBe accentSums
    atBatch(accents, 1) shouldBe accentSums
    accentSums shouldBe row(accents.collect().toMap)
    (composed + "!") should not be (decomposed + "!")
  }

  it should "drop a null projected key and skip the key predicate" in {
    val calls = new AtomicInteger
    val preds = new AtomicInteger
    val reduced = DistCollection(Seq("drop" -> 1, "b" -> 2, "" -> 3), 1)
      .map(projectNull(calls))
      .filterKeys { (key: String) =>
        val _ = preds.incrementAndGet()
        key != "!"
      }
      .reduceByKey[String, Int](_ + _)
    val at = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      preds.set(0)
      StringPairProject.resetPublishedForTest()
      StringHashReduce.resetByKeyForTest()
      val found = reduced.collect().toMap
      StringPairProject.publishedForTest shouldBe 0
      StringHashReduce.byKeyCallsForTest shouldBe 0
      (found, calls.get, preds.get)
    }
    val (sums, seen, kept) = at(8192)
    sums shouldBe Map("b!" -> 2)
    seen shouldBe 3
    kept shouldBe 2
    at(1) shouldBe (sums, seen, kept)
    calls.set(0)
    row(reduced.collect().toMap) shouldBe sums
    calls.get shouldBe seen
  }

  it should "decline a kept null projected value and match the row path" in {
    val calls = new AtomicInteger
    val kept = DistCollection(Seq(stringPair("a", absentInt)), 1)
      .map(keepNull(calls))
      .filterKeys((key: String) => key != "!")
      .reduceByKey[String, Int](_ + _)
    ColumnarPlanner.lower(kept.plan).map(_.shape) shouldBe Some(
      "StringHashAggregate(Exchange(FilterKey(Project(Scan))))",
    )
    calls.set(0)
    StringPairProject.resetPublishedForTest()
    StringHashReduce.resetByKeyForTest()
    ColumnarPlanner.tryRun(kept.plan) shouldBe None
    StringPairProject.publishedForTest shouldBe 0
    StringHashReduce.byKeyCallsForTest shouldBe 0
    calls.set(0)
    StringPairProject.resetPublishedForTest()
    val columnar = cells(kept.collect())
    StringPairProject.publishedForTest shouldBe 0
    columnar shouldBe Seq(("a!", "null"))
    columnar shouldBe cells(row(kept.collect()))

    val dropped = DistCollection(
      Seq(stringPair("keep", Integer.valueOf(1)), stringPair("drop", absentInt)),
      1,
    ).map(keepNull(calls))
      .filterKeys((key: String) => key != "drop!")
      .reduceByKey[String, Int](_ + _)
    val droppedSums = atBatch(dropped, 8192)
    droppedSums shouldBe Map("keep!" -> 1)
    atBatch(dropped, 1) shouldBe droppedSums
    ColumnarPlanner.tryRun(dropped.plan).isDefined shouldBe true
    droppedSums shouldBe row(dropped.collect().toMap)

    val preds = new AtomicInteger
    val byValue = DistCollection(Seq(stringPair("a", absentInt)), 1)
      .map(keepNull(calls))
      .filterValues { (value: Int) =>
        val _ = preds.incrementAndGet()
        value > 0
      }
      .reduceByKey[String, Int](_ + _)
    ColumnarPlanner.lower(byValue.plan).map(_.shape) shouldBe Some(
      "StringHashAggregate(Exchange(FilterValue(Project(Scan))))",
    )
    preds.set(0)
    StringPairProject.resetPublishedForTest()
    StringHashReduce.resetByKeyForTest()
    ColumnarPlanner.tryRun(byValue.plan) shouldBe None
    // The value slot declines before the predicate. The row path then sees the unboxed 0.
    preds.get shouldBe 0
    StringPairProject.publishedForTest shouldBe 0
    StringHashReduce.byKeyCallsForTest shouldBe 0
    val droppedNull = cells(byValue.collect())
    droppedNull shouldBe Seq.empty[(String, String)]
    droppedNull shouldBe cells(row(byValue.collect()))

    val keptByValue = DistCollection(Seq(stringPair("a", absentInt)), 1)
      .map(keepNull(calls))
      .filterValues((value: Int) => value >= 0)
      .reduceByKey[String, Int](_ + _)
    StringPairProject.resetPublishedForTest()
    StringHashReduce.resetByKeyForTest()
    ColumnarPlanner.tryRun(keptByValue.plan) shouldBe None
    val keptCells = cells(keptByValue.collect())
    keptCells shouldBe Seq(("a!", "null"))
    keptCells shouldBe cells(row(keptByValue.collect()))
    StringPairProject.publishedForTest shouldBe 0
    StringHashReduce.byKeyCallsForTest shouldBe 0
  }

  it should "publish a bare string pair project and still call byKey" in {
    val reduced = sample()
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2))
      .reduceByKey[String, Int](_ + _)
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "StringHashAggregate(Exchange(Project(Scan)))",
    )
    val found = reduced.collect().toMap
    found shouldBe Map("a!" -> 3, "b!" -> 3, "!" -> 4)
    StringPairProject.publishedForTest should be > 0
    StringHashReduce.byKeyCallsForTest should be > 0
    found shouldBe row(reduced.collect().toMap)
  }

  it should "leave a second slot, a whole-row filter, and a second map empty" in {
    val source = sample()
    val mapped = source.map((pair: (String, Int)) => (pair._1 + "!", pair._2))
    val once = mapped.filterKeys((key: String) => key != "!")
    ColumnarPlanner.lower(once.filterValues((value: Int) => value > 0).plan) shouldBe None
    ColumnarPlanner.lower(mapped.filter((pair: (String, Int)) => pair._2 > 0).plan) shouldBe None
    val nested = mapped.map((pair: (String, Int)) => (pair._1 + "?", pair._2))
    ColumnarPlanner.lower(nested.filterKeys((key: String) => key != "!").plan) shouldBe None
    ColumnarPlanner.lower(
      once.filterValues((value: Int) => value > 0).reduceByKey[String, Int](_ + _).plan,
    ) shouldBe None
    ColumnarPlanner.lower(
      mapped.filter((pair: (String, Int)) => pair._2 > 0).reduceByKey[String, Int](_ + _).plan,
    ) shouldBe None
    ColumnarPlanner.lower(
      nested.filterKeys((key: String) => key != "!").reduceByKey[String, Int](_ + _).plan,
    ) shouldBe None
  }

  it should "reduce one slot above a filtered string pair map without publishing" in {
    val reduced = DistCollection(Seq("a" -> 1, "b" -> 2, "" -> 3, "a" -> 4), 1)
      .filterKeys((key: String) => key != "b")
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2))
      .filterValues((value: Int) => value > 1)
      .reduceByKey[String, Int](_ + _)
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "StringHashAggregate(Exchange(FilterValue(Project(FilterKey(Scan)))))",
    )
    val found = atBatch(reduced, 8192)
    found shouldBe Map("a!" -> 4, "!" -> 3)
    atBatch(reduced, 1) shouldBe found
    found shouldBe row(reduced.collect().toMap)
  }

  it should "combine a shared projection and a code whose projection changes" in {
    val shared = DistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3), 1)
      .map((pair: (String, Int)) => ("k", pair._2))
      .filterKeys((key: String) => key != "!")
      .reduceByKey[String, Int](_ + _)
    atBatch(shared, 8192) shouldBe Map("k" -> 6)
    atBatch(shared, 1) shouldBe Map("k" -> 6)

    val changed = DistCollection(Seq("a" -> 1, "a" -> 2, "a" -> 3), 1)
      .map { (pair: (String, Int)) =>
        val key = if (pair._2 == 1) "p" else "q"
        (key, pair._2)
      }
      .filterKeys((key: String) => key != "!")
      .reduceByKey[String, Int](_ + _)
    val sums = atBatch(changed, 8192)
    sums shouldBe Map("p" -> 1, "q" -> 5)
    atBatch(changed, 1) shouldBe sums
    sums shouldBe row(changed.collect().toMap)
  }

  private def sample(): DistCollection[(String, Int)] =
    DistCollection(Seq("a" -> 1, "a" -> 2, "b" -> 3, "" -> 4), 1)

  /** Collects, and checks the slot arm published nothing and did not call `byKey`. */
  private def atBatch(
      reduced: DistCollection[(String, Int)],
      batchSize: Int,
  ): Map[String, Int] =
    ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = batchSize))
    ScarletRuntime.get.shuffle.clear()
    StringPairProject.resetPublishedForTest()
    StringHashReduce.resetByKeyForTest()
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    val found =
      try reduced.collect().toMap
      finally ColumnMetrics.restore(previous)
    StringPairProject.publishedForTest shouldBe 0
    StringHashReduce.byKeyCallsForTest shouldBe 0
    sink.fusedCount should be > 0L
    found

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def cells(rows: Iterable[(String, Int)]): Seq[(String, String)] =
    rows.map { row =>
      val product = row.asInstanceOf[Product]
      (String.valueOf(product.productElement(0)), String.valueOf(product.productElement(1)))
    }.toList

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def shown(rows: Iterable[(String, Int)]): Seq[String] =
    rows.map { row =>
      val product = row.asInstanceOf[Product]
      String.valueOf(product.productElement(0)) + ":" + String.valueOf(product.productElement(1))
    }.toList

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  /** A present string key witnesses the scan. The null keys have present values. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def presentNullKeys(): DistCollection[(String, Int)] =
    DistCollection(
      Plan.Source(
        Seq(
          Partition(
            Seq(
              stringPair("a", Integer.valueOf(1)),
              stringPair(null, Integer.valueOf(9)),
              stringPair("", Integer.valueOf(4)),
              stringPair(null, Integer.valueOf(8)),
            ),
          ),
        ),
      ),
    )

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def shiftNullKey(calls: AtomicInteger): ((String, Int)) => (String, Int) =
    pair => {
      val _ = calls.incrementAndGet()
      val product = pair.asInstanceOf[Product]
      val value = product.productElement(1)
      val key = product.productElement(0) match
        case null => "n"
        case present: String => present + "!"
        case other => throw new IllegalArgumentException(s"pair map saw $other")
      Tuple2(key, value).asInstanceOf[(String, Int)]
    }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def projectNull(calls: AtomicInteger): ((String, Int)) => (String, Int) =
    pair => {
      val _ = calls.incrementAndGet()
      val product = pair.asInstanceOf[Product]
      val value = product.productElement(1)
      val key = product.productElement(0) match
        case "drop" => null
        case present: String => present + "!"
        case other => throw new IllegalArgumentException(s"pair map saw $other")
      Tuple2(key, value).asInstanceOf[(String, Int)]
    }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def keepNull(calls: AtomicInteger): ((String, Int)) => (String, Int) =
    pair => {
      val _ = calls.incrementAndGet()
      val product = pair.asInstanceOf[Product]
      val key = product.productElement(0) match
        case present: String => present + "!"
        case other => throw new IllegalArgumentException(s"pair map saw $other")
      Tuple2(key, product.productElement(1)).asInstanceOf[(String, Int)]
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def stringPair(key: String, value: java.lang.Integer): (String, Int) =
    Tuple2(key, value).asInstanceOf[(String, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentInt: java.lang.Integer = null
