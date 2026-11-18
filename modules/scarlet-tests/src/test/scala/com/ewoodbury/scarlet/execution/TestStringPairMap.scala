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
 * A `(String, Int) => (String, Int)` map lowers to a project and feeds `countByKey`.
 *
 * The function runs on every row the row path would, including a null key. `""` stays a present
 * key. `"é"` and `"e\u0301"` stay distinct. A filter above the project, and a map under
 * `sortByStringKey`, stay on the row path.
 */
class TestStringPairMap extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

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

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "a string pair map" should "match the row path on a null key, including the call count" in {
    val source = nullKeySource()
    val (rowCounts, rowCalls) = countShifted(source, columnar = false)
    val (columnarCounts, columnarCalls) = countShifted(source, columnar = true)
    rowCounts shouldBe Map("a!" -> 2, "n" -> 2, "!" -> 1)
    columnarCounts shouldBe rowCounts
    columnarCalls shouldBe rowCalls
    columnarCalls shouldBe 5
    val counted = source.map(shiftNullKey(new AtomicInteger)).countByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(Project(Scan)))",
    )
  }

  it should "keep an empty key and the two accented forms distinct" in {
    val composed = "é"
    val decomposed = "e\u0301"
    val source = DistCollection(
      Seq("" -> 1, "" -> 2, composed -> 3, decomposed -> 4, composed -> 5),
      1,
    )
    val counted = source.map((pair: (String, Int)) => (pair._1 + "!", pair._2)).countByKey
    countsOf(counted.collect()) shouldBe Map(
      "!" -> 2,
      composed + "!" -> 2,
      decomposed + "!" -> 1,
    )
    countsOf(row(counted.collect())) shouldBe countsOf(counted.collect())
    (composed + "!") should not be (decomposed + "!")
  }

  it should "match batch size 1 and 8192, including call counts" in {
    val source = nullKeySource()
    val small = countShifted(source, columnar = true, batchSize = 1)
    val wide = countShifted(source, columnar = true, batchSize = 8192)
    small shouldBe wide
    small._2 shouldBe 5
  }

  it should "collect both columns of the pair map" in {
    val mapped = nullKeySource().map(shiftNullKey(new AtomicInteger))
    shown(mapped.collect()) shouldBe shown(row(mapped.collect()))
    shown(mapped.collect()) shouldBe Seq("a!:1", "n:9", "a!:null", "!:4", "n:8")
  }

  it should "hash the projected dictionary without decoding the input pairs" in {
    val source = DistCollection(Seq("a" -> 1, "a" -> 2, "b" -> 3, "" -> 4), 1)
    val counted = source.map((pair: (String, Int)) => (pair._1 + "!", pair._2)).countByKey
    StringPairProject.resetPublishedForTest()
    val sink = new CountingSink
    val rows = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      counted.collect()
    }
    sink.decodedValueCount shouldBe rows.size.toLong
    rows.size shouldBe 3
    StringPairProject.publishedForTest should be > 0
  }

  it should "refuse a filter above the project and a map under sortByStringKey" in {
    val source = DistCollection(Seq("a" -> 1, "b" -> 2, "" -> 3), 1)
    val mapped = source.map((pair: (String, Int)) => (pair._1 + "!", pair._2))
    ColumnarPlanner.lower(mapped.filter((pair: (String, Int)) => pair._2 > 0).plan) shouldBe None
    ColumnarPlanner.lower(mapped.filterKeys((key: String) => key.nonEmpty).plan) shouldBe None
    ColumnarPlanner.lower(mapped.filterValues((value: Int) => value > 0).plan) shouldBe None
    ColumnarPlanner.lower(mapped.sortByStringKey.plan) shouldBe None
    ColumnarPlanner.lower(
      mapped.filter((pair: (String, Int)) => pair._2 > 0).countByKey.plan,
    ) shouldBe None
  }

  it should "lower a filter under the map and match the row path" in {
    val source = DistCollection(Seq("a" -> 1, "b" -> 2, "" -> 3), 1)
    val filtered = source
      .filter((pair: (String, Int)) => pair._1 != "b")
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2))
    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some("Project(FilterRow(Scan))")
    shown(filtered.collect()) shouldBe shown(row(filtered.collect()))
    val keyed = source
      .filterKeys((key: String) => key != "b")
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2 + 1))
    ColumnarPlanner.lower(keyed.plan).map(_.shape) shouldBe Some("Project(FilterKey(Scan))")
    shown(keyed.collect()) shouldBe Seq("a!:2", "!:4")
  }

  it should "lower two tuple maps and refuse a non-tuple map" in {
    val source = DistCollection(Seq("a" -> 1, "b" -> 2), 1)
    val twice = source
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2))
      .map((pair: (String, Int)) => (pair._1 + "?", pair._2 + 1))
    ColumnarPlanner.lower(twice.plan).map(_.shape) shouldBe Some("Project(Project(Scan))")
    shown(twice.collect()) shouldBe Seq("a!?:2", "b!?:3")
    val scalars = source.map((pair: (String, Int)) => pair._2)
    ColumnarPlanner.lower(scalars.plan) shouldBe None
  }

  it should "lower a type-changing pair map and decline it at runtime" in {
    val source = DistCollection(Seq("aa" -> 1, "b" -> 2), 1)
    val widened = source.map((pair: (String, Int)) => (pair._1.length, pair._2))
    ColumnarPlanner.lower(widened.plan).map(_.shape) shouldBe Some("Project(Scan)")
    ColumnarPlanner.tryRun(widened.plan) shouldBe None
    widened.collect().toList shouldBe row(widened.collect().toList)
  }

  private def countShifted(
      source: DistCollection[(String, Int)],
      columnar: Boolean,
      batchSize: Int = 8,
  ): (Map[String, Int], Int) =
    val calls = new AtomicInteger
    val counted = source.map(shiftNullKey(calls)).countByKey
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = columnar, columnarBatchSize = batchSize))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      (countsOf(counted.collect()), calls.get())
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

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

  /** A present string key witnesses the scan. Later null keys stay in the batch. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKeySource(): DistCollection[(String, Int)] =
    DistCollection(
      Plan.Source(
        Seq(
          Partition(
            Seq(
              stringPair("a", Integer.valueOf(1)),
              stringPair(null, Integer.valueOf(9)),
              stringPair("a", absentInt),
              stringPair("", Integer.valueOf(4)),
              stringPair(null, Integer.valueOf(8)),
            ),
          ),
        ),
      ),
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def stringPair(key: String, value: java.lang.Integer): (String, Int) =
    Tuple2(key, value).asInstanceOf[(String, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentInt: java.lang.Integer = null

  private def countsOf(rows: Iterable[(String, Int)]): Map[String, Int] =
    rows.foldLeft(Map.empty[String, Int]) { (seen, row) =>
      require(!seen.contains(row._1), s"duplicate count for ${row._1}")
      seen.updated(row._1, row._2)
    }

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
