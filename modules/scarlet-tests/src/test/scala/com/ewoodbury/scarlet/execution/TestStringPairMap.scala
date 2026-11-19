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
 * key. `"é"` and `"e\u0301"` stay distinct. One key or value filter above that project lowers. A
 * whole-row filter, a second slot, and a second map stay empty. One project under
 * `sortByStringKey` lowers. `mapValues`, a second map, and a slot or whole-row filter above that
 * project stay empty.
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

  it should "lower one slot above the project, sort that project, and refuse a second slot and a second map" in {
    val source = DistCollection(Seq("a" -> 1, "b" -> 2, "" -> 3), 1)
    val mapped = source.map((pair: (String, Int)) => (pair._1 + "!", pair._2))
    ColumnarPlanner.lower(mapped.filter((pair: (String, Int)) => pair._2 > 0).plan) shouldBe None
    val byKey = mapped.filterKeys((key: String) => key.nonEmpty)
    val byValue = mapped.filterValues((value: Int) => value > 0)
    ColumnarPlanner.lower(byKey.plan).map(_.shape) shouldBe Some("FilterKey(Project(Scan))")
    ColumnarPlanner.lower(byValue.plan).map(_.shape) shouldBe Some("FilterValue(Project(Scan))")
    ColumnarPlanner.lower(mapped.sortByStringKey.plan).map(_.shape) shouldBe Some(
      "SortByStringKey(Project(Scan))",
    )
    ColumnarPlanner.lower(
      mapped.filter((pair: (String, Int)) => pair._2 > 0).countByKey.plan,
    ) shouldBe None
    ColumnarPlanner.lower(byKey.filterKeys((key: String) => key != "x").plan) shouldBe None
    ColumnarPlanner.lower(byValue.filterKeys((key: String) => key.nonEmpty).plan) shouldBe None
    ColumnarPlanner.lower(
      byKey.map((pair: (String, Int)) => (pair._1 + "?", pair._2)).plan,
    ) shouldBe None
    val twice = mapped.map((pair: (String, Int)) => (pair._1 + "?", pair._2))
    ColumnarPlanner.lower(twice.filterKeys((key: String) => key.nonEmpty).plan) shouldBe None
    val stacked = source
      .filterKeys((key: String) => key != "b")
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2))
      .filterKeys((key: String) => key != "!")
    ColumnarPlanner.lower(stacked.plan).map(_.shape) shouldBe Some(
      "FilterKey(Project(FilterKey(Scan)))",
    )
    ColumnarPlanner.lower(stacked.countByKey.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(FilterKey(Project(FilterKey(Scan)))))",
    )
  }

  it should "sort a remapped string key through StringRankSplit" in {
    val source = DistCollection(Seq("b" -> 2, "a" -> 1, "" -> 3), 1)
    val mapped = source.map((pair: (String, Int)) => (pair._1 + "!", pair._2))
    val sorted = mapped.sortByStringKey
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some(
      "SortByStringKey(Project(Scan))",
    )
    shown(sorted.collect()) shouldBe Seq("!:3", "a!:1", "b!:2")
    shown(sorted.collect()) shouldBe shown(row(sorted.collect()))
    shownAt(sorted, 1) shouldBe shownAt(sorted, 8192)
    StringPairProject.resetPublishedForTest()
    StringRankSplitRuns.resetForTest()
    val _ = sorted.collect()
    StringPairProject.publishedForTest should be > 0
    StringRankSplitRuns.filesWrittenForTest shouldBe 0L

    val composed = "é"
    val decomposed = "e\u0301"
    val accents = DistCollection(Seq(composed -> 1, "a" -> 2, decomposed -> 3), 1)
      .map((pair: (String, Int)) => (pair._1, pair._2))
      .sortByStringKey
    shown(accents.collect()) shouldBe Seq("a:2", s"$decomposed:3", s"$composed:1")
    composed should not be decomposed
    shown(accents.collect()) shouldBe shown(row(accents.collect()))

    val nulls = nullEmptySource().map((pair: (String, Int)) => (pair._1, pair._2)).sortByStringKey
    shown(nulls.collect()) shouldBe Seq("null:1", ":3", "null:4", "b:2")
    shown(nulls.collect()) shouldBe shown(row(nulls.collect()))

    val under = source
      .filterKeys((key: String) => key != "b")
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2))
      .sortByStringKey
    ColumnarPlanner.lower(under.plan).map(_.shape) shouldBe Some(
      "SortByStringKey(Project(FilterKey(Scan)))",
    )
    shown(under.collect()) shouldBe Seq("!:3", "a!:1")
    shown(under.collect()) shouldBe shown(row(under.collect()))
    val rowUnder = source
      .filter((pair: (String, Int)) => pair._1 != "b")
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2))
      .sortByStringKey
    ColumnarPlanner.lower(rowUnder.plan).map(_.shape) shouldBe Some(
      "SortByStringKey(Project(FilterRow(Scan)))",
    )
    shown(rowUnder.collect()) shouldBe shown(row(rowUnder.collect()))

    ColumnarPlanner.lower(
      mapped.filterKeys((key: String) => key.nonEmpty).sortByStringKey.plan,
    ) shouldBe None
    ColumnarPlanner.lower(
      mapped.filter((pair: (String, Int)) => pair._2 > 0).sortByStringKey.plan,
    ) shouldBe None
    val twice = mapped.map((pair: (String, Int)) => (pair._1 + "?", pair._2))
    ColumnarPlanner.lower(twice.sortByStringKey.plan) shouldBe None
    ColumnarPlanner.lower(
      source.mapValues((value: Int) => value + 1).sortByStringKey.plan,
    ) shouldBe None
    val bad = source.map(widen).sortByStringKey
    ColumnarPlanner.lower(bad.plan).map(_.shape) shouldBe Some("SortByStringKey(Project(Scan))")
    val lowered = ColumnarPlanner.lower(bad.plan).getOrElse(fail("expected a project sort"))
    a[ColumnarFallback] should be thrownBy {
      val _ = ColumnarInterpreter.interpret(bad.plan, lowered)
    }
    ColumnarPlanner.tryRun(bad.plan) shouldBe None
  }

  it should "count a remapped key after filterKeys and match batch size 1 and 8192" in {
    val source = DistCollection(Seq("a" -> 1, "a" -> 2, "b" -> 3, "" -> 4), 1)
    val counted = source
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2))
      .filterKeys((key: String) => key != "!")
      .countByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(FilterKey(Project(Scan))))",
    )
    val expected = Map("a!" -> 2, "b!" -> 1)
    countsOf(counted.collect()) shouldBe expected
    countsOf(row(counted.collect())) shouldBe expected
    val filtered = source
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2))
      .filterKeys((key: String) => key != "!")
    shown(filtered.collect()) shouldBe Seq("a!:1", "a!:2", "b!:3")
    StringPairProject.resetPublishedForTest()
    countsOf(counted.collect()) shouldBe expected
    StringPairProject.publishedForTest should be > 0
    countsAt(counted, 1) shouldBe countsAt(counted, 8192)
  }

  it should "match the row path for a null input key, an empty key, and a null projected key" in {
    val (rowCounts, rowCalls) = countKept(nullKeySource(), columnar = false)
    val (columnarCounts, columnarCalls) = countKept(nullKeySource(), columnar = true)
    rowCounts shouldBe Map("a!" -> 2, "n" -> 2)
    columnarCounts shouldBe rowCounts
    columnarCalls shouldBe rowCalls
    columnarCalls shouldBe 5
    val keptEmpty = DistCollection(Seq("" -> 1, "a" -> 2), 1)
      .map((pair: (String, Int)) => (pair._1, pair._2))
      .filterKeys((key: String) => key != "a")
      .countByKey
    countsOf(keptEmpty.collect()) shouldBe Map("" -> 1)
    countsOf(row(keptEmpty.collect())) shouldBe Map("" -> 1)
    val calls = new AtomicInteger
    val dropped = DistCollection(Seq("drop" -> 1, "b" -> 2, "" -> 3), 1)
      .map(projectNull(calls))
      .filterKeys((key: String) => key != "!")
      .countByKey
    calls.set(0)
    countsOf(dropped.collect()) shouldBe Map("b!" -> 1)
    calls.get() shouldBe 3
    countsOf(row(dropped.collect())) shouldBe Map("b!" -> 1)
    val composed = "é"
    val decomposed = "e\u0301"
    val accents = DistCollection(Seq(composed -> 3, decomposed -> 4, composed -> 5), 1)
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2))
      .filterKeys((key: String) => key != "!")
      .countByKey
    val accentCounts = Map(composed + "!" -> 2, decomposed + "!" -> 1)
    countsOf(accents.collect()) shouldBe accentCounts
    countsOf(row(accents.collect())) shouldBe accentCounts
    (composed + "!") should not be (decomposed + "!")
  }

  it should "lower filterValues above the project and match the row path" in {
    val source = DistCollection(Seq("a" -> 1, "a" -> 2, "b" -> 3, "" -> 4), 1)
    val counted = source
      .map((pair: (String, Int)) => (pair._1 + "!", pair._2))
      .filterValues((value: Int) => value > 1)
      .countByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(FilterValue(Project(Scan))))",
    )
    val expected = Map("a!" -> 1, "b!" -> 1, "!" -> 1)
    countsOf(counted.collect()) shouldBe expected
    countsOf(row(counted.collect())) shouldBe expected
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

  private def shownAt(collection: DistCollection[(String, Int)], batchSize: Int): Seq[String] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = true, columnarBatchSize = batchSize))
      ScarletRuntime.get.shuffle.clear()
      shown(collection.collect())
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def widen(pair: (String, Int)): (String, Int) =
    (pair._1.length, pair._2).asInstanceOf[(String, Int)]

  private def countsAt(
      counted: DistCollection[(String, Int)],
      batchSize: Int,
  ): Map[String, Int] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = true, columnarBatchSize = batchSize))
      ScarletRuntime.get.shuffle.clear()
      countsOf(counted.collect())
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def countKept(
      source: DistCollection[(String, Int)],
      columnar: Boolean,
  ): (Map[String, Int], Int) =
    val calls = new AtomicInteger
    val counted = source
      .map(shiftNullKey(calls))
      .filterKeys((key: String) => key != "!")
      .countByKey
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = columnar))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      (countsOf(counted.collect()), calls.get())
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

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

  /** A present string key witnesses the scan. Later null keys stay in the batch. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullEmptySource(): DistCollection[(String, Int)] =
    DistCollection(
      Seq(
        stringPair("b", Integer.valueOf(2)),
        stringPair(null, Integer.valueOf(1)),
        stringPair("", Integer.valueOf(3)),
        stringPair(null, Integer.valueOf(4)),
      ),
      1,
    )

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
