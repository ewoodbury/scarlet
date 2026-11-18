package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.StringPairProject
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * One `(String, Int) => (String, Int)` map lowers to a project and feeds `countByKey`.
 *
 * The row path is the oracle, including a null-key call count. `""` is a present key. `"é"` and
 * `"e\u0301"` stay distinct. A filter above the project, and a map under `sortByStringKey`, stay
 * empty. The function body is not inspected: a widened cell lowers, then the kernel declines.
 */
class TestStringPairMap extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val accent = "é"
  private val decomposed = "e\u0301"

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

  "a string pair map" should "lower to a project and count the remapped key" in {
    val source = DistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3), 1)
    val mapped = source.map { case (key, value) => (key + "!", value) }
    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some("Project(Scan)")
    val counted = mapped.countByKey
    ColumnarPlanner.lower(counted.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(Project(Scan)))",
    )
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      countsOf(counted.collect())
    }
    val countedRows = atBatch(8192)
    countedRows shouldBe Map("a!" -> 2, "b!" -> 1)
    atBatch(1) shouldBe countedRows
    countedRows shouldBe countsOf(row(counted.collect()))

    StringPairProject.resetPublishedForTest()
    val projected = mapped.collect().toList
    StringPairProject.publishedForTest should be > 0
    projected shouldBe List("a!" -> 1, "b!" -> 2, "a!" -> 3)
    projected shouldBe row(mapped.collect().toList)
  }

  it should "match null-key call counts, keep an empty key, and keep both accented forms" in {
    val calls = new AtomicInteger
    val counted = nullSource.map(shiftNull(calls)).countByKey
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      val found = countsOf(counted.collect())
      (found, calls.get)
    }
    val (found, callCount) = atBatch(8192)
    found shouldBe Map(
      "a!" -> 1,
      "gone" -> 3,
      "!" -> 2,
      accent + "!" -> 1,
      decomposed + "!" -> 1,
      "b!" -> 1,
    )
    (accent + "!") should not be (decomposed + "!")
    atBatch(1) shouldBe (found, callCount)
    calls.set(0)
    countsOf(row(counted.collect())) shouldBe found
    calls.get shouldBe callCount

    val plain = DistCollection(Seq("" -> 1, accent -> 2, decomposed -> 3, "" -> 4), 1)
    val identity = plain.map((pair: (String, Int)) => (pair._1, pair._2)).countByKey
    val kept = countsOf(identity.collect())
    kept shouldBe Map("" -> 2, accent -> 1, decomposed -> 1)
    kept shouldBe countsOf(row(identity.collect()))
    kept.keySet.contains(accent) shouldBe true
    kept.keySet.contains(decomposed) shouldBe true
  }

  it should "count a null value and still write both columns" in {
    val calls = new AtomicInteger
    val mapped = nullValueSource.map(shiftNull(calls))
    val counted = mapped.countByKey
    calls.set(0)
    countsOf(counted.collect()) shouldBe Map("b!" -> 1, "a!" -> 1)
    calls.get shouldBe 2
    calls.set(0)
    countsOf(row(counted.collect())) shouldBe Map("b!" -> 1, "a!" -> 1)
    calls.get shouldBe 2

    StringPairProject.resetPublishedForTest()
    val projected = mapped.collect().toList
    StringPairProject.publishedForTest should be > 0
    projected.map(pair => pair._1) shouldBe List("b!", "a!")
    absentValue(projected.lift(0).getOrElse(fail("missing null value"))) shouldBe true
    projected.lift(1).getOrElse(fail("missing present value"))._2 shouldBe 4
  }

  it should "lower a widened pair and fall back when the cell is not a string" in {
    val source = DistCollection(Seq("ab" -> 1, "c" -> 2), 1)
    val widened = source.map((pair: (String, Int)) => (pair._1.length, pair._2))
    ColumnarPlanner.lower(widened.plan).map(_.shape) shouldBe Some("Project(Scan)")
    ColumnarPlanner.tryRun(widened.plan) shouldBe None
    widened.collect().toList shouldBe row(widened.collect().toList)

    val text = source.map((pair: (String, Int)) => pair._1)
    ColumnarPlanner.lower(text.plan) shouldBe None
    text.collect().toList shouldBe row(text.collect().toList)
  }

  it should "leave a filter above the project, and a map under sort, on the row path" in {
    val source = DistCollection(Seq("a" -> 1, "b" -> 2, "a" -> 3), 1)
    val mapped = source.map { case (key, value) => (key + "!", value) }
    val byKey = mapped.filterKeys(notKey("b!"))
    val byRow = mapped.filter((pair: (String, Int)) => pair._2 > 1)
    val sorted = mapped.sortByStringKey
    val sandwiched = mapped
      .filter((pair: (String, Int)) => pair._2 > 0)
      .map { case (key, value) => (key + "?", value) }

    ColumnarPlanner.lower(byKey.plan) shouldBe None
    ColumnarPlanner.lower(byRow.plan) shouldBe None
    ColumnarPlanner.lower(sorted.plan) shouldBe None
    ColumnarPlanner.lower(sandwiched.plan) shouldBe None
    byKey.collect().toList shouldBe row(byKey.collect().toList)
    byRow.collect().toList shouldBe row(byRow.collect().toList)
    sorted.collect().toList shouldBe row(sorted.collect().toList)
    sandwiched.collect().toList shouldBe row(sandwiched.collect().toList)
  }

  it should "lower a filter under the map and a second map" in {
    val source = DistCollection(Seq("keep" -> 1, "drop" -> 2, "keep" -> 3), 1)
    val underKey = source
      .filterKeys(notKey("drop"))
      .map { case (key, value) => (key + "!", value) }
    val underRow = source
      .filter((pair: (String, Int)) => pair._2 > 1)
      .map { case (key, value) => (key + "!", value) }
    val twice = source
      .map { case (key, value) => (key + "!", value) }
      .map { case (key, value) => (key + "?", value) }

    ColumnarPlanner.lower(underKey.plan).map(_.shape) shouldBe Some("Project(FilterKey(Scan))")
    ColumnarPlanner.lower(underKey.countByKey.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(Project(FilterKey(Scan))))",
    )
    ColumnarPlanner.lower(underRow.plan).map(_.shape) shouldBe Some("Project(FilterRow(Scan))")
    ColumnarPlanner.lower(twice.plan).map(_.shape) shouldBe Some("Project(Project(Scan))")
    countsOf(underKey.countByKey.collect()) shouldBe Map("keep!" -> 2)
    countsOf(underKey.countByKey.collect()) shouldBe countsOf(row(underKey.countByKey.collect()))
    underRow.collect().toList shouldBe row(underRow.collect().toList)
    twice.collect().toList shouldBe List("keep!?" -> 1, "drop!?" -> 2, "keep!?" -> 3)
    twice.collect().toList shouldBe row(twice.collect().toList)

    ColumnarPlanner.lower(underKey.keys.plan).map(_.shape) shouldBe Some(
      "Key(Project(FilterKey(Scan)))",
    )
    ColumnarPlanner.lower(underKey.values.plan).map(_.shape) shouldBe Some(
      "Value(Project(FilterKey(Scan)))",
    )
    underKey.keys.collect().toList shouldBe row(underKey.keys.collect().toList)
    underKey.values.collect().toList shouldBe row(underKey.values.collect().toList)

    val valued = underKey.mapValues((value: Int) => value + 1)
    ColumnarPlanner.lower(valued.plan).map(_.shape) shouldBe Some(
      "MapValue(Project(FilterKey(Scan)))",
    )
    valued.collect().toList shouldBe row(valued.collect().toList)

    val reduced = underKey.reduceByKey((left: Int, right: Int) => left + right)
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "StringHashAggregate(Exchange(Project(FilterKey(Scan))))",
    )
    reduced.collect().toMap shouldBe row(reduced.collect().toMap)

    val grouped = underKey.groupByKey
    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some(
      "GroupByKey(Exchange(Project(FilterKey(Scan))))",
    )
    groupsOf(grouped.collect()) shouldBe groupsOf(row(grouped.collect()))
  }

  private def countsOf(rows: Iterable[(String, Int)]): Map[String, Int] =
    rows.foldLeft(Map.empty[String, Int]) { (seen, row) =>
      require(!seen.contains(row._1), s"duplicate count for ${row._1}")
      seen.updated(row._1, row._2)
    }

  private def groupsOf(rows: Iterable[(String, Iterable[Int])]): Map[String, List[Int]] =
    rows.map { case (key, values) => key -> values.toList }.toMap

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def shiftNull(calls: AtomicInteger): ((String, Int)) => (String, Int) =
    pair => {
      val _ = calls.incrementAndGet()
      val product = pair.asInstanceOf[Product]
      val value = product.productElement(1)
      val key = product.productElement(0) match
        case null => "gone"
        case present: String => present + "!"
        case other => throw new IllegalArgumentException(s"pair map saw $other")
      Tuple2(key, value).asInstanceOf[(String, Int)]
    }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullSource: DistCollection[(String, Int)] =
    def cell(key: Any, value: Any): (String, Int) =
      Tuple2(key, value).asInstanceOf[(String, Int)]
    DistCollection(
      Plan.Source(
        Seq(
          Partition(
            Seq(
              cell("a", Int.box(1)),
              cell(null, Int.box(2)),
              cell("", Int.box(3)),
              cell(accent, Int.box(4)),
              cell(decomposed, Int.box(5)),
              cell("b", null),
              cell(null, null),
            ),
          ),
          Partition(Seq(cell(null, Int.box(6)), cell("", Int.box(7)))),
        ),
      ),
    )

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullValueSource: DistCollection[(String, Int)] =
    def cell(key: String, value: Any): (String, Int) =
      Tuple2(key, value).asInstanceOf[(String, Int)]
    DistCollection(Plan.Source(Seq(Partition(Seq(cell("b", null), cell("a", Int.box(4)))))))

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentValue(row: (String, Int)): Boolean = row.productElement(1) match
    case null => true
    case _ => false

  private def notKey(dropped: String): String => Boolean = key => key match
    case `dropped` => false
    case _ => true

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
