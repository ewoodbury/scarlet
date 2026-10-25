package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, NoOpSink}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

class TestColumnarActions extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "take" should "take an int filter and map in partition order and decode only the requested rows" in {
    val mapCalls = new AtomicInteger
    def mapped = DistCollection(1 to 12, 4).filter(_ % 2 == 0).map { value =>
      mapCalls.incrementAndGet()
      value * 3
    }

    val expected = row(mapped.take(3))
    mapCalls.set(0)
    armColumnar()
    ColumnMetrics.withSink(new CountingSink) {
      mapped.take(3) shouldBe expected
      ColumnMetrics.decodedValueCount shouldBe 3L
      mapCalls.get() shouldBe 3
      assertNarrow(mapped, 4)
    }
    mapped.first() shouldBe mapped.take(1).headOption.getOrElse(fail("take(1) was empty"))
  }

  it should "take a prefix that spans several morsels of one partition" in {
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 4, threadPoolSize = 1),
    )
    val values = DistCollection(1 to 20, 1).filter(_ % 2 == 0).map(_ + 1)

    values.take(7) shouldBe List(3, 5, 7, 9, 11, 13, 15)
    values.take(12) shouldBe List(3, 5, 7, 9, 11, 13, 15, 17, 19, 21)
  }

  it should "stay on the row path when columnar execution is disabled" in {
    ScarletConf.set(originalConf.copy(columnarExecution = false))
    ColumnMetrics.withSink(new CountingSink) {
      DistCollection(1 to 8, 2).map(_ * 2).take(3) shouldBe List(2, 4, 6)
      ColumnMetrics.decodedValueCount shouldBe 0L
    }
  }

  it should "return empty for zero and negative limits" in {
    armColumnar()
    val values = DistCollection(1 to 8, 3).map(_ * 2)

    values.take(0) shouldBe empty
    values.take(-2) shouldBe empty
  }

  it should "return all values when the limit exceeds the source" in {
    def values = DistCollection(1 to 5, 3).filter(_ > 1).map(_ + 10)

    val expected = row(values.take(20))
    columnar(values.take(20)) shouldBe expected
  }

  it should "take accepted pair and string plans like the row path" in {
    def pairs = DistCollection(Seq(1 -> 10, 2 -> 20, 3 -> 30, 4 -> 40), 2)
      .filterValues[Int, Int](_ >= 20)
    def strings = DistCollection(Seq("red", "blue", "red", "green"), 2)
      .filterStringEquals("red")
      .map(_.toUpperCase(java.util.Locale.ENGLISH))

    val expectedPairs = row(pairs.take(2))
    val expectedStrings = row(strings.take(1))

    columnar(pairs.take(2)) shouldBe expectedPairs
    columnar(strings.take(1)) shouldBe expectedStrings
    columnar(ColumnarPlanner.partitioning(pairs.plan).map(_.layout)) shouldBe
      Some(Layout.Columnar(ScarletConf.get.columnarBatchSize))
    columnar(ColumnarPlanner.partitioning(strings.plan).map(_.layout)) shouldBe
      Some(Layout.Utf8Dict(ScarletConf.get.columnarBatchSize))
  }

  it should "keep exchange plans on the row path" in {
    def reduced = DistCollection(Seq(1 -> 2, 2 -> 4, 1 -> 8, 3 -> 6), 2)
      .reduceByKey[Int, Int](_ + _)

    val expected = row(reduced.take(2))
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    ColumnMetrics.withSink(new CountingSink) {
      reduced.take(2) shouldBe expected
      ColumnMetrics.batchCount shouldBe 0L
      ColumnMetrics.decodedValueCount shouldBe 0L
    }
  }

  "count" should "match the row path on a filtered column" in {
    val ints = Seq(1, 2, 3, 4, 5)
    columnar(DistCollection(ints, 2).filter(_ > 2).count()) shouldBe 3L
    columnar(DistCollection(ints, 2).filter(_ > 2).count()) shouldBe
      row(DistCollection(ints, 2).filter(_ > 2).count())
  }

  it should "count grouped keys" in {
    val pairs = Seq(1 -> 10, 1 -> 3, 2 -> 5, 3 -> 1)

    def grouped = DistCollection(pairs, 2).reduceByKey[Int, Int](_ + _).count()

    columnar(grouped) shouldBe 3L
    columnar(grouped) shouldBe row(grouped)
  }

  it should "count a string filter, including rows that survive" in {
    columnar(DistCollection(Seq("a", "bb", "c"), 2).filter(_.length > 1).count()) shouldBe 1L
  }

  it should "count dictionary literal filters from the code mask" in {
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 2, threadPoolSize = 4),
    )
    val rows = stringsWithNull()

    def source = DistCollection(rows, 1)
    def single = source.filterStringEquals("US")
    def several = source.filterStringIn("US", "CA", "MX")
    def absent = source.filterStringIn("US", "ZZ")
    def nothing = source.filterStringEquals("ZZ")
    def everything = source.filterStringIn("US", "CA", "GB")
    def contradicted = source.filterStringEquals("US").filterStringEquals("CA")
    def left = source.filterStringEquals("US").filterStringIn("CA", "US")
    def right = source.filterStringIn("US", "CA").filterStringEquals("US")
    def blocked = source.filter(keepsLongOrNull).filterStringEquals("US")
    def mapped = source.filterStringEquals("US").map((value: String) => value)

    sameCount(single)
    sameCount(several)
    sameCount(absent)
    sameCount(nothing)
    sameCount(everything)
    sameCount(contradicted)
    sameCount(left)
    sameCount(right)
    left.count() shouldBe right.count()
    sameCount(blocked)
    blocked.count() shouldBe 0L
    sameCount(mapped)

    val hits = new AtomicInteger
    val counted = source.filterStringEquals("US").map { (value: String) =>
      hits.incrementAndGet()
      value
    }
    counted.count() shouldBe 3L
    hits.get() shouldBe 3

    ColumnarPlanner.partitioning(single.plan).isDefined shouldBe true
    ColumnarPlanner.tryCount(single.plan) shouldBe Some(3L)
    ColumnarPlanner.tryCount(several.plan) shouldBe Some(4L)
    ColumnarPlanner.tryCount(nothing.plan) shouldBe Some(0L)
    ColumnarPlanner.tryCount(everything.plan) shouldBe Some(5L)

    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 2, threadPoolSize = 4),
    )
    ColumnMetrics.withSink(new CountingSink) {
      single.count() shouldBe 3L
      ColumnMetrics.batchCount shouldBe 1L
    }

    val empty = DistCollection.ofStrings(Array.empty[String], 3).filterStringEquals("US")
    empty.count() shouldBe 0L
    ColumnarPlanner.tryCount(empty.plan) shouldBe Some(0L)
    ColumnarPlanner.partitioning(empty.plan).isDefined shouldBe true

    val gapped = DistCollection.ofStrings(Array("a", "b", "a"), 4).filterStringEquals("a")
    gapped.count() shouldBe 2L
    ColumnarPlanner.tryCount(gapped.plan) shouldBe Some(2L)
  }

  it should "filter dictionary codes without intermediate batches" in {
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 16, threadPoolSize = 1),
    )
    ColumnMetrics.withSink(new CountingSink) {
      val filtered = DistCollection(Seq("US", "CA", "US"), 1).filterStringEquals("US").collect()
      filtered shouldBe Seq("US", "US")
      ColumnMetrics.batchCount shouldBe 2L
      ColumnMetrics.visitCount shouldBe 1L
    }
  }

  it should "stay correct when morsels split a partition" in {
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 2, threadPoolSize = 4),
    )
    val values = DistCollection(Seq(1, 2, 3, 4, 5), 1).filter(_ > 1)

    values.count() shouldBe 4L
    values.sum shouldBe 14L
  }

  it should "refuse a mistyped map before execution and run it once per row" in {
    ScarletConf.set(originalConf.copy(columnarExecution = true, threadPoolSize = 1))
    val hits = new AtomicInteger
    def mapped = DistCollection(Seq(1, 2, 3), 1).map { value =>
      hits.incrementAndGet()
      value.toString
    }

    ColumnMetrics.withSink(new CountingSink) {
      ColumnarPlanner.tryCount(mapped.plan) shouldBe None
      ColumnarPlanner.trySumInt(mapped.plan) shouldBe None
      hits.get() shouldBe 0
      assertNoColumnarWork()
    }

    mapped.count() shouldBe 3L
    hits.get() shouldBe 3
    hits.set(0)
    mapped.collect() shouldBe Seq("1", "2", "3")
    hits.get() shouldBe 3
  }

  it should "refuse a mistyped double map before execution and run it once per row" in {
    ScarletConf.set(originalConf.copy(columnarExecution = true, threadPoolSize = 1))
    val hits = new AtomicInteger
    def mapped = DistCollection(Seq(1.0, 2.0), 1).map { value =>
      hits.incrementAndGet()
      value.toInt
    }

    ColumnMetrics.withSink(new CountingSink) {
      ColumnarPlanner.tryCount(mapped.plan) shouldBe None
      ColumnarPlanner.trySumInt(mapped.plan) shouldBe None
      hits.get() shouldBe 0
      assertNoColumnarWork()
    }

    mapped.sum shouldBe 3L
    hits.get() shouldBe 2
    hits.set(0)
    mapped.collect() shouldBe Seq(1, 2)
    hits.get() shouldBe 2
  }

  it should "invoke a mistyped map once per row" in {
    ScarletConf.set(originalConf.copy(columnarExecution = true, threadPoolSize = 1))
    val rows = Seq(1, 2, 3, 4, 5)
    val directHits = new AtomicInteger
    val chainHits = new AtomicInteger
    def direct = DistCollection(rows, 1).map { value =>
      directHits.incrementAndGet()
      value.toLong
    }
    def pairs = DistCollection(rows, 1).map(value => (value, value))
    def filtered = DistCollection(rows, 1).filter(_ > 0).map(_.toString)
    def lengths = DistCollection(Seq("red", "blue", "green"), 1).map(_.length)
    def chain = DistCollection(rows, 1)
      .filter(_ > 0)
      .map(_ + 1)
      .map { value =>
        chainHits.incrementAndGet()
        value.toString
      }

    ColumnarPlanner.partitioning(direct.plan) shouldBe None
    ColumnarPlanner.partitioning(pairs.plan) shouldBe None
    ColumnarPlanner.partitioning(filtered.plan) shouldBe None
    ColumnarPlanner.partitioning(lengths.plan) shouldBe None
    ColumnarPlanner.partitioning(chain.plan) shouldBe None
    ColumnarPlanner.tryRun(direct.plan) shouldBe None
    ColumnarPlanner.tryRun(chain.plan) shouldBe None
    directHits.get() shouldBe 0
    chainHits.get() shouldBe 0

    direct.collect() shouldBe Seq(1L, 2L, 3L, 4L, 5L)
    directHits.get() shouldBe rows.length
    pairs.collect() shouldBe Seq((1, 1), (2, 2), (3, 3), (4, 4), (5, 5))
    filtered.collect() shouldBe Seq("1", "2", "3", "4", "5")
    lengths.collect() shouldBe Seq(3, 4, 5)
    chain.collect() shouldBe Seq("2", "3", "4", "5", "6")
    chainHits.get() shouldBe rows.length
  }

  "sum" should "add a filtered int column as a long" in {
    val ints = Seq(1, 2, 3, 4, 5)

    def summed = DistCollection(ints, 3).filter(_ > 0).map(_ * 2).sum

    columnar(summed) shouldBe 30L
    columnar(summed) shouldBe row(summed)
  }

  it should "not wrap Int.MaxValue" in {
    val values = Seq(Int.MaxValue, Int.MaxValue)
    val wide = Int.MaxValue.toLong * 2L

    columnar(DistCollection(values, 2).sum) shouldBe wide
    row(DistCollection(values, 2).sum) shouldBe wide
  }

  it should "sum negatives and return zero for empty input" in {
    columnar(DistCollection(Seq(-3, 4, -1), 2).sum) shouldBe 0L
    columnar(DistCollection(Seq.empty[Int], 2).sum) shouldBe 0L
    columnar(DistCollection(Seq.empty[Int], 2).count()) shouldBe 0L
    columnar(DistCollection(Seq.empty[Double], 1).sum) shouldBe 0.0
  }

  it should "left-fold a double partition across morsel boundaries" in {
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 2, threadPoolSize = 4),
    )
    val values = Seq(1.0, 1.0e16, -1.0e16, 1.0)
    val rows = DistCollection(values, 1)

    rows.sum shouldBe 1.0
    rows.map(value => value).sum shouldBe 1.0
    rows.filter((_: Double) => true).sum shouldBe 1.0
    row(rows.sum) shouldBe 1.0
  }

  it should "sum a filtered double column" in {
    def summed = DistCollection(Seq(1.0, -2.0, 3.5), 2).filter(_ > 0.0).sum

    columnar(summed) shouldBe 4.5
    columnar(summed) shouldBe row(summed)
  }

  it should "sum and count an empty filter result" in {
    def dropped = DistCollection(Seq(1, 2, 3), 3).filter(_ > 10)

    columnar(dropped.count()) shouldBe 0L
    columnar(dropped.sum) shouldBe 0L
  }

  it should "sum a union on the row path" in {
    val left = DistCollection(Seq(Int.MaxValue), 1)
    val right = DistCollection(Seq(1), 1)

    columnar(left.union(right).sum) shouldBe Int.MaxValue.toLong + 1L
  }

  "groupedSum" should "sum one key of two max ints, and reduceByKey still wraps" in {
    val rows = Seq(1 -> Int.MaxValue, 1 -> Int.MaxValue)
    val wide = Int.MaxValue.toLong * 2L

    def source = DistCollection(rows, 2)

    columnar(source.groupedSum) shouldBe wide
    row(source.groupedSum) shouldBe wide
    columnar(source.reduceByKey[Int, Int](_ + _).collect().toMap) shouldBe Map(
      1 -> (Int.MaxValue + Int.MaxValue),
    )
    columnar(assertNarrow(source, 2))
  }

  it should "sum the same rows after filterValues" in {
    val rows = Seq(1 -> Int.MaxValue, 1 -> Int.MaxValue)
    val wide = Int.MaxValue.toLong * 2L

    def filtered = DistCollection(rows, 2).filterValues[Int, Int](_ == Int.MaxValue)

    columnar(filtered.groupedSum) shouldBe wide
    row(filtered.groupedSum) shouldBe wide
    columnar(assertNarrow(filtered, 2))
  }

  it should "keep a columnar layout through filterKeys" in {
    val rows = Seq(1 -> Int.MaxValue, 1 -> Int.MaxValue)
    val wide = Int.MaxValue.toLong * 2L

    def filtered = DistCollection(rows, 2).filterKeys[Int, Int](_ == 1)

    columnar(filtered.groupedSum) shouldBe wide
    columnar(assertNarrow(filtered, 2))
  }

  it should "return zero for an empty source through the row engine" in {
    def empty = DistCollection(Seq.empty[(Int, Int)], 2)

    columnar(empty.groupedSum) shouldBe 0L
    row(empty.groupedSum) shouldBe 0L
    columnar(ColumnarPlanner.partitioning(empty.plan) shouldBe None)
  }

  it should "return zero when filterValues keeps no rows" in {
    val rows = Seq(1 -> 10, 2 -> 20)

    def filtered = DistCollection(rows, 2).filterValues[Int, Int](_ < 0)

    columnar(filtered.groupedSum) shouldBe 0L
    row(filtered.groupedSum) shouldBe 0L
    columnar(assertNarrow(filtered, 2))
  }

  it should "sum a row filter inside the grouped scan" in {
    val rows = Seq(1 -> 10, 2 -> 3, 4 -> 8, 5 -> 1)
    val hits = new AtomicInteger

    def filtered = DistCollection(rows, 2).filter { case (key, value) =>
      hits.incrementAndGet()
      key % 2 == 0 && value > 2
    }

    val expected = row(filtered.groupedSum)
    hits.set(0)
    columnar(filtered.groupedSum) shouldBe expected
    hits.get() shouldBe rows.length
    columnar(assertNarrow(filtered, 2))
    columnar(filtered.groupedSum) shouldBe 11L
  }

  it should "sum a pair map inside the grouped scan" in {
    val rows = Seq(1 -> 10, 2 -> 20, 3 -> 5)
    val hits = new AtomicInteger

    def mapped = DistCollection(rows, 2).map { case (key, value) =>
      hits.incrementAndGet()
      (key + 1, value + key)
    }

    hits.set(0)
    columnar(ColumnarPlanner.partitioning(mapped.plan))
    hits.get() shouldBe 0
    val expected = row(mapped.groupedSum)
    hits.set(0)
    columnar(mapped.groupedSum) shouldBe expected
    hits.get() shouldBe rows.length
    columnar(ColumnarPlanner.partitioning(mapped.plan) shouldBe None)
    columnar(
      ColumnarPlanner.groupedSumPeel(mapped.plan) shouldBe Some("GroupedSum(MapPair(Scan))"),
    )
    columnar(mapped.groupedSum) shouldBe 41L
  }

  it should "apply a filter before a map that reads the rewritten pair" in {
    val rows = Seq(1 -> 1, 2 -> 3, 3 -> 5)
    val filters = new AtomicInteger
    val maps = new AtomicInteger

    def planned = DistCollection(rows, 1)
      .filter { case (_, value) =>
        filters.incrementAndGet()
        value > 2
      }
      .map { case (key, value) =>
        maps.incrementAndGet()
        (value, key)
      }

    val expected = row(planned.groupedSum)
    filters.set(0)
    maps.set(0)
    columnar(planned.groupedSum) shouldBe expected
    filters.get() shouldBe rows.length
    maps.get() shouldBe 2
    columnar(ColumnarPlanner.partitioning(planned.plan) shouldBe None)
    columnar(
      ColumnarPlanner.groupedSumPeel(planned.plan) shouldBe Some(
        "GroupedSum(MapPair(FilterRow(Scan)))",
      ),
    )
    columnar(planned.groupedSum) shouldBe 5L
  }

  it should "return zero when a row filter keeps nothing and stay columnar" in {
    val rows = Seq(1 -> 4, 2 -> 5)
    val hits = new AtomicInteger

    def filtered = DistCollection(rows, 2).filter { case (_, value) =>
      hits.incrementAndGet()
      value < 0
    }

    hits.set(0)
    val summed = columnar(filtered.groupedSum)
    summed shouldBe 0L
    hits.get() shouldBe rows.length
    columnar(assertNarrow(filtered, 2))
    row(filtered.groupedSum) shouldBe 0L
  }

  it should "drop a key the key predicate rejects" in {
    def filtered = DistCollection(Seq(1 -> 5, 2 -> 9, 1 -> 1), 2).filterKeys[Int, Int](_ == 1)

    columnar(filtered.groupedSum) shouldBe 6L
    columnar(filtered.groupedSum) shouldBe row(filtered.groupedSum)
    columnar(assertNarrow(filtered, 2))
  }

  it should "materialise only the grouped-sum batches when a filter or map precedes it" in {
    val rows = Seq(1 -> 10, 1 -> 4, 2 -> 7, 3 -> 1, 2 -> 6)
    val kept = new AtomicInteger
    val rewritten = new AtomicInteger

    def source = DistCollection(rows, 1)
    def filtered = source.filter { case (key, value) =>
      kept.incrementAndGet()
      key > 0 && value > 3
    }
    def mapped = source.map { case (key, value) =>
      rewritten.incrementAndGet()
      (key + value, value - key)
    }

    val filteredExpected = groupedRow(filtered.groupedSum)
    val mappedExpected = groupedRow(mapped.groupedSum)
    kept.set(0)
    rewritten.set(0)
    val filteredFootprint = groupedFootprint(filtered.groupedSum)
    val mappedFootprint = groupedFootprint(mapped.groupedSum)
    val plainFootprint = groupedFootprint(source.groupedSum)

    filteredFootprint.result shouldBe filteredExpected
    mappedFootprint.result shouldBe mappedExpected
    kept.get() shouldBe rows.length
    rewritten.get() shouldBe rows.length
    val width = groupedConf(columnarExecution = true).defaultShufflePartitions.toLong
    val batches = 1L + 1L + width + width
    val visits = 1L + 1L + 1L + width
    filteredFootprint.batches shouldBe batches
    filteredFootprint.visits shouldBe visits
    mappedFootprint.batches shouldBe batches
    mappedFootprint.visits shouldBe visits
    plainFootprint.batches shouldBe batches
    plainFootprint.visits shouldBe visits
  }

  it should "sum a flatMapped pair plan on the row path" in {
    def expanded = DistCollection(Seq(1 -> 2, 3 -> 4), 1).flatMap { case (key, value) =>
      Seq((key, value), (key, value + 1))
    }

    columnar(expanded.groupedSum) shouldBe row(expanded.groupedSum)
    columnar(ColumnarPlanner.partitioning(expanded.plan) shouldBe None)
  }

  it should "fold every group sum" in {
    val rows = Seq(1 -> Int.MaxValue, 2 -> 3, 1 -> Int.MaxValue, 2 -> 4)
    val wide = Int.MaxValue.toLong * 2L + 7L

    def source = DistCollection(rows, 2)

    columnar(source.groupedSum) shouldBe wide
    row(source.groupedSum) shouldBe wide
  }

  it should "select the wide kernel for each accepted pair shape" in {
    val rows = Seq(1 -> 10, 2 -> 3, 1 -> 4)
    val source = DistCollection(rows, 2)
    val primitive = DistCollection.ofIntPairs(Array(1, 2, 1), Array(10, 3, 4), 2)
    val empty = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 2)
    val shapes = Seq(
      source.plan,
      source.filterValues[Int, Int](_ > 0).plan,
      source.filterKeys[Int, Int](_ > 0).plan,
      source.filter { case (key, value) => key > 0 && value > 1 }.plan,
      source.map { case (key, value) => (key + 1, value + key) }.plan,
      source.filter { case (_, value) => value > 0 }.map { case (key, value) => (value, key) }.plan,
      primitive.plan,
      empty.plan,
    )

    columnar {
      shapes.foreach { plan =>
        ColumnarPlanner.tryGroupedSum(plan) shouldBe defined
      }
    }
  }

  it should "drop null keys and null values on both grouped-sum paths" in {
    val rows = Seq(
      erasedPair(Some(1), Some(10)),
      erasedPair(None, Some(99)),
      erasedPair(Some(1), None),
      erasedPair(None, None),
      erasedPair(Some(2), Some(3)),
    )
    def source = DistCollection(rows, 2)

    columnar(source.groupedSum) shouldBe 13L
    row(source.groupedSum) shouldBe 13L
  }

  it should "drop a null component before a mapped grouped sum in either row order" in {
    val present = erasedPair(Some(1), Some(10))
    val nullKey = erasedPair(None, Some(99))
    val nullValue = erasedPair(Some(4), None)
    val hits = new AtomicInteger

    def mapped(rows: Seq[(Int, Int)]) =
      DistCollection(rows, 1).map { case (key, value) =>
        hits.incrementAndGet()
        (key + 1, value + key)
      }

    def agree(rows: Seq[(Int, Int)], expected: Long): Unit =
      val planned = mapped(rows)
      hits.set(0)
      columnar(planned.groupedSum) shouldBe expected
      hits.get() shouldBe 1
      hits.set(0)
      row(planned.groupedSum) shouldBe expected
      hits.get() shouldBe 1
      columnar(ColumnarPlanner.tryGroupedSum(planned.plan) shouldBe Some(expected))

    agree(Seq(present, nullKey), 11L)
    agree(Seq(nullKey, present), 11L)
    agree(Seq(present, nullValue), 11L)
    agree(Seq(nullValue, present), 11L)
    agree(Seq(nullKey, nullValue, present), 11L)
    agree(Seq(present, nullValue, nullKey), 11L)

    def agreePlain(rows: Seq[(Int, Int)], expected: Long): Unit =
      val planned = DistCollection(rows, 1)
      columnar(planned.groupedSum) shouldBe expected
      row(planned.groupedSum) shouldBe expected
      columnar(ColumnarPlanner.tryGroupedSum(planned.plan) shouldBe Some(expected))

    agreePlain(Seq(present, nullKey), 10L)
    agreePlain(Seq(nullKey, present), 10L)
    agreePlain(Seq(present, nullValue), 10L)
    agreePlain(Seq(nullValue, present), 10L)
  }

  it should "reject a non-int cell on both grouped-sum paths" in {
    def summed(rows: Seq[(Int, Int)]) = DistCollection(rows, 1).groupedSum

    val badKey = Seq(erasedPair(Some(1), Some(2)), erasedCells("nope", Int.box(4)))
    val badValue = Seq(erasedPair(Some(1), Some(2)), erasedCells(Int.box(1), "nope"))
    val keyMessage = "grouped sum expected an Int key, found java.lang.String"
    val valueMessage = "grouped sum expected an Int value, found java.lang.String"

    val columnarKey = intercept[IllegalArgumentException](columnar(summed(badKey)))
    val rowKey = intercept[IllegalArgumentException](row(summed(badKey)))
    columnarKey.getMessage shouldBe keyMessage
    rowKey.getMessage shouldBe keyMessage

    val columnarValue = intercept[IllegalArgumentException](columnar(summed(badValue)))
    val rowValue = intercept[IllegalArgumentException](row(summed(badValue)))
    columnarValue.getMessage shouldBe valueMessage
    rowValue.getMessage shouldBe valueMessage
  }

  "primitive sources" should "sum an int array like a boxed vector" in {
    val values = Array(1, 2, 3, 4, 5)

    def primitive = DistCollection.ofInts(values, 3).filter(_ > 0).map(_ * 2).sum
    def boxed = DistCollection(values.toVector, 3).filter(_ > 0).map(_ * 2).sum

    columnar(primitive) shouldBe 30L
    columnar(primitive) shouldBe columnar(boxed)
    row(primitive) shouldBe row(boxed)
    columnar {
      ColumnMetrics.withSink(new CountingSink) {
        primitive shouldBe 30L
        ColumnMetrics.primitiveEncodeCount shouldBe 3L
      }
    }
    columnar {
      ColumnMetrics.withSink(new CountingSink) {
        boxed shouldBe 30L
        ColumnMetrics.primitiveEncodeCount shouldBe 0L
      }
    }
  }

  it should "sum the copied ints after the caller mutates the array" in {
    val input = Array(1, 2, 3, 4)
    val rows = DistCollection.ofInts(input, 2)
    input(0) = 100

    columnar(rows.sum) shouldBe 10L
    columnar(rows.collect()) shouldBe Seq(1, 2, 3, 4)
    row(rows.sum) shouldBe 10L
  }

  it should "sum a primitive int source when morsels split a partition" in {
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 2, threadPoolSize = 4),
    )

    DistCollection.ofInts(Array(1, 2, 3, 4, 5), 1).filter(_ > 1).sum shouldBe 14L
  }

  it should "sum a double array like a boxed vector" in {
    val values = Array(1.5, -2.0, 3.25)

    def primitive = DistCollection.ofDoubles(values, 2).filter(_ > 0.0).sum
    def boxed = DistCollection(values.toVector, 2).filter(_ > 0.0).sum

    columnar(primitive) shouldBe 4.75
    columnar(primitive) shouldBe columnar(boxed)
    row(primitive) shouldBe row(boxed)
    columnar {
      ColumnMetrics.withSink(new CountingSink) {
        primitive shouldBe 4.75
        ColumnMetrics.primitiveEncodeCount shouldBe 2L
      }
    }
    columnar {
      ColumnMetrics.withSink(new CountingSink) {
        boxed shouldBe 4.75
        ColumnMetrics.primitiveEncodeCount shouldBe 0L
      }
    }
  }

  it should "reduce an int-pair array like a boxed vector without wrapping" in {
    val keys = Array(1, 2, 1, 3, 2)
    val values = Array(10, 20, 5, 7, 1)
    val expected = Map(1 -> 15, 2 -> 21, 3 -> 7)

    def primitive =
      DistCollection.ofIntPairs(keys, values, 2).reduceByKey[Int, Int](_ + _).collect().toMap
    def boxed =
      DistCollection(keys.zip(values).toVector, 2).reduceByKey[Int, Int](_ + _).collect().toMap

    columnar(primitive) shouldBe expected
    columnar(primitive) shouldBe columnar(boxed)
    row(primitive) shouldBe row(boxed)
    columnar {
      ColumnMetrics.withSink(new CountingSink) {
        primitive shouldBe expected
        ColumnMetrics.primitiveEncodeCount shouldBe 2L
      }
    }
    columnar {
      ColumnMetrics.withSink(new CountingSink) {
        boxed shouldBe expected
        ColumnMetrics.primitiveEncodeCount shouldBe 0L
      }
    }
  }

  "int pair projections" should "sum values from the shared value column" in {
    val keys = Array(1, -4, 3, Int.MaxValue)
    val values = Array(Int.MaxValue, 5, -2, Int.MaxValue)

    def rows = DistCollection.ofIntPairs(keys, values, 1)
    def summed = rows.values.sum

    val expected = row(summed)
    armColumnar()
    ColumnMetrics.withSink(new CountingSink) {
      summed shouldBe expected
      assertColumnar(rows.values, width = 1)
      assertOnePass()
    }
    columnar(DistCollection.ofIntPairs(keys, values, 2).values.collect()) shouldBe
      row(DistCollection.ofIntPairs(keys, values, 2).values.collect())
  }

  it should "sum keys from the shared key column" in {
    val keys = Array(1, -4, 3, Int.MaxValue)
    val values = Array(Int.MaxValue, 5, -2, Int.MaxValue)

    def rows = DistCollection.ofIntPairs(keys, values, 1)
    def summed = rows.keys.sum

    val expected = row(summed)
    armColumnar()
    ColumnMetrics.withSink(new CountingSink) {
      summed shouldBe expected
      assertColumnar(rows.keys, width = 1)
      assertOnePass()
    }
    columnar(DistCollection.ofIntPairs(keys, values, 2).keys.collect()) shouldBe
      row(DistCollection.ofIntPairs(keys, values, 2).keys.collect())
  }

  it should "sum mapValues that stay ints without writing a value column" in {
    val keys = Array(1, -4, 3, Int.MaxValue)
    val values = Array(10, 20, 30, 40)
    val calls = new AtomicInteger

    // mapValues keeps the pair. The sum is the mapped value column.
    def rows = DistCollection.ofIntPairs(keys, values, 1).mapValues { (value: Int) =>
      calls.incrementAndGet()
      value * 2
    }
    def summed = rows.values.sum

    val expected = row(summed)
    calls.getAndSet(0)
    armColumnar()
    ColumnarPlanner.partitioning(rows.plan)
    calls.get() shouldBe 0
    ColumnMetrics.withSink(new CountingSink) {
      summed shouldBe expected
      calls.get() shouldBe values.length
      assertColumnar(rows, width = 1)
      assertOnePass()
    }
    val projected = DistCollection.ofIntPairs(keys, values, 2).mapValues[Int, Int, Int](_ * 2)
    columnar(projected.collect()) shouldBe row(projected.collect())
  }

  it should "keep an anonymous int mapValues on the columnar path" in {
    val keys = Array(1, 2)
    val values = Array(3, 4)
    val double: Int => Int = new Function1[Int, Int]:
      def apply(value: Int): Int = value * 2

    def summed = DistCollection.ofIntPairs(keys, values, 1).mapValues(double).values.sum

    val expected = row(summed)
    armColumnar()
    summed shouldBe expected
    assertColumnar(
      DistCollection.ofIntPairs(keys, values, 1).mapValues(double),
      width = 1,
    )
  }

  it should "leave an overloaded apply(Any) beside apply(Int) on the row path" in {
    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    val checked = locally {
      val overloaded = new Function1[Any, Any]:
        def apply(value: Any): Any = "nope"
        def apply(value: Int): Int = value * 2
      val mapped =
        DistCollection.ofIntPairs(Array(1), Array(10), 1).mapValues[Int, Int, Any](overloaded)

      armColumnar()
      ColumnarPlanner.partitioning(mapped.plan) shouldBe None
      mapped.collect().toList shouldBe List((1, "nope"))
      columnar(mapped.collect().toList) shouldBe row(mapped.collect().toList)
    }
    checked
  }

  it should "leave a mapValues that changes type on the row path" in {
    val keys = Array(1, 2, 3)
    val values = Array(4, 5, 6)

    def strings = DistCollection.ofIntPairs(keys, values, 2).mapValues[Int, Int, String](_.toString)
    def widened = DistCollection.ofIntPairs(keys, values, 2).mapValues[Int, Int, Long](_.toLong)

    armColumnar()
    ColumnarPlanner.partitioning(strings.plan) shouldBe None
    ColumnarPlanner.partitioning(widened.plan) shouldBe None
    val collected = ColumnMetrics.withSink(new CountingSink) {
      val rows = strings.collect()
      ColumnMetrics.batchCount shouldBe 0L
      ColumnMetrics.visitCount shouldBe 0L
      rows
    }
    collected shouldBe row(strings.collect())
    columnar(widened.collect()) shouldBe row(widened.collect())
  }

  it should "sum values after filterValues and stay columnar" in {
    val keys = Array(1, 2, 3)
    val values = Array(-1, 4, 5)

    def filtered = DistCollection.ofIntPairs(keys, values, 2).filterValues[Int, Int](_ > 0)

    val expected = row(filtered.values.sum)
    armColumnar()
    filtered.values.sum shouldBe expected
    assertColumnar(filtered.values, width = 2)
  }

  it should "return zero for an empty pair projection on the columnar path" in {
    def empty = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 1)

    columnar {
      empty.values.sum shouldBe 0L
      empty.keys.sum shouldBe 0L
      assertColumnar(empty.values, width = 1)
      assertColumnar(empty.keys, width = 1)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Null"))
  private def keepsLongOrNull(value: String): Boolean =
    (value eq null) || value.length > 2

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def stringsWithNull(): Seq[String] =
    Seq("US", "CA", null, "US", "GB", "US")

  private def sameCount(collection: DistCollection[_]): Unit =
    val expected = row(collection.count())
    columnar {
      collection.count() shouldBe expected
      ColumnarPlanner.tryCount(collection.plan) shouldBe Some(expected)
    }

  private def assertNoColumnarWork(): Unit =
    ColumnMetrics.batchCount shouldBe 0L
    ColumnMetrics.visitCount shouldBe 0L
    ColumnMetrics.decodedValueCount shouldBe 0L
    ColumnMetrics.primitiveEncodeCount shouldBe 0L

  private def armColumnar(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 1024, threadPoolSize = 4),
    )

  private def assertColumnar[A](collection: DistCollection[A], width: Int): Unit =
    ColumnarPlanner.partitioning(collection.plan) shouldBe Some(
      PartitioningInfo(
        Distribution.Unknown(width),
        OrderingTag.Unsorted,
        Layout.Columnar(ScarletConf.get.columnarBatchSize),
      ),
    )

  /** The source copy is the only batch, and the sum visits that one morsel. */
  private def assertOnePass(): Unit =
    ColumnMetrics.batchCount shouldBe 1L
    ColumnMetrics.visitCount shouldBe 1L

  private final case class Footprint(result: Long, batches: Long, visits: Long)

  /** One partition, so the scan is one morsel. Shuffle width is the exchange and the final sum. */
  private def groupedConf(columnarExecution: Boolean): ScarletConf =
    originalConf.copy(
      columnarExecution = columnarExecution,
      columnarBatchSize = 1024,
      defaultShufflePartitions = 4,
      threadPoolSize = 2,
    )

  private def groupedRow(body: => Long): Long =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(groupedConf(columnarExecution = false))
    body

  private def groupedFootprint(body: => Long): Footprint =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(groupedConf(columnarExecution = true))
    ColumnMetrics.withSink(new CountingSink) {
      Footprint(body, ColumnMetrics.batchCount, ColumnMetrics.visitCount)
    }

  private def assertNarrow[A](collection: DistCollection[A], width: Int): Unit =
    val info =
      ColumnarPlanner.partitioning(collection.plan).getOrElse(fail("expected a columnar plan"))
    info.distribution shouldBe Distribution.Unknown(width)
    info.ordering shouldBe OrderingTag.Unsorted
    info.layout shouldBe Layout.Columnar(ScarletConf.get.columnarBatchSize)

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def erasedPair(key: Option[Int], value: Option[Int]): (Int, Int) =
    val left: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val right: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    erasedCells(left, right)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def erasedCells(key: AnyRef, value: AnyRef): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  private def columnar[A](body: => A): A =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    body

  private def row[A](body: => A): A =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf.copy(columnarExecution = false))
    body
