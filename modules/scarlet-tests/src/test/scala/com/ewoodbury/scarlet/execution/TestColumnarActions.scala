package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.ColumnMetrics
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

class TestColumnarActions extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
    ColumnMetrics.disarm()
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

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

  it should "stay correct when morsels split a partition" in {
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 2, threadPoolSize = 4),
    )
    val values = DistCollection(Seq(1, 2, 3, 4, 5), 1).filter(_ > 1)

    values.count() shouldBe 4L
    values.sum shouldBe 14L
  }

  it should "fall back from a mistyped map once" in {
    ScarletConf.set(originalConf.copy(columnarExecution = true, threadPoolSize = 1))
    val hits = new AtomicInteger
    def mapped = DistCollection(Seq(1, 2, 3), 1).map { value =>
      hits.incrementAndGet()
      value.toString
    }

    mapped.count() shouldBe 3L
    val afterCount = hits.getAndSet(0)
    mapped.collect() shouldBe Seq("1", "2", "3")
    hits.get() shouldBe afterCount
  }

  it should "fall back from a mistyped double map once" in {
    ScarletConf.set(originalConf.copy(columnarExecution = true, threadPoolSize = 1))
    val hits = new AtomicInteger
    def mapped = DistCollection(Seq(1.0, 2.0), 1).map { value =>
      hits.incrementAndGet()
      value.toInt
    }

    mapped.sum shouldBe 3L
    val afterSum = hits.getAndSet(0)
    mapped.collect() shouldBe Seq(1, 2)
    hits.get() shouldBe afterSum
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
    columnar(assertNarrow(mapped, 2))
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
    columnar(assertNarrow(planned, 1))
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

  "primitive sources" should "sum an int array like a boxed vector" in {
    val values = Array(1, 2, 3, 4, 5)

    def primitive = DistCollection.ofInts(values, 3).filter(_ > 0).map(_ * 2).sum
    def boxed = DistCollection(values.toVector, 3).filter(_ > 0).map(_ * 2).sum

    columnar(primitive) shouldBe 30L
    columnar(primitive) shouldBe columnar(boxed)
    row(primitive) shouldBe row(boxed)
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
  }

  "int pair projections" should "sum values from the shared value column" in {
    val keys = Array(1, -4, 3, Int.MaxValue)
    val values = Array(Int.MaxValue, 5, -2, Int.MaxValue)

    def rows = DistCollection.ofIntPairs(keys, values, 1)
    def summed = rows.values.sum

    val expected = row(summed)
    armColumnar()
    ColumnMetrics.arm()
    summed shouldBe expected
    assertColumnar(rows.values, width = 1)
    assertOnePass()
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
    ColumnMetrics.arm()
    summed shouldBe expected
    assertColumnar(rows.keys, width = 1)
    assertOnePass()
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
    ColumnMetrics.arm()
    summed shouldBe expected
    calls.get() shouldBe values.length
    assertColumnar(rows, width = 1)
    assertOnePass()
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

  it should "leave a mapValues that changes type on the row path" in {
    val keys = Array(1, 2, 3)
    val values = Array(4, 5, 6)

    def strings = DistCollection.ofIntPairs(keys, values, 2).mapValues[Int, Int, String](_.toString)
    def widened = DistCollection.ofIntPairs(keys, values, 2).mapValues[Int, Int, Long](_.toLong)

    armColumnar()
    ColumnarPlanner.partitioning(strings.plan) shouldBe None
    ColumnarPlanner.partitioning(widened.plan) shouldBe None
    ColumnMetrics.arm()
    val collected = strings.collect()
    ColumnMetrics.batchCount shouldBe 0L
    ColumnMetrics.visitCount shouldBe 0L
    ColumnMetrics.disarm()
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

  it should "return zero for an empty pair projection through the row engine" in {
    def empty = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 1)

    columnar(empty.values.sum) shouldBe 0L
    columnar(empty.keys.sum) shouldBe 0L
    columnar(ColumnarPlanner.partitioning(empty.values.plan) shouldBe None)
  }

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
    val batches = ColumnMetrics.batchCount
    val visits = ColumnMetrics.visitCount
    ColumnMetrics.disarm()
    batches shouldBe 1L
    visits shouldBe 1L

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
    ColumnMetrics.arm()
    val result = body
    val batches = ColumnMetrics.batchCount
    val visits = ColumnMetrics.visitCount
    ColumnMetrics.disarm()
    Footprint(result, batches, visits)

  private def assertNarrow[A](collection: DistCollection[A], width: Int): Unit =
    val info =
      ColumnarPlanner.partitioning(collection.plan).getOrElse(fail("expected a columnar plan"))
    info.distribution shouldBe Distribution.Unknown(width)
    info.ordering shouldBe OrderingTag.Unsorted
    info.layout shouldBe Layout.Columnar(ScarletConf.get.columnarBatchSize)

  private def columnar[A](body: => A): A =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    body

  private def row[A](body: => A): A =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf.copy(columnarExecution = false))
    body
