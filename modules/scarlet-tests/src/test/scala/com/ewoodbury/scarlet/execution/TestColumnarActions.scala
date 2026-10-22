package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

class TestColumnarActions extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def afterEach(): Unit =
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

  it should "return zero for an empty source" in {
    columnar(DistCollection(Seq.empty[(Int, Int)], 2).groupedSum) shouldBe 0L
    row(DistCollection(Seq.empty[(Int, Int)], 2).groupedSum) shouldBe 0L
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
