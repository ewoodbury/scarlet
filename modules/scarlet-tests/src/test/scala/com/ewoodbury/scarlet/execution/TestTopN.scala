package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `sortByIntValueDesc.take` is a bounded heap when `n` is below the live row count, and the full
 * sort when `n` covers every live row. The prefix matches a stable full sort then take, including
 * n past the end, n = 1, n = 0, equal values broken by key ascending, a full tie kept in input
 * order, a null value, a null key, and the signed extremes.
 */
class TestTopN extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

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

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "sortByIntValueDesc" should "lower a scan or a filter stack and refuse a generic sortBy" in {
    val source = DistCollection.ofIntPairs(Array(4, 1, 2), Array(10, 30, 20), 2)
    val sorted = source.sortByIntValueDesc
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some("SortByIntValueDesc(Scan)")
    ColumnarPlanner.lower(source.filterKeys((key: Int) => key > 0).sortByIntValueDesc.plan)
      .map(_.shape) shouldBe Some("SortByIntValueDesc(FilterKey(Scan))")
    val generic = source.sortBy(pair => (pair._2, pair._1))(using
      Ordering.Tuple2(Ordering.Int.reverse, Ordering.Int),
    )
    ColumnarPlanner.lower(generic.plan).isEmpty shouldBe true
    generic.plan shouldBe a[Plan.SortByOp[_, _]]
  }

  it should "match sort-then-take on random pairs, including n past the end, one, and zero" in {
    val keys = Array(7, 1, 4, 9, 2, 8, 3, 5, 6, 0, 1, 4)
    val values = Array(10, 40, 10, 5, 40, 0, 7, 40, -3, 8, 11, 12)
    val source = DistCollection.ofIntPairs(keys, values, 3)
    val sorted = source.sortByIntValueDesc
    val full = rowCollect(sorted)
    Seq(0, 1, 2, 5, full.length, full.length + 4).foreach { count =>
      ColumnarPlanner.tryTake(sorted.plan, count) shouldBe Some(full.take(count))
      sorted.take(count) shouldBe full.take(count).toList
      sorted.take(count) shouldBe rowTake(sorted, count)
    }
    collect(sorted) shouldBe full
  }

  it should "break an equal value by key ascending" in {
    val source = DistCollection.ofIntPairs(
      Array(5, 1, 3, 1, 4, 2),
      Array(9, 9, 9, 8, 9, 9),
      2,
    )
    val sorted = source.sortByIntValueDesc
    val taken = sorted.take(4)
    taken shouldBe List(1 -> 9, 2 -> 9, 3 -> 9, 4 -> 9)
    taken shouldBe rowTake(sorted, 4)
    sorted.take(6) shouldBe List(1 -> 9, 2 -> 9, 3 -> 9, 4 -> 9, 5 -> 9, 1 -> 8)
  }

  it should "order a null value as zero and keep it null" in {
    val rows = Seq(
      intPair(boxedInt(4), boxedInt(3)),
      intPair(boxedInt(1), noInt),
      intPair(boxedInt(2), boxedInt(5)),
      intPair(boxedInt(3), boxedInt(0)),
      intPair(boxedInt(8), boxedInt(-1)),
    )
    val source = DistCollection(rows, 2)
    val sorted = source.sortByIntValueDesc
    val full = rowCollect(sorted)
    collect(sorted) shouldBe full
    Seq(0, 1, 3, full.length).foreach { count =>
      ColumnarPlanner.tryTake(sorted.plan, count) shouldBe Some(full.take(count))
      sorted.take(count) shouldBe full.take(count).toList
    }
    val prefix = ColumnarPlanner.tryTake(sorted.plan, 3).getOrElse(fail("missing prefix"))
    prefix.exists(pair => !present(pair, 1)) shouldBe true
  }

  it should "keep a null key, order it as zero, and report that key null" in {
    // Present (1, 9) is first so the scan classifies as int pairs. The null key's bit stays
    // clear. It orders as key 0, ahead of a present 0 in the later batch.
    val rows = Seq(
      intPair(boxedInt(1), boxedInt(9)),
      intPair(noInt, boxedInt(5)),
      intPair(boxedInt(-1), boxedInt(5)),
      intPair(boxedInt(0), boxedInt(5)),
      intPair(boxedInt(2), boxedInt(5)),
      intPair(noInt, boxedInt(0)),
    )
    val source = DistCollection(rows, 2)
    val sorted = source.sortByIntValueDesc
    val expected = Seq(
      (Some(1), Some(9)),
      (Some(-1), Some(5)),
      (None, Some(5)),
      (Some(0), Some(5)),
      (Some(2), Some(5)),
      (None, Some(0)),
    )
    val columnar = collect(sorted).map(shown)
    columnar shouldBe expected
    columnar shouldBe rowCollect(sorted).map(shown)
    columnar.count(pair => pair._1.isEmpty) shouldBe 2
    Seq(0, 1, 3, expected.length, expected.length + 2).foreach { count =>
      val taken = ColumnarPlanner.tryTake(sorted.plan, count).getOrElse(fail("missing prefix"))
      taken.map(shown) shouldBe expected.take(count)
      sorted.take(count).map(shown) shouldBe expected.take(count)
    }
    val held = ColumnarPlanner
      .tryTake(sorted.plan, 3)
      .getOrElse(fail("missing prefix"))
      .lift(2)
      .getOrElse(fail("missing null key"))
    present(held, 0) shouldBe false
    cell(held, 1) shouldBe Some(5)
  }

  it should "keep earlier batch then smaller row id when key and value tie across two batches" in {
    // Two partitions encode as two batches, three rows each. (1, 9) is row 0 and row 1 of the
    // first batch and row 0 of the second. The three copies are listed in that order.
    val keys = Array(1, 1, 4, 1, 1, 0)
    val values = Array(9, 9, 9, 9, 7, 9)
    val source = DistCollection.ofIntPairs(keys, values, 2)
    source.plan match
      case Plan.Source(partitions, _) =>
        partitions.length shouldBe 2
        partitions.map(_.data.size) shouldBe Seq(3, 3)
      case other => fail(s"expected a two-batch source, got $other")
    val sorted = source.sortByIntValueDesc
    val expected = List(0 -> 9, 1 -> 9, 1 -> 9, 1 -> 9, 4 -> 9, 1 -> 7)
    collect(sorted) shouldBe expected
    collect(sorted) shouldBe rowCollect(sorted)
    // limit 4 is the heap. limit == live and limit past the end are the full sort.
    sorted.take(4) shouldBe expected.take(4)
    sorted.take(expected.length) shouldBe expected
    sorted.take(expected.length + 2) shouldBe expected
    ColumnarPlanner.tryTake(sorted.plan, 4) shouldBe Some(expected.take(4))
    ColumnarPlanner.tryTake(sorted.plan, expected.length + 2) shouldBe Some(expected)
  }

  it should "order Int.MaxValue before Int.MinValue by value and the reverse by key" in {
    val keys = Array(Int.MinValue, Int.MaxValue, 0, -1, 1, 2)
    val values = Array(Int.MaxValue, Int.MaxValue, 1, 0, Int.MinValue, Int.MinValue)
    val source = DistCollection.ofIntPairs(keys, values, 2)
    val sorted = source.sortByIntValueDesc
    val expected = List(
      Int.MinValue -> Int.MaxValue,
      Int.MaxValue -> Int.MaxValue,
      0 -> 1,
      -1 -> 0,
      1 -> Int.MinValue,
      2 -> Int.MinValue,
    )
    collect(sorted) shouldBe expected
    // limit 2 is the heap. limit == live and limit past the end are the full sort.
    sorted.take(2) shouldBe expected.take(2)
    sorted.take(expected.length) shouldBe expected
    sorted.take(expected.length + 3) shouldBe expected
    ColumnarPlanner.tryTake(sorted.plan, 2) shouldBe Some(expected.take(2))
    ColumnarPlanner.tryTake(sorted.plan, expected.length + 3) shouldBe Some(expected)
  }

  it should "agree with a local stable sort of shuffled pairs" in {
    val keys = Array.tabulate(80)(index => (index * 17) % 11)
    val values = Array.tabulate(80)(index => (index * 13) % 19 - 5)
    val source = DistCollection.ofIntPairs(keys, values, 4)
    val sorted = source.sortByIntValueDesc
    val expected = localOrder(keys, values)
    collect(sorted) shouldBe expected
    sorted.take(100) shouldBe expected
    sorted.take(7) shouldBe expected.take(7)
    CrossEngineShapes.pairChecksum(sorted.take(7)) shouldBe
      CrossEngineShapes.pairChecksum(expected.take(7))
    val saved = ScarletConf.get
    ScarletConf.set(saved.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      sorted.take(7) shouldBe expected.take(7)
      sorted.take(7) shouldBe rowTake(sorted, 7)
      collect(sorted).take(7) shouldBe expected.take(7)
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
  }

  it should "heap a filtered owned scan in the same order as the row path" in {
    val keys = Array(5, 1, 3, 1, 4, 2, 8, 0, 6, 2)
    val values = Array(9, 9, 9, 8, 9, 9, -1, 4, 9, 4)
    val source = DistCollection.ofIntPairs(keys, values, 2)
    val sorted = source
      .filterKeys((key: Int) => key % 3 > 0)
      .filterValues((value: Int) => value >= 0)
      .sortByIntValueDesc
    val full = rowCollect(sorted)
    sorted.take(4) shouldBe full.take(4).toList
    sorted.take(4) shouldBe rowTake(sorted, 4)
    sorted.take(full.length) shouldBe full.toList
    sorted.take(full.length + 2) shouldBe full.toList
    collect(sorted) shouldBe full
    val saved = ScarletConf.get
    ScarletConf.set(saved.copy(columnarBatchSize = 1))
    try
      ScarletRuntime.get.shuffle.clear()
      sorted.take(4) shouldBe full.take(4).toList
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
  }

  private def localOrder(keys: Array[Int], values: Array[Int]): List[(Int, Int)] =
    keys.indices
      .map(index => (index, keys(index), values(index)))
      .sortBy { case (index, key, value) => (~value, key, index) }
      .map { case (_, key, value) => key -> value }
      .toList

  private def collect[A](collection: DistCollection[A]): Seq[A] =
    ScarletRuntime.get.shuffle.clear()
    collection.collect().toSeq

  private def rowCollect[A](collection: DistCollection[A]): Seq[A] =
    rowAction(collection.collect().toSeq)

  private def rowTake[A](collection: DistCollection[A], count: Int): List[A] =
    rowAction(collection.take(count))

  private def rowAction[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noInt: java.lang.Integer = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)

  private def present(pair: (Int, Int), ordinal: Int): Boolean =
    cell(pair, ordinal).isDefined

  private def cell(pair: (Int, Int), ordinal: Int): Option[Int] =
    pair.productElement(ordinal) match
      case value: java.lang.Integer => Some(value.intValue())
      case _ => None

  private def shown(pair: (Int, Int)): (Option[Int], Option[Int]) =
    (cell(pair, 0), cell(pair, 1))
