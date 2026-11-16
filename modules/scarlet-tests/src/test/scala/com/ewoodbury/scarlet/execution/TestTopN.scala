package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Bounded heap for `sortByIntValueDesc.take`. The prefix matches a stable full sort then take,
 * including n past the end, n = 1, n = 0, equal values broken by `sk`, and a null value.
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

  it should "break equal values on sk ascending and keep input order on a full tie" in {
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
    pair.productElement(ordinal) match
      case _: java.lang.Integer => true
      case _ => false
