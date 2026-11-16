package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Bounded heap for `sortBy` of int pairs on value descending, then key ascending.
 *
 * The heap and a stable full sort agree on ties, including a null ordered as zero. A null stays
 * null. `collect` stays on the row path. `take` is the heap.
 */
class TestTopN extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "top-n" should "keep every row when n is larger than the input" in {
    val rows = Seq(4 -> 1, 1 -> 9, 3 -> 3, 2 -> 9)
    val sorted = CrossEngineShapes.sorted(DistCollection(rows, 2))
    sorted.take(10) shouldBe stable(rows, 10)
    sorted.take(10) shouldBe sorted.collect()
  }

  it should "keep the single best row" in {
    val rows = Seq(4 -> 1, 1 -> 9, 3 -> 3, 2 -> 8)
    val sorted = CrossEngineShapes.sorted(DistCollection(rows, 2))
    sorted.take(1) shouldBe List(1 -> 9)
    sorted.take(1) shouldBe sorted.collect().take(1)
  }

  it should "return empty for n = 0 without scanning" in {
    val seen = Array(0)
    val data = DistCollection(Seq(1 -> 2, 3 -> 4), 1).filterValues((_: Int) => {
      seen(0) += 1
      true
    })
    val sorted = CrossEngineShapes.sorted(data)
    sorted.take(0) shouldBe Seq.empty[(Int, Int)]
    seen(0) shouldBe 0
    val op = ColumnarPlanner.lower(sorted.plan).getOrElse(fail("expected a top-n plan"))
    SortArms.take(sorted.plan, op, 0).isEmpty shouldBe true
    seen(0) shouldBe 0
  }

  it should "break equal values by key ascending" in {
    val rows = Seq(4 -> 7, 1 -> 7, 3 -> 7, 2 -> 7)
    val sorted = CrossEngineShapes.sorted(DistCollection(rows, 2))
    sorted.take(3) shouldBe List(1 -> 7, 2 -> 7, 3 -> 7)
    sorted.take(3) shouldBe sorted.collect().take(3)
  }

  it should "keep input order when the value and the key both tie" in {
    val rows = Seq(1 -> 5, 2 -> 5, 1 -> 5, 0 -> 5)
    val sorted = CrossEngineShapes.sorted(DistCollection(rows, 2))
    val expected = List(0 -> 5, 1 -> 5, 1 -> 5, 2 -> 5)
    sorted.take(4) shouldBe expected
    sorted.take(4) shouldBe sorted.collect().take(4)
  }

  it should "order a null as zero and keep the null" in {
    val rows = Seq(
      intPair(boxed(1), boxed(5)),
      intPair(noInt, boxed(0)),
      intPair(boxed(0), boxed(0)),
      intPair(boxed(2), noInt),
      intPair(boxed(3), boxed(5)),
    )
    val sorted = CrossEngineShapes.sorted(DistCollection(rows, 2))
    val expected = Seq(
      intPair(boxed(1), boxed(5)),
      intPair(boxed(3), boxed(5)),
      intPair(noInt, boxed(0)),
      intPair(boxed(0), boxed(0)),
      intPair(boxed(2), noInt),
    )
    val taken = sorted.take(10)
    taken shouldBe expected
    val indexed = taken.toArray
    present(indexed(2), 0) shouldBe false
    present(indexed(4), 1) shouldBe false
    ColumnarPlanner.tryRun(sorted.plan) shouldBe None
  }

  it should "match a stable sort-then-take on random input" in {
    val random = new scala.util.Random(58)
    val rows = (0 until 200).map(index => (index, random.nextInt(30) - 5))
    val sorted = CrossEngineShapes.sorted(DistCollection(rows, 3))
    val expected = stable(rows, 17)
    sorted.take(17) shouldBe expected
    sorted.take(17) shouldBe sorted.collect().take(17)
    sorted.take(200) shouldBe sorted.collect()
  }

  it should "lower a filtered value sort and leave other sorts alone" in {
    val data = DistCollection
      .ofIntPairs(Array(4, 7, 8, 3, 5, 6, 1, 2), Array(10, 1, -1, 8, 9, 7, 100, 99), 2)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
    val sorted = CrossEngineShapes.sorted(data)
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some(
      "TopN(FilterValue(FilterKey(Scan)))",
    )
    ColumnarPlanner.tryTake(sorted.plan, 3).isDefined shouldBe true
    ColumnarPlanner.tryRun(sorted.plan) shouldBe None
    sorted.take(3) shouldBe List(1 -> 100, 2 -> 99, 4 -> 10)
    val byKey = data.sortBy(pair => pair._1)
    ColumnarPlanner.lower(byKey.plan) shouldBe None
    val ascending = data.sortBy(pair => (pair._2, pair._1))(using Ordering[(Int, Int)])
    ColumnarPlanner.lower(ascending.plan) shouldBe None
  }

  private def stable(rows: Seq[(Int, Int)], n: Int): Seq[(Int, Int)] =
    rows
      .sortBy { case (key, value) => (value, key) }(
        using Ordering.Tuple2(Ordering.Int.reverse, Ordering.Int),
      )
      .take(n)

  private def present(pair: (Int, Int), ordinal: Int): Boolean =
    pair.productElement(ordinal) match
      case _: java.lang.Integer => true
      case _ => false

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noInt: java.lang.Integer = null

  private def boxed(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)
