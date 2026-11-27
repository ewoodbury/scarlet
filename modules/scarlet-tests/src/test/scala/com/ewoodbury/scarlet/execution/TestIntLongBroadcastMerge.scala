package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Broadcast and sort-merge joins of `mapValues` from `(Int, Int)` to `(Int, Long)`.
 *
 * A null key does not match. The preserved side keeps it, and the other value is absent. A null
 * long stays null and is not a missing partner. The row path ignores the strategy hint, so a
 * present-value collect is the oracle. An int-pair join stays the int class.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Any",
  ),
)
class TestIntLongBroadcastMerge extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private enum LongCell:
    case Missing, NullLong
    case Value(n: Long)

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(original.copy(columnarExecution = true, threadPoolSize = 1))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "broadcast and sort-merge" should "match the row join for present longs" in {
    val left = widened(Array(1, 2, 2), Array(10, 20, 30))
    val right = widened(Array(2, 4), Array(5, 6))
    val filtered = left.filterValues((value: Long) => value > 15L)

    checkPresent(Plan.JoinStrategy.Broadcast, left, right, filtered, broadcastShapes)
    checkPresent(Plan.JoinStrategy.SortMerge, left, right, filtered, sortMergeShapes)
  }

  it should "keep a null key on the preserved side" in {
    val left = pairs(
      Seq(cell(intRef(1), intRef(10)), cell(absentInt, intRef(4))),
    ).mapValues((value: Int) => value.toLong)
    val right = pairs(
      Seq(cell(intRef(1), intRef(5)), cell(absentInt, intRef(9))),
    ).mapValues((value: Int) => value.toLong)

    List(Plan.JoinStrategy.Broadcast, Plan.JoinStrategy.SortMerge).foreach { strategy =>
      same(inner(left, right, strategy))
      same(leftOuter(left, right, strategy))
      same(rightOuter(left, right, strategy))
      same(fullOuter(left, right, strategy))
    }
  }

  it should "keep a null long and still tell a missing partner apart" in {
    val left = pairs(
      Seq(cell(intRef(2), intRef(8)), cell(intRef(1), absentInt)),
    ).mapValues((value: Int) => value.toLong)
    val right = pairs(
      Seq(cell(intRef(1), intRef(5)), cell(intRef(3), intRef(7))),
    ).mapValues((value: Int) => value.toLong)
    val matched = Seq((Some(1), LongCell.NullLong, LongCell.Value(5L)))
    val leftOnly = (Some(2), LongCell.Value(8L), LongCell.Missing)
    val rightOnly = (Some(3), LongCell.Missing, LongCell.Value(7L))

    // Row mapValues unboxes a null int to zero, so that collect is not the oracle.
    List(Plan.JoinStrategy.Broadcast, Plan.JoinStrategy.SortMerge).foreach { strategy =>
      seen(columnar(inner(left, right, strategy))) shouldBe matched
      seen(columnar(leftOuter(left, right, strategy))) shouldBe matched :+ leftOnly
      seen(columnar(rightOuter(left, right, strategy))) shouldBe matched :+ rightOnly
      seen(columnar(fullOuter(left, right, strategy))) shouldBe matched ++ Seq(leftOnly, rightOnly)
    }
  }

  private val broadcastShapes = Shapes(
    "IntLongBroadcastJoin(IntLongMapValue(Scan), IntLongMapValue(Scan))",
    "IntLongBroadcastLeftOuter(IntLongMapValue(Scan), IntLongMapValue(Scan))",
    "IntLongBroadcastRightOuter(IntLongMapValue(Scan), IntLongMapValue(Scan))",
    "IntLongBroadcastFullOuter(IntLongMapValue(Scan), IntLongMapValue(Scan))",
    "IntLongBroadcastLeftOuter(FilterValue(IntLongMapValue(Scan)), IntLongMapValue(Scan))",
  )

  private val sortMergeShapes = Shapes(
    "IntLongSortMergeJoin(Exchange(IntLongMapValue(Scan)), Exchange(IntLongMapValue(Scan)))",
    "IntLongSortMergeLeftOuter(Exchange(IntLongMapValue(Scan)), Exchange(IntLongMapValue(Scan)))",
    "IntLongSortMergeRightOuter(Exchange(IntLongMapValue(Scan)), Exchange(IntLongMapValue(Scan)))",
    "IntLongSortMergeFullOuter(Exchange(IntLongMapValue(Scan)), Exchange(IntLongMapValue(Scan)))",
    "IntLongSortMergeLeftOuter(Exchange(FilterValue(IntLongMapValue(Scan))), " +
      "Exchange(IntLongMapValue(Scan)))",
  )

  private def checkPresent(
      strategy: Plan.JoinStrategy,
      left: DistCollection[(Int, Long)],
      right: DistCollection[(Int, Long)],
      filtered: DistCollection[(Int, Long)],
      shapes: Shapes,
  ): Unit =
    val joined = inner(left, right, strategy)
    val leftJoin = leftOuter(left, right, strategy)
    val rightJoin = rightOuter(left, right, strategy)
    val fullJoin = fullOuter(left, right, strategy)
    val filteredJoin = leftOuter(filtered, right, strategy)

    shape(joined) shouldBe shapes.inner
    shape(leftJoin) shouldBe shapes.left
    shape(rightJoin) shouldBe shapes.right
    shape(fullJoin) shouldBe shapes.full
    shape(filteredJoin) shouldBe shapes.filtered
    same(joined)
    same(leftJoin)
    same(rightJoin)
    same(fullJoin)
    same(filteredJoin)

  private def inner(
      left: DistCollection[(Int, Long)],
      right: DistCollection[(Int, Long)],
      strategy: Plan.JoinStrategy,
  ): Plan[_] = strategy match
    case Plan.JoinStrategy.Broadcast => left.broadcastJoin(right).plan
    case Plan.JoinStrategy.SortMerge => left.sortMergeJoin(right).plan
    case Plan.JoinStrategy.ShuffleHash => left.shuffleHashJoin(right).plan

  private def leftOuter(
      left: DistCollection[(Int, Long)],
      right: DistCollection[(Int, Long)],
      strategy: Plan.JoinStrategy,
  ): Plan[_] =
    Plan.LeftOuterJoinOp(left.plan, right.plan, Some(strategy))

  private def rightOuter(
      left: DistCollection[(Int, Long)],
      right: DistCollection[(Int, Long)],
      strategy: Plan.JoinStrategy,
  ): Plan[_] =
    Plan.RightOuterJoinOp(left.plan, right.plan, Some(strategy))

  private def fullOuter(
      left: DistCollection[(Int, Long)],
      right: DistCollection[(Int, Long)],
      strategy: Plan.JoinStrategy,
  ): Plan[_] =
    Plan.FullOuterJoinOp(left.plan, right.plan, Some(strategy))

  private def same(plan: Plan[_]): Unit =
    // These plans do not share one result type. Collect only needs the plan tree.
    val columnarRows = seen(columnar(plan))
    val rowRows = seen(row(DistCollection(plan.asInstanceOf[Plan[Any]]).collect().toList))
    columnarRows shouldBe rowRows

  private def seen(rows: Seq[Any]): Seq[(Option[Int], LongCell, LongCell)] =
    rows
      .map { row =>
        val product = row.asInstanceOf[Product]
        val values = product.productElement(1).asInstanceOf[Product]
        (
          optInt(product.productElement(0)),
          longCell(values.productElement(0)),
          longCell(values.productElement(1)),
        )
      }
      .sortBy { cell =>
        (cell._1.getOrElse(Int.MinValue), rank(cell._2), rank(cell._3))
      }

  private def widened(keys: Array[Int], values: Array[Int]): DistCollection[(Int, Long)] =
    DistCollection.ofIntPairs(keys, values, 2).mapValues((value: Int) => value.toLong)

  private def pairs(rows: Seq[(Int, Int)]): DistCollection[(Int, Int)] =
    DistCollection(rows, 1)

  private def shape(plan: Plan[_]): String =
    ColumnarPlanner.lower(plan).map(_.shape).getOrElse("none")

  private def columnar(plan: Plan[_]): Seq[Any] =
    ColumnarPlanner
      .tryRun(plan)
      .getOrElse(fail(s"stayed on the row path: $plan"))
      .flatMap(_.data)
      .toList

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def cell(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  private def absentInt: java.lang.Integer = null

  private def intRef(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)

  private def optInt(value: Any): Option[Int] = value match
    case null => None
    case found: Int => Some(found)
    case other => fail(s"unexpected key $other")

  private def longCell(value: Any): LongCell = value match
    case None => LongCell.Missing
    case Some(inner) => boxed(inner)
    case other => boxed(other)

  private def boxed(value: Any): LongCell = value match
    case null => LongCell.NullLong
    case found: Long => LongCell.Value(found)
    case other => fail(s"unexpected long $other")

  private def rank(cell: LongCell): (Int, Long) = cell match
    case LongCell.Missing => (0, 0L)
    case LongCell.NullLong => (1, 0L)
    case LongCell.Value(n) => (2, n)

  private final case class Shapes(
      inner: String,
      left: String,
      right: String,
      full: String,
      filtered: String,
  )
