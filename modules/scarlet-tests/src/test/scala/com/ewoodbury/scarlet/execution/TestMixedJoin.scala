package com.ewoodbury.scarlet.execution
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, HashJoin}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.support.Check.shouldBeEmpty

/**
 * One lowered join side and one `flatMap` side.
 *
 * The lowered side publishes a batch and [[HashJoin]] does not run. A whole-plan row rerun matches
 * the rows and publishes nothing. A null key or null value on the lowered side has to collect the
 * same rows as the row join: [[ColumnarPlanner.tryRun]] throws on a null `Int32` pair, and a
 * lowered `filterKeys` drops a null the row predicate would unbox. [[MixedJoin]] returns `None`
 * for that side. Outer joins keep unmatched keys that sit on the `flatMap` side, including a
 * `flatMap` that emits nothing and one that emits two rows.
 */
class TestMixedJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()
    HashJoin.resetKernelCallsForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    HashJoin.resetKernelCallsForTest()
    ()

  "two lowered sides" should "run the hash-join kernel" in {
    val joined = pairs.join(others)
    ColumnarPlanner.lower(joined.plan).isDefined shouldBe true
    HashJoin.resetKernelCallsForTest()
    joined.collect()
    HashJoin.kernelCallsForTest should be > 0
  }

  "an inner join with flatMap on the right" should "publish the left batches and match rows" in {
    val joined = pairs.join(others.flatMap(keep))
    ColumnarPlanner.lower(joined.plan) shouldBe Option.empty[PhysicalOp]
    agree(joined, published = true)
  }

  "an inner join with flatMap on the left" should "publish the right batches and match rows" in {
    val joined = pairs.flatMap(keep).join(others)
    ColumnarPlanner.lower(joined.plan) shouldBe Option.empty[PhysicalOp]
    agree(joined, published = true)
  }

  "a filtered present side" should "publish its batches and match the row join" in {
    val joined = pairs.filterKeys((key: Int) => key > 0).join(others.flatMap(twice))
    ColumnarPlanner.lower(joined.plan) shouldBe Option.empty[PhysicalOp]
    agree(joined, published = true)
  }

  "outer joins with flatMap on the left" should "keep unmatched keys from that side" in {
    val left = pairs.flatMap(twice)
    val right = others
    agree(left.leftOuterJoin(right), published = true)
    agree(left.rightOuterJoin(right), published = true)
    agree(left.fullOuterJoin(right), published = true)
  }

  "a right outer join with flatMap on the right" should "keep unmatched keys from that side" in {
    agree(pairs.rightOuterJoin(others.flatMap(twice)), published = true)
  }

  "a flatMap that emits nothing" should "leave the columnar side on the row outer join" in {
    val left = pairs.flatMap(none)
    val right = others
    agree(left.join(right), published = true)
    agree(left.leftOuterJoin(right), published = true)
    agree(left.rightOuterJoin(right), published = true)
    agree(left.fullOuterJoin(right), published = true)
  }

  "a flatMap that emits two rows" should "match the row join" in {
    val left = pairs.flatMap(twice)
    val right = others
    agree(left.join(right), published = true)
    agree(left.leftOuterJoin(right), published = true)
    agree(left.rightOuterJoin(right), published = true)
    agree(left.fullOuterJoin(right), published = true)
  }

  "a null key on the columnar side" should "fall back and match the row join" in {
    val thrown = the[IllegalArgumentException] thrownBy ColumnarPlanner.tryRun(nullKey.plan)
    thrown.getMessage should include("null Int32 pair")
    val partner = others.flatMap(keep)
    val inner = nullKey.join(partner)
    MixedJoin.partitions(inner.plan).shouldBeEmpty
    val innerRows = agree(inner, published = false)
    innerRows.exists(_.contains("null")) shouldBe false

    val full = partner.fullOuterJoin(nullKey)
    MixedJoin.partitions(full.plan).shouldBeEmpty
    val fullRows = agree(full, published = false)
    fullRows.exists(_.contains("null")) shouldBe true
  }

  "a null value on the columnar side" should "fall back and match the row join" in {
    val thrown = the[IllegalArgumentException] thrownBy ColumnarPlanner.tryRun(nullValue.plan)
    thrown.getMessage should include("null Int32 pair")
    val joined = DistCollection.ofIntPairs(Array(2), Array(200), 1).flatMap(keep).join(nullValue)
    MixedJoin.partitions(joined.plan).shouldBeEmpty
    val rows = agree(joined, published = false)
    rows.exists(_.contains("null")) shouldBe true
  }

  "filterKeys of a null key" should "keep the key the row predicate unboxes" in {
    val filtered = nullKey.filterKeys((key: Int) => key >= 0)
    val joined = others.flatMap(keep).rightOuterJoin(filtered)
    MixedJoin.partitions(joined.plan).shouldBeEmpty
    val rows = agree(joined, published = false)
    rows.exists(_.contains("null")) shouldBe true
  }

  "filterKeys of a null value" should "keep the value the row join keeps" in {
    val filtered = nullValue.filterKeys((key: Int) => key >= 0)
    val joined = DistCollection.ofIntPairs(Array(2), Array(200), 1).flatMap(keep).join(filtered)
    MixedJoin.partitions(joined.plan).shouldBeEmpty
    val rows = agree(joined, published = false)
    rows.exists(_.contains("null")) shouldBe true
  }

  "two flatMap sides" should "stay on the row executor" in {
    val joined = pairs.flatMap(keep).join(others.flatMap(twice))
    MixedJoin.partitions(joined.plan).shouldBeEmpty
    agree(joined, published = false)
  }

  private def agree[A](joined: DistCollection[A], published: Boolean): Seq[String] =
    if published then
      ScarletRuntime.get.shuffle.clear()
      MixedJoin.partitions(joined.plan).isDefined shouldBe true
    HashJoin.resetKernelCallsForTest()
    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      show(joined.collect())
    }
    val expected = row(show(joined.collect()))
    HashJoin.kernelCallsForTest shouldBe 0
    if published then sink.batchCount should be > 0L
    found.sorted shouldBe expected.sorted
    found

  private def pairs: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 2, 1), Array(10, 20, 30), 2)

  private def others: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 3), Array(100, 300), 2)

  private def keep(row: (Int, Int)): Seq[(Int, Int)] = Seq(row)

  private def none(row: (Int, Int)): Seq[(Int, Int)] =
    val _ = row
    Seq.empty[(Int, Int)]

  private def twice(row: (Int, Int)): Seq[(Int, Int)] =
    Seq(row, (row._1, row._2 + 1))

  /** A present pair first, so the scan lowers, then a null key. */
  private def nullKey: DistCollection[(Int, Int)] =
    DistCollection(Seq((1, 10), erased(None, Some(20)), (2, 30)), 1)

  /** A present pair first, so the scan lowers, then a null value on key 2. */
  private def nullValue: DistCollection[(Int, Int)] =
    DistCollection(Seq((1, 10), erased(Some(2), None), (3, 30)), 1)

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def show(rows: Iterable[Any]): Seq[String] = rows.toSeq.map(showCell)

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Product",
    ),
  )
  private def showCell(cell: Any): String = cell match
    case null => "null"
    case None => "None"
    case Some(value) => "Some(" + showCell(value) + ")"
    case product: Product =>
      val cells = (0 until product.productArity).map { index =>
        showCell(product.productElement(index))
      }
      cells.mkString("[", ",", "]")
    case other => String.valueOf(other)

  /** Named erasure boundary. `Tuple2` keeps a null `Integer`; `(Int, Int)` would unbox it. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erased(key: Option[Int], value: Option[Int]): (Int, Int) =
    val left: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val right: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(left, right).asInstanceOf[(Int, Int)]
