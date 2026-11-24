package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, HashJoin}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.execution.schema.Describe
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * A join with one `flatMap` side decodes the other side and joins on rows.
 *
 * The join does not lower. [[Describe.describe]] has no tag for it, and the tag is not the route.
 * A whole-plan row rerun matches the rows and publishes nothing. A hash join of the columnar side
 * to the row side moves [[HashJoin.hashJoinCallsForTest]]. The columnar side has to publish a
 * batch, and that counter has to stay zero. `take` is not this shape.
 */
class TestMixedJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val saved = ScarletConf.get

  override def beforeEach(): Unit =
    ScarletConf.set(saved.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()
    HashJoin.resetHashJoinCallsForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(saved)
    HashJoin.resetHashJoinCallsForTest()
    ()

  "a join with a flatMap on the right" should "decode the left side and stay off the hash join" in {
    val columnar = left
    val unbound = widen(right)
    assertMixed(columnar, unbound, columnar.join(unbound))
  }

  "a join with a flatMap on the left" should "decode the right side and stay off the hash join" in {
    val columnar = right
    val unbound = widen(left)
    assertMixed(columnar, unbound, unbound.join(columnar))
  }

  "a join of a filtered scan and a flatMap" should "decode the filter and stay off the hash join" in {
    val columnar = left.filterKeys((key: Int) => key > 1)
    val unbound = widen(right)
    assertMixed(columnar, unbound, columnar.join(unbound))
  }

  "a left outer join with one flatMap side" should "decode the scan and stay off the hash join" in {
    val columnar = left
    val unbound = widen(right)
    assertMixed(columnar, unbound, columnar.leftOuterJoin(unbound))
  }

  "a right outer join with one flatMap side" should "decode the scan and stay off the hash join" in {
    val columnar = left
    val unbound = widen(right)
    assertMixed(columnar, unbound, columnar.rightOuterJoin(unbound))
  }

  "a full outer join with one flatMap side" should "decode the scan and stay off the hash join" in {
    val columnar = left
    val unbound = widen(right)
    assertMixed(columnar, unbound, columnar.fullOuterJoin(unbound))
  }

  "a join of two scans" should "still run the hash-join kernel" in {
    val joined = left.join(right)
    ColumnarPlanner.lower(joined.plan).isDefined shouldBe true
    Describe.describe(joined.plan).map(_.choice.tag) shouldBe Some("HashJoin")

    val found = ColumnMetrics.withSink(new CountingSink) {
      ScarletRuntime.get.shuffle.clear()
      joined.collect()
    }
    HashJoin.hashJoinCallsForTest should be > 0
    found should contain theSameElementsAs row(joined.collect())
  }

  private def assertMixed[A](
      columnar: DistCollection[(Int, Int)],
      unbound: DistCollection[_],
      joined: DistCollection[A],
  ): Unit =
    Describe.describe(joined.plan) shouldBe Option.empty[Describe.Described]
    ColumnarPlanner.lower(joined.plan) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.lower(columnar.plan).isDefined shouldBe true
    ColumnarPlanner.lower(unbound.plan) shouldBe Option.empty[PhysicalOp]

    val child = new CountingSink
    val childRan = ColumnMetrics.withSink(child) {
      ScarletRuntime.get.shuffle.clear()
      ColumnarPlanner.tryRun(columnar.plan).isDefined
    }
    childRan shouldBe true
    child.batchCount should be > 0L

    HashJoin.resetHashJoinCallsForTest()
    val sink = new CountingSink
    val found = ColumnMetrics.withSink(sink) {
      ScarletRuntime.get.shuffle.clear()
      joined.collect()
    }
    HashJoin.hashJoinCallsForTest shouldBe 0
    sink.batchCount shouldBe child.batchCount
    sink.batchCount should be > 0L
    found should contain theSameElementsAs row(joined.collect())

  /** One added to each value, so dropping the function changes the join. */
  private def widen(rows: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    rows.flatMap { (pair: (Int, Int)) =>
      val (key, value) = pair
      Seq((key, value + 1))
    }

  private def left: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 1, 2, 3), Array(10, 11, 20, 30), 2)

  private def right: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(Array(1, 1, 2, 4), Array(7, 8, 9, 10), 2)

  private def row[A](body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(current.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(current)
      ScarletRuntime.get.shuffle.clear()
