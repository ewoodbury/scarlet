package com.ewoodbury.scarlet.execution
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.support.Check

/**
 * Unhinted inner joins broadcast when the build side's stored row count is at or under the
 * threshold, and hash otherwise.
 *
 * A named hint stays. An outer join stays on its hash node. A count by key does not consult the
 * rule. A wide join uses the same threshold.
 */
class TestJoinCost extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

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
        spillEnabled = false,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "an int join" should "broadcast a small build and hash a build over the threshold" in {
    val left = DistCollection.ofIntPairs(Array(1, 2, 1), Array(10, 20, 11), 2)
    val right = DistCollection.ofIntPairs(Array(1, 3, 2), Array(100, 300, 200), 2)
    val plain = left.join(right)
    val hashed = left.shuffleHashJoin(right)

    shape(plain.plan) shouldBe "BroadcastJoin(Scan, Scan)"
    shape(hashed.plan) shouldBe "HashJoin(Exchange(Scan), Exchange(Scan))"
    shape(left.sortMergeJoin(right).plan) shouldBe "SortMergeJoin(Exchange(Scan), Exchange(Scan))"
    shape(left.broadcastJoin(right).plan) shouldBe "BroadcastJoin(Scan, Scan)"
    plain.collect() should contain theSameElementsAs hashed.collect()

    withThreshold(2L) {
      shape(left.join(right).plan) shouldBe "HashJoin(Exchange(Scan), Exchange(Scan))"
      shape(left.broadcastJoin(right).plan) shouldBe "BroadcastJoin(Scan, Scan)"
    }
  }

  it should "leave a count and an outer join on the kernels they already use" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 2)
    val right = DistCollection.ofIntPairs(Array(1, 3), Array(100, 30), 2)
    shape(left.countByKey.plan) shouldBe "CountByKey(Exchange(Scan))"
    shape(left.leftOuterJoin(right).plan) shouldBe "HashLeftOuter(Exchange(Scan), Exchange(Scan))"
    val filtered = right.filter((pair: (Int, Int)) => pair._2 > 0)
    val filteredShape = ColumnarPlanner.lower(left.join(filtered).plan).map(_.shape)
    filteredShape.forall(_.startsWith("HashJoin(")) shouldBe true
  }

  "a wide join" should "use the same threshold" in {
    val left = wide(Array(1, 2), Array(10, 20))
    val right = wide(Array(1, 2, 3), Array(100, 200, 300))
    shape(left.joinColumns(right, 0, 0).plan) shouldBe "WideBroadcastJoin(Scan,Scan)"
    shape(left.hashJoinColumns(right, 0, 0).plan) shouldBe "WideHashJoin(Scan,Scan)"
    shape(left.broadcastJoinColumns(right, 0, 0).plan) shouldBe "WideBroadcastJoin(Scan,Scan)"
    rows(left.joinColumns(right, 0, 0)) shouldBe rows(left.hashJoinColumns(right, 0, 0))

    withThreshold(2L) {
      shape(left.joinColumns(right, 0, 0).plan) shouldBe "WideHashJoin(Scan,Scan)"
      shape(left.broadcastJoinColumns(right, 0, 0).plan) shouldBe "WideBroadcastJoin(Scan,Scan)"
    }
  }

  private def wide(keys: Array[Int], values: Array[Int]): DistCollection[WideRow] =
    DistCollection.ofColumns(
      Vector(WideInput.ints("k", keys), WideInput.ints("v", values)),
      2,
    )

  private def shape(plan: Plan[_]): String =
    ColumnarPlanner.lower(plan).map(_.shape).getOrElse(fail(s"did not lower $plan"))

  private def rows(joined: DistCollection[WideRow]): List[String] =
    joined.collect().map(Check.text(_)).toList.sorted

  private def withThreshold(threshold: Long)(body: => Unit): Unit =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(broadcastJoinThreshold = threshold))
      body
    finally ScarletConf.set(saved)
