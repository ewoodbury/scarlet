package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import com.ewoodbury.scarlet.api.DistCollection

/** Sort-merge, broadcast, and hash inner joins on the same pairs. */
class TestColumnarJoinStrategies
    extends AnyFlatSpec
    with BeforeAndAfterEach
    with ColumnarContractFixture:

  override def beforeEach(): Unit =
    super.beforeEach()
    installContractConf()

  override def afterEach(): Unit =
    restoreContractConf()
    ()

  "the columnar contract" should "run sort-merge on the columnar path and broadcast beside it" in {
    val left = DistCollection(Seq(1 -> 10, 1 -> 11, 2 -> 20), 2)
    val right = DistCollection(Seq(1 -> 100, 1 -> 101, 3 -> 30), 2)
    val sortMerge = left.sortMergeJoin(right).plan
    val broadcast = left.broadcastJoin(right).plan
    val plain = left.join(right).plan
    val hashed = left.shuffleHashJoin(right).plan
    val expected = Seq(
      1 -> ((10, 100)),
      1 -> ((10, 101)),
      1 -> ((11, 100)),
      1 -> ((11, 101)),
    )

    ColumnarPlanner.partitioning(sortMerge).map(_.layout) shouldBe Some(columnarLayout)
    ColumnarPlanner.lower(sortMerge).map(_.shape) shouldBe Some(
      "SortMergeJoin(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.tryRun(sortMerge) shouldBe defined
    ColumnarPlanner.tryTake(sortMerge, 1) shouldBe None
    collected(sortMerge) should contain theSameElementsAs rowResult(sortMerge)
    rowResult(sortMerge) should contain theSameElementsAs expected

    ColumnarPlanner.partitioning(broadcast).map(_.layout) shouldBe Some(columnarLayout)
    ColumnarPlanner.lower(broadcast).map(_.shape) shouldBe Some("BroadcastJoin(Scan, Scan)")
    ColumnarPlanner.tryRun(broadcast) shouldBe defined
    collected(broadcast) should contain theSameElementsAs rowResult(broadcast)
    rowResult(broadcast) should contain theSameElementsAs expected

    ColumnarPlanner.partitioning(plain).map(_.layout) shouldBe Some(columnarLayout)
    ColumnarPlanner.lower(plain).map(_.shape) shouldBe Some("BroadcastJoin(Scan, Scan)")
    ColumnarPlanner.tryRun(plain) shouldBe defined
    collected(plain) should contain theSameElementsAs rowResult(plain)
    rowResult(plain) should contain theSameElementsAs expected

    ColumnarPlanner.partitioning(hashed).map(_.layout) shouldBe Some(columnarLayout)
    ColumnarPlanner.lower(hashed).map(_.shape) shouldBe Some(
      "HashJoin(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.tryRun(hashed) shouldBe defined
  }
