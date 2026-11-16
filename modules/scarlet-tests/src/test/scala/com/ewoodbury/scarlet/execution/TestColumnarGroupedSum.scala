package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.Plan

/** Grouped-sum fusion and bounded take, outside the fixture lists. */
class TestColumnarGroupedSum
    extends AnyFlatSpec
    with BeforeAndAfterEach
    with ColumnarContractFixture:

  override def beforeEach(): Unit =
    super.beforeEach()
    installContractConf()

  override def afterEach(): Unit =
    restoreContractConf()
    ()

  "the columnar contract" should "keep fused grouped sum action-level and bounded take on its accepted physical shape" in {
    // A pair map that stays (Int, Int) is refused by lower, so partitioning and tryRun are the
    // row path. tryGroupedSum still fuses that map. The peel label names the walked steps.
    val mappedPairs =
      Plan.MapOp(pairSource, (pair: (Int, Int)) => (pair._1 + 1, pair._2 + pair._1))
    ColumnarPlanner.partitioning(mappedPairs) shouldBe Option.empty[PartitioningInfo]
    ColumnarPlanner.lower(mappedPairs) shouldBe Option.empty[PhysicalOp]
    ColumnarPlanner.tryRun(mappedPairs) shouldBe None
    ColumnarPlanner.tryGroupedSum(mappedPairs) shouldBe Some(66L)
    ColumnarPlanner.groupedSumPeel(mappedPairs) shouldBe Some("GroupedSum(MapPair(Scan))")

    // A type-changing pair map is excluded. lower refuses it, and tryGroupedSum does not fuse it.
    // The peel label is the walked MapPair step, not an action shape.
    val widenedPairs = Plan.MapOp(pairSource, (pair: (Int, Int)) => (pair._1.toLong, pair._2))
    assertRowPath(widenedPairs)
    ColumnarPlanner.tryGroupedSum(widenedPairs) shouldBe None
    ColumnarPlanner.groupedSumPeel(widenedPairs) shouldBe Some("GroupedSum(MapPair(Scan))")

    ColumnarPlanner.tryGroupedSum(pairSource) shouldBe Some(60L)
    ColumnarPlanner.groupedSumPeel(pairSource) shouldBe Some("GroupedSum(Scan)")
    ColumnarPlanner.partitioning(emptyPairSource).map(_.layout) shouldBe Some(columnarLayout)
    ColumnarPlanner.lower(emptyPairSource).map(_.shape) shouldBe Some("Scan")
    ColumnarPlanner.groupedSumPeel(emptyPairSource) shouldBe Some("GroupedSum(Scan)")
    DistCollection(emptyPairSource).groupedSum shouldBe 0L
    DistCollection(emptyPairSource).collect() shouldBe empty
    DistCollection(emptyPairSource).take(3) shouldBe empty
    val keyFilter = Plan.FilterKeysOp(pairSource, (key: Int) => key > 1)
    ColumnarPlanner.tryGroupedSum(keyFilter) shouldBe Some(50L)
    ColumnarPlanner.groupedSumPeel(keyFilter) shouldBe Some("GroupedSum(FilterKey(Scan))")
    val valueFilter = Plan.FilterValuesOp(pairSource, (value: Int) => value > 10)
    ColumnarPlanner.tryGroupedSum(valueFilter) shouldBe Some(50L)
    ColumnarPlanner.groupedSumPeel(valueFilter) shouldBe Some("GroupedSum(FilterValue(Scan))")
    val rowFilter = Plan.FilterOp(pairSource, (pair: (Int, Int)) => pair._1 > 1)
    ColumnarPlanner.tryGroupedSum(rowFilter) shouldBe Some(50L)
    ColumnarPlanner.groupedSumPeel(rowFilter) shouldBe Some("GroupedSum(FilterRow(Scan))")

    val intPipeline = Plan.MapOp(
      Plan.FilterOp(intSource, (value: Int) => value % 2 == 0),
      (value: Int) => value * 10,
    )
    ColumnarPlanner.lower(intPipeline).map(_.shape) shouldBe Some("Pipeline2(Scan)")
    ColumnarPlanner.tryTake(intPipeline, 2) shouldBe Some(Seq(20, 40))

    val pairFilter = Plan.FilterValuesOp(pairSource, (value: Int) => value > 10)
    ColumnarPlanner.lower(pairFilter).map(_.shape) shouldBe Some("FilterValue(Scan)")
    ColumnarPlanner.tryTake(pairFilter, 1) shouldBe Some(Seq(2 -> 20))

    val stringPipeline = DistCollection(stringSource)
      .filterStringEquals("red")
      .map(_.toUpperCase(java.util.Locale.ENGLISH))
      .plan
    ColumnarPlanner.lower(stringPipeline).map(_.shape) shouldBe Some("Project(Filter(Scan))")
    ColumnarPlanner.tryTake(stringPipeline, 1) shouldBe Some(Seq("RED"))
  }
