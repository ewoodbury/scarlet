package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.Plan

/**
 * Type-changing element maps stay on the row path; a boolean project after a filter is refused.
 */
class TestColumnarElementMaps
    extends AnyFlatSpec
    with BeforeAndAfterEach
    with ColumnarContractFixture:

  override def beforeEach(): Unit =
    super.beforeEach()
    installContractConf()

  override def afterEach(): Unit =
    restoreContractConf()
    ()

  "the columnar contract" should "refuse a boolean project after a filter and run the mapper once per survivor" in {
    booleanFilterThenMap.observe()
  }

  it should "keep a type-changing element map on the row path" in {
    val intToLong = Plan.MapOp(intSource, (value: Int) => value.toLong)
    val intToPair = Plan.MapOp(intSource, (value: Int) => (value, value))
    val filteredToString = Plan.MapOp(
      Plan.FilterOp(intSource, (value: Int) => value > 0),
      (value: Int) => value.toString,
    )
    val doubleToInt = Plan.MapOp(doubleSource, (value: Double) => value.toInt)
    val stringLength = Plan.MapOp(stringSource, (value: String) => value.length)
    val insidePipeline = Plan.FilterOp(
      Plan.MapOp(
        Plan.MapOp(intSource, (value: Int) => value + 1),
        (value: Int) => value.toString,
      ),
      (value: String) => value.nonEmpty,
    )
    val preserved = Plan.MapOp(intSource, (value: Int) => value + 1)
    val preservedDouble = Plan.MapOp(doubleSource, (value: Double) => value + 1.0)

    assertRowPath(intToLong)
    assertRowPath(intToPair)
    assertRowPath(filteredToString)
    assertRowPath(doubleToInt)
    assertRowPath(stringLength)
    assertRowPath(insidePipeline)
    DistCollection(intToLong).collect() shouldBe Seq(1L, 2L, 3L, 4L)
    DistCollection(intToPair).collect() shouldBe Seq((1, 1), (2, 2), (3, 3), (4, 4))
    DistCollection(filteredToString).collect() shouldBe Seq("1", "2", "3", "4")
    DistCollection(doubleToInt).collect() shouldBe Seq(1, 2, 3)
    DistCollection(stringLength).collect() shouldBe Seq(3, 4, 5)
    DistCollection(insidePipeline).collect() shouldBe Seq("2", "3", "4", "5")

    ColumnarPlanner.partitioning(preserved) shouldBe Some(narrowInfo(2, columnarLayout))
    ColumnarPlanner.lower(preserved).map(_.shape) shouldBe Some("Project(Scan)")
    DistCollection(preserved).collect() shouldBe Seq(2, 3, 4, 5)
    ColumnarPlanner.partitioning(preservedDouble) shouldBe Some(narrowInfo(2, columnarLayout))
    ColumnarPlanner.lower(preservedDouble).map(_.shape) shouldBe Some("Project(Scan)")
    DistCollection(preservedDouble).collect() shouldBe Seq(2.5, 3.5, 4.5)
  }
