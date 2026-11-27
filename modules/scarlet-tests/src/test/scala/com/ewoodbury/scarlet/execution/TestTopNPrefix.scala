package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * The first 100 rows of an int value sort, against the row path.
 *
 * A few hundred owned rows, with repeated values, take the two-filter heap. A second set plants
 * one null value after a present row, so the scan still classifies, and that null orders as zero
 * inside the same prefix.
 */
class TestTopNPrefix extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val limit = 100

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 64,
        threadPoolSize = 4,
        defaultShufflePartitions = 4,
        spillEnabled = false,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "sortByIntValueDesc" should "match the row prefix on a few hundred tied rows" in {
    val rows = 400
    val keys = Array.tabulate(rows)(index => index % 32)
    val values = Array.tabulate(rows)(index => (index * 13) % 17 - 3)
    val sorted = DistCollection
      .ofIntPairs(keys, values, 2)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .sortByIntValueDesc
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some(
      "SortByIntValueDesc(FilterValue(FilterKey(Scan)))",
    )
    val taken = sorted.take(limit)
    taken shouldBe rowTake(sorted, limit)
    taken.length shouldBe limit
    taken.map(_._2).distinct.length should be < taken.length
  }

  it should "keep a null value inside the first hundred when it orders there" in {
    val rows = 300
    val data = (0 until rows).map { index =>
      val key = boxedInt(index % 32)
      val value = if (index == 1) noInt else boxedInt((index % 7) - 6)
      intPair(key, value)
    }
    val sorted = DistCollection(data, 2).sortByIntValueDesc
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some("SortByIntValueDesc(Scan)")
    val taken = sorted.take(limit).map(shown)
    taken shouldBe rowTake(sorted, limit).map(shown)
    taken.length shouldBe limit
    taken.exists { case (_, value) => value.isEmpty } shouldBe true
    taken.map(_._2).count(_.contains(0)) should be > 1
  }

  private def rowTake[A](collection: DistCollection[A], count: Int): List[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      collection.take(count)
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noInt: java.lang.Integer = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)

  private def shown(pair: (Int, Int)): (Option[Int], Option[Int]) =
    (cell(pair, 0), cell(pair, 1))

  private def cell(pair: (Int, Int), ordinal: Int): Option[Int] =
    pair.productElement(ordinal) match
      case value: java.lang.Integer => Some(value.intValue())
      case _ => None
