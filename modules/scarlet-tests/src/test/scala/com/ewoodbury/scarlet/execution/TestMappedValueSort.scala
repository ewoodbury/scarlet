package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * One int `mapValues` under `sortByIntValueDesc`.
 *
 * The sort orders the mapped pairs and does not read the function. A null value stays null and is
 * not passed to `mapValues`. A null key orders as zero and stays null. A second map and a slot
 * above the map stay off this path; those refusals live with the other sort admissions.
 */
class TestMappedValueSort extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 2,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
      ),
    )
    ScarletRuntime.get.shuffle.clear()
    RankSplitRuns.resetForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    RankSplitRuns.resetForTest()
    ScarletConf.set(originalConf)
    ()

  "sortByIntValueDesc" should "sort one int mapValues through IntTopN" in {
    val shifted = DistCollection
      .ofIntPairs(Array(1, 2, 3), Array(10, 30, 20), 2)
      .mapValues((value: Int) => value + 1)
      .sortByIntValueDesc
    shape(shifted) shouldBe "SortByIntValueDesc(MapValue(Scan))"
    collect(shifted) shouldBe Seq(2 -> 31, 3 -> 21, 1 -> 11)
    collect(shifted) shouldBe row(shifted.collect().toSeq)
    shifted.take(2) shouldBe List(2 -> 31, 3 -> 21)
    shifted.take(2) shouldBe row(shifted.take(2))
    collectAt(shifted, 1) shouldBe collectAt(shifted, 8192)

    val under = DistCollection
      .ofIntPairs(Array(1, 2, 3), Array(10, 30, 20), 2)
      .filterKeys((key: Int) => key > 1)
      .filterValues((value: Int) => value > 15)
      .mapValues((value: Int) => value + 1)
      .sortByIntValueDesc
    shape(under) shouldBe "SortByIntValueDesc(MapValue(FilterValue(FilterKey(Scan))))"
    collect(under) shouldBe Seq(2 -> 31, 3 -> 21)
    collect(under) shouldBe row(under.collect().toSeq)
    under.take(1) shouldBe List(2 -> 31)

    val sortPlan = shifted.plan match
      case Plan.SortByIntValueDescOp(sourcePlan) => sourcePlan
      case _ => fail("expected SortByIntValueDescOp")
    val mapped =
      ColumnarPlanner.lower(shifted.plan).getOrElse(fail("expected a value sort")) match
        case PhysicalOp.SortByIntValueDesc(child) => child
        case _ => fail("expected SortByIntValueDesc")
    FilterChain
      .selected(
        sortPlan,
        mapped,
        selectedPartition = None,
        preserveAbsence = true,
        ColumnUse.ordinals(sortPlan, mapped),
      ) shouldBe None
    val filteredPlan = under.plan match
      case Plan.SortByIntValueDescOp(sourcePlan) => sourcePlan
      case _ => fail("expected SortByIntValueDescOp")
    val filteredMap =
      ColumnarPlanner.lower(under.plan).getOrElse(fail("expected a filtered value sort")) match
        case PhysicalOp.SortByIntValueDesc(child) => child
        case _ => fail("expected SortByIntValueDesc")
    FilterChain
      .selected(
        filteredPlan,
        filteredMap,
        selectedPartition = None,
        preserveAbsence = true,
        ColumnUse.ordinals(filteredPlan, filteredMap),
      ) shouldBe None
    IntTopN.resetBatchesForTest()
    collect(shifted) shouldBe Seq(2 -> 31, 3 -> 21, 1 -> 11)
    IntTopN.batchCallsForTest should be > 0

    val calls = new AtomicInteger
    val nulls = DistCollection(
      Seq(
        intPair(boxedInt(1), boxedInt(5)),
        intPair(boxedInt(2), noInt),
        intPair(boxedInt(3), boxedInt(0)),
        intPair(boxedInt(4), boxedInt(-1)),
        intPair(noInt, boxedInt(5)),
      ),
      2,
    ).mapValues { (value: Int) =>
      val _ = calls.incrementAndGet()
      value + 1
    }.sortByIntValueDesc
    shape(nulls) shouldBe "SortByIntValueDesc(MapValue(Scan))"
    calls.set(0)
    val nullFound = collect(nulls)
    // Four present values. The null value stays null and is not passed to mapValues.
    // Stage.mapValues unboxes a null Int to 0 and calls the function, so this collect is not
    // compared to the row path. A null key, below, is not unboxed and does match.
    calls.get() shouldBe 4
    shown(nullFound) shouldBe Seq("null:6", "1:6", "3:1", "2:null", "4:0")
    shown(nulls.take(2)) shouldBe Seq("null:6", "1:6")
    shown(collectAt(nulls, 1)) shouldBe shown(collectAt(nulls, 8192))
    nullFound.exists(pair => !present(pair, 0)) shouldBe true
    nullFound.exists(pair => !present(pair, 1)) shouldBe true

    // A null key is not unboxed by mapValues. It orders as zero and stays null on both paths.
    val nullKey = DistCollection(
      Seq(
        intPair(boxedInt(2), boxedInt(1)),
        intPair(noInt, boxedInt(5)),
        intPair(boxedInt(1), boxedInt(5)),
      ),
      2,
    ).mapValues((value: Int) => value + 1).sortByIntValueDesc
    val nullKeyFound = collect(nullKey)
    shown(nullKeyFound) shouldBe Seq("null:6", "1:6", "2:2")
    shown(nullKeyFound) shouldBe shown(row(nullKey.collect().toSeq))
    shown(nullKey.take(2)) shouldBe Seq("null:6", "1:6")
    shown(nullKey.take(2)) shouldBe shown(row(nullKey.take(2)))
  }

  it should "sort one slot above an int pair project through IntTopN" in {
    val valueSlot = DistCollection
      .ofIntPairs(Array(1, 2, 3), Array(10, 30, 20), 2)
      .map((pair: (Int, Int)) => (pair._1, pair._2 + 1))
      .filterKeys((key: Int) => key > 1)
      .sortByIntValueDesc
    shape(valueSlot) shouldBe "SortByIntValueDesc(FilterKey(Project(Scan)))"
    collect(valueSlot) shouldBe Seq(2 -> 31, 3 -> 21)
    collect(valueSlot) shouldBe row(valueSlot.collect().toSeq)
    valueSlot.take(1) shouldBe List(2 -> 31)
    valueSlot.take(1) shouldBe row(valueSlot.take(1))
    collectAt(valueSlot, 1) shouldBe collectAt(valueSlot, 8192)

    val byValue = DistCollection
      .ofIntPairs(Array(1, 2, 3), Array(10, 30, 20), 2)
      .map((pair: (Int, Int)) => (pair._1, pair._2 + 1))
      .filterValues((value: Int) => value > 20)
      .sortByIntValueDesc
    shape(byValue) shouldBe "SortByIntValueDesc(FilterValue(Project(Scan)))"
    // 21 passes `> 20`, so the projected `(3, 21)` stays. take(1) is the leading pair.
    collect(byValue) shouldBe Seq(2 -> 31, 3 -> 21)
    collect(byValue) shouldBe row(byValue.collect().toSeq)
    byValue.take(1) shouldBe List(2 -> 31)
    collectAt(byValue, 1) shouldBe collectAt(byValue, 8192)

    val sortPlan = valueSlot.plan match
      case Plan.SortByIntValueDescOp(sourcePlan) => sourcePlan
      case _ => fail("expected SortByIntValueDescOp")
    val filterOp =
      ColumnarPlanner.lower(valueSlot.plan).getOrElse(fail("expected a slot sort")) match
        case PhysicalOp.SortByIntValueDesc(child) => child
        case _ => fail("expected SortByIntValueDesc")
    FilterChain
      .selected(
        sortPlan,
        filterOp,
        selectedPartition = None,
        preserveAbsence = true,
        ColumnUse.ordinals(sortPlan, filterOp),
      )
      .isDefined shouldBe true
    IntPairProject.resetPublishedForTest()
    IntTopN.resetBatchesForTest()
    collect(valueSlot) shouldBe Seq(2 -> 31, 3 -> 21)
    IntPairProject.publishedForTest should be > 0
    IntTopN.batchCallsForTest should be > 0

    val nulls = DistCollection(
      Seq(
        intPair(boxedInt(1), boxedInt(5)),
        intPair(boxedInt(2), noInt),
        intPair(boxedInt(3), boxedInt(0)),
        intPair(boxedInt(4), boxedInt(-1)),
      ),
      2,
    ).map((pair: (Int, Int)) => copyPair(pair))
      .filterKeys((key: Int) => key > 0)
      .sortByIntValueDesc
    shape(nulls) shouldBe "SortByIntValueDesc(FilterKey(Project(Scan)))"
    val nullFound = collect(nulls)
    shown(nullFound) shouldBe Seq("1:5", "2:null", "3:0", "4:-1")
    shown(nullFound) shouldBe shown(row(nulls.collect().toSeq))
    shown(collectAt(nulls, 1)) shouldBe shown(collectAt(nulls, 8192))
  }

  private def shape(data: DistCollection[_]): String =
    ColumnarPlanner.lower(data.plan).map(_.shape).getOrElse(fail("expected a columnar plan"))

  private def collect(data: DistCollection[(Int, Int)]): Seq[(Int, Int)] =
    ScarletRuntime.get.shuffle.clear()
    data.collect().toSeq

  private def collectAt(data: DistCollection[(Int, Int)], batch: Int): Seq[(Int, Int)] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarBatchSize = batch))
      collect(data)
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
  private def shown(rows: Seq[(Int, Int)]): Seq[String] =
    rows.map { row =>
      val product = row.asInstanceOf[Product]
      String.valueOf(product.productElement(0)) + ":" + String.valueOf(product.productElement(1))
    }

  private def present(pair: (Int, Int), ordinal: Int): Boolean =
    pair.productElement(ordinal) match
      case _: java.lang.Integer => true
      case _ => false

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def intPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  /** Copies both cells, including a null. `productElement` does not unbox. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def copyPair(pair: (Int, Int)): (Int, Int) =
    val product = pair.asInstanceOf[Product]
    Tuple2(product.productElement(0), product.productElement(1)).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noInt: java.lang.Integer = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)
