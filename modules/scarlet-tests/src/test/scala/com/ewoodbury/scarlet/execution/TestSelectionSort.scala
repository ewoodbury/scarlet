package com.ewoodbury.scarlet.execution

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Int `sortByIntKey` over a filter selection.
 *
 * A filter stack keeps the survivor index, including one slot filter. The rank split sorts those
 * rows and still publishes a compacted sorted batch. A null key orders as zero and stays null.
 * Ties keep input order. A scan still sorts on the dense path. Over a tiny spill budget the
 * survivor index is spilled. Dropped rows are not written. Spill off writes nothing.
 */
class TestSelectionSort extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val keys = Array(1, -1, 2, 3, 4, 0, 5, 6)
  private val values = Array(10, 20, 30, 40, 50, 5, 60, 70)
  private val kept = Seq(2 -> 30, 3 -> 40, 4 -> 50, 5 -> 60, 6 -> 70)
  private val stackShape = "SortByIntKey(FilterValue(FilterKey(Scan)))"

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

  "a two-filter sort" should "match the row path, the dense sort, and skip the compact" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val source = pairs
    val filtered = source
      .filterKeys { (key: Int) =>
        val _ = keyCalls.incrementAndGet()
        key > 0
      }
      .filterValues { (value: Int) =>
        val _ = valueCalls.incrementAndGet()
        value > 15
      }
    val sorted = filtered.sortByIntKey

    shape(sorted) shouldBe stackShape
    val found = collect(sorted)
    // One pass: the key predicate sees every row, and the value predicate sees the six positive
    // keys. A second interpret would double both counts.
    keyCalls.get() shouldBe keys.length
    valueCalls.get() shouldBe 6
    found shouldBe kept
    found shouldBe row(sorted.collect().toSeq)
    found shouldBe decoded(denseSorted(filtered))
    assertFewerBatches(sorted, filtered, source)
  }

  it should "keep duplicate keys in input order, including across partitions" in {
    val source = DistCollection.ofIntPairs(
      Array(1, 2, 1, 2, 1, 2, 1, 2),
      Array(10, 20, 30, 40, 50, 60, 70, 80),
      2,
    )
    val sorted = keepAll(source).sortByIntKey
    val found = collect(sorted)

    shape(sorted) shouldBe stackShape
    found shouldBe Seq(1 -> 10, 1 -> 30, 1 -> 50, 1 -> 70, 2 -> 20, 2 -> 40, 2 -> 60, 2 -> 80)
    found shouldBe row(sorted.collect().toSeq)
    found shouldBe decoded(denseSorted(keepAll(source)))
  }

  it should "keep a null key that passes a value filter and order it as zero" in {
    val rows = Seq(
      intPair(boxedInt(2), boxedInt(20)),
      intPair(noInt, boxedInt(5)),
      intPair(boxedInt(-4), boxedInt(1)),
      intPair(boxedInt(0), boxedInt(8)),
      intPair(noInt, boxedInt(3)),
      intPair(boxedInt(1), noInt),
    )
    val sorted = DistCollection(rows, 2)
      .filterValues((value: Int) => value > 0)
      .filterValues((value: Int) => value < 100)
      .sortByIntKey
    val found = collect(sorted)

    shape(sorted) shouldBe "SortByIntKey(FilterValue(FilterValue(Scan)))"
    found shouldBe row(sorted.collect().toSeq)
    found shouldBe Seq(
      intPair(boxedInt(-4), boxedInt(1)),
      intPair(noInt, boxedInt(5)),
      intPair(boxedInt(0), boxedInt(8)),
      intPair(noInt, boxedInt(3)),
      intPair(boxedInt(2), boxedInt(20)),
    )
    val parts = sortedParts(sorted.plan)
    val flat = parts.flatMap(batch => cells(batch))
    flat shouldBe Seq(
      (Some(-4), Some(1)),
      (None, Some(5)),
      (Some(0), Some(8)),
      (None, Some(3)),
      (Some(2), Some(20)),
    )
  }

  it should "keep a null value on a selected row and drop a null key" in {
    val rows = Seq(
      intPair(boxedInt(1), boxedInt(10)),
      intPair(noInt, boxedInt(99)),
      intPair(boxedInt(0), noInt),
      intPair(boxedInt(2), boxedInt(20)),
    )
    val sorted = DistCollection(rows, 2)
      .filterKeys((key: Int) => key > -1)
      .filterKeys((key: Int) => key < 10)
      .sortByIntKey
    val found = collect(sorted)

    shape(sorted) shouldBe "SortByIntKey(FilterKey(FilterKey(Scan)))"
    // A null Int unboxes to 0 on the row path, so both key predicates keep that row. The columnar
    // key filter drops a null key and does not call the predicate. The null value stays.
    found shouldBe Seq(
      intPair(boxedInt(0), noInt),
      intPair(boxedInt(1), boxedInt(10)),
      intPair(boxedInt(2), boxedInt(20)),
    )
    row(sorted.collect().toSeq) shouldBe Seq(
      intPair(noInt, boxedInt(99)),
      intPair(boxedInt(0), noInt),
      intPair(boxedInt(1), boxedInt(10)),
      intPair(boxedInt(2), boxedInt(20)),
    )
    val flat = sortedParts(sorted.plan).flatMap(batch => cells(batch))
    flat shouldBe Seq((Some(0), None), (Some(1), Some(10)), (Some(2), Some(20)))
  }

  it should "drop a null key under a single filter and keep it on the row path" in {
    val rows = Seq(
      intPair(boxedInt(1), boxedInt(10)),
      intPair(noInt, boxedInt(99)),
      intPair(boxedInt(2), boxedInt(20)),
    )
    val sorted = DistCollection(rows, 2)
      .filterKeys((key: Int) => key > -1)
      .sortByIntKey
    val found = collect(sorted)

    shape(sorted) shouldBe "SortByIntKey(FilterKey(Scan))"
    found shouldBe Seq(intPair(boxedInt(1), boxedInt(10)), intPair(boxedInt(2), boxedInt(20)))
    row(sorted.collect().toSeq) shouldBe Seq(
      intPair(noInt, boxedInt(99)),
      intPair(boxedInt(1), boxedInt(10)),
      intPair(boxedInt(2), boxedInt(20)),
    )
  }

  it should "sort one hot key in input order" in {
    val rows = 8
    val source = DistCollection.ofIntPairs(Array.fill(rows)(7), Array.tabulate(rows)(_ + 1), 2)
    val sorted = source
      .filterKeys((key: Int) => key == 7)
      .filterValues((value: Int) => value > 0)
      .sortByIntKey
    val found = collect(sorted)

    shape(sorted) shouldBe stackShape
    found shouldBe (1 to rows).map(value => 7 -> value)
    found shouldBe row(sorted.collect().toSeq)
  }

  it should "keep every row when both filters pass" in {
    val source = pairs
    val filtered = keepAll(source)
    val sorted = filtered.sortByIntKey
    val found = collect(sorted)
    val plain = collect(source.sortByIntKey)

    shape(sorted) shouldBe stackShape
    found shouldBe plain
    found shouldBe row(sorted.collect().toSeq)
    found shouldBe decoded(denseSorted(filtered))
  }

  it should "publish empty output partitions when both filters reject every row" in {
    val sorted = pairs
      .filterKeys((_: Int) => false)
      .filterValues((_: Int) => true)
      .sortByIntKey
    val parts = sortedParts(sorted.plan)

    shape(sorted) shouldBe stackShape
    collect(sorted) shouldBe empty
    collect(sorted) shouldBe row(sorted.collect().toSeq)
    parts.length shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach { batch =>
      batch.length shouldBe 0
      batch.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int32)
    }
  }

  it should "publish empty output partitions for an empty source" in {
    val sorted = DistCollection
      .ofIntPairs(Array.empty[Int], Array.empty[Int], 2)
      .filterKeys((_: Int) => true)
      .filterValues((_: Int) => true)
      .sortByIntKey
    val parts = sortedParts(sorted.plan)

    shape(sorted) shouldBe stackShape
    collect(sorted) shouldBe empty
    parts.length shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(_.length shouldBe 0)
  }

  it should "match the in-memory selection sort and the row path when spill is on" in {
    val source = pairs
    val sorted = source
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 15)
      .sortByIntKey
    val inMemory = collect(sorted)

    withSpill(budget = 1L) { _ =>
      val spilled = collect(sorted)
      spilled shouldBe inMemory
      spilled shouldBe kept
      spilled shouldBe row(sorted.collect().toSeq)
      RankSplitRuns.filesWrittenForTest should be > 0L
    }
  }

  it should "match the in-memory sort when spill is on and the prefix fits" in {
    val source = pairs
    val sorted = source
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 15)
      .sortByIntKey
    val inMemory = collect(sorted)

    withSpill(budget = 64L * 1024L * 1024L) { _ =>
      val stayed = collect(sorted)
      stayed shouldBe inMemory
      stayed shouldBe kept
      stayed shouldBe row(sorted.collect().toSeq)
      RankSplitRuns.filesWrittenForTest shouldBe 0L
    }
  }

  "a single filter" should "sort the survivor index and skip the compact" in {
    val source = pairs
    val filtered = source.filterKeys((key: Int) => key > 0)
    val sorted = filtered.sortByIntKey

    shape(sorted) shouldBe "SortByIntKey(FilterKey(Scan))"
    val found = collect(sorted)
    found shouldBe Seq(1 -> 10, 2 -> 30, 3 -> 40, 4 -> 50, 5 -> 60, 6 -> 70)
    found shouldBe row(sorted.collect().toSeq)
    assertFewerBatches(sorted, filtered, source)
  }

  "a scan" should "take the dense path, and one pair project should sort through rank split" in {
    val source = pairs
    val sorted = source.sortByIntKey
    val mapped = source.map((pair: (Int, Int)) => (pair._1, pair._2)).sortByIntKey

    shape(sorted) shouldBe "SortByIntKey(Scan)"
    collect(sorted) shouldBe row(sorted.collect().toSeq)
    val direct = published {
      val scan = ColumnarPlanner.lower(source.plan).getOrElse(fail("expected a scan"))
      val batches = ColumnarInterpreter.pairsOf(ColumnarInterpreter.interpret(source.plan, scan))
      RankSplitRuns.sortAndSplit(
        batches,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ScarletConf.get,
      )
    }
    published(sorted.collect()) shouldBe direct

    shape(mapped) shouldBe "SortByIntKey(Project(Scan))"
    collect(mapped) shouldBe collect(sorted)
    collect(mapped) shouldBe row(mapped.collect().toSeq)
    val shifted = DistCollection
      .ofIntPairs(Array(3, 1, 2), Array(30, 10, 20), 2)
      .map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
      .sortByIntKey
    shape(shifted) shouldBe "SortByIntKey(Project(Scan))"
    collect(shifted) shouldBe Seq(2 -> 10, 3 -> 20, 4 -> 30)
    collect(shifted) shouldBe row(shifted.collect().toSeq)
    collectAt(shifted, 1) shouldBe collectAt(shifted, 8192)

    val under = source
      .filterKeys((key: Int) => key > 0)
      .map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
      .sortByIntKey
    shape(under) shouldBe "SortByIntKey(Project(FilterKey(Scan)))"
    collect(under) shouldBe row(under.collect().toSeq)

    RankSplit.resetForTest()
    IntPairProject.resetPublishedForTest()
    val found = collect(mapped)
    found shouldBe collect(sorted)
    IntPairProject.publishedForTest should be > 0
    RankSplit.mergePassesForTest should be > 0
  }

  it should "order a null key from a pair project as zero and keep the null" in {
    val rows = Seq(
      intPair(boxedInt(1), boxedInt(10)),
      intPair(noInt, boxedInt(99)),
      intPair(boxedInt(-5), boxedInt(3)),
      intPair(boxedInt(0), boxedInt(7)),
      intPair(noInt, noInt),
    )
    val source = DistCollection(rows, 2)
    val mapped = source.map((pair: (Int, Int)) => copyPair(pair)).sortByIntKey
    val plain = source.sortByIntKey

    shape(mapped) shouldBe "SortByIntKey(Project(Scan))"
    val found = collect(mapped)
    shown(found) shouldBe shown(collect(plain))
    shown(found) shouldBe shown(row(mapped.collect().toSeq))
    shown(collectAt(mapped, 1)) shouldBe shown(collectAt(mapped, 8192))
    effectiveKeys(found) shouldBe effectiveKeys(found).sorted
    found.exists(pair => !present(pair, 0)) shouldBe true
    found.exists(pair => !present(pair, 1)) shouldBe true
  }

  it should "sort one slot above an int pair project through rank split" in {
    val shifted = DistCollection
      .ofIntPairs(Array(3, 1, 2), Array(30, 10, 20), 2)
      .map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
    val keyed = shifted.filterKeys((key: Int) => key > 2).sortByIntKey
    shape(keyed) shouldBe "SortByIntKey(FilterKey(Project(Scan)))"
    collect(keyed) shouldBe Seq(3 -> 20, 4 -> 30)
    collect(keyed) shouldBe row(keyed.collect().toSeq)
    collectAt(keyed, 1) shouldBe collectAt(keyed, 8192)

    // Value 10 fails `> 10`, so the mapped `(2, 10)` is dropped.
    val valued = shifted.filterValues((value: Int) => value > 10).sortByIntKey
    shape(valued) shouldBe "SortByIntKey(FilterValue(Project(Scan)))"
    collect(valued) shouldBe Seq(3 -> 20, 4 -> 30)
    collect(valued) shouldBe row(valued.collect().toSeq)
    collectAt(valued, 1) shouldBe collectAt(valued, 8192)

    val onStack = pairs
      .filterKeys((key: Int) => key > 0)
      .map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
      .filterKeys((key: Int) => key > 2)
      .sortByIntKey
    shape(onStack) shouldBe "SortByIntKey(FilterKey(Project(FilterKey(Scan))))"
    collect(onStack) shouldBe row(onStack.collect().toSeq)

    val nulls = DistCollection(
      Seq(
        intPair(boxedInt(1), boxedInt(10)),
        intPair(noInt, boxedInt(99)),
        intPair(boxedInt(-5), boxedInt(3)),
        intPair(boxedInt(0), boxedInt(7)),
        intPair(noInt, noInt),
      ),
      2,
    ).map((pair: (Int, Int)) => copyPair(pair))
      .filterValues((value: Int) => value > 0)
      .sortByIntKey
    shape(nulls) shouldBe "SortByIntKey(FilterValue(Project(Scan)))"
    val nullFound = collect(nulls)
    shown(nullFound) shouldBe shown(row(nulls.collect().toSeq))
    shown(collectAt(nulls, 1)) shouldBe shown(collectAt(nulls, 8192))
    effectiveKeys(nullFound) shouldBe effectiveKeys(nullFound).sorted
    nullFound.exists(pair => !present(pair, 0)) shouldBe true

    RankSplit.resetForTest()
    IntPairProject.resetPublishedForTest()
    val again = collect(keyed)
    again shouldBe Seq(3 -> 20, 4 -> 30)
    IntPairProject.publishedForTest should be > 0
    RankSplit.mergePassesForTest should be > 0
    val filteredPlan = keyed.plan match
      case Plan.SortByIntKeyOp(sourcePlan) => sourcePlan
      case _ => fail("expected SortByIntKeyOp")
    val filterOp = ColumnarPlanner.lower(keyed.plan).getOrElse(fail("expected a slot sort")) match
      case PhysicalOp.SortByIntKey(inner) => inner
      case _ => fail("expected SortByIntKey")
    FilterChain
      .selected(
        filteredPlan,
        filterOp,
        selectedPartition = None,
        preserveAbsence = true,
        ColumnUse.ordinals(filteredPlan, filterOp),
      )
      .isDefined shouldBe true
  }

  "sortByIntValueDesc" should "sort one int-pair project through IntTopN" in {
    val shifted = DistCollection
      .ofIntPairs(Array(1, 2, 3), Array(10, 30, 20), 2)
      .map((pair: (Int, Int)) => (pair._1, pair._2 + 1))
      .sortByIntValueDesc
    shape(shifted) shouldBe "SortByIntValueDesc(Project(Scan))"
    collect(shifted) shouldBe Seq(2 -> 31, 3 -> 21, 1 -> 11)
    collect(shifted) shouldBe row(shifted.collect().toSeq)
    shifted.take(2) shouldBe List(2 -> 31, 3 -> 21)
    shifted.take(2) shouldBe row(shifted.take(2))
    collectAt(shifted, 1) shouldBe collectAt(shifted, 8192)

    val under = DistCollection
      .ofIntPairs(Array(1, 2, 3), Array(10, 30, 20), 2)
      .filterKeys((key: Int) => key > 1)
      .filterValues((value: Int) => value > 15)
      .map((pair: (Int, Int)) => (pair._1, pair._2 + 1))
      .sortByIntValueDesc
    shape(under) shouldBe "SortByIntValueDesc(Project(FilterValue(FilterKey(Scan))))"
    collect(under) shouldBe Seq(2 -> 31, 3 -> 21)
    collect(under) shouldBe row(under.collect().toSeq)
    under.take(1) shouldBe List(2 -> 31)

    val sortPlan = shifted.plan match
      case Plan.SortByIntValueDescOp(sourcePlan) => sourcePlan
      case _ => fail("expected SortByIntValueDescOp")
    val project =
      ColumnarPlanner.lower(shifted.plan).getOrElse(fail("expected a value sort")) match
        case PhysicalOp.SortByIntValueDesc(child) => child
        case _ => fail("expected SortByIntValueDesc")
    FilterChain
      .selected(
        sortPlan,
        project,
        selectedPartition = None,
        preserveAbsence = true,
        ColumnUse.ordinals(sortPlan, project),
      ) shouldBe None
    IntPairProject.resetPublishedForTest()
    IntTopN.resetBatchesForTest()
    collect(shifted) shouldBe Seq(2 -> 31, 3 -> 21, 1 -> 11)
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
    ).map((pair: (Int, Int)) => copyPair(pair)).sortByIntValueDesc
    shape(nulls) shouldBe "SortByIntValueDesc(Project(Scan))"
    val nullFound = collect(nulls)
    shown(nullFound) shouldBe Seq("1:5", "2:null", "3:0", "4:-1")
    shown(nullFound) shouldBe shown(row(nulls.collect().toSeq))
    shown(nulls.take(2)) shouldBe Seq("1:5", "2:null")
    shown(collectAt(nulls, 1)) shouldBe shown(collectAt(nulls, 8192))
    nullFound.exists(pair => !present(pair, 1)) shouldBe true
  }

  "sortByIntValueDesc" should "sort one MapKey through IntTopN" in {
    val keyedDesc = DistCollection
      .ofIntPairs(Array(1, 2, 3), Array(10, 30, 20), 2)
      .mapIntKey((key: Int) => key + 1)
      .sortByIntValueDesc
    shape(keyedDesc) shouldBe "SortByIntValueDesc(MapKey(Scan))"
    collect(keyedDesc) shouldBe Seq(3 -> 30, 4 -> 20, 2 -> 10)
    collect(keyedDesc) shouldBe row(keyedDesc.collect().toSeq)
    keyedDesc.take(2) shouldBe List(3 -> 30, 4 -> 20)
    keyedDesc.take(2) shouldBe row(keyedDesc.take(2))
    collectAt(keyedDesc, 1) shouldBe collectAt(keyedDesc, 8192)

    val under = DistCollection
      .ofIntPairs(Array(1, 2, 3), Array(10, 30, 20), 2)
      .filterKeys((key: Int) => key > 1)
      .filterValues((value: Int) => value > 15)
      .mapIntKey((key: Int) => key + 1)
      .sortByIntValueDesc
    shape(under) shouldBe "SortByIntValueDesc(MapKey(FilterValue(FilterKey(Scan))))"
    collect(under) shouldBe Seq(3 -> 30, 4 -> 20)
    collect(under) shouldBe row(under.collect().toSeq)
    under.take(1) shouldBe List(3 -> 30)

    val sortPlan = keyedDesc.plan match
      case Plan.SortByIntValueDescOp(sourcePlan) => sourcePlan
      case _ => fail("expected SortByIntValueDescOp")
    val mapped =
      ColumnarPlanner.lower(keyedDesc.plan).getOrElse(fail("expected a value sort")) match
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
    MapIntKey.resetPublishedForTest()
    IntTopN.resetBatchesForTest()
    collect(keyedDesc) shouldBe Seq(3 -> 30, 4 -> 20, 2 -> 10)
    MapIntKey.publishedForTest should be > 0
    IntTopN.batchCallsForTest should be > 0

    val calls = new AtomicInteger
    val nulls = DistCollection(
      Seq(
        intPair(boxedInt(1), boxedInt(5)),
        intPair(noInt, boxedInt(9)),
        intPair(boxedInt(2), noInt),
        intPair(boxedInt(3), boxedInt(0)),
      ),
      2,
    ).mapIntKey { (key: Int) =>
      val _ = calls.incrementAndGet()
      key + 1
    }.sortByIntValueDesc
    shape(nulls) shouldBe "SortByIntValueDesc(MapKey(Scan))"
    calls.set(0)
    val nullFound = collect(nulls)
    calls.get() shouldBe 3
    shown(nullFound) shouldBe Seq("null:9", "2:5", "3:null", "4:0")
    calls.set(0)
    shown(nullFound) shouldBe shown(row(nulls.collect().toSeq))
    calls.get() shouldBe 3
    shown(nulls.take(2)) shouldBe Seq("null:9", "2:5")
    shown(collectAt(nulls, 1)) shouldBe shown(collectAt(nulls, 8192))
    nullFound.exists(pair => !present(pair, 0)) shouldBe true
    nullFound.exists(pair => !present(pair, 1)) shouldBe true
  }

  it should "sort one key or value filter above MapKey through IntTopN" in {
    val slotAboveMap = DistCollection
      .ofIntPairs(Array(1, 2, 3), Array(10, 30, 20), 2)
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 2)
      .sortByIntValueDesc
    shape(slotAboveMap) shouldBe "SortByIntValueDesc(FilterKey(MapKey(Scan)))"
    val found = collect(slotAboveMap)
    found shouldBe Seq(3 -> 30, 4 -> 20)
    found shouldBe row(slotAboveMap.collect().toSeq)
    slotAboveMap.take(1) shouldBe List(3 -> 30)
    slotAboveMap.take(1) shouldBe row(slotAboveMap.take(1))
    collectAt(slotAboveMap, 1) shouldBe collectAt(slotAboveMap, 8192)

    val byValue = DistCollection
      .ofIntPairs(Array(1, 2, 3), Array(10, 30, 20), 2)
      .mapIntKey((key: Int) => key + 1)
      .filterValues((value: Int) => value > 15)
      .sortByIntValueDesc
    shape(byValue) shouldBe "SortByIntValueDesc(FilterValue(MapKey(Scan)))"
    val valuesFound = collect(byValue)
    valuesFound shouldBe Seq(3 -> 30, 4 -> 20)
    valuesFound shouldBe row(byValue.collect().toSeq)
    collectAt(byValue, 1) shouldBe collectAt(byValue, 8192)

    val under = DistCollection
      .ofIntPairs(Array(1, 2, 3), Array(10, 30, 20), 2)
      .filterKeys((key: Int) => key > 1)
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 2)
      .sortByIntValueDesc
    shape(under) shouldBe "SortByIntValueDesc(FilterKey(MapKey(FilterKey(Scan))))"
    collect(under) shouldBe Seq(3 -> 30, 4 -> 20)
    collect(under) shouldBe row(under.collect().toSeq)

    val sortPlan = slotAboveMap.plan match
      case Plan.SortByIntValueDescOp(sourcePlan) => sourcePlan
      case _ => fail("expected SortByIntValueDescOp")
    val mapped =
      ColumnarPlanner.lower(slotAboveMap.plan).getOrElse(fail("expected a value sort")) match
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
    MapIntKey.resetPublishedForTest()
    IntTopN.resetBatchesForTest()
    collect(slotAboveMap) shouldBe Seq(3 -> 30, 4 -> 20)
    MapIntKey.publishedForTest should be > 0
    IntTopN.batchCallsForTest should be > 0

    val calls = new AtomicInteger
    val nulls = DistCollection(
      Seq(
        intPair(boxedInt(1), boxedInt(5)),
        intPair(noInt, boxedInt(5)),
        intPair(boxedInt(2), noInt),
        intPair(boxedInt(3), boxedInt(0)),
      ),
      2,
    ).mapIntKey { (key: Int) =>
      val _ = calls.incrementAndGet()
      key + 1
    }.filterValues((value: Int) => value > 0)
      .sortByIntValueDesc
    shape(nulls) shouldBe "SortByIntValueDesc(FilterValue(MapKey(Scan)))"
    calls.set(0)
    val nullFound = collect(nulls)
    calls.get() shouldBe 3
    shown(nullFound) shouldBe Seq("null:5", "2:5")
    calls.set(0)
    shown(nullFound) shouldBe shown(row(nulls.collect().toSeq))
    calls.get() shouldBe 3

    val valueCalls = new AtomicInteger
    val nullValues = DistCollection(
      Seq(
        intPair(boxedInt(1), boxedInt(5)),
        intPair(noInt, boxedInt(9)),
        intPair(boxedInt(2), noInt),
        intPair(boxedInt(3), boxedInt(0)),
      ),
      2,
    ).mapIntKey { (key: Int) =>
      val _ = valueCalls.incrementAndGet()
      key + 1
    }.filterKeys((key: Int) => key > 0)
      .sortByIntValueDesc
    shape(nullValues) shouldBe "SortByIntValueDesc(FilterKey(MapKey(Scan)))"
    valueCalls.set(0)
    val valueFound = collect(nullValues)
    valueCalls.get() shouldBe 3
    shown(valueFound) shouldBe Seq("2:5", "3:null", "4:0")
    valueCalls.set(0)
    shown(valueFound) shouldBe shown(row(nullValues.collect().toSeq))
    valueCalls.get() shouldBe 3
  }

  it should "sort one key or value filter above MapKey through RankSplit" in {
    val mappedKey = DistCollection
      .ofIntPairs(Array(3, 1, 2), Array(30, 10, 20), 2)
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 2)
      .sortByIntKey
    shape(mappedKey) shouldBe "SortByIntKey(FilterKey(MapKey(Scan)))"
    collect(mappedKey) shouldBe Seq(3 -> 20, 4 -> 30)
    collect(mappedKey) shouldBe row(mappedKey.collect().toSeq)
    collectAt(mappedKey, 1) shouldBe collectAt(mappedKey, 8192)

    // Value 10 fails `> 10`, so the mapped `(2, 10)` is dropped.
    val byValue = DistCollection
      .ofIntPairs(Array(3, 1, 2), Array(30, 10, 20), 2)
      .mapIntKey((key: Int) => key + 1)
      .filterValues((value: Int) => value > 10)
      .sortByIntKey
    shape(byValue) shouldBe "SortByIntKey(FilterValue(MapKey(Scan)))"
    collect(byValue) shouldBe Seq(3 -> 20, 4 -> 30)
    collect(byValue) shouldBe row(byValue.collect().toSeq)
    collectAt(byValue, 1) shouldBe collectAt(byValue, 8192)

    val under = DistCollection
      .ofIntPairs(Array(3, 1, 2), Array(30, 10, 20), 2)
      .filterKeys((key: Int) => key > 1)
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 2)
      .sortByIntKey
    shape(under) shouldBe "SortByIntKey(FilterKey(MapKey(FilterKey(Scan))))"
    collect(under) shouldBe Seq(3 -> 20, 4 -> 30)
    collect(under) shouldBe row(under.collect().toSeq)

    val sortPlan = mappedKey.plan match
      case Plan.SortByIntKeyOp(sourcePlan) => sourcePlan
      case _ => fail("expected SortByIntKeyOp")
    val mapped =
      ColumnarPlanner.lower(mappedKey.plan).getOrElse(fail("expected a key sort")) match
        case PhysicalOp.SortByIntKey(child) => child
        case _ => fail("expected SortByIntKey")
    FilterChain
      .selected(
        sortPlan,
        mapped,
        selectedPartition = None,
        preserveAbsence = true,
        ColumnUse.ordinals(sortPlan, mapped),
      ) shouldBe None
    MapIntKey.resetPublishedForTest()
    RankSplit.resetForTest()
    collect(mappedKey) shouldBe Seq(3 -> 20, 4 -> 30)
    MapIntKey.publishedForTest should be > 0
    RankSplit.mergePassesForTest should be > 0
  }

  "other maps" should "stay off sortByIntKey" in {
    val source = pairs
    val values = source.mapValues((value: Int) => value + 1).sortByIntKey
    val twice = source
      .map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
      .map((pair: (Int, Int)) => (pair._1, pair._2 + 1))
    val above = source
      .map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
      .filter((pair: (Int, Int)) => pair._2 > 0)
      .sortByIntKey
    val twiceDesc = twice.sortByIntValueDesc
    val aboveMap = source
      .mapValues((value: Int) => value + 1)
      .filterValues((value: Int) => value > 0)
      .sortByIntValueDesc
    val twiceMapped = source
      .mapValues((value: Int) => value + 1)
      .mapValues((value: Int) => value + 1)
      .sortByIntValueDesc
    val onProject = source
      .map((pair: (Int, Int)) => (pair._1, pair._2))
      .mapValues((value: Int) => value + 1)
      .sortByIntValueDesc
    val second = source
      .map((pair: (Int, Int)) => (pair._1 + 1, pair._2))
      .filterKeys((key: Int) => key > 2)
      .filterValues((value: Int) => value > 0)
      .sortByIntKey
    val secondAbove = source
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 0)
      .sortByIntKey
    val secondKey = source
      .mapIntKey((key: Int) => key + 1)
      .mapIntKey((key: Int) => key + 1)
      .sortByIntKey
    val rowAboveKey = source
      .mapIntKey((key: Int) => key + 1)
      .filter((pair: (Int, Int)) => pair._2 > 0)
      .sortByIntKey
    val secondSlot = source
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 0)
      .sortByIntValueDesc
    val rowAboveMap = source
      .mapIntKey((key: Int) => key + 1)
      .filter((pair: (Int, Int)) => pair._2 > 0)
      .sortByIntValueDesc
    val slotAboveSecond = source
      .mapIntKey((key: Int) => key + 1)
      .mapIntKey((key: Int) => key + 1)
      .filterKeys((key: Int) => key > 0)
      .sortByIntValueDesc
    val secondMap = source
      .mapIntKey((key: Int) => key + 1)
      .mapIntKey((key: Int) => key + 1)
      .sortByIntValueDesc
    val projectUnderMap = source
      .map((pair: (Int, Int)) => (pair._1, pair._2))
      .mapIntKey((key: Int) => key + 1)
      .sortByIntValueDesc
    val rowAboveProject = source
      .map((pair: (Int, Int)) => (pair._1, pair._2))
      .filter((pair: (Int, Int)) => pair._2 > 0)
      .sortByIntValueDesc
    val secondAboveProject = source
      .map((pair: (Int, Int)) => (pair._1, pair._2))
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 0)
      .sortByIntValueDesc
    val longs = DistCollection
      .ofLongPairs(Array(3L, 1L), Array(30L, 10L), 1)
      .map((pair: (Long, Long)) => (pair._1.toInt, pair._2.toInt))
    val strings =
      DistCollection(Seq("b" -> 2, "a" -> 1), 1).map((pair: (String, Int)) => (pair._2, pair._2))
    val bad = source.map((pair: (Int, Int)) => stringKey(pair)).sortByIntKey

    ColumnarPlanner.lower(values.plan) shouldBe None
    ColumnarPlanner.lower(twice.plan).map(_.shape) shouldBe Some("Project(Project(Scan))")
    ColumnarPlanner.lower(twice.sortByIntKey.plan) shouldBe None
    ColumnarPlanner.lower(above.plan) shouldBe None
    ColumnarPlanner.lower(twiceDesc.plan) shouldBe None
    ColumnarPlanner.lower(aboveMap.plan) shouldBe None
    ColumnarPlanner.lower(twiceMapped.plan) shouldBe None
    ColumnarPlanner.lower(onProject.plan) shouldBe None
    ColumnarPlanner.lower(second.plan) shouldBe None
    ColumnarPlanner.lower(secondAbove.plan) shouldBe None
    ColumnarPlanner.lower(secondKey.plan) shouldBe None
    ColumnarPlanner.lower(rowAboveKey.plan) shouldBe None
    ColumnarPlanner.lower(secondSlot.plan) shouldBe None
    ColumnarPlanner.lower(rowAboveMap.plan) shouldBe None
    ColumnarPlanner.lower(slotAboveSecond.plan) shouldBe None
    ColumnarPlanner.lower(secondMap.plan) shouldBe None
    ColumnarPlanner.lower(projectUnderMap.plan) shouldBe None
    ColumnarPlanner.lower(rowAboveProject.plan) shouldBe None
    ColumnarPlanner.lower(secondAboveProject.plan) shouldBe None
    ColumnarPlanner.lower(longs.plan).map(_.shape) shouldBe Some("Project(Scan)")
    ColumnarPlanner.lower(longs.sortByIntKey.plan) shouldBe None
    ColumnarPlanner.lower(longs.sortByIntValueDesc.plan) shouldBe None
    ColumnarPlanner.lower(strings.plan).map(_.shape) shouldBe Some("Project(Scan)")
    ColumnarPlanner.lower(strings.sortByIntKey.plan) shouldBe None
    ColumnarPlanner.lower(strings.sortByIntValueDesc.plan) shouldBe None
    shape(bad) shouldBe "SortByIntKey(Project(Scan))"
    val lowered = ColumnarPlanner.lower(bad.plan).getOrElse(fail("expected a project sort"))
    a[ColumnarFallback] should be thrownBy {
      val _ = ColumnarInterpreter.interpret(bad.plan, lowered)
    }
    ColumnarPlanner.tryRun(bad.plan) shouldBe None
  }

  "sortAndSplitSelected" should "match sortAndSplit of the materialised prefix" in {
    val batch = BatchCodec.intPairs(Seq(4 -> 1, 1 -> 2, 4 -> 3, 2 -> 4, 1 -> 5))
    // Rows 0, 2, and 3. The key-1 row at index 1 is dropped.
    val selected = Selected(batch, Array(0, 2, 3), 3)
    val parts = RankSplit.sortAndSplitSelected(Vector(selected), partitions = 2, parallelism = 1)
    val dense = RankSplit.sortAndSplit(
      Vector(Selection.materialize(selected)),
      partitions = 2,
      parallelism = 1,
    )

    identical(parts, dense)
    val flat = parts.flatMap(batch => cells(batch))
    flat shouldBe Seq((Some(2), Some(4)), (Some(4), Some(1)), (Some(4), Some(3)))
  }

  it should "order a dirty null key as zero and keep the null" in {
    val keys = Int32Column.of(Array(7, 1, 4), Validity.pack(3, row => row != 0), 3)
    val values = Int32Column.of(Array(10, 20, 30), Validity.pack(3, row => row != 2), 3)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(keys, values),
      3,
    )
    val selected = Selected(batch, Array(0, 1, 2), 3)
    val parts = RankSplit.sortAndSplitSelected(Vector(selected), partitions = 1, parallelism = 1)
    val sorted = parts.headOption.getOrElse(fail("expected one partition"))

    cells(sorted) shouldBe Seq((None, Some(10)), (Some(1), Some(20)), (Some(4), None))
    val dense = RankSplit.sortAndSplit(
      Vector(Selection.materialize(selected)),
      partitions = 1,
      parallelism = 1,
    )
    identical(parts, dense)
  }

  it should "keep one hot key in survivor order and ignore a dropped row" in {
    val batch = BatchCodec.intPairs(Seq(7 -> 1, 7 -> 2, 7 -> 3, 7 -> 4))
    val selected = Selected(batch, Array(0, 2), 2)
    val parts = RankSplit.sortAndSplitSelected(Vector(selected), partitions = 1, parallelism = 1)

    parts.flatMap(one => cells(one)) shouldBe Seq((Some(7), Some(1)), (Some(7), Some(3)))
  }

  it should "publish empty int-pair batches for an empty selection" in {
    val batch = BatchCodec.intPairs(Seq(1 -> 10, 2 -> 20))
    val empty = Selected(batch, Array(0, 1), 0)
    val parts =
      RankSplit.sortAndSplitSelected(Vector(empty, empty), partitions = 3, parallelism = 1)

    parts.length shouldBe 3
    parts.foreach { one =>
      one.length shouldBe 0
      one.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int32)
    }
  }

  it should "sort a live prefix beside an empty selection" in {
    val live = BatchCodec.intPairs(Seq(3 -> 30, 1 -> 10))
    val dead = BatchCodec.intPairs(Seq(9 -> 90))
    val selected = Vector(Selected(live, Array(0, 1), 2), Selected(dead, Array(0), 0))
    val serial = RankSplit.sortAndSplitSelected(selected, partitions = 2, parallelism = 1)
    val parallel = RankSplit.sortAndSplitSelected(selected, partitions = 2, parallelism = 2)

    identical(parallel, serial)
    serial.flatMap(batch => cells(batch)) shouldBe Seq((Some(1), Some(10)), (Some(3), Some(30)))
  }

  it should "match the dense split when every row is selected" in {
    val left = BatchCodec.intPairs(Seq(5 -> 50, 1 -> 10))
    val right = BatchCodec.intPairs(Seq(1 -> 11, 0 -> 0))
    val selected = Vector(allRows(left), allRows(right))
    val viaSelection = RankSplit.sortAndSplitSelected(selected, partitions = 2, parallelism = 2)
    val viaDense = RankSplit.sortAndSplit(Vector(left, right), partitions = 2, parallelism = 1)

    identical(viaSelection, viaDense)
  }

  it should "reject an empty vector, a non-positive width, and a non-positive parallelism" in {
    val empty = BatchCodec.intPairs(Seq.empty[(Int, Int)])
    val selected = Vector(Selected(empty, Array.empty[Int], 0))
    val missing = the[IllegalArgumentException] thrownBy {
      RankSplit.sortAndSplitSelected(Vector.empty, partitions = 2, parallelism = 1)
    }
    val width = the[IllegalArgumentException] thrownBy {
      RankSplit.sortAndSplitSelected(selected, partitions = 0, parallelism = 1)
    }
    val workers = the[IllegalArgumentException] thrownBy {
      RankSplit.sortAndSplitSelected(selected, partitions = 1, parallelism = 0)
    }

    missing.getMessage should include("sortAndSplitSelected needs at least one batch")
    width.getMessage should include("numPartitions must be positive")
    workers.getMessage should include("parallelism must be positive")
  }

  "sortAndSplitSelected spill" should "match the in-memory split and write the survivor index" in {
    val batch = BatchCodec.intPairs(Seq(4 -> 1, 1 -> 2, 3 -> 3, 1 -> 4))
    val selected = Vector(Selected(batch, Array(0, 1, 3), 3))
    val memory = RankSplit.sortAndSplitSelected(selected, partitions = 2, parallelism = 1)
    val nullKeys = Int32Column.of(Array(4, 1, 3, 1), Validity.pack(4, row => row != 1), 4)
    val nullValues = Int32Column.of(Array(1, 2, 3, 4), Validity.pack(4, row => row != 3), 4)
    val nullBatch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(nullKeys, nullValues),
      4,
    )
    // Drop the present key 3. The null key orders as zero and stays null.
    val nullSelected = Vector(Selected(nullBatch, Array(0, 1, 3), 3))
    val nullMemory = RankSplit.sortAndSplitSelected(nullSelected, partitions = 2, parallelism = 1)

    withSpill(budget = 1L) { dir =>
      val spilled = RankSplitRuns.sortAndSplitSelected(
        selected,
        partitions = 2,
        parallelism = 2,
        ScarletConf.get,
      )
      identical(spilled, memory)
      RankSplitRuns.filesWrittenForTest should be > 0L
      SpillStore.listForTest(dir) shouldBe Vector.empty[Path]

      RankSplitRuns.resetForTest()
      val nullSpilled = RankSplitRuns.sortAndSplitSelected(
        nullSelected,
        partitions = 2,
        parallelism = 1,
        ScarletConf.get,
      )
      identical(nullSpilled, nullMemory)
      val nullFlat = nullSpilled.flatMap(one => cells(one))
      nullFlat should contain((None, Some(2)))
      nullFlat should contain((Some(1), None))
      nullFlat.exists { case (Some(3), Some(3)) => true; case _ => false } shouldBe false
      RankSplitRuns.filesWrittenForTest should be > 0L
    }
    RankSplitRuns.resetForTest()
    ScarletConf.set(ScarletConf.get.copy(spillEnabled = false, spillBudgetBytes = 0L))
    val stayed = RankSplitRuns.sortAndSplitSelected(
      selected,
      partitions = 2,
      parallelism = 1,
      ScarletConf.get,
    )
    identical(stayed, memory)
    RankSplitRuns.filesWrittenForTest shouldBe 0L
  }

  it should "match the in-memory split when the prefix fits the budget" in {
    val batch = BatchCodec.intPairs(Seq(4 -> 1, 1 -> 2, 3 -> 3, 1 -> 4))
    val selected = Vector(Selected(batch, Array(0, 1, 3), 3))
    val memory = RankSplit.sortAndSplitSelected(selected, partitions = 2, parallelism = 1)
    val nullKeys = Int32Column.of(Array(4, 1, 3, 1), Validity.pack(4, row => row != 1), 4)
    val nullValues = Int32Column.of(Array(1, 2, 3, 4), Validity.pack(4, row => row != 3), 4)
    val nullBatch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(nullKeys, nullValues),
      4,
    )
    val nullSelected = Vector(Selected(nullBatch, Array(0, 1, 3), 3))
    val nullMemory = RankSplit.sortAndSplitSelected(nullSelected, partitions = 2, parallelism = 1)

    withSpill(budget = 64L * 1024L * 1024L) { _ =>
      val stayed = RankSplitRuns.sortAndSplitSelected(
        selected,
        partitions = 2,
        parallelism = 2,
        ScarletConf.get,
      )
      identical(stayed, memory)
      RankSplitRuns.filesWrittenForTest shouldBe 0L

      RankSplitRuns.resetForTest()
      val nullStayed = RankSplitRuns.sortAndSplitSelected(
        nullSelected,
        partitions = 2,
        parallelism = 1,
        ScarletConf.get,
      )
      identical(nullStayed, nullMemory)
      RankSplitRuns.filesWrittenForTest shouldBe 0L
    }
  }

  private def pairs: DistCollection[(Int, Int)] =
    DistCollection.ofIntPairs(keys, values, 2)

  private def keepAll(source: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    source.filterKeys((_: Int) => true).filterValues((_: Int) => true)

  private def shape(data: DistCollection[_]): String =
    ColumnarPlanner.lower(data.plan).map(_.shape).getOrElse(fail("expected a columnar plan"))

  private def assertFewerBatches(
      sorted: DistCollection[(Int, Int)],
      filtered: DistCollection[(Int, Int)],
      source: DistCollection[(Int, Int)],
  ): Unit =
    val (compactPublishes, dense, selectedCount) = batchGap(sorted, filtered, source)
    compactPublishes should be > 0L
    (dense - selectedCount) shouldBe compactPublishes

  /**
   * `dense` compacts the filter, then rank-splits. `selectedCount` is the sort collect. The two
   * differ by the compact when the sort itself consumes the survivor index.
   */
  private def batchGap(
      sorted: DistCollection[(Int, Int)],
      filtered: DistCollection[(Int, Int)],
      source: DistCollection[(Int, Int)],
  ): (Long, Long, Long) =
    val encode = published {
      val scan = ColumnarPlanner.lower(source.plan).getOrElse(fail("expected a scan"))
      ColumnarInterpreter.interpret(source.plan, scan)
    }
    val selectedCount = published {
      ScarletRuntime.get.shuffle.clear()
      sorted.collect()
    }
    val (afterFilter, dense) = ColumnMetrics.withSink(new CountingSink) {
      val op = ColumnarPlanner.lower(filtered.plan).getOrElse(fail("expected a filter"))
      val columns = ColumnarInterpreter.interpret(filtered.plan, op)
      val mid = ColumnMetrics.batchCount
      val _ = RankSplitRuns.sortAndSplit(
        ColumnarInterpreter.pairsOf(columns),
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ScarletConf.get,
      )
      (mid, ColumnMetrics.batchCount)
    }
    (afterFilter - encode, dense, selectedCount)

  private def denseSorted(filtered: DistCollection[(Int, Int)]): Vector[ColumnBatch] =
    val op = ColumnarPlanner.lower(filtered.plan).getOrElse(fail("expected a filter"))
    val batches = ColumnarInterpreter.pairsOf(ColumnarInterpreter.interpret(filtered.plan, op))
    RankSplitRuns.sortAndSplit(
      batches,
      ColumnarPlanner.shuffleWidth,
      ColumnarPlanner.parallelism,
      ScarletConf.get,
    )

  private def sortedParts(plan: Plan[(Int, Int)]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered sort"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.Pairs(parts) => parts
      case other => fail(s"expected int pairs, got $other")

  private def identical(actual: Vector[ColumnBatch], expected: Vector[ColumnBatch]): Unit =
    actual.length shouldBe expected.length
    actual.zip(expected).foreach { (left, right) =>
      left.length shouldBe right.length
      left.schema shouldBe right.schema
      columnImage(left, 0) shouldBe columnImage(right, 0)
      columnImage(left, 1) shouldBe columnImage(right, 1)
    }

  private def columnImage(batch: ColumnBatch, ordinal: Int): (Seq[Int], Seq[Boolean]) =
    val column = int32(batch, ordinal)
    val stored = (0 until batch.length).map(row => column.values(row))
    val valid = (0 until batch.length).map(row => column.isValid(row))
    (stored, valid)

  private def cells(batch: ColumnBatch): Seq[(Option[Int], Option[Int])] =
    val keyColumn = int32(batch, 0)
    val valueColumn = int32(batch, 1)
    (0 until batch.length).map { row =>
      val key = if (keyColumn.isValid(row)) then Some(keyColumn.values(row)) else None
      val value = if (valueColumn.isValid(row)) then Some(valueColumn.values(row)) else None
      (key, value)
    }

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  private def allRows(batch: ColumnBatch): Selected =
    Selected(batch, Array.tabulate(batch.length)(row => row), batch.length)

  private def decoded(parts: Vector[ColumnBatch]): Seq[(Int, Int)] =
    parts.flatMap(batch => BatchCodec.decodeIntPairs(batch))

  private def published[A](body: => A): Long =
    ColumnMetrics.withSink(new CountingSink) {
      val _ = body
      ColumnMetrics.batchCount
    }

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

  private def withSpill(budget: Long)(body: Path => Unit): Unit =
    val dir = Files.createTempDirectory("scarlet-selection-sort")
    val saved = ScarletConf.get
    RankSplitRuns.resetForTest()
    ScarletConf.set(
      saved.copy(
        spillEnabled = true,
        spillBudgetBytes = budget,
        spillDirectory = dir.toString,
        spillFanIn = 2,
      ),
    )
    try body(dir)
    finally
      ScarletConf.set(saved)
      RankSplitRuns.resetForTest()
      SpillStore.listForTest(dir).foreach { path =>
        val _ = Files.deleteIfExists(path)
      }
      val _ = Files.deleteIfExists(dir)

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

  /** A string key in an int-pair slot. The pair project declines the cell. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def stringKey(pair: (Int, Int)): (Int, Int) =
    Tuple2(pair._1.toString, java.lang.Integer.valueOf(pair._2)).asInstanceOf[(Int, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
  private def shown(rows: Seq[(Int, Int)]): Seq[String] =
    rows.map { row =>
      val product = row.asInstanceOf[Product]
      String.valueOf(product.productElement(0)) + ":" + String.valueOf(product.productElement(1))
    }

  private def effectiveKeys(rows: Seq[(Int, Int)]): Seq[Int] =
    rows.map(pair => if (present(pair, 0)) storedInt(pair, 0) else 0)

  private def present(pair: (Int, Int), ordinal: Int): Boolean =
    pair.productElement(ordinal) match
      case _: java.lang.Integer => true
      case _ => false

  private def storedInt(pair: (Int, Int), ordinal: Int): Int =
    pair.productElement(ordinal) match
      case key: java.lang.Integer => key.intValue()
      case other => fail(s"expected an int component, found $other")

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def noInt: java.lang.Integer = null

  private def boxedInt(value: Int): java.lang.Integer = java.lang.Integer.valueOf(value)
