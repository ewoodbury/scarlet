package com.ewoodbury.scarlet.execution
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}

/**
 * Int `sortByIntKey` over a filter selection.
 *
 * A filter stack keeps the survivor index, including one slot filter. The rank split sorts those
 * rows and still publishes a compacted sorted batch. A null key orders as zero and stays null.
 * Ties keep input order. A scan still sorts on the dense path. Over a tiny spill budget the
 * survivor index is spilled. Dropped rows are not written. Spill off writes nothing.
 */
class TestSelectionSort extends SelectionSortSupport:
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
      ) shouldBe Option.empty[Vector[Selected]]
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
      ) shouldBe Option.empty[Vector[Selected]]
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
      ) shouldBe Option.empty[Vector[Selected]]
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
      ) shouldBe Option.empty[Vector[Selected]]
    MapIntKey.resetPublishedForTest()
    RankSplit.resetForTest()
    collect(mappedKey) shouldBe Seq(3 -> 20, 4 -> 30)
    MapIntKey.publishedForTest should be > 0
    RankSplit.mergePassesForTest should be > 0
  }
