package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{ExecutionService, Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Row and columnar `countByKey`. Equality is by key, because the two engines do not share
 * first-seen order. A null key is dropped. A null value still counts.
 */
class TestCountByKey extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = 8,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
      ),
    )
    ScarletRuntime.get.shuffle.clear()
    FoldedCount.resetPassesForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "countByKey" should "match the row path for an empty source" in {
    assertInt(DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 1), Map.empty)
    val wide = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 4).countByKey
    assertColumnar(wide.plan, "CountByKey(Exchange(Scan))")
    val parts = ExecutionService.get.executePartitions(wide.plan)
    parts.size shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(_.data shouldBe empty)
    countsOf(wide.collect()) shouldBe Map.empty[Int, Int]
  }

  it should "match the row path for duplicate keys in one partition" in {
    assertInt(
      DistCollection
        .ofIntPairs(Array(5, 5, 6, Int.MinValue, Int.MinValue), Array(1, 2, 3, 4, 5), 1),
      Map(5 -> 2, 6 -> 1, Int.MinValue -> 2),
    )
  }

  it should "match the row path across four partitions, including a key on two of them" in {
    // Contiguous split of four rows puts key 5 on the first and third partitions.
    assertInt(
      DistCollection.ofIntPairs(Array(5, 6, 5, 7), Array(1, 2, 3, 4), 4),
      Map(5 -> 2, 6 -> 1, 7 -> 1),
    )
  }

  it should "match the row path for a key seen once" in {
    assertInt(
      DistCollection.ofIntPairs(Array(1, 2, 3, 4), Array(10, 20, 30, 40), 4),
      Map(1 -> 1, 2 -> 1, 3 -> 1, 4 -> 1),
    )
  }

  it should "count a key that arrives on both input partitions" in {
    // Length 3 and two partitions: the first partition holds (5, 6) and the second holds 5.
    assertInt(
      DistCollection.ofIntPairs(Array(5, 6, 5), Array(1, 2, 3), 2),
      Map(5 -> 2, 6 -> 1),
    )
  }

  it should "drop null keys on both engines" in {
    val source = DistCollection(Plan.Source(Seq(Partition(Seq(nullKey(1), nullKey(2))))))
    val counted = source.countByKey
    ColumnarPlanner.lower(counted.plan) shouldBe None
    countsOf(counted.collect()) shouldBe Map.empty[String, Int]
    countsOf(row(counted.collect())) shouldBe Map.empty[String, Int]
  }

  it should "count a null value without reading it" in {
    val source = DistCollection(
      Plan.Source(Seq(Partition(Seq(nullValue(1), (1, "kept"), nullValue(2))))),
    )
    val counted = source.countByKey
    ColumnarPlanner.lower(counted.plan) shouldBe None
    countsOf(counted.collect()) shouldBe Map(1 -> 2, 2 -> 1)
    countsOf(row(counted.collect())) shouldBe Map(1 -> 2, 2 -> 1)
  }

  it should "count a boxed null value that arrives after the first row" in {
    lateNullCounts() shouldBe Map(1 -> 2, 2 -> 1)
    lateNullRowCounts() shouldBe Map(1 -> 2, 2 -> 1)
  }

  it should "count a null value under one key filter" in {
    lateNullOneFilterCounts() shouldBe Map(1 -> 2)
  }

  it should "lower two filters and match the row path" in {
    val source = DistCollection.ofIntPairs(Array(1, 2, 3, 1, 3), Array(1, -1, 4, 5, 0), 4)
    val counted = source
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .countByKey
    assertColumnar(counted.plan, "CountByKey(Exchange(FilterValue(FilterKey(Scan))))")
    val found = countsOf(counted.collect())
    found shouldBe countsOf(row(counted.collect()))
    found shouldBe Map(1 -> 2)
  }

  it should "fold one filter and a two-filter stack in the hash path's key order" in {
    val source = DistCollection.ofIntPairs(
      Array(5, 5, 6, 1, 5, 7, 6, 1, 8, 5),
      Array(1, -2, 3, 4, 0, -5, 6, 7, 8, 9),
      2,
    )
    assertFolded(
      source.filterKeys((key: Int) => key % 2 != 0),
      Map(5 -> 4, 1 -> 2, 7 -> 1),
    )
    assertFolded(
      source
        .filterKeys((key: Int) => key % 2 != 0)
        .filterValues((value: Int) => value >= 0),
      Map(5 -> 3, 1 -> 2),
    )
  }

  it should "fold duplicate keys, both partitions, nulls, and an empty source" in {
    val duplicates = DistCollection.ofIntPairs(Array(4, 4, 4, 9), Array(1, 2, 3, 4), 1)
    assertFolded(duplicates.filterValues((value: Int) => value > 0), Map(4 -> 3, 9 -> 1))

    /* Length 3 and two partitions: key 5 is on both, and the batch size cuts each side into
     * morsels. */
    val both = DistCollection.ofIntPairs(Array(5, 6, 5, 6, 7, 5), Array(1, -1, 2, 3, 0, 4), 2)
    assertFolded(
      both.filterKeys((key: Int) => key != 7).filterValues((value: Int) => value >= 0),
      Map(5 -> 3, 6 -> 1),
    )

    val empty = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 1)
    assertFolded(empty.filterKeys((key: Int) => key > 0), Map.empty)

    FoldedCount.resetPassesForTest()
    // tryRun and collect each count the same int-slot filter, so the fold is entered twice.
    lateNullOneFilterCounts() shouldBe Map(1 -> 2)
    FoldedCount.passesForTest shouldBe 2

    nullKeyCounts() shouldBe Map(1 -> 2, 2 -> 1)
  }

  it should "choose the folded path only for an int slot stack" in {
    val ints = DistCollection.ofIntPairs(Array(1, 2, 3, 1, 3), Array(1, -1, 4, 5, 0), 2)
    val two = ints
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .countByKey
    foldsOf(two.plan) shouldBe true
    FoldedCount.resetPassesForTest()
    countsOf(two.collect()) shouldBe Map(1 -> 2)
    FoldedCount.passesForTest shouldBe 1

    val plain = ints.countByKey
    foldsOf(plain.plan) shouldBe false
    FoldedCount.resetPassesForTest()
    plain.collect()
    FoldedCount.passesForTest shouldBe 0

    val strings = DistCollection(
      Plan.Source(Seq(Partition(Seq("a" -> 1, "b" -> 2)), Partition(Seq("a" -> 3)))),
    )
    val wholeRow = strings.filter((pair: (String, Int)) => pair._1 != "b").countByKey
    foldsOf(wholeRow.plan) shouldBe false
    FoldedCount.resetPassesForTest()
    countsOf(wholeRow.collect()) shouldBe Map("a" -> 2)
    FoldedCount.passesForTest shouldBe 0

    // A value filter above mapValues sees the mapped value, so the fold must not read the source.
    val mapped = ints
      .mapValues((value: Int) => value + 1)
      .filterValues((value: Int) => value > 0)
      .countByKey
    foldsOf(mapped.plan) shouldBe false
    FoldedCount.resetPassesForTest()
    countsOf(mapped.collect()) shouldBe Map(1 -> 2, 3 -> 2)
    countsOf(mapped.collect()) shouldBe countsOf(row(mapped.collect()))
    FoldedCount.passesForTest shouldBe 0
  }

  it should "fold one mapValues and still match the row path" in {
    val calls = new java.util.concurrent.atomic.AtomicInteger
    val plus: Int => Int = (value: Int) => {
      val _ = calls.incrementAndGet()
      value + 1
    }

    List(1, 4).foreach { parts =>
      val source = DistCollection.ofIntPairs(Array(5, 5, 6, 7, 5), Array(1, 2, 3, 4, 5), parts)
      val once = source.mapValues(plus).countByKey
      foldsOf(once.plan) shouldBe true
      calls.set(0)
      FoldedCount.resetPassesForTest()
      countsOf(once.collect()) shouldBe Map(5 -> 3, 6 -> 1, 7 -> 1)
      calls.get shouldBe 0
      FoldedCount.passesForTest shouldBe 1
      countsOf(once.collect()) shouldBe countsOf(row(once.collect()))

      val twice = source.mapValues(plus).mapValues(plus).countByKey
      foldsOf(twice.plan) shouldBe false
      FoldedCount.resetPassesForTest()
      countsOf(twice.collect()) shouldBe Map(5 -> 3, 6 -> 1, 7 -> 1)
      FoldedCount.passesForTest shouldBe 0
      countsOf(twice.collect()) shouldBe countsOf(row(twice.collect()))

      val keyed = source.mapValues(plus).filterKeys((key: Int) => key != 6).countByKey
      foldsOf(keyed.plan) shouldBe true
      calls.set(0)
      FoldedCount.resetPassesForTest()
      countsOf(keyed.collect()) shouldBe Map(5 -> 3, 7 -> 1)
      calls.get shouldBe 0
      FoldedCount.passesForTest shouldBe 1
    }

    val duplicates = DistCollection.ofIntPairs(Array(4, 4, 4, 9), Array(1, 2, 3, 4), 1)
    val folded = duplicates.mapValues((value: Int) => value + 1).countByKey
    foldsOf(folded.plan) shouldBe true
    FoldedCount.resetPassesForTest()
    countsOf(folded.collect()) shouldBe Map(4 -> 3, 9 -> 1)
    FoldedCount.passesForTest shouldBe 1

    val under = DistCollection
      .ofIntPairs(Array(1, 1, 2), Array(0, 5, -1), 1)
      .filterValues((value: Int) => value > 0)
      .mapValues(plus)
      .countByKey
    foldsOf(under.plan) shouldBe true
    calls.set(0)
    FoldedCount.resetPassesForTest()
    countsOf(under.collect()) shouldBe Map(1 -> 1)
    calls.get shouldBe 0
    FoldedCount.passesForTest shouldBe 1
    countsOf(under.collect()) shouldBe countsOf(row(under.collect()))

    val nullable = nullIntKeySource().mapValues(plus).countByKey
    foldsOf(nullable.plan) shouldBe true
    calls.set(0)
    FoldedCount.resetPassesForTest()
    countsOf(nullable.collect()) shouldBe Map(1 -> 2, 2 -> 2)
    calls.get shouldBe 0
    FoldedCount.passesForTest shouldBe 1
    countsOf(nullable.collect()) shouldBe countsOf(row(nullable.collect()))
  }

  it should "keep a key-changing map on the row path" in {
    val source = DistCollection.ofIntPairs(Array(1, 2, 1, 3), Array(10, 20, 30, 40), 4)
    val changed = source.map((pair: (Int, Int)) => (pair._1 + 1, pair._2)).countByKey
    ColumnarPlanner.partitioning(changed.plan) shouldBe None
    countsOf(changed.collect()) shouldBe Map(2 -> 2, 3 -> 1, 4 -> 1)
    countsOf(changed.collect()) shouldBe countsOf(row(changed.collect()))

    // The plan node is MapOp. A function that happens to keep the key is still not folded.
    val sameKey = source.map((pair: (Int, Int)) => (pair._1, pair._2 + 1)).countByKey
    ColumnarPlanner.partitioning(sameKey.plan) shouldBe None
    foldsOf(sameKey.plan) shouldBe false
    countsOf(sameKey.collect()) shouldBe Map(1 -> 2, 2 -> 1, 3 -> 1)
    countsOf(sameKey.collect()) shouldBe countsOf(row(sameKey.collect()))
  }

  it should "pin the filter-countbykey checksum" in {
    val rows = 1000000
    val groups = 1024
    val values = benchValues(rows)
    val keys = Array.tabulate(rows)(row => row % groups)
    FilterCountChecksum.expected(keys, values, groups) shouldBe 10821571L
    ScarletConf.set(
      ScarletConf.get
        .copy(columnarBatchSize = 8192, threadPoolSize = 4, defaultShufflePartitions = 4),
    )
    val counted = DistCollection
      .ofIntPairs(keys, values, 2)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .countByKey
    foldsOf(counted.plan) shouldBe true
    FoldedCount.resetPassesForTest()
    FilterCountChecksum.collected(counted.collect(), groups) shouldBe 10821571L
    FoldedCount.passesForTest shouldBe 1
  }

  "FoldedCount" should "match the survivor index, including nulls, and refuse what it cannot walk" in {
    val keepKey: ColumnKernel.Int32Predicate = key => key % 2 != 0
    val keepValue: ColumnKernel.Int32Predicate = value => value >= 0
    val one = List(Selection.Step.Int32(0, keepKey))
    val two = List(Selection.Step.Int32(0, keepKey), Selection.Step.Int32(1, keepValue))
    val three = List(
      Selection.Step.Int32(0, keepKey),
      Selection.Step.Int32(1, keepValue),
      Selection.Step.Int32(0, keepKey),
    )
    val batch = intPairs(
      Array(1, 2, 3, 1, 4, 5, 1, 7, 8, 9),
      Array(1, -1, 2, 3, 0, -4, 5, 6, 7, -2),
      keyPresent = row => row != 2,
      valuePresent = row => row != 6,
    )
    List(one, two, three).foreach { steps =>
      assertSameOrder(Vector(batch), steps, partitions = 2, batchSize = 4)
    }

    val valueOnly = List(Selection.Step.Int32(1, keepValue))
    assertSameOrder(Vector(batch), valueOnly, partitions = 1, batchSize = 4)
    kernelCounts(folded(Vector(batch), valueOnly, 1, 4)) shouldBe Map(
      1 -> 2,
      4 -> 1,
      7 -> 1,
      8 -> 1,
    )

    FoldedCount.admits(Nil) shouldBe false
    FoldedCount.admitsUnmapped(Nil) shouldBe true
    FoldedCount.count(Vector(batch), Nil, 1, 1, 8) shouldBe None
    val unmapped = FoldedCount.countUnmapped(Vector(batch), Nil, 2, 1, 4)
    kernelCounts(unmapped.getOrElse(fail("unmapped count declined"))) shouldBe kernelCounts(
      HashCount.byKey(Vector(batch), 2, 1, 4),
    )
    val wholeRow: PairKernel.IntPairPredicate = (key, value) => key > value
    val rowStep = List(Selection.Step.IntPair(wholeRow))
    FoldedCount.admits(rowStep) shouldBe false
    FoldedCount.count(Vector(batch), rowStep, 1, 1, 8) shouldBe None

    val empty =
      FoldedCount.count(Vector.empty, one, numPartitions = 4, parallelism = 1, batchSize = 8)
    empty.map(_.length) shouldBe Some(4)
    empty.map(_.map(_.length)) shouldBe Some(Seq(0, 0, 0, 0))

    ScarletConf.set(
      ScarletConf.get
        .copy(spillEnabled = false, spillBudgetBytes = FoldedCount.chargedBytes(4) - 1L),
    )
    FoldedCount.count(Vector(batch), one, 1, 1, 8) shouldBe None
    ScarletConf.set(
      ScarletConf.get.copy(
        spillEnabled = false,
        spillBudgetBytes = FoldedCount.chargedBytes(batch.length.toLong),
      ),
    )
    FoldedCount.count(Vector(batch), one, 1, 1, 8).nonEmpty shouldBe true
  }

  it should "keep the index path when survivors fit and the source estimate does not" in {
    val rows = 20
    val keys = Array.tabulate(rows)(row => row)
    val values = Array.tabulate(rows)(row => if ((row & 1) == 0) then 1 else -1)
    val counted = DistCollection
      .ofIntPairs(keys, values, 1)
      .filterValues((value: Int) => value >= 0)
      .countByKey
    val survivors = rows / 2
    ScarletConf.set(
      ScarletConf.get.copy(
        spillEnabled = false,
        spillBudgetBytes = FoldedCount.chargedBytes(survivors.toLong),
      ),
    )
    FoldedCount.resetPassesForTest()
    countsOf(counted.collect()) shouldBe (0 until rows by 2).map(key => key -> 1).toMap
    FoldedCount.passesForTest shouldBe 0
    countsOf(row(counted.collect())) shouldBe (0 until rows by 2).map(key => key -> 1).toMap
  }

  it should "lower string keys and match the row path" in {
    val strings = DistCollection(
      Plan.Source(Seq(Partition(Seq("a" -> 1, "b" -> 2)), Partition(Seq("a" -> 3)))),
    )
    assertString(strings, Map("a" -> 2, "b" -> 1))
  }

  it should "drop a null string key, keep an empty key, and count a null int" in {
    val source = DistCollection(
      Seq(
        stringPair("a", java.lang.Integer.valueOf(1)),
        nullKey(9),
        stringPair("a", absentInt),
        stringPair("", java.lang.Integer.valueOf(4)),
        stringPair("", java.lang.Integer.valueOf(5)),
      ),
      2,
    )
    assertString(source, Map("a" -> 2, "" -> 2))
  }

  it should "lower a string-pair filter stack and match the row path" in {
    val source = DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq("a" -> 1, "b" -> -1, "a" -> 4, "c" -> 0)),
          Partition(Seq("b" -> 3, "a" -> 2)),
        ),
      ),
    )
    val counted = source
      .filterKeys((key: String) => key != "b")
      .filterValues((value: Int) => value > 0)
      .countByKey
    val lowered = ColumnarPlanner.lower(counted.plan)
    lowered.map(_.shape) shouldBe Some("CountByKey(Exchange(FilterValue(FilterKey(Scan))))")
    lowered.map(_.kind) shouldBe Some(PhysicalOp.Kind.StringPairs)
    val found = countsOf(counted.collect())
    found shouldBe countsOf(row(counted.collect()))
    found shouldBe Map("a" -> 3)
  }

  it should "lower long keys and keep hash collisions distinct" in {
    val wide = 1L << 32
    java.lang.Long.hashCode(1L) shouldBe java.lang.Long.hashCode(wide)
    assertLong(
      DistCollection.ofLongPairs(Array(1L, 1L, 2L, wide), Array(9L, 8L, 7L, 1L), 2),
      Map(1L -> 2, 2L -> 1, wide -> 1),
    )
  }

  it should "count a null long value and drop a null long key" in {
    val source = DistCollection(
      Seq(
        longPair(longRef(1L), longRef(2L)),
        longPair(longRef(1L), absent),
        longPair(absent, longRef(3L)),
        longPair(longRef(4L), longRef(5L)),
      ),
      2,
    )
    assertLong(source, Map(1L -> 2, 4L -> 1))
  }

  it should "lower a long-pair filter stack and match the row path" in {
    val source = DistCollection.ofLongPairs(
      Array(1L, -2L, 3L, 1L, 4L),
      Array(1L, 8L, -4L, 5L, 0L),
      4,
    )
    val counted = source
      .filterKeys((key: Long) => key > 0L)
      .filterValues((value: Long) => value >= 0L)
      .countByKey
    val lowered = ColumnarPlanner.lower(counted.plan)
    lowered.map(_.shape) shouldBe Some("CountByKey(Exchange(FilterValue(FilterKey(Scan))))")
    lowered.map(_.kind) shouldBe Some(PhysicalOp.Kind.LongCounts)
    val found = counted.collect().toList
    found should contain theSameElementsAs row(counted.collect().toList)
    countsOf(found) shouldBe Map(1L -> 2, 4L -> 1)
  }

  it should "lower an empty long-pair source" in {
    assertLong(
      DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], 1),
      Map.empty,
    )
    val wide = DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], 4).countByKey
    val lowered = ColumnarPlanner.lower(wide.plan)
    lowered.map(_.shape) shouldBe Some("CountByKey(Exchange(Scan))")
    lowered.map(_.kind) shouldBe Some(PhysicalOp.Kind.LongCounts)
    val parts = ExecutionService.get.executePartitions(wide.plan)
    parts.size shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(_.data shouldBe empty)
    countsOf(wide.collect()) shouldBe Map.empty[Long, Int]
  }

  it should "hash-shuffle, and bypass that shuffle when the keys are already hashed" in {
    val pairs = DistCollection.ofIntPairs(Array(1, 1, 2), Array(3, 4, 5), 1).plan
    val width = ScarletConf.get.defaultShufflePartitions
    val direct = StageBuilder.buildStageGraph(Plan.CountByKeyOp(pairs))
    direct.stages.size shouldBe 2
    val shuffled = direct.stages(direct.finalStageId)
    shuffled.isShuffleStage shouldBe true
    shuffled.wideOp shouldBe Some(
      CountByKeyWideOp(SimpleWideOpMeta(kind = WideOpKind.GroupByKey, numPartitions = width)),
    )

    val bypassed = StageBuilder.buildStageGraph(
      Plan.CountByKeyOp(Plan.PartitionByOp(pairs, width)),
    )
    bypassed.stages.size shouldBe 3
    bypassed.stages(bypassed.finalStageId).isShuffleStage shouldBe false
  }

  "HashCount" should "sum partial counts instead of counting partial rows" in {
    val batch = intPairs(Array(1, 1, 1), Array(7, 8, 9))
    kernelCounts(HashCount.byKey(Vector(batch), 2, 1, 8)) shouldBe Map(1 -> 3)
    kernelCounts(HashCount.byKey(Vector(batch), 2, 1, 2)) shouldBe Map(1 -> 3)
  }

  it should "count a key column and ignore a missing value column" in {
    val keysOnly = new ColumnBatch(
      Vector(LogicalType.Int32),
      Vector(Int32Column.of(Array(1, 1, 2), Validity.allValid(3), 3)),
      3,
    )
    kernelCounts(Vector(HashCount.int32(keysOnly))) shouldBe Map(1 -> 2, 2 -> 1)
  }

  it should "drop a null key and still count a null value" in {
    val missingKeys = intPairs(Array(1, 2, 3), Array(4, 5, 6), keyPresent = _ => false)
    val dropped = HashCount.byKey(Vector(missingKeys), 4, 1, 8)
    dropped.length shouldBe 4
    kernelCounts(dropped) shouldBe Map.empty[Int, Int]

    val missingValues = intPairs(Array(4, 4, 5), Array(1, 2, 3), valuePresent = _ => false)
    kernelCounts(HashCount.byKey(Vector(missingValues), 2, 1, 8)) shouldBe Map(4 -> 2, 5 -> 1)
  }

  it should "count a survivor index and skip a null key inside it" in {
    val batch = intPairs(Array(1, 2, 1, 1), Array(0, 0, 0, 0))
    val selected = Selected(batch, Array(0, 2, 3), 3)
    kernelCounts(HashCount.byKeySelected(Vector(selected), 2, 1, 2)) shouldBe Map(1 -> 3)

    val nullable = intPairs(Array(1, 2, 3), Array(0, 0, 0), keyPresent = row => row != 1)
    val survivors = Selected(nullable, Array(0, 1, 2), 3)
    kernelCounts(HashCount.byKeySelected(Vector(survivors), 2, 1, 8)) shouldBe Map(1 -> 1, 3 -> 1)
  }

  it should "publish empty batches for an empty input without consulting the budget" in {
    ScarletConf.set(ScarletConf.get.copy(spillBudgetBytes = 0L, spillEnabled = false))
    val counted = HashCount.byKey(Vector.empty, 4, 1, 8)
    counted.length shouldBe 4
    kernelCounts(counted) shouldBe Map.empty[Int, Int]
  }

  it should "refuse when a one-row table does not fit, spill on or off" in {
    val batch = intPairs(Array(1, 2, 1), Array(4, 5, 6))
    classOf[ClassCastException].isAssignableFrom(classOf[ClosedKernelRefusal]) shouldBe false
    List(false, true).foreach { enabled =>
      ScarletConf.set(ScarletConf.get.copy(spillEnabled = enabled, spillBudgetBytes = 1L))
      val refused = the[ClosedKernelRefusal] thrownBy HashCount.byKey(Vector(batch), 2, 1, 8)
      refused.rows shouldBe 3L
      refused.bytes should be > refused.budget
      refused.budget shouldBe 1L
      refused.getMessage should include(s"${refused.rows} input rows")
      refused.getMessage should include(s"${refused.bytes} bytes")
      refused.getMessage should include(s"${refused.budget} byte budget")
    }
  }

  it should "refuse an over-budget survivor index before the table exists" in {
    val batch = intPairs(Array(1, 2, 1, 4), Array(0, 0, 0, 0))
    val selected = Selected(batch, Array(0, 2), 2)
    List(false, true).foreach { enabled =>
      ScarletConf.set(ScarletConf.get.copy(spillEnabled = enabled, spillBudgetBytes = 1L))
      val refused =
        the[ClosedKernelRefusal] thrownBy HashCount.byKeySelected(Vector(selected), 2, 1, 8)
      refused.rows shouldBe 2L
      refused.budget shouldBe 1L
      refused.bytes should be > refused.budget
    }
  }

  it should "collect an empty countByKey when the budget is zero" in {
    ScarletConf.set(
      ScarletConf.get.copy(columnarExecution = true, spillBudgetBytes = 0L, spillEnabled = false),
    )
    ScarletRuntime.get.shuffle.clear()
    val source = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 1)
    countsOf(source.countByKey.collect()) shouldBe Map.empty[Int, Int]
  }

  it should "fail the action when the columnar budget refuses" in {
    val source = DistCollection.ofIntPairs(Array(1, 1, 2), Array(4, 5, 6), 2)
    List(false, true).foreach { enabled =>
      ScarletConf.set(
        ScarletConf.get
          .copy(columnarExecution = true, spillEnabled = enabled, spillBudgetBytes = 1L),
      )
      ScarletRuntime.get.shuffle.clear()
      // The row path would count these keys. The action must fail before that map is produced.
      val refused = the[ClosedKernelRefusal] thrownBy source.countByKey.collect()
      refused.rows shouldBe 3L
      refused.budget shouldBe 1L
      refused.bytes should be > refused.budget
    }
  }

  private def assertFolded(
      filtered: DistCollection[(Int, Int)],
      expected: Map[Int, Int],
  ): Unit =
    val counted = filtered.countByKey
    foldsOf(counted.plan) shouldBe true
    FoldedCount.resetPassesForTest()
    val found = counted.collect().toList
    FoldedCount.passesForTest shouldBe 1
    found shouldBe hashCounted(counted)
    countsOf(found) shouldBe expected
    countsOf(found) shouldBe countsOf(row(counted.collect()))

  private def foldsOf(plan: Plan[_]): Boolean =
    ColumnarPlanner.lower(plan) match
      case Some(op) =>
        (plan, op) match
          case (
                Plan.CountByKeyOp(source),
                PhysicalOp.CountByKey(PhysicalOp.Exchange(child)),
              ) =>
            CountByKeyArms.folds(source, child, ColumnUse.ordinals(plan, op))
          case _ => false
      case None => false

  private def hashCounted(counted: DistCollection[(Int, Int)]): List[(Int, Int)] =
    val plan = counted.plan
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("countByKey did not lower"))
    val (source, child) = (plan, op) match
      case (
            Plan.CountByKeyOp(source),
            PhysicalOp.CountByKey(PhysicalOp.Exchange(child)),
          ) =>
        (source, child)
      case _ => fail(s"countByKey lowered to ${op.shape}")
    val ordinals = ColumnUse.ordinals(plan, op)
    val selected = FilterChain
      .selected(source, child, None, preserveAbsence = false, ordinals)
      .getOrElse(fail("filter stack did not select"))
    val conf = ScarletConf.get
    decoded(
      HashCount.byKeySelected(
        selected,
        conf.defaultShufflePartitions,
        conf.threadPoolSize,
        conf.columnarBatchSize,
      ),
    )

  private def assertSameOrder(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      partitions: Int,
      batchSize: Int,
  ): Unit =
    val viaFold = folded(batches, steps, partitions, batchSize)
    val selected = batches.map(batch => assembled(batch, steps, batchSize))
    val viaHash = HashCount.byKeySelected(selected, partitions, parallelism = 2, batchSize)
    decoded(viaFold) shouldBe decoded(viaHash)

  private def folded(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      partitions: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    FoldedCount
      .count(batches, steps, partitions, parallelism = 2, batchSize)
      .getOrElse(fail("folded count declined"))

  private def assembled(
      batch: ColumnBatch,
      steps: List[Selection.Step],
      batchSize: Int,
  ): Selected =
    val morsels = Morsels.of(batch, batchSize).map { morsel =>
      Selection.applySelected(morsel.batch, morsel.start, morsel.length, steps)
    }
    val rows = new Array[Int](batch.length)
    val total = morsels.foldLeft(0) { (written, morsel) =>
      System.arraycopy(morsel.rows, 0, rows, written, morsel.count)
      written + morsel.count
    }
    Selected(batch, rows, total)

  private def decoded(batches: Vector[ColumnBatch]): List[(Int, Int)] =
    batches.flatMap(batch => BatchCodec.decodeIntPairs(batch)).toList

  /**
   * The `QueryBench` generator. The pinned checksum is `(key * 31) xor count`, folded in key
   * order.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def benchValues(rows: Int): Array[Int] =
    val values = new Array[Int](rows)
    var index = 0
    var state = 0x12345678
    while (index < rows) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    values

  private def nonNegative(value: Any): Boolean = value match
    case found: Int => found >= 0
    case _ => false

  /**
   * Named erasure boundary. A present pair witnesses the column, and a later null key stays
   * absent. `countByKey` drops that key.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullKeySource(): DistCollection[(Any, Any)] =
    def cell(key: Any, value: Any): (Any, Any) = (key, value)
    DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq(cell(1, 4), cell(null, 9), cell(1, 5), cell(2, -1))),
          Partition(Seq(cell(2, 6), cell(null, 8))),
        ),
      ),
    )

  /**
   * A present pair witnesses the column. Later null keys stay absent and are dropped. The value
   * filter keeps non-negative ints.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def nullKeyCounts(): Map[Int, Int] =
    val nullable = nullKeySource().filterValues(nonNegative).countByKey
    val found = anyCounts(nullable.collect())
    found shouldBe anyCounts(row(nullable.collect()))
    found

  /** Named erasure boundary. The boxed key is an `Int` once the null keys have been dropped. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Any"))
  private def anyCounts(rows: Iterable[(Any, Int)]): Map[Int, Int] =
    countsOf(rows.map { case (key, count) => (key.asInstanceOf[Int], count) })

  private def assertInt(source: DistCollection[(Int, Int)], expected: Map[Int, Int]): Unit =
    val counted = source.countByKey
    assertColumnar(counted.plan, "CountByKey(Exchange(Scan))")
    val found = countsOf(counted.collect())
    found shouldBe countsOf(row(counted.collect()))
    found shouldBe expected
    counted.count() shouldBe expected.size.toLong

  private def assertString(
      source: DistCollection[(String, Int)],
      expected: Map[String, Int],
  ): Unit =
    val counted = source.countByKey
    val lowered = ColumnarPlanner.lower(counted.plan)
    lowered.map(_.shape) shouldBe Some("CountByKey(Exchange(Scan))")
    lowered.map(_.kind) shouldBe Some(PhysicalOp.Kind.StringPairs)
    ColumnarPlanner.partitioning(counted.plan).map(_.layout) shouldBe Some(
      Layout.StringPair(ScarletConf.get.columnarBatchSize),
    )
    val found = countsOf(counted.collect())
    found shouldBe countsOf(row(counted.collect()))
    found shouldBe expected

  private def assertLong(source: DistCollection[(Long, Long)], expected: Map[Long, Int]): Unit =
    val counted = source.countByKey
    val lowered = ColumnarPlanner.lower(counted.plan)
    lowered.map(_.shape) shouldBe Some("CountByKey(Exchange(Scan))")
    lowered.map(_.kind) shouldBe Some(PhysicalOp.Kind.LongCounts)
    ColumnarPlanner.partitioning(counted.plan).map(_.layout) shouldBe Some(
      Layout.Columnar(ScarletConf.get.columnarBatchSize),
    )
    val found = counted.collect().toList
    found should contain theSameElementsAs row(counted.collect().toList)
    countsOf(found) shouldBe expected
    countsOf(found) shouldBe countsOf(row(counted.collect()))

  private def assertColumnar(plan: Plan[_], shape: String): Unit =
    ColumnarPlanner.lower(plan).map(_.shape) shouldBe Some(shape)
    ColumnarPlanner.partitioning(plan).map(_.layout) shouldBe Some(
      Layout.Columnar(ScarletConf.get.columnarBatchSize),
    )

  private def countsOf[K](rows: Iterable[(K, Int)]): Map[K, Int] =
    rows.foldLeft(Map.empty[K, Int]) { (seen, row) =>
      require(!seen.contains(row._1), s"duplicate count for ${row._1}")
      seen.updated(row._1, row._2)
    }

  private def kernelCounts(batches: Vector[ColumnBatch]): Map[Int, Int] =
    countsOf(batches.flatMap(batch => BatchCodec.decodeIntPairs(batch)))

  private def intPairs(
      keys: Array[Int],
      values: Array[Int],
      keyPresent: Int => Boolean = _ => true,
      valuePresent: Int => Boolean = _ => true,
  ): ColumnBatch =
    val rows = keys.length
    require(values.length == rows, "key and value columns differ in length")
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.pack(rows, keyPresent), rows),
        Int32Column.of(values, Validity.pack(rows, valuePresent), rows),
      ),
      rows,
    )

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def lateNullSource(): DistCollection[(Any, Any)] =
    DistCollection(
      Plan.Source(Seq(Partition(Seq[(Any, Any)]((1, 2), (1, null), (2, 3))))),
    )

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
  private def lateNullCounts(): Map[Int, Int] =
    require(lateNullKept(), "late-null source unboxed the null value")
    val counted = lateNullSource().countByKey
    require(ColumnarPlanner.lower(counted.plan).nonEmpty, "late-null source did not lower")
    require(ColumnarPlanner.tryRun(counted.plan).nonEmpty, "late-null count left the column path")
    counted.collect().map { case (key, value) => (key.asInstanceOf[Int], value) }.toMap

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
    ),
  )
  private def lateNullOneFilterCounts(): Map[Int, Int] =
    val counted = lateNullSource().filterKeys { (key: Any) =>
      key match
        case found: Int => found == 1
        case _ => false
    }.countByKey
    require(
      ColumnarPlanner.lower(counted.plan).nonEmpty,
      "one-filter late-null source did not lower",
    )
    require(
      ColumnarPlanner.tryRun(counted.plan).nonEmpty,
      "one-filter late-null count left the column path",
    )
    counted.collect().map { case (key, value) => (key.asInstanceOf[Int], value) }.toMap

  private def lateNullKept(): Boolean =
    val plan = lateNullSource().plan
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("late-null source did not lower"))
    ColumnarInterpreter.pairsOf(ColumnarInterpreter.interpret(plan, op)).exists { batch =>
      val values = batch.columns.lift(1) match
        case Some(column: Int32Column) => column
        case other => fail(s"expected an Int32 value column, got $other")
      (0 until batch.length).exists(row => !values.isValid(row) && values.values(row) == 0)
    }

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
  private def lateNullRowCounts(): Map[Int, Int] =
    lateNullSource().countByKey
      .collect()
      .map { case (key, value) =>
        (key.asInstanceOf[Int], value)
      }
      .toMap

  /** Named erasure boundary. `Tuple2` keeps a null `Integer`; `(String, Int)` would unbox it. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def stringPair(key: String, value: java.lang.Integer): (String, Int) =
    Tuple2(key, value).asInstanceOf[(String, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentInt: java.lang.Integer = null

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def longPair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absent: java.lang.Long = null

  private def longRef(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  private def nullKey(value: Int): (String, Int) = (null: String) -> value

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullValue(key: Int): (Int, String) = key -> (null: String)

  /**
   * A present pair witnesses the column. A later null key stays absent and `countByKey` drops it.
   * The value is a present int, so `mapValues` can declare `Int => Int`.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullIntKeySource(): DistCollection[(Int, Int)] =
    def cell(key: Any, value: Int): (Int, Int) =
      Tuple2(key, Int.box(value)).asInstanceOf[(Int, Int)]
    DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq(cell(1, 4), cell(null, 9), cell(1, 5), cell(2, -1))),
          Partition(Seq(cell(2, 6), cell(null, 8))),
        ),
      ),
    )

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
