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
      DistCollection.ofIntPairs(Array(5, 5, 6, Int.MinValue, Int.MinValue), Array(1, 2, 3, 4, 5), 1),
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

  it should "fall back when a boxed pair source holds a null value after the first row" in {
    lateNullCounts() shouldBe Map(1 -> 2, 2 -> 1)
    lateNullRowCounts() shouldBe Map(1 -> 2, 2 -> 1)
  }

  it should "fall back when one key filter meets a null value after the first row" in {
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

  it should "stay on the row path for string keys" in {
    val strings = DistCollection(
      Plan.Source(Seq(Partition(Seq("a" -> 1, "b" -> 2)), Partition(Seq("a" -> 3)))),
    )
    ColumnarPlanner.lower(strings.countByKey.plan) shouldBe None
    countsOf(strings.countByKey.collect()) shouldBe Map("a" -> 2, "b" -> 1)
    countsOf(row(strings.countByKey.collect())) shouldBe Map("a" -> 2, "b" -> 1)
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
        ScarletConf.get.copy(columnarExecution = true, spillEnabled = enabled, spillBudgetBytes = 1L),
      )
      ScarletRuntime.get.shuffle.clear()
      // The row path would count these keys. The action must fail before that map is produced.
      val refused = the[ClosedKernelRefusal] thrownBy source.countByKey.collect()
      refused.rows shouldBe 3L
      refused.budget shouldBe 1L
      refused.bytes should be > refused.budget
    }
  }

  private def assertInt(source: DistCollection[(Int, Int)], expected: Map[Int, Int]): Unit =
    val counted = source.countByKey
    assertColumnar(counted.plan, "CountByKey(Exchange(Scan))")
    val found = countsOf(counted.collect())
    found shouldBe countsOf(row(counted.collect()))
    found shouldBe expected
    counted.count() shouldBe expected.size.toLong

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
    val counted = lateNullSource().countByKey
    require(ColumnarPlanner.lower(counted.plan).nonEmpty, "late-null source did not lower")
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
    counted.collect().map { case (key, value) => (key.asInstanceOf[Int], value) }.toMap

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
  private def lateNullRowCounts(): Map[Int, Int] =
    lateNullSource().countByKey.collect().map { case (key, value) =>
      (key.asInstanceOf[Int], value)
    }.toMap

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def longPair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absent: java.lang.Long = null

  private def longRef(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  private def nullKey(value: Int): (String, Int) = (null: String) -> value

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullValue(key: Int): (Int, String) = key -> (null: String)

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
