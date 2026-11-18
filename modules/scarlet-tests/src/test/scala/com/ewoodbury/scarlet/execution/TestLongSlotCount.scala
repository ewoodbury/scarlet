package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, LongHashCount, LongPairProject}
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * One long slot above a long-pair project, then `countByKey`.
 *
 * The count walks source morsels, runs the project adapter, and applies the slot. It does not
 * publish the pair batch, and [[LongHashCount.byKey]] does not see the filtered columns. Collect
 * of the filtered map still writes both columns.
 */
class TestLongSlotCount extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

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
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "countByKey" should "count one slot above a long pair map without publishing the pair batch" in {
    val source = DistCollection.ofLongPairs(Array(1L, 2L, 1L, 3L), Array(10L, 20L, 30L, 40L), 1)
    val mapped = source.map((pair: (Long, Long)) => (pair._1 + 1L, pair._2))
    val byKey = mapped.filterKeys((key: Long) => key > 2L).countByKey
    val byValue = mapped.filterValues((value: Long) => value >= 20L).countByKey
    ColumnarPlanner.lower(byKey.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(FilterKey(Project(Scan))))",
    )
    ColumnarPlanner.lower(byValue.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(FilterValue(Project(Scan))))",
    )

    val atBatch = (counted: DistCollection[(Long, Int)], size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      LongPairProject.resetPublishedForTest()
      LongHashCount.resetByKeyForTest()
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      val found =
        try countsOf(counted.collect())
        finally ColumnMetrics.restore(previous)
      LongPairProject.publishedForTest shouldBe 0
      LongHashCount.byKeyCallsForTest shouldBe 0
      sink.fusedCount should be > 0L
      found
    }
    val keyed = atBatch(byKey, 8192)
    keyed shouldBe Map(3L -> 1, 4L -> 1)
    atBatch(byKey, 1) shouldBe keyed
    keyed shouldBe countsOf(row(byKey.collect()))

    val valued = atBatch(byValue, 8192)
    valued shouldBe Map(2L -> 1, 3L -> 1, 4L -> 1)
    atBatch(byValue, 1) shouldBe valued
    valued shouldBe countsOf(row(byValue.collect()))

    LongPairProject.resetPublishedForTest()
    val projected = mapped.filterKeys((key: Long) => key > 2L).collect().toList.sorted
    LongPairProject.publishedForTest should be > 0
    projected shouldBe List((3L, 20L), (4L, 40L))

    val stacked = source
      .filterKeys((key: Long) => key > 0L)
      .filterValues((value: Long) => value >= 20L)
      .map((pair: (Long, Long)) => (pair._1 + 1L, pair._2))
      .filterKeys((key: Long) => key > 2L)
      .countByKey
    ColumnarPlanner.lower(stacked.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(FilterKey(Project(FilterValue(FilterKey(Scan))))))",
    )
    val stackedCounts = atBatch(stacked, 8192)
    stackedCounts shouldBe Map(3L -> 1, 4L -> 1)
    atBatch(stacked, 1) shouldBe stackedCounts
    stackedCounts shouldBe countsOf(row(stacked.collect()))

    val calls = new AtomicInteger
    val nullable = nullLongKeySource().map(shiftNullLongKey(calls, nullKey = 7L))
    val nullKeyed = nullable.filterKeys((key: Long) => key > 2L).countByKey
    val nullAt = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      val found = countsOf(nullKeyed.collect())
      (found, calls.get)
    }
    val (nullCounts, nullCalls) = nullAt(8192)
    nullCounts shouldBe Map(3L -> 2, 7L -> 2)
    nullAt(1) shouldBe (nullCounts, nullCalls)
    calls.set(0)
    countsOf(row(nullKeyed.collect())) shouldBe nullCounts
    calls.get shouldBe nullCalls

    val valueCalls = new AtomicInteger
    val values = nullLongValueSource()
      .map(shiftNullLongKey(valueCalls, nullKey = 7L))
      .filterKeys((key: Long) => key > 0L)
      .countByKey
    valueCalls.set(0)
    countsOf(values.collect()) shouldBe Map(2L -> 1, 5L -> 1)
    valueCalls.get shouldBe 2
    valueCalls.set(0)
    countsOf(row(values.collect())) shouldBe Map(2L -> 1, 5L -> 1)
    valueCalls.get shouldBe 2

    val droppedKeyCalls = new AtomicInteger
    val dropped = source
      .map { (pair: (Long, Long)) =>
        val _ = droppedKeyCalls.incrementAndGet()
        erased(absent, longRef(pair._2))
      }
      .filterKeys { (key: Long) =>
        droppedKeyCalls.incrementAndGet()
        key > 0L
      }
      .countByKey
    droppedKeyCalls.set(0)
    LongPairProject.resetPublishedForTest()
    LongHashCount.resetByKeyForTest()
    countsOf(dropped.collect()) shouldBe Map.empty[Long, Int]
    droppedKeyCalls.get shouldBe 4
    LongPairProject.publishedForTest shouldBe 0
    LongHashCount.byKeyCallsForTest shouldBe 0

    val valueSlotCalls = new AtomicInteger
    val nullKeyValues = nullLongKeySource()
      .map((pair: (Long, Long)) => pair)
      .filterValues { (value: Long) =>
        valueSlotCalls.incrementAndGet()
        value >= 0L
      }
      .countByKey
    ColumnarPlanner.lower(nullKeyValues.plan).map(_.shape) shouldBe Some(
      "CountByKey(Exchange(FilterValue(Project(Scan))))",
    )
    valueSlotCalls.set(0)
    LongPairProject.resetPublishedForTest()
    LongHashCount.resetByKeyForTest()
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = 8192))
    ScarletRuntime.get.shuffle.clear()
    countsOf(nullKeyValues.collect()) shouldBe Map(1L -> 2, 2L -> 1)
    valueSlotCalls.get shouldBe 6
    LongPairProject.publishedForTest shouldBe 0
    LongHashCount.byKeyCallsForTest shouldBe 0
    valueSlotCalls.set(0)
    countsOf(row(nullKeyValues.collect())) shouldBe Map(1L -> 2, 2L -> 1)
    valueSlotCalls.get shouldBe 6

    val strings = DistCollection(Seq("a" -> 1), 1)
    val stringMapped = strings.map((pair: (String, Int)) => (pair._1, pair._2))
    ColumnarPlanner.lower(stringMapped.filterKeys((key: String) => key.nonEmpty).plan) shouldBe
      Option.empty[PhysicalOp]
  }

  private def countsOf[K](rows: Iterable[(K, Int)]): Map[K, Int] =
    rows.foldLeft(Map.empty[K, Int]) { (seen, row) =>
      require(!seen.contains(row._1), s"duplicate count for ${row._1}")
      seen.updated(row._1, row._2)
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def erased(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absent: java.lang.Long = null

  private def longRef(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  /**
   * A null key becomes `nullKey`. A present key is incremented. `productElement` keeps a null
   * value, so both paths count it. The row path and the long-pair project call this once per row.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def shiftNullLongKey(
      calls: AtomicInteger,
      nullKey: Long,
  ): ((Long, Long)) => (Long, Long) =
    pair => {
      val _ = calls.incrementAndGet()
      val product = pair.asInstanceOf[Product]
      val value = product.productElement(1)
      val key = product.productElement(0) match
        case null => nullKey
        case present: Long => present + 1L
        case other => throw new IllegalArgumentException(s"pair map saw $other")
      Tuple2(Long.box(key), value).asInstanceOf[(Long, Long)]
    }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullLongKeySource(): DistCollection[(Long, Long)] =
    def cell(key: Any, value: Long): (Long, Long) =
      Tuple2(key, Long.box(value)).asInstanceOf[(Long, Long)]
    DistCollection(
      Plan.Source(
        Seq(
          Partition(
            Seq(cell(longRef(1L), 4L), cell(null, 9L), cell(longRef(1L), 5L), cell(longRef(2L), -1L)),
          ),
          Partition(Seq(cell(longRef(2L), 6L), cell(null, 8L))),
        ),
      ),
    )

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def nullLongValueSource(): DistCollection[(Long, Long)] =
    def cell(key: Long, value: Any): (Long, Long) =
      Tuple2(Long.box(key), value).asInstanceOf[(Long, Long)]
    DistCollection(Plan.Source(Seq(Partition(Seq(cell(4L, longRef(4L)), cell(1L, null))))))

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
