package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * A long-pair `Project` reduced without publishing the pair batch.
 *
 * The adapter runs on every row, including a null key. A null projected key is dropped. A null
 * projected value stays a null group until another value arrives. [[LongHashReduce.byKey]] is the
 * published-column path and does not see this project.
 */
class TestProjectedLongReduce extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true, defaultShufflePartitions = 4))
    ScarletRuntime.get.shuffle.clear()
    LongPairProject.resetPublishedForTest()
    LongHashReduce.resetByKeyForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  it should "reduce a long pair map without publishing the pair batch" in {
    val source = DistCollection.ofLongPairs(Array(1L, 2L, 1L, 3L), Array(10L, 20L, 30L, 40L), 1)
    val mapped = source.map((pair: (Long, Long)) => (pair._1 + 1L, pair._2))
    val changed = mapped.reduceByKey[Long, Long](_ + _)
    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some("Project(Scan)")
    ColumnarPlanner.lower(changed.plan).map(_.shape) shouldBe Some(
      "LongHashAggregate(Exchange(Project(Scan)))",
    )
    val expected = Map(2L -> 40L, 3L -> 20L, 4L -> 40L)
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      LongPairProject.resetPublishedForTest()
      LongHashReduce.resetByKeyForTest()
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      val found =
        try changed.collect().toMap
        finally ColumnMetrics.restore(previous)
      LongPairProject.publishedForTest shouldBe 0
      LongHashReduce.byKeyCallsForTest shouldBe 0
      sink.fusedCount should be > 0L
      found
    }
    val summed = atBatch(8192)
    summed shouldBe expected
    atBatch(1) shouldBe summed
    summed shouldBe row(changed.collect().toMap)

    LongPairProject.resetPublishedForTest()
    val projected = mapped.collect().toList.sorted
    LongPairProject.publishedForTest should be > 0
    projected shouldBe List((2L, 10L), (2L, 30L), (3L, 20L), (4L, 40L))

    val calls = new AtomicInteger
    val nullable = nullLongKeySource().map(shiftNullLongKey(calls, nullKey = 7L)).reduceByKey[
      Long,
      Long,
    ](_ + _)
    val nullAt = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      LongPairProject.resetPublishedForTest()
      LongHashReduce.resetByKeyForTest()
      val found = nullable.collect().toMap
      LongPairProject.publishedForTest shouldBe 0
      LongHashReduce.byKeyCallsForTest shouldBe 0
      (found, calls.get)
    }
    val (nullSums, nullCalls) = nullAt(8192)
    nullSums shouldBe Map(2L -> 9L, 3L -> 5L, 7L -> 17L)
    nullAt(1) shouldBe (nullSums, nullCalls)
    calls.set(0)
    row(nullable.collect().toMap) shouldBe nullSums
    calls.get shouldBe nullCalls

    val valueCalls = new AtomicInteger
    val values = nullLongValueSource()
      .map(shiftNullLongKey(valueCalls, nullKey = 7L))
      .reduceByKey[Long, Long](_ + _)
    val valueAt = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      valueCalls.set(0)
      LongPairProject.resetPublishedForTest()
      LongHashReduce.resetByKeyForTest()
      val found = values.collect().toList
      LongPairProject.publishedForTest shouldBe 0
      LongHashReduce.byKeyCallsForTest shouldBe 0
      (found, valueCalls.get)
    }
    val (valueSums, valueCount) = valueAt(8192)
    valueSums should contain theSameElementsAs Seq(
      pair(longRef(5L), longRef(4L)),
      pair(longRef(2L), absent),
    )
    val (again, againCount) = valueAt(1)
    again should contain theSameElementsAs valueSums
    againCount shouldBe valueCount
    valueCalls.set(0)
    row(values.collect().toList) should contain theSameElementsAs valueSums
    valueCalls.get shouldBe valueCount
    valueCount shouldBe 2
  }

  it should "count byKey when the long pairs are already published" in {
    val source = DistCollection.ofLongPairs(Array(1L, 2L, 1L, 3L), Array(10L, 20L, 30L, 40L), 1)
    val plain = source.reduceByKey[Long, Long](_ + _)
    ColumnarPlanner.lower(plain.plan).map(_.shape) shouldBe Some(
      "LongHashAggregate(Exchange(Scan))",
    )
    plain.collect().toMap shouldBe Map(1L -> 40L, 2L -> 20L, 3L -> 40L)
    LongHashReduce.byKeyCallsForTest should be > 0
    LongPairProject.publishedForTest shouldBe 0
  }

  it should "drop a null projected key and keep the row-path sums" in {
    val source = DistCollection.ofLongPairs(Array(1L, 2L, 1L, 3L), Array(10L, 20L, 30L, 40L), 1)
    val calls = new AtomicInteger
    val reduced = source.map(dropLongKey(calls, dropped = 2L)).reduceByKey[Long, Long](_ + _)
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "LongHashAggregate(Exchange(Project(Scan)))",
    )
    val atBatch = (size: Int) => {
      ScarletConf.set(ScarletConf.get.copy(columnarExecution = true, columnarBatchSize = size))
      ScarletRuntime.get.shuffle.clear()
      calls.set(0)
      LongPairProject.resetPublishedForTest()
      LongHashReduce.resetByKeyForTest()
      val found = reduced.collect().toMap
      LongPairProject.publishedForTest shouldBe 0
      LongHashReduce.byKeyCallsForTest shouldBe 0
      (found, calls.get)
    }
    val (sums, seen) = atBatch(8192)
    sums shouldBe Map(1L -> 40L, 3L -> 40L)
    atBatch(1) shouldBe (sums, seen)
    calls.set(0)
    row(reduced.collect().toMap) shouldBe sums
    calls.get shouldBe seen
  }

  it should "leave a filter above the long project on the row path" in {
    val source = DistCollection.ofLongPairs(Array(1L, 2L, 1L, 3L), Array(10L, 20L, 30L, 40L), 1)
    val above = source
      .map((pair: (Long, Long)) => (pair._1 + 1L, pair._2))
      .filterKeys((key: Long) => key > 0L)
      .reduceByKey[Long, Long](_ + _)
    ColumnarPlanner.lower(above.plan) shouldBe Option.empty[PhysicalOp]
    above.collect().toMap shouldBe Map(2L -> 40L, 3L -> 20L, 4L -> 40L)
    LongPairProject.publishedForTest shouldBe 0
    LongHashReduce.byKeyCallsForTest shouldBe 0
    above.collect().toMap shouldBe row(above.collect().toMap)
  }

  it should "reduce a filtered long project without publishing the pair batch" in {
    val source = DistCollection.ofLongPairs(Array(1L, 2L, 1L, 3L), Array(10L, 20L, 30L, 40L), 1)
    val reduced = source
      .filterKeys((key: Long) => key % 3L != 0L)
      .filterValues((value: Long) => value >= 0L)
      .map((pair: (Long, Long)) => (pair._1 + 1L, pair._2))
      .reduceByKey[Long, Long](_ + _)
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "LongHashAggregate(Exchange(Project(FilterValue(FilterKey(Scan)))))",
    )
    val found = reduced.collect().toMap
    LongPairProject.publishedForTest shouldBe 0
    LongHashReduce.byKeyCallsForTest shouldBe 0
    found shouldBe Map(2L -> 40L, 3L -> 20L)
    found shouldBe row(reduced.collect().toMap)
  }

  it should "unbox a null projected value when a later morsel brings a value" in {
    ScarletConf.set(
      originalConf.copy(columnarExecution = true, columnarBatchSize = 1, threadPoolSize = 2),
    )
    val input = Seq(
      pair(longRef(3L), longRef(4L)),
      pair(longRef(1L), absent),
      pair(longRef(1L), longRef(5L)),
    )
    val rows = DistCollection(input, 1)
    val combine = (left: Long, right: Long) => left + right + 1L
    val reduced = rows.map(keepNulls).reduceByKey[Long, Long](combine)
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "LongHashAggregate(Exchange(Project(Scan)))",
    )
    val columnar = reduced.collect().toList
    LongPairProject.publishedForTest shouldBe 0
    LongHashReduce.byKeyCallsForTest shouldBe 0
    columnar should contain theSameElementsAs Seq(
      pair(longRef(1L), longRef(6L)),
      pair(longRef(3L), longRef(4L)),
    )
    columnar should contain theSameElementsAs row(
      rows.map(keepNulls).reduceByKey[Long, Long](combine).collect().toList,
    )
  }

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
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Throw",
    ),
  )
  private def dropLongKey(calls: AtomicInteger, dropped: Long): ((Long, Long)) => (Long, Long) =
    pair => {
      val _ = calls.incrementAndGet()
      val product = pair.asInstanceOf[Product]
      val value = product.productElement(1)
      val key = product.productElement(0) match
        case null => null
        case present: Long if present == dropped => null
        case present: Long => Long.box(present)
        case other => throw new IllegalArgumentException(s"pair map saw $other")
      Tuple2(key, value).asInstanceOf[(Long, Long)]
    }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def keepNulls(pair: (Long, Long)): (Long, Long) =
    val product = pair.asInstanceOf[Product]
    Tuple2(product.productElement(0), product.productElement(1)).asInstanceOf[(Long, Long)]

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
            Seq(
              cell(longRef(1L), 4L),
              cell(null, 9L),
              cell(longRef(1L), 5L),
              cell(longRef(2L), -1L),
            ),
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

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def pair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absent: java.lang.Long = null

  private def longRef(value: Long): java.lang.Long = java.lang.Long.valueOf(value)

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
