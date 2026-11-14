package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * The filtered long reduce folds selection into the partial table.
 *
 * Results match [[LongHashReduce.byKeySelected]] and the row path. The partial-set counter is the
 * worker count for one batch, not a timing check.
 */
class TestFoldedLongReduce extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val keepKey: ColumnKernel.Int64Predicate = (key: Long) => key % 3L != 0L
  private val keepValue: ColumnKernel.Int64Predicate = (value: Long) => value >= 0L
  private val keepAll: ColumnKernel.Int64Predicate = (_: Long) => true
  private val keepNone: ColumnKernel.Int64Predicate = (_: Long) => false
  private val steps = List(Selection.Step.Int64(0, keepKey), Selection.Step.Int64(1, keepValue))
  private val plus: LongHashReduce.LongBinOp = (left, right) => left + right

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true, defaultShufflePartitions = 4))
    ScarletRuntime.get.shuffle.clear()
    FoldedLongReduce.resetPartialSetsForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    FoldedLongReduce.resetPartialSetsForTest()
    ()

  "FoldedLongReduce" should "match the index path on duplicate keys at one partition and at four" in {
    val batch = longPairs(
      Seq((1L, 10L), (1L, 1L), (2L, 3L), (4L, 8L), (2L, 4L), (5L, 7L), (5L, 2L), (3L, 9L)),
    )
    val selected = index(batch, steps)

    decode(fold(Vector(batch), steps, numPartitions = 1, parallelism = 1, batchSize = 3)) shouldBe
      decode(viaIndex(Vector(selected), numPartitions = 1, parallelism = 1, batchSize = 3))
    decode(fold(Vector(batch), steps, numPartitions = 4, parallelism = 2, batchSize = 3)) shouldBe
      decode(viaIndex(Vector(selected), numPartitions = 4, parallelism = 2, batchSize = 3))
  }

  it should "keep first-seen order when every row passes" in {
    val batch = longPairs(Seq((3L, 1L), (1L, 2L), (3L, 4L), (2L, 8L), (1L, 1L)))
    val order = List(Selection.Step.Int64(0, keepAll))

    decode(fold(Vector(batch), order, numPartitions = 1, parallelism = 1, batchSize = 2)) shouldBe
      Seq((3L, 5L), (1L, 3L), (2L, 8L))
  }

  it should "keep first-seen order when the partial table grows past its minimum" in {
    // A range of three rows sizes a 16-slot table (limit 8). Twelve distinct keys double it.
    // Key 8 is first seen with a null value, which is the insert that crosses the limit.
    val count = 24
    val keys = Array.tabulate(count)(row => (row % 12).toLong)
    val values = Array.tabulate(count)(row => (row + 1).toLong)
    val valuePresent = Array.tabulate(count)(row => row != 8)
    val batch = sparse(keys, Array.fill(count)(true), values, valuePresent)
    val order = List(Selection.Step.Int64(0, keepAll))
    val selected = index(batch, order)

    decode(fold(Vector(batch), order, numPartitions = 1, parallelism = 1, batchSize = 3)) shouldBe
      decode(viaIndex(Vector(selected), numPartitions = 1, parallelism = 1, batchSize = 3))
    decode(fold(Vector(batch), order, numPartitions = 4, parallelism = 4, batchSize = 3)) shouldBe
      decode(viaIndex(Vector(selected), numPartitions = 4, parallelism = 4, batchSize = 3))
  }

  it should "drop a null key, keep one null value, and unbox a second null" in {
    val batch = sparse(
      Array(1L, 7L, 1L, 2L, 3L, 2L),
      keyPresent = Array(true, false, true, true, true, true),
      Array(0L, 9L, 0L, 5L, 0L, 5L),
      valuePresent = Array(false, true, false, true, false, true),
    )
    val combine: LongHashReduce.LongBinOp = (left, right) => left + right + 1L
    val selected = index(batch, List(Selection.Step.Int64(0, keepAll)))

    decode(
      fold(
        Vector(batch),
        List(Selection.Step.Int64(0, keepAll)),
        numPartitions = 4,
        parallelism = 2,
        batchSize = 1,
        combine,
      ),
    ) shouldBe decode(
      viaIndex(Vector(selected), numPartitions = 4, parallelism = 2, batchSize = 1, combine),
    )
  }

  it should "sum a key that is present in every source batch" in {
    val batches = Vector(
      longPairs(Seq((7L, 1L), (1L, 2L))),
      longPairs(Seq((7L, 3L), (2L, 4L))),
      longPairs(Seq((7L, 5L))),
      longPairs(Seq((7L, 6L), (3L, 7L))),
    )
    val selected = batches.map(batch => index(batch, List(Selection.Step.Int64(0, keepAll))))

    decode(fold(batches, List(Selection.Step.Int64(0, keepAll)), 1, 2, 2)) shouldBe
      decode(viaIndex(selected, 1, 2, 2))
    val wide = decode(fold(batches, List(Selection.Step.Int64(0, keepAll)), 4, 4, 2))
    wide shouldBe decode(viaIndex(selected, 4, 4, 2))
    wide.map { case (key, value) => key -> value }.toMap shouldBe Map(
      7L -> 15L,
      1L -> 2L,
      2L -> 4L,
      3L -> 7L,
    )
  }

  it should "return one empty batch per partition for an empty input" in {
    val parts = fold(Vector.empty, steps, numPartitions = 4, parallelism = 2, batchSize = 8)

    parts.length shouldBe 4
    parts.foreach { batch =>
      batch.length shouldBe 0
      batch.schema shouldBe Vector(LogicalType.Int64, LogicalType.Int64)
    }
    FoldedLongReduce.partialSetsForTest shouldBe 0
  }

  it should "publish an empty bucket per partition when the filter keeps nothing" in {
    val batch = longPairs(Seq((1L, 4L), (2L, 5L), (4L, 6L), (5L, 7L)))
    val dropped = List(Selection.Step.Int64(0, keepNone), Selection.Step.Int64(1, keepValue))
    val parts = fold(Vector(batch), dropped, numPartitions = 4, parallelism = 2, batchSize = 2)

    parts.length shouldBe 4
    parts.foreach(_.length shouldBe 0)
    decode(parts) shouldBe decode(
      viaIndex(Vector(index(batch, dropped)), numPartitions = 4, parallelism = 2, batchSize = 2),
    )
  }

  it should "match a long-pair row filter, including a row the predicate drops" in {
    val batch = longPairs(Seq((1L, 4L), (2L, -1L), (1L, 5L), (-3L, 8L)))
    val rowKeep: LongPairKernel.LongPairPredicate =
      (key: Long, value: Long) => key > 0L && value > 0L
    val rowSteps = List(Selection.Step.LongPair(rowKeep))

    decode(fold(Vector(batch), rowSteps, numPartitions = 1, parallelism = 2, batchSize = 2)) shouldBe
      decode(viaIndex(Vector(index(batch, rowSteps)), numPartitions = 1, parallelism = 2, batchSize = 2))
  }

  it should "report one partial set per worker" in {
    val batch = longPairs((0 until 32).map(row => (row % 5).toLong -> (row + 1).toLong))
    val order = List(Selection.Step.Int64(0, keepAll))

    val _ = fold(Vector(batch), order, numPartitions = 1, parallelism = 1, batchSize = 4)
    FoldedLongReduce.partialSetsForTest shouldBe 1

    FoldedLongReduce.resetPartialSetsForTest()
    val wide = fold(Vector(batch), order, numPartitions = 4, parallelism = 4, batchSize = 4)
    FoldedLongReduce.partialSetsForTest shouldBe 4
    decode(wide) shouldBe decode(
      viaIndex(Vector(index(batch, order)), numPartitions = 4, parallelism = 4, batchSize = 4),
    )
  }

  "reduceByKey" should "match the row path for duplicates, an empty source, and a filter that keeps nothing" in {
    val keys = Array(1L, 1L, 2L, 2L, 4L, 4L, 5L, 5L)
    val values = Array(10L, 1L, 3L, 4L, -1L, 8L, 7L, 2L)

    reduced(keys, values, partitions = 1) should contain theSameElementsAs rowReduced(
      keys,
      values,
      partitions = 1,
    )
    FoldedLongReduce.partialSetsForTest should be > 0
    reduced(keys, values, partitions = 4) should contain theSameElementsAs rowReduced(
      keys,
      values,
      partitions = 4,
    )
    reduced(keys, values, partitions = 1).map { case (key, value) => key -> value }.toMap shouldBe
      Map(1L -> 11L, 2L -> 7L, 4L -> 8L, 5L -> 9L)

    val empty = DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], 4)
    empty
      .filterKeys((key: Long) => keepKey(key))
      .reduceByKey[Long, Long](_ + _)
      .collect() shouldBe Seq.empty[(Long, Long)]

    val dropped = DistCollection
      .ofLongPairs(keys, values, 2)
      .filterKeys((key: Long) => keepNone(key))
      .reduceByKey[Long, Long](_ + _)
    dropped.collect() shouldBe Seq.empty[(Long, Long)]
    row(dropped.collect()) shouldBe Seq.empty[(Long, Long)]
  }

  it should "match the row path when one key is on every source partition" in {
    val keys = Array(8L, 1L, 8L, 2L, 8L, 5L, 8L, 4L)
    val values = Array(1L, 10L, 1L, 10L, 1L, 10L, 1L, 10L)
    val columnar = reduced(keys, values, partitions = 4)

    columnar should contain theSameElementsAs rowReduced(keys, values, partitions = 4)
    columnar.map { case (key, value) => key -> value }.toMap.apply(8L) shouldBe 4L
  }

  it should "match the row path for null keys and null values" in {
    val input = Seq(
      pair(longRef(1L), longRef(10L)),
      pair(longRef(1L), absent),
      pair(absent, longRef(7L)),
      pair(longRef(2L), absent),
      pair(longRef(3L), longRef(4L)),
      pair(longRef(3L), longRef(5L)),
      pair(longRef(4L), absent),
      pair(longRef(4L), absent),
    )
    val rows = DistCollection(input, 4)
    val combine = (left: Long, right: Long) => left + right + 1L
    val reducedPairs = rows
      .filterKeys((key: Long) => keepAll(key))
      .reduceByKey[Long, Long](combine)

    reducedPairs.collect().toList should contain theSameElementsAs row(
      rows.reduceByKey[Long, Long](combine).collect().toList,
    )
  }

  private def fold(
      batches: Vector[ColumnBatch],
      stack: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: LongHashReduce.LongBinOp = plus,
  ): Vector[ColumnBatch] =
    FoldedLongReduce.reduce(batches, stack, numPartitions, parallelism, batchSize, op)

  private def viaIndex(
      selected: Vector[Selected],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: LongHashReduce.LongBinOp = plus,
  ): Vector[ColumnBatch] =
    LongHashReduce.byKeySelected(selected, numPartitions, parallelism, batchSize, op)

  private def index(batch: ColumnBatch, stack: List[Selection.Step]): Selected =
    Selection.applySelected(batch, 0, batch.length, stack)

  private def decode(parts: Vector[ColumnBatch]): Seq[(Long, Long)] =
    parts.flatMap(batch => BatchCodec.decodeLongPairs(batch))

  private def reduced(keys: Array[Long], values: Array[Long], partitions: Int): Seq[(Long, Long)] =
    DistCollection
      .ofLongPairs(keys, values, partitions)
      .filterKeys((key: Long) => keepKey(key))
      .filterValues((value: Long) => keepValue(value))
      .reduceByKey[Long, Long](_ + _)
      .collect()
      .toList

  private def rowReduced(
      keys: Array[Long],
      values: Array[Long],
      partitions: Int,
  ): Seq[(Long, Long)] =
    row(reduced(keys, values, partitions))

  private def longPairs(rows: Seq[(Long, Long)]): ColumnBatch =
    val keys = Int64Column(rows.map(_._1))
    val values = Int64Column(rows.map(_._2))
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(keys, values),
      rows.length,
    )

  private def sparse(
      keys: Array[Long],
      keyPresent: Array[Boolean],
      values: Array[Long],
      valuePresent: Array[Boolean],
  ): ColumnBatch =
    val rows = keys.length
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.of(keys, Validity.pack(rows, row => keyPresent(row)), rows),
        Int64Column.of(values, Validity.pack(rows, row => valuePresent(row)), rows),
      ),
      rows,
    )

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
