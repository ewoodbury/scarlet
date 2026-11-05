package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{ExecutionService, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Int `reduceByKey` over a filter selection.
 *
 * A two-filter stack keeps the survivor index and the scan drops those rows. The exchange still
 * sees one dense partial per input batch. A null value is dropped, including on a selected row. A
 * null on a row outside the live prefix is not read.
 */
class TestSelectionReduce extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val keys = Array(1, 2, -1, 4, Int.MinValue, 1, 2, 1, 4)
  private val values = Array(10, 20, 30, 40, 50, 11, 21, 12, 41)
  private val plus: HashReduce.IntBinOp = (left, right) => left + right
  private val maxOp: HashReduce.IntBinOp = (left, right) => math.max(left, right)

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

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "a two-filter reduceByKey" should "match the row path and skip the compact" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val source = DistCollection.ofIntPairs(keys, values, 4)
    val filtered = source
      .filterKeys { (key: Int) =>
        val _ = keyCalls.incrementAndGet()
        key > 0
      }
      .filterValues { (value: Int) =>
        val _ = valueCalls.incrementAndGet()
        value > 20
      }
    val reduced = filtered.reduceByKey[Int, Int](_ + _)

    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "HashAggregate(Exchange(FilterValue(FilterKey(Scan))))",
    )
    val found = reduced.collect().toMap
    // One pass: the key predicate sees every row, and the value predicate sees the seven keys
    // that passed. A second interpret would double both counts.
    keyCalls.get() shouldBe keys.length
    valueCalls.get() shouldBe 7
    found shouldBe Map(2 -> 21, 4 -> 81)
    found shouldBe row(reduced.collect().toMap)

    val encode = published {
      val scan = ColumnarPlanner.lower(source.plan).getOrElse(fail("expected a pair scan"))
      ColumnarInterpreter.interpret(source.plan, scan)
    }
    val selectedCount = published(reduced.collect())
    val (afterFilter, dense) = ColumnMetrics.withSink(new CountingSink) {
      val op = ColumnarPlanner.lower(filtered.plan).getOrElse(fail("expected a pair filter plan"))
      val columns = ColumnarInterpreter.interpret(filtered.plan, op)
      val mid = ColumnMetrics.batchCount
      val _ = HashReduce.byKey(
        ColumnarInterpreter.pairsOf(columns),
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
        plus,
      )
      (mid, ColumnMetrics.batchCount)
    }
    val compactPublishes = afterFilter - encode
    compactPublishes should be > 0L
    (dense - selectedCount) shouldBe compactPublishes
  }

  it should "drop every row when both filters reject the batch" in {
    val source = DistCollection.ofIntPairs(keys, values, 4)
    val reduced = source
      .filterKeys((_: Int) => false)
      .filterValues((_: Int) => true)
      .reduceByKey[Int, Int](_ + _)

    reduced.collect() shouldBe empty
    reduced.collect() shouldBe row(reduced.collect())
    reduced.count() shouldBe 0L
  }

  it should "keep every row when both filters pass" in {
    val source = DistCollection.ofIntPairs(keys, values, 4)
    val reduced = source
      .filterKeys((_: Int) => true)
      .filterValues((_: Int) => true)
      .reduceByKey[Int, Int](_ + _)

    reduced.collect().toMap shouldBe row(reduced.collect().toMap)
    reduced.collect().toMap shouldBe Map(
      1 -> (10 + 11 + 12),
      2 -> (20 + 21),
      -1 -> 30,
      4 -> (40 + 41),
      Int.MinValue -> 50,
    )
  }

  it should "sum one hot key across morsels" in {
    val rows = 20
    val source = DistCollection.ofIntPairs(Array.fill(rows)(7), Array.tabulate(rows)(_ + 1), 2)
    val reduced = source
      .filterKeys((key: Int) => key == 7)
      .filterValues((value: Int) => value > 0)
      .reduceByKey[Int, Int](_ + _)

    reduced.collect().toMap shouldBe Map(7 -> rows * (rows + 1) / 2)
    reduced.collect().toMap shouldBe row(reduced.collect().toMap)
  }

  it should "publish an empty partition for every shuffle slot of an empty source" in {
    val reduced = DistCollection
      .ofIntPairs(Array.empty[Int], Array.empty[Int], 2)
      .filterKeys((_: Int) => true)
      .filterValues((_: Int) => true)
      .reduceByKey[Int, Int](_ + _)
    val parts = ExecutionService.get.executePartitions(reduced.plan)

    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "HashAggregate(Exchange(FilterValue(FilterKey(Scan))))",
    )
    parts.size shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(_.data shouldBe empty)
    reduced.collect() shouldBe empty
  }

  "int32Selected" should "ignore a null outside the prefix and drop a null it does scan" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq((Some(1), None), (Some(2), Some(5)), (None, Some(9))),
    )
    val dropped = Selected(batch, Array(1, 0, 2), 1)
    val kept = HashReduce.int32Selected(dropped, 0, 1, keyOrdinal = 0, valueOrdinal = 1, plus)

    sums(Vector(kept)) shouldBe Map(2 -> 5)

    val nullValue = Selected(batch, Array(0), 1)
    val droppedValue =
      HashReduce.int32Selected(nullValue, 0, 1, keyOrdinal = 0, valueOrdinal = 1, plus)
    droppedValue.length shouldBe 0

    val nullKey = Selected(batch, Array(2), 1)
    val droppedKey =
      HashReduce.int32Selected(nullKey, 0, 1, keyOrdinal = 0, valueOrdinal = 1, plus)
    droppedKey.length shouldBe 0

    val mixed = Selected(batch, Array(0, 1, 2), 3)
    val reduced = HashReduce.byKeySelected(
      Vector(mixed),
      numPartitions = 2,
      parallelism = 2,
      batchSize = 1,
      plus,
    )
    sums(reduced) shouldBe Map(2 -> 5)
  }

  it should "scan only the requested prefix" in {
    val batch = BatchCodec.intPairs(Seq((1, 10), (2, 20), (3, 30)))
    val ids = Array(0, 2, 2)
    val selected = Selected(batch, ids, 2)
    val both = HashReduce.int32Selected(selected, 0, 2, keyOrdinal = 0, valueOrdinal = 1, plus)
    val tail = HashReduce.int32Selected(selected, 1, 2, keyOrdinal = 0, valueOrdinal = 1, plus)
    val none = HashReduce.int32Selected(selected, 0, 0, keyOrdinal = 0, valueOrdinal = 1, plus)

    sums(Vector(both)) shouldBe Map(1 -> 10, 3 -> 30)
    sums(Vector(tail)) shouldBe Map(3 -> 30)
    none.length shouldBe 0
    selected.rows should be theSameInstanceAs ids
    ids.toSeq shouldBe Seq(0, 2, 2)
    an[IllegalArgumentException] should be thrownBy {
      HashReduce.int32Selected(selected, 0, 3, keyOrdinal = 0, valueOrdinal = 1, plus)
    }
  }

  "byKeySelected" should "match byKey when every row is selected" in {
    val rows = (0 until 20).map(index => (index % 4, index + 1))
    val batch = BatchCodec.intPairs(rows)
    val ids = Array.tabulate(batch.length)(identity)
    val selected = Selected(batch, ids, batch.length)
    val viaSelection = HashReduce.byKeySelected(
      Vector(selected),
      numPartitions = 4,
      parallelism = 3,
      batchSize = 8,
      plus,
    )
    val viaDense = HashReduce.byKey(
      Vector(batch),
      numPartitions = 4,
      parallelism = 3,
      batchSize = 8,
      plus,
    )

    sums(viaSelection) shouldBe sums(viaDense)
    selected.rows should be theSameInstanceAs ids
    ids.toSeq shouldBe (0 until batch.length)
  }

  it should "apply max across index chunks" in {
    val batch = BatchCodec.intPairs(Seq((1, 3), (1, 9), (2, 1), (1, 4)))
    val selected = Selected(batch, Array(0, 1, 2, 3), 4)
    val reduced = HashReduce.byKeySelected(
      Vector(selected),
      numPartitions = 2,
      parallelism = 2,
      batchSize = 2,
      maxOp,
    )

    sums(reduced) shouldBe Map(1 -> 9, 2 -> 1)
  }

  it should "sum one hot key from index chunks" in {
    val rows = 17
    val batch = BatchCodec.intPairs((0 until rows).map(_ => (4, 3)))
    val selected = Selected(batch, Array.tabulate(rows)(identity), rows)
    val viaSelection = HashReduce.byKeySelected(
      Vector(selected),
      numPartitions = 4,
      parallelism = 4,
      batchSize = 5,
      plus,
    )
    val viaDense = HashReduce.byKey(
      Vector(batch),
      numPartitions = 4,
      parallelism = 4,
      batchSize = 5,
      plus,
    )

    sums(viaSelection) shouldBe Map(4 -> rows * 3)
    sums(viaSelection) shouldBe sums(viaDense)
  }

  it should "return an empty bucket per partition for an empty selection" in {
    val batch = BatchCodec.intPairs(Seq((1, 1), (2, 2), (3, 3)))
    val empty = Selected(batch, Array(0, 1, 2), 0)
    val reduced = HashReduce.byKeySelected(
      Vector(empty),
      numPartitions = 3,
      parallelism = 2,
      batchSize = 2,
      plus,
    )

    reduced.length shouldBe 3
    reduced.map(_.length) shouldBe Seq(0, 0, 0)
  }

  it should "drop a selection whose rows are all null" in {
    val batch = BatchCodec.nullableIntPairs(
      Seq((None, Some(4)), (Some(7), None), (None, None)),
    )
    val selected = Selected(batch, Array(0, 1, 2), 3)
    val viaSelection = HashReduce.byKeySelected(
      Vector(selected),
      numPartitions = 2,
      parallelism = 2,
      batchSize = 2,
      plus,
    )
    val viaDense = HashReduce.byKey(
      Vector(batch),
      numPartitions = 2,
      parallelism = 2,
      batchSize = 2,
      plus,
    )

    val emptySums = Map.empty[Int, Int]
    sums(viaSelection) shouldBe emptySums
    sums(viaSelection) shouldBe sums(viaDense)
  }

  "perBatchSelected" should "merge chunks of one selection and keep selections apart" in {
    val firstIds = Array(0, 1, 2, 3)
    val first = Selected(BatchCodec.intPairs(Seq((1, 1), (1, 1), (1, 1), (1, 1))), firstIds, 4)
    val secondBatch = BatchCodec.intPairs(Seq((1, 10), (1, 10), (1, 10), (1, 10)))
    val second = Selected(secondBatch, Array(0, 1, 2, 3), 4)
    val out = partials(Vector(first, second), batchSize = 2, parallelism = 2)
    val (left, right) = two(out)

    sums(Vector(left)) shouldBe Map(1 -> 4)
    sums(Vector(right)) shouldBe Map(1 -> 40)
    first.rows should be theSameInstanceAs firstIds
  }

  it should "emit one empty partial for an empty selection" in {
    val batch = BatchCodec.intPairs(Seq((1, 1), (2, 2)))
    val left = Selected(batch, Array(0, 1), 0)
    val right = Selected(batch, Array(0, 1), 0)
    val out = partials(Vector(left, right), batchSize = 2, parallelism = 2)

    out.length shouldBe 2
    out.map(_.length) shouldBe Seq(0, 0)
    an[IllegalArgumentException] should be thrownBy {
      partials(Vector.empty, batchSize = 0, parallelism = 1)
    }
  }

  private def two(batches: Vector[ColumnBatch]): (ColumnBatch, ColumnBatch) = batches match
    case Vector(left, right) => (left, right)
    case other => fail(s"expected two partials, got ${other.length}")

  private def partials(
      selected: Vector[Selected],
      batchSize: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    PartialCombine.perBatchSelected(
      selected,
      batchSize,
      parallelism,
      (one, from, until) =>
        HashReduce.int32Selected(one, from, until, keyOrdinal = 0, valueOrdinal = 1, plus),
      batch => HashReduce.int32(batch, keyOrdinal = 0, valueOrdinal = 1, plus),
    )

  private def sums(batches: Vector[ColumnBatch]): Map[Int, Int] =
    batches.flatMap(batch => BatchCodec.decodeIntPairs(batch)).toMap

  private def published[A](body: => A): Long =
    ColumnMetrics.withSink(new CountingSink) {
      val _ = body
      ColumnMetrics.batchCount
    }

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
