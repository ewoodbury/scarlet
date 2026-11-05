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
 * String `reduceByKey` over a filter selection.
 *
 * A two-filter stack keeps the survivor index and the scan drops those rows. The exchange still
 * sees one dense partial per input batch. A null key is dropped. `""` is kept. A null value on a
 * dropped row is not read. A null value on a selected row follows [[StringHashReduce.reduce]]: a
 * clear value bit is not a null this kernel understands, so the stored int is combined.
 */
class TestSelectionStringReduce extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val plus: (Int, Int) => Int = (left, right) => left + right
  private val maxOp: (Int, Int) => Int = (left, right) => math.max(left, right)

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        // Six rows fit in one batch, so the dense reduce does not slice. The batch-count gap is
        // the compact the selection does not publish.
        columnarBatchSize = 8,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "a two-filter string reduceByKey" should "match the row path and skip the compact" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val rows = source
    val filtered = rows
      .filterKeys { (key: String) =>
        val _ = keyCalls.incrementAndGet()
        keeps(key)
      }
      .filterValues { (value: Int) =>
        val _ = valueCalls.incrementAndGet()
        value > 0
      }
    val reduced = filtered.reduceByKey[String, Int](_ + _)
    val leftDictionary = dictionaryOf(leftRows)
    val rightDictionary = dictionaryOf(rightRows)

    leftDictionary.indexOf("") should not be rightDictionary.indexOf("")
    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "StringHashAggregate(Exchange(FilterValue(FilterKey(Scan))))",
    )
    val found = reduced.collect().toMap
    // One pass: the key predicate sees every row, and the value predicate sees the ten keys that
    // passed. A second interpret would double both counts.
    keyCalls.get() shouldBe leftRows.length + rightRows.length
    valueCalls.get() shouldBe 10
    found shouldBe Map("" -> 3, "red" -> 10, "blue" -> 7)
    found shouldBe row(reduced.collect().toMap)
    selectedOf(reduced.plan).isDefined shouldBe true

    val encode = published {
      val scan = ColumnarPlanner.lower(rows.plan).getOrElse(fail("expected a string-pair scan"))
      ColumnarInterpreter.interpret(rows.plan, scan)
    }
    val selectedCount = published(reduced.collect())
    val (afterFilter, dense) = ColumnMetrics.withSink(new CountingSink) {
      val op = ColumnarPlanner.lower(filtered.plan).getOrElse(fail("expected a string filter"))
      val columns = ColumnarInterpreter.interpret(filtered.plan, op)
      val mid = ColumnMetrics.batchCount
      val _ = StringHashReduce.byKey(
        StringPairWalk.batches(columns),
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
    val reduced = source
      .filterKeys((_: String) => false)
      .filterValues((_: Int) => true)
      .reduceByKey[String, Int](_ + _)

    reduced.collect() shouldBe empty
    reduced.collect() shouldBe row(reduced.collect())
    reduced.count() shouldBe 0L
  }

  it should "keep every row when both filters pass" in {
    val reduced = source
      .filterKeys((_: String) => true)
      .filterValues((_: Int) => true)
      .reduceByKey[String, Int](_ + _)
    val found = reduced.collect().toMap

    found shouldBe row(reduced.collect().toMap)
    found shouldBe Map("" -> 3, "red" -> 10, "blue" -> 5, "no" -> 11)
    found.contains("") shouldBe true
    found.keys.exists(BatchCodec.isAbsent) shouldBe false
  }

  it should "sum one hot key across morsels" in {
    val rows = 20
    val hot = DistCollection(Seq.tabulate(rows)(index => "hot" -> (index + 1)), 2)
    val reduced = hot
      .filterKeys((key: String) => key.nonEmpty)
      .filterValues((value: Int) => value > 0)
      .reduceByKey[String, Int](_ + _)

    reduced.collect().toMap shouldBe Map("hot" -> rows * (rows + 1) / 2)
    reduced.collect().toMap shouldBe row(reduced.collect().toMap)
  }

  it should "leave a single filter on the dense reduce" in {
    val once = source.filterValues((value: Int) => value > 0)
    val reduced = once.reduceByKey[String, Int](_ + _)

    selectedOf(reduced.plan) shouldBe None
    reduced.collect().toMap shouldBe row(reduced.collect().toMap)
    reduced.collect().toMap shouldBe Map("" -> 3, "red" -> 10, "blue" -> 7, "no" -> 11)
  }

  "reduceSelected" should "ignore a null outside the prefix and match reduce on a selected null" in {
    val batch = nullableBatch
    // Row 0 is a null value. It sits past the live prefix, so the scan must not read it.
    val dropped = Selected(batch, Array(1, 0, 2, 3), 1)
    val kept = StringHashReduce.reduceSelected(dropped, 0, 1, plus)

    sums(Vector(kept)) shouldBe Map("" -> 1)

    val nullValue = Selected(batch, Array(0), 1)
    val selectedNull = StringHashReduce.reduceSelected(nullValue, 0, 1, plus)
    val alone = pairBatch(
      DictUtf8Column.fromIterable(Seq("red")),
      Int32Column.of(Array(7), Validity.pack(1, _ => false), 1),
    )
    sums(Vector(selectedNull)) shouldBe sums(Vector(StringHashReduce.reduce(alone, plus)))
    sums(Vector(selectedNull)) shouldBe Map("red" -> 7)

    val nullKey = Selected(batch, Array(2), 1)
    StringHashReduce.reduceSelected(nullKey, 0, 1, plus).length shouldBe 0

    val emptyKey = Selected(batch, Array(1), 1)
    sums(Vector(StringHashReduce.reduceSelected(emptyKey, 0, 1, plus))) shouldBe Map("" -> 1)
    StringHash.hash("") shouldBe 0
  }

  it should "scan only the requested prefix" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 10, "b" -> 20, "c" -> 30))
    val ids = Array(0, 2, 2)
    val selected = Selected(batch, ids, 2)
    val both = StringHashReduce.reduceSelected(selected, 0, 2, plus)
    val tail = StringHashReduce.reduceSelected(selected, 1, 2, plus)
    val none = StringHashReduce.reduceSelected(selected, 0, 0, plus)

    sums(Vector(both)) shouldBe Map("a" -> 10, "c" -> 30)
    sums(Vector(tail)) shouldBe Map("c" -> 30)
    none.length shouldBe 0
    selected.rows should be theSameInstanceAs ids
    ids.toSeq shouldBe Seq(0, 2, 2)
    an[IllegalArgumentException] should be thrownBy {
      StringHashReduce.reduceSelected(selected, 0, 3, plus)
    }
  }

  "byKeySelected" should "match byKey when every row is selected" in {
    val batch = nullableBatch
    val ids = Array(0, 1, 2, 3)
    val selected = Selected(batch, ids, batch.length)
    val viaSelection = StringHashReduce.byKeySelected(
      Vector(selected),
      numPartitions = 4,
      parallelism = 2,
      batchSize = 2,
      plus,
    )
    val viaDense = StringHashReduce.byKey(
      Vector(batch),
      numPartitions = 4,
      parallelism = 2,
      batchSize = 2,
      plus,
    )

    sums(viaSelection) shouldBe sums(viaDense)
    sums(viaSelection) shouldBe Map("red" -> 7, "" -> 1, "blue" -> 4)
    selected.rows should be theSameInstanceAs ids
  }

  it should "apply max across index chunks" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 3, "a" -> 9, "b" -> 1, "a" -> 4))
    val selected = Selected(batch, Array(0, 1, 2, 3), 4)
    val reduced = StringHashReduce.byKeySelected(
      Vector(selected),
      numPartitions = 2,
      parallelism = 2,
      batchSize = 2,
      maxOp,
    )

    sums(reduced) shouldBe Map("a" -> 9, "b" -> 1)
  }

  it should "sum one hot key from index chunks" in {
    val rows = 17
    val batch = BatchCodec.stringIntPairs(Seq.fill(rows)("hot" -> 3))
    val selected = Selected(batch, Array.tabulate(rows)(identity), rows)
    val viaSelection = StringHashReduce.byKeySelected(
      Vector(selected),
      numPartitions = 4,
      parallelism = 2,
      batchSize = 5,
      plus,
    )
    val viaDense = StringHashReduce.byKey(
      Vector(batch),
      numPartitions = 4,
      parallelism = 2,
      batchSize = 5,
      plus,
    )

    sums(viaSelection) shouldBe Map("hot" -> rows * 3)
    sums(viaSelection) shouldBe sums(viaDense)
  }

  it should "return an empty bucket per partition for an empty selection" in {
    val batch = BatchCodec.stringIntPairs(Seq("a" -> 1, "b" -> 2, "c" -> 3))
    val empty = Selected(batch, Array(0, 1, 2), 0)
    val reduced = StringHashReduce.byKeySelected(
      Vector(empty),
      numPartitions = 3,
      parallelism = 2,
      batchSize = 2,
      plus,
    )
    val missing = StringHashReduce.byKeySelected(
      Vector.empty,
      numPartitions = 3,
      parallelism = 2,
      batchSize = 2,
      plus,
    )

    reduced.length shouldBe 3
    missing.length shouldBe 3
    (reduced ++ missing).foreach(assertEmptyPair)
    an[IllegalArgumentException] should be thrownBy {
      StringHashReduce.byKeySelected(Vector.empty, numPartitions = 0, parallelism = 1, batchSize = 2, plus)
    }
  }

  it should "drop a selection whose keys are all null" in {
    val batch = pairBatch(
      DictUtf8Column.fromIterable(Seq(absent, absent, absent)),
      Int32Column.of(Array(4, 7, 1), Validity.allValid(3), 3),
    )
    val selected = Selected(batch, Array(0, 1, 2), 3)
    val viaSelection = StringHashReduce.byKeySelected(
      Vector(selected),
      numPartitions = 2,
      parallelism = 2,
      batchSize = 2,
      plus,
    )
    val viaDense = StringHashReduce.byKey(
      Vector(batch),
      numPartitions = 2,
      parallelism = 2,
      batchSize = 2,
      plus,
    )

    sums(viaSelection) shouldBe Map.empty[String, Int]
    sums(viaSelection) shouldBe sums(viaDense)
  }

  it should "merge two dictionaries by string when their intern order differs" in {
    val leftDictionary = Array("", "red", "blue")
    val rightDictionary = Array("blue", "red", "")
    leftDictionary.indexOf("") should not be rightDictionary.indexOf("")
    val left = pairBatch(
      DictUtf8Column.of(leftDictionary, Array(0, 1, 2), Validity.allValid(3), 3),
      Int32Column.of(Array(1, 5, 6), Validity.allValid(3), 3),
    )
    val right = pairBatch(
      DictUtf8Column.of(rightDictionary, Array(0, 1, 2), Validity.allValid(3), 3),
      Int32Column.of(Array(8, 9, 2), Validity.allValid(3), 3),
    )
    val selected = Vector(
      Selected(left, Array(0, 1, 2), 3),
      Selected(right, Array(0, 1, 2), 3),
    )
    val expected = Map("" -> 3, "red" -> 14, "blue" -> 14)

    sums(StringHashReduce.byKeySelected(selected, numPartitions = 1, parallelism = 1, batchSize = 2, plus)) shouldBe
      expected
    sums(StringHashReduce.byKey(Vector(left, right), numPartitions = 1, parallelism = 1, batchSize = 2, plus)) shouldBe
      expected
    sums(StringHashReduce.byKeySelected(selected, numPartitions = 4, parallelism = 2, batchSize = 2, plus)) shouldBe
      expected
  }

  /**
   * Row 0 is `"red"` with a clear value bit and stored `7`. Row 1 is `"" -> 1`. Row 2 is a null
   * key. Row 3 is `"blue" -> 4`.
   */
  private def nullableBatch: ColumnBatch =
    pairBatch(
      DictUtf8Column.fromIterable(Seq("red", "", absent, "blue")),
      Int32Column.of(Array(7, 1, 9, 4), Validity.pack(4, row => row != 0), 4),
    )

  private def source: DistCollection[(String, Int)] =
    DistCollection(Plan.Source(Seq(Partition(leftRows), Partition(rightRows))))

  private def leftRows: Seq[(String, Int)] = Seq(
    "" -> 1,
    "red" -> 5,
    "no" -> 3,
    nullKey(9),
    "red" -> 1,
    "blue" -> -2,
  )

  private def rightRows: Seq[(String, Int)] = Seq(
    "blue" -> 6,
    "red" -> 4,
    "" -> 2,
    nullKey(7),
    "no" -> 8,
    "blue" -> 1,
  )

  private def keeps(key: String): Boolean =
    BatchCodec.isAbsent(key) || (key match
      case "no" => false
      case _ => true
    )

  private def selectedOf(plan: Plan[_]): Option[Vector[Selected]] = plan match
    case Plan.ReduceByKeyOp(filterPlan, _) =>
      ColumnarPlanner.lower(plan) match
        case Some(PhysicalOp.StringHashAggregate(PhysicalOp.Exchange(child))) =>
          FilterChain.selected(filterPlan, child, None, false, Set(0, 1))
        case _ => fail("expected a string hash aggregate")
    case _ => fail("expected reduceByKey")

  private def sums(batches: Vector[ColumnBatch]): Map[String, Int] =
    batches.flatMap(batch => BatchCodec.decodeStringPairs(batch)).toMap

  private def assertEmptyPair(batch: ColumnBatch): Unit =
    batch.length shouldBe 0
    batch.schema shouldBe Vector(LogicalType.Utf8Dict, LogicalType.Int32)

  private def pairBatch(keys: DictUtf8Column, values: Int32Column): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(keys, values),
      keys.length,
    )

  private def dictionaryOf(rows: Seq[(String, Int)]): Seq[String] =
    BatchCodec.stringIntPairs(rows).columns.toList match
      case (column: DictUtf8Column) :: _ => column.dictionary.toSeq
      case other => fail(s"expected a dictionary, found $other")

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

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absent: String = null
