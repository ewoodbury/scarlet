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
 * Columnar `keys`, `values`, `mapValues`, `filterKeys`, and `filterValues` on `(String, Int)`.
 *
 * The row path is the oracle. A null key stays in the batch. A null value stays on the row path.
 * Distinct and a string `mapValues` stay on the row path. `reduceByKey`, `groupByKey`, and an
 * inner shuffle-hash join lower.
 */
class TestStringPairNarrow extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get
  private val accent = "é"
  private val decomposed = "e\u0301"
  private val emoji = "😀"

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(
      original.copy(columnarExecution = true, columnarBatchSize = 3, threadPoolSize = 1),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "string pair narrow ops" should "match the row path across two dictionaries" in {
    val rows = source
    val keyed = rows.keys
    val valued = rows.values
    val mapped = rows.mapValues((value: Int) => value + 1)
    val byKey = rows.filterKeys(isRed)
    val byValue = rows.filterValues((value: Int) => value > 0)

    columnar(keyed) shouldBe rowOf(keyed)
    columnar(valued) shouldBe rowOf(valued)
    columnar(mapped) shouldBe rowOf(mapped)
    columnar(byKey) shouldBe rowOf(byKey)
    columnar(byValue) shouldBe rowOf(byValue)

    layout(keyed) shouldBe Layout.Utf8Dict(batchSize)
    layout(valued) shouldBe Layout.Columnar(batchSize)
    layout(mapped) shouldBe Layout.StringPair(batchSize)
    layout(byKey) shouldBe Layout.StringPair(batchSize)
    layout(byValue) shouldBe Layout.StringPair(batchSize)

    ColumnarPlanner.lower(keyed.plan).map(_.shape) shouldBe Some("Key(Scan)")
    ColumnarPlanner.lower(valued.plan).map(_.shape) shouldBe Some("Value(Scan)")
    ColumnarPlanner.lower(mapped.plan).map(_.shape) shouldBe Some("MapValue(Scan)")
    ColumnarPlanner.lower(byKey.plan).map(_.shape) shouldBe Some("FilterKey(Scan)")
    ColumnarPlanner.lower(byValue.plan).map(_.shape) shouldBe Some("FilterValue(Scan)")
  }

  it should "keep both an empty key and a null key, and not the accented form" in {
    val kept = columnar(source.filterKeys(absentOrEmpty))

    kept shouldBe rowOf(source.filterKeys(absentOrEmpty))
    kept.exists(pair => isNullKey(pair._1)) shouldBe true
    kept.exists(pair => isEmptyKey(pair._1)) shouldBe true
    kept.exists(pair => isAccent(pair._1)) shouldBe false
    kept.exists(pair => isDecomposed(pair._1)) shouldBe false
  }

  it should "keep only null keys, and only empty keys, as separate filters" in {
    columnar(source.filterKeys(isNullKey)) shouldBe Seq(nullKey(4), nullKey(11))
    columnar(source.filterKeys(isEmptyKey)) shouldBe Seq("" -> 3, "" -> 10)
  }

  it should "call filterKeys and mapValues once per row, including a null key" in {
    val filterCalls = new AtomicInteger
    val mapCalls = new AtomicInteger
    val filtered = source.filterKeys { (key: String) =>
      filterCalls.incrementAndGet()
      isRed(key)
    }
    val mapped = source.mapValues { (value: Int) =>
      mapCalls.incrementAndGet()
      value + 1
    }

    columnar(filtered) shouldBe Seq("red" -> 1, "red" -> 0, "red" -> 9)
    filterCalls.get() shouldBe rowCount
    val mappedRows = columnar(mapped)
    mapCalls.get() shouldBe rowCount
    mappedRows.exists(pair => isNullKey(pair._1)) shouldBe true
    mappedRows shouldBe rowOf(source.mapValues((value: Int) => value + 1))
  }

  it should "drop every value except the rows filterKeys kept" in {
    val reds = source.filterKeys(isRed).values

    columnar(reds) shouldBe Seq(1, 0, 9)
    demand(reds) shouldBe Set(0, 1)
  }

  it should "encode only the key column for keys of filterKeys" in {
    val keyed = source.filterKeys(isRed).keys

    demand(keyed) shouldBe Set(0)
    schemas(keyed) shouldBe Seq(Vector(LogicalType.Utf8Dict), Vector(LogicalType.Utf8Dict))
    columnar(keyed) shouldBe Seq("red", "red", "red")
  }

  it should "still encode both columns for keys of mapValues" in {
    val mapped = source.mapValues((value: Int) => value + 1)
    val keyed = mapped.keys

    demand(keyed) shouldBe Set(0, 1)
    schemas(keyed).foreach(schema => schema shouldBe Vector(LogicalType.Utf8Dict))
    width(mapped) shouldBe 2
  }

  it should "sum values and count a filter on the columnar path" in {
    val valued = source.values
    val positive = source.filterValues((value: Int) => value > 0)

    ColumnarPlanner.trySumInt(valued.plan) shouldBe Some(rowOf(valued).map(_.toLong).sum)
    valued.sum shouldBe rowOf(valued).map(_.toLong).sum
    ColumnarPlanner.tryCount(positive.plan) shouldBe Some(rowOf(positive).length.toLong)
    positive.count() shouldBe rowOf(positive).length.toLong
  }

  it should "concatenate a union of string pairs and keep both sides" in {
    val union = source.union(source)

    ColumnarPlanner.lower(union.plan).map(_.shape) shouldBe Some("Union(Scan, Scan)")
    columnar(union) shouldBe rowOf(union)
    columnar(union.keys) shouldBe rowOf(union.keys)
    columnar(union.filterKeys(isRed)) shouldBe rowOf(union.filterKeys(isRed))
  }

  it should "take a prefix that spans a morsel boundary" in {
    source.take(4) shouldBe rowOf(source).take(4)
  }

  it should "lower a leading null key and refuse a null value or all-null keys" in {
    val leading = pairs(Seq(nullKey(1), "red" -> 2))
    val nullValue = pairs(Seq("red" -> 1, erasedNullValue("blue")))
    val bothNull = pairs(Seq(erasedNullPair))
    val onlyNull = pairs(Seq(nullKey(1), nullKey(2)))
    val empty = pairs(Seq.empty)

    ColumnarPlanner.lower(leading.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.StringPairs)
    columnar(leading) shouldBe Seq(nullKey(1), "red" -> 2)
    ColumnarPlanner.lower(nullValue.plan) shouldBe None
    stayedOnRows(nullValue) shouldBe Seq("red" -> 1, erasedNullValue("blue"))
    ColumnarPlanner.lower(bothNull.plan) shouldBe None
    ColumnarPlanner.lower(onlyNull.plan) shouldBe None
    stayedOnRows(onlyNull) shouldBe Seq(nullKey(1), nullKey(2))
    ColumnarPlanner.lower(empty.plan) shouldBe None
  }

  it should "lower reduceByKey, groupByKey, and an inner join, and keep distinct and a string mapValues on the row path" in {
    val rows = source
    val grouped = rows.groupByKey
    ColumnarPlanner.lower(rows.reduceByKey((left: Int, right: Int) => left + right).plan).map(
      _.shape,
    ) shouldBe Some("StringHashAggregate(Exchange(Scan))")
    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    valuesByKey(columnar(grouped)) shouldBe valuesByKey(rowOf(grouped))
    ColumnarPlanner.lower(rows.distinct().plan) shouldBe None
    ColumnarPlanner.lower(rows.join(rows).plan).map(_.shape) shouldBe Some(
      "StringHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(rows.mapValues((value: Int) => value.toString).plan) shouldBe None
    ColumnarPlanner.groupedSumPeel(rows.filterKeys(isRed).plan) shouldBe None
    ColumnarPlanner.tryGroupedSum(rows.filterKeys(isRed).plan) shouldBe None
  }

  it should "resolve the same strings from dictionaries with different first-seen order" in {
    val left = BatchCodec.stringIntPairs(leftRows)
    val right = BatchCodec.stringIntPairs(rightRows)
    val leftWords = dictionary(left)
    val rightWords = dictionary(right)

    leftWords.head shouldBe "red"
    rightWords.head shouldBe emoji
    leftWords.indexOf(accent) should not be rightWords.indexOf(accent)

    val accents = columnar(source.filterKeys(isAccent))
    accents shouldBe Seq(accent -> 5, accent -> 0)
    accents.exists(pair => isDecomposed(pair._1)) shouldBe false
    columnar(source.filterKeys(isDecomposed)) shouldBe Seq(decomposed -> 6, decomposed -> -3)
  }

  private def source: DistCollection[(String, Int)] =
    DistCollection(Plan.Source(Seq(Partition(leftRows), Partition(rightRows))))

  private def leftRows: Seq[(String, Int)] = Seq(
    "red" -> 1,
    "red" -> 0,
    "" -> 3,
    nullKey(4),
    accent -> 5,
    decomposed -> 6,
    emoji -> 7,
  )

  private def rightRows: Seq[(String, Int)] = Seq(
    emoji -> 8,
    "red" -> 9,
    "" -> 10,
    nullKey(11),
    accent -> 0,
    decomposed -> -3,
  )

  private def rowCount: Int = leftRows.length + rightRows.length

  private def valuesByKey(rows: Seq[(String, Iterable[Int])]): Map[String, Seq[Int]] =
    rows.map { case (key, values) => key -> values.toSeq }.toMap

  private def pairs(rows: Seq[(String, Int)]): DistCollection[(String, Int)] =
    DistCollection(Plan.Source(Seq(Partition(rows))))

  private def batchSize: Int = ScarletConf.get.columnarBatchSize

  private def columnar[A](collection: DistCollection[A]): Seq[A] =
    val lowered =
      ColumnarPlanner.tryRun(collection.plan).getOrElse(fail("stayed on the row path"))
    lowered.flatMap(_.data).toList

  private def rowOf[A](collection: DistCollection[A]): Seq[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      collection.collect().toList
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def stayedOnRows[A](collection: DistCollection[A]): Seq[A] =
    ColumnMetrics.withSink(new CountingSink) {
      val rows = collection.collect().toList
      ColumnMetrics.batchCount shouldBe 0L
      rows
    }

  private def layout[A](collection: DistCollection[A]): Layout =
    ColumnarPlanner
      .partitioning(collection.plan)
      .getOrElse(fail("expected a columnar plan"))
      .layout

  private def demand[A](collection: DistCollection[A]): Set[Int] =
    val op = ColumnarPlanner.lower(collection.plan).getOrElse(fail("expected a columnar plan"))
    ColumnUse.ordinals(collection.plan, op)

  private def schemas[A](collection: DistCollection[A]): Seq[IndexedSeq[LogicalType]] =
    batches(collection).map(_.schema)

  private def width[A](collection: DistCollection[A]): Int =
    val widths = batches(collection).map(_.columns.length).distinct
    widths shouldBe Seq(2)
    2

  private def batches[A](collection: DistCollection[A]): Vector[ColumnBatch] =
    val op = ColumnarPlanner.lower(collection.plan).getOrElse(fail("expected a columnar plan"))
    ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(collection.plan, op))

  private def dictionary(batch: ColumnBatch): Array[String] = batch.columns.toList match
    case (column: DictUtf8Column) :: _ => column.dictionary
    case _ => fail(s"expected a dictionary, found ${batch.schema}")

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erasedNullValue(key: String): (String, Int) =
    Tuple2[String, Integer](key, null).asInstanceOf[(String, Int)]

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erasedNullPair: (String, Int) =
    Tuple2[String, Integer](null, null).asInstanceOf[(String, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def isNullKey(key: String): Boolean = key match
    case null => true
    case _ => false

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentOrEmpty(key: String): Boolean = key match
    case null => true
    case text => text.isEmpty

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def isEmptyKey(key: String): Boolean = key match
    case null => false
    case text => text.isEmpty

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Null"))
  private def isRed(key: String): Boolean = key match
    case null => false
    case text => text == "red"

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Null"))
  private def isAccent(key: String): Boolean = key match
    case null => false
    case text => text == accent

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Null"))
  private def isDecomposed(key: String): Boolean = key match
    case null => false
    case text => text == decomposed
