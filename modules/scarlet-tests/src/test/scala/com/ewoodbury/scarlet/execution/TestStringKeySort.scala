package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, PlanWide, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * In-memory rank split of a string-pair scan. A present key orders by `String.compareTo` (UTF-16
 * code units). A null key orders as `""` and stays null. Ties keep input order. The row path is
 * the oracle. Spill is [[TestStringSortSpill]]. This suite leaves spill off.
 *
 * An empty `(String, Int)` source has no string-pair descriptor and no witness, so it stays on
 * the row path. The zero-row kernel is called directly.
 */
class TestStringKeySort extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val accent = "é"
  private val decomposed = "e\u0301"
  private val emoji = "😀"

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true))
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "sortByStringKey" should "match the row path across two dictionaries" in {
    dictionary(BatchCodec.stringIntPairs(leftRows)) shouldBe List("b", "a", accent, "", emoji)
    dictionary(BatchCodec.stringIntPairs(rightRows)) shouldBe List(
      "a",
      "b",
      decomposed,
      emoji,
      "",
    )

    val sorted = mixed.sortByStringKey
    val columnar = collect(sorted)
    val expected = inputRows.sortBy(pair => effective(pair._1))

    columnar shouldBe rowOf(sorted)
    columnar shouldBe expected
    collect(sorted) shouldBe expected
    effectiveKeys(columnar) shouldBe effectiveKeys(columnar).sorted
    columnar.count(pair => isNullKey(pair._1)) shouldBe 2
    columnar.count(pair => isEmptyKey(pair._1)) shouldBe 2
  }

  it should "keep input order for equal strings and for a null beside empty" in {
    val columnar = collect(mixed.sortByStringKey)

    columnar.take(4).map(pair => (isNullKey(pair._1), pair._2)) shouldBe Seq(
      (false, 5),
      (true, 6),
      (false, 12),
      (true, 13),
    )
    columnar.slice(4, 7).map(_._2) shouldBe Seq(2, 8, 14)
    columnar.slice(7, 10).map(_._2) shouldBe Seq(1, 3, 9)
    columnar.drop(10) shouldBe Seq(decomposed -> 10, accent -> 4, emoji -> 7, emoji -> 11)
  }

  it should "sort each output partition, keep global order, and balance the sizes" in {
    val parts = sortedParts(mixed.sortByStringKey.plan)
    val expected = inputRows.sortBy(pair => effective(pair._1))

    parts.map(_.length) shouldBe Vector(4, 3, 4, 3)
    parts.map(_.schema).distinct shouldBe Seq(pairSchema)
    assertRankSplit(parts)
    parts.flatMap(batch => BatchCodec.decodeStringPairs(batch)) shouldBe expected
    parts.map(dictionary) shouldBe Seq(
      List(""),
      List("a"),
      List("b", decomposed),
      List(accent, emoji),
    )
    assertStoredRows(parts, expected)
  }

  it should "leave an empty source on the row path and split empty batches in the kernel" in {
    val empty = DistCollection(Seq.empty[(String, Int)], 4).sortByStringKey
    ColumnarPlanner.lower(empty.plan) shouldBe None
    collect(empty) shouldBe Nil
    rowOf(empty) shouldBe Nil

    val blanks = Vector(
      BatchCodec.stringIntPairs(Seq.empty[(String, Int)]),
      BatchCodec.stringIntPairs(Seq.empty[(String, Int)]),
    )
    val split = StringRankSplit.sortAndSplit(blanks, 3, 2)
    split.length shouldBe 3
    split.foreach { batch =>
      batch.length shouldBe 0
      batch.schema shouldBe pairSchema
    }
    val firstEmpty = at(split, 0)
    val secondEmpty = at(split, 1)
    firstEmpty shouldNot be theSameInstanceAs at(blanks, 0)
    firstEmpty shouldNot be theSameInstanceAs secondEmpty
    secondEmpty shouldNot be theSameInstanceAs at(split, 2)
  }

  it should "put one row in the first output partition" in {
    val source = DistCollection(Plan.Source(Seq(Partition(Seq("solo" -> 7)))))
    val parts = sortedParts(source.sortByStringKey.plan)

    parts.map(_.length) shouldBe Vector(1, 0, 0, 0)
    collect(source.sortByStringKey) shouldBe Seq("solo" -> 7)
    rowOf(source.sortByStringKey) shouldBe Seq("solo" -> 7)
    dictionary(at(parts, 0)) shouldBe List("solo")
    parts.drop(1).foreach { batch =>
      batch.schema shouldBe pairSchema
      dictionary(batch) shouldBe Nil
    }
  }

  it should "keep one sorted partition when the shuffle width is one" in {
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = 1))
    val parts = sortedParts(mixed.sortByStringKey.plan)

    parts.map(_.length) shouldBe Vector(inputRows.length)
    parts.flatMap(batch => BatchCodec.decodeStringPairs(batch)) shouldBe inputRows.sortBy(pair =>
      effective(pair._1),
    )
    dictionary(at(parts, 0)) shouldBe List("", "a", "b", decomposed, accent, emoji)
    assertRankSplit(parts)
  }

  it should "balance a prime partition count" in {
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = 5))
    val source = DistCollection(
      Plan.Source(Seq(Partition(primeRows.take(6)), Partition(primeRows.drop(6)))),
    )
    val parts = sortedParts(source.sortByStringKey.plan)
    val expected = primeRows.sortBy(pair => effective(pair._1))

    parts.map(_.length) shouldBe Vector(3, 2, 3, 2, 2)
    collect(source.sortByStringKey) shouldBe rowOf(source.sortByStringKey)
    parts.flatMap(batch => BatchCodec.decodeStringPairs(batch)) shouldBe expected
    assertRankSplit(parts)
  }

  it should "decode the same prefix as the row path, including across a partition boundary" in {
    val sorted = mixed.sortByStringKey
    val full = rowOf(sorted)
    Seq(0, 1, 4, 5, full.length, full.length + 2).foreach { count =>
      ColumnarPlanner.tryTake(sorted.plan, count) shouldBe Some(full.take(count))
      sorted.take(count) shouldBe full.take(count)
    }
  }

  it should "leave keys, values, filterKeys, and a filter above the sort on the row path" in {
    val sorted = mixed.sortByStringKey
    val projections = Seq(
      sorted.keys,
      sorted.values,
      sorted.filterKeys(blankKey),
      sorted.filter((pair: (String, Int)) => pair._2 > 2),
    )
    projections.foreach { collection =>
      ColumnarPlanner.lower(collection.plan) shouldBe None
      collect(collection) shouldBe rowOf(collection)
    }
    collect(sorted.values) shouldBe Seq(5, 6, 12, 13, 2, 8, 14, 1, 3, 9, 10, 4, 7, 11)
  }

  it should "sort a whole-row filter and leave a map under the sort on the row path" in {
    val filtered = mixed.filter((pair: (String, Int)) => pair._2 != 1).sortByStringKey
    val mapped = mixed.map((pair: (String, Int)) => (pair._1, pair._2)).sortByStringKey

    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some(
      "SortByStringKey(FilterRow(Scan))",
    )
    ColumnarPlanner.lower(mapped.plan) shouldBe None
    collect(filtered) shouldBe rowOf(filtered)
    collect(mapped) shouldBe rowOf(mapped)
  }

  it should "lower filterKeys then sortByStringKey and match the row path" in {
    val sorted = mixed.filterKeys(blankKey).sortByStringKey
    val scan = mixed.sortByStringKey
    val above = scan.filterKeys(blankKey)
    val underMap = mixed.mapValues((value: Int) => value + 1).filterKeys(blankKey)

    ColumnarPlanner.lower(scan.plan).map(_.shape) shouldBe Some("SortByStringKey(Scan)")
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some(
      "SortByStringKey(FilterKey(Scan))",
    )
    collect(sorted) shouldBe rowOf(sorted)
    ColumnarPlanner.lower(above.plan) shouldBe None
    collect(above) shouldBe rowOf(above)
    ColumnarPlanner.lower(underMap.plan).map(_.shape) shouldBe Some("FilterKey(MapValue(Scan))")
    ColumnarPlanner.lower(underMap.sortByStringKey.plan) shouldBe None
  }

  it should "report range distribution, ascending order, and SortByStringKey(Scan)" in {
    val sorted = mixed.sortByStringKey
    val info = ColumnarPlanner.partitioning(sorted.plan).getOrElse(fail("expected a layout"))

    sorted.plan shouldBe a[Plan.SortByStringKeyOp]
    PlanWide.isWide(sorted.plan) shouldBe true
    PlanWide.isDirectlyWide(sorted.plan) shouldBe true
    info.distribution shouldBe Distribution.Range(
      ScarletConf.get.defaultShufflePartitions,
      PartitioningInfo.RowKeyOrdinals,
    )
    info.ordering shouldBe OrderingTag.Sorted(PartitioningInfo.RowKeyOrdinals, Seq(true))
    info.layout shouldBe Layout.StringPair(ScarletConf.get.columnarBatchSize)
    ColumnarPlanner.lower(sorted.plan).map(_.shape) shouldBe Some("SortByStringKey(Scan)")
    ColumnarPlanner.lower(sorted.plan).map(_.kind) shouldBe Some(PhysicalOp.Kind.StringPairs)
  }

  it should "agree when the kernel runs on one worker or several" in {
    val input = Vector(
      BatchCodec.stringIntPairs(leftRows),
      BatchCodec.stringIntPairs(Seq.empty[(String, Int)]),
      BatchCodec.stringIntPairs(rightRows),
    )
    val serial = StringRankSplit.sortAndSplit(input, 4, 1)
    val parallel = StringRankSplit.sortAndSplit(input, 4, 4)
    val throughPlanner = sortedParts(mixed.sortByStringKey.plan)

    decodeAll(serial) shouldBe decodeAll(parallel)
    serial.map(_.length) shouldBe parallel.map(_.length)
    serial.map(dictionary) shouldBe parallel.map(dictionary)
    decodeAll(throughPlanner) shouldBe decodeAll(parallel)
  }

  private def mixed: DistCollection[(String, Int)] =
    DistCollection(
      Plan.Source(
        Seq(
          Partition(leftRows),
          Partition(Seq.empty[(String, Int)]),
          Partition(rightRows),
        ),
      ),
    )

  /** First-seen dictionary: `b`, `a`, the accent, `""`, the emoji. A null key is not interned. */
  private def leftRows: Seq[(String, Int)] = Seq(
    "b" -> 1,
    "a" -> 2,
    "b" -> 3,
    accent -> 4,
    "" -> 5,
    nullKey(6),
    emoji -> 7,
  )

  /** First-seen dictionary: `a`, `b`, the decomposed form, the emoji, `""`. */
  private def rightRows: Seq[(String, Int)] = Seq(
    "a" -> 8,
    "b" -> 9,
    decomposed -> 10,
    emoji -> 11,
    "" -> 12,
    nullKey(13),
    "a" -> 14,
  )

  private def inputRows: Seq[(String, Int)] = leftRows ++ rightRows

  private def primeRows: Seq[(String, Int)] = Seq(
    "m" -> 1,
    "a" -> 2,
    "m" -> 3,
    "b" -> 4,
    "a" -> 5,
    "c" -> 6,
    "b" -> 7,
    "d" -> 8,
    "a" -> 9,
    "c" -> 10,
    "e" -> 11,
    "b" -> 12,
  )

  private def pairSchema: Vector[LogicalType] = Vector(LogicalType.Utf8Dict, LogicalType.Int32)

  private def assertRankSplit(parts: Vector[ColumnBatch]): Unit =
    val (low, high) = parts.map(_.length).foldLeft((Int.MaxValue, 0)) {
      case ((smallest, largest), length) =>
        (math.min(smallest, length), math.max(largest, length))
    }
    (high - low) should be <= 1
    parts.foreach(assertSortedPartition)
    parts.zip(parts.drop(1)).foreach { (current, next) =>
      if (current.length > 0 && next.length > 0) then
        lastEffective(current) should be <= firstEffective(next)
    }
    val keys = parts.flatMap(effectiveKeysOf)
    keys shouldBe keys.sorted
    parts.foreach { batch =>
      if (batch.length > 0) then dictionary(batch) shouldBe distinctPresent(utf8Column(batch))
    }

  private def assertSortedPartition(batch: ColumnBatch): Unit =
    val keys = effectiveKeysOf(batch)
    keys shouldBe keys.sorted

  private def assertStoredRows(parts: Vector[ColumnBatch], expected: Seq[(String, Int)]): Unit =
    val located = parts.flatMap { batch =>
      (0 until batch.length).map(row => (batch, row))
    }
    located.length shouldBe expected.length
    located.zip(expected).foreach { case ((batch, row), pair) =>
      val keys = utf8Column(batch)
      val values = intColumn(batch)
      values.isValid(row) shouldBe true
      values.values(row) shouldBe pair._2
      if (isNullKey(pair._1)) then keys.isValid(row) shouldBe false
      else
        keys.isValid(row) shouldBe true
        keys.dictionary(keys.codes(row)) shouldBe pair._1
    }

  private def sortedParts(plan: Plan[(String, Int)]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered sort"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.StringPairs(parts) => parts
      case other => fail(s"expected string pairs, got $other")

  private def decodeAll(parts: Vector[ColumnBatch]): Seq[(String, Int)] =
    parts.flatMap(batch => BatchCodec.decodeStringPairs(batch))

  private def dictionary(batch: ColumnBatch): List[String] =
    utf8Column(batch).dictionary.toList

  private def distinctPresent(column: DictUtf8Column): List[String] =
    val reversed = (0 until column.length).foldLeft(List.empty[String]) { (seen, row) =>
      if (!column.isValid(row)) then seen
      else
        val text = column.dictionary(column.codes(row))
        if (seen.contains(text)) seen else text :: seen
    }
    reversed.reverse

  private def at[A](values: Vector[A], index: Int): A =
    values.lift(index).getOrElse(fail(s"missing index $index"))

  private def effectiveKeysOf(batch: ColumnBatch): Seq[String] =
    val keys = utf8Column(batch)
    (0 until batch.length).map(row => effectiveAt(keys, row))

  private def effectiveAt(column: DictUtf8Column, row: Int): String =
    if (column.isValid(row)) column.dictionary(column.codes(row)) else ""

  private def firstEffective(batch: ColumnBatch): String =
    effectiveAt(utf8Column(batch), 0)

  private def lastEffective(batch: ColumnBatch): String =
    effectiveAt(utf8Column(batch), batch.length - 1)

  private def effectiveKeys(rows: Seq[(String, Int)]): Seq[String] =
    rows.map(pair => effective(pair._1))

  private def utf8Column(batch: ColumnBatch): DictUtf8Column =
    batch.columns.lift(0).getOrElse(fail("missing key column")) match
      case column: DictUtf8Column => column
      case other => fail(s"expected Utf8Dict, found ${other.logicalType}")

  private def intColumn(batch: ColumnBatch): Int32Column =
    batch.columns.lift(1).getOrElse(fail("missing value column")) match
      case column: Int32Column => column
      case other => fail(s"expected Int32, found ${other.logicalType}")

  private def collect[A](collection: DistCollection[A]): List[A] =
    ScarletRuntime.get.shuffle.clear()
    collection.collect().toList

  private def rowOf[A](collection: DistCollection[A]): List[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      collection.collect().toList
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def effective(key: String): String = key match
    case null => ""
    case text => text

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def isNullKey(key: String): Boolean = key match
    case null => true
    case _ => false

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def isEmptyKey(key: String): Boolean = key match
    case null => false
    case text => text.isEmpty

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def blankKey(key: String): Boolean = key match
    case null => true
    case text => text.isEmpty
