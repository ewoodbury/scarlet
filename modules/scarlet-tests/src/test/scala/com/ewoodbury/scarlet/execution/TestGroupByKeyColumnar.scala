package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{ExecutionService, Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Int, long, and string `groupByKey` on the columnar path, compared with the row engine per key.
 */
class TestGroupByKeyColumnar extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val keys = Array(1, 2, -1, 4, Int.MinValue, 1, 2, 1, 4)
  private val values = Array(10, 20, 30, 40, 50, 11, 21, 12, 41)

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

  "groupByKey" should "match the row path value order and count groups" in {
    val source = DistCollection.ofIntPairs(keys, values, 4)
    val grouped = source.groupByKey
    val found = valuesByKey(grouped.collect())

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    source.count() shouldBe 9L
    grouped.count() shouldBe 5L
    grouped.count() shouldBe row(grouped.count())
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(5L)
    assertCountBuildsNoLists(grouped)
    found shouldBe valuesByKey(row(grouped.collect()))
    found shouldBe Map(
      1 -> Seq(10, 11, 12),
      2 -> Seq(20, 21),
      -1 -> Seq(30),
      4 -> Seq(40, 41),
      Int.MinValue -> Seq(50),
    )
    ExecutionService.get.executePartitions(grouped.plan).size shouldBe
      ScarletConf.get.defaultShufflePartitions
  }

  it should "count the same groups at batch size 1 and 8192" in {
    countedAt(1) shouldBe countedAt(8192)
    countedAt(1) shouldBe 5L
    filteredCount(1) shouldBe filteredCount(8192)
    filteredCount(1) shouldBe 2L
    longCountedAt(1) shouldBe longCountedAt(8192)
    longCountedAt(1) shouldBe 5L
    stringCountedAt(1) shouldBe stringCountedAt(8192)
    stringCountedAt(1) shouldBe 5L
  }

  it should "drop a null key, keep a null value's key, and build no value lists" in {
    assertNullKeyCount()
  }

  it should "keep duplicate values and several input partitions" in {
    val source = DistCollection.ofIntPairs(Array(3, 3, 5, 3), Array(1, 2, 3, 4), 4)
    val grouped = source.groupByKey

    valuesByKey(grouped.collect()) shouldBe valuesByKey(row(grouped.collect()))
    valuesByKey(grouped.collect()) shouldBe Map(3 -> Seq(1, 2, 4), 5 -> Seq(3))
    grouped.count() shouldBe 2L
  }

  it should "publish an empty partition for every shuffle slot of an empty source" in {
    val grouped = DistCollection.ofIntPairs(Array.empty[Int], Array.empty[Int], 2).groupByKey
    val parts = ExecutionService.get.executePartitions(grouped.plan)

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    parts.size shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(_.data shouldBe empty)
    grouped.collect() shouldBe empty
    grouped.count() shouldBe 0L
  }

  it should "keep empty shuffle partitions beside the populated ones" in {
    val grouped = DistCollection.ofIntPairs(Array(4, 8, 0, 4), Array(1, 2, 3, 4), 3).groupByKey
    val parts = ExecutionService.get.executePartitions(grouped.plan)

    parts.size shouldBe 4
    parts.count(_.data.nonEmpty) shouldBe 1
    valuesByKey(grouped.collect()) shouldBe Map(4 -> Seq(1, 4), 8 -> Seq(2), 0 -> Seq(3))
    grouped.count() shouldBe 3L
  }

  it should "lower a filterValues child and int keys, decode a map, and refuse values and take" in {
    val source = DistCollection.ofIntPairs(keys, values, 4)
    val filtered = source.filterValues((value: Int) => value > 10).groupByKey
    val grouped = source.groupByKey

    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some(
      "GroupByKey(Exchange(FilterValue(Scan)))",
    )
    valuesByKey(filtered.collect()) shouldBe valuesByKey(row(filtered.collect()))
    valuesByKey(filtered.collect())(1) shouldBe Seq(11, 12)
    ColumnarPlanner.lower(grouped.keys.plan).map(_.shape) shouldBe Some(
      "Key(CountByKey(Exchange(Scan)))",
    )
    ColumnarPlanner.partitioning(grouped.values.plan) shouldBe None
    ColumnarPlanner.lower(grouped.values.plan) shouldBe None
    ColumnarPlanner
      .lower(grouped.map((row: (Int, Iterable[Int])) => row._1).plan)
      .map(_.shape) shouldBe Some("GroupedDecode(GroupByKey(Exchange(Scan)))")
    ColumnarPlanner.tryTake(grouped.plan, 1) shouldBe None
    grouped.take(2) shouldBe row(grouped.take(2))
  }

  it should "match the row path for two pair filters and skip the compact" in {
    val source = DistCollection.ofIntPairs(keys, values, 4)
    val filtered = source
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 20)
    val grouped = filtered.groupByKey

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some(
      "GroupByKey(Exchange(FilterValue(FilterKey(Scan))))",
    )
    valuesByKey(grouped.collect()) shouldBe valuesByKey(row(grouped.collect()))
    valuesByKey(grouped.collect()) shouldBe Map(2 -> Seq(21), 4 -> Seq(40, 41))

    // A bare scan collect stays on the row path, so it publishes nothing. The encode baseline is
    // the columnar scan both groupByKey paths start from.
    val encode = published {
      val scan = ColumnarPlanner.lower(source.plan).getOrElse(fail("expected a pair scan"))
      ColumnarInterpreter.interpret(source.plan, scan)
    }
    val selected = published(grouped.collect())
    val (afterFilter, dense) = ColumnMetrics.withSink(new CountingSink) {
      val op = ColumnarPlanner.lower(filtered.plan).getOrElse(fail("expected a pair filter plan"))
      val columns = ColumnarInterpreter.interpret(filtered.plan, op)
      val mid = ColumnMetrics.batchCount
      val _ = GroupByKey.int32(
        ColumnarInterpreter.pairsOf(columns),
        ScarletConf.get.defaultShufflePartitions,
        ScarletConf.get.threadPoolSize,
      )
      (mid, ColumnMetrics.batchCount)
    }
    val compactPublishes = afterFilter - encode
    compactPublishes should be > 0L
    (dense - selected) shouldBe compactPublishes
  }

  it should "ignore a null value on a dropped row and keep one on a selected row" in {
    val keyColumn = Int32Column.of(Array(1, 2), Validity.allValid(2), 2)
    val valueColumn = Int32Column.of(Array(0, 10), Validity.pack(2, row => row == 1), 2)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(keyColumn, valueColumn),
      2,
    )
    val dropped = Selected(batch, Array(1, 0), 1)
    val grouped = GroupByKey.int32Selected(Vector(dropped), partitions = 1, parallelism = 1)

    grouped.flatMap(_.rows).map { case (key, groupedValues) =>
      key -> groupedValues.toSeq
    } shouldBe Seq(2 -> Seq(10))

    val selectedNull = Selected(batch, Array(0), 1)
    val kept = GroupByKey.int32Selected(Vector(selectedNull), partitions = 1, parallelism = 1)
    shownInts(kept) shouldBe Seq(1 -> Seq("null"))
  }

  it should "refuse a union of two grouped results" in {
    val left = DistCollection.ofIntPairs(Array(1, 2), Array(10, 20), 2).groupByKey
    val right = DistCollection.ofIntPairs(Array(1, 3), Array(11, 30), 2).groupByKey
    val union = left.union(right)

    ColumnarPlanner.lower(union.plan) shouldBe None
    valuesByKey(union.collect()) shouldBe valuesByKey(row(union.collect()))
  }

  "string groupByKey" should "match the row path value order and count groups" in {
    val source = stringSource
    val grouped = source.groupByKey
    val found = stringValues(grouped.collect())

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    ColumnarPlanner.partitioning(grouped.plan).map(_.layout) shouldBe Some(
      Layout.GroupedString(ScarletConf.get.columnarBatchSize),
    )
    source.count() shouldBe 12L
    grouped.count() shouldBe 5L
    grouped.count() shouldBe row(grouped.count())
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(5L)
    assertCountBuildsNoLists(grouped)
    found shouldBe stringValues(row(grouped.collect()))
    found shouldBe Map(
      "" -> Seq(1, 8),
      accent -> Seq(2, 9, 9),
      decomposed -> Seq(3, 10),
      emoji -> Seq(4, 7),
      "red" -> Seq(5, 6),
    )
    accent should not be decomposed
    ExecutionService.get.executePartitions(grouped.plan).size shouldBe
      ScarletConf.get.defaultShufflePartitions
  }

  it should "drop a null key on count and keep a null value's key" in {
    val source = DistCollection(
      Seq(
        "a" -> 1,
        erasedString(None, Some(9)),
        "b" -> 2,
        erasedString(Some("c"), None),
        "" -> 4,
        "a" -> 5,
      ),
      2,
    )
    val grouped = source.groupByKey

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    GroupByKey.resetValueListsBuilt()
    grouped.count() shouldBe 4L
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(4L)
    GroupByKey.valueListsBuilt shouldBe 0L
    grouped.count() shouldBe row(grouped.count())
  }

  it should "keep duplicate values and several input partitions" in {
    val source = DistCollection(Seq("red" -> 1, "red" -> 1, "blue" -> 2, "red" -> 3), 4)
    val grouped = source.groupByKey

    stringValues(grouped.collect()) shouldBe stringValues(row(grouped.collect()))
    stringValues(grouped.collect()) shouldBe Map("red" -> Seq(1, 1, 3), "blue" -> Seq(2))
    grouped.count() shouldBe 2L
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(2L)
  }

  it should "leave an empty iterable on the row path" in {
    val grouped = DistCollection(Seq.empty[(String, Int)], 2).groupByKey
    val parts = ExecutionService.get.executePartitions(grouped.plan)

    ColumnarPlanner.lower(grouped.plan) shouldBe None
    parts.size shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(_.data shouldBe empty)
    grouped.collect() shouldBe empty
    grouped.count() shouldBe 0L
    grouped.count() shouldBe row(grouped.count())
  }

  it should "publish an empty grouped batch when a filter keeps nothing" in {
    val grouped = stringSource.filterValues((value: Int) => value < 0).groupByKey
    val parts = ExecutionService.get.executePartitions(grouped.plan)

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some(
      "GroupByKey(Exchange(FilterValue(Scan)))",
    )
    parts.size shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(_.data shouldBe empty)
    grouped.collect() shouldBe empty
    grouped.count() shouldBe 0L
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(0L)
  }

  it should "lower a filterValues child and keys, decode a map, and refuse values and take" in {
    val filtered = stringSource.filterValues((value: Int) => value > 4).groupByKey
    val grouped = stringSource.groupByKey

    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some(
      "GroupByKey(Exchange(FilterValue(Scan)))",
    )
    stringValues(filtered.collect()) shouldBe stringValues(row(filtered.collect()))
    stringValues(filtered.collect()) shouldBe Map(
      "" -> Seq(8),
      accent -> Seq(9, 9),
      decomposed -> Seq(10),
      emoji -> Seq(7),
      "red" -> Seq(5, 6),
    )
    ColumnarPlanner.lower(grouped.keys.plan).map(_.shape) shouldBe Some(
      "Key(CountByKey(Exchange(Scan)))",
    )
    assertKeysMatch(grouped.keys)
    ColumnarPlanner.lower(grouped.values.plan) shouldBe None
    ColumnarPlanner
      .lower(grouped.map((row: (String, Iterable[Int])) => row._1).plan)
      .map(_.shape) shouldBe Some("GroupedDecode(GroupByKey(Exchange(Scan)))")
    ColumnarPlanner.tryTake(grouped.plan, 1) shouldBe None
    grouped.take(2) shouldBe row(grouped.take(2))
  }

  it should "refuse a union of two grouped string results" in {
    val left = DistCollection(Seq("a" -> 1, "b" -> 2), 2).groupByKey
    val right = DistCollection(Seq("a" -> 3, "c" -> 4), 2).groupByKey
    val union = left.union(right)

    ColumnarPlanner.lower(union.plan) shouldBe None
    stringValues(union.collect()) shouldBe stringValues(row(union.collect()))
  }

  "long groupByKey" should "match the row path value order and count groups" in {
    val source = DistCollection.ofLongPairs(longKeys, longPayloads, 4)
    val grouped = source.groupByKey
    val found = longByKey(grouped.collect())

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    ColumnarPlanner.partitioning(grouped.plan).map(_.layout) shouldBe Some(
      Layout.GroupedLong(ScarletConf.get.columnarBatchSize),
    )
    source.count() shouldBe 9L
    grouped.count() shouldBe 5L
    grouped.count() shouldBe row(grouped.count())
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(5L)
    assertCountBuildsNoLists(grouped)
    found shouldBe longByKey(row(grouped.collect()))
    found shouldBe Map(
      1L -> Seq(10L, 11L, 12L),
      2L -> Seq(20L, 21L),
      -1L -> Seq(30L),
      4L -> Seq(40L, 41L),
      Long.MinValue -> Seq(50L),
    )
    ExecutionService.get.executePartitions(grouped.plan).size shouldBe
      ScarletConf.get.defaultShufflePartitions
  }

  it should "count hash-colliding longs as distinct groups" in {
    val collided = 1L << 32
    val grouped = DistCollection
      .ofLongPairs(
        Array(1L, collided, Long.MinValue, 1L, collided),
        Array(1L, 2L, 3L, 4L, 5L),
        4,
      )
      .groupByKey

    GroupByKey.resetValueListsBuilt()
    grouped.count() shouldBe 3L
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(3L)
    GroupByKey.valueListsBuilt shouldBe 0L
    grouped.count() shouldBe row(grouped.count())
  }

  it should "drop a null key on count and keep a null value's key" in {
    val source = DistCollection(
      Seq(
        (1L, 4L),
        erasedLong(None, Some(9L)),
        (5L, 1L),
        (1L, 5L),
        (5L, 3L),
        erasedLong(Some(2L), None),
        erasedLong(None, Some(8L)),
      ),
      2,
    )
    val grouped = source.groupByKey

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    GroupByKey.resetValueListsBuilt()
    grouped.count() shouldBe 3L
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(3L)
    GroupByKey.valueListsBuilt shouldBe 0L
    grouped.count() shouldBe row(grouped.count())
  }

  it should "drop a null key and keep the other values in order" in {
    val batch = longBatch(
      Array(1L, 2L, 1L),
      Array(10L, 99L, 11L),
      keyPresent = row => row != 1,
      valuePresent = _ => true,
    )
    val parts = GroupByKey.int64(Vector(batch), partitions = 4, parallelism = 1)

    parts.length shouldBe 4
    longRows(parts(1)) shouldBe Seq(1L -> Seq(10L, 11L))
    parts.zipWithIndex.foreach { (grouped, index) =>
      if (index == 1) then grouped.groups shouldBe 1
      else grouped.groups shouldBe 0
    }
  }

  it should "keep a null value on a present key and drop one on a null key" in {
    val presentKey = longBatch(
      Array(1L, 1L),
      Array(10L, 20L),
      keyPresent = _ => true,
      valuePresent = row => row != 1,
    )
    val nullKey = longBatch(
      Array(2L, 1L),
      Array(99L, 10L),
      keyPresent = row => row != 0,
      valuePresent = row => row != 0,
    )
    val present = GroupByKey.int64(Vector(presentKey), partitions = 2, parallelism = 1)
    val absent = GroupByKey.int64(Vector(nullKey), partitions = 2, parallelism = 1)

    shownLongs(present) should contain theSameElementsAs Seq(1L -> Seq("10", "null"))
    shownLongs(absent) should contain theSameElementsAs Seq(1L -> Seq("10"))
  }

  it should "publish an empty batch for every shuffle slot of an empty source" in {
    val grouped = DistCollection.ofLongPairs(Array.empty[Long], Array.empty[Long], 2).groupByKey
    val parts = ExecutionService.get.executePartitions(grouped.plan)

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    parts.size shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(_.data shouldBe empty)
    grouped.collect() shouldBe empty
    grouped.count() shouldBe 0L
    grouped.count() shouldBe row(grouped.count())
  }

  it should "keep duplicate values and several input partitions" in {
    val source = DistCollection.ofLongPairs(Array(3L, 3L, 5L, 3L), Array(1L, 2L, 3L, 4L), 4)
    val grouped = source.groupByKey

    longByKey(grouped.collect()) shouldBe longByKey(row(grouped.collect()))
    longByKey(grouped.collect()) shouldBe Map(3L -> Seq(1L, 2L, 4L), 5L -> Seq(3L))
    grouped.count() shouldBe 2L
  }

  it should "lower a whole-row filter and a slot filter, decode a map, and lower keys" in {
    val source = DistCollection.ofLongPairs(longKeys, longPayloads, 4)
    val filtered = source.filter((pair: (Long, Long)) => pair._2 > 10L).groupByKey
    val keyed = source.filterKeys[Long, Long](_ > 0L).groupByKey
    val valued = source.filterValues[Long, Long](_ > 10L).groupByKey
    val grouped = source.groupByKey

    ColumnarPlanner.lower(filtered.plan).map(_.shape) shouldBe Some(
      "GroupByKey(Exchange(FilterRow(Scan)))",
    )
    longByKey(filtered.collect()) shouldBe longByKey(row(filtered.collect()))
    longByKey(filtered.collect())(1L) shouldBe Seq(11L, 12L)
    ColumnarPlanner.lower(keyed.plan).map(_.shape) shouldBe Some(
      "GroupByKey(Exchange(FilterKey(Scan)))",
    )
    ColumnarPlanner.lower(valued.plan).map(_.shape) shouldBe Some(
      "GroupByKey(Exchange(FilterValue(Scan)))",
    )
    longByKey(keyed.collect()) shouldBe longByKey(row(keyed.collect()))
    longByKey(valued.collect()) shouldBe longByKey(row(valued.collect()))
    ColumnarPlanner.lower(grouped.keys.plan).map(_.shape) shouldBe Some(
      "Key(CountByKey(Exchange(Scan)))",
    )
    assertKeysMatch(grouped.keys)
    ColumnarPlanner.lower(grouped.values.plan) shouldBe None
    ColumnarPlanner
      .lower(grouped.map((row: (Long, Iterable[Long])) => row._1).plan)
      .map(_.shape) shouldBe Some("GroupedDecode(GroupByKey(Exchange(Scan)))")
    ColumnarPlanner.tryTake(grouped.plan, 1) shouldBe None
    grouped.take(2) shouldBe row(grouped.take(2))
  }

  it should "refuse a union of two grouped long results" in {
    val left = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 2).groupByKey
    val right = DistCollection.ofLongPairs(Array(1L, 3L), Array(11L, 30L), 2).groupByKey
    val union = left.union(right)

    ColumnarPlanner.lower(union.plan) shouldBe None
    longByKey(union.collect()) shouldBe longByKey(row(union.collect()))
  }

  it should "refuse grouped batches as column batches" in {
    val source = DistCollection.ofLongPairs(Array(1L, 2L), Array(10L, 20L), 2)
    val grouped = source.groupByKey
    val op = ColumnarPlanner.lower(grouped.plan).getOrElse(fail("expected long groupByKey"))
    val columns = ColumnarInterpreter.interpret(grouped.plan, op)
    val refused = the[IllegalStateException] thrownBy ColumnarInterpreter.batchesOf(columns)

    refused.getMessage shouldBe "grouped batches are not column batches"
  }

  private val accent = "é"
  private val decomposed = "e\u0301"
  private val emoji = "😀"

  private def stringSource: DistCollection[(String, Int)] =
    DistCollection(Plan.Source(Seq(Partition(leftRows), Partition(rightRows))))

  private def leftRows: Seq[(String, Int)] = Seq(
    "" -> 1,
    accent -> 2,
    decomposed -> 3,
    emoji -> 4,
    "red" -> 5,
    nullText(9),
    "red" -> 6,
  )

  private def rightRows: Seq[(String, Int)] = Seq(
    emoji -> 7,
    "" -> 8,
    accent -> 9,
    decomposed -> 10,
    accent -> 9,
  )

  private def stringValues(rows: Iterable[(String, Iterable[Int])]): Map[String, Seq[Int]] =
    rows.map { case (key, values) => key -> values.toSeq }.toMap

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullText(value: Int): (String, Int) = (null: String) -> value

  private def published[A](body: => A): Long =
    ColumnMetrics.withSink(new CountingSink) {
      val _ = body
      ColumnMetrics.batchCount
    }

  private val longKeys = Array(1L, 2L, -1L, 4L, Long.MinValue, 1L, 2L, 1L, 4L)
  private val longPayloads = Array(10L, 20L, 30L, 40L, 50L, 11L, 21L, 12L, 41L)

  private def longBatch(
      keyValues: Array[Long],
      valueValues: Array[Long],
      keyPresent: Int => Boolean,
      valuePresent: Int => Boolean,
  ): ColumnBatch =
    val rows = keyValues.length
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.of(keyValues, Validity.pack(rows, keyPresent), rows),
        Int64Column.of(valueValues, Validity.pack(rows, valuePresent), rows),
      ),
      rows,
    )

  private def longRows(batch: GroupedLongBatch): Seq[(Long, Seq[Long])] =
    batch.rows.map { case (key, values) => (key, values.toSeq) }

  /** A clear value bit is the text `null`. The iterator is read as references. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def shownInts(parts: Vector[GroupedIntBatch]): Seq[(Int, Seq[String])] =
    parts.flatMap(_.rows).map { (key, values) =>
      val refs = values.iterator.asInstanceOf[Iterator[AnyRef]]
      (key, refs.map(cell => if (cell == null) "null" else cell.toString).toSeq)
    }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def shownLongs(parts: Vector[GroupedLongBatch]): Seq[(Long, Seq[String])] =
    parts.flatMap(_.rows).map { (key, values) =>
      val refs = values.iterator.asInstanceOf[Iterator[AnyRef]]
      (key, refs.map(cell => if (cell == null) "null" else cell.toString).toSeq)
    }

  private def longByKey(rows: Iterable[(Long, Iterable[Long])]): Map[Long, Seq[Long]] =
    rows.map { case (key, values) => key -> values.toSeq }.toMap

  private def valuesByKey(rows: Iterable[(Int, Iterable[Int])]): Map[Int, Seq[Int]] =
    rows.map { case (key, values) => key -> values.toSeq }.toMap

  /** Columnar `count` reads occupancy only. `collect` still builds one list per group. */
  private def assertCountBuildsNoLists[K, V](grouped: DistCollection[(K, Iterable[V])]): Unit =
    GroupByKey.resetValueListsBuilt()
    val counted = grouped.count()
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(counted)
    GroupByKey.valueListsBuilt shouldBe 0L
    GroupByKey.resetValueListsBuilt()
    val _ = grouped.collect()
    GroupByKey.valueListsBuilt should be > 0L

  private def countedAt(size: Int): Long =
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
    val grouped = DistCollection.ofIntPairs(keys, values, 4).groupByKey
    ColumnarPlanner.tryCount(grouped.plan).getOrElse(fail(s"batch $size did not count"))

  private def longCountedAt(size: Int): Long =
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
    val grouped = DistCollection.ofLongPairs(longKeys, longPayloads, 4).groupByKey
    ColumnarPlanner.tryCount(grouped.plan).getOrElse(fail(s"long batch $size did not count"))

  private def stringCountedAt(size: Int): Long =
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
    val grouped = stringSource.groupByKey
    ColumnarPlanner.tryCount(grouped.plan).getOrElse(fail(s"string batch $size did not count"))

  private def filteredCount(size: Int): Long =
    ScarletConf.set(ScarletConf.get.copy(columnarBatchSize = size))
    DistCollection
      .ofIntPairs(keys, values, 4)
      .filterKeys((key: Int) => key > 0)
      .filterValues((value: Int) => value > 20)
      .groupByKey
      .count()

  /**
   * Named erasure boundary. The source is boxed so a null key and a null value can sit beside
   * present ints. The null key is not a group. Key 2 has a null value and still counts.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Null",
    ),
  )
  private def assertNullKeyCount(): Unit =
    def cell(key: Any, value: Any): (Any, Any) = (key, value)
    val source = DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq(cell(1, 4), cell(null, 9), cell(5, 1), cell(1, 5))),
          Partition(Seq(cell(5, 3), cell(2, null), cell(null, 8))),
        ),
      ),
    )
    val grouped = source.groupByKey

    ColumnarPlanner.lower(grouped.plan).map(_.shape) shouldBe Some("GroupByKey(Exchange(Scan))")
    GroupByKey.resetValueListsBuilt()
    grouped.count() shouldBe 3L
    ColumnarPlanner.tryCount(grouped.plan) shouldBe Some(3L)
    GroupByKey.valueListsBuilt shouldBe 0L
    grouped.count() shouldBe row(grouped.count())

  /** Named erasure boundary. `Tuple2` keeps a null `Long`. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erasedLong(key: Option[Long], value: Option[Long]): (Long, Long) =
    val left: AnyRef = key match
      case Some(present) => Long.box(present)
      case None => null
    val right: AnyRef = value match
      case Some(present) => Long.box(present)
      case None => null
    Tuple2(left, right).asInstanceOf[(Long, Long)]

  /** Named erasure boundary. `Tuple2` keeps a null `String` or `Integer`. */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erasedString(key: Option[String], value: Option[Int]): (String, Int) =
    val left: AnyRef = key match
      case Some(present) => present
      case None => null
    val right: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(left, right).asInstanceOf[(String, Int)]

  /** Per-partition sequence, then the concatenation. Columnar `keys` builds no value lists. */
  private def assertKeysMatch[A](keys: DistCollection[A]): Unit =
    GroupByKey.resetValueListsBuilt()
    val found = keyPartitions(keys.plan)
    GroupByKey.valueListsBuilt shouldBe 0L
    found shouldBe row(keyPartitions(keys.plan))
    keys.collect().toList shouldBe row(keys.collect().toList)

  private def keyPartitions[A](plan: Plan[A]): Seq[Seq[A]] =
    ExecutionService.get.executePartitions(plan).map(_.data.toSeq)

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
