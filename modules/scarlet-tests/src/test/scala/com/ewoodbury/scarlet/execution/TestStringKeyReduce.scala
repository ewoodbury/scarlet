package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.HashPartitioner

/**
 * Columnar `reduceByKey` on `(String, Int)`.
 *
 * The row path is the oracle. Each input partition interns its own dictionary, and the second
 * partition sees the same strings in a different order. A null key is dropped. `""` is kept.
 * Output order inside a bucket is first-seen. `(String, Long)`, `groupByKey`, and `join` stay on
 * the row path.
 */
class TestStringKeyReduce extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get
  private val accent = "é"
  private val decomposed = "e\u0301"
  private val emoji = "😀"

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(
      original.copy(
        columnarExecution = true,
        columnarBatchSize = 2,
        threadPoolSize = 2,
        defaultShufflePartitions = 4,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "reduceByKey" should "match the row path for string keys from two dictionaries" in {
    val leftDictionary = dictionaryOf(leftRows)
    val rightDictionary = dictionaryOf(rightRows)
    leftDictionary.head shouldBe ""
    rightDictionary.head shouldBe emoji
    leftDictionary.indexOf(accent) should not be rightDictionary.indexOf(accent)
    leftDictionary.indexOf("") should not be rightDictionary.indexOf("")

    val rows = source
    val reduced = rows.reduceByKey[String, Int]((left, right) => left + right)
    val summed = reduced.collect().toMap
    val expected = grouped((left, right) => left + right)

    summed shouldBe expected.flatten.toMap
    summed shouldBe row(
      rows.reduceByKey[String, Int]((left, right) => left + right).collect().toMap,
    )
    summed("") shouldBe 3
    summed(accent) shouldBe 9
    summed(decomposed) shouldBe 11
    summed(emoji) shouldBe 11
    summed("red") shouldBe 10
    summed("blue") shouldBe 3
    hasNullKey(summed.keys) shouldBe false
    StringHash.hash("") shouldBe 0

    ColumnarPlanner.lower(reduced.plan).map(_.shape) shouldBe Some(
      "StringHashAggregate(Exchange(Scan))",
    )
    val info = ColumnarPlanner.partitioning(reduced.plan).getOrElse(fail("reduce stayed on rows"))
    info.layout shouldBe Layout.StringPair(ScarletConf.get.columnarBatchSize)
    info.distribution shouldBe Distribution.Hash(
      ScarletConf.get.defaultShufflePartitions,
      PartitioningInfo.RowKeyOrdinals,
    )
    demand(reduced) shouldBe Set(0, 1)
    assertBuckets(reduced.plan, expected)
  }

  it should "match the row path for a max reducer" in {
    val rows = source
    val reduced = rows.reduceByKey[String, Int](math.max)
    val maximized = reduced.collect().toMap
    val expected = grouped(math.max)

    maximized shouldBe expected.flatten.toMap
    maximized shouldBe row(rows.reduceByKey[String, Int](math.max).collect().toMap)
    maximized("") shouldBe 2
    maximized(accent) shouldBe 6
    maximized(decomposed) shouldBe 8
    maximized(emoji) shouldBe 7
    maximized("red") shouldBe 5
    maximized("blue") shouldBe 3
    hasNullKey(maximized.keys) shouldBe false
    assertBuckets(reduced.plan, expected)
  }

  it should "project keys and values and filter keys above the aggregate" in {
    val rows = source
    val reduced = rows.reduceByKey[String, Int]((left, right) => left + right)
    val keyed = reduced.keys
    val valued = reduced.values
    val named = reduced.filterKeys(keepNamed)
    val summed = grouped((left, right) => left + right).flatten.toMap

    ColumnarPlanner.lower(keyed.plan).map(_.shape) shouldBe Some(
      "Key(StringHashAggregate(Exchange(Scan)))",
    )
    ColumnarPlanner.lower(valued.plan).map(_.shape) shouldBe Some(
      "Value(StringHashAggregate(Exchange(Scan)))",
    )
    ColumnarPlanner.lower(named.plan).map(_.shape) shouldBe Some(
      "FilterKey(StringHashAggregate(Exchange(Scan)))",
    )
    layout(keyed) shouldBe Layout.Utf8Dict(ScarletConf.get.columnarBatchSize)
    layout(valued) shouldBe Layout.Columnar(ScarletConf.get.columnarBatchSize)
    layout(named) shouldBe Layout.StringPair(ScarletConf.get.columnarBatchSize)
    demand(keyed) shouldBe Set(0, 1)
    demand(valued) shouldBe Set(0, 1)
    demand(named) shouldBe Set(0, 1)

    keyed.collect().toSet shouldBe summed.keySet
    keyed.collect().toSet shouldBe row(rows.reduceByKey[String, Int](_ + _).keys.collect().toSet)
    valued.collect().toSeq.sorted shouldBe summed.values.toSeq.sorted
    valued.collect().toSeq.sorted shouldBe row(
      rows.reduceByKey[String, Int](_ + _).values.collect().toSeq.sorted,
    )
    named.collect().toMap shouldBe summed.filter { case (key, _) => keepNamed(key) }
    named.collect().toMap shouldBe row(
      rows.reduceByKey[String, Int](_ + _).filterKeys(keepNamed).collect().toMap,
    )
    named.collect().toMap.contains("") shouldBe true
    hasNullKey(named.collect().toMap.keys) shouldBe false
  }

  it should "keep a long value, groupByKey, and a join on the row path" in {
    val rows = source
    val longs = DistCollection(Seq("a" -> 1L, "b" -> 2L), 2)
    val nullValue = DistCollection(
      Plan.Source(Seq(Partition(Seq(erasedNullValue("blue"), "red" -> 1)))),
    )

    ColumnarPlanner.lower(
      longs.reduceByKey[String, Long]((left, right) => left + right).plan,
    ) shouldBe None
    ColumnarPlanner.lower(rows.groupByKey.plan) shouldBe None
    ColumnarPlanner.lower(rows.join(rows).plan) shouldBe None
    ColumnarPlanner.lower(nullValue.reduceByKey[String, Int](_ + _).plan) shouldBe None
  }

  it should "emit one empty string-pair batch per shuffle partition" in {
    val width = 4
    val direct = StringHashReduce.byKey(
      Vector.empty,
      numPartitions = width,
      parallelism = 2,
      batchSize = 2,
      (left, right) => left + right,
    )
    direct.length shouldBe width
    direct.foreach(assertEmptyPair)

    val dropped = source.filterKeys((_: String) => false).reduceByKey[String, Int](_ + _)
    val parts = stringPairs(dropped.plan)
    parts.length shouldBe ScarletConf.get.defaultShufflePartitions
    parts.foreach(assertEmptyPair)
    dropped.collect() shouldBe empty
  }

  private def source: DistCollection[(String, Int)] =
    DistCollection(Plan.Source(Seq(Partition(leftRows), Partition(rightRows))))

  private def leftRows: Seq[(String, Int)] = Seq(
    "" -> 1,
    accent -> 2,
    decomposed -> 3,
    emoji -> 4,
    "red" -> 5,
    nullKey(9),
    "red" -> 1,
    accent -> 6,
  )

  private def rightRows: Seq[(String, Int)] = Seq(
    emoji -> 7,
    decomposed -> 8,
    accent -> 1,
    "" -> 2,
    "blue" -> 3,
    "red" -> 4,
  )

  /** First-seen order inside each hash bucket. A null key is omitted. */
  private def grouped(op: (Int, Int) => Int): Vector[Seq[(String, Int)]] =
    val parts = ScarletConf.get.defaultShufflePartitions
    val partitioner = new HashPartitioner
    val seed = Vector.fill(parts)(Vector.empty[(String, Int)])
    Seq(leftRows, rightRows).flatten.foldLeft(seed) { (buckets, pair) =>
      val (key, value) = pair
      if (hasNullKey(Seq(key))) then buckets
      else
        val index = partitioner.partition(key, parts)
        val bucket = buckets(index)
        val at = bucket.indexWhere { case (found, _) => sameKey(found, key) }
        val updated =
          if (at < 0) then bucket :+ (key, value)
          else
            val (found, current) = bucket(at)
            bucket.updated(at, (found, op(current, value)))
        buckets.updated(index, updated)
    }

  private def assertBuckets(plan: Plan[_], expected: Vector[Seq[(String, Int)]]): Unit =
    val parts = stringPairs(plan)
    parts.length shouldBe expected.length
    val partitioner = new HashPartitioner
    parts.zip(expected).zipWithIndex.foreach { case ((batch, rows), index) =>
      batch.schema shouldBe Vector(LogicalType.Utf8Dict, LogicalType.Int32)
      val decoded = BatchCodec.decodeStringPairs(batch)
      decoded shouldBe rows
      decoded.foreach { (key, _) =>
        hasNullKey(Seq(key)) shouldBe false
        partitioner.partition(key, parts.length) shouldBe index
        Math.floorMod(StringHash.hash(key), parts.length) shouldBe index
      }
      assertPresent(batch)
    }
    parts.headOption.foreach { batch =>
      BatchCodec.decodeStringPairs(batch).exists { case (key, _) => key.isEmpty } shouldBe true
    }

  private def assertPresent(batch: ColumnBatch): Unit =
    batch.columns.toList match
      case (keys: DictUtf8Column) :: (values: Int32Column) :: Nil =>
        (0 until batch.length).foreach { row =>
          keys.isValid(row) shouldBe true
          values.isValid(row) shouldBe true
        }
      case other => fail(s"expected a string pair, found $other")

  private def assertEmptyPair(batch: ColumnBatch): Unit =
    batch.length shouldBe 0
    batch.schema shouldBe Vector(LogicalType.Utf8Dict, LogicalType.Int32)

  private def stringPairs(plan: Plan[_]): Vector[ColumnBatch] =
    val lowered =
      ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered plan"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.StringPairs(parts) => parts
      case other => fail(s"expected string pairs, got $other")

  private def layout[A](collection: DistCollection[A]): Layout =
    ColumnarPlanner
      .partitioning(collection.plan)
      .getOrElse(fail("expected a columnar plan"))
      .layout

  private def demand[A](collection: DistCollection[A]): Set[Int] =
    val op = ColumnarPlanner.lower(collection.plan).getOrElse(fail("expected a columnar plan"))
    ColumnUse.ordinals(collection.plan, op)

  private def dictionaryOf(rows: Seq[(String, Int)]): Seq[String] =
    BatchCodec.stringIntPairs(rows).columns.toList match
      case (column: DictUtf8Column) :: _ => column.dictionary.toSeq
      case other => fail(s"expected a dictionary, found $other")

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

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def hasNullKey(keys: Iterable[String]): Boolean =
    keys.exists {
      case null => true
      case _ => false
    }

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Null"))
  private def sameKey(left: String, right: String): Boolean = (left, right) match
    case (null, null) => true
    case (null, _) | (_, null) => false
    case (present, other) => present == other

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Null"))
  private def keepNamed(key: String): Boolean = key match
    case null => false
    case text => text.isEmpty || text == "red" || text == "blue"

  private def row[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
