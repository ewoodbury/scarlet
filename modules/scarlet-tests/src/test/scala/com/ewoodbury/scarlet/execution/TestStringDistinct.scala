package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Columnar string `distinct`: one copy of each string, including one null and one `""`.
 *
 * The oracle is `values.toSet`. The row `Stage.distinct` dedupes per partition and is not the
 * oracle. Order is unspecified. `count` is the global cardinality.
 */
class TestStringDistinct extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get
  private val accent = "é"
  private val emoji = "😀"
  private val absent: String = BatchCodec.stringElement(None)

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

  "string distinct" should "keep one copy of a string that appears in two dictionaries" in {
    val left = Seq("", accent, emoji, "red", "", accent, absent)
    val right = Seq(emoji, "blue", absent, accent, "", "red", emoji)
    dictionaryIndex(left, accent) should not be dictionaryIndex(right, accent)
    dictionaryIndex(left, "") should not be dictionaryIndex(right, "")

    val values = left ++ right
    val distinct = strings(left, right).distinct()
    expect(distinct, values)
    val parts = stringBatches(distinct.plan)
    val flat = parts.flatMap(batch => BatchCodec.decodeStrings(batch))
    flat.count(BatchCodec.isAbsent) shouldBe 1
    flat.count(isEmpty) shouldBe 1
    parts.zipWithIndex.foreach { (batch, index) =>
      BatchCodec.decodeStrings(batch).foreach { text =>
        bucketOf(text, parts.length) shouldBe index
      }
    }
  }

  it should "keep both a null and an empty string in partition 0" in {
    val left = Seq(absent, "", absent, "a")
    val right = Seq("", absent, "")
    val distinct = strings(left, right).distinct()
    expect(distinct, left ++ right)
    val parts = stringBatches(distinct.plan)
    val zero = BatchCodec.decodeStrings(parts.headOption.getOrElse(fail("missing partition 0")))
    zero.count(BatchCodec.isAbsent) shouldBe 1
    zero.count(isEmpty) shouldBe 1
    parts.drop(1).foreach { batch =>
      val decoded = BatchCodec.decodeStrings(batch)
      decoded.exists(BatchCodec.isAbsent) shouldBe false
      decoded.exists(isEmpty) shouldBe false
    }
  }

  it should "dedupe an all-null bucket to one null" in {
    val buckets = DistinctKernel.strings(Vector(BatchCodec.strings(Seq(absent, absent))), 4)
    val decoded = buckets.flatMap(batch => BatchCodec.decodeStrings(batch))

    decoded.count(BatchCodec.isAbsent) shouldBe 1
    decoded.length shouldBe 1
  }

  it should "keep both strings that share a hash code" in {
    val leftCode = "Aa"
    val rightCode = "BB"
    leftCode.hashCode shouldBe rightCode.hashCode
    StringHash.hash(leftCode) shouldBe StringHash.hash(rightCode)

    val values = Seq(leftCode, rightCode, leftCode, rightCode, leftCode)
    val distinct = DistCollection(values, 2).distinct()
    expect(distinct, values)
    val parts = stringBatches(distinct.plan)
    val home = Math.floorMod(StringHash.hash(leftCode), parts.length)
    val decoded = BatchCodec.decodeStrings(
      parts.lift(home).getOrElse(fail(s"missing bucket $home")),
    )
    decoded.count(_ == leftCode) shouldBe 1
    decoded.count(_ == rightCode) shouldBe 1
  }

  it should "return empty for empty input, including one partition and a prime width" in {
    val emptySource = DistCollection.ofStrings(Array.empty[String], 3)
    ColumnarPlanner.lower(emptySource.distinct().plan).map(_.shape) shouldBe Some(
      "Dedupe(Exchange(Scan))",
    )
    emptySource.distinct().collect() shouldBe empty
    emptySource.distinct().count() shouldBe 0L
    val direct = DistinctKernel.strings(Vector.empty, 5)
    direct.length shouldBe 5
    direct.foreach { batch =>
      batch.length shouldBe 0
      batch.schema shouldBe Vector(LogicalType.Utf8Dict)
    }

    val values = Seq("red", "red", accent, "", absent, accent, emoji, "red")
    withWidth(1) {
      val distinct = DistCollection(values, 1).distinct()
      expect(distinct, values)
      stringBatches(distinct.plan).length shouldBe 1
    }
    withWidth(7) {
      val distinct = strings(values.take(4), values.drop(2)).distinct()
      expect(distinct, values)
      stringBatches(distinct.plan).length shouldBe 7
    }
  }

  it should "keep one null key and one empty key" in {
    val rows = DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq("" -> 1, nullKey(2), accent -> 3, "" -> 4)),
          Partition(Seq(nullKey(5), accent -> 6, emoji -> 7, "" -> 8)),
        ),
      ),
    )
    val distinct = rows.keys.distinct()
    ColumnarPlanner.lower(distinct.plan).map(_.shape) shouldBe Some(
      "Dedupe(Exchange(Key(Scan)))",
    )
    ColumnarPlanner.partitioning(distinct.plan).map(_.layout) shouldBe Some(
      Layout.Utf8Dict(ScarletConf.get.columnarBatchSize),
    )
    expect(distinct, Seq("", absent, accent, emoji))
  }

  it should "lower a string filter then distinct" in {
    val values = Seq("red", "blue", "red", "green", "blue")
    val distinct = DistCollection(values, 2)
      .filter((text: String) => text != "green")
      .distinct()
    ColumnarPlanner.lower(distinct.plan).map(_.shape) shouldBe Some(
      "Dedupe(Exchange(Filter(Scan)))",
    )
    expect(distinct, Seq("red", "blue"))
  }

  it should "refuse take because distinct is not partition-preserving" in {
    val plan = DistCollection(Seq("a", "a", "b"), 2).distinct().plan
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("distinct stayed on rows"))
    lowered.shape shouldBe "Dedupe(Exchange(Scan))"
    PartitionPreserving.of(lowered) shouldBe Option.empty[Int]
    ColumnarPlanner.tryTake(plan, 1) shouldBe None
    an[ColumnarFallback] should be thrownBy {
      ColumnarInterpreter.interpret(plan, lowered, Some(0))
    }
  }

  it should "leave an all-null string source on the row path" in {
    val source = Plan.Source(Seq(Partition(Seq(absent, absent))))
    ColumnarPlanner.lower(source) shouldBe None
    ColumnarPlanner.lower(Plan.DistinctOp(source)) shouldBe None
    ColumnarPlanner.lower(DistCollection(Seq(1L, 1L), 2).distinct().plan) shouldBe None
  }

  private def strings(left: Seq[String], right: Seq[String]): DistCollection[String] =
    DistCollection(Plan.Source(Seq(Partition(left), Partition(right))))

  private def expect(distinct: DistCollection[String], values: Seq[String]): Unit =
    val got = distinct.collect()
    got.toSet shouldBe values.toSet
    got.size shouldBe values.toSet.size
    distinct.count() shouldBe values.toSet.size.toLong

  private def stringBatches(plan: Plan[_]): Vector[ColumnBatch] =
    val lowered = ColumnarPlanner.lower(plan).getOrElse(fail("expected a lowered plan"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.Strings(parts) => parts
      case other => fail(s"expected strings, got $other")

  private def dictionaryIndex(values: Seq[String], text: String): Int =
    BatchCodec.strings(values).columns.toList match
      case (column: DictUtf8Column) :: _ => column.dictionary.indexOf(text)
      case other => fail(s"expected a dictionary, found $other")

  private def bucketOf(text: String, partitions: Int): Int =
    if (BatchCodec.isAbsent(text)) then 0 else Math.floorMod(StringHash.hash(text), partitions)

  private def isEmpty(text: String): Boolean =
    !BatchCodec.isAbsent(text) && text.isEmpty

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def nullKey(value: Int): (String, Int) = (null: String) -> value

  private def withWidth(width: Int)(body: => Unit): Unit =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(defaultShufflePartitions = width))
      ScarletRuntime.get.shuffle.clear()
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()
