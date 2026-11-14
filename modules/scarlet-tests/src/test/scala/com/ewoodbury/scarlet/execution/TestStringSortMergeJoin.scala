package com.ewoodbury.scarlet.execution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * String inner sort-merge against the row path.
 *
 * [[JoinDigest.fromString]] compares the multiset. A direct kernel check pins match order. The
 * over-budget case throws and writes no spill file.
 */
class TestStringSortMergeJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val shape = "StringSortMergeJoin(Exchange(Scan), Exchange(Scan))"
  private val accent = "é"
  private val decomposed = "e\u0301"

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletConf.set(originalConf.copy(columnarExecution = true, defaultShufflePartitions = 4))
    ScarletRuntime.get.shuffle.clear()
    StringHashJoinSpill.resetForTest()
    SortMergeRuns.resetForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    StringHashJoinSpill.resetForTest()
    SortMergeRuns.resetForTest()
    ()

  "string inner sort-merge" should "match the row path when either side is empty" in {
    Seq(1, 4).foreach { width =>
      val present = DistCollection(Seq("a" -> 1, "b" -> 2), width)
      val empty = present.filterKeys((_: String) => false)
      val noRows = Seq.empty[(String, (Int, Int))]
      val filtered = "Exchange(FilterKey(Scan))"
      val scan = "Exchange(Scan)"
      agree(empty.sortMergeJoin(present), noRows, width, s"StringSortMergeJoin($filtered, $scan)")
      agree(present.sortMergeJoin(empty), noRows, width, s"StringSortMergeJoin($scan, $filtered)")
      agree(
        empty.sortMergeJoin(empty),
        noRows,
        width,
        s"StringSortMergeJoin($filtered, $filtered)",
      )
    }
  }

  it should "emit duplicate keys in match order and keep a null value" in {
    // Left "a" is dictionary code 1. Right "a" is code 0. The match is the decoded string.
    // The last row of each side is a null key and must not match "".
    val left = batch(
      Array("b", "a", ""),
      Array(0, 1, 2, 1, 0),
      Array(true, true, true, true, false),
      Array(20, 10, 7, 11, 99),
      Array(true, true, true, false, true),
    )
    val right = batch(
      Array("a", "c", ""),
      Array(0, 1, 0, 2, 0),
      Array(true, true, true, true, false),
      Array(100, 30, 0, 8, 5),
      Array(true, true, false, true, true),
    )
    val joined = StringSortMerge.inner(left, right)
    shown(BatchCodec.decodeStringJoinKeepingNulls(joined, joined.length)) shouldBe Seq(
      ",7,8",
      "a,10,100",
      "a,10,null",
      "a,null,100",
      "a,null,null",
    )
  }

  it should "drop a null key and agree with the row path" in {
    Seq(1, 4).foreach { width =>
      val left = pairs(
        Seq(
          erased("a", intRef(10)),
          erased(absentKey, intRef(99)),
          erased("", intRef(7)),
          erased("d", intRef(40)),
        ),
        width,
      )
      val right = pairs(
        Seq(
          erased("a", intRef(100)),
          erased(absentKey, intRef(7)),
          erased("", intRef(8)),
          erased("d", absent),
        ),
        width,
      )
      val expected = Seq(
        pair("", pair(intRef(7), intRef(8))),
        pair("a", pair(intRef(10), intRef(100))),
        pair("d", pair(intRef(40), absent)),
      )
      agree(left.sortMergeJoin(right), expected, width, shape)
    }
  }

  it should "match a key present ten times on one side" in {
    Seq(1, 4).foreach { width =>
      val left = DistCollection(Seq.tabulate(10)(index => "k" -> index) :+ ("z" -> 90), width)
      val right = DistCollection(Seq("k" -> 100, "k" -> 101, "m" -> 80), width)
      val expected = for
        leftValue <- 0 until 10
        rightValue <- Seq(100, 101)
      yield "k" -> ((leftValue, rightValue))
      agree(left.sortMergeJoin(right), expected, width, shape)
    }
  }

  it should "match an empty string and keep an accent distinct from its decomposition" in {
    accent should not be decomposed
    Seq(1, 4).foreach { width =>
      val left = DistCollection(Seq("" -> 1, accent -> 2, decomposed -> 3), width)
      val right = DistCollection(Seq("" -> 10, decomposed -> 20, accent -> 30), width)
      val expected = Seq(
        "" -> ((1, 10)),
        accent -> ((2, 30)),
        decomposed -> ((3, 20)),
      )
      agree(left.sortMergeJoin(right), expected, width, shape)
    }
  }

  it should "lower to StringSortMergeJoin and leave hash and broadcast on their nodes" in {
    val left = DistCollection(Seq("a" -> 1, "b" -> 2), 4)
    val right = DistCollection(Seq("a" -> 10, "c" -> 3), 4)
    ColumnarPlanner.lower(left.sortMergeJoin(right).plan).map(_.shape) shouldBe Some(shape)
    ColumnarPlanner.lower(left.sortMergeJoin(right).plan).map(_.kind) shouldBe Some(
      PhysicalOp.Kind.StringTriples,
    )
    ColumnarPlanner.lower(left.join(right).plan).map(_.shape) shouldBe Some(
      "StringHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(left.shuffleHashJoin(right).plan).map(_.shape) shouldBe Some(
      "StringHashJoin(Exchange(Scan), Exchange(Scan))",
    )
    ColumnarPlanner.lower(left.broadcastJoin(right).plan).map(_.shape) shouldBe Some(
      "StringBroadcastJoin(Scan, Scan)",
    )
    ColumnarPlanner.tryTake(left.sortMergeJoin(right).plan, 1) shouldBe None
  }

  it should "throw on an over-budget sort-merge and write no spill file" in {
    ScarletConf.set(
      ScarletConf.get.copy(spillEnabled = true, spillBudgetBytes = 1L, columnarBatchSize = 4),
    )
    val left = DistCollection(Seq("a" -> 1, "b" -> 2), 4)
    val right = DistCollection(Seq("a" -> 10, "c" -> 3), 4)
    val plan = left.sortMergeJoin(right).plan
    val thrown = intercept[IllegalStateException] {
      val _ = ColumnarPlanner.tryRun(plan)
    }
    thrown should not be a[ColumnarFallback]
    thrown.getMessage should include("string inner sort-merge spill retains about")
    thrown.getMessage should include("above the budget of 1 bytes")
    SortMergeRuns.filesWrittenForTest shouldBe 0L
    SortMergeRuns.lastDirectoryForTest shouldBe ""
    StringHashJoinSpill.filesWrittenForTest shouldBe 0L
    StringHashJoinSpill.lastDirectoryForTest shouldBe ""
    val collected = intercept[IllegalStateException] {
      val _ = left.sortMergeJoin(right).collect()
    }
    collected should not be a[ColumnarFallback]
    SortMergeRuns.filesWrittenForTest shouldBe 0L
    StringHashJoinSpill.filesWrittenForTest shouldBe 0L
  }

  private def agree(
      joined: DistCollection[(String, (Int, Int))],
      expected: Iterable[Product],
      width: Int,
      expectedShape: String,
  ): Unit =
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = width))
    val columnar = joined.collect().toList
    val rowRows = row(joined.collect().toList)
    shown(columnar) should contain theSameElementsAs shown(expected)
    val left = JoinDigest.fromString(columnar)
    val right = JoinDigest.fromString(rowRows)
    withClue(JoinDigest.difference(left, right)) {
      JoinDigest.agrees(left, right) shouldBe true
    }
    ColumnarPlanner.lower(joined.plan).map(_.shape) shouldBe Some(expectedShape)
    stringTripleBatches(joined.plan).length shouldBe width

  private def stringTripleBatches(plan: Plan[_]): Vector[ColumnBatch] =
    val lowered =
      ColumnarPlanner.lower(plan).getOrElse(fail(s"expected a lowered plan, got $plan"))
    ColumnarInterpreter.interpret(plan, lowered) match
      case ColumnarInterpreter.Columns.StringTriples(parts) => parts
      case other => fail(s"expected string triples, got $other")

  private def batch(
      dictionary: Array[String],
      codes: Array[Int],
      keyPresent: Array[Boolean],
      values: Array[Int],
      valuePresent: Array[Boolean],
  ): ColumnBatch =
    val width = codes.length
    val keyColumn = DictUtf8Column.of(
      dictionary,
      codes,
      Validity.pack(width, row => keyPresent(row)),
      width,
    )
    val valueColumn =
      Int32Column.of(values, Validity.pack(width, row => valuePresent(row)), width)
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(keyColumn, valueColumn),
      width,
    )

  private def pairs(
      rows: Seq[(String, Int)],
      width: Int,
  ): DistCollection[(String, Int)] =
    val buckets = Array.fill(width)(Seq.newBuilder[(String, Int)])
    rows.zipWithIndex.foreach { (entry, index) =>
      val _ = buckets(index % width) += entry
    }
    DistCollection(Plan.Source(buckets.toIndexedSeq.map(builder => Partition(builder.result()))))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def pair[A, B](key: A, value: B): (A, B) =
    Tuple2(key, value).asInstanceOf[(A, B)]

  /**
   * A `(String, Int)` cell. The cast is on the tuple, so a null `Integer` is not unboxed to `0`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def erased(key: String, value: Integer): (String, Int) =
    Tuple2[String, Integer](key, value).asInstanceOf[(String, Int)]

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absent: Integer = null

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def absentKey: String = null

  private def intRef(value: Int): Integer = Integer.valueOf(value)

  /**
   * Named erasure boundary. A join row may hold a null int. `productElement` reads it without
   * unboxing. Tuple `==` unboxes a null int.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def shown(rows: Iterable[Product]): Seq[String] =
    rows.toSeq.map { entry =>
      val values = entry.productElement(1).asInstanceOf[Product]
      Seq(entry.productElement(0), values.productElement(0), values.productElement(1))
        .map {
          case null => "null"
          case value: Int => String.valueOf(value)
          case value: String => value
          case _ => "other"
        }
        .mkString(",")
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
