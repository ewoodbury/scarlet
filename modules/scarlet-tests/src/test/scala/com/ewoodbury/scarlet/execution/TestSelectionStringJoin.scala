package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.columnar.StringHashJoin.StringJoinInput
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * String inner shuffle-hash join over a filter selection.
 *
 * A filter stack on either side, including one filter, keeps the survivor index. The exchange scatters those rows
 * and does not slice them. A null key on a selected row is dropped. `""` matches. A null value
 * stays a clear validity bit and is not an absence bit. The two sides keep independent
 * dictionaries.
 */
class TestSelectionStringJoin extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val originalConf = ScarletConf.get
  private val buildLeft = Seq("a" -> 1, "b" -> 2, "no" -> 3)
  private val buildRight = Seq("a" -> 4, "c" -> -1, "b" -> 6)
  private val probeLeft = Seq("b" -> 10, "no" -> 11, "a" -> 12)
  private val probeRight = Seq("a" -> -5, "d" -> 14, "c" -> 15)
  private val buildOnlyMatches = Seq(
    "a" -> ((1, 12)),
    "a" -> ((1, -5)),
    "a" -> ((4, 12)),
    "a" -> ((4, -5)),
    "b" -> ((2, 10)),
    "b" -> ((6, 10)),
  )
  private val probeOnlyMatches = Seq(
    "a" -> ((1, 12)),
    "a" -> ((4, 12)),
    "b" -> ((2, 10)),
    "b" -> ((6, 10)),
    "c" -> ((-1, 15)),
  )
  private val bothMatches = Seq(
    "a" -> ((1, 12)),
    "a" -> ((4, 12)),
    "b" -> ((2, 10)),
    "b" -> ((6, 10)),
  )
  private val buildShape =
    "StringHashJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(Scan))"
  private val probeShape =
    "StringHashJoin(Exchange(Scan), Exchange(FilterValue(FilterKey(Scan))))"
  private val bothShape =
    "StringHashJoin(Exchange(FilterValue(FilterKey(Scan))), " +
      "Exchange(FilterValue(FilterKey(Scan))))"
  private val tripleSchema = Vector(LogicalType.Utf8Dict, LogicalType.Int32, LogicalType.Int32)

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        // Three rows fit in one batch, so the dense join does not publish an extra slice. The
        // batch-count gap is the compact the selection does not publish.
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

  "a two-filter string inner join" should "match the row path and skip the compact on the build side" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val build = buildPairs
    val probe = probePairs
    val filtered = keep(build, keyCalls, valueCalls)
    val joined = filtered.join(probe)

    shape(joined) shouldBe buildShape
    val found = joined.collect().toSeq
    // One pass: the key predicate sees every build row, and the value predicate sees the five
    // keys that passed. A second interpret would double both counts.
    keyCalls.get() shouldBe 6
    valueCalls.get() shouldBe 5
    selectionOf(filtered.plan).isDefined shouldBe true
    found should contain theSameElementsAs buildOnlyMatches
    found should contain theSameElementsAs row(joined.collect().toSeq)
    val hinted = filtered.shuffleHashJoin(probe)
    shape(hinted) shouldBe buildShape
    hinted.collect().toSeq should contain theSameElementsAs buildOnlyMatches
    assertFewerBatches(joined, build, probe, Some(filtered), None)
  }

  it should "match the row path and skip the compact on the probe side" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val build = buildPairs
    val probe = probePairs
    val filtered = keep(probe, keyCalls, valueCalls)
    val joined = build.join(filtered)

    shape(joined) shouldBe probeShape
    val found = joined.collect().toSeq
    keyCalls.get() shouldBe 6
    valueCalls.get() shouldBe 5
    selectionOf(filtered.plan).isDefined shouldBe true
    found should contain theSameElementsAs probeOnlyMatches
    found should contain theSameElementsAs row(joined.collect().toSeq)
    assertFewerBatches(joined, build, probe, None, Some(filtered))
  }

  it should "match the row path and skip the compact on both sides" in {
    val buildKeyCalls = new AtomicInteger
    val buildValueCalls = new AtomicInteger
    val probeKeyCalls = new AtomicInteger
    val probeValueCalls = new AtomicInteger
    val build = buildPairs
    val probe = probePairs
    val filteredBuild = keep(build, buildKeyCalls, buildValueCalls)
    val filteredProbe = keep(probe, probeKeyCalls, probeValueCalls)
    val joined = filteredBuild.join(filteredProbe)

    shape(joined) shouldBe bothShape
    val found = joined.collect().toSeq
    buildKeyCalls.get() shouldBe 6
    buildValueCalls.get() shouldBe 5
    probeKeyCalls.get() shouldBe 6
    probeValueCalls.get() shouldBe 5
    selectionOf(filteredBuild.plan).isDefined shouldBe true
    selectionOf(filteredProbe.plan).isDefined shouldBe true
    found should contain theSameElementsAs bothMatches
    found should contain theSameElementsAs row(joined.collect().toSeq)
    assertFewerBatches(joined, build, probe, Some(filteredBuild), Some(filteredProbe))
  }

  it should "drop a null key on a selected row and match an empty string" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val build = DistCollection(
      Seq("" -> 1, nullKey(99), "red" -> 5, "no" -> 3),
      2,
    )
    val filtered = build
      .filterKeys { (key: String) =>
        val _ = keyCalls.incrementAndGet()
        keepNull(key)
      }
      .filterValues { (value: Int) =>
        val _ = valueCalls.incrementAndGet()
        value > 0
      }
    val probe = DistCollection(Seq("" -> 10, nullKey(7), "red" -> 8), 2)
    val joined = filtered.join(probe)
    val expected = Seq("" -> ((1, 10)), "red" -> ((5, 8)))

    shape(joined) shouldBe buildShape
    val found = joined.collect().toSeq
    // The value predicate sees the null key, so that row survived the key filter. The exchange
    // still drops it. `""` is a present key and matches.
    keyCalls.get() shouldBe 4
    valueCalls.get() shouldBe 3
    found should contain theSameElementsAs expected
    found.exists { case (key, _) => BatchCodec.isAbsent(key) } shouldBe false
    found.map(_._1) should contain("")
    found should contain theSameElementsAs row(joined.collect().toSeq)
    ColumnarPlanner.tryRun(joined.plan).isDefined shouldBe true
  }

  it should "emit the cartesian product of duplicate keys" in {
    val build = DistCollection(Seq("a" -> 1, "a" -> 2, "b" -> 3), 2)
      .filterKeys((_: String) => true)
      .filterValues((_: Int) => true)
    val probe = DistCollection(Seq("a" -> 10, "a" -> 11, "c" -> 4), 2)
      .filterKeys((_: String) => true)
      .filterValues((_: Int) => true)
    val joined = build.join(probe)
    val expected = Seq(
      "a" -> ((1, 10)),
      "a" -> ((1, 11)),
      "a" -> ((2, 10)),
      "a" -> ((2, 11)),
    )

    shape(joined) shouldBe bothShape
    selectionOf(build.plan).isDefined shouldBe true
    val found = joined.collect().toSeq
    found should contain theSameElementsAs expected
    found should contain theSameElementsAs row(joined.collect().toSeq)
  }

  it should "join nothing from an empty selection" in {
    val batch = BatchCodec.stringIntPairs(Seq("" -> 1, "red" -> 2))
    val empty = Selected(batch, Array(0, 1), 0)
    val probe = BatchCodec.stringIntPairs(Seq("" -> 10, "red" -> 20))
    val parts = StringHashJoin.inner(
      StringJoinInput.Selected(Vector(empty)),
      StringJoinInput.Dense(Vector(probe)),
      numPartitions = 4,
      parallelism = 1,
    )

    parts.length shouldBe 4
    parts.map(_.length).sum shouldBe 0
    parts.foreach { batch =>
      batch.schema shouldBe tripleSchema
    }
  }

  it should "drop every row when both filters reject the batch" in {
    val joined = buildPairs
      .filterKeys((_: String) => false)
      .filterValues((_: Int) => true)
      .join(probePairs)

    shape(joined) shouldBe buildShape
    joined.collect() shouldBe empty
    joined.collect() shouldBe row(joined.collect())
    published(joined.collect()) should be > 0L
  }

  it should "keep every row when both filters pass" in {
    val build = DistCollection(Seq("" -> 1, nullKey(9), "red" -> 2), 2)
    val probe = DistCollection(Seq("" -> 10, nullKey(8), "red" -> 3), 2)
    val joined = build
      .filterKeys((_: String) => true)
      .filterValues((_: Int) => true)
      .join(probe)
    val expected = Seq("" -> ((1, 10)), "red" -> ((2, 3)))

    shape(joined) shouldBe buildShape
    val found = joined.collect().toSeq
    found should contain theSameElementsAs expected
    found should contain theSameElementsAs build.join(probe).collect().toSeq
    found should contain theSameElementsAs row(joined.collect().toSeq)
    found.exists { case (key, _) => BatchCodec.isAbsent(key) } shouldBe false
  }

  it should "join one hot key across morsels" in {
    val rows = 20
    val build = DistCollection(Seq.tabulate(rows)(index => "hot" -> (index + 1)), 2)
      .filterKeys((key: String) => key.nonEmpty)
      .filterValues((value: Int) => value > 0)
    val joined = build.join(DistCollection(Seq("hot" -> 100), 2))
    val expected = (1 to rows).map(value => "hot" -> ((value, 100)))

    shape(joined) shouldBe buildShape
    val found = joined.collect().toSeq
    found should contain theSameElementsAs expected
    found should contain theSameElementsAs row(joined.collect().toSeq)
    published(joined.collect()) should be > 0L
  }

  it should "match two dictionaries by string when their intern order differs" in {
    val build = DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq("b" -> 1, "a" -> 2)),
          Partition(Seq("a" -> 3)),
        ),
      ),
    ).filterKeys((key: String) => keepKey(key)).filterValues((value: Int) => value > 0)
    val probe = DistCollection(
      Plan.Source(
        Seq(
          Partition(Seq("a" -> 10, "z" -> 7)),
          Partition(Seq("b" -> 20)),
        ),
      ),
    )
    val joined = build.join(probe)
    val expected = Seq("b" -> ((1, 20)), "a" -> ((2, 10)), "a" -> ((3, 10)))

    shape(joined) shouldBe buildShape
    val found = joined.collect().toSeq
    found should contain theSameElementsAs expected
    found should contain theSameElementsAs row(joined.collect().toSeq)
  }

  it should "join one filter through the survivor index" in {
    val calls = new AtomicInteger
    val build = buildPairs.filterValues { (value: Int) =>
      calls.incrementAndGet()
      value > 0
    }
    val joined = build.join(probePairs)

    shape(joined) shouldBe "StringHashJoin(Exchange(FilterValue(Scan)), Exchange(Scan))"
    val found = joined.collect().toSeq
    val columnarCalls = calls.get()
    found should contain theSameElementsAs row(joined.collect().toSeq)
    columnarCalls shouldBe buildLeft.length + buildRight.length
    selectionOf(build.plan) shouldBe defined
  }

  it should "lower a broadcast hint and a sort-merge hint through a filter" in {
    val left = buildPairs.filterKeys((key: String) => keepKey(key)).filterValues((_: Int) => true)
    val right = probePairs
    val filtered = "Exchange(FilterValue(FilterKey(Scan)))"
    val scan = "Exchange(Scan)"
    val broadcast = left.broadcastJoin(right)

    shape(broadcast) shouldBe "StringBroadcastJoin(FilterValue(FilterKey(Scan)), Scan)"
    broadcast.collect().toList should contain theSameElementsAs row(broadcast.collect().toList)
    val merged = left.sortMergeJoin(right)
    shape(merged) shouldBe
      "StringSortMergeJoin(Exchange(FilterValue(FilterKey(Scan))), Exchange(Scan))"
    merged.collect().toList should contain theSameElementsAs row(merged.collect().toList)
    Seq(
      (left.leftOuterJoin(right), s"StringHashLeftOuter($filtered, $scan)"),
      (left.rightOuterJoin(right), s"StringHashRightOuter($filtered, $scan)"),
      (left.fullOuterJoin(right), s"StringHashFullOuter($filtered, $scan)"),
    ).foreach { (joined, expected) =>
      shape(joined) shouldBe expected
      ColumnarPlanner.tryRun(joined.plan).isDefined shouldBe true
      joined.collect().toList should contain theSameElementsAs row(joined.collect().toList)
    }
  }

  it should "keep a matched null value" in {
    val source = DistCollection(
      Plan.Source(Seq(Partition(Seq("red" -> 1, erasedNullValue("red"), "red" -> 2)))),
    )
    val joined = source.filterKeys((_: String) => true).join(DistCollection(Seq("red" -> 3), 1))
    val op = ColumnarPlanner.lower(joined.plan).getOrElse(fail("null value join did not lower"))

    op.shape should include("StringHashJoin")
    ColumnarPlanner.tryRun(joined.plan).isDefined shouldBe true
    val found = joined.collect().toSeq
    shown(found) should contain theSameElementsAs shown(row(joined.collect().toSeq))
    shown(found) should contain("red,null,3")
  }

  "StringJoinInput" should "read only the selected prefix across two dictionaries" in {
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
    // Row 1 is `"red"`. It sits outside the live prefix, so a scan of the whole array would
    // match it.
    val leftSelected = Selected(left, Array(0, 2), 2)
    val rightSelected = Selected(right, Array(0, 1, 2), 3)
    val probeDictionary = utf8(right).dictionary
    val parts = StringHashJoin.inner(
      StringJoinInput.Selected(Vector(leftSelected)),
      StringJoinInput.Selected(Vector(rightSelected)),
      numPartitions = 1,
      parallelism = 1,
    )
    val gathered = StringHashJoin.inner(
      Vector(Selection.materialize(leftSelected)),
      Vector(Selection.materialize(rightSelected)),
      partitions = 4,
      parallelism = 2,
    )
    val wide = StringHashJoin.inner(
      StringJoinInput.Selected(Vector(leftSelected)),
      StringJoinInput.Selected(Vector(rightSelected)),
      numPartitions = 4,
      parallelism = 2,
    )
    val expected = Seq("" -> ((1, 2)), "blue" -> ((6, 8)))

    triples(parts) should contain theSameElementsAs expected
    triples(parts) should contain theSameElementsAs triples(gathered)
    triples(wide) should contain theSameElementsAs expected
    val batch = parts.lift(0).getOrElse(fail("expected the one join partition"))
    utf8(batch).dictionary should be theSameInstanceAs probeDictionary
  }

  it should "drop a selected null key and ignore a null value outside the prefix" in {
    val keys = DictUtf8Column.fromIterable(Seq("red", "", absent, "blue"))
    val values = Int32Column.of(Array(7, 1, 9, 4), Validity.pack(4, row => row != 0), 4)
    val batch = pairBatch(keys, values)
    // Row 0 is a null value. Rows 1 and 3 are live. Row 2 is a null key and is not selected.
    val dropped = Selected(batch, Array(1, 3), 2)
    val probe = BatchCodec.stringIntPairs(Seq("" -> 10, "blue" -> 20, "red" -> 30))
    val parts = StringHashJoin.inner(
      StringJoinInput.Selected(Vector(dropped)),
      StringJoinInput.Dense(Vector(probe)),
      numPartitions = 1,
      parallelism = 1,
    )

    ColumnPipeline.valuesFullyPresent(Vector(dropped)) shouldBe true
    triples(parts) should contain theSameElementsAs Seq("" -> ((1, 10)), "blue" -> ((4, 20)))

    val selectedNull = Selected(batch, Array(0, 1), 2)
    ColumnPipeline.valuesFullyPresent(Vector(selectedNull)) shouldBe false
    val kept = StringHashJoin.inner(
      StringJoinInput.Selected(Vector(selectedNull)),
      StringJoinInput.Dense(Vector(probe)),
      numPartitions = 1,
      parallelism = 1,
    )
    val joined = kept.lift(0).getOrElse(fail("missing partition"))
    val outKeys = utf8(joined)
    val left = int32(joined, 1)
    val right = int32(joined, 2)
    val red = rowOf(outKeys, "red")
    val empty = rowOf(outKeys, "")

    joined.length shouldBe 2
    outKeys.isValid(red) shouldBe true
    left.isValid(red) shouldBe false
    left.isAbsent(red) shouldBe false
    right.isValid(red) shouldBe true
    right.isAbsent(red) shouldBe false
    right.values(red) shouldBe 30
    outKeys.isValid(empty) shouldBe true
    left.isValid(empty) shouldBe true
    left.isAbsent(empty) shouldBe false
    left.values(empty) shouldBe 1
    right.isValid(empty) shouldBe true
    right.isAbsent(empty) shouldBe false
    right.values(empty) shouldBe 10

    val nullKeyRow = Selected(batch, Array(1, 2), 2)
    val droppedKey = StringHashJoin.inner(
      StringJoinInput.Selected(Vector(nullKeyRow)),
      StringJoinInput.Dense(Vector(probe)),
      numPartitions = 1,
      parallelism = 1,
    )
    triples(droppedKey) should contain theSameElementsAs Seq("" -> ((1, 10)))
  }

  private def buildPairs: DistCollection[(String, Int)] =
    DistCollection(Plan.Source(Seq(Partition(buildLeft), Partition(buildRight))))

  private def probePairs: DistCollection[(String, Int)] =
    DistCollection(Plan.Source(Seq(Partition(probeLeft), Partition(probeRight))))

  private def keep(
      source: DistCollection[(String, Int)],
      keyCalls: AtomicInteger,
      valueCalls: AtomicInteger,
  ): DistCollection[(String, Int)] =
    source
      .filterKeys { (key: String) =>
        val _ = keyCalls.incrementAndGet()
        keepKey(key)
      }
      .filterValues { (value: Int) =>
        val _ = valueCalls.incrementAndGet()
        value > 0
      }

  private def keepKey(key: String): Boolean = key match
    case "no" => false
    case _ => true

  private def keepNull(key: String): Boolean =
    BatchCodec.isAbsent(key) || keepKey(key)

  private def shape(data: DistCollection[_]): String =
    ColumnarPlanner.lower(data.plan).map(_.shape).getOrElse(fail("expected a columnar plan"))

  private def selectionOf(plan: Plan[_]): Option[Vector[Selected]] =
    ColumnarPlanner.lower(plan) match
      case Some(op) => FilterChain.selected(plan, op, None, false, Set(0, 1))
      case None => fail("expected a columnar plan")

  /**
   * The dense baseline compacts the filter, then joins. The selection collect skips that compact.
   * The string join does not slice, so the two batch counts differ by the compact alone.
   */
  private def assertFewerBatches(
      joined: DistCollection[(String, (Int, Int))],
      build: DistCollection[(String, Int)],
      probe: DistCollection[(String, Int)],
      filteredBuild: Option[DistCollection[(String, Int)]],
      filteredProbe: Option[DistCollection[(String, Int)]],
  ): Unit =
    val encode = published {
      val _ = stringBatches(build)
      val _ = stringBatches(probe)
    }
    val selectedCount = published {
      ScarletRuntime.get.shuffle.clear()
      joined.collect()
    }
    val (afterFilter, dense) = ColumnMetrics.withSink(new CountingSink) {
      val buildBatches = stringBatches(filteredBuild.getOrElse(build))
      val probeBatches = stringBatches(filteredProbe.getOrElse(probe))
      val mid = ColumnMetrics.batchCount
      val _ = StringHashJoin.inner(
        buildBatches,
        probeBatches,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
      )
      (mid, ColumnMetrics.batchCount)
    }
    val compactPublishes = afterFilter - encode
    selectedCount should be > 0L
    compactPublishes should be > 0L
    (dense - selectedCount) shouldBe compactPublishes

  private def stringBatches(data: DistCollection[(String, Int)]): Vector[ColumnBatch] =
    val op = ColumnarPlanner.lower(data.plan).getOrElse(fail("expected a string-pair plan"))
    StringPairWalk.batches(ColumnarInterpreter.interpret(data.plan, op))

  private def triples(batches: Vector[ColumnBatch]): Seq[(String, (Int, Int))] =
    batches.filter(_.length > 0).flatMap(batch => BatchCodec.decodeStringJoin(batch))

  private def utf8(batch: ColumnBatch): DictUtf8Column = batch.columns.toList match
    case (column: DictUtf8Column) :: _ => column
    case other => fail(s"expected a dictionary key, found $other")

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def rowOf(keys: DictUtf8Column, text: String): Int =
    (0 until keys.length).find { row =>
      keys.isValid(row) && keys.dictionary(keys.codes(row)) == text
    }.getOrElse(fail(s"missing key $text"))

  private def pairBatch(keys: DictUtf8Column, values: Int32Column): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(keys, values),
      keys.length,
    )

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

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erasedNullValue(key: String): (String, Int) =
    // A null Int is not expressible as (String, Int); Tuple2[String, Integer] erases to the same
    // tuple, so the cast only restores the erased type.
    Tuple2[String, Integer](key, null).asInstanceOf[(String, Int)]

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
    rows.toSeq.map { row =>
      val values = row.productElement(1).asInstanceOf[Product]
      Seq(row.productElement(0), values.productElement(0), values.productElement(1))
        .map {
          case null => "null"
          case value: Int => String.valueOf(value)
          case value: String => value
          case _ => "other"
        }
        .mkString(",")
    }
