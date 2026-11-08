package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.{DictionaryStringPredicate, DistCollection}
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * A string-pair stack of `filterKeys` / `filterValues`, including one filter, publishes one
 * survivor index. A whole-row filter does the same, including a stack of one. Collect, take, and
 * `groupByKey` read that index. A map and a scan stay on [[StringPairWalk]]. Counts are
 * [[ColumnMetrics.batchCount]]: the gap against a direct interpret is the compact the selection
 * path does not publish.
 */
class TestSelectionStrings extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      original.copy(
        columnarExecution = true,
        columnarBatchSize = 2,
        threadPoolSize = 1,
        defaultShufflePartitions = 4,
      ),
    )
    ScarletRuntime.get.shuffle.clear()

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "a two-filter string-pair stack" should "collect and take the survivor index in one pass" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val stacked = filtered(keyCalls, valueCalls)
    val expected = row(stacked.collect().toList)

    shape(stacked.plan) shouldBe "FilterValue(FilterKey(Scan))"
    keyCalls.set(0)
    valueCalls.set(0)
    val (collected, batches, decoded) = footprint(stacked.collect().toList)
    collected shouldBe expected
    collected shouldBe List(
      nullKey(4),
      "" -> 1,
      "red" -> 5,
      "" -> 8,
      nullKey(2),
      "blue" -> 6,
    )
    decoded shouldBe collected.length.toLong
    keyCalls.get() shouldBe 8
    valueCalls.get() shouldBe 7
    val encode = published {
      ColumnarInterpreter.interpret(source.plan, lowered(source.plan))
    }
    val compacting = published {
      ColumnarInterpreter.interpret(stacked.plan, lowered(stacked.plan))
    }
    val compactPublishes = compacting - encode
    compactPublishes should be > 0L
    (compacting - batches) shouldBe compactPublishes

    val expectedPrefix = row(stacked.take(1))
    keyCalls.set(0)
    valueCalls.set(0)
    val (prefix, prefixBatches, prefixDecoded) = footprint(stacked.take(1))
    prefix shouldBe expectedPrefix
    prefix shouldBe List(nullKey(4))
    prefixDecoded shouldBe 1L
    keyCalls.get() shouldBe 4
    valueCalls.get() shouldBe 3
    val encodeOne = published {
      ColumnarInterpreter.interpret(source.plan, lowered(source.plan), Some(0))
    }
    prefixBatches shouldBe encodeOne

    keyCalls.set(0)
    valueCalls.set(0)
    val (none, noneBatches, noneDecoded) = footprint(stacked.take(0))
    none shouldBe empty
    noneBatches shouldBe 0L
    noneDecoded shouldBe 0L
    keyCalls.get() shouldBe 0

    FilterChain
      .selected(stacked.plan, lowered(stacked.plan), None, false, Set(0, 1))
      .isDefined shouldBe true
  }

  it should "keep dictionary literals on the selection path" in {
    val literals = new DictionaryStringPredicate[String](
      Vector("red", ""),
      implicitly[String =:= String],
    )
    val stacked = source.filterKeys(literals).filterValues((value: Int) => value > 0)
    val grouped = stacked.groupByKey
    val expected = row(stacked.collect().toList)

    shape(stacked.plan) shouldBe "FilterValue(FilterKey(Scan))"
    val (collected, batches, _) = footprint(stacked.collect().toList)
    collected shouldBe expected
    collected shouldBe List("" -> 1, "red" -> 5, "" -> 8)
    val encode = published {
      ColumnarInterpreter.interpret(source.plan, lowered(source.plan))
    }
    val compacting = published {
      ColumnarInterpreter.interpret(stacked.plan, lowered(stacked.plan))
    }
    val compactPublishes = compacting - encode
    compactPublishes should be > 0L
    (compacting - batches) shouldBe compactPublishes
    groupedStrings(grouped.collect()) shouldBe groupedStrings(row(grouped.collect()))
    groupedStrings(grouped.collect()) shouldBe Map("" -> Seq(1, 8), "red" -> Seq(5))
  }

  "string groupByKey" should "scatter the survivor index and skip the compact" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val stacked = filtered(keyCalls, valueCalls)
    val grouped = stacked.groupByKey
    val expected = groupedStrings(row(grouped.collect()))

    shape(grouped.plan) shouldBe "GroupByKey(Exchange(FilterValue(FilterKey(Scan))))"
    keyCalls.set(0)
    valueCalls.set(0)
    val found = groupedStrings(grouped.collect())
    found shouldBe expected
    found shouldBe Map("" -> Seq(1, 8), "red" -> Seq(5), "blue" -> Seq(6))
    keyCalls.get() shouldBe 8
    valueCalls.get() shouldBe 7

    val encode = published {
      ColumnarInterpreter.interpret(source.plan, lowered(source.plan))
    }
    val selected = published(grouped.collect())
    val (afterFilter, dense) = ColumnMetrics.withSink(new CountingSink) {
      val columns = ColumnarInterpreter.interpret(stacked.plan, lowered(stacked.plan))
      val mid = ColumnMetrics.batchCount
      val _ = GroupByKey.strings(
        StringPairWalk.batches(columns),
        ScarletConf.get.defaultShufflePartitions,
        ScarletConf.get.threadPoolSize,
      )
      (mid, ColumnMetrics.batchCount)
    }
    val compactPublishes = afterFilter - encode
    compactPublishes should be > 0L
    (dense - selected) shouldBe compactPublishes
  }

  it should "keep an empty string, drop a null key, and refuse only a selected null value" in {
    val present = BatchCodec.stringIntPairs(Seq(nullKey(4), "" -> 1, "red" -> 99, "blue" -> 6))
    // Row 2 sits past the live prefix. A scan of the whole array would keep 99.
    val selected = Selected(present, Array(0, 1, 3, 2), 3)
    val one = HashExchange.partitionAllSelectedUtf8(Vector(selected), keyOrdinal = 0, 1, 1)
    val only = one.headOption.getOrElse(fail("missing partition"))

    only should not be theSameInstanceAs(present)
    dictionaryOf(only) should be theSameInstanceAs dictionaryOf(present)
    BatchCodec.decodeStringPairs(only) shouldBe Seq("" -> 1, "blue" -> 6)

    val wide = HashExchange.partitionAllSelectedUtf8(Vector(selected), keyOrdinal = 0, 2, 1)
    val kept = wide.flatMap(part => BatchCodec.decodeStringPairs(part))
    kept should contain theSameElementsAs Seq("" -> 1, "blue" -> 6)
    val home = Math.floorMod(StringHash.hash(""), 2)
    val bucket = wide.lift(home).getOrElse(fail("missing bucket"))
    BatchCodec.decodeStringPairs(bucket).map(_._1) should contain("")

    val grouped = GroupByKey.stringsSelected(Vector(selected), partitions = 2, parallelism = 1)
    groupedStrings(grouped) shouldBe Map("" -> Seq(1), "blue" -> Seq(6))

    val values = pairBatch(
      DictUtf8Column.fromIterable(Seq("red", "")),
      Int32Column.of(Array(0, 1), Validity.pack(2, row => row == 1), 2),
      2,
    )
    val dropped = Selected(values, Array(1, 0), 1)
    groupedStrings(GroupByKey.stringsSelected(Vector(dropped), 1, 1)) shouldBe Map(
      "" -> Seq(1),
    )
    val caught = the[IllegalArgumentException] thrownBy {
      GroupByKey.stringsSelected(Vector(Selected(values, Array(0), 1)), 1, 1)
    }
    caught.getMessage should include("clear value bit")

    val both = pairBatch(
      DictUtf8Column.fromIterable(Seq(nullKey(0)._1, "red")),
      Int32Column.of(Array(0, 10), Validity.pack(2, row => row == 1), 2),
      2,
    )
    val hidden = the[IllegalArgumentException] thrownBy {
      GroupByKey.stringsSelected(Vector(Selected(both, Array(0), 1)), 1, 1)
    }
    hidden.getMessage should include("clear value bit")
    groupedStrings(GroupByKey.stringsSelected(Vector(Selected(both, Array(1, 0), 1)), 1, 1)) shouldBe
      Map("red" -> Seq(10))
  }

  it should "group two dictionaries by string when their intern order differs" in {
    val leftDictionary = Array("", "red", "blue")
    val rightDictionary = Array("blue", "red", "")
    leftDictionary.indexOf("") should not be rightDictionary.indexOf("")
    val left = pairBatch(
      DictUtf8Column.of(leftDictionary, Array(0, 1, 2), Validity.allValid(3), 3),
      Int32Column.of(Array(1, 5, 6), Validity.allValid(3), 3),
      3,
    )
    val right = pairBatch(
      DictUtf8Column.of(rightDictionary, Array(0, 1, 2), Validity.allValid(3), 3),
      Int32Column.of(Array(8, 9, 2), Validity.allValid(3), 3),
      3,
    )
    val selected = Vector(
      Selected(left, Array(0, 1, 2), 3),
      Selected(right, Array(0, 1, 2), 3),
    )
    val expected = Map("" -> Seq(1, 2), "red" -> Seq(5, 9), "blue" -> Seq(6, 8))

    groupedStrings(GroupByKey.stringsSelected(selected, 1, 1)) shouldBe expected
    groupedStrings(GroupByKey.strings(Vector(left, right), 1, 1)) shouldBe expected
    groupedStrings(GroupByKey.stringsSelected(selected, 4, 1)) shouldBe expected
  }

  it should "index one slot filter and leave a map and a scan on the dense path" in {
    val single = source.filterKeys((key: String) => keeps(key))
    val mapped = source.mapValues((value: Int) => value + 1)
    val bare = source.groupByKey
    val once = source.filterValues((value: Int) => value > 0)
    val groupedOnce = once.groupByKey

    shape(single.plan) shouldBe "FilterKey(Scan)"
    shape(mapped.plan) shouldBe "MapValue(Scan)"
    shape(bare.plan) shouldBe "GroupByKey(Exchange(Scan))"
    shape(once.plan) shouldBe "FilterValue(Scan)"
    FilterChain.selected(single.plan, lowered(single.plan), None, false, Set(0, 1)) shouldBe defined
    FilterChain.selected(mapped.plan, lowered(mapped.plan), None, false, Set(0, 1)) shouldBe None
    FilterChain.selected(source.plan, lowered(source.plan), None, false, Set(0, 1)) shouldBe None
    FilterChain.selected(once.plan, lowered(once.plan), None, false, Set(0, 1)) shouldBe defined

    val encode = published {
      ColumnarInterpreter.interpret(source.plan, lowered(source.plan))
    }
    val singleBatches = footprint(single.collect().toList)._2
    val singleDense = published {
      ColumnarInterpreter.interpret(single.plan, lowered(single.plan))
    }
    val singleCompact = singleDense - encode
    singleCompact should be > 0L
    (singleDense - singleBatches) shouldBe singleCompact
    single.collect().toList shouldBe row(single.collect().toList)

    val mappedBatches = footprint(mapped.collect().toList)._2
    val mappedDense = published {
      ColumnarInterpreter.interpret(mapped.plan, lowered(mapped.plan))
    }
    mappedBatches shouldBe mappedDense
    mapped.collect().toList shouldBe row(mapped.collect().toList)

    val bareBatches = published(bare.collect())
    val bareDense = published {
      val columns = ColumnarInterpreter.interpret(source.plan, lowered(source.plan))
      GroupByKey.strings(StringPairWalk.batches(columns), width, workers)
    }
    (bareDense - bareBatches) shouldBe 0L

    val onceBatches = published(groupedOnce.collect())
    val (afterFilter, onceDense) = ColumnMetrics.withSink(new CountingSink) {
      val columns = ColumnarInterpreter.interpret(once.plan, lowered(once.plan))
      val mid = ColumnMetrics.batchCount
      val _ = GroupByKey.strings(StringPairWalk.batches(columns), width, workers)
      (mid, ColumnMetrics.batchCount)
    }
    val onceCompact = afterFilter - encode
    onceCompact should be > 0L
    (onceDense - onceBatches) shouldBe onceCompact
    groupedStrings(groupedOnce.collect()) shouldBe groupedStrings(row(groupedOnce.collect()))
  }

  it should "keep one whole-row string filter, and a stack of two, on one index" in {
    val calls = new AtomicInteger
    val once = source.filter { (row: (String, Int)) =>
      calls.incrementAndGet()
      keeps(row._1)
    }
    val twice = once.filter { (row: (String, Int)) =>
      calls.incrementAndGet()
      row._2 > Int.MinValue
    }
    val onceOp = lowered(once.plan)
    val twiceOp = lowered(twice.plan)
    val onceCompiled =
      PredicatePlan.compile(once.plan, onceOp).getOrElse(fail("one row filter did not compile"))
    val twiceCompiled =
      PredicatePlan.compile(twice.plan, twiceOp).getOrElse(fail("two row filters did not compile"))

    shape(once.plan) shouldBe "FilterRow(Scan)"
    shape(twice.plan) shouldBe "FilterRow(FilterRow(Scan))"
    PredicatePlan.selectionSteps(onceCompiled) match
      case Some(Vector(Selection.Step.StringPair(_))) => ()
      case other => fail(s"expected one string-pair row step, got $other")
    PredicatePlan.selectionSteps(twiceCompiled) match
      case Some(Vector(Selection.Step.StringPair(_), Selection.Step.StringPair(_))) => ()
      case other => fail(s"expected two string-pair row steps, got $other")
    PredicatePlan.encoded(twiceCompiled, Set(0)).atoms match
      case Vector(PredicatePlan.Atom.StringRow(_), PredicatePlan.Atom.StringRow(_)) => ()
      case other => fail(s"a whole-row step must stay a row step, got $other")
    calls.get() shouldBe 0
    FilterChain.selected(once.plan, onceOp, None, false, Set(0, 1)) shouldBe defined
    FilterChain.selected(twice.plan, twiceOp, None, false, Set(0, 1)) shouldBe defined

    calls.set(0)
    val onceGot = once.collect().toList
    calls.get() shouldBe 8
    onceGot shouldBe row(once.collect().toList)
    calls.set(0)
    val got = twice.collect().toList
    calls.get() shouldBe 15
    got shouldBe List(
      nullKey(4),
      "" -> 1,
      "red" -> 0,
      "red" -> 5,
      "" -> 8,
      nullKey(2),
      "blue" -> 6,
    )
    got shouldBe row(twice.collect().toList)
    twice.count() shouldBe row(twice.count())
  }

  it should "keep a null key and an empty key, and leave accents distinct" in {
    val composed = "é"
    val decomposed = "e\u0301"
    val rows = DistCollection(Seq(composed -> 1, decomposed -> 2, "" -> 3, nullKey(4)), 1)
    val filtered = rows.filter((row: (String, Int)) => row._1 != decomposed)
    val op = lowered(filtered.plan)
    val steps = PredicatePlan
      .selectionSteps(
        PredicatePlan.compile(filtered.plan, op).getOrElse(fail("accent filter did not compile")),
      )
      .getOrElse(fail("accent filter has no steps"))

    steps match
      case Vector(Selection.Step.StringPair(_)) => ()
      case other => fail(s"expected one string-pair row step, got $other")
    val got = filtered.collect().toList
    got shouldBe List(composed -> 1, "" -> 3, nullKey(4))
    got shouldBe row(filtered.collect().toList)
    filtered.count() shouldBe row(filtered.count())
  }

  it should "keep a null int the row path keeps, and not store it as zero" in {
    val rows = Seq(
      boxedStringInt("a", null),
      boxedStringInt(null, java.lang.Integer.valueOf(2)),
      boxedStringInt("", java.lang.Integer.valueOf(0)),
    )
    val input = DistCollection(rows, 1)
    val filtered = input.filter { (row: (String, Int)) =>
      BatchCodec.isAbsent(row._1) || row._1 == "a" || row._1 == ""
    }
    val got = filtered.collect().toList

    sameStringInts(got, row(filtered.collect().toList))
    got.length shouldBe 3
    absentComponent(got.head, 1) shouldBe true
    absentComponent(got(1), 0) shouldBe true
    intComponent(got(1), 1) shouldBe 2
    stringComponent(got(2), 0) shouldBe ""
    intComponent(got(2), 1) shouldBe 0
    filtered.count() shouldBe row(filtered.count())
  }

  it should "keep a mixed whole-row and key stack on one index" in {
    val filtered = source
      .filter((row: (String, Int)) => row._2 > 0)
      .filterValues((value: Int) => value < 10)
      .filterKeys((key: String) => keeps(key))
    val op = lowered(filtered.plan)
    val compiled =
      PredicatePlan.compile(filtered.plan, op).getOrElse(fail("mixed stack did not compile"))
    val steps =
      PredicatePlan.selectionSteps(compiled).getOrElse(fail("mixed stack has no steps"))

    steps match
      case Vector(
            Selection.Step.StringPair(_),
            Selection.Step.Int32(valueOrdinal, _),
            Selection.Step.Utf8(keyOrdinal, _),
          ) =>
        valueOrdinal shouldBe 1
        keyOrdinal shouldBe 0
      case other => fail(s"expected one string-pair step vector, got $other")
    PredicatePlan.encoded(compiled, Set(1)).atoms match
      case Vector(
            PredicatePlan.Atom.StringRow(_),
            PredicatePlan.Atom.PairValue(ordinal, _),
            PredicatePlan.Atom.Utf8(_, _),
          ) =>
        ordinal shouldBe 1
      case other => fail(s"a whole-row step must keep the value ordinal, got $other")
    FilterChain.selected(filtered.plan, op, None, false, Set(0, 1)) shouldBe defined
    filtered.collect().toList shouldBe row(filtered.collect().toList)
    filtered.count() shouldBe row(filtered.count())
  }

  private def source: DistCollection[(String, Int)] =
    DistCollection(Plan.Source(Seq(Partition(leftRows), Partition(rightRows))))

  private def leftRows: Seq[(String, Int)] =
    Seq(nullKey(4), "" -> 1, "no" -> 3, "red" -> 0)

  private def rightRows: Seq[(String, Int)] =
    Seq("red" -> 5, "" -> 8, nullKey(2), "blue" -> 6)

  private def filtered(
      keyCalls: AtomicInteger,
      valueCalls: AtomicInteger,
  ): DistCollection[(String, Int)] =
    source
      .filterKeys { (key: String) =>
        keyCalls.incrementAndGet()
        keeps(key)
      }
      .filterValues { (value: Int) =>
        valueCalls.incrementAndGet()
        value > 0
      }

  private def keeps(key: String): Boolean =
    BatchCodec.isAbsent(key) || key != "no"

  private def shape(plan: Plan[_]): String =
    ColumnarPlanner.lower(plan).map(_.shape).getOrElse(fail("expected a columnar plan"))

  private def lowered(plan: Plan[_]): PhysicalOp =
    ColumnarPlanner.lower(plan).getOrElse(fail("expected a columnar plan"))

  private def width: Int = ScarletConf.get.defaultShufflePartitions

  private def workers: Int = ScarletConf.get.threadPoolSize

  private def footprint[A](body: => A): (A, Long, Long) =
    ColumnMetrics.withSink(new CountingSink) {
      val value = body
      (value, ColumnMetrics.batchCount, ColumnMetrics.decodedValueCount)
    }

  private def published[A](body: => A): Long =
    ColumnMetrics.withSink(new CountingSink) {
      val _ = body
      ColumnMetrics.batchCount
    }

  private def groupedStrings(rows: Iterable[(String, Iterable[Int])]): Map[String, Seq[Int]] =
    rows.map { case (key, values) => key -> values.toSeq }.toMap

  private def groupedStrings(parts: Vector[GroupedStringBatch]): Map[String, Seq[Int]] =
    parts.flatMap(_.rows).map { case (key, values) => key -> values.toSeq }.toMap

  private def dictionaryOf(batch: ColumnBatch): Array[String] =
    batch.columns.headOption match
      case Some(column: DictUtf8Column) => column.dictionary
      case _ => fail("expected a dictionary key")

  private def pairBatch(keys: DictUtf8Column, values: Int32Column, length: Int): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(keys, values),
      length,
    )

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

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def boxedStringInt(key: String, value: java.lang.Integer): (String, Int) =
    Tuple2(key, value).asInstanceOf[(String, Int)]

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Null",
    ),
  )
  private def sameStringInts(left: Seq[(String, Int)], right: Seq[(String, Int)]): Unit =
    left.length shouldBe right.length
    left.zip(right).foreach { (one, other) =>
      one.productElement(0) shouldBe other.productElement(0)
      one.productElement(1) shouldBe other.productElement(1)
    }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def absentComponent(row: (String, Int), ordinal: Int): Boolean =
    row.productElement(ordinal).asInstanceOf[AnyRef] eq null

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def intComponent(row: (String, Int), ordinal: Int): Int =
    row.productElement(ordinal) match
      case value: Int => value
      case other => fail(s"expected a present int, got $other")

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def stringComponent(row: (String, Int), ordinal: Int): String =
    row.productElement(ordinal) match
      case value: String => value
      case other => fail(s"expected a present string, got $other")
