package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Plan-level proof that a stack of unfused filters compacts once. Counts are
 * [[ColumnMetrics.batchCount]], not wall time. A lone filter is left to the existing kernels,
 * whose formulas live in the morsel and pair suites.
 */
class TestSelectionBoundary extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get

  override def afterEach(): Unit =
    ColumnMetrics.restore(NoOpSink)
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "a boolean stack" should "compact once and call the second predicate on every survivor" in {
    val rows = Seq(true, false, true)
    val firstCalls = new AtomicInteger
    val secondCalls = new AtomicInteger
    val stacked = DistCollection(rows, 1)
      .filter { (value: Boolean) =>
        firstCalls.incrementAndGet()
        value || !value
      }
      .filter { (value: Boolean) =>
        secondCalls.incrementAndGet()
        value
      }

    shape(stacked.plan) shouldBe "Filter(Filter(Scan))"
    val (collected, batches, visits, _) = footprint(stacked.collect())
    collected shouldBe Seq(true, true)
    batches shouldBe 2L
    visits shouldBe 1L
    firstCalls.get() shouldBe 3
    secondCalls.get() shouldBe 3
  }

  it should "compact a stack of three filters once" in {
    val stacked = DistCollection(Seq(true, false, true, false), 1)
      .filter((value: Boolean) => value || !value)
      .filter((value: Boolean) => value || !value)
      .filter((value: Boolean) => value)

    shape(stacked.plan) shouldBe "Filter(Filter(Filter(Scan)))"
    val (collected, batches, visits, _) = footprint(stacked.collect())
    collected shouldBe Seq(true, true)
    batches shouldBe 2L
    visits shouldBe 1L
  }

  "dictionary literals" should "compact two equals filters once and count without that stack" in {
    val source = DistCollection(Seq("US", "CA", "US", "GB"), 1)
    val stacked = source.filterStringEquals("US").filterStringEquals("US")

    shape(stacked.plan) shouldBe "Filter(Filter(Scan))"
    val expected = row(stacked.collect())
    val (collected, batches, visits, _) = footprint(stacked.collect())
    collected shouldBe expected
    batches shouldBe 2L
    visits shouldBe 1L

    val (counted, countBatches, _, _) = footprint(stacked.count())
    counted shouldBe 2L
    countBatches shouldBe 1L
  }

  it should "run a general predicate on an empty cell and then a dictionary literal" in {
    val source = DistCollection(stringsWithNull(), 1)
    val calls = new AtomicInteger
    val sawAbsent = new AtomicInteger
    val stacked = source
      .filter { (value: String) =>
        calls.incrementAndGet()
        if (BatchCodec.isAbsent(value)) then sawAbsent.incrementAndGet()
        BatchCodec.isAbsent(value) || isUs(value)
      }
      .filterStringEquals("US")

    shape(stacked.plan) shouldBe "Filter(Filter(Scan))"
    val expected = row(stacked.collect())
    calls.set(0)
    sawAbsent.set(0)
    val (collected, batches, visits, _) = footprint(stacked.collect())
    collected shouldBe expected
    collected shouldBe Seq("US", "US")
    batches shouldBe 2L
    visits shouldBe 1L
    calls.get() shouldBe stringsWithNull().length
    sawAbsent.get() shouldBe 1
  }

  it should "not call a later general predicate on rows a dictionary literal dropped" in {
    val calls = new AtomicInteger
    val sawAbsent = new AtomicInteger
    val stacked = DistCollection(stringsWithNull(), 1)
      .filterStringEquals("US")
      .filter { (value: String) =>
        calls.incrementAndGet()
        if (BatchCodec.isAbsent(value)) then sawAbsent.incrementAndGet()
        !BatchCodec.isAbsent(value)
      }

    shape(stacked.plan) shouldBe "Filter(Filter(Scan))"
    val (collected, batches, visits, _) = footprint(stacked.collect())
    collected shouldBe Seq("US", "US")
    batches shouldBe 2L
    visits shouldBe 1L
    calls.get() shouldBe 2
    sawAbsent.get() shouldBe 0
  }

  it should "keep two general filters on the pipeline and still stack a hand-built pair" in {
    val source = DistCollection(stringsWithNull(), 1)
    val logical = Plan.FilterOp(
      Plan.FilterOp(source.plan, keepsAbsentOrUs),
      keepsAbsentOrUs,
    )
    shape(logical) shouldBe "Pipeline2(Scan)"

    val scan = ColumnarPlanner.lower(source.plan).getOrElse(fail("expected a string scan"))
    val op = PhysicalOp.Filter(
      PhysicalOp.Filter(scan, PhysicalOp.Slot.Element),
      PhysicalOp.Slot.Element,
    )
    arm()
    val (decoded, batches, visits, _) = footprint {
      val columns = ColumnarInterpreter.interpret(logical, op)
      BatchCodec.decodeNullableStrings(onlyBatch(columns))
    }
    decoded shouldBe Seq(Some("US"), None, Some("US"))
    batches shouldBe 2L
    visits shouldBe 1L

    val utf8 = footprint {
      val columns = ColumnarInterpreter.interpret(logical, op)
      onlyBatch(columns).columns.lift(0).getOrElse(fail("expected a column"))
    }._1 match
      case column: DictUtf8Column => column
      case other => fail(s"expected Utf8Dict, got $other")
    utf8.isValid(1) shouldBe false
    utf8.codes(1) shouldBe 0

    val piped = source.filter(keepsAbsentOrUs).filter(keepsAbsentOrUs)
    row(piped.collect()) shouldBe footprint(piped.collect())._1
  }

  "pair filters" should "compact FilterValue(FilterKey) once" in {
    val firstCalls = new AtomicInteger
    val secondCalls = new AtomicInteger
    val stacked = DistCollection(Seq((1, 2), (3, 4), (5, 0), (0, 7)), 1)
      .filterKeys { (key: Int) =>
        firstCalls.incrementAndGet()
        key % 2 == 1
      }
      .filterValues { (value: Int) =>
        secondCalls.incrementAndGet()
        value >= 0
      }

    shape(stacked.plan) shouldBe "FilterValue(FilterKey(Scan))"
    val expected = row(stacked.collect())
    firstCalls.set(0)
    secondCalls.set(0)
    val (collected, batches, visits, _) = footprint(stacked.collect())
    collected shouldBe Seq((1, 2), (3, 4), (5, 0))
    collected shouldBe expected
    batches shouldBe 2L
    visits shouldBe 1L
    firstCalls.get() shouldBe 4
    secondCalls.get() shouldBe 3
  }

  it should "compact two FilterKey nodes once" in {
    val stacked = DistCollection(Seq((1, 2), (3, 4), (4, 8), (0, 3)), 1)
      .filterKeys((key: Int) => key != 4)
      .filterKeys((key: Int) => key >= 0)

    shape(stacked.plan) shouldBe "FilterKey(FilterKey(Scan))"
    val (collected, batches, visits, _) = footprint(stacked.collect())
    collected shouldBe Seq((1, 2), (3, 4), (0, 3))
    batches shouldBe 2L
    visits shouldBe 1L
  }

  it should "compact FilterRow(FilterKey) once" in {
    val stacked = DistCollection(Seq((1, 5), (4, 1), (0, 3)), 1)
      .filterKeys((key: Int) => key > 0)
      .filter { case (key, value) => key < value }

    shape(stacked.plan) shouldBe "FilterRow(FilterKey(Scan))"
    val (collected, batches, visits, _) = footprint(stacked.collect())
    collected shouldBe Seq((1, 5))
    batches shouldBe 2L
    visits shouldBe 1L
  }

  it should "keep a null payload beside FilterKey and skip a null predicate column" in {
    val rows = nullPairs()
    val firstCalls = new AtomicInteger
    val secondCalls = new AtomicInteger
    val source = DistCollection(rows, 1)
    val stacked = source
      .filterKeys { (key: Int) =>
        firstCalls.incrementAndGet()
        key != 4
      }
      .filterKeys { (key: Int) =>
        secondCalls.incrementAndGet()
        key >= 0
      }
    shape(stacked.plan) shouldBe "FilterKey(FilterKey(Scan))"

    arm()
    val op = ColumnarPlanner.lower(stacked.plan).getOrElse(fail("expected a pair plan"))
    val (decoded, batches, visits, _) = footprint {
      val columns =
        ColumnarInterpreter.interpret(stacked.plan, op, preserveAbsence = true)
      BatchCodec.decodeNullableIntPairs(onlyBatch(columns))
    }
    decoded shouldBe Seq((Some(1), None), (Some(0), Some(3)))
    batches shouldBe 2L
    visits shouldBe 1L
    firstCalls.get() shouldBe 3
    secondCalls.get() shouldBe 2

    val valueCalls = new AtomicInteger
    val valued = source
      .filterKeys((key: Int) => key >= 0)
      .filterValues { (value: Int) =>
        valueCalls.incrementAndGet()
        if (value == 0) then fail("value predicate saw a null payload")
        value > 0
      }
    shape(valued.plan) shouldBe "FilterValue(FilterKey(Scan))"
    val valueOp = ColumnarPlanner.lower(valued.plan).getOrElse(fail("expected a pair plan"))
    val (valueRows, valueBatches, _, _) = footprint {
      val columns =
        ColumnarInterpreter.interpret(valued.plan, valueOp, preserveAbsence = true)
      BatchCodec.decodeNullableIntPairs(onlyBatch(columns))
    }
    valueRows shouldBe Seq((Some(4), Some(8)), (Some(0), Some(3)))
    valueBatches shouldBe 2L
    valueCalls.get() shouldBe 2

    val rowCalls = new AtomicInteger
    val rowed = source
      .filterKeys((key: Int) => key >= 0)
      .filter { case (key, value) =>
        rowCalls.incrementAndGet()
        if (value == 0) then fail("row predicate saw a null payload")
        key < 10
      }
    shape(rowed.plan) shouldBe "FilterRow(FilterKey(Scan))"
    val rowOp = ColumnarPlanner.lower(rowed.plan).getOrElse(fail("expected a pair plan"))
    footprint {
      val columns = ColumnarInterpreter.interpret(rowed.plan, rowOp, preserveAbsence = true)
      BatchCodec.decodeNullableIntPairs(onlyBatch(columns))
    }._1 shouldBe Seq((Some(4), Some(8)), (Some(0), Some(3)))
    rowCalls.get() shouldBe 2
  }

  "take" should "decode only the requested prefix of a filter stack" in {
    val stacked = DistCollection(stringsWithNull(), 1)
      .filter((value: String) => BatchCodec.isAbsent(value) || value.nonEmpty)
      .filterStringEquals("US")

    shape(stacked.plan) shouldBe "Filter(Filter(Scan))"
    val (prefix, batches, _, decoded) = footprint(stacked.take(1))
    prefix shouldBe Seq("US")
    decoded shouldBe 1L
    batches shouldBe 2L

    val (rest, restBatches, _, restDecoded) = footprint(stacked.take(10))
    rest shouldBe Seq("US", "US")
    restDecoded shouldBe 2L
    restBatches shouldBe 2L
  }

  "long filters" should "stay one publish per pipeline step and compact a hand-built stack once" in {
    val source = DistCollection.ofLongs(Array(0L, 1L, -1L, 0L), 1)
    val piped = source.filter((value: Long) => value >= 0L).filter((value: Long) => value == 0L)
    shape(piped.plan) shouldBe "Pipeline2(Scan)"
    val (_, pipelineBatches, pipelineVisits, _) = footprint(piped.collect())
    pipelineBatches shouldBe 3L
    pipelineVisits shouldBe 1L

    val firstCalls = new AtomicInteger
    val secondCalls = new AtomicInteger
    val logical = Plan.FilterOp(
      Plan.FilterOp(
        source.plan,
        (value: Long) => {
          firstCalls.incrementAndGet()
          value >= 0L
        },
      ),
      (value: Long) => {
        secondCalls.incrementAndGet()
        value == 0L
      },
    )
    val scan = ColumnarPlanner.lower(source.plan).getOrElse(fail("expected a long scan"))
    val op = PhysicalOp.Filter(
      PhysicalOp.Filter(scan, PhysicalOp.Slot.Element),
      PhysicalOp.Slot.Element,
    )
    arm()
    val (collected, batches, visits, _) = footprint {
      ColumnarPlanner
        .runLowered(logical, op)
        .getOrElse(fail("hand-built stack did not run"))
        .flatMap(_.data)
    }
    collected shouldBe Seq(0L, 0L)
    batches shouldBe 2L
    visits shouldBe 1L
    firstCalls.get() shouldBe 4
    secondCalls.get() shouldBe 3
  }

  it should "agree with the row path when morsels run in parallel" in {
    val values = (0 until 200).map(row => row % 3 == 0)
    val stacked = DistCollection(values, 1)
      .filter((value: Boolean) => value || !value)
      .filter((value: Boolean) => value)
    val expected = row(stacked.collect())
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(
      original.copy(columnarExecution = true, columnarBatchSize = 8, threadPoolSize = 4),
    )
    stacked.collect() shouldBe expected
  }

  it should "send a long-pair filter stack to the row path" in {
    val calls = new AtomicInteger
    val source = DistCollection.ofLongPairs(Array(1L, -2L, 0L), Array(4L, 5L, 6L), 1)
    val keep = (pair: (Long, Long)) => {
      calls.incrementAndGet()
      pair._1 >= 0L && pair._2 > 0L
    }
    val stacked = source.filter(keep).filter(keep)
    arm()
    val scan = ColumnarPlanner.lower(source.plan).getOrElse(fail("expected a long-pair scan"))
    val op = PhysicalOp.Filter(
      PhysicalOp.Filter(scan, PhysicalOp.Slot.Row),
      PhysicalOp.Slot.Row,
    )

    val thrown = intercept[ClassCastException] {
      ColumnarInterpreter.interpret(stacked.plan, op)
    }
    thrown.getMessage shouldBe "columnar op expected int pairs"
    ColumnarPlanner.runLowered(stacked.plan, op) shouldBe None
    calls.get() shouldBe 0
    ColumnarPlanner.lower(stacked.plan) shouldBe Option.empty[PhysicalOp]
    stacked.collect().toList shouldBe row(stacked.collect().toList)
  }

  it should "resolve a dictionary mask once per partition across morsels" in {
    val rows = dictionaryRows()
    val stacked = DistCollection(rows, 2).filterStringEquals("US").filterStringIn("US", "FR")
    val expected = row(stacked.collect().toList)
    val perPartition = ColumnBatches.cuts(rows.length / 2, wideBatch).length
    perPartition should be > 1

    Selection.resetCodeMaskBuilds()
    wide()
    val (collected, visits) = counted(stacked.collect().toList)
    collected shouldBe expected
    collected.exists(BatchCodec.isAbsent) shouldBe false
    collected should contain("US")
    visits should be > 1L
    // Two literal steps, two partitions. A rebuild per morsel would be steps times morsels.
    Selection.codeMaskBuilds shouldBe 2 * 2
    Selection.codeMaskBuilds should be < (2 * 2 * perPartition)
    assertSurvivorsValid(stacked.plan)
  }

  it should "match the row path for pair, triple, and dictionary stacks across morsels" in {
    val pairRows = 18
    val keys = Array.tabulate(pairRows)(row => row - 1)
    val values = Array.tabulate(pairRows)(row => if (row == 1) 0 else row)
    val pairs = DistCollection
      .ofIntPairs(keys, values, 1)
      .filterKeys((key: Int) => key >= 0)
      .filterValues((value: Int) => value % 2 == 0)
    val pairExpected = row(pairs.collect().toList)
    wide()
    val (pairCollected, pairVisits) = counted(pairs.collect().toList)
    pairCollected shouldBe pairExpected
    pairCollected should contain((0, 0))
    pairCollected.exists(_._1 < 0) shouldBe false
    pairVisits should be > 1L
    assertSurvivorsValid(pairs.plan)

    val nullable = DistCollection(nullablePairs(), 1)
      .filterKeys((key: Int) => key % 2 == 0)
      .filterKeys((key: Int) => key >= 0)
    wide()
    val nullableOp = ColumnarPlanner.lower(nullable.plan).getOrElse(fail("expected a pair plan"))
    val nullableBatch = onlyBatch {
      ColumnarInterpreter.interpret(nullable.plan, nullableOp, preserveAbsence = true)
    }
    val decoded = BatchCodec.decodeNullableIntPairs(nullableBatch)
    decoded shouldBe Seq(
      (Some(4), None),
      (Some(0), Some(3)),
      (Some(2), Some(5)),
      (Some(6), Some(7)),
      (Some(8), Some(1)),
      (Some(10), Some(0)),
      (Some(12), Some(4)),
    )
    val nullableKeys = int32(nullableBatch, 0)
    val nullableValues = int32(nullableBatch, 1)
    decoded.zipWithIndex.foreach { (pair, row) =>
      val (_, value) = pair
      nullableKeys.isValid(row) shouldBe true
      nullableValues.isValid(row) shouldBe value.isDefined
      if (value.isEmpty) then nullableValues.values(row) shouldBe 0
    }

    val joinRows = 20
    val joinKeys = Array.tabulate(joinRows)(identity)
    val build = DistCollection.ofIntPairs(joinKeys, Array.tabulate(joinRows)(_ * 10), 1)
    val probe = DistCollection.ofIntPairs(joinKeys, Array.tabulate(joinRows)(identity), 1)
    val triples = build.join(probe).filterJoinKey(_ % 2 == 0).filterJoinLeft(_ % 20 == 0)
    wide()
    val tripleExpected = rowUnder(triples.collect().toList)
    val (tripleCollected, tripleVisits) = counted(triples.collect().toList)
    tripleCollected shouldBe tripleExpected
    tripleCollected should contain((0, (0, 0)))
    tripleCollected.exists(_._1 % 2 == 1) shouldBe false
    tripleVisits should be > 1L
    assertSurvivorsValid(triples.plan)

    val texts = DistCollection(dictionaryRows(), 2)
      .filterStringEquals("US")
      .filterStringIn("US", "FR")
    val textExpected = row(texts.collect().toList)
    wide()
    val (textCollected, textVisits) = counted(texts.collect().toList)
    textCollected shouldBe textExpected
    textCollected.foreach(text => BatchCodec.isAbsent(text) shouldBe false)
    textVisits should be > 1L
    assertSurvivorsValid(texts.plan)
  }

  private val wideBatch = 4

  private def wide(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(
      original.copy(
        columnarExecution = true,
        columnarBatchSize = wideBatch,
        threadPoolSize = 4,
        defaultShufflePartitions = 1,
      ),
    )

  private def counted[A](body: => A): (A, Long) =
    ColumnMetrics.withSink(new CountingSink) {
      val value = body
      (value, ColumnMetrics.visitCount)
    }

  private def rowUnder[A](body: => A): A =
    val saved = ScarletConf.get
    try
      ScarletRuntime.get.shuffle.clear()
      ScarletConf.set(saved.copy(columnarExecution = false))
      body
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  private def assertSurvivorsValid(plan: Plan[_]): Unit =
    val op = ColumnarPlanner.lower(plan).getOrElse(fail("expected a columnar plan"))
    ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op)).foreach { batch =>
      (0 until batch.length).foreach { row =>
        batch.columns.foreach(column => column.isValid(row) shouldBe true)
      }
    }

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def dictionaryRows(): Vector[String] =
    Vector("CA", "US", "CA", "GB", null, "US", "MX", "US", "ZZ", "US")

  private def nullablePairs(): Seq[(Int, Int)] =
    Seq(
      (1, 2),
      erased(Some(4), None),
      erased(None, Some(8)),
      (0, 3),
      (2, 5),
      (6, 7),
      (8, 1),
      (3, 3),
      (10, 0),
      (12, 4),
    )

  private def shape(plan: Plan[_]): String =
    arm()
    ColumnarPlanner.lower(plan).map(_.shape).getOrElse(fail("expected a columnar plan"))

  private def arm(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(
      original.copy(columnarExecution = true, columnarBatchSize = 1024, threadPoolSize = 1),
    )

  private def footprint[A](body: => A): (A, Long, Long, Long) =
    arm()
    ColumnMetrics.withSink(new CountingSink) {
      val value = body
      (value, ColumnMetrics.batchCount, ColumnMetrics.visitCount, ColumnMetrics.decodedValueCount)
    }

  private def row[A](body: => A): A =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original.copy(columnarExecution = false))
    body

  private def keepsAbsentOrUs(value: String): Boolean =
    BatchCodec.isAbsent(value) || isUs(value)

  private def isUs(value: String): Boolean =
    !BatchCodec.isAbsent(value) && value.equals("US")

  private def onlyBatch(columns: ColumnarInterpreter.Columns): ColumnBatch =
    ColumnarInterpreter
      .batchesOf(columns)
      .lift(0)
      .getOrElse(fail("expected one batch"))

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def stringsWithNull(): Seq[String] =
    Seq("US", "CA", null, "US")

  private def nullPairs(): Seq[(Int, Int)] =
    Seq(
      (4, 8),
      erased(Some(1), None),
      erased(None, Some(9)),
      (0, 3),
    )

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def erased(key: Option[Int], value: Option[Int]): (Int, Int) =
    val left: AnyRef = key match
      case Some(present) => Int.box(present)
      case None => null
    val right: AnyRef = value match
      case Some(present) => Int.box(present)
      case None => null
    Tuple2(left, right).asInstanceOf[(Int, Int)]
