package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import scala.util.Random

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * The fused pair-filter fold and the encode-then-walk agree.
 *
 * Owned arrays are cut into the same morsels as the old batch. A count or sum publishes no batch
 * and does not encode. A boxed null stays absent: the predicate is not called, and a null measure
 * adds nothing. The direct range check starts past zero, so a shared off-by-start cannot hide
 * behind both paths using the same cuts.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class TestFusedPairFold extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val original = ScarletConf.get
  private val countChecksum = 333036L
  private val sumChecksum = 166553391274L

  override def beforeEach(): Unit =
    super.beforeEach()
    ScarletRuntime.get.shuffle.clear()
    arm(batch = 8, threads = 1)

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(original)
    ()

  "an empty owned partition" should "count and sum zero without calling the predicate" in {
    val calls = new AtomicInteger
    val filtered = DistCollection
      .ofIntPairs(Array.empty[Int], Array.empty[Int], 1)
      .filterKeys { (key: Int) =>
        val _ = calls.incrementAndGet()
        key >= 0
      }

    val counted = measure(filtered.count())
    counted.result shouldBe 0L
    counted.visits shouldBe 1L
    counted.batches shouldBe 0L
    counted.encodes shouldBe 0L
    counted.decoded shouldBe 0L
    calls.get() shouldBe 0
    calls.set(0)
    measure(filtered.values.sum).result shouldBe 0L
    calls.get() shouldBe 0
    filtered.count() shouldBe twoPassCount(filtered)
    filtered.values.sum shouldBe twoPassSum(filtered, 1)
  }

  "one owned row" should "count a match and drop a miss" in {
    arm(batch = 8, threads = 1)
    val kept = DistCollection
      .ofIntPairs(Array(4), Array(5), 1)
      .filterValues((value: Int) => value >= 0)
    val dropped = DistCollection
      .ofIntPairs(Array(4), Array(-1), 1)
      .filterValues((value: Int) => value >= 0)

    kept.count() shouldBe 1L
    kept.values.sum shouldBe 5L
    kept.keys.sum shouldBe 4L
    dropped.count() shouldBe 0L
    dropped.values.sum shouldBe 0L
    dropped.keys.sum shouldBe 0L
    kept.count() shouldBe twoPassCount(kept)
    dropped.values.sum shouldBe twoPassSum(dropped, 1)
  }

  "an owned scan" should "keep every row or no row" in {
    val keys = Array(1, 2, 3, 4)
    val values = Array(10, -2, 0, 7)
    val all = DistCollection
      .ofIntPairs(keys, values, 1)
      .filterKeys((_: Int) => true)
    val none = DistCollection
      .ofIntPairs(keys, values, 1)
      .filterKeys((_: Int) => false)

    all.count() shouldBe 4L
    all.values.sum shouldBe 15L
    all.keys.sum shouldBe 10L
    none.count() shouldBe 0L
    none.values.sum shouldBe 0L
    none.keys.sum shouldBe 0L
    all.count() shouldBe twoPassCount(all)
    none.values.sum shouldBe twoPassSum(none, 1)
    none.keys.sum shouldBe twoPassSum(none, 0)
  }

  "a null value" should "add nothing under sum and not reach the value predicate" in {
    val calls = new AtomicInteger
    val filtered = boxedSource.filterValues { (value: Int) =>
      val _ = calls.incrementAndGet()
      value >= 0
    }

    calls.set(0)
    val counted = measure(filtered.count())
    counted.result shouldBe 4L
    counted.batches shouldBe 0L
    counted.encodes shouldBe 0L
    calls.get() shouldBe 5
    calls.set(0)
    val summed = measure(filtered.values.sum)
    summed.result shouldBe 26L
    summed.batches shouldBe 0L
    summed.encodes shouldBe 0L
    calls.get() shouldBe 5
    filtered.keys.sum shouldBe 8L
    filtered.count() shouldBe twoPassCount(filtered)
    filtered.values.sum shouldBe twoPassSum(filtered, 1)
    filtered.keys.sum shouldBe twoPassSum(filtered, 0)
  }

  "two filters" should "skip a null value and a key the first predicate rejects" in {
    val keyCalls = new AtomicInteger
    val valueCalls = new AtomicInteger
    val filtered = boxedSource
      .filterKeys { (key: Int) =>
        val _ = keyCalls.incrementAndGet()
        key > 0
      }
      .filterValues { (value: Int) =>
        val _ = valueCalls.incrementAndGet()
        value >= 0
      }

    keyCalls.set(0)
    valueCalls.set(0)
    val counted = measure(filtered.count())
    counted.result shouldBe 3L
    counted.batches shouldBe 0L
    counted.encodes shouldBe 0L
    counted.decoded shouldBe 0L
    keyCalls.get() shouldBe 6
    valueCalls.get() shouldBe 4
    keyCalls.set(0)
    valueCalls.set(0)
    measure(filtered.values.sum).result shouldBe 17L
    keyCalls.get() shouldBe 6
    valueCalls.get() shouldBe 4
    filtered.keys.sum shouldBe 8L
    filtered.count() shouldBe twoPassCount(filtered)
    filtered.values.sum shouldBe twoPassSum(filtered, 1)
  }

  "a key filter over a null value" should "keep the row and skip that measure" in {
    val filtered = boxedSource.filterKeys((key: Int) => key > 0)

    filtered.count() shouldBe 5L
    filtered.values.sum shouldBe 16L
    filtered.keys.sum shouldBe 14L
    ColumnarPlanner.tryCount(filtered.plan) shouldBe Some(5L)
    ColumnarPlanner.trySumInt(filtered.values.plan) shouldBe Some(16L)
    filtered.count() shouldBe twoPassCount(filtered)
    filtered.values.sum shouldBe twoPassSum(filtered, 1)
    filtered.keys.sum shouldBe twoPassSum(filtered, 0)
  }

  "owned morsels" should "fold the same total on one thread and on four" in {
    val rows = 20
    val keys = Array.tabulate(rows)(identity)
    val values = Array.tabulate(rows)(row => row - 5)
    val parts = 2
    val batch = 4
    val expectedVisits = visitTotal(rows, parts, batch)
    val expected = keys.indices.foldLeft(0L) { (total, row) =>
      if (keys(row) % 3 != 0 && values(row) >= 0) then total + 1L else total
    }

    List(1, 4).foreach { threads =>
      arm(batch, threads)
      val filtered = DistCollection
        .ofIntPairs(keys, values, parts)
        .filterKeys((key: Int) => key % 3 != 0)
        .filterValues((value: Int) => value >= 0)
      val counted = measure(filtered.count())
      counted.result shouldBe expected
      counted.visits shouldBe expectedVisits
      counted.batches shouldBe 0L
      counted.encodes shouldBe 0L
      counted.decoded shouldBe 0L
      measure(filtered.values.sum).visits shouldBe expectedVisits
      filtered.count() shouldBe twoPassCount(filtered)
      filtered.values.sum shouldBe twoPassSum(filtered, 1)
      filtered.keys.sum shouldBe twoPassSum(filtered, 0)
    }
  }

  "a non-zero start" should "match Selection on the copied batch" in {
    val keys = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val values = Array(0, -1, 2, -3, 4, 5, -6, 7, 8, -9)
    val calls = new AtomicInteger
    val steps = List(
      Selection.Step.Int32(0, new Selection.Int32Function(key => key % 2 == 0)),
      Selection.Step.Int32(
        1,
        new Selection.Int32Function(value => {
          val _ = calls.incrementAndGet()
          value >= 0
        }),
      ),
    )
    val batch = BatchCodec.copiedIntPairs(keys, values)
    val start = 3
    val length = 6

    PairSourceFold.count(keys, values, start, length, steps) shouldBe
      Selection.count(batch, start, length, steps)
    PairSourceFold.sum(keys, values, start, length, steps, 1) shouldBe
      Selection.sumInt32(batch, start, length, steps, 1)
    PairSourceFold.sum(keys, values, start, length, steps, 0) shouldBe
      Selection.sumInt32(batch, start, length, steps, 0)
    calls.set(0)
    PairSourceFold.count(keys, values, start, 0, steps) shouldBe 0L
    PairSourceFold.sum(keys, values, start, 0, steps, 1) shouldBe 0L
    calls.get() shouldBe 0
  }

  "random predicates" should "agree with the old walk and with an array oracle" in {
    val batches = Array(1, 3, 1024)
    (1 to 40).foreach { seed =>
      val random = new Random(seed)
      val rows = seed % 17
      val parts = 1 + seed % 3
      val batch = batches(seed % batches.length)
      val threads = if (seed % 2 == 0) then 1 else 4
      val modulus = 1 + random.nextInt(17)
      val remainder = random.nextInt(modulus)
      val threshold = random.nextInt(200) - 100
      val modulus2 = 1 + random.nextInt(11)
      val remainder2 = random.nextInt(modulus2)
      val keys = Array.fill(rows)(random.nextInt(201) - 100)
      val values = Array.fill(rows)(random.nextInt(201) - 100)
      val keepKey = (key: Int) => key % modulus != remainder
      val keepValue = (value: Int) => value >= threshold
      val keepKey2 = (key: Int) => key % modulus2 != remainder2
      val keepRow = (key: Int, value: Int) => key + value > threshold
      val mode = seed % 5

      arm(batch, threads)
      val filtered = mode match
        case 0 => DistCollection.ofIntPairs(keys, values, parts).filterKeys(keepKey)
        case 1 => DistCollection.ofIntPairs(keys, values, parts).filterValues(keepValue)
        case 2 =>
          DistCollection
            .ofIntPairs(keys, values, parts)
            .filterKeys(keepKey)
            .filterValues(keepValue)
        case 3 =>
          DistCollection.ofIntPairs(keys, values, parts).filter { case (key, value) =>
            keepRow(key, value)
          }
        case _ =>
          DistCollection
            .ofIntPairs(keys, values, parts)
            .filterKeys(keepKey)
            .filterValues(keepValue)
            .filterKeys(keepKey2)

      var index = 0
      var counted = 0L
      var summed = 0L
      var keyed = 0L
      while (index < rows) {
        val key = keys(index)
        val value = values(index)
        val keep = mode match
          case 0 => keepKey(key)
          case 1 => keepValue(value)
          case 2 => keepKey(key) && keepValue(value)
          case 3 => keepRow(key, value)
          case _ => keepKey(key) && keepValue(value) && keepKey2(key)
        if (keep) then
          counted += 1L
          summed += value.toLong
          keyed += key.toLong
        index += 1
      }

      withClue(s"seed $seed rows $rows batch $batch threads $threads mode $mode: ") {
        val notes = measure(filtered.count())
        notes.result shouldBe counted
        notes.batches shouldBe 0L
        notes.encodes shouldBe 0L
        notes.decoded shouldBe 0L
        notes.visits shouldBe visitTotal(rows, parts, batch)
        filtered.values.sum shouldBe summed
        filtered.keys.sum shouldBe keyed
        filtered.count() shouldBe twoPassCount(filtered)
        filtered.values.sum shouldBe twoPassSum(filtered, 1)
        filtered.keys.sum shouldBe twoPassSum(filtered, 0)
      }
    }
  }

  "random nulls" should "agree with the encode-then-walk" in {
    (1 to 12).foreach { seed =>
      val random = new Random(1000 + seed)
      val rows = 1 + random.nextInt(24)
      val parts = 1 + seed % 3
      val keys = Array.fill(rows)(random.nextInt(21) - 10)
      val values = Array.fill(rows)(random.nextInt(21) - 10)
      val boxed = (0 until rows).map { row =>
        if (row == 0) then (keys(row), values(row))
        else if (row % 5 == 0) then erased(None, Some(values(row)))
        else if (row % 5 == 1) then erased(Some(keys(row)), None)
        else (keys(row), values(row))
      }
      arm(batch = 3, threads = if (seed % 2 == 0) then 1 else 4)
      val filtered = DistCollection(boxed, parts)
        .filterKeys((key: Int) => key % 2 != 0)
        .filterValues((value: Int) => value >= 0)

      withClue(s"null seed $seed rows $rows: ") {
        val notes = measure(filtered.count())
        notes.batches shouldBe 0L
        notes.encodes shouldBe 0L
        notes.decoded shouldBe 0L
        notes.visits shouldBe parts.toLong
        notes.result shouldBe twoPassCount(filtered)
        filtered.values.sum shouldBe twoPassSum(filtered, 1)
        filtered.keys.sum shouldBe twoPassSum(filtered, 0)
      }
    }
  }

  "the cross-engine pair" should "keep the published checksums" in {
    val rows = 1000000
    val parts = 2
    val batch = 8192
    arm(batch, threads = 4)
    val keys = Array.tabulate(rows)(row => row % 1024)
    val values = lcgValues(rows)
    val filtered = DistCollection
      .ofIntPairs(keys, values, parts)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)

    val counted = measure(filtered.count())
    counted.result shouldBe countChecksum
    counted.batches shouldBe 0L
    counted.encodes shouldBe 0L
    counted.decoded shouldBe 0L
    counted.visits shouldBe visitTotal(rows, parts, batch)
    val summed = measure(filtered.values.sum)
    summed.result shouldBe sumChecksum
    summed.batches shouldBe 0L
    summed.encodes shouldBe 0L
    summed.visits shouldBe counted.visits
    filtered.count() shouldBe twoPassCount(filtered)
    filtered.values.sum shouldBe twoPassSum(filtered, 1)
    filtered.keys.sum shouldBe twoPassSum(filtered, 0)
  }

  "a non-int component" should "use the encode rejection" in {
    val badValue = DistCollection(
      Seq((1, 2), erasedCells(Int.box(3), "nope")),
      1,
    ).filterKeys((_: Int) => true)
    val valueThrown = the[IllegalArgumentException] thrownBy badValue.count()
    valueThrown.getMessage shouldBe "pair component expected an Int value, found java.lang.String"

    val badKey = DistCollection(
      Seq((1, 2), erasedCells("nope", Int.box(4))),
      1,
    ).filterValues((_: Int) => true)
    val keyThrown = the[IllegalArgumentException] thrownBy badKey.values.sum
    keyThrown.getMessage shouldBe "pair component expected an Int key, found java.lang.String"
  }

  /** Same rows as [[TestNullPairScan]]. The null value sits on a key the key filter keeps. */
  private def boxedSource: DistCollection[(Int, Int)] =
    DistCollection(
      Seq(
        (1, 10),
        erased(Some(2), None),
        (2, 7),
        (5, 0),
        (4, -1),
        (0, 9),
      ),
      1,
    )

  private def arm(batch: Int, threads: Int): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(
      original.copy(
        columnarExecution = true,
        columnarBatchSize = batch,
        threadPoolSize = threads,
      ),
    )

  private final case class Notes(
      result: Long,
      batches: Long,
      visits: Long,
      encodes: Long,
      decoded: Long,
  )

  private def measure(body: => Long): Notes =
    ScarletRuntime.get.shuffle.clear()
    ColumnMetrics.withSink(new CountingSink) {
      Notes(
        body,
        ColumnMetrics.batchCount,
        ColumnMetrics.visitCount,
        ColumnMetrics.primitiveEncodeCount,
        ColumnMetrics.decodedValueCount,
      )
    }

  /** Same split as [[DistCollection.ofIntPairs]]: the first `rows % parts` partitions are longer. */
  private def visitTotal(rows: Int, parts: Int, batch: Int): Long =
    val base = rows / parts
    val extra = rows % parts
    var part = 0
    var visits = 0L
    while (part < parts) {
      val length = base + (if (part < extra) then 1 else 0)
      visits += ColumnBatches.cuts(length, batch).length.toLong
      part += 1
    }
    visits

  private def twoPassCount(filtered: DistCollection[(Int, Int)]): Long =
    val (plan, op) = lowered(filtered.plan)
    val steps = countSteps(plan, op)
    val batches = SourceEncode.encodePairs(pairScan(plan, op), ColumnUse.ordinals(plan, op))
    foldEncoded(batches) { morsel =>
      Selection.count(morsel.batch, morsel.start, morsel.length, steps)
    }

  private def twoPassSum(filtered: DistCollection[(Int, Int)], ordinal: Int): Long =
    val (plan, op) = lowered(filtered.plan)
    val steps = sumSteps(plan, op)
    val batches = SourceEncode.encodeGroupedPairs(pairScan(plan, op))
    foldEncoded(batches) { morsel =>
      Selection.sumInt32(morsel.batch, morsel.start, morsel.length, steps, ordinal)
    }

  private def foldEncoded(batches: Vector[com.ewoodbury.scarlet.columnar.ColumnBatch])(
      fn: Morsel => Long,
  ): Long =
    if (batches.isEmpty) then 0L
    else
      val morsels = batches.flatMap(batch => Morsels.of(batch, ColumnarPlanner.batchSize))
      val partials =
        MorselScheduler.map(morsels, ColumnarPlanner.parallelism)((_, morsel) => fn(morsel))
      partials.foldLeft(0L)(_ + _)

  private def lowered(plan: Plan[_]): (Plan[_], PhysicalOp) =
    val op = ColumnarPlanner.lower(plan).getOrElse(fail(s"lower refused $plan"))
    (plan, op)

  private def countSteps(plan: Plan[_], op: PhysicalOp): List[Selection.Step] =
    val compiled =
      PredicatePlan.compile(plan, op).getOrElse(fail("count filters did not compile"))
    PredicatePlan
      .selectionSteps(PredicatePlan.encoded(compiled, ColumnUse.ordinals(plan, op)))
      .getOrElse(fail("count filters did not become steps"))
      .toList

  private def sumSteps(plan: Plan[_], op: PhysicalOp): List[Selection.Step] =
    val compiled = PredicatePlan.compile(plan, op).getOrElse(fail("sum filters did not compile"))
    PredicatePlan.selectionSteps(compiled).getOrElse(fail("sum filters did not become steps")).toList

  private def pairScan(plan: Plan[_], op: PhysicalOp): Seq[Partition[_]] =
    PredicatePlan.childOf(plan, op) match
      case (_, PhysicalOp.Scan(PhysicalOp.Kind.Pairs, partitions)) => partitions
      case (_, other) => fail(s"expected a pair scan, found ${other.shape}")

  private def lcgValues(count: Int): Array[Int] =
    val generated = new Array[Int](count)
    var index = 0
    var state = 0x12345678
    while (index < count) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      generated(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    generated

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
    erasedCells(left, right)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def erasedCells(key: AnyRef, value: AnyRef): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]
