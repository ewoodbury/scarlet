package com.ewoodbury.scarlet.columnar

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.execution.ExecutionModuleSupport
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * `countByKey` at one million and ten million rows.
 *
 * Spill is on. A stripe is charged one morsel, then sized by the keys it actually inserts. The 3
 * million row case is one worker and 682 keys: its largest table has to stay far below a table
 * sized for the rows.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class TestCountKeyScale extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val groups = 1024
  private val batchSize = 8192
  private val budget = 64L * 1024L * 1024L
  private val originalConf = ScarletConf.get
  private val keep: ColumnKernel.Int32Predicate = _ => true
  private val keepKey: ColumnKernel.Int32Predicate = key => key % 3 != 0
  private val keepValue: ColumnKernel.Int32Predicate = value => value >= 0
  private val twoFilters =
    List(Selection.Step.Int32(0, keepKey), Selection.Step.Int32(1, keepValue))

  override def beforeEach(): Unit =
    super.beforeEach()
    ExecutionModuleSupport.initialize()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        threadPoolSize = 4,
        defaultShufflePartitions = 4,
        spillEnabled = true,
        spillBudgetBytes = budget,
      ),
    )
    ScarletRuntime.get.shuffle.clear()
    FoldedCount.resetPassesForTest()
    FoldedCount.resetReuseForTest()
    HashCount.resetReuseForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "countByKey" should "match the filtered checksum at 1e6 and 1e7 rows" in {
    assertFiltered(1000000, 11147169L)
    assertFiltered(10000000, 14142272L)
  }

  it should "count one key at both scales in one and four partitions" in {
    List(1000000, 10000000).foreach { rows =>
      List(1, 4).foreach { parts =>
        val keys = Array.fill(rows)(7)
        val values = Array.fill(rows)(1)
        val found = countAll(keys, values, parts)
        found shouldBe List(7 -> rows)
        FoldedCount.passesForTest shouldBe 1
      }
    }
  }

  it should "count every unique key that fits and refuse ten million" in {
    assertUnique(1000000)
    List(1, 4).foreach { parts =>
      val rows = 10000000
      val keys = Array.tabulate(rows)(identity)
      val values = Array.fill(rows)(0)
      val refused = the[ClosedKernelRefusal] thrownBy countAll(keys, values, parts)
      refused.budget shouldBe budget
      refused.bytes should be > budget
    }
  }

  it should "drop null keys and still count a null value at both scales" in {
    List(1000000, 10000000).foreach { rows =>
      assertNullKey(rows)
      assertNullValue(rows)
    }
  }

  "FoldedCount" should "keep a 3M-row stripe of 682 keys off a row-sized table" in {
    val rows = 3000000
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = Array.fill(rows)(1)
    val seen = tally(keys, values, keepAllValues = true)
    FoldedCount.resetPassesForTest()
    FoldedCount.resetReuseForTest()
    val counted = FoldedCount
      .countOwned(
        Vector((keys, values)),
        twoFilters,
        numPartitions = 1,
        parallelism = 1,
        batchSize,
      )
      .getOrElse(fail("folded count declined"))
    FoldedCount.passesForTest shouldBe 1
    assertCounts(counted, seen)
    val peak = FoldedCount.peakSlotsForTest
    val bound = ColumnHash.tableSize(rows) / 64
    withClue(s"peak $peak slots, row-sized bound $bound ") {
      peak should be > 0
      peak should be < bound
    }
  }

  it should "grow a short-morsel table in first-seen order" in {
    val rows = 60
    val keys = Array.tabulate(rows)(row => row % 30)
    val values = Array.fill(rows)(0)
    val steps = List(Selection.Step.Int32(1, keep))
    val counted = FoldedCount
      .countOwned(Vector((keys, values)), steps, numPartitions = 1, parallelism = 1, batchSize = 4)
      .getOrElse(fail("folded count declined"))
    val found = decode(counted)
    found.map(_._1) shouldBe (0 until 30).toList
    found.map(_._2).distinct shouldBe List(2)
  }

  it should "refuse a grow before the next table exceeds the budget" in {
    ScarletConf.set(ScarletConf.get.copy(spillEnabled = true, spillBudgetBytes = 1000L))
    FoldedCount.resetReuseForTest()
    val rows = 200
    val keys = Array.tabulate(rows)(identity)
    val values = Array.fill(rows)(0)
    val steps = List(Selection.Step.Int32(0, keep))
    val refused = the[ClosedKernelRefusal] thrownBy {
      val _ = FoldedCount
        .countOwned(
          Vector((keys, values)),
          steps,
          numPartitions = 1,
          parallelism = 1,
          batchSize = 4,
        )
        .getOrElse(fail("folded count declined"))
    }
    refused.bytes shouldBe 128L * 13L
    refused.bytes should be > refused.budget
    FoldedCount.peakSlotsForTest shouldBe 64
  }

  it should "count a dense span in first-seen order" in {
    val rows = 200
    val keys = Array.tabulate(rows)(row => row % 50)
    val values = Array.fill(rows)(0)
    val steps = List(Selection.Step.Int32(0, keep), Selection.Step.Int32(1, keep))
    val counted = FoldedCount
      .countOwned(Vector((keys, values)), steps, numPartitions = 1, parallelism = 1, batchSize = 8)
      .getOrElse(fail("folded count declined"))
    val found = decode(counted)
    found.map(_._1) shouldBe (0 until 50).toList
    found.map(_._2).distinct shouldBe List(4)
  }

  private def assertFiltered(rows: Int, checksum: Long): Unit =
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = lcg(rows)
    val seen = tally(keys, values, keepAllValues = false)
    cross(seen) shouldBe checksum
    val order = firstSeen(keys, values)
    List(1, 4).foreach { parts =>
      val found = countAction(keys, values, parts)
      FoldedCount.passesForTest shouldBe 1
      crossRows(found) shouldBe checksum
      found.size shouldBe seen.count(_ > 0)
      found.foreach { case (key, count) =>
        count shouldBe seen(key)
      }
      if (parts == 1) then found.map(_._1) shouldBe order
    }

  private def assertUnique(rows: Int): Unit =
    val keys = Array.tabulate(rows)(identity)
    val values = Array.fill(rows)(0)
    List(1, 4).foreach { parts =>
      val found = countAll(keys, values, parts)
      FoldedCount.passesForTest shouldBe 1
      val hit = new Array[Boolean](rows)
      found.foreach { case (key, count) =>
        count shouldBe 1
        hit(key) shouldBe false
        hit(key) = true
      }
      hit.forall(identity) shouldBe true
    }

  private def assertNullKey(rows: Int): Unit =
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = Array.fill(rows)(1)
    val batch = column(keys, values, keyPresent = row => row % 10 != 0, valuePresent = _ => true)
    val seen = new Array[Int](groups)
    var row = 0
    while (row < rows) {
      if (row % 10 != 0) then seen(keys(row)) += 1
      row += 1
    }
    val steps = List(Selection.Step.Int32(1, keep))
    List(1, 4).foreach { parts =>
      val counted = FoldedCount
        .count(Vector(batch), steps, parts, parallelism = 4, batchSize)
        .getOrElse(fail("null-key count declined"))
      assertCounts(counted, seen)
    }

  private def assertNullValue(rows: Int): Unit =
    val keys = Array.tabulate(rows)(row => row % groups)
    val values = Array.fill(rows)(1)
    val batch = column(keys, values, keyPresent = _ => true, valuePresent = row => row % 10 != 0)
    val every = new Array[Int](groups)
    val presentOnly = new Array[Int](groups)
    var row = 0
    while (row < rows) {
      every(keys(row)) += 1
      if (row % 10 != 0) then presentOnly(keys(row)) += 1
      row += 1
    }
    val unread = List(Selection.Step.Int32(0, keep))
    val read = List(Selection.Step.Int32(0, keep), Selection.Step.Int32(1, keepValue))
    List(1, 4).foreach { parts =>
      val kept = FoldedCount
        .count(Vector(batch), unread, parts, parallelism = 4, batchSize)
        .getOrElse(fail("null-value count declined"))
      assertCounts(kept, every)
      val dropped = FoldedCount
        .count(Vector(batch), read, parts, parallelism = 4, batchSize)
        .getOrElse(fail("null-value filter declined"))
      assertCounts(dropped, presentOnly)
    }

  private def countAll(keys: Array[Int], values: Array[Int], parts: Int): List[(Int, Int)] =
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = parts))
    ScarletRuntime.get.shuffle.clear()
    FoldedCount.resetPassesForTest()
    DistCollection
      .ofIntPairs(keys, values, parts)
      .filterKeys((key: Int) => key >= 0)
      .filterValues((value: Int) => value >= 0)
      .countByKey
      .collect()
      .toList

  private def countAction(keys: Array[Int], values: Array[Int], parts: Int): List[(Int, Int)] =
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = parts))
    ScarletRuntime.get.shuffle.clear()
    FoldedCount.resetPassesForTest()
    DistCollection
      .ofIntPairs(keys, values, parts)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .countByKey
      .collect()
      .toList

  private def assertCounts(batches: Vector[ColumnBatch], seen: Array[Int]): Unit =
    val found = decode(batches)
    found.size shouldBe seen.count(_ > 0)
    found.foreach { case (key, count) =>
      count shouldBe seen(key)
    }

  private def decode(batches: Vector[ColumnBatch]): List[(Int, Int)] =
    batches.flatMap(batch => BatchCodec.decodeIntPairs(batch)).toList

  private def tally(keys: Array[Int], values: Array[Int], keepAllValues: Boolean): Array[Int] =
    val seen = new Array[Int](groups)
    var row = 0
    while (row < keys.length) {
      val key = keys(row)
      val value = values(row)
      if (key % 3 != 0 && (keepAllValues || value >= 0)) then seen(key) += 1
      row += 1
    }
    seen

  private def firstSeen(keys: Array[Int], values: Array[Int]): List[Int] =
    val hit = new Array[Boolean](groups)
    val order = new Array[Int](groups)
    var row = 0
    var written = 0
    while (row < keys.length) {
      val key = keys(row)
      if (key % 3 != 0 && values(row) >= 0 && !hit(key)) then
        hit(key) = true
        order(written) = key
        written += 1
      row += 1
    }
    order.iterator.take(written).toList

  private def cross(seen: Array[Int]): Long =
    var key = 0
    var sum = 0L
    while (key < seen.length) {
      if (seen(key) > 0) then sum += key.toLong * 31L + seen(key).toLong
      key += 1
    }
    sum

  private def crossRows(rows: List[(Int, Int)]): Long =
    rows.foldLeft(0L)((sum, row) => sum + row._1.toLong * 31L + row._2.toLong)

  private def lcg(rows: Int): Array[Int] =
    val values = new Array[Int](rows)
    var index = 0
    var state = 0x12345678
    while (index < rows) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    values

  private def column(
      keys: Array[Int],
      values: Array[Int],
      keyPresent: Int => Boolean,
      valuePresent: Int => Boolean,
  ): ColumnBatch =
    val rows = keys.length
    require(values.length == rows, "key and value columns differ in length")
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.pack(rows, keyPresent), rows),
        Int32Column.of(values, Validity.pack(rows, valuePresent), rows),
      ),
      rows,
    )
