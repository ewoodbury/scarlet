package com.ewoodbury.scarlet.columnar

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.execution.ExecutionModuleSupport
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Long `reduceByKey` at one million and ten million rows.
 *
 * Duplicate-heavy input keeps the direct value table: the key span is 1024, not the row count. A
 * null, a unique key domain, or a key that escapes the prefix stays on the hash table. The 3
 * million row case is one worker and 682 keys. Its direct table has to stay under the span cap.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class TestLongReduceScale extends AnyFlatSpec with Matchers with BeforeAndAfterEach:

  private val groups = 1024
  private val batchSize = 8192
  private val scales = List(1000000, 10000000)
  private val originalConf = ScarletConf.get
  private val keepAll: ColumnKernel.Int64Predicate = _ => true
  private val keepKey: ColumnKernel.Int64Predicate = key => key % 3L != 0L
  private val keepValue: ColumnKernel.Int64Predicate = value => value >= 0L
  private val twoFilters =
    List(Selection.Step.Int64(0, keepKey), Selection.Step.Int64(1, keepValue))
  private val plus: LongHashReduce.LongBinOp = (left: Long, right: Long) => left + right

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
        spillBudgetBytes = 64L * 1024L * 1024L,
      ),
    )
    ScarletRuntime.get.shuffle.clear()
    DirectLongReduce.resetPeakForTest()
    LongHashReduce.resetReuseForTest()

  override def afterEach(): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(originalConf)
    ()

  "reduceByKey" should "match the filtered checksum at 1e6 and 1e7 rows" in {
    assertFiltered(1000000, 166564205407L)
    assertFiltered(10000000, 1663934572679L)
  }

  it should "sum one key at both scales in one and four partitions" in {
    scales.foreach(rows => List(1, 4).foreach(parts => assertSingleKey(rows, parts)))
  }

  it should "keep every unique key at both scales in one and four partitions" in {
    scales.foreach(rows => List(1, 4).foreach(parts => assertUnique(rows, parts)))
  }

  it should "drop null keys and keep a null value at both scales" in {
    scales.foreach { rows =>
      List(1, 4).foreach { parts =>
        assertNullKey(rows, parts)
        assertNullValue(rows, parts)
      }
    }
  }

  "FoldedLongReduce" should "keep a 3M-row stripe of 682 keys off a row-sized table" in {
    val rows = 3000000
    val keys = Array.tabulate(rows)(row => (row % groups).toLong)
    val values = Array.fill(rows)(1L)
    DirectLongReduce.resetPeakForTest()
    LongHashReduce.resetReuseForTest()
    val reduced = FoldedLongReduce.reduce(
      Vector(dense(keys, values)),
      twoFilters,
      numPartitions = 1,
      parallelism = 1,
      batchSize,
      plus,
    )
    val found = decode(reduced)
    val sums = expectedSums(keys, values, keepValue = false)
    found.length shouldBe 682
    found.map(_._1) shouldBe expectedKeys(keys, values, keepValue = false)
    found.foreach { case (key, value) => value shouldBe sums(key.toInt) }
    val peak = DirectLongReduce.peakSlotsForTest
    val bound = ColumnHash.tableSize(rows) / 64
    withClue(s"peak $peak slots, row-sized bound $bound ") {
      DirectLongReduce.hitsForTest should be > 0
      peak should be > 0
      peak should be <= 4096
      peak should be < bound
    }
    LongHashReduce.peakSlotsForTest should be < bound
  }

  it should "hash a key that escapes the prefix domain" in {
    val rows = 9000
    val keys = Array.tabulate(rows)(row => if (row < 8192) then 1L else 50000L)
    val values = Array.fill(rows)(1L)
    DirectLongReduce.resetPeakForTest()
    val reduced = FoldedLongReduce.reduce(
      Vector(dense(keys, values)),
      List(Selection.Step.Int64(0, keepAll)),
      numPartitions = 1,
      parallelism = 1,
      batchSize,
      plus,
    )
    decode(reduced) shouldBe Seq((1L, 8192L), (50000L, 808L))
    DirectLongReduce.hitsForTest shouldBe 0
  }

  private def assertFiltered(rows: Int, checksum: Long): Unit =
    val keys = Array.tabulate(rows)(row => (row % groups).toLong)
    val values = lcg(rows)
    val order = expectedKeys(keys, values, keepValue = true)
    val sums = expectedSums(keys, values, keepValue = true)
    cross(order, sums) shouldBe checksum
    List(1, 4).foreach { parts =>
      DirectLongReduce.resetPeakForTest()
      val found = reduceFiltered(keys, values, parts)
      crossPairs(found) shouldBe checksum
      found.map(_._1).distinct.size shouldBe order.length
      found.foreach { case (key, value) =>
        value shouldBe sums(key.toInt)
      }
      if (parts == 1) then found.map(_._1) shouldBe order
      DirectLongReduce.hitsForTest should be > 0
      DirectLongReduce.peakSlotsForTest should be <= 4096
    }

  private def assertSingleKey(rows: Int, parts: Int): Unit =
    val keys = Array.fill(rows)(7L)
    val values = Array.fill(rows)(1L)
    DirectLongReduce.resetPeakForTest()
    val found = reduceAll(keys, values, parts)
    found shouldBe List(7L -> rows.toLong)
    DirectLongReduce.hitsForTest should be > 0
    DirectLongReduce.peakSlotsForTest should be <= 4096

  private def assertUnique(rows: Int, parts: Int): Unit =
    val keys = Array.tabulate(rows)(row => row.toLong)
    val values = Array.fill(rows)(1L)
    val steps = List(Selection.Step.Int64(0, keepAll), Selection.Step.Int64(1, keepValue))
    ScarletConf.set(
      ScarletConf.get.copy(defaultShufflePartitions = parts, spillBudgetBytes = 1L << 30),
    )
    ScarletRuntime.get.shuffle.clear()
    DirectLongReduce.resetPeakForTest()
    val reduced =
      FoldedLongReduce.reduce(Vector(dense(keys, values)), steps, parts, 4, batchSize, plus)
    val seen = new Array[Boolean](rows)
    var count = 0
    reduced.foreach { batch =>
      val outKeys = ColumnHash.int64At(batch, 0)
      val outValues = ColumnHash.int64At(batch, 1)
      var row = 0
      while (row < batch.length) do
        val key = outKeys.values(row).toInt
        outValues.values(row) shouldBe 1L
        seen(key) shouldBe false
        seen(key) = true
        count += 1
        row += 1
    }
    count shouldBe rows
    seen.forall(identity) shouldBe true
    DirectLongReduce.hitsForTest shouldBe 0

  private def assertNullKey(rows: Int, parts: Int): Unit =
    val keys = Array.tabulate(rows)(row => (row % groups).toLong)
    val values = Array.fill(rows)(1L)
    val present = Array.tabulate(rows)(row => row % 10 != 0)
    val sums = new Array[Long](groups)
    var row = 0
    while (row < rows) do
      if (present(row)) then sums(keys(row).toInt) += 1L
      row += 1
    val batch = sparse(keys, present, values, Array.fill(rows)(true))
    val steps = List(Selection.Step.Int64(1, keepValue))
    DirectLongReduce.resetPeakForTest()
    val reduced = FoldedLongReduce.reduce(Vector(batch), steps, parts, 4, batchSize, plus)
    DirectLongReduce.hitsForTest shouldBe 0
    val found = decode(reduced)
    found.length shouldBe sums.count(_ > 0L)
    found.foreach { case (key, value) =>
      value shouldBe sums(key.toInt)
    }

  private def assertNullValue(rows: Int, parts: Int): Unit =
    val keys = Array.tabulate(rows) { row =>
      if (row == rows - 1) then 5000L else (row % groups).toLong
    }
    val values = Array.fill(rows)(1L)
    val present = Array.tabulate(rows) { row =>
      keys(row) != 3L && keys(row) != 5000L && row % 10 != 0
    }
    val state = new Array[Byte](groups)
    val sums = new Array[Long](groups)
    var row = 0
    while (row < rows) do
      val key = keys(row)
      if (key != 5000L) then note(state, sums, key.toInt, present(row), values(row))
      row += 1
    val batch = sparse(keys, Array.fill(rows)(true), values, present)
    val steps = List(Selection.Step.Int64(0, keepAll))
    DirectLongReduce.resetPeakForTest()
    val reduced = FoldedLongReduce.reduce(Vector(batch), steps, parts, 4, batchSize, plus)
    DirectLongReduce.hitsForTest shouldBe 0
    var lone: Option[Long] = None
    var published = 0
    reduced.foreach { batchOut =>
      published += batchOut.length
      val outKeys = ColumnHash.int64At(batchOut, 0)
      val outValues = ColumnHash.int64At(batchOut, 1)
      var cursor = 0
      while (cursor < batchOut.length) do
        val key = outKeys.values(cursor)
        if (key == 5000L) then
          outValues.isValid(cursor) shouldBe false
          lone = Some(key)
        else
          val mark = state(key.toInt)
          if (mark == 1) then outValues.isValid(cursor) shouldBe false
          else
            mark shouldBe 2.toByte
            outValues.isValid(cursor) shouldBe true
            outValues.values(cursor) shouldBe sums(key.toInt)
        cursor += 1
    }
    lone shouldBe Some(5000L)
    published shouldBe groups + 1
    state(3) shouldBe 2.toByte
    sums(3) shouldBe 0L

  private def note(
      state: Array[Byte],
      sums: Array[Long],
      key: Int,
      live: Boolean,
      value: Long,
  ): Unit =
    val mark = state(key)
    if (!live) then
      if (mark == 0) then state(key) = 1
      else if (mark == 1) then
        state(key) = 2
        sums(key) = 0L
      else sums(key) = sums(key) + 0L
    else if (mark == 0) then
      state(key) = 2
      sums(key) = value
    else if (mark == 1) then
      state(key) = 2
      sums(key) = value
    else sums(key) = sums(key) + value

  private def reduceFiltered(
      keys: Array[Long],
      values: Array[Long],
      parts: Int,
  ): List[(Long, Long)] =
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = parts))
    ScarletRuntime.get.shuffle.clear()
    DistCollection
      .ofLongPairs(keys, values, parts)
      .filterKeys((key: Long) => key % 3L != 0L)
      .filterValues((value: Long) => value >= 0L)
      .reduceByKey[Long, Long](_ + _)
      .collect()
      .toList

  private def reduceAll(keys: Array[Long], values: Array[Long], parts: Int): List[(Long, Long)] =
    ScarletConf.set(ScarletConf.get.copy(defaultShufflePartitions = parts))
    ScarletRuntime.get.shuffle.clear()
    DistCollection
      .ofLongPairs(keys, values, parts)
      .filterKeys((key: Long) => key >= 0L)
      .filterValues((value: Long) => value >= 0L)
      .reduceByKey[Long, Long](_ + _)
      .collect()
      .toList

  private def expectedKeys(
      keys: Array[Long],
      values: Array[Long],
      keepValue: Boolean,
  ): List[Long] =
    val seen = new Array[Boolean](groups)
    val order = new Array[Long](groups)
    var row = 0
    var written = 0
    while (row < keys.length) do
      val key = keys(row)
      val value = values(row)
      if (key % 3L != 0L && (!keepValue || value >= 0L) && !seen(key.toInt)) then
        seen(key.toInt) = true
        order(written) = key
        written += 1
      row += 1
    order.iterator.take(written).toList

  private def expectedSums(
      keys: Array[Long],
      values: Array[Long],
      keepValue: Boolean,
  ): Array[Long] =
    val sums = new Array[Long](groups)
    var row = 0
    while (row < keys.length) do
      val key = keys(row)
      val value = values(row)
      if (key % 3L != 0L && (!keepValue || value >= 0L)) then sums(key.toInt) += value
      row += 1
    sums

  private def cross(order: List[Long], sums: Array[Long]): Long =
    order.foldLeft(0L)((sum, key) => sum + key * 31L + sums(key.toInt))

  private def crossPairs(rows: List[(Long, Long)]): Long =
    rows.foldLeft(0L)((sum, row) => sum + row._1 * 31L + row._2)

  private def decode(batches: Vector[ColumnBatch]): List[(Long, Long)] =
    batches.flatMap(batch => BatchCodec.decodeLongPairs(batch)).toList

  private def dense(keys: Array[Long], values: Array[Long]): ColumnBatch =
    val rows = keys.length
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.of(keys, Validity.allValid(rows), rows),
        Int64Column.of(values, Validity.allValid(rows), rows),
      ),
      rows,
    )

  private def sparse(
      keys: Array[Long],
      keyPresent: Array[Boolean],
      values: Array[Long],
      valuePresent: Array[Boolean],
  ): ColumnBatch =
    val rows = keys.length
    new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int64),
      Vector(
        Int64Column.of(keys, Validity.pack(rows, row => keyPresent(row)), rows),
        Int64Column.of(values, Validity.pack(rows, row => valuePresent(row)), rows),
      ),
      rows,
    )

  private def lcg(count: Int): Array[Long] =
    val generated = new Array[Long](count)
    var index = 0
    var state = 0x12345678
    while (index < count) do
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      generated(index) = ((mixed % 2000001) - 1000000).toLong
      index += 1
    generated
