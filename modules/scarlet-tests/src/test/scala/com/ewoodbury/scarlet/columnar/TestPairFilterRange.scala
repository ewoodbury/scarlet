package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Range scans for pair and triple filters. A window can start past row 0, and a tail can be
 * shorter than the batch. A null pair component inside the window drops that row and is not passed
 * to the predicate. Nulls outside the window are not visited. A null triple component inside the
 * window is still named at its absolute row, and the predicate is not called. Output columns are
 * sized to the range. A full-range scan matches the whole-batch call.
 */
class TestPairFilterRange extends AnyFlatSpec with Matchers:

  private val poison = 999
  private val schema2 = Vector(LogicalType.Int32, LogicalType.Int32)
  private val schema3 = Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32)

  "a pair filter range" should "skip nulls before, inside, and after a window past row 0" in {
    val keys = Array(poison, 2, 3, 4, poison, 6, 7, poison, 9)
    val values = Array(1, poison, 30, 40, 50, poison, -1, poison, 90)
    val keyPresent = Array(false, true, true, true, false, true, true, false, true)
    val valuePresent = Array(true, false, true, true, true, false, true, false, true)
    val start = 3
    val length = 4
    val batch = pairs(keys, values, keyPresent, valuePresent)
    val calls = new AtomicInteger
    val filtered = PairKernel.filter(
      batch,
      start,
      length,
      (key, value) => {
        calls.incrementAndGet()
        if (key == poison || value == poison) then
          throw new IllegalStateException("predicate saw a null slot")
        value > 0
      },
    )

    calls.get() shouldBe 2
    filtered.length shouldBe 1
    BatchCodec.decodeIntPairs(filtered) shouldBe Seq((4, 40))
    filtered.schema shouldBe batch.schema
    int32(filtered, 0).values.length shouldBe length
    int32(filtered, 1).values.length shouldBe length
    int32(filtered, 0).validity.capacity shouldBe length
    int32(filtered, 0).values should not be theSameInstanceAs(keys)
    int32(filtered, 1).values should not be theSameInstanceAs(values)
    keys(0) shouldBe poison
    values(8) shouldBe 90

    val nullOnly = PairKernel.filter(
      batch,
      4,
      1,
      (_, _) => throw new IllegalStateException("predicate saw a null row"),
    )
    nullOnly.length shouldBe 0
    int32(nullOnly, 0).values.length shouldBe 1
    int32(nullOnly, 1).values.length shouldBe 1
  }

  it should "scan a tail shorter than the batch" in {
    val batch = BatchCodec.intPairs((1 to 10).map(row => (row, row * 10)))
    val filtered = PairKernel.filter(batch, 8, 2, (key, _) => key % 2 == 0)

    BatchCodec.decodeIntPairs(filtered) shouldBe Seq((10, 100))
    filtered.length shouldBe 1
    int32(filtered, 0).values.length shouldBe 2
    int32(filtered, 1).values.length shouldBe 2
    int32(filtered, 0).validity.capacity shouldBe 2
    int32(filtered, 1).validity.capacity shouldBe 2
    int32(filtered, 0).values should not be theSameInstanceAs(int32(batch, 0).values)
  }

  it should "match the whole-batch filter when the range is the whole batch" in {
    val rows = Seq(
      (Some(1), Some(10)),
      (None, Some(9)),
      (Some(2), None),
      (Some(3), Some(-1)),
      (Some(4), Some(40)),
    )
    val batch = BatchCodec.nullableIntPairs(rows)
    val keep: PairKernel.IntPairPredicate = (key, value) => key > 0 && value > 0
    val whole = PairKernel.filter(batch, keep)
    val ranged = PairKernel.filter(batch, 0, batch.length, keep)

    BatchCodec.decodeIntPairs(whole) shouldBe Seq((1, 10), (4, 40))
    sameIntBatch(whole, ranged)
    int32(whole, 0).values.length shouldBe batch.length
    int32(whole, 1).values.length shouldBe batch.length
    int32(ranged, 0).values should not be theSameInstanceAs(int32(batch, 0).values)

    val rawKeys = Array(1, -2, 8, poison)
    val rawValues = Array(10, 20, 30, poison)
    val shortened = new ColumnBatch(
      schema2,
      Vector(
        Int32Column.of(rawKeys, Validity.allValid(rawKeys.length), length = 3),
        Int32Column.of(rawValues, Validity.allValid(rawValues.length), length = 3),
      ),
      3,
    )
    val positive: PairKernel.IntPairPredicate = (key, _) => key > 0
    val wholeShort = PairKernel.filter(shortened, positive)
    val rangedShort = PairKernel.filter(shortened, 0, shortened.length, positive)

    BatchCodec.decodeIntPairs(wholeShort) shouldBe Seq((1, 10), (8, 30))
    sameIntBatch(wholeShort, rangedShort)
    int32(wholeShort, 0).values.length shouldBe shortened.length
    rawKeys shouldBe Array(1, -2, 8, poison)
    rawValues shouldBe Array(10, 20, 30, poison)
  }

  it should "return an empty batch for an empty range" in {
    val batch = BatchCodec.intPairs(Seq((1, 10), (2, 20), (3, 30)))
    val calls = new AtomicInteger
    val filtered = PairKernel.filter(
      batch,
      2,
      0,
      (_, _) => {
        calls.incrementAndGet()
        true
      },
    )

    calls.get() shouldBe 0
    filtered.length shouldBe 0
    BatchCodec.decodeIntPairs(filtered) shouldBe Seq.empty[(Int, Int)]
    filtered.schema shouldBe batch.schema
    int32(filtered, 0).values.length shouldBe 0
    int32(filtered, 1).values.length shouldBe 0
    int32(filtered, 0).values should not be theSameInstanceAs(int32(batch, 0).values)
  }

  it should "publish an empty batch when the window is entirely rejected" in {
    val batch = BatchCodec.intPairs(Seq((1, 10), (2, 20), (3, 30), (4, 40), (5, 50)))
    val calls = new AtomicInteger
    val filtered = PairKernel.filter(
      batch,
      1,
      3,
      (key, value) => {
        calls.incrementAndGet()
        if (key == 1 || key == 5) then
          throw new IllegalStateException("predicate saw a row outside the range")
        value < 0
      },
    )

    calls.get() shouldBe 3
    filtered.length shouldBe 0
    BatchCodec.decodeIntPairs(filtered) shouldBe Seq.empty[(Int, Int)]
    int32(filtered, 0).values.length shouldBe 3
    int32(filtered, 1).values.length shouldBe 3
    int32(filtered, 0).validity.capacity shouldBe 3
    int32(filtered, 0).validity.words shouldBe Validity.prefixValid(3, 0).words
    int32(filtered, 1).validity.words shouldBe Validity.prefixValid(3, 0).words
  }

  "a triple filter range" should "keep survivors in a window and ignore nulls outside it" in {
    val keys = Array(1, poison, 3, 4, 5, 6, poison)
    val left = Array(10, 20, poison, 40, 50, 60, 70)
    val right = Array(7, 8, 9, 11, 12, -1, poison)
    val present = Array(true, false, false, true, true, true, false)
    val leftPresent = Array(true, true, false, true, true, true, true)
    val rightPresent = Array(true, true, true, true, true, true, false)
    val start = 3
    val length = 3
    val batch = triples(keys, left, right, present, leftPresent, rightPresent)
    val calls = new AtomicInteger
    val filtered = PairKernel.filterTriple(
      batch,
      start,
      length,
      (key, leftValue, rightValue) => {
        calls.incrementAndGet()
        if (key == poison || leftValue == poison || rightValue == poison) then
          throw new IllegalStateException("predicate saw a null slot")
        rightValue > 0
      },
    )

    calls.get() shouldBe 3
    filtered.length shouldBe 2
    BatchCodec.decodeIntTriples(filtered) shouldBe Seq((4, (40, 11)), (5, (50, 12)))
    filtered.schema shouldBe batch.schema
    (0 until 3).foreach { ordinal =>
      int32(filtered, ordinal).values.length shouldBe length
      int32(filtered, ordinal).validity.capacity shouldBe length
    }
    int32(filtered, 0).values should not be theSameInstanceAs(keys)
    keys(1) shouldBe poison
    right(6) shouldBe poison
  }

  it should "scan a tail shorter than the batch" in {
    val rows = 10
    val keys = Array.tabulate(rows)(row => row + 1)
    val left = Array.tabulate(rows)(row => (row + 1) * 10)
    val right = Array.tabulate(rows)(row => row + 1)
    val batch = denseTriples(keys, left, right)
    val filtered = PairKernel.filterTriple(batch, 8, 2, (key, _, _) => key % 2 == 0)

    BatchCodec.decodeIntTriples(filtered) shouldBe Seq((10, (100, 10)))
    filtered.length shouldBe 1
    (0 until 3).foreach { ordinal =>
      int32(filtered, ordinal).values.length shouldBe 2
      int32(filtered, ordinal).validity.capacity shouldBe 2
    }
  }

  it should "name a null inside the window and leave nulls outside it" in {
    val rows = 6
    val keys = Array(1, 2, 3, 4, 5, 6)
    val left = Array(10, 0, 30, 40, 50, 60)
    val right = Array(7, 8, 9, 11, 0, 13)
    val leftPresent = Array(true, false, true, true, true, true)
    val rightPresent = Array(true, true, true, true, false, true)
    val all = Array.fill(rows)(true)
    val batch = triples(keys, left, right, all, leftPresent, rightPresent)
    val calls = new AtomicInteger
    val inside = intercept[IllegalArgumentException] {
      PairKernel.filterTriple(
        batch,
        3,
        2,
        (_, _, _) => {
          calls.incrementAndGet()
          true
        },
      )
    }
    calls.get() shouldBe 0
    inside.getMessage shouldBe "requirement failed: null Int32 join value at row 4"

    val before = intercept[IllegalArgumentException] {
      PairKernel.filterTriple(batch, 0, batch.length, (_, _, _) => true)
    }
    before.getMessage shouldBe "requirement failed: null Int32 join value at row 1"

    val kept = PairKernel.filterTriple(batch, 2, 2, (key, _, _) => key == 3)
    BatchCodec.decodeIntTriples(kept) shouldBe Seq((3, (30, 9)))
    val tail = PairKernel.filterTriple(batch, 5, 1, (_, _, _) => true)
    BatchCodec.decodeIntTriples(tail) shouldBe Seq((6, (60, 13)))
  }

  it should "match the whole-batch filter when the range is the whole batch" in {
    val batch = denseTripleRows(
      Array(1, 2, 3, 4),
      Array(10, 20, 30, 40),
      Array(7, 8, -1, 9),
    )
    val keep: PairKernel.IntTriplePredicate = (_, _, right) => right > 0
    val whole = PairKernel.filterTriple(batch, keep)
    val ranged = PairKernel.filterTriple(batch, 0, batch.length, keep)

    BatchCodec.decodeIntTriples(whole) shouldBe Seq((1, (10, 7)), (2, (20, 8)), (4, (40, 9)))
    sameIntBatch(whole, ranged)
    (0 until 3).foreach { ordinal =>
      int32(whole, ordinal).values.length shouldBe batch.length
    }

    val rawKeys = Array(1, 2, 3, poison)
    val rawLeft = Array(10, 20, 30, poison)
    val rawRight = Array(7, -1, 9, poison)
    val valid = Validity.allValid(rawKeys.length)
    val shortened = new ColumnBatch(
      schema3,
      Vector(
        Int32Column.of(rawKeys, valid, length = 3),
        Int32Column.of(rawLeft, valid, length = 3),
        Int32Column.of(rawRight, valid, length = 3),
      ),
      3,
    )
    val wholeShort = PairKernel.filterTriple(shortened, keep)
    val rangedShort = PairKernel.filterTriple(shortened, 0, shortened.length, keep)

    BatchCodec.decodeIntTriples(wholeShort) shouldBe Seq((1, (10, 7)), (3, (30, 9)))
    sameIntBatch(wholeShort, rangedShort)
    rawKeys(3) shouldBe poison
    rawRight(3) shouldBe poison
  }

  it should "return an empty batch for an empty range" in {
    val keys = Array(1, 2, 3)
    val left = Array(10, 0, 30)
    val right = Array(7, 8, 9)
    val leftPresent = Array(true, false, true)
    val batch = triples(
      keys,
      left,
      right,
      Array(true, true, true),
      leftPresent,
      Array(true, true, true),
    )
    val calls = new AtomicInteger
    val filtered = PairKernel.filterTriple(
      batch,
      1,
      0,
      (_, _, _) => {
        calls.incrementAndGet()
        true
      },
    )

    calls.get() shouldBe 0
    filtered.length shouldBe 0
    BatchCodec.decodeIntTriples(filtered) shouldBe Seq.empty[(Int, (Int, Int))]
    filtered.schema shouldBe batch.schema
    (0 until 3).foreach { ordinal =>
      int32(filtered, ordinal).values.length shouldBe 0
    }
  }

  it should "publish an empty batch when the window is entirely rejected" in {
    val batch = denseTripleRows(
      Array(1, 2, 3, 4, 5),
      Array(10, 20, 30, 40, 50),
      Array(1, 2, 3, 4, 5),
    )
    val calls = new AtomicInteger
    val filtered = PairKernel.filterTriple(
      batch,
      1,
      3,
      (key, _, _) => {
        calls.incrementAndGet()
        if (key == 1 || key == 5) then
          throw new IllegalStateException("predicate saw a row outside the range")
        false
      },
    )

    calls.get() shouldBe 3
    filtered.length shouldBe 0
    BatchCodec.decodeIntTriples(filtered) shouldBe Seq.empty[(Int, (Int, Int))]
    (0 until 3).foreach { ordinal =>
      int32(filtered, ordinal).values.length shouldBe 3
      int32(filtered, ordinal).validity.capacity shouldBe 3
      int32(filtered, ordinal).validity.words shouldBe Validity.prefixValid(3, 0).words
    }
  }

  private def pairs(
      keys: Array[Int],
      values: Array[Int],
      keyPresent: Array[Boolean],
      valuePresent: Array[Boolean],
  ): ColumnBatch =
    val rows = keys.length
    new ColumnBatch(
      schema2,
      Vector(
        Int32Column.of(keys, Validity.pack(rows, row => keyPresent(row)), rows),
        Int32Column.of(values, Validity.pack(rows, row => valuePresent(row)), rows),
      ),
      rows,
    )

  private def triples(
      keys: Array[Int],
      left: Array[Int],
      right: Array[Int],
      keyPresent: Array[Boolean],
      leftPresent: Array[Boolean],
      rightPresent: Array[Boolean],
  ): ColumnBatch =
    val rows = keys.length
    new ColumnBatch(
      schema3,
      Vector(
        Int32Column.of(keys, Validity.pack(rows, row => keyPresent(row)), rows),
        Int32Column.of(left, Validity.pack(rows, row => leftPresent(row)), rows),
        Int32Column.of(right, Validity.pack(rows, row => rightPresent(row)), rows),
      ),
      rows,
    )

  private def denseTriples(keys: Array[Int], left: Array[Int], right: Array[Int]): ColumnBatch =
    val present = Array.fill(keys.length)(true)
    triples(keys, left, right, present, present, present)

  private def denseTripleRows(keys: Array[Int], left: Array[Int], right: Array[Int]): ColumnBatch =
    val rows = keys.length
    new ColumnBatch(
      schema3,
      Vector(
        Int32Column.of(keys, Validity.allValid(rows), rows),
        Int32Column.of(left, Validity.allValid(rows), rows),
        Int32Column.of(right, Validity.allValid(rows), rows),
      ),
      rows,
    )

  private def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal).getOrElse(fail(s"missing column $ordinal")) match
      case column: Int32Column => column
      case other => fail(s"expected Int32, got $other")

  private def sameIntBatch(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    left.schema shouldBe right.schema
    left.columns.length shouldBe right.columns.length
    left.columns.indices.foreach { ordinal =>
      val leftColumn = int32(left, ordinal)
      val rightColumn = int32(right, ordinal)
      leftColumn.length shouldBe rightColumn.length
      leftColumn.values.toSeq shouldBe rightColumn.values.toSeq
      leftColumn.validity.capacity shouldBe rightColumn.validity.capacity
      leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq
    }
