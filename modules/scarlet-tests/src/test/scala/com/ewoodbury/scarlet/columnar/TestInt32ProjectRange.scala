package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Int project over a row range. A window can start past row 0, and a tail can be shorter than the
 * batch. Nulls before and inside the window stay null, and the mapper is not called for them. The
 * output column is sized to the range. Other columns are compacted by absolute row index. A
 * full-range scan matches the whole-batch call, including a column whose capacity exceeds its
 * live length.
 */
class TestInt32ProjectRange extends AnyFlatSpec with Matchers:

  "an int project range" should "map a window past row 0 and leave nulls uncalled" in {
    val poison = 999
    val stored = Array(poison, 1, poison, 4, poison, 8, poison, -3, 7)
    val present = Array(false, true, false, true, false, true, false, true, true)
    val start = 3
    val length = 3
    val ints = nullableInts(stored, present)
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectInt32(
      ints,
      0,
      start,
      length,
      value => {
        calls.incrementAndGet()
        if (value == poison) then throw new IllegalStateException("mapper saw a null slot")
        value + 1
      },
    )

    calls.get() shouldBe 2
    mapped.length shouldBe length
    BatchCodec.decodeNullableInts(mapped) shouldBe Seq(Some(5), None, Some(9))
    int32(mapped).values.length shouldBe length
    int32(mapped).validity.capacity shouldBe length
    int32(mapped).values(1) shouldBe 0
    int32(mapped).isValid(1) shouldBe false
    int32(mapped).values should not be theSameInstanceAs(stored)
    int32(mapped).validity should not be theSameInstanceAs(int32(ints).validity)
    stored shouldBe Array(poison, 1, poison, 4, poison, 8, poison, -3, 7)

    val nullCalls = new AtomicInteger
    val nullOnly = ColumnKernel.projectInt32(
      ints,
      0,
      2,
      1,
      value => {
        nullCalls.incrementAndGet()
        value
      },
    )
    nullCalls.get() shouldBe 0
    nullOnly.length shouldBe 1
    BatchCodec.decodeNullableInts(nullOnly) shouldBe Seq(None)
    int32(nullOnly).values(0) shouldBe 0
  }

  it should "scan a tail shorter than the batch" in {
    val ints = BatchCodec.ints(1 to 10)
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectInt32(
      ints,
      0,
      8,
      2,
      value => {
        calls.incrementAndGet()
        value + 1
      },
    )

    calls.get() shouldBe 2
    mapped.length shouldBe 2
    BatchCodec.decodeInts(mapped) shouldBe Seq(10, 11)
    int32(mapped).values.length shouldBe 2
    int32(mapped).validity.capacity shouldBe 2
    int32(mapped).values should not be theSameInstanceAs(int32(ints).values)
  }

  it should "return an empty batch for an empty range and call nothing" in {
    val ints = BatchCodec.ints(Seq(1, 2, 3))
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectInt32(
      ints,
      0,
      1,
      0,
      _ => {
        calls.incrementAndGet()
        0
      },
    )

    calls.get() shouldBe 0
    mapped.length shouldBe 0
    BatchCodec.decodeInts(mapped) shouldBe Seq.empty[Int]
    mapped.schema shouldBe ints.schema
    int32(mapped).values should not be theSameInstanceAs(int32(ints).values)
  }

  it should "match the whole-batch project when the range is the whole batch" in {
    val rows = Seq(Some(1), None, Some(-2), Some(4))
    val batch = BatchCodec.nullableInts(rows)
    val calls = new AtomicInteger
    val mapper: ColumnKernel.Int32Map = value =>
      calls.incrementAndGet()
      value + 1
    val whole = ColumnKernel.projectInt32(batch, 0, mapper)
    val ranged = ColumnKernel.projectInt32(batch, 0, 0, batch.length, mapper)

    calls.get() shouldBe 6
    BatchCodec.decodeNullableInts(whole) shouldBe Seq(Some(2), None, Some(-1), Some(5))
    sameIntBatch(whole, ranged)
    int32(whole).validity should be theSameInstanceAs int32(batch).validity
    int32(ranged).validity should be theSameInstanceAs int32(batch).validity
    int32(whole).values should not be theSameInstanceAs(int32(batch).values)

    val raw = Array(1, -2, 8, 99)
    val validity = Validity.allValid(raw.length)
    val shortened = single(Int32Column.of(raw, validity, length = 3))
    val wholeShort = ColumnKernel.projectInt32(
      shortened,
      0,
      value => if (value == 99) then throw new IllegalStateException("read past length") else value + 1,
    )
    val rangedShort = ColumnKernel.projectInt32(
      shortened,
      0,
      0,
      shortened.length,
      value => if (value == 99) then throw new IllegalStateException("read past length") else value + 1,
    )

    BatchCodec.decodeInts(wholeShort) shouldBe Seq(2, -1, 9)
    sameIntBatch(wholeShort, rangedShort)
    int32(wholeShort).values.length shouldBe raw.length
    int32(wholeShort).validity should be theSameInstanceAs validity
    raw shouldBe Array(1, -2, 8, 99)
  }

  it should "copy the other columns of the scanned range" in {
    val width = 5
    val keys = Array(99, 10, 20, 30, 40)
    val keyPresent = Array(false, true, true, true, true)
    val sides = Array(100, 200, 300, 400, 500)
    val sidePresent = Array(true, true, false, true, true)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.pack(width, row => keyPresent(row)), width),
        Int32Column.of(sides, Validity.pack(width, row => sidePresent(row)), width),
      ),
      width,
    )
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectInt32(
      batch,
      0,
      1,
      3,
      value => {
        calls.incrementAndGet()
        if (value == 99) then throw new IllegalStateException("mapper saw a null slot")
        value + 1
      },
    )

    calls.get() shouldBe 3
    mapped.length shouldBe 3
    at(mapped, 0) match
      case column: Int32Column =>
        column.values.length shouldBe 3
        column.values(0) shouldBe 11
        column.values(1) shouldBe 21
        column.values(2) shouldBe 31
        column.values should not be theSameInstanceAs(keys)
      case other => fail(s"expected Int32, got $other")
    at(mapped, 1) match
      case column: Int32Column =>
        column.length shouldBe 3
        column.values.length shouldBe width
        column.isValid(0) shouldBe true
        column.values(0) shouldBe 200
        column.isValid(1) shouldBe false
        column.values(1) shouldBe 0
        column.isValid(2) shouldBe true
        column.values(2) shouldBe 400
      case other => fail(s"expected Int32, got $other")
  }

  private def single(column: Column): ColumnBatch =
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  private def nullableInts(values: Array[Int], present: Array[Boolean]): ColumnBatch =
    val column =
      Int32Column.of(values, Validity.pack(values.length, row => present(row)), values.length)
    single(column)

  private def int32(batch: ColumnBatch): Int32Column =
    at(batch, 0) match
      case column: Int32Column => column
      case other => fail(s"expected Int32, got $other")

  private def at(batch: ColumnBatch, ordinal: Int): Column =
    batch.columns.lift(ordinal).getOrElse(fail(s"missing column $ordinal"))

  private def sameIntBatch(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    left.schema shouldBe right.schema
    int32(left).length shouldBe int32(right).length
    int32(left).values.toSeq shouldBe int32(right).values.toSeq
    int32(left).validity.capacity shouldBe int32(right).validity.capacity
    int32(left).validity.words.toSeq shouldBe int32(right).validity.words.toSeq
