package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestColumnKernelProject extends AnyFlatSpec with Matchers with ColumnKernelFixture:

  "project" should "map present values and keep nulls as zero" in {
    val ints = Seq(Some(2), None, Some(0))
    val longs = Seq(Some(Long.MinValue), None)
    val floats = Seq(None, Some(1.5))
    val bools = Seq(Some(true), None, Some(false))

    val inputInts = BatchCodec.nullableInts(ints)
    val projectedInts = ColumnKernel.projectInt32(inputInts, 0, _ * 10)
    BatchCodec.decodeNullableInts(projectedInts) shouldBe Seq(Some(20), None, Some(0))
    at(projectedInts, 0) match
      case column: Int32Column =>
        column.values(1) shouldBe 0
        column.isValid(1) shouldBe false
        column.validity should be theSameInstanceAs at(inputInts, 0).validity
      case other => fail(s"expected Int32, got $other")

    BatchCodec.decodeNullableLongs(
      ColumnKernel.projectInt64(BatchCodec.nullableLongs(longs), 0, value => value),
    ) shouldBe longs
    BatchCodec.decodeNullableFloat64s(
      ColumnKernel.projectFloat64(BatchCodec.nullableFloat64s(floats), 0, _ * 2.0),
    ) shouldBe Seq(None, Some(3.0))
    val projectedBools = ColumnKernel.projectBool(BatchCodec.nullableBools(bools), 0, value => !value)
    BatchCodec.decodeNullableBools(projectedBools) shouldBe Seq(Some(false), None, Some(true))
    at(projectedBools, 0) match
      case column: BoolColumn =>
        column.values(0) shouldBe 0.toByte
        column.values(1) shouldBe 0.toByte
        column.values(2) shouldBe 1.toByte
      case other => fail(s"expected Bool, got $other")
  }

  it should "map null strings and build a new dictionary" in {
    val rows = Seq(Some("a"), None, Some("bb"), Some("a"))
    val input = BatchCodec.nullableStrings(rows)
    val projected = ColumnKernel.projectUtf8(
      input,
      0,
      value =>
        Option(value) match
          case None => "n"
          case Some("bb") => BatchCodec.stringElement(None)
          case Some(text) => text.toUpperCase(java.util.Locale.ENGLISH),
    )
    val column = utf8(projected)

    BatchCodec.decodeNullableStrings(projected) shouldBe Seq(Some("A"), Some("n"), None, Some("A"))
    column.dictionary.toSeq shouldBe Seq("A", "n")
    column.dictionary shouldNot be theSameInstanceAs utf8(input).dictionary
    column.isValid(2) shouldBe false
    column.codes(2) shouldBe 0
    column.codes(3) shouldBe column.codes(0)
  }

  it should "not call the function on a null, and should repair a nonzero null slot" in {
    val raw = Array(42, 3)
    val column = Int32Column.of(raw, Validity.pack(2, row => row == 1), 2)
    val projected = ColumnKernel.projectInt32(
      single(column),
      0,
      value =>
        if (value == 42) then throw new IllegalStateException("mapper saw a null slot")
        else value + 1,
    )

    BatchCodec.decodeNullableInts(projected) shouldBe Seq(None, Some(4))
    at(projected, 0) match
      case out: Int32Column => out.values(0) shouldBe 0
      case other => fail(s"expected Int32, got $other")
  }

  it should "share untouched columns and ignore slots past the live length" in {
    val pairs = BatchCodec.intPairs(Seq((1, 10), (2, 20)))
    val projected = ColumnKernel.projectInt32(pairs, 0, _ + 1)
    at(projected, 1) should be theSameInstanceAs at(pairs, 1)
    BatchCodec.decodeIntPairs(projected) shouldBe Seq((2, 10), (3, 20))

    val raw = Array(1, 2, 99)
    val column = Int32Column.of(raw, Validity.allValid(raw.length), length = 2)
    val shortened = ColumnKernel.projectInt32(
      single(column),
      0,
      value => if (value == 99) then throw new IllegalStateException("read past length") else value + 1,
    )
    BatchCodec.decodeInts(shortened) shouldBe Seq(2, 3)
    shortened.length shouldBe 2
  }

  "project range" should "map a window past row 0 and leave nulls uncalled" in {
    val poison = 999L
    val stored = Array(poison, 1L, poison, 4L, poison, 8L, poison, -3L, 7L)
    val present = Array(false, true, false, true, false, true, false, true, true)
    val start = 3
    val length = 3
    val longs = nullableLongs(stored, present)
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectInt64(
      longs,
      0,
      start,
      length,
      value => {
        calls.incrementAndGet()
        if (value == poison) then throw new IllegalStateException("mapper saw a null slot")
        value + 1L
      },
    )

    calls.get() shouldBe 2
    mapped.length shouldBe length
    BatchCodec.decodeNullableLongs(mapped) shouldBe Seq(Some(5L), None, Some(9L))
    long64(mapped).values.length shouldBe length
    long64(mapped).validity.capacity shouldBe length
    long64(mapped).values(1) shouldBe 0L
    long64(mapped).isValid(1) shouldBe false
    long64(mapped).values should not be theSameInstanceAs(stored)
    long64(mapped).validity should not be theSameInstanceAs(long64(longs).validity)
    stored shouldBe Array(poison, 1L, poison, 4L, poison, 8L, poison, -3L, 7L)

    val nullCalls = new AtomicInteger
    val nullOnly = ColumnKernel.projectInt64(
      longs,
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
    BatchCodec.decodeNullableLongs(nullOnly) shouldBe Seq(None)
    long64(nullOnly).values(0) shouldBe 0L
  }

  it should "scan a tail shorter than the batch" in {
    val longs = BatchCodec.longs((1 to 10).map(_.toLong))
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectInt64(
      longs,
      0,
      8,
      2,
      value => {
        calls.incrementAndGet()
        value + 1L
      },
    )

    calls.get() shouldBe 2
    mapped.length shouldBe 2
    BatchCodec.decodeLongs(mapped) shouldBe Seq(10L, 11L)
    long64(mapped).values.length shouldBe 2
    long64(mapped).validity.capacity shouldBe 2
    long64(mapped).values should not be theSameInstanceAs(long64(longs).values)
  }

  it should "return an empty batch for an empty range and call nothing" in {
    val longs = BatchCodec.longs(Seq(1L, 2L, 3L))
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectInt64(
      longs,
      0,
      1,
      0,
      _ => {
        calls.incrementAndGet()
        0L
      },
    )

    calls.get() shouldBe 0
    mapped.length shouldBe 0
    BatchCodec.decodeLongs(mapped) shouldBe Seq.empty[Long]
    mapped.schema shouldBe longs.schema
    long64(mapped).values should not be theSameInstanceAs(long64(longs).values)
  }

  it should "match the whole-batch project when the range is the whole batch" in {
    val rows = Seq(Some(1L), None, Some(-2L), Some(4L))
    val batch = BatchCodec.nullableLongs(rows)
    val calls = new AtomicInteger
    val mapper: ColumnKernel.Int64Map = value =>
      calls.incrementAndGet()
      value + 1L
    val whole = ColumnKernel.projectInt64(batch, 0, mapper)
    val ranged = ColumnKernel.projectInt64(batch, 0, 0, batch.length, mapper)

    calls.get() shouldBe 6
    BatchCodec.decodeNullableLongs(whole) shouldBe Seq(Some(2L), None, Some(-1L), Some(5L))
    sameLongBatch(whole, ranged)
    long64(whole).validity should be theSameInstanceAs long64(batch).validity
    long64(ranged).validity should be theSameInstanceAs long64(batch).validity
    long64(whole).values should not be theSameInstanceAs(long64(batch).values)

    val raw = Array(1L, -2L, 8L, 99L)
    val validity = Validity.allValid(raw.length)
    val shortened = single(Int64Column.of(raw, validity, length = 3))
    val wholeShort = ColumnKernel.projectInt64(
      shortened,
      0,
      value => if (value == 99L) then throw new IllegalStateException("read past length") else value + 1L,
    )
    val rangedShort = ColumnKernel.projectInt64(
      shortened,
      0,
      0,
      shortened.length,
      value => if (value == 99L) then throw new IllegalStateException("read past length") else value + 1L,
    )

    BatchCodec.decodeLongs(wholeShort) shouldBe Seq(2L, -1L, 9L)
    sameLongBatch(wholeShort, rangedShort)
    long64(wholeShort).values.length shouldBe raw.length
    long64(wholeShort).validity should be theSameInstanceAs validity
    raw shouldBe Array(1L, -2L, 8L, 99L)
  }

  it should "filter then map a range with nulls before, inside, and after it" in {
    val poison = 999L
    val stored = Array(poison, 1L, poison, 4L, poison, 8L, poison, -3L, 7L)
    val present = Array(false, true, false, true, false, true, false, true, true)
    val start = 3
    val length = 3
    val longs = nullableLongs(stored, present)
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val keep: ColumnKernel.Int64Predicate = value =>
      predicates.incrementAndGet()
      if (value == poison) then throw new IllegalStateException("predicate saw a null slot")
      value > 0L
    val project: ColumnKernel.Int64Map = value =>
      mappers.incrementAndGet()
      if (value == poison) then throw new IllegalStateException("mapper saw a null slot")
      value * 2L
    val piped = Int64Pipeline(
      Morsel(longs, start, length),
      List(Int64Pipeline.Step.Filter(keep), Int64Pipeline.Step.Project(project)),
    )
    val stepwise = ColumnKernel.projectInt64(
      ColumnKernel.filterInt64(longs, 0, start, length, keep),
      0,
      project,
    )

    predicates.get() shouldBe 4
    mappers.get() shouldBe 4
    BatchCodec.decodeNullableLongs(piped) shouldBe Seq(Some(8L), Some(16L))
    sameLongBatch(piped, stepwise)
    stored shouldBe Array(poison, 1L, poison, 4L, poison, 8L, poison, -3L, 7L)
  }

  it should "copy the other columns of the scanned range" in {
    val width = 5
    val keys = Array(99L, 10L, 20L, 30L, 40L)
    val keyPresent = Array(false, true, true, true, true)
    val sides = Array(100, 200, 300, 400, 500)
    val sidePresent = Array(true, true, false, true, true)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int32),
      Vector(
        Int64Column.of(keys, Validity.pack(width, row => keyPresent(row)), width),
        Int32Column.of(sides, Validity.pack(width, row => sidePresent(row)), width),
      ),
      width,
    )
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectInt64(
      batch,
      0,
      1,
      3,
      value => {
        calls.incrementAndGet()
        if (value == 99L) then throw new IllegalStateException("mapper saw a null slot")
        value + 1L
      },
    )

    calls.get() shouldBe 3
    mapped.length shouldBe 3
    at(mapped, 0) match
      case column: Int64Column =>
        column.values.length shouldBe 3
        column.values(0) shouldBe 11L
        column.values(1) shouldBe 21L
        column.values(2) shouldBe 31L
        column.values should not be theSameInstanceAs(keys)
      case other => fail(s"expected Int64, got $other")
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
