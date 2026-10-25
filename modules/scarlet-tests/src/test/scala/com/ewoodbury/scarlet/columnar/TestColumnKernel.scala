package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestColumnKernel extends AnyFlatSpec with Matchers:

  private def single(column: Column): ColumnBatch =
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  private def utf8(batch: ColumnBatch): DictUtf8Column = utf8At(batch, 0)

  private def int32(batch: ColumnBatch): Int32Column =
    at(batch, 0) match
      case column: Int32Column => column
      case other => fail(s"expected Int32, got $other")

  private def long64(batch: ColumnBatch): Int64Column =
    at(batch, 0) match
      case column: Int64Column => column
      case other => fail(s"expected Int64, got $other")

  private def nullableInts(values: Array[Int], present: Array[Boolean]): ColumnBatch =
    val column =
      Int32Column.of(values, Validity.pack(values.length, row => present(row)), values.length)
    single(column)

  private def nullableLongs(values: Array[Long], present: Array[Boolean]): ColumnBatch =
    val column =
      Int64Column.of(values, Validity.pack(values.length, row => present(row)), values.length)
    single(column)

  private def sameIntBatch(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    left.schema shouldBe right.schema
    left.columns.length shouldBe right.columns.length
    left.columns.indices.foreach { ordinal =>
      (at(left, ordinal), at(right, ordinal)) match
        case (leftColumn: Int32Column, rightColumn: Int32Column) =>
          leftColumn.length shouldBe rightColumn.length
          leftColumn.values.toSeq shouldBe rightColumn.values.toSeq
          leftColumn.validity.capacity shouldBe rightColumn.validity.capacity
          leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq
        case _ => fail(s"column $ordinal is not a pair of Int32 columns")
    }

  private def sameLongBatch(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    left.schema shouldBe right.schema
    long64(left).length shouldBe long64(right).length
    long64(left).values.toSeq shouldBe long64(right).values.toSeq
    long64(left).validity.capacity shouldBe long64(right).validity.capacity
    long64(left).validity.words.toSeq shouldBe long64(right).validity.words.toSeq

  private def float64(batch: ColumnBatch): Float64Column =
    at(batch, 0) match
      case column: Float64Column => column
      case other => fail(s"expected Float64, got $other")

  private def utf8At(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    at(batch, ordinal) match
      case column: DictUtf8Column => column
      case other => fail(s"expected Utf8Dict, got $other")

  private def at(batch: ColumnBatch, ordinal: Int): Column =
    batch.columns.lift(ordinal).getOrElse(fail(s"missing column $ordinal"))

  "filter" should "match a Seq predicate on ints, longs, floats, and bools" in {
    val ints = Seq(1, -2, 0, 4)
    val longs = Seq(1L, -2L, 0L)
    val floats = Seq(1.5, -0.25, 0.0)
    val bools = Seq(true, false, true)

    BatchCodec.decodeInts(ColumnKernel.filterInt32(BatchCodec.ints(ints), 0, _ > 0)) shouldBe Seq(
      1,
      4,
    )
    BatchCodec.decodeLongs(ColumnKernel.filterInt64(BatchCodec.longs(longs), 0, _ < 0L)) shouldBe
      Seq(-2L)
    BatchCodec.decodeFloat64s(
      ColumnKernel.filterFloat64(BatchCodec.float64s(floats), 0, _ < 0.0),
    ) shouldBe Seq(-0.25)
    BatchCodec.decodeBools(ColumnKernel.filterBool(BatchCodec.bools(bools), 0, value => value))
      .shouldBe(Seq(true, true))

    val dirty = BoolColumn.of(Array[Byte](2, 0), Validity.allValid(2), 2)
    val normalized = ColumnKernel.filterBool(single(dirty), 0, value => value)
    at(normalized, 0) match
      case column: BoolColumn =>
        column.length shouldBe 1
        column.values(0) shouldBe 1.toByte
      case other => fail(s"expected Bool, got $other")
  }

  it should "drop nulls without calling the predicate, and keep a present zero" in {
    val ints = Seq(Some(0), None, Some(1))
    val longs = Seq(Some(1L), None, Some(Long.MinValue))
    val floats = Seq(Some(Double.NaN), Some(1.0), None)
    val bools = Seq(Some(true), Some(false), None)

    val filteredInts = ColumnKernel.filterInt32(BatchCodec.nullableInts(ints), 0, _ >= 0)
    BatchCodec.decodeNullableInts(filteredInts) shouldBe Seq(Some(0), Some(1))

    BatchCodec.decodeNullableLongs(
      ColumnKernel.filterInt64(
        BatchCodec.nullableLongs(longs),
        0,
        value => {
          if (value == 0L) then throw new IllegalStateException("predicate saw a null slot")
          value < 0L
        },
      ),
    ) shouldBe Seq(Some(Long.MinValue))

    BatchCodec.decodeNullableFloat64s(
      ColumnKernel.filterFloat64(
        BatchCodec.nullableFloat64s(floats),
        0,
        value => value > 0.0,
      ),
    ) shouldBe Seq(Some(1.0))

    BatchCodec.decodeNullableBools(
      ColumnKernel.filterBool(
        BatchCodec.nullableBools(bools),
        0,
        value => value,
      ),
    ) shouldBe Seq(Some(true))
  }

  it should "not call the predicate on a null slot that stores a passing payload" in {
    val values = Array(5)
    val column = Int32Column.of(values, Validity.pack(1, _ => false), 1)
    val batch = single(column)

    val filtered = ColumnKernel.filterInt32(
      batch,
      0,
      value => throw new IllegalStateException(s"predicate saw $value"),
    )

    filtered.length shouldBe 0
    values shouldBe Array(5)
    column.length shouldBe 1
  }

  it should "compact every column in order and store zero in a surviving null" in {
    val left = Array(1, 2, 3)
    val right = Array(9, 42, 8)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(left, Validity.allValid(left.length), left.length),
        Int32Column.of(right, Validity.pack(right.length, row => row != 1), right.length),
      ),
      left.length,
    )

    val filtered = ColumnKernel.filterInt32(batch, 0, _ >= 2)

    BatchCodec.decodeNullableIntPairs(filtered) shouldBe Seq((Some(2), None), (Some(3), Some(8)))
    at(filtered, 1) match
      case column: Int32Column =>
        column.values(0) shouldBe 0
        column.isValid(0) shouldBe false
        column.validity.capacity shouldBe column.values.length
      case other => fail(s"expected Int32, got $other")
  }

  it should "compact longs, floats, and bools beside the predicate column" in {
    val width = 3
    val keys = Int32Column.of(Array(1, 5, -3), Validity.allValid(width), width)
    val longs = Int64Column.of(Array(99L, 8L, 7L), Validity.pack(width, row => row != 0), width)
    val floats = Float64Column.of(Array(1.5, -2.0, 9.0), Validity.allValid(width), width)
    val bools = BoolColumn.of(Array[Byte](0, 2, 1), Validity.allValid(width), width)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int64, LogicalType.Float64, LogicalType.Bool),
      Vector(keys, longs, floats, bools),
      width,
    )

    val filtered = ColumnKernel.filterInt32(batch, 0, _ > 0)

    filtered.length shouldBe 2
    at(filtered, 1) match
      case column: Int64Column =>
        column.isValid(0) shouldBe false
        column.values(0) shouldBe 0L
        column.isValid(1) shouldBe true
        column.values(1) shouldBe 8L
      case other => fail(s"expected Int64, got $other")
    at(filtered, 2) match
      case column: Float64Column =>
        column.values(0) shouldBe 1.5
        column.values(1) shouldBe -2.0
      case other => fail(s"expected Float64, got $other")
    at(filtered, 3) match
      case column: BoolColumn =>
        column.values(0) shouldBe 0.toByte
        column.isValid(0) shouldBe true
        column.values(1) shouldBe 1.toByte
        column.isValid(1) shouldBe true
      case other => fail(s"expected Bool, got $other")
  }

  it should "keep nulls that sit past a 64-row boundary" in {
    val rows = (0 until 70).map { row =>
      val left = Some(row)
      val right = if (row == 64 || row == 66) None else Some(1)
      (left, right)
    }

    val filtered = ColumnKernel.filterInt32(
      BatchCodec.nullableIntPairs(rows),
      0,
      value => value % 2 == 0,
    )

    val expected = rows.collect { case (Some(value), right) if value % 2 == 0 => (Some(value), right) }
    BatchCodec.decodeNullableIntPairs(filtered) shouldBe expected
    filtered.length shouldBe expected.length
  }

  it should "ignore slots past the live row count" in {
    val raw = Array(1, -2, 8, 99)
    val column = Int32Column.of(raw, Validity.allValid(raw.length), length = 3)
    val filtered = ColumnKernel.filterInt32(single(column), 0, _ > 0)

    BatchCodec.decodeInts(filtered) shouldBe Seq(1, 8)
    filtered.length shouldBe 2
    at(filtered, 0) match
      case out: Int32Column =>
        out.values.length should be >= out.length
        out.validity.capacity shouldBe out.values.length
      case other => fail(s"expected Int32, got $other")
  }

  it should "return an empty batch when the input is empty or nothing survives" in {
    val empty = ColumnKernel.filterInt32(BatchCodec.ints(Seq.empty), 0, _ > 0)
    val dropped = ColumnKernel.filterInt32(BatchCodec.ints(Seq(-1, -2)), 0, _ > 0)
    val pairs = ColumnKernel.filterInt32(BatchCodec.intPairs(Seq((1, 2))), 1, _ > 10)

    BatchCodec.decodeInts(empty) shouldBe Seq.empty[Int]
    BatchCodec.decodeInts(dropped) shouldBe Seq.empty[Int]
    empty.schema shouldBe Vector(LogicalType.Int32)
    pairs.length shouldBe 0
    pairs.schema shouldBe Vector(LogicalType.Int32, LogicalType.Int32)
    BatchCodec.decodeIntPairs(pairs) shouldBe Seq.empty[(Int, Int)]
  }

  it should "keep a null string when the predicate says so, and share the dictionary" in {
    val rows = Seq(Some("a"), None, Some("bb"), Some("a"))
    val input = BatchCodec.nullableStrings(rows)
    val kept = ColumnKernel.filterUtf8(
      input,
      0,
      value =>
        Option(value) match
          case None | Some("a") => true
          case Some(_) => false,
    )
    val dropped = ColumnKernel.filterUtf8(
      input,
      0,
      value =>
        Option(value) match
          case Some(text) => text.startsWith("b")
          case None => false,
    )

    BatchCodec.decodeNullableStrings(kept) shouldBe Seq(Some("a"), None, Some("a"))
    BatchCodec.decodeNullableStrings(dropped) shouldBe Seq(Some("bb"))
    utf8(kept).dictionary should be theSameInstanceAs utf8(input).dictionary
    utf8(kept).isValid(1) shouldBe false
    utf8(kept).codes(1) shouldBe 0
  }

  it should "count literal codes without publishing a column" in {
    val rows = Seq(Some("a"), None, Some("b"), Some("a"), Some("c"))
    val input = BatchCodec.nullableStrings(rows)
    val absent = DictUtf8Column.of(
      Array("a", "b"),
      Array(-1, 0, -1, 1),
      Validity.allValid(4),
      4,
    )
    val absentBatch = single(absent)
    val empty = BatchCodec.strings(Seq.empty)

    ColumnMetrics.withSink(new CountingSink) {
      ColumnKernel.countByValues(input, 0, Seq("a", "c")) shouldBe 3L
      ColumnKernel.countByValues(input, 0, Seq("missing")) shouldBe 0L
      ColumnKernel.countByValues(input, 0, Seq("a", "b", "c")) shouldBe 4L
      ColumnKernel.countByValues(input, 0, Seq.empty[String]) shouldBe 0L
      ColumnKernel.countByValues(absentBatch, 0, Seq("a")) shouldBe 1L
      ColumnKernel.countByValues(absentBatch, 0, Seq("b")) shouldBe 1L
      ColumnKernel.countByValues(empty, 0, Seq("a")) shouldBe 0L
      ColumnMetrics.batchCount shouldBe 0L
    }
    input.length shouldBe 5

    val forward = Seq(Seq("a", "b"), Seq("b", "c"))
    val reverse = Seq(Seq("c", "b"), Seq("b", "a"))
    ColumnKernel.countByGroups(input, 0, forward) shouldBe 1L
    ColumnKernel.countByGroups(input, 0, reverse) shouldBe 1L
    ColumnKernel.countByGroups(input, 0, Seq(Seq("a"), Seq("b"))) shouldBe 0L
    ColumnKernel.countByGroups(absentBatch, 0, Seq(Seq("a", "b"))) shouldBe
      ColumnKernel.filterUtf8ByValues(absentBatch, 0, Seq("a", "b")).length.toLong
    ColumnKernel.countByGroups(input, 0, Seq.empty) shouldBe 0L
  }

  it should "compact a string column beside the predicate and keep its null" in {
    val keys = Int32Column.of(Array(1, 2, 3), Validity.allValid(3), 3)
    val texts = DictUtf8Column.fromNullable(Seq(Some("a"), None, Some("b")))
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Utf8Dict),
      Vector(keys, texts),
      3,
    )

    val filtered = ColumnKernel.filterInt32(batch, 0, _ >= 2)

    filtered.length shouldBe 2
    val column = utf8At(filtered, 1)
    column.dictionary should be theSameInstanceAs texts.dictionary
    column.isValid(0) shouldBe false
    column.codes(0) shouldBe 0
    column.isValid(1) shouldBe true
    column.dictionary(column.codes(1)) shouldBe "b"
  }

  "filter range" should "skip nulls before, inside, and after a range past row 0" in {
    val poison = 999
    val stored = Array(poison, 1, poison, 4, poison, 8, poison, -3, 7)
    val present = Array(false, true, false, true, false, true, false, true, true)
    val start = 3
    val length = 3
    val ints = nullableInts(stored, present)

    val intCalls = new AtomicInteger
    val filteredInts = ColumnKernel.filterInt32(
      ints,
      0,
      start,
      length,
      value => {
        intCalls.incrementAndGet()
        if (value == poison) then throw new IllegalStateException("predicate saw a null slot")
        value > 0
      },
    )

    intCalls.get() shouldBe 2
    filteredInts.length shouldBe 2
    BatchCodec.decodeInts(filteredInts) shouldBe Seq(4, 8)
    int32(filteredInts).values.length shouldBe length
    int32(filteredInts).validity.capacity shouldBe length
    int32(filteredInts).values should not be theSameInstanceAs(stored)
    stored shouldBe Array(poison, 1, poison, 4, poison, 8, poison, -3, 7)

    val nullOnly = ColumnKernel.filterInt32(
      ints,
      0,
      2,
      1,
      value => throw new IllegalStateException(s"predicate saw $value"),
    )
    nullOnly.length shouldBe 0
    int32(nullOnly).values.length shouldBe 1

    val longStored = stored.map(_.toLong)
    val longs = nullableLongs(longStored, present)
    val longCalls = new AtomicInteger
    val filteredLongs = ColumnKernel.filterInt64(
      longs,
      0,
      start,
      length,
      value => {
        longCalls.incrementAndGet()
        if (value == poison.toLong) then
          throw new IllegalStateException("predicate saw a null slot")
        value > 0L
      },
    )

    longCalls.get() shouldBe 2
    filteredLongs.length shouldBe 2
    BatchCodec.decodeLongs(filteredLongs) shouldBe Seq(4L, 8L)
    long64(filteredLongs).values.length shouldBe length
    long64(filteredLongs).values should not be theSameInstanceAs(longStored)
  }

  it should "scan a tail shorter than the batch" in {
    val ints = BatchCodec.ints(1 to 10)
    val filteredInts = ColumnKernel.filterInt32(ints, 0, 8, 2, value => value % 2 == 0)

    BatchCodec.decodeInts(filteredInts) shouldBe Seq(10)
    filteredInts.length shouldBe 1
    int32(filteredInts).values.length shouldBe 2
    int32(filteredInts).validity.capacity shouldBe 2
    int32(filteredInts).values should not be theSameInstanceAs(int32(ints).values)

    val longs = BatchCodec.longs((1 to 10).map(_.toLong))
    val filteredLongs = ColumnKernel.filterInt64(longs, 0, 8, 2, value => value % 2L == 0L)

    BatchCodec.decodeLongs(filteredLongs) shouldBe Seq(10L)
    filteredLongs.length shouldBe 1
    long64(filteredLongs).values.length shouldBe 2
    long64(filteredLongs).validity.capacity shouldBe 2
  }

  it should "compact other columns by absolute index" in {
    val width = 5
    val keys = Array(7, 1, 2, 3, 4)
    val keyPresent = Array(true, true, true, false, true)
    val values = Array(99, 10, 20, 30, 40)
    val valuePresent = Array(true, true, true, true, true)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.pack(width, row => keyPresent(row)), width),
        Int32Column.of(values, Validity.pack(width, row => valuePresent(row)), width),
      ),
      width,
    )
    val calls = new AtomicInteger
    val filtered = ColumnKernel.filterInt32(
      batch,
      1,
      1,
      3,
      value => {
        calls.incrementAndGet()
        if (value == 99) then
          throw new IllegalStateException("predicate saw a row outside the range")
        value >= 20
      },
    )

    calls.get() shouldBe 3
    filtered.length shouldBe 2
    BatchCodec.decodeNullableIntPairs(filtered) shouldBe Seq(
      (Some(2), Some(20)),
      (None, Some(30)),
    )
    at(filtered, 0) match
      case column: Int32Column =>
        column.values.length shouldBe width
        column.validity.capacity shouldBe width
        column.values(0) shouldBe 2
        column.isValid(1) shouldBe false
        column.values(1) shouldBe 0
      case other => fail(s"expected Int32, got $other")
    at(filtered, 1) match
      case column: Int32Column =>
        column.values.length shouldBe 3
        column.values should not be theSameInstanceAs(values)
      case other => fail(s"expected Int32, got $other")

    val longKeys = Array(99L, 10L, 20L, 30L, 40L)
    val longPresent = Array(false, true, true, true, true)
    val sides = Array(100, 200, 300, 400, 500)
    val sidePresent = Array(true, true, false, true, true)
    val longBatch = new ColumnBatch(
      Vector(LogicalType.Int64, LogicalType.Int32),
      Vector(
        Int64Column.of(longKeys, Validity.pack(width, row => longPresent(row)), width),
        Int32Column.of(sides, Validity.pack(width, row => sidePresent(row)), width),
      ),
      width,
    )
    val longCalls = new AtomicInteger
    val filteredLongs = ColumnKernel.filterInt64(
      longBatch,
      0,
      1,
      3,
      value => {
        longCalls.incrementAndGet()
        if (value == 99L) then throw new IllegalStateException("predicate saw a null slot")
        value >= 20L
      },
    )

    longCalls.get() shouldBe 3
    filteredLongs.length shouldBe 2
    at(filteredLongs, 0) match
      case column: Int64Column =>
        column.values.length shouldBe 3
        column.values(0) shouldBe 20L
        column.values(1) shouldBe 30L
        column.values should not be theSameInstanceAs(longKeys)
      case other => fail(s"expected Int64, got $other")
    at(filteredLongs, 1) match
      case column: Int32Column =>
        column.values.length shouldBe width
        column.validity.capacity shouldBe width
        column.isValid(0) shouldBe false
        column.values(0) shouldBe 0
        column.isValid(1) shouldBe true
        column.values(1) shouldBe 400
      case other => fail(s"expected Int32, got $other")
  }

  it should "match the whole-batch filter when the range is the whole batch" in {
    val rows = Seq(Some(1), None, Some(-2), Some(4), Some(0))
    val ints = BatchCodec.nullableInts(rows)
    val whole = ColumnKernel.filterInt32(ints, 0, value => value >= 0)
    val ranged = ColumnKernel.filterInt32(ints, 0, 0, ints.length, value => value >= 0)

    BatchCodec.decodeNullableInts(whole) shouldBe Seq(Some(1), Some(4), Some(0))
    sameIntBatch(whole, ranged)
    int32(ranged).values should not be theSameInstanceAs(int32(ints).values)
    int32(whole).values.length shouldBe ints.length

    val raw = Array(1, -2, 8, 99)
    val shortened = single(Int32Column.of(raw, Validity.allValid(raw.length), length = 3))
    val wholeShort = ColumnKernel.filterInt32(shortened, 0, value => value > 0)
    val rangedShort =
      ColumnKernel.filterInt32(shortened, 0, 0, shortened.length, value => value > 0)

    BatchCodec.decodeInts(wholeShort) shouldBe Seq(1, 8)
    sameIntBatch(wholeShort, rangedShort)
    int32(wholeShort).values.length shouldBe shortened.length
    raw shouldBe Array(1, -2, 8, 99)

    val pairs = BatchCodec.nullableIntPairs(
      Seq((Some(0), Some(1)), (None, Some(9)), (Some(2), None), (Some(3), Some(4))),
    )
    val wholePairs = ColumnKernel.filterInt32(pairs, 0, value => value % 2 == 0)
    val rangedPairs =
      ColumnKernel.filterInt32(pairs, 0, 0, pairs.length, value => value % 2 == 0)

    BatchCodec.decodeNullableIntPairs(wholePairs) shouldBe Seq(
      (Some(0), Some(1)),
      (Some(2), None),
    )
    sameIntBatch(wholePairs, rangedPairs)
    at(wholePairs, 1) match
      case column: Int32Column => column.values.length shouldBe pairs.length
      case other => fail(s"expected Int32, got $other")

    val longRows = Seq(Some(1L), None, Some(-2L), Some(4L))
    val longBatch = BatchCodec.nullableLongs(longRows)
    val wholeLongs = ColumnKernel.filterInt64(longBatch, 0, value => value > 0L)
    val rangedLongs =
      ColumnKernel.filterInt64(longBatch, 0, 0, longBatch.length, value => value > 0L)

    BatchCodec.decodeNullableLongs(wholeLongs) shouldBe Seq(Some(1L), Some(4L))
    sameLongBatch(wholeLongs, rangedLongs)
    long64(wholeLongs).values.length shouldBe longBatch.length
  }

  it should "return an empty batch for an empty range" in {
    val batch = BatchCodec.ints(Seq(1, 2, 3, 4))
    val calls = new AtomicInteger
    val filtered = ColumnKernel.filterInt32(
      batch,
      0,
      2,
      0,
      _ => {
        calls.incrementAndGet()
        true
      },
    )

    calls.get() shouldBe 0
    filtered.length shouldBe 0
    BatchCodec.decodeInts(filtered) shouldBe Seq.empty[Int]
    filtered.schema shouldBe batch.schema
    int32(filtered).values should not be theSameInstanceAs(int32(batch).values)

    val pairs = BatchCodec.intPairs(Seq((1, 10), (2, 20), (3, 30)))
    val dropped = ColumnKernel.filterInt32(pairs, 0, 1, 0, _ => true)
    dropped.length shouldBe 0
    dropped.schema shouldBe pairs.schema
    at(dropped, 1) match
      case column: Int32Column => column.values.length shouldBe pairs.length
      case other => fail(s"expected Int32, got $other")

    val longs = BatchCodec.longs(Seq(1L, 2L))
    val emptyLongs = ColumnKernel.filterInt64(longs, 0, 0, 0, _ => true)
    emptyLongs.length shouldBe 0
    BatchCodec.decodeLongs(emptyLongs) shouldBe Seq.empty[Long]
  }

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

  "filter then project" should "match the same functions on a Seq" in {
    val values = Seq(Some(-1), None, Some(3), Some(0), Some(4))
    val batch = ColumnKernel.projectInt32(
      ColumnKernel.filterInt32(BatchCodec.nullableInts(values), 0, _ > 0),
      0,
      _ * 2,
    )

    val expected = values.collect { case Some(value) if value > 0 => Some(value * 2) }
    BatchCodec.decodeNullableInts(batch) shouldBe expected
  }

  "pipelines" should "fuse filters and projects without invoking functions for nulls" in {
    val column = Int32Column.of(Array(2, 42, 3), Validity.pack(3, row => row != 1), 3)
    val batch = single(column)
    val fusedFilter = Int32Pipeline.FP(
      batch,
      _ > 0,
      value =>
        if (value == 42) then throw new IllegalStateException("mapper saw a null slot")
        else value * 10,
    )
    val fusedProjects = Int32Pipeline.PP(
      batch,
      _ + 1,
      value =>
        if (value == 42) then throw new IllegalStateException("mapper saw a null slot")
        else value + 1,
    )

    BatchCodec.decodeNullableInts(fusedFilter) shouldBe Seq(Some(20), Some(30))
    BatchCodec.decodeNullableInts(fusedProjects) shouldBe Seq(Some(4), None, Some(5))
  }

  it should "size compacted int buffers to the live rows" in {
    val input = Int32Column.of(Array(1, 2, 3, 99, 100), Validity.allValid(5), length = 3)
    val first = Int32Pipeline.FP(single(input), _ > 1, value => value)
    val second = Int32Pipeline.FP(first, _ > 2, _ + 1)

    BatchCodec.decodeInts(first) shouldBe Seq(2, 3)
    BatchCodec.decodeInts(second) shouldBe Seq(4)
    int32(first).values.length shouldBe 3
    int32(first).validity.capacity shouldBe 3
    int32(second).values.length shouldBe 2
    int32(second).validity.capacity shouldBe 2
  }

  it should "match stepwise float kernels, and skip null slots" in {
    val input = single(
      Float64Column.of(Array(-1.0, 42.0, 2.0, 0.0), Validity.pack(4, row => row != 1), 4),
    )
    val fusedFilter = Float64Pipeline.FP(
      input,
      _ > 0.0,
      value =>
        if (value == 42.0) then throw new IllegalStateException("mapper saw a null slot")
        else value * 10.0,
    )
    val stepwiseFilter = ColumnKernel.projectFloat64(
      ColumnKernel.filterFloat64(input, 0, _ > 0.0),
      0,
      _ * 10.0,
    )
    val fusedProject = Float64Pipeline.PF(
      input,
      value =>
        if (value == 42.0) then throw new IllegalStateException("mapper saw a null slot")
        else value + 1.0,
      value =>
        if (value == 42.0) then throw new IllegalStateException("predicate saw a null slot")
        else value > 0.0,
    )
    val stepwiseProject = ColumnKernel.filterFloat64(
      ColumnKernel.projectFloat64(
        input,
        0,
        value =>
          if (value == 42.0) then throw new IllegalStateException("mapper saw a null slot")
          else value + 1.0,
      ),
      0,
      _ > 0.0,
    )
    val fusedMaps = Float64Pipeline.PP(
      input,
      value =>
        if (value == 42.0) then throw new IllegalStateException("mapper saw a null slot")
        else value + 1.0,
      _ * 2.0,
    )
    val stepwiseMaps = ColumnKernel.projectFloat64(
      ColumnKernel.projectFloat64(
        input,
        0,
        value =>
          if (value == 42.0) then throw new IllegalStateException("mapper saw a null slot")
          else value + 1.0,
      ),
      0,
      _ * 2.0,
    )

    BatchCodec.decodeNullableFloat64s(fusedFilter) shouldBe
      BatchCodec.decodeNullableFloat64s(stepwiseFilter)
    BatchCodec.decodeNullableFloat64s(fusedProject) shouldBe
      BatchCodec.decodeNullableFloat64s(stepwiseProject)
    BatchCodec.decodeNullableFloat64s(fusedMaps) shouldBe
      BatchCodec.decodeNullableFloat64s(stepwiseMaps)
    float64(fusedMaps).validity should be theSameInstanceAs float64(input).validity
    float64(fusedMaps).values(1) shouldBe 0.0
  }

  it should "keep float bits and size compacted buffers to the live rows" in {
    val negativeZero = -0.0
    val input = Float64Column.of(
      Array(negativeZero, Double.NaN, 99.0),
      Validity.allValid(3),
      length = 2,
    )
    val projected = Float64Pipeline.PP(
      single(input),
      value =>
        if (value == 99.0) then throw new IllegalStateException("read past length")
        else value,
      value => value,
    )
    val filtered = Float64Pipeline.FP(
      single(input),
      value => java.lang.Double.doubleToRawLongBits(value) < 0,
      value => value,
    )

    java.lang.Double.doubleToRawLongBits(float64(projected).values(0)) shouldBe
      java.lang.Double.doubleToRawLongBits(negativeZero)
    java.lang.Double.doubleToRawLongBits(float64(projected).values(1)) shouldBe
      java.lang.Double.doubleToRawLongBits(Double.NaN)
    BatchCodec.decodeFloat64s(filtered) shouldBe Seq(negativeZero)
    float64(filtered).values.length shouldBe 2
    float64(filtered).validity.capacity shouldBe 2
  }

  it should "share a filter-only string dictionary and keep a null the predicate accepts" in {
    val rows = Seq(Some("a"), None, Some("bb"), Some("a"))
    val input = BatchCodec.nullableStrings(rows)
    val fused = Utf8Pipeline.FF(
      input,
      value =>
        Option(value) match
          case None | Some("a") => true
          case Some(_) => false,
      value =>
        Option(value) match
          case None | Some("a") => true
          case Some(_) => false,
    )
    val stepwise = ColumnKernel.filterUtf8(
      ColumnKernel.filterUtf8(
        input,
        0,
        value =>
          Option(value) match
            case None | Some("a") => true
            case Some(_) => false,
      ),
      0,
      value =>
        Option(value) match
          case None | Some("a") => true
          case Some(_) => false,
    )

    BatchCodec.decodeNullableStrings(fused) shouldBe BatchCodec.decodeNullableStrings(stepwise)
    utf8(fused).dictionary should be theSameInstanceAs utf8(input).dictionary
    utf8(fused).isValid(1) shouldBe false
  }

  it should "drop an empty cell and not hand it to a later function" in {
    val rows = Seq(Some("a"), None, Some("bb"))
    val input = BatchCodec.nullableStrings(rows)
    val fused = Utf8Pipeline.FF(
      input,
      value => Option(value).exists(_.length > 1),
      value =>
        if (Option(value).isEmpty) then
          throw new IllegalStateException("second predicate saw an empty cell")
        else true,
    )
    val projected = Utf8Pipeline.FP(
      input,
      value => Option(value).nonEmpty,
      value =>
        if (Option(value).isEmpty) then
          throw new IllegalStateException("mapper saw an empty cell")
        else value.toUpperCase(java.util.Locale.ENGLISH),
    )

    BatchCodec.decodeNullableStrings(fused) shouldBe Seq(Some("bb"))
    BatchCodec.decodeNullableStrings(projected) shouldBe Seq(Some("A"), Some("BB"))
  }

  it should "intern only the strings a projecting pipeline stores" in {
    val rows = Seq(Some("a"), None, Some("bb"), Some("a"))
    val input = BatchCodec.nullableStrings(rows)
    val fusedMaps = Utf8Pipeline.PP(
      input,
      value =>
        Option(value) match
          case None => "n"
          case Some(text) => text.toUpperCase(java.util.Locale.ENGLISH),
      value =>
        Option(value) match
          case Some("BB") => BatchCodec.stringElement(None)
          case Some(text) => text
          case None => BatchCodec.stringElement(None),
    )
    val stepwiseMaps = ColumnKernel.projectUtf8(
      ColumnKernel.projectUtf8(
        input,
        0,
        value =>
          Option(value) match
            case None => "n"
            case Some(text) => text.toUpperCase(java.util.Locale.ENGLISH),
      ),
      0,
      value =>
        Option(value) match
          case Some("BB") => BatchCodec.stringElement(None)
          case Some(text) => text
          case None => BatchCodec.stringElement(None),
    )
    val fusedDrop = Utf8Pipeline.PF(
      input,
      value =>
        Option(value) match
          case None => "n"
          case Some(text) => text,
      value => Option(value).contains("a"),
    )

    BatchCodec.decodeNullableStrings(fusedMaps) shouldBe
      BatchCodec.decodeNullableStrings(stepwiseMaps)
    utf8(fusedMaps).dictionary.toSeq shouldBe utf8(stepwiseMaps).dictionary.toSeq
    BatchCodec.decodeNullableStrings(fusedDrop) shouldBe Seq(Some("a"), Some("a"))
    utf8(fusedDrop).dictionary.toSeq shouldBe Seq("a")
    utf8(fusedMaps).dictionary shouldNot be theSameInstanceAs utf8(input).dictionary
  }

  "kernels" should "reject invalid ordinals, column types, or pipeline widths" in {
    val ints = BatchCodec.ints(Seq(1))
    val pairs = BatchCodec.intPairs(Seq((1, 2)))

    an[IllegalArgumentException] should be thrownBy ColumnKernel.filterInt32(ints, 1, _ > 0)
    an[IllegalArgumentException] should be thrownBy ColumnKernel.filterInt32(ints, -1, _ > 0)
    an[IllegalArgumentException] should be thrownBy ColumnKernel.filterInt64(ints, 0, _ > 0L)
    an[IllegalArgumentException] should be thrownBy ColumnKernel.projectFloat64(pairs, 0, _ * 2.0)
    an[IllegalArgumentException] should be thrownBy ColumnKernel.projectBool(ints, 0, value => value)
    an[IllegalArgumentException] should be thrownBy Int32Pipeline.PP(pairs, _ + 1, _ + 1)
    an[IllegalArgumentException] should be thrownBy
      Float64Pipeline.PP(pairs, _ + 1.0, _ + 1.0)
    an[IllegalArgumentException] should be thrownBy
      Utf8Pipeline.PP(pairs, (value: String) => value, (value: String) => value)
  }

  "triple filter" should "name the null component and keep dense survivors in order" in {
    val rows = 3
    val keys = Int32Column.of(Array(1, 2, 3), Validity.allValid(rows), rows)
    val left = Int32Column.of(Array(10, 20, 30), Validity.allValid(rows), rows)
    val right = Int32Column.of(Array(7, 8, 9), Validity.allValid(rows), rows)
    val schema = Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32)
    val dense = new ColumnBatch(schema, Vector(keys, left, right), rows)
    val kept = PairKernel.filterTriple(dense, (key, _, _) => key != 2)

    BatchCodec.decodeIntTriples(kept) shouldBe Seq((1, (10, 7)), (3, (30, 9)))

    val rightHole = Int32Column.of(Array(7, 0, 9), Validity.pack(rows, row => row != 1), rows)
    val sparse = new ColumnBatch(schema, Vector(keys, left, rightHole), rows)
    val caught = intercept[IllegalArgumentException] {
      PairKernel.filterTriple(sparse, (_, _, _) => true)
    }
    caught.getMessage shouldBe "requirement failed: null Int32 join value at row 1"
  }
