package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Range scans for float, string, and boolean filters, and for float and string projects. A window
 * can start past row 0, and a tail can be shorter than the batch. Nulls before the window are not
 * visited. A float or bool null inside the window is not passed to the predicate or the mapper. A
 * string predicate or project sees every cell in the window, including a null and an empty string.
 * The output column is sized to the range. Other columns are compacted by absolute row index. A
 * full-range scan matches the whole-batch call.
 */
class TestColumnFilterRange extends AnyFlatSpec with Matchers:

  private val poison = 999.0
  private val negativeZero = -0.0
  private val payloadNaN = java.lang.Double.longBitsToDouble(0x7ff0000000000001L)

  "a float filter range" should "skip nulls before, inside, and after a window past row 0" in {
    val stored = Array(poison, 1.0, poison, payloadNaN, negativeZero, 4.0, poison, -3.0, 7.0)
    val present = Array(false, true, false, true, true, true, false, true, true)
    val start = 3
    val length = 3
    val batch = nullableFloats(stored, present)
    val calls = new AtomicInteger
    val filtered = ColumnKernel.filterFloat64(
      batch,
      0,
      start,
      length,
      value => {
        calls.incrementAndGet()
        if (value == poison) then throw new IllegalStateException("predicate saw a null slot")
        value > 0.0
      },
    )

    calls.get() shouldBe 3
    filtered.length shouldBe 1
    BatchCodec.decodeFloat64s(filtered) shouldBe Seq(4.0)
    float64(filtered).values.length shouldBe length
    float64(filtered).validity.capacity shouldBe length
    float64(filtered).values should not be theSameInstanceAs(stored)
    stored(0) shouldBe poison
    java.lang.Double.doubleToRawLongBits(stored(4)) shouldBe
      java.lang.Double.doubleToRawLongBits(negativeZero)

    val kept = ColumnKernel.filterFloat64(batch, 0, start, length, value => keepFloat(value))
    kept.length shouldBe 3
    bits(float64(kept).values(0)) shouldBe bits(payloadNaN)
    bits(float64(kept).values(1)) shouldBe bits(negativeZero)
    float64(kept).values(2) shouldBe 4.0
    float64(kept).isValid(0) shouldBe true
    float64(kept).isValid(1) shouldBe true

    val nullOnly = ColumnKernel.filterFloat64(
      batch,
      0,
      2,
      1,
      value => throw new IllegalStateException(s"predicate saw $value"),
    )
    nullOnly.length shouldBe 0
    float64(nullOnly).values.length shouldBe 1
  }

  it should "scan a tail shorter than the batch" in {
    val batch = BatchCodec.float64s((1 to 10).map(_.toDouble))
    val filtered = ColumnKernel.filterFloat64(batch, 0, 8, 2, value => value % 2.0 == 0.0)

    BatchCodec.decodeFloat64s(filtered) shouldBe Seq(10.0)
    filtered.length shouldBe 1
    float64(filtered).values.length shouldBe 2
    float64(filtered).validity.capacity shouldBe 2
    float64(filtered).values should not be theSameInstanceAs(float64(batch).values)
  }

  it should "compact other columns by absolute index" in {
    val width = 5
    val keys = Array(7, 1, 2, 3, 4)
    val keyPresent = Array(true, true, true, false, true)
    val values = Array(poison, 10.0, negativeZero, payloadNaN, 40.0)
    val valuePresent = Array(true, true, true, true, true)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Float64),
      Vector(
        Int32Column.of(keys, Validity.pack(width, row => keyPresent(row)), width),
        Float64Column.of(values, Validity.pack(width, row => valuePresent(row)), width),
      ),
      width,
    )
    val calls = new AtomicInteger
    val filtered = ColumnKernel.filterFloat64(
      batch,
      1,
      1,
      3,
      value => {
        calls.incrementAndGet()
        if (value == poison) then
          throw new IllegalStateException("predicate saw a row outside the range")
        keepFloat(value) && bits(value) != bits(negativeZero)
      },
    )

    calls.get() shouldBe 3
    filtered.length shouldBe 2
    at(filtered, 0) match
      case column: Int32Column =>
        column.values.length shouldBe width
        column.validity.capacity shouldBe width
        column.values(0) shouldBe 1
        column.isValid(1) shouldBe false
        column.values(1) shouldBe 0
      case other => fail(s"expected Int32, got $other")
    at(filtered, 1) match
      case column: Float64Column =>
        column.values.length shouldBe 3
        column.values(0) shouldBe 10.0
        bits(column.values(1)) shouldBe bits(payloadNaN)
        column.values should not be theSameInstanceAs(values)
      case other => fail(s"expected Float64, got $other")
  }

  it should "match the whole-batch filter when the range is the whole batch" in {
    val rows = Seq(Some(1.0), None, Some(negativeZero), Some(payloadNaN), Some(0.0))
    val batch = BatchCodec.nullableFloat64s(rows)
    val whole = ColumnKernel.filterFloat64(batch, 0, value => keepFloat(value))
    val ranged = ColumnKernel.filterFloat64(batch, 0, 0, batch.length, value => keepFloat(value))

    whole.length shouldBe 3
    sameFloatBatch(whole, ranged)
    float64(ranged).values should not be theSameInstanceAs(float64(batch).values)
    float64(whole).values.length shouldBe batch.length
    bits(float64(whole).values(1)) shouldBe bits(negativeZero)
    bits(float64(whole).values(2)) shouldBe bits(payloadNaN)

    val raw = Array(1.0, -2.0, 8.0, poison)
    val shortened = single(Float64Column.of(raw, Validity.allValid(raw.length), length = 3))
    val wholeShort = ColumnKernel.filterFloat64(shortened, 0, value => value > 0.0)
    val rangedShort =
      ColumnKernel.filterFloat64(shortened, 0, 0, shortened.length, value => value > 0.0)

    BatchCodec.decodeFloat64s(wholeShort) shouldBe Seq(1.0, 8.0)
    sameFloatBatch(wholeShort, rangedShort)
    float64(wholeShort).values.length shouldBe shortened.length
    raw(3) shouldBe poison

    val pairs = new ColumnBatch(
      Vector(LogicalType.Float64, LogicalType.Int32),
      Vector(
        Float64Column.of(
          Array(0.0, negativeZero, payloadNaN, 4.0),
          Validity.pack(4, row => row != 2),
          4,
        ),
        Int32Column.of(Array(1, 2, 3, 4), Validity.allValid(4), 4),
      ),
      4,
    )
    val wholePairs = ColumnKernel.filterFloat64(pairs, 0, value => keepFloat(value))
    val rangedPairs = ColumnKernel.filterFloat64(pairs, 0, 0, pairs.length, value => keepFloat(value))

    sameFloatBatch(wholePairs, rangedPairs)
    at(wholePairs, 1) match
      case column: Int32Column =>
        column.values.toSeq shouldBe Seq(2, 4, 0, 0)
        column.values.length shouldBe pairs.length
      case other => fail(s"expected Int32, got $other")
  }

  it should "return an empty batch for an empty range" in {
    val batch = BatchCodec.float64s(Seq(1.0, 2.0, 3.0, 4.0))
    val calls = new AtomicInteger
    val filtered = ColumnKernel.filterFloat64(
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
    BatchCodec.decodeFloat64s(filtered) shouldBe Seq.empty[Double]
    filtered.schema shouldBe batch.schema
    float64(filtered).values should not be theSameInstanceAs(float64(batch).values)

    val dropped = ColumnKernel.filterFloat64(
      new ColumnBatch(
        Vector(LogicalType.Float64, LogicalType.Int32),
        Vector(
          Float64Column.of(Array(1.0, 2.0), Validity.allValid(2), 2),
          Int32Column.of(Array(10, 20), Validity.allValid(2), 2),
        ),
        2,
      ),
      0,
      1,
      0,
      _ => true,
    )
    dropped.length shouldBe 0
    at(dropped, 1) match
      case column: Int32Column => column.values.length shouldBe 2
      case other => fail(s"expected Int32, got $other")
  }

  "a string filter range" should "see a null and an empty cell inside the window only" in {
    val dictionary = Array("in", "", "no", "out")
    val codes = Array(0, 1, 3, 0, 1, 0, 2, 1, 3)
    val present = Array(false, true, true, false, true, true, true, true, true)
    val batch = strings(dictionary, codes, present)
    val start = 3
    val length = 4
    val calls = new AtomicInteger
    val filtered = ColumnKernel.filterUtf8(
      batch,
      0,
      start,
      length,
      value => {
        calls.incrementAndGet()
        keepText(value)
      },
    )

    calls.get() shouldBe 4
    filtered.length shouldBe 3
    BatchCodec.decodeNullableStrings(filtered) shouldBe Seq(None, Some(""), Some("in"))
    val column = utf8(filtered)
    column.dictionary should be theSameInstanceAs dictionary
    column.codes.length shouldBe length
    column.validity.capacity shouldBe length
    column.isValid(0) shouldBe false
    column.codes(0) shouldBe 0
    column.isValid(1) shouldBe true
    column.dictionary(column.codes(1)) shouldBe ""
    column.codes should not be theSameInstanceAs(codes)

    val outside = ColumnKernel.filterUtf8(batch, 0, 0, 3, value => keepText(value))
    BatchCodec.decodeNullableStrings(outside) shouldBe Seq(None, Some(""))
    utf8(outside).dictionary should be theSameInstanceAs dictionary
  }

  it should "scan a tail shorter than the batch" in {
    val batch = BatchCodec.strings(Seq("a", "b", "a", "b", "a", "b", "a", "b", "a", "b"))
    val filtered = ColumnKernel.filterUtf8(
      batch,
      0,
      8,
      2,
      value =>
        value match
          case "a" => true
          case _ => false,
    )

    BatchCodec.decodeStrings(filtered) shouldBe Seq("a")
    filtered.length shouldBe 1
    utf8(filtered).codes.length shouldBe 2
    utf8(filtered).validity.capacity shouldBe 2
    utf8(filtered).dictionary should be theSameInstanceAs utf8(batch).dictionary
  }

  it should "compact other columns by absolute index" in {
    val dictionary = Array("in", "", "no")
    val codes = Array(0, 0, 1, 2, 0)
    val present = Array(true, false, true, true, true)
    val sides = Array(100, 200, 300, 400, 500)
    val width = 5
    val batch = new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(
        DictUtf8Column.of(dictionary, codes, Validity.pack(width, row => present(row)), width),
        Int32Column.of(sides, Validity.pack(width, row => row != 2), width),
      ),
      width,
    )
    val calls = new AtomicInteger
    val filtered = ColumnKernel.filterUtf8(
      batch,
      0,
      1,
      3,
      value => {
        calls.incrementAndGet()
        value match
          case "in" => throw new IllegalStateException("predicate saw a row outside the range")
          case _ => keepText(value)
      },
    )

    calls.get() shouldBe 3
    filtered.length shouldBe 2
    val texts = utf8(filtered)
    texts.dictionary should be theSameInstanceAs dictionary
    texts.codes.length shouldBe 3
    texts.isValid(0) shouldBe false
    texts.codes(0) shouldBe 0
    texts.isValid(1) shouldBe true
    texts.dictionary(texts.codes(1)) shouldBe ""
    at(filtered, 1) match
      case column: Int32Column =>
        column.values.length shouldBe width
        column.isValid(0) shouldBe true
        column.values(0) shouldBe 200
        column.isValid(1) shouldBe false
        column.values(1) shouldBe 0
      case other => fail(s"expected Int32, got $other")
  }

  it should "match the whole-batch filter and keep the dictionary" in {
    val rows = Seq(Some("in"), None, Some(""), Some("no"), Some("in"))
    val batch = BatchCodec.nullableStrings(rows)
    val whole = ColumnKernel.filterUtf8(batch, 0, value => keepText(value))
    val ranged = ColumnKernel.filterUtf8(batch, 0, 0, batch.length, value => keepText(value))

    BatchCodec.decodeNullableStrings(whole) shouldBe Seq(Some("in"), None, Some(""), Some("in"))
    sameUtf8Batch(whole, ranged)
    utf8(ranged).dictionary should be theSameInstanceAs utf8(batch).dictionary
    utf8(whole).codes.length shouldBe batch.length
    utf8(whole).isValid(1) shouldBe false
    utf8(whole).codes(1) shouldBe 0

    val rawCodes = Array(0, 1, 0, 2)
    val dictionary = Array("in", "", "no")
    val shortened = single(
      DictUtf8Column.of(dictionary, rawCodes, Validity.allValid(rawCodes.length), length = 3),
    )
    val wholeShort = ColumnKernel.filterUtf8(shortened, 0, value => keepText(value))
    val rangedShort = ColumnKernel.filterUtf8(shortened, 0, 0, shortened.length, value => keepText(value))

    BatchCodec.decodeNullableStrings(wholeShort) shouldBe Seq(Some("in"), Some(""), Some("in"))
    sameUtf8Batch(wholeShort, rangedShort)
    utf8(wholeShort).dictionary should be theSameInstanceAs dictionary
    rawCodes shouldBe Array(0, 1, 0, 2)
  }

  it should "return an empty batch for an empty range" in {
    val batch = BatchCodec.strings(Seq("in", ""))
    val calls = new AtomicInteger
    val filtered = ColumnKernel.filterUtf8(
      batch,
      0,
      1,
      0,
      _ => {
        calls.incrementAndGet()
        true
      },
    )

    calls.get() shouldBe 0
    filtered.length shouldBe 0
    BatchCodec.decodeStrings(filtered) shouldBe Seq.empty[String]
    utf8(filtered).dictionary should be theSameInstanceAs utf8(batch).dictionary
    utf8(filtered).codes should not be theSameInstanceAs(utf8(batch).codes)
  }

  "a dictionary string filter range" should "select codes in the window and share the dictionary" in {
    val dictionary = Array("in", "", "no", "out")
    val codes = Array(0, 1, 0, 0, 1, 0, 2, 1, 3)
    val present = Array(false, true, true, false, true, true, true, true, true)
    val batch = strings(dictionary, codes, present)
    val filtered = ColumnKernel.filterUtf8ByValues(batch, 0, 3, 4, Seq("in", ""))

    BatchCodec.decodeStrings(filtered) shouldBe Seq("", "in")
    filtered.length shouldBe 2
    utf8(filtered).dictionary should be theSameInstanceAs dictionary
    utf8(filtered).codes.length shouldBe 4
    utf8(filtered).validity.capacity shouldBe 4
    utf8(filtered).codes should not be theSameInstanceAs(codes)

    val outside = ColumnKernel.filterUtf8ByValues(batch, 0, 0, 3, Seq("in"))
    BatchCodec.decodeStrings(outside) shouldBe Seq("in")
    utf8(outside).dictionary should be theSameInstanceAs dictionary
  }

  it should "scan a tail shorter than the batch" in {
    val batch = BatchCodec.strings(Seq("a", "b", "a", "b", "a", "b", "a", "b", "a", "b"))
    val filtered = ColumnKernel.filterUtf8ByValues(batch, 0, 8, 2, Seq("a"))

    BatchCodec.decodeStrings(filtered) shouldBe Seq("a")
    utf8(filtered).codes.length shouldBe 2
    utf8(filtered).dictionary should be theSameInstanceAs utf8(batch).dictionary
  }

  it should "compact other columns by absolute index" in {
    val dictionary = Array("in", "", "no")
    val codes = Array(0, 0, 1, 2, 0)
    val present = Array(true, false, true, true, true)
    val sides = Array(100, 200, 300, 400, 500)
    val width = 5
    val batch = new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(
        DictUtf8Column.of(dictionary, codes, Validity.pack(width, row => present(row)), width),
        Int32Column.of(sides, Validity.allValid(width), width),
      ),
      width,
    )
    val filtered = ColumnKernel.filterUtf8ByValues(batch, 0, 1, 3, Seq("", "no"))

    filtered.length shouldBe 2
    val texts = utf8(filtered)
    texts.dictionary should be theSameInstanceAs dictionary
    texts.codes.length shouldBe 3
    texts.isValid(0) shouldBe true
    texts.dictionary(texts.codes(0)) shouldBe ""
    texts.isValid(1) shouldBe true
    texts.dictionary(texts.codes(1)) shouldBe "no"
    at(filtered, 1) match
      case column: Int32Column =>
        column.values.length shouldBe width
        column.values(0) shouldBe 300
        column.values(1) shouldBe 400
      case other => fail(s"expected Int32, got $other")
  }

  it should "match the whole-batch filter and keep the dictionary" in {
    val rows = Seq(Some("in"), None, Some(""), Some("no"), Some("in"))
    val batch = BatchCodec.nullableStrings(rows)
    val whole = ColumnKernel.filterUtf8ByValues(batch, 0, Seq("in", ""))
    val ranged = ColumnKernel.filterUtf8ByValues(batch, 0, 0, batch.length, Seq("in", ""))

    BatchCodec.decodeStrings(whole) shouldBe Seq("in", "", "in")
    sameUtf8Batch(whole, ranged)
    utf8(ranged).dictionary should be theSameInstanceAs utf8(batch).dictionary
    utf8(whole).codes.length shouldBe batch.length
  }

  it should "return an empty batch for an empty range" in {
    val batch = BatchCodec.strings(Seq("in", ""))
    val filtered = ColumnKernel.filterUtf8ByValues(batch, 0, 1, 0, Seq("in", ""))

    filtered.length shouldBe 0
    BatchCodec.decodeStrings(filtered) shouldBe Seq.empty[String]
    utf8(filtered).dictionary should be theSameInstanceAs utf8(batch).dictionary
  }

  "a bool filter range" should "skip nulls before, inside, and after a window past row 0" in {
    val stored = Array[Byte](2, 1, 2, 1, 2, 0, 2, 1, 0)
    val present = Array(false, true, false, true, false, true, false, true, true)
    val start = 3
    val length = 3
    val batch = nullableBools(stored, present)
    val calls = new AtomicInteger
    val filtered = ColumnKernel.filterBool(
      batch,
      0,
      start,
      length,
      value => {
        calls.incrementAndGet()
        value
      },
    )

    calls.get() shouldBe 2
    filtered.length shouldBe 1
    BatchCodec.decodeBools(filtered) shouldBe Seq(true)
    bool(filtered).values.length shouldBe length
    bool(filtered).validity.capacity shouldBe length
    bool(filtered).values(0) shouldBe 1.toByte
    bool(filtered).values should not be theSameInstanceAs(stored)
    stored shouldBe Array[Byte](2, 1, 2, 1, 2, 0, 2, 1, 0)

    val nullOnly = ColumnKernel.filterBool(
      batch,
      0,
      2,
      1,
      value => throw new IllegalStateException(s"predicate saw $value"),
    )
    nullOnly.length shouldBe 0
    bool(nullOnly).values.length shouldBe 1
  }

  it should "scan a tail shorter than the batch" in {
    val batch = BatchCodec.bools((1 to 10).map(row => row % 2 == 0))
    val filtered = ColumnKernel.filterBool(batch, 0, 8, 2, value => value)

    BatchCodec.decodeBools(filtered) shouldBe Seq(true)
    filtered.length shouldBe 1
    bool(filtered).values.length shouldBe 2
    bool(filtered).validity.capacity shouldBe 2
    bool(filtered).values should not be theSameInstanceAs(bool(batch).values)
  }

  it should "compact other columns by absolute index" in {
    val width = 5
    val keys = Array(7, 1, 2, 3, 4)
    val keyPresent = Array(true, true, true, false, true)
    val values = Array[Byte](2, 1, 0, 2, 1)
    val valuePresent = Array(true, true, true, true, false)
    val batch = new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Bool),
      Vector(
        Int32Column.of(keys, Validity.pack(width, row => keyPresent(row)), width),
        BoolColumn.of(values, Validity.pack(width, row => valuePresent(row)), width),
      ),
      width,
    )
    val calls = new AtomicInteger
    val filtered = ColumnKernel.filterBool(
      batch,
      1,
      1,
      3,
      value => {
        calls.incrementAndGet()
        value
      },
    )

    calls.get() shouldBe 3
    filtered.length shouldBe 2
    at(filtered, 0) match
      case column: Int32Column =>
        column.values.length shouldBe width
        column.values(0) shouldBe 1
        column.isValid(1) shouldBe false
        column.values(1) shouldBe 0
      case other => fail(s"expected Int32, got $other")
    at(filtered, 1) match
      case column: BoolColumn =>
        column.values.length shouldBe 3
        column.values(0) shouldBe 1.toByte
        column.values(1) shouldBe 1.toByte
        column.values should not be theSameInstanceAs(values)
      case other => fail(s"expected Bool, got $other")
  }

  it should "match the whole-batch filter when the range is the whole batch" in {
    val rows = Seq(Some(true), None, Some(false), Some(true))
    val batch = BatchCodec.nullableBools(rows)
    val whole = ColumnKernel.filterBool(batch, 0, value => value)
    val ranged = ColumnKernel.filterBool(batch, 0, 0, batch.length, value => value)

    BatchCodec.decodeNullableBools(whole) shouldBe Seq(Some(true), Some(true))
    sameBoolBatch(whole, ranged)
    bool(ranged).values should not be theSameInstanceAs(bool(batch).values)
    bool(whole).values.length shouldBe batch.length

    val raw = Array[Byte](2, 0, 1, 9)
    val shortened = single(BoolColumn.of(raw, Validity.allValid(raw.length), length = 3))
    val wholeShort = ColumnKernel.filterBool(shortened, 0, value => value)
    val rangedShort = ColumnKernel.filterBool(shortened, 0, 0, shortened.length, value => value)

    BatchCodec.decodeBools(wholeShort) shouldBe Seq(true, true)
    sameBoolBatch(wholeShort, rangedShort)
    bool(wholeShort).values(0) shouldBe 1.toByte
    bool(wholeShort).values.length shouldBe shortened.length
    raw shouldBe Array[Byte](2, 0, 1, 9)

    val pairs = new ColumnBatch(
      Vector(LogicalType.Bool, LogicalType.Int32),
      Vector(
        BoolColumn.of(Array[Byte](0, 2, 0, 1), Validity.pack(4, row => row != 2), 4),
        Int32Column.of(Array(1, 2, 3, 4), Validity.allValid(4), 4),
      ),
      4,
    )
    val wholePairs = ColumnKernel.filterBool(pairs, 0, value => value)
    val rangedPairs = ColumnKernel.filterBool(pairs, 0, 0, pairs.length, value => value)

    sameBoolBatch(wholePairs, rangedPairs)
    at(wholePairs, 1) match
      case column: Int32Column =>
        column.values.toSeq shouldBe Seq(2, 4, 0, 0)
        column.values.length shouldBe pairs.length
      case other => fail(s"expected Int32, got $other")
  }

  it should "return an empty batch for an empty range" in {
    val batch = BatchCodec.bools(Seq(true, false, true))
    val calls = new AtomicInteger
    val filtered = ColumnKernel.filterBool(
      batch,
      0,
      1,
      0,
      _ => {
        calls.incrementAndGet()
        true
      },
    )

    calls.get() shouldBe 0
    filtered.length shouldBe 0
    BatchCodec.decodeBools(filtered) shouldBe Seq.empty[Boolean]
    filtered.schema shouldBe batch.schema
    bool(filtered).values should not be theSameInstanceAs(bool(batch).values)
  }

  "a float project range" should "map a window past row 0 and leave nulls uncalled" in {
    val stored = Array(poison, 1.0, poison, payloadNaN, negativeZero, 4.0, poison, -3.0, 7.0)
    val present = Array(false, true, false, true, true, true, false, true, true)
    val start = 3
    val length = 3
    val batch = nullableFloats(stored, present)
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectFloat64(
      batch,
      0,
      start,
      length,
      value => {
        calls.incrementAndGet()
        if (value == poison) then throw new IllegalStateException("mapper saw a null slot")
        value + 1.0
      },
    )

    calls.get() shouldBe 3
    mapped.length shouldBe length
    float64(mapped).values.length shouldBe length
    float64(mapped).validity.capacity shouldBe length
    float64(mapped).isValid(0) shouldBe true
    float64(mapped).isValid(1) shouldBe true
    float64(mapped).isValid(2) shouldBe true
    bits(float64(mapped).values(0)) shouldBe bits(payloadNaN + 1.0)
    bits(float64(mapped).values(1)) shouldBe bits(negativeZero + 1.0)
    float64(mapped).values(2) shouldBe 5.0
    float64(mapped).values should not be theSameInstanceAs(stored)
    float64(mapped).validity should not be theSameInstanceAs(float64(batch).validity)
    stored(0) shouldBe poison
    bits(stored(3)) shouldBe bits(payloadNaN)
    bits(stored(4)) shouldBe bits(negativeZero)

    val kept = ColumnKernel.projectFloat64(batch, 0, start, length, value => value)
    bits(float64(kept).values(0)) shouldBe bits(payloadNaN)
    bits(float64(kept).values(1)) shouldBe bits(negativeZero)
    float64(kept).values(2) shouldBe 4.0

    val nullCalls = new AtomicInteger
    val nullOnly = ColumnKernel.projectFloat64(
      batch,
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
    float64(nullOnly).isValid(0) shouldBe false
    float64(nullOnly).values(0) shouldBe 0.0
  }

  it should "scan a tail shorter than the batch" in {
    val batch = BatchCodec.float64s((1 to 10).map(_.toDouble))
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectFloat64(
      batch,
      0,
      8,
      2,
      value => {
        calls.incrementAndGet()
        value + 1.0
      },
    )

    calls.get() shouldBe 2
    mapped.length shouldBe 2
    BatchCodec.decodeFloat64s(mapped) shouldBe Seq(10.0, 11.0)
    float64(mapped).values.length shouldBe 2
    float64(mapped).validity.capacity shouldBe 2
    float64(mapped).values should not be theSameInstanceAs(float64(batch).values)
  }

  it should "return an empty batch for an empty range and call nothing" in {
    val batch = BatchCodec.float64s(Seq(1.0, 2.0, 3.0))
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectFloat64(
      batch,
      0,
      1,
      0,
      _ => {
        calls.incrementAndGet()
        0.0
      },
    )

    calls.get() shouldBe 0
    mapped.length shouldBe 0
    BatchCodec.decodeFloat64s(mapped) shouldBe Seq.empty[Double]
    mapped.schema shouldBe batch.schema
    float64(mapped).values should not be theSameInstanceAs(float64(batch).values)
  }

  it should "match the whole-batch project when the range is the whole batch" in {
    val rows = Seq(Some(1.0), None, Some(negativeZero), Some(payloadNaN))
    val batch = BatchCodec.nullableFloat64s(rows)
    val calls = new AtomicInteger
    val mapper: ColumnKernel.Float64Map = value =>
      calls.incrementAndGet()
      value
    val whole = ColumnKernel.projectFloat64(batch, 0, mapper)
    val ranged = ColumnKernel.projectFloat64(batch, 0, 0, batch.length, mapper)

    calls.get() shouldBe 6
    sameFloatBatch(whole, ranged)
    float64(whole).validity should be theSameInstanceAs float64(batch).validity
    float64(ranged).validity should be theSameInstanceAs float64(batch).validity
    float64(whole).length shouldBe batch.length
    bits(float64(whole).values(2)) shouldBe bits(negativeZero)
    bits(float64(whole).values(3)) shouldBe bits(payloadNaN)
    float64(whole).isValid(1) shouldBe false
    float64(whole).values(1) shouldBe 0.0

    val raw = Array(1.0, negativeZero, payloadNaN, poison)
    val validity = Validity.allValid(raw.length)
    val shortened = single(Float64Column.of(raw, validity, length = 3))
    val wholeShort = ColumnKernel.projectFloat64(
      shortened,
      0,
      value => if (value == poison) then throw new IllegalStateException("read past length") else value,
    )
    val rangedShort = ColumnKernel.projectFloat64(
      shortened,
      0,
      0,
      shortened.length,
      value => if (value == poison) then throw new IllegalStateException("read past length") else value,
    )

    sameFloatBatch(wholeShort, rangedShort)
    float64(wholeShort).values.length shouldBe raw.length
    float64(wholeShort).validity should be theSameInstanceAs validity
    bits(float64(wholeShort).values(1)) shouldBe bits(negativeZero)
    bits(float64(wholeShort).values(2)) shouldBe bits(payloadNaN)
    raw(3) shouldBe poison
  }

  it should "copy the other columns of the scanned range" in {
    val width = 5
    val keys = Array(poison, 10.0, negativeZero, payloadNaN, 40.0)
    val keyPresent = Array(false, true, true, true, true)
    val sides = Array(100, 200, 300, 400, 500)
    val batch = new ColumnBatch(
      Vector(LogicalType.Float64, LogicalType.Int32),
      Vector(
        Float64Column.of(keys, Validity.pack(width, row => keyPresent(row)), width),
        Int32Column.of(sides, Validity.pack(width, row => row != 2), width),
      ),
      width,
    )
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectFloat64(
      batch,
      0,
      1,
      3,
      value => {
        calls.incrementAndGet()
        if (value == poison) then throw new IllegalStateException("mapper saw a null slot")
        value
      },
    )

    calls.get() shouldBe 3
    mapped.length shouldBe 3
    at(mapped, 0) match
      case column: Float64Column =>
        column.values.length shouldBe 3
        bits(column.values(0)) shouldBe bits(10.0)
        bits(column.values(1)) shouldBe bits(negativeZero)
        bits(column.values(2)) shouldBe bits(payloadNaN)
        column.values should not be theSameInstanceAs(keys)
      case other => fail(s"expected Float64, got $other")
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

  "a string project range" should "map empty cells inside the window and build a new dictionary" in {
    val dictionary = Array("out", "", "in", "zz")
    val codes = Array(0, 1, 0, 2, 1, 2, 3)
    val present = Array(true, true, false, true, true, false, true)
    val batch = strings(dictionary, codes, present)
    val start = 3
    val length = 3
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectUtf8(
      batch,
      0,
      start,
      length,
      value => {
        calls.incrementAndGet()
        value match
          case "out" | "zz" => throw new IllegalStateException("mapper saw a row outside the range")
          case other if BatchCodec.isAbsent(other) => BatchCodec.stringElement(None)
          case text => text.toUpperCase(java.util.Locale.ENGLISH)
      },
    )

    calls.get() shouldBe 3
    mapped.length shouldBe length
    BatchCodec.decodeNullableStrings(mapped) shouldBe Seq(Some("IN"), Some(""), None)
    val column = utf8(mapped)
    column.dictionary shouldNot be theSameInstanceAs dictionary
    column.dictionary.toSeq shouldBe Seq("IN", "")
    column.codes.length shouldBe length
    column.validity.capacity shouldBe length
    column.codes.toSeq shouldBe Seq(0, 1, 0)
    column.isValid(2) shouldBe false
    column.codes(2) shouldBe 0
    column.codes should not be theSameInstanceAs(codes)
    codes shouldBe Array(0, 1, 0, 2, 1, 2, 3)
  }

  it should "scan a tail shorter than the batch" in {
    val batch = BatchCodec.strings(Seq("a", "b", "a", "b", "a", "b", "a", "b", "a", "b"))
    val mapped = ColumnKernel.projectUtf8(batch, 0, 8, 2, _.toUpperCase(java.util.Locale.ENGLISH))

    BatchCodec.decodeStrings(mapped) shouldBe Seq("A", "B")
    mapped.length shouldBe 2
    utf8(mapped).codes.length shouldBe 2
    utf8(mapped).validity.capacity shouldBe 2
    utf8(mapped).dictionary shouldNot be theSameInstanceAs utf8(batch).dictionary
    utf8(mapped).dictionary.toSeq shouldBe Seq("A", "B")
  }

  it should "return an empty batch for an empty range and call nothing" in {
    val batch = BatchCodec.strings(Seq("in", ""))
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectUtf8(
      batch,
      0,
      1,
      0,
      _ => {
        calls.incrementAndGet()
        "x"
      },
    )

    calls.get() shouldBe 0
    mapped.length shouldBe 0
    BatchCodec.decodeStrings(mapped) shouldBe Seq.empty[String]
    utf8(mapped).dictionary.toSeq shouldBe Seq.empty[String]
    utf8(mapped).dictionary shouldNot be theSameInstanceAs utf8(batch).dictionary
    utf8(mapped).codes should not be theSameInstanceAs(utf8(batch).codes)
  }

  it should "match the whole-batch project and keep the dictionary rule" in {
    val rows = Seq(Some("in"), None, Some(""), Some("no"), Some("in"))
    val batch = BatchCodec.nullableStrings(rows)
    val mapper: ColumnKernel.Utf8Map = value =>
      if (BatchCodec.isAbsent(value)) then BatchCodec.stringElement(None)
      else value.toUpperCase(java.util.Locale.ENGLISH)
    val whole = ColumnKernel.projectUtf8(batch, 0, mapper)
    val ranged = ColumnKernel.projectUtf8(batch, 0, 0, batch.length, mapper)

    BatchCodec.decodeNullableStrings(whole) shouldBe Seq(Some("IN"), None, Some(""), Some("NO"), Some("IN"))
    ranged.length shouldBe whole.length
    utf8(ranged).dictionary.toSeq shouldBe utf8(whole).dictionary.toSeq
    utf8(ranged).dictionary shouldNot be theSameInstanceAs utf8(batch).dictionary
    utf8(whole).dictionary shouldNot be theSameInstanceAs utf8(batch).dictionary
    utf8(ranged).codes.toSeq shouldBe utf8(whole).codes.toSeq
    utf8(ranged).validity.capacity shouldBe utf8(whole).validity.capacity
    utf8(ranged).validity.words.toSeq shouldBe utf8(whole).validity.words.toSeq
    utf8(whole).isValid(1) shouldBe false
    utf8(whole).codes(1) shouldBe 0
    utf8(whole).dictionary.toSeq shouldBe Seq("IN", "", "NO")

    val rawCodes = Array(0, 1, 0, 2)
    val dictionary = Array("in", "", "no")
    val shortened = single(
      DictUtf8Column.of(dictionary, rawCodes, Validity.allValid(rawCodes.length), length = 3),
    )
    val wholeShort = ColumnKernel.projectUtf8(shortened, 0, mapper)
    val rangedShort = ColumnKernel.projectUtf8(shortened, 0, 0, shortened.length, mapper)

    BatchCodec.decodeNullableStrings(wholeShort) shouldBe Seq(Some("IN"), Some(""), Some("IN"))
    utf8(rangedShort).dictionary.toSeq shouldBe utf8(wholeShort).dictionary.toSeq
    utf8(wholeShort).codes.toSeq shouldBe utf8(rangedShort).codes.toSeq
    utf8(wholeShort).dictionary shouldNot be theSameInstanceAs dictionary
    rawCodes shouldBe Array(0, 1, 0, 2)
  }

  it should "copy the other columns of the scanned range" in {
    val dictionary = Array("in", "", "no")
    val codes = Array(0, 0, 1, 2, 0)
    val present = Array(true, false, true, true, true)
    val sides = Array(100, 200, 300, 400, 500)
    val width = 5
    val batch = new ColumnBatch(
      Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      Vector(
        DictUtf8Column.of(dictionary, codes, Validity.pack(width, row => present(row)), width),
        Int32Column.of(sides, Validity.pack(width, row => row != 2), width),
      ),
      width,
    )
    val calls = new AtomicInteger
    val mapped = ColumnKernel.projectUtf8(
      batch,
      0,
      1,
      3,
      value => {
        calls.incrementAndGet()
        value match
          case "in" => throw new IllegalStateException("mapper saw a row outside the range")
          case other if BatchCodec.isAbsent(other) => "n"
          case text => text.toUpperCase(java.util.Locale.ENGLISH)
      },
    )

    calls.get() shouldBe 3
    mapped.length shouldBe 3
    val texts = utf8(mapped)
    texts.dictionary shouldNot be theSameInstanceAs dictionary
    texts.dictionary.toSeq shouldBe Seq("n", "", "NO")
    texts.codes.length shouldBe 3
    texts.isValid(0) shouldBe true
    texts.dictionary(texts.codes(0)) shouldBe "n"
    texts.isValid(1) shouldBe true
    texts.dictionary(texts.codes(1)) shouldBe ""
    at(mapped, 1) match
      case column: Int32Column =>
        column.values.length shouldBe width
        column.isValid(0) shouldBe true
        column.values(0) shouldBe 200
        column.isValid(1) shouldBe false
        column.values(1) shouldBe 0
      case other => fail(s"expected Int32, got $other")
  }

  private def keepFloat(value: Double): Boolean =
    java.lang.Double.isNaN(value) ||
      java.lang.Double.doubleToRawLongBits(value) < 0L ||
      value > 0.0

  private def keepText(value: String): Boolean = value match
    case "in" | "" => true
    case other => BatchCodec.isAbsent(other)

  private def bits(value: Double): Long =
    java.lang.Double.doubleToRawLongBits(value)

  private def single(column: Column): ColumnBatch =
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  private def nullableFloats(values: Array[Double], present: Array[Boolean]): ColumnBatch =
    val column =
      Float64Column.of(values, Validity.pack(values.length, row => present(row)), values.length)
    single(column)

  private def nullableBools(values: Array[Byte], present: Array[Boolean]): ColumnBatch =
    val column =
      BoolColumn.of(values, Validity.pack(values.length, row => present(row)), values.length)
    single(column)

  private def strings(
      dictionary: Array[String],
      codes: Array[Int],
      present: Array[Boolean],
  ): ColumnBatch =
    val column = DictUtf8Column.of(
      dictionary,
      codes,
      Validity.pack(codes.length, row => present(row)),
      codes.length,
    )
    single(column)

  private def float64(batch: ColumnBatch): Float64Column =
    at(batch, 0) match
      case column: Float64Column => column
      case other => fail(s"expected Float64, got $other")

  private def utf8(batch: ColumnBatch): DictUtf8Column =
    at(batch, 0) match
      case column: DictUtf8Column => column
      case other => fail(s"expected Utf8Dict, got $other")

  private def bool(batch: ColumnBatch): BoolColumn =
    at(batch, 0) match
      case column: BoolColumn => column
      case other => fail(s"expected Bool, got $other")

  private def at(batch: ColumnBatch, ordinal: Int): Column =
    batch.columns.lift(ordinal).getOrElse(fail(s"missing column $ordinal"))

  private def sameFloatBatch(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    left.schema shouldBe right.schema
    left.columns.indices.foreach { ordinal =>
      (at(left, ordinal), at(right, ordinal)) match
        case (leftColumn: Float64Column, rightColumn: Float64Column) =>
          leftColumn.length shouldBe rightColumn.length
          leftColumn.values.indices.foreach { row =>
            bits(leftColumn.values(row)) shouldBe bits(rightColumn.values(row))
          }
          leftColumn.validity.capacity shouldBe rightColumn.validity.capacity
          leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq
        case (leftColumn: Int32Column, rightColumn: Int32Column) =>
          leftColumn.values.toSeq shouldBe rightColumn.values.toSeq
          leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq
        case _ => fail(s"column $ordinal does not match")
    }

  private def sameUtf8Batch(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    left.schema shouldBe right.schema
    utf8(left).length shouldBe utf8(right).length
    utf8(left).dictionary should be theSameInstanceAs utf8(right).dictionary
    utf8(left).codes.toSeq shouldBe utf8(right).codes.toSeq
    utf8(left).validity.capacity shouldBe utf8(right).validity.capacity
    utf8(left).validity.words.toSeq shouldBe utf8(right).validity.words.toSeq

  private def sameBoolBatch(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    left.schema shouldBe right.schema
    left.columns.indices.foreach { ordinal =>
      (at(left, ordinal), at(right, ordinal)) match
        case (leftColumn: BoolColumn, rightColumn: BoolColumn) =>
          leftColumn.length shouldBe rightColumn.length
          leftColumn.values.toSeq shouldBe rightColumn.values.toSeq
          leftColumn.validity.capacity shouldBe rightColumn.validity.capacity
          leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq
        case (leftColumn: Int32Column, rightColumn: Int32Column) =>
          leftColumn.values.toSeq shouldBe rightColumn.values.toSeq
          leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq
        case _ => fail(s"column $ordinal does not match")
    }
