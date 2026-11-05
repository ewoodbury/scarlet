package com.ewoodbury.scarlet.columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TestColumnKernelPipeline extends AnyFlatSpec with Matchers with ColumnKernelFixture:

  private def fullMorsel(batch: ColumnBatch): Morsel = Morsel(batch, 0, batch.length)

  private def float64(batch: ColumnBatch): Float64Column =
    at(batch, 0) match
      case column: Float64Column => column
      case other => fail(s"expected Float64, got $other")

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
      fullMorsel(batch),
      _ > 0,
      value =>
        if (value == 42) then throw new IllegalStateException("mapper saw a null slot")
        else value * 10,
    )
    val fusedProjects = Int32Pipeline.PP(
      fullMorsel(batch),
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
    val first = Int32Pipeline.FP(fullMorsel(single(input)), _ > 1, value => value)
    val second = Int32Pipeline.FP(fullMorsel(first), _ > 2, _ + 1)

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
      fullMorsel(input),
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
      fullMorsel(input),
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
      fullMorsel(input),
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
      fullMorsel(single(input)),
      value =>
        if (value == 99.0) then throw new IllegalStateException("read past length")
        else value,
      value => value,
    )
    val filtered = Float64Pipeline.FP(
      fullMorsel(single(input)),
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
      fullMorsel(input),
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
      fullMorsel(input),
      value => Option(value).exists(_.length > 1),
      value =>
        if (Option(value).isEmpty) then
          throw new IllegalStateException("second predicate saw an empty cell")
        else true,
    )
    val projected = Utf8Pipeline.FP(
      fullMorsel(input),
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
      fullMorsel(input),
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
      fullMorsel(input),
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
    an[IllegalArgumentException] should be thrownBy Int32Pipeline.PP(
      fullMorsel(pairs),
      _ + 1,
      _ + 1,
    )
    an[IllegalArgumentException] should be thrownBy
      Float64Pipeline.PP(fullMorsel(pairs), _ + 1.0, _ + 1.0)
    an[IllegalArgumentException] should be thrownBy
      Utf8Pipeline.PP(fullMorsel(pairs), (value: String) => value, (value: String) => value)
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
