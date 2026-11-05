package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.columnar.*

class TestMorselRanges extends AnyFlatSpec with Matchers with MorselFixture:

  "an int element map" should "project a range that starts past zero" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.ints(Morsel(sparseBatch, 3, 3), List(projectStep(calls), projectStep(calls)))

    BatchCodec.decodeNullableInts(mapped) shouldBe Seq(Some(32), None, Some(52))
    mapped.length shouldBe 3
    // Two present rows in the range, two maps each. Six present rows sit in the batch.
    calls.get() shouldBe 4
  }

  it should "filter then map a range with nulls before, inside, and after it" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val mapped = ElementPipeline.ints(
      Morsel(sparseBatch, 3, 3),
      List(filterStep(predicates), projectStep(mappers, value => value * 2)),
    )

    BatchCodec.decodeNullableInts(mapped) shouldBe Seq(Some(60), Some(100))
    predicates.get() shouldBe 2
    mappers.get() shouldBe 2
  }

  it should "scan a tail shorter than the source batch" in {
    val calls = new AtomicInteger
    val batch = sparseBatch
    val mapped = ElementPipeline.ints(
      Morsel(batch, 7, 2),
      List(projectStep(calls), projectStep(calls)),
    )

    batch.length should be > 2
    BatchCodec.decodeNullableInts(mapped) shouldBe Seq(None, Some(82))
    mapped.length shouldBe 2
    calls.get() shouldBe 2
  }

  it should "return an empty batch for an empty range and call nothing" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.ints(
      Morsel(sparseBatch, 4, 0),
      List(filterStep(calls), projectStep(calls)),
    )

    mapped.length shouldBe 0
    BatchCodec.decodeNullableInts(mapped) shouldBe Seq.empty[Option[Int]]
    calls.get() shouldBe 0
  }

  it should "match a full-range scan to the whole-batch kernels" in {
    val batch = sparseBatch
    val morsel = Morsel(batch, 0, batch.length)
    val fusedProjects = ElementPipeline.ints(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Project((value: Int) => value + 1),
        ColumnarPlanner.ElementStep.Project((value: Int) => value * 2),
      ),
    )
    val stepwiseProjects = ColumnKernel.projectInt32(
      ColumnKernel.projectInt32(batch, 0, _ + 1),
      0,
      _ * 2,
    )
    sameIntColumn(fusedProjects, stepwiseProjects)

    val fusedFilter = ElementPipeline.ints(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Filter((value: Int) => value > 20),
        ColumnarPlanner.ElementStep.Project((value: Int) => value * 2),
      ),
    )
    val stepwiseFilter = ColumnKernel.projectInt32(
      ColumnKernel.filterInt32(batch, 0, _ > 20),
      0,
      _ * 2,
    )
    sameIntColumn(fusedFilter, stepwiseFilter)
  }

  it should "leave rows outside the range uncalled" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val seen = new AtomicInteger
    val keep: Int => Boolean = value =>
      predicates.incrementAndGet()
      if (value == poison || value < 30 || value > 50) then
        seen.incrementAndGet()
        ()
      value > 0
    val project: Int => Int = value =>
      mappers.incrementAndGet()
      if (value == poison || value < 30 || value > 50) then
        seen.incrementAndGet()
        ()
      value
    val mapped = ElementPipeline.ints(
      Morsel(sparseBatch, 3, 3),
      List(
        ColumnarPlanner.ElementStep.Filter(keep),
        ColumnarPlanner.ElementStep.Project(project),
      ),
    )

    BatchCodec.decodeNullableInts(mapped) shouldBe Seq(Some(30), Some(50))
    predicates.get() shouldBe 2
    mappers.get() shouldBe 2
    seen.get() shouldBe 0
  }

  "a long element map" should "project a range that starts past zero" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.longs(
      Morsel(sparseLongBatch, 3, 3),
      List(projectLongStep(calls), projectLongStep(calls)),
    )

    BatchCodec.decodeNullableLongs(mapped) shouldBe Seq(Some(32L), None, Some(52L))
    mapped.length shouldBe 3
    // Two present rows in the range, two maps each. Six present rows sit in the batch.
    calls.get() shouldBe 4
  }

  it should "filter then map a range with nulls before, inside, and after it" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val mapped = ElementPipeline.longs(
      Morsel(sparseLongBatch, 3, 3),
      List(filterLongStep(predicates), projectLongStep(mappers, value => value * 2L)),
    )

    BatchCodec.decodeNullableLongs(mapped) shouldBe Seq(Some(60L), Some(100L))
    predicates.get() shouldBe 2
    mappers.get() shouldBe 2
  }

  it should "scan a tail shorter than the source batch" in {
    val calls = new AtomicInteger
    val batch = sparseLongBatch
    val mapped = ElementPipeline.longs(
      Morsel(batch, 7, 2),
      List(projectLongStep(calls), projectLongStep(calls)),
    )

    batch.length should be > 2
    BatchCodec.decodeNullableLongs(mapped) shouldBe Seq(None, Some(82L))
    mapped.length shouldBe 2
    calls.get() shouldBe 2
  }

  it should "return an empty batch for an empty range and call nothing" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.longs(
      Morsel(sparseLongBatch, 4, 0),
      List(filterLongStep(calls), projectLongStep(calls)),
    )

    mapped.length shouldBe 0
    BatchCodec.decodeNullableLongs(mapped) shouldBe Seq.empty[Option[Long]]
    calls.get() shouldBe 0
  }

  it should "match a full-range scan to the whole-batch kernels" in {
    val batch = sparseLongBatch
    val morsel = Morsel(batch, 0, batch.length)
    val fusedProjects = ElementPipeline.longs(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Project((value: Long) => value + 1L),
        ColumnarPlanner.ElementStep.Project((value: Long) => value * 2L),
      ),
    )
    val stepwiseProjects = ColumnKernel.projectInt64(
      ColumnKernel.projectInt64(batch, 0, _ + 1L),
      0,
      _ * 2L,
    )
    sameLongColumn(fusedProjects, stepwiseProjects)

    val fusedFilter = ElementPipeline.longs(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Filter((value: Long) => value > 20L),
        ColumnarPlanner.ElementStep.Project((value: Long) => value * 2L),
      ),
    )
    val stepwiseFilter = ColumnKernel.projectInt64(
      ColumnKernel.filterInt64(batch, 0, _ > 20L),
      0,
      _ * 2L,
    )
    sameLongColumn(fusedFilter, stepwiseFilter)
  }

  it should "leave rows outside the range uncalled" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val seen = new AtomicInteger
    val keep: Long => Boolean = value =>
      predicates.incrementAndGet()
      if (value == poison.toLong || value < 30L || value > 50L) then
        seen.incrementAndGet()
        ()
      value > 0L
    val project: Long => Long = value =>
      mappers.incrementAndGet()
      if (value == poison.toLong || value < 30L || value > 50L) then
        seen.incrementAndGet()
        ()
      value
    val mapped = ElementPipeline.longs(
      Morsel(sparseLongBatch, 3, 3),
      List(
        ColumnarPlanner.ElementStep.Filter(keep),
        ColumnarPlanner.ElementStep.Project(project),
      ),
    )

    BatchCodec.decodeNullableLongs(mapped) shouldBe Seq(Some(30L), Some(50L))
    predicates.get() shouldBe 2
    mappers.get() shouldBe 2
    seen.get() shouldBe 0
  }

  "a float element map" should "project a range that starts past zero" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.floats(
      Morsel(sparseFloatBatch, 3, 3),
      List(projectFloatStep(calls), projectFloatStep(calls)),
    )

    BatchCodec.decodeNullableFloat64s(mapped) shouldBe Seq(Some(32.0), None, Some(52.0))
    mapped.length shouldBe 3
    // Two present rows in the range, two maps each. Six present rows sit in the batch.
    calls.get() shouldBe 4
  }

  it should "filter then map a range with nulls before, inside, and after it" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val mapped = ElementPipeline.floats(
      Morsel(sparseFloatBatch, 3, 3),
      List(filterFloatStep(predicates), projectFloatStep(mappers, value => value * 2.0)),
    )

    BatchCodec.decodeNullableFloat64s(mapped) shouldBe Seq(Some(60.0), Some(100.0))
    predicates.get() shouldBe 2
    mappers.get() shouldBe 2
  }

  it should "scan a tail shorter than the source batch" in {
    val calls = new AtomicInteger
    val batch = sparseFloatBatch
    val mapped = ElementPipeline.floats(
      Morsel(batch, 7, 2),
      List(projectFloatStep(calls), projectFloatStep(calls)),
    )

    batch.length should be > 2
    BatchCodec.decodeNullableFloat64s(mapped) shouldBe Seq(None, Some(82.0))
    mapped.length shouldBe 2
    calls.get() shouldBe 2
  }

  it should "return an empty batch for an empty range and call nothing" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.floats(
      Morsel(sparseFloatBatch, 4, 0),
      List(filterFloatStep(calls), projectFloatStep(calls)),
    )

    mapped.length shouldBe 0
    BatchCodec.decodeNullableFloat64s(mapped) shouldBe Seq.empty[Option[Double]]
    calls.get() shouldBe 0
  }

  it should "match a full-range scan to the whole-batch kernels" in {
    val batch = floatBitsBatch
    val morsel = Morsel(batch, 0, batch.length)
    val keepBits: Double => Double = value => value
    val fusedProjects = ElementPipeline.floats(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Project(keepBits),
        ColumnarPlanner.ElementStep.Project(keepBits),
      ),
    )
    val stepwiseProjects = ColumnKernel.projectFloat64(
      ColumnKernel.projectFloat64(batch, 0, value => value),
      0,
      value => value,
    )
    sameFloatColumn(fusedProjects, stepwiseProjects)
    bits(float64(fusedProjects).values(0)) shouldBe bits(negativeZero)
    bits(float64(fusedProjects).values(1)) shouldBe bits(payloadNaN)
    float64(fusedProjects).isValid(2) shouldBe false
    float64(fusedProjects).validity should be theSameInstanceAs float64(batch).validity

    val fusedFilter = ElementPipeline.floats(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Filter((value: Double) => value > 20.0),
        ColumnarPlanner.ElementStep.Project((value: Double) => value * 2.0),
      ),
    )
    val stepwiseFilter = ColumnKernel.projectFloat64(
      ColumnKernel.filterFloat64(batch, 0, _ > 20.0),
      0,
      _ * 2.0,
    )
    sameFloatColumn(fusedFilter, stepwiseFilter)
  }

  it should "leave rows outside the range uncalled" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val seen = new AtomicInteger
    val keep: Double => Boolean = value =>
      predicates.incrementAndGet()
      if (value == poison.toDouble || value < 30.0 || value > 50.0) then
        seen.incrementAndGet()
        ()
      value > 0.0
    val project: Double => Double = value =>
      mappers.incrementAndGet()
      if (value == poison.toDouble || value < 30.0 || value > 50.0) then
        seen.incrementAndGet()
        ()
      value
    val mapped = ElementPipeline.floats(
      Morsel(sparseFloatBatch, 3, 3),
      List(
        ColumnarPlanner.ElementStep.Filter(keep),
        ColumnarPlanner.ElementStep.Project(project),
      ),
    )

    BatchCodec.decodeNullableFloat64s(mapped) shouldBe Seq(Some(30.0), Some(50.0))
    predicates.get() shouldBe 2
    mappers.get() shouldBe 2
    seen.get() shouldBe 0
  }

  "a string element map" should "project a range that starts past zero" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.strings(
      Morsel(sparseStringBatch, 3, 3),
      List(projectStringStep(calls), projectStringStep(calls)),
    )

    BatchCodec.decodeNullableStrings(mapped) shouldBe Seq(Some("IN"), Some(""), None)
    mapped.length shouldBe 3
    // "in", the empty string, and the null, twice each. The empty string outside the window is not
    // one of them.
    calls.get() shouldBe 6
    utf8(mapped).dictionary shouldNot be theSameInstanceAs utf8(sparseStringBatch).dictionary
    utf8(mapped).dictionary.toSeq shouldBe Seq("IN", "")
  }

  it should "filter then map a range with nulls and empty cells before, inside, and after it" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val mapped = ElementPipeline.strings(
      Morsel(sparseStringBatch, 3, 3),
      List(filterStringStep(predicates), projectStringStep(mappers)),
    )

    BatchCodec.decodeNullableStrings(mapped) shouldBe Seq(Some("IN"))
    // The predicate sees the in-window null and the in-window empty string. The mapper does not.
    predicates.get() shouldBe 3
    mappers.get() shouldBe 1
    utf8(mapped).dictionary.toSeq shouldBe Seq("IN")
    utf8(mapped).dictionary shouldNot be theSameInstanceAs utf8(sparseStringBatch).dictionary
  }

  it should "scan a tail shorter than the source batch" in {
    val calls = new AtomicInteger
    val batch = sparseStringBatch
    val mapped = ElementPipeline.strings(
      Morsel(batch, 6, 2),
      List(projectStringStep(calls), projectStringStep(calls)),
    )

    batch.length should be > 2
    BatchCodec.decodeNullableStrings(mapped) shouldBe Seq(Some("CC"), None)
    mapped.length shouldBe 2
    calls.get() shouldBe 4
  }

  it should "return an empty batch for an empty range and call nothing" in {
    val calls = new AtomicInteger
    val mapped = ElementPipeline.strings(
      Morsel(sparseStringBatch, 4, 0),
      List(filterStringStep(calls), projectStringStep(calls)),
    )

    mapped.length shouldBe 0
    BatchCodec.decodeNullableStrings(mapped) shouldBe Seq.empty[Option[String]]
    calls.get() shouldBe 0
  }

  it should "match a full-range scan to the whole-batch kernels" in {
    val batch = sparseStringBatch
    val morsel = Morsel(batch, 0, batch.length)
    val upper: String => String = value =>
      if (BatchCodec.isAbsent(value)) then BatchCodec.stringElement(None)
      else value.toUpperCase(java.util.Locale.ENGLISH)
    val piped = ElementPipeline.strings(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Project(upper),
        ColumnarPlanner.ElementStep.Project(upper),
      ),
    )
    val stepwise = ColumnKernel.projectUtf8(
      ColumnKernel.projectUtf8(
        batch,
        0,
        value =>
          if (BatchCodec.isAbsent(value)) then BatchCodec.stringElement(None)
          else value.toUpperCase(java.util.Locale.ENGLISH),
      ),
      0,
      value =>
        if (BatchCodec.isAbsent(value)) then BatchCodec.stringElement(None)
        else value.toUpperCase(java.util.Locale.ENGLISH),
    )
    val fused = Utf8Pipeline.PP(
      morsel,
      value =>
        if (BatchCodec.isAbsent(value)) then BatchCodec.stringElement(None)
        else value.toUpperCase(java.util.Locale.ENGLISH),
      value =>
        if (BatchCodec.isAbsent(value)) then BatchCodec.stringElement(None)
        else value.toUpperCase(java.util.Locale.ENGLISH),
    )
    sameStringColumn(piped, stepwise)
    utf8(piped).dictionary.toSeq shouldBe utf8(fused).dictionary.toSeq
    utf8(piped).dictionary shouldNot be theSameInstanceAs utf8(batch).dictionary

    val keep: String => Boolean = value =>
      value match
        case "in" | "" => true
        case other => BatchCodec.isAbsent(other)
    val filtered = ElementPipeline.strings(
      morsel,
      List(
        ColumnarPlanner.ElementStep.Filter(keep),
        ColumnarPlanner.ElementStep.Filter(keep),
      ),
    )
    utf8(filtered).dictionary should be theSameInstanceAs utf8(batch).dictionary
    BatchCodec.decodeNullableStrings(filtered) shouldBe Seq(Some(""), None, Some("in"), Some(""), None, None)
  }

  it should "leave rows outside the range uncalled" in {
    val predicates = new AtomicInteger
    val mappers = new AtomicInteger
    val seen = new AtomicInteger
    val keep: String => Boolean = value =>
      predicates.incrementAndGet()
      value match
        case "out" | "zz" =>
          seen.incrementAndGet()
          false
        case other => !BatchCodec.isAbsent(other) && other.nonEmpty
    val project: String => String = value =>
      mappers.incrementAndGet()
      value match
        case "out" | "zz" | "" =>
          seen.incrementAndGet()
          value
        case other if BatchCodec.isAbsent(other) =>
          seen.incrementAndGet()
          BatchCodec.stringElement(None)
        case text => text.toUpperCase(java.util.Locale.ENGLISH)
    val mapped = ElementPipeline.strings(
      Morsel(sparseStringBatch, 3, 3),
      List(
        ColumnarPlanner.ElementStep.Filter(keep),
        ColumnarPlanner.ElementStep.Project(project),
      ),
    )

    BatchCodec.decodeNullableStrings(mapped) shouldBe Seq(Some("IN"))
    predicates.get() shouldBe 3
    mappers.get() shouldBe 1
    seen.get() shouldBe 0
  }
