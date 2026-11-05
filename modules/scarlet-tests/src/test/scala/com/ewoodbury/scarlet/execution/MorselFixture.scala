package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/** Helpers for the morsel scan and range-scan suites. */
trait MorselFixture extends Matchers with Assertions:

  protected val originalConf = ScarletConf.get
  protected val poison = 999

  // Rows [0, 4) are present, so a prefix check of that width looks dense. The morsel starts at 6.
  // Row 4 is a null before the morsel and row 7 is a null inside it. Both store `poison`.
  protected def ranged(build: (Array[Long], Array[Boolean]) => ColumnBatch): Morsel =
    val values = Array[Long](1, 1, 1, 1, poison.toLong, 50, 100, poison.toLong, 100, 100)
    val present = Array(true, true, true, true, false, true, true, false, true, true)
    val batch = build(values, present)
    Morsel(batch, start = 6, length = 4)

  protected def intColumn(values: Array[Long], present: Array[Boolean]): ColumnBatch =
    val stored = values.map(_.toInt)
    val column =
      Int32Column.of(stored, Validity.pack(stored.length, row => present(row)), stored.length)
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  protected def longColumn(values: Array[Long], present: Array[Boolean]): ColumnBatch =
    val column =
      Int64Column.of(values, Validity.pack(values.length, row => present(row)), values.length)
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  protected def projectInts(calls: AtomicInteger): Int => Int =
    value =>
      calls.incrementAndGet()
      if (value == poison) then throw new IllegalStateException("int kernel saw a null slot")
      value

  protected def keepInts(calls: AtomicInteger): Int => Boolean =
    value =>
      calls.incrementAndGet()
      if (value == poison) then throw new IllegalStateException("int kernel saw a null slot")
      value > 0

  protected def projectStep(
      calls: AtomicInteger,
      fn: Int => Int = value => value + 1,
  ): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Project(countMap(calls, fn))

  protected def filterStep(calls: AtomicInteger): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Filter(keepInts(calls))

  protected def countMap(calls: AtomicInteger, fn: Int => Int): Int => Int =
    value =>
      calls.incrementAndGet()
      if (value == poison) then throw new IllegalStateException("int map saw a null slot")
      fn(value)

  // Row 2 is null before the sample range. Row 4 is null inside it. Rows 6 and 8 are present
  // after it, and row 7 is null after it. Null slots store `poison`.
  protected def sparseBatch: ColumnBatch =
    val values = Array(10, 11, poison, 30, poison, 50, 60, poison, 80)
    val present = Array(true, true, false, true, false, true, true, false, true)
    intColumn(values.map(_.toLong), present)

  protected def sameIntColumn(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    (left.columns.headOption, right.columns.headOption) match
      case (Some(leftColumn: Int32Column), Some(rightColumn: Int32Column)) =>
        leftColumn.length shouldBe rightColumn.length
        leftColumn.values.toSeq shouldBe rightColumn.values.toSeq
        leftColumn.validity.capacity shouldBe rightColumn.validity.capacity
        leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq
      case _ => fail("expected Int32 columns")

  protected def sparseLongBatch: ColumnBatch =
    val values = Array(10L, 11L, poison.toLong, 30L, poison.toLong, 50L, 60L, poison.toLong, 80L)
    val present = Array(true, true, false, true, false, true, true, false, true)
    longColumn(values, present)

  protected val negativeZero = -0.0

  protected val payloadNaN: Double = java.lang.Double.longBitsToDouble(0x7ff0000000000001L)

  protected def bits(value: Double): Long = java.lang.Double.doubleToRawLongBits(value)

  protected def float64(batch: ColumnBatch): Float64Column = batch.columns.headOption match
    case Some(column: Float64Column) => column
    case _ => fail("expected a Float64 column")

  protected def utf8(batch: ColumnBatch): DictUtf8Column = batch.columns.headOption match
    case Some(column: DictUtf8Column) => column
    case _ => fail("expected a Utf8Dict column")

  protected def sparseFloatBatch: ColumnBatch =
    val values = Array(10.0, 11.0, poison.toDouble, 30.0, poison.toDouble, 50.0, 60.0, poison.toDouble, 80.0)
    val present = Array(true, true, false, true, false, true, true, false, true)
    floatColumn(values, present)

  protected def floatBitsBatch: ColumnBatch =
    val values = Array(negativeZero, payloadNaN, poison.toDouble, 30.0)
    val present = Array(true, true, false, true)
    floatColumn(values, present)

  protected def floatColumn(values: Array[Double], present: Array[Boolean]): ColumnBatch =
    val column =
      Float64Column.of(values, Validity.pack(values.length, row => present(row)), values.length)
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  protected def sameFloatColumn(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    (left.columns.headOption, right.columns.headOption) match
      case (Some(leftColumn: Float64Column), Some(rightColumn: Float64Column)) =>
        leftColumn.length shouldBe rightColumn.length
        leftColumn.values.indices.foreach { row =>
          bits(leftColumn.values(row)) shouldBe bits(rightColumn.values(row))
        }
        leftColumn.validity.capacity shouldBe rightColumn.validity.capacity
        leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq
      case _ => fail("expected Float64 columns")

  // Row 1 is an empty string outside the window. Row 4 is an empty string inside it. Rows 2 and 5
  // are nulls outside and inside. Row 7 is a null after the window.
  protected def sparseStringBatch: ColumnBatch =
    val dictionary = Array("out", "", "in", "cc", "zz")
    val codes = Array(0, 1, 0, 2, 1, 0, 3, 0, 4)
    val present = Array(true, true, false, true, true, false, true, false, true)
    stringColumn(dictionary, codes, present)

  protected def stringColumn(
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
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  protected def sameStringColumn(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    val leftColumn = utf8(left)
    val rightColumn = utf8(right)
    leftColumn.length shouldBe rightColumn.length
    leftColumn.dictionary.toSeq shouldBe rightColumn.dictionary.toSeq
    leftColumn.codes.toSeq shouldBe rightColumn.codes.toSeq
    leftColumn.validity.capacity shouldBe rightColumn.validity.capacity
    leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq

  protected def projectFloatStep(
      calls: AtomicInteger,
      fn: Double => Double = value => value + 1.0,
  ): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Project(countFloatMap(calls, fn))

  protected def filterFloatStep(calls: AtomicInteger): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Filter(keepFloats(calls))

  protected def countFloatMap(calls: AtomicInteger, fn: Double => Double): Double => Double =
    value =>
      calls.incrementAndGet()
      if (value == poison.toDouble) then
        throw new IllegalStateException("float map saw a null slot")
      fn(value)

  protected def keepFloats(calls: AtomicInteger): Double => Boolean =
    value =>
      calls.incrementAndGet()
      if (value == poison.toDouble) then
        throw new IllegalStateException("float filter saw a null slot")
      value > 0.0

  protected def projectStringStep(calls: AtomicInteger): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Project(countStringMap(calls))

  protected def filterStringStep(calls: AtomicInteger): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Filter(keepStrings(calls))

  protected def countStringMap(calls: AtomicInteger): String => String =
    value =>
      calls.incrementAndGet()
      value match
        case "out" | "zz" =>
          throw new IllegalStateException("string map saw a row outside the range")
        case other if BatchCodec.isAbsent(other) => BatchCodec.stringElement(None)
        case text => text.toUpperCase(java.util.Locale.ENGLISH)

  protected def keepStrings(calls: AtomicInteger): String => Boolean =
    value =>
      calls.incrementAndGet()
      value match
        case "out" | "zz" | "" => false
        case other => !BatchCodec.isAbsent(other)

  protected def sameLongColumn(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    (left.columns.headOption, right.columns.headOption) match
      case (Some(leftColumn: Int64Column), Some(rightColumn: Int64Column)) =>
        leftColumn.length shouldBe rightColumn.length
        leftColumn.values.toSeq shouldBe rightColumn.values.toSeq
        leftColumn.validity.capacity shouldBe rightColumn.validity.capacity
        leftColumn.validity.words.toSeq shouldBe rightColumn.validity.words.toSeq
      case _ => fail("expected Int64 columns")

  protected def projectLongStep(
      calls: AtomicInteger,
      fn: Long => Long = value => value + 1L,
  ): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Project(countLongMap(calls, fn))

  protected def filterLongStep(calls: AtomicInteger): ColumnarPlanner.ElementStep =
    ColumnarPlanner.ElementStep.Filter(keepLongs(calls))

  protected def countLongMap(calls: AtomicInteger, fn: Long => Long): Long => Long =
    value =>
      calls.incrementAndGet()
      if (value == poison.toLong) then
        throw new IllegalStateException("long map saw a null slot")
      fn(value)

  protected def projectLongs(calls: AtomicInteger): Long => Long =
    value =>
      calls.incrementAndGet()
      if (value == poison.toLong) then
        throw new IllegalStateException("long kernel saw a null slot")
      value

  protected def keepLongs(calls: AtomicInteger): Long => Boolean =
    value =>
      calls.incrementAndGet()
      if (value == poison.toLong) then
        throw new IllegalStateException("long kernel saw a null slot")
      value > 0L

  protected def checkRegroup[A](
      collection: DistCollection[A],
      expected: Seq[Seq[A]],
      visits: Int,
  ): Unit =
    ColumnarPlanner.lower(collection.plan).map(_.shape) shouldBe Some(
      "Pipeline2(Pipeline4(Scan))",
    )
    val ran = ColumnarPlanner.tryRun(collection.plan).getOrElse(fail("chain was refused"))
    ran.map(_.data.toSeq) shouldBe expected
    ran.lift(1).getOrElse(fail("missing regrouped partition")).data shouldBe empty
    val collected = measure(collection.collect().toSeq)
    collected.result shouldBe expected.flatten
    collected.visits shouldBe visits.toLong
    collected.result shouldBe rowCollect(collection)

  protected def rowCollect[A](collection: DistCollection[A]): Seq[A] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = false))
      ScarletRuntime.get.shuffle.clear()
      collection.collect().toSeq
    finally
      ScarletConf.set(saved)
      ScarletRuntime.get.shuffle.clear()

  protected def morselCuts(rows: Int, batchSize: Int): Int =
    ColumnBatches.cuts(rows, batchSize).length

  protected def intSource(partitions: Seq[Seq[Int]]): DistCollection[Int] =
    DistCollection(
      Plan.Source(partitions.map(rows => Partition(rows)), Plan.SourceDescriptor.Ints),
    )

  protected def longSource(partitions: Seq[Seq[Int]]): DistCollection[Long] =
    DistCollection(
      Plan.Source(
        partitions.map(rows => Partition(rows.map(_.toLong))),
        Plan.SourceDescriptor.Longs,
      ),
    )

  protected def sixInts(source: DistCollection[Int]): DistCollection[Int] =
    source.filter(_ > 0).map(_ * 10).map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1)

  protected def sixLongs(source: DistCollection[Long]): DistCollection[Long] =
    source.filter(_ > 0L).map(_ * 10L).map(_ + 1L).map(_ + 1L).map(_ + 1L).map(_ + 1L)

  protected def arm(batchSize: Int, threads: Int = 1): Unit =
    ScarletRuntime.get.shuffle.clear()
    ScarletConf.set(
      originalConf.copy(
        columnarExecution = true,
        columnarBatchSize = batchSize,
        threadPoolSize = threads,
      ),
    )

  protected final case class Footprint[A](result: A, batches: Long, visits: Long)

  protected def measure[A](body: => A): Footprint[A] =
    ScarletRuntime.get.shuffle.clear()
    ColumnMetrics.withSink(new CountingSink) {
      Footprint(body, ColumnMetrics.batchCount, ColumnMetrics.visitCount)
    }
