package com.ewoodbury.scarlet.columnar

import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers

/** Batch accessors shared by the column-kernel suites. */
trait ColumnKernelFixture extends Matchers with Assertions:

  protected def single(column: Column): ColumnBatch =
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  protected def utf8(batch: ColumnBatch): DictUtf8Column = utf8At(batch, 0)

  protected def int32(batch: ColumnBatch): Int32Column =
    at(batch, 0) match
      case column: Int32Column => column
      case other => fail(s"expected Int32, got $other")

  protected def long64(batch: ColumnBatch): Int64Column =
    at(batch, 0) match
      case column: Int64Column => column
      case other => fail(s"expected Int64, got $other")

  protected def nullableLongs(values: Array[Long], present: Array[Boolean]): ColumnBatch =
    val column =
      Int64Column.of(values, Validity.pack(values.length, row => present(row)), values.length)
    single(column)

  protected def sameLongBatch(left: ColumnBatch, right: ColumnBatch): Unit =
    left.length shouldBe right.length
    left.schema shouldBe right.schema
    long64(left).length shouldBe long64(right).length
    long64(left).values.toSeq shouldBe long64(right).values.toSeq
    long64(left).validity.capacity shouldBe long64(right).validity.capacity
    long64(left).validity.words.toSeq shouldBe long64(right).validity.words.toSeq

  protected def utf8At(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    at(batch, ordinal) match
      case column: DictUtf8Column => column
      case other => fail(s"expected Utf8Dict, got $other")

  protected def at(batch: ColumnBatch, ordinal: Int): Column =
    batch.columns.lift(ordinal).getOrElse(fail(s"missing column $ordinal"))
