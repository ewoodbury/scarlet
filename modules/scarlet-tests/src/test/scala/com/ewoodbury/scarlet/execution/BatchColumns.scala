package com.ewoodbury.scarlet.execution

import org.scalatest.Assertions

import com.ewoodbury.scarlet.columnar.{ColumnBatch, Int32Column}

/** Int column accessors for the outer sort-merge suites. */
object BatchColumns extends Assertions:

  def int32(batch: ColumnBatch, ordinal: Int): Int32Column =
    batch.columns.lift(ordinal) match
      case Some(column: Int32Column) => column
      case other => fail(s"expected Int32 at $ordinal, got $other")
