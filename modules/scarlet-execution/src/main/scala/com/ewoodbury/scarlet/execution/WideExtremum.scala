package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnBatch, ColumnExtremum}
import com.ewoodbury.scarlet.core.{ColumnKind, Plan, ScarletConf, WideRow}

/**
 * Minimum or maximum of one numeric column of a wide row.
 *
 * Null slots are skipped. An empty column, or a column of only nulls, is zero. A plan that does
 * not lower, or a column of another kind, returns [[None]] so the action reruns on rows.
 */
private[execution] object WideExtremum:

  def minInt(plan: Plan[WideRow], ordinal: Int): Option[Int] =
    batches(plan, ordinal, ColumnKind.Int32).map(rows => ColumnExtremum.minInt32(rows, ordinal))

  def maxInt(plan: Plan[WideRow], ordinal: Int): Option[Int] =
    batches(plan, ordinal, ColumnKind.Int32).map(rows => ColumnExtremum.maxInt32(rows, ordinal))

  def minLong(plan: Plan[WideRow], ordinal: Int): Option[Long] =
    batches(plan, ordinal, ColumnKind.Int64).map(rows => ColumnExtremum.minInt64(rows, ordinal))

  def maxLong(plan: Plan[WideRow], ordinal: Int): Option[Long] =
    batches(plan, ordinal, ColumnKind.Int64).map(rows => ColumnExtremum.maxInt64(rows, ordinal))

  def minFloat(plan: Plan[WideRow], ordinal: Int): Option[Float] =
    batches(plan, ordinal, ColumnKind.Float32).map(rows =>
      ColumnExtremum.minFloat32(rows, ordinal),
    )

  def maxFloat(plan: Plan[WideRow], ordinal: Int): Option[Float] =
    batches(plan, ordinal, ColumnKind.Float32).map(rows =>
      ColumnExtremum.maxFloat32(rows, ordinal),
    )

  def minDouble(plan: Plan[WideRow], ordinal: Int): Option[Double] =
    batches(plan, ordinal, ColumnKind.Float64).map(rows =>
      ColumnExtremum.minFloat64(rows, ordinal),
    )

  def maxDouble(plan: Plan[WideRow], ordinal: Int): Option[Double] =
    batches(plan, ordinal, ColumnKind.Float64).map(rows =>
      ColumnExtremum.maxFloat64(rows, ordinal),
    )

  private def batches(
      plan: Plan[WideRow],
      ordinal: Int,
      kind: ColumnKind,
  ): Option[Vector[ColumnBatch]] =
    if (!ScarletConf.get.columnarExecution) then None
    else
      ColumnarPlanner.lower(plan).flatMap { op =>
        if (op.kind eq PhysicalOp.Kind.Columns) then
          WideLowering.fieldsOf(op).flatMap { fields =>
            if (ordinal >= 0 && ordinal < fields.size && (fields(ordinal).kind == kind)) then
              Some(WideRun.batches(op).toVector)
            else None
          }
        else None
      }
