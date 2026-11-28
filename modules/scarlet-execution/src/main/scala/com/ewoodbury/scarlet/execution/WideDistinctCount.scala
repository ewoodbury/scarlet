package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.WideDistinct
import com.ewoodbury.scarlet.core.{ColumnKind, Plan, ScarletConf, WideRow}

/**
 * Exact distinct count of one int, long, or string column.
 *
 * Nulls are skipped. An empty string counts. A plan that does not lower, or a column that is not
 * an int, a long, or a string, returns [[None]] so the action reruns on rows.
 */
private[execution] object WideDistinctCount:

  def count(plan: Plan[WideRow], ordinal: Int): Option[Long] =
    lowered(plan, ordinal).map { (op, kind) =>
      WideDistinct.count(WideRun.batches(op), ordinal, kind)
    }

  private def lowered(plan: Plan[WideRow], ordinal: Int): Option[(PhysicalOp, ColumnKind)] =
    if (!ScarletConf.get.columnarExecution) then None
    else
      ColumnarPlanner.lower(plan).flatMap { op =>
        if (op.kind eq PhysicalOp.Kind.Columns) then
          WideLowering.fieldsOf(op).flatMap { fields =>
            if (ordinal >= 0 && ordinal < fields.size) then
              fields(ordinal).kind match
                case ColumnKind.Int32 | ColumnKind.Int64 | ColumnKind.Utf8 =>
                  Some((op, fields(ordinal).kind))
                case _ => None
            else None
          }
        else None
      }
