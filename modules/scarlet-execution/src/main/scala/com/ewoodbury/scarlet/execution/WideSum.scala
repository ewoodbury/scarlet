package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnReduce, WideDecimal}
import com.ewoodbury.scarlet.core.{ColumnKind, Plan, ScarletConf, WideRow}

/**
 * Sum of one long column of a wide row.
 *
 * Null slots are skipped. The total wraps. The add is [[ColumnReduce.sumInt64]], the same kernel a
 * finished long column already uses. A plan that does not lower, or a column that is not a long,
 * returns [[None]] so the action reruns on rows.
 */
private[execution] object WideSum:

  def longColumn(plan: Plan[WideRow], ordinal: Int): Option[Long] =
    lowered(plan, ordinal).flatMap { (op, kind) =>
      kind match
        case ColumnKind.Int64 =>
          Some(ColumnReduce.sumInt64(WideRun.batches(op).toVector, ordinal))
        case _ => None
    }

  /** Ungrouped decimal sum. [[None]] when the plan does not lower or the column is not decimal. */
  def decimalColumn(plan: Plan[WideRow], ordinal: Int): Option[Long] =
    lowered(plan, ordinal).flatMap { (op, kind) =>
      kind match
        case ColumnKind.Decimal(_, _) =>
          Some(WideDecimal.sum(WideRun.batches(op).toVector, ordinal))
        case _ => None
    }

  private def lowered(plan: Plan[WideRow], ordinal: Int): Option[(PhysicalOp, ColumnKind)] =
    if (!ScarletConf.get.columnarExecution) then None
    else
      ColumnarPlanner.lower(plan).flatMap { op =>
        if (op.kind eq PhysicalOp.Kind.Columns) then
          WideLowering.fieldsOf(op).flatMap { fields =>
            if (ordinal >= 0 && ordinal < fields.size) then Some((op, fields(ordinal).kind))
            else None
          }
        else None
      }
