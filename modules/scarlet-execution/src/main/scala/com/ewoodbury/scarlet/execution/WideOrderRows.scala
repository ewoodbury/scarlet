package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.*

/**
 * In-memory sort and limit of wide rows. This is the row fold for
 * [[com.ewoodbury.scarlet.columnar.WideOrder]].
 *
 * A null sort cell orders as zero and the row stays. Ties keep input order, and a descending sort
 * does not reverse them. Each result is one partition.
 */
private[execution] object WideOrderRows:

  def sort(op: Plan.WideSortOp): Seq[Partition[WideRow]] =
    val parts = ExecutionService.get.executePartitions(op.source)
    val rows = parts.flatMap(_.data).toVector
    val fields = schema(op.source, rows)
    val kind = fields.lift(op.ordinal).map(_.kind).getOrElse {
      throw new IllegalArgumentException(
        s"column ${op.ordinal} outside width ${fields.size}",
      )
    }
    kind match
      case ColumnKind.Int32 | ColumnKind.Int64 => ()
      case other =>
        throw new IllegalArgumentException(s"sort column ${op.ordinal} is $other")
    Seq(Partition(ordered(rows, op.ordinal, op.descending)))

  def limit(op: Plan.WideLimitOp): Seq[Partition[WideRow]] =
    require(op.count >= 0, "limit must not be negative")
    val parts = ExecutionService.get.executePartitions(op.source)
    val kept = parts.iterator.flatMap(_.data.iterator).take(op.count).toVector
    Seq(Partition(kept))

  private def ordered(
      rows: Vector[WideRow],
      ordinal: Int,
      descending: Boolean,
  ): Vector[WideRow] =
    val ordering =
      if descending then Ordering.Tuple2(Ordering.Long.reverse, Ordering.Int)
      else Ordering.Tuple2(Ordering.Long, Ordering.Int)
    rows.zipWithIndex
      .sortBy((row, index) => (keyOf(row, ordinal), index))(ordering)
      .map((row, _) => row)

  private def keyOf(row: WideRow, ordinal: Int): Long =
    row.cells.lift(ordinal) match
      case Some(Cell.Int32(value)) => value.toLong
      case Some(Cell.Int64(value)) => value
      case Some(Cell.Null) => 0L
      case Some(other) =>
        throw new IllegalArgumentException(s"sort cell is $other")
      case None =>
        throw new IllegalArgumentException(s"column $ordinal outside width ${row.cells.size}")

  private def schema(plan: Plan[WideRow], rows: Vector[WideRow]): Vector[ColumnField] =
    WideFields.known(plan).orElse(rows.headOption.map(_.fields)).getOrElse {
      throw new IllegalArgumentException("wide sort has no schema")
    }
