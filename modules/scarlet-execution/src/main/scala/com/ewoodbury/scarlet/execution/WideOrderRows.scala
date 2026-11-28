package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.*

/**
 * In-memory sort and limit of wide rows. This is the row fold for
 * [[com.ewoodbury.scarlet.columnar.WideOrder]].
 *
 * A null number orders as zero and the row stays. A null string orders as empty and the row stays.
 * The last key keeps input order, and a descending first key does not reverse that order. String
 * order is JVM `compareTo`. Each result is one partition.
 */
private[execution] object WideOrderRows:

  def sort(op: Plan.WideSortOp): Seq[Partition[WideRow]] =
    val parts = ExecutionService.get.executePartitions(op.source)
    val rows = parts.flatMap(_.data).toVector
    val fields = schema(op.source, rows)
    val kind = kindAt(fields, op.ordinal)
    op.second match
      case None =>
        kind match
          case ColumnKind.Int32 | ColumnKind.Int64 =>
            Seq(Partition(ordered(rows, op.ordinal, op.descending)))
          case ColumnKind.Utf8 =>
            Seq(Partition(orderedText(rows, op.ordinal, op.descending)))
          case other =>
            throw new IllegalArgumentException(s"sort column ${op.ordinal} is $other")
      case Some(next) =>
        val secondKind = kindAt(fields, next)
        Seq(Partition(orderedPair(rows, op.ordinal, op.descending, kind, next, secondKind)))

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

  private def orderedText(
      rows: Vector[WideRow],
      ordinal: Int,
      descending: Boolean,
  ): Vector[WideRow] =
    val ordering =
      if descending then Ordering.Tuple2(Ordering.String.reverse, Ordering.Int)
      else Ordering.Tuple2(Ordering.String, Ordering.Int)
    rows.zipWithIndex
      .sortBy((row, index) => (textKey(row, ordinal), index))(ordering)
      .map((row, _) => row)

  private def orderedPair(
      rows: Vector[WideRow],
      ordinal: Int,
      descending: Boolean,
      kind: ColumnKind,
      second: Int,
      secondKind: ColumnKind,
  ): Vector[WideRow] =
    rows.zipWithIndex
      .sortWith { case ((left, leftAt), (right, rightAt)) =>
        val primary = cellOrder(left, right, ordinal, descending, kind)
        if primary != 0 then primary < 0
        else
          val next = cellOrder(left, right, second, descending = false, secondKind)
          if next != 0 then next < 0 else leftAt < rightAt
      }
      .map((row, _) => row)

  private def cellOrder(
      left: WideRow,
      right: WideRow,
      ordinal: Int,
      descending: Boolean,
      kind: ColumnKind,
  ): Int =
    val compared = kind match
      case ColumnKind.Int32 | ColumnKind.Int64 =>
        java.lang.Long.compare(keyOf(left, ordinal), keyOf(right, ordinal))
      case ColumnKind.Utf8 =>
        textKey(left, ordinal).compareTo(textKey(right, ordinal))
      case other =>
        throw new IllegalArgumentException(s"sort column $ordinal is $other")
    if descending then -compared else compared

  private def kindAt(fields: Vector[ColumnField], ordinal: Int): ColumnKind =
    fields.lift(ordinal).map(_.kind).getOrElse {
      throw new IllegalArgumentException(s"column $ordinal outside width ${fields.size}")
    }

  private def textKey(row: WideRow, ordinal: Int): String = row.cells.lift(ordinal) match
    case Some(Cell.Utf8(value)) => value
    case Some(Cell.Null) => ""
    case Some(other) =>
      throw new IllegalArgumentException(s"sort cell is $other")
    case None =>
      throw new IllegalArgumentException(s"column $ordinal outside width ${row.cells.size}")

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
