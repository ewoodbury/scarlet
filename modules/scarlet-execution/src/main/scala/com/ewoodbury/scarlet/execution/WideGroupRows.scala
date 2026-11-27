package com.ewoodbury.scarlet.execution

import scala.collection.mutable

import com.ewoodbury.scarlet.core.*

/**
 * In-memory group of wide rows. This is the row fold for
 * [[com.ewoodbury.scarlet.columnar.WideGroup]].
 *
 * A null key is dropped. A null measure increments count and is skipped by sum, min, and max. A
 * key whose measures are all null still emits a row. Int sums widen to long. Long sums wrap. The
 * plan is not `GroupByKey`, and this fold does not build a list of values.
 */
private[execution] object WideGroupRows:

  def partitions(group: Plan.WideGroupOp): Seq[Partition[WideRow]] =
    val (fields, rows) = materialize(group.source)
    val out = WideAggregate.output(fields, group.key, group.measure).getOrElse {
      throw new IllegalArgumentException(
        s"columns ${group.key} and ${group.measure} of ${fields.map(_.kind)} do not group",
      )
    }
    Seq(Partition(fold(rows, group.key, group.measure, fields(group.measure).kind, out)))

  private def materialize(plan: Plan[WideRow]): (Vector[ColumnField], Vector[WideRow]) =
    val parts = ExecutionService.get.executePartitions(plan)
    val fromColumns = parts.iterator.map(_.data).collectFirst { case columns: WideColumns =>
      columns.fields
    }
    val rows = parts.flatMap(_.data).toVector
    val fields = fromColumns
      .orElse(WideFields.known(plan))
      .orElse(rows.headOption.map(_.fields))
      .getOrElse(throw new IllegalArgumentException("wide group has no schema"))
    (fields, rows)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class Acc:
    var count: Long = 0L
    var seen: Boolean = false
    var longSum: Long = 0L
    var doubleSum: Double = 0.0
    var intMin: Int = 0
    var intMax: Int = 0
    var longMin: Long = 0L
    var longMax: Long = 0L
    var doubleMin: Double = 0.0
    var doubleMax: Double = 0.0
    var floatMin: Float = 0.0f
    var floatMax: Float = 0.0f

    def addInt(value: Int): Unit =
      if (!seen) then
        seen = true
        intMin = value
        intMax = value
        longSum = value.toLong
      else
        if (value < intMin) then intMin = value
        if (value > intMax) then intMax = value
        longSum += value.toLong

    def addLong(value: Long): Unit =
      if (!seen) then
        seen = true
        longMin = value
        longMax = value
        longSum = value
      else
        if (value < longMin) then longMin = value
        if (value > longMax) then longMax = value
        longSum += value

    def addDouble(value: Double): Unit =
      if (!seen) then
        seen = true
        doubleMin = value
        doubleMax = value
        doubleSum = value
      else
        if (value < doubleMin) then doubleMin = value
        if (value > doubleMax) then doubleMax = value
        doubleSum += value

    def addFloat(value: Float): Unit =
      if (!seen) then
        seen = true
        floatMin = value
        floatMax = value
        doubleSum = value.toDouble
      else
        if (value < floatMin) then floatMin = value
        if (value > floatMax) then floatMax = value
        doubleSum += value.toDouble

  private def fold(
      rows: Vector[WideRow],
      key: Int,
      measure: Int,
      measureKind: ColumnKind,
      fields: Vector[ColumnField],
  ): Vector[WideRow] =
    val groups = mutable.LinkedHashMap.empty[Cell, Acc]
    rows.foreach { row =>
      cellAt(row, key) match
        case Cell.Null => ()
        case keyCell =>
          val acc = groups.getOrElseUpdate(keyCell, new Acc)
          acc.count += 1L
          cellAt(row, measure) match
            case Cell.Null => ()
            case Cell.Int32(value) => acc.addInt(value)
            case Cell.Int64(value) => acc.addLong(value)
            case Cell.Float64(value) => acc.addDouble(value)
            case Cell.Float32(value) => acc.addFloat(value)
            case other =>
              throw new IllegalArgumentException(s"measure $other does not aggregate")
    }
    groups.iterator.map { (keyCell, acc) =>
      WideRow(fields, Vector(keyCell, Cell.Int64(acc.count)) ++ aggregateCells(acc, measureKind))
    }.toVector

  private def aggregateCells(acc: Acc, measureKind: ColumnKind): Vector[Cell] =
    if (!acc.seen) then Vector(Cell.Null, Cell.Null, Cell.Null)
    else
      measureKind match
        case ColumnKind.Int32 =>
          Vector(Cell.Int64(acc.longSum), Cell.Int32(acc.intMin), Cell.Int32(acc.intMax))
        case ColumnKind.Int64 =>
          Vector(Cell.Int64(acc.longSum), Cell.Int64(acc.longMin), Cell.Int64(acc.longMax))
        case ColumnKind.Float64 =>
          Vector(
            Cell.Float64(acc.doubleSum),
            Cell.Float64(acc.doubleMin),
            Cell.Float64(acc.doubleMax),
          )
        case ColumnKind.Float32 =>
          Vector(
            Cell.Float64(acc.doubleSum),
            Cell.Float32(acc.floatMin),
            Cell.Float32(acc.floatMax),
          )
        case other =>
          throw new IllegalArgumentException(s"measure $other does not aggregate")

  private def cellAt(row: WideRow, ordinal: Int): Cell =
    if (ordinal >= 0 && ordinal < row.cells.size) then row.cells(ordinal)
    else throw new IllegalArgumentException(s"column $ordinal outside width ${row.cells.size}")
