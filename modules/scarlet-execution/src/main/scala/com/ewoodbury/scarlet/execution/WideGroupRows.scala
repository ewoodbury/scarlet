package com.ewoodbury.scarlet.execution

import scala.collection.mutable

import com.ewoodbury.scarlet.core.*

/**
 * In-memory group of wide rows. This is the row fold for
 * [[com.ewoodbury.scarlet.columnar.WideGroup]].
 *
 * A null key is dropped. A null measure increments count and is skipped by sum, min, and max. A
 * key whose measures are all null still emits a row. A string measure emits count, min, and max,
 * and has no sum. `textMin` appends the min of one other string. String order is JVM `compareTo`.
 * `sum2` appends the sum of one other int or long. `distinct` appends the size of that key's set
 * after those values. Int sums widen to long. Long sums wrap. The plan is not `GroupByKey`, and
 * this fold does not build a list of values.
 */
private[execution] object WideGroupRows:

  def partitions(group: Plan.WideGroupOp): Seq[Partition[WideRow]] =
    val (fields, rows) = materialize(group.source)
    WideAggregate.checkSum(fields, group.key, group.measure, group.key2, group.sum2)
    val out =
      WideAggregate
        .output(
          fields,
          group.key,
          group.measure,
          group.key2,
          group.distinct,
          group.textMin,
          group.sum2,
        )
        .getOrElse {
          throw new IllegalArgumentException(
            s"columns ${group.key}, ${group.key2}, and ${group.measure} of ${fields.map(_.kind)} do not group",
          )
        }
    val folded = group.key2 match
      case None =>
        fold(
          rows,
          group.key,
          group.measure,
          fields(group.measure).kind,
          out,
          group.distinct,
          group.textMin,
          group.sum2,
        )
      case Some(second) =>
        foldTwo(
          rows,
          group.key,
          second,
          group.measure,
          fields(group.measure).kind,
          out,
          group.distinct,
          group.textMin,
          group.sum2,
        )
    Seq(Partition(folded))

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

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Var",
      "org.wartremover.warts.MutableDataStructures",
    ),
  )
  private final class Acc(trackDistinct: Boolean):
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
    var textMin: String = ""
    var textMax: String = ""
    var secondSeen: Boolean = false
    var secondText: String = ""
    var extraSum: Long = 0L
    private val distinctValues: Option[mutable.HashSet[Cell]] =
      if trackDistinct then Some(mutable.HashSet.empty[Cell]) else None

    def noteDistinct(cell: Cell): Unit =
      distinctValues.foreach { values =>
        cell match
          case Cell.Null => ()
          case Cell.Int32(_) | Cell.Int64(_) | Cell.Utf8(_) =>
            val _ = values.add(cell)
          case Cell.Float64(_) =>
            throw new IllegalArgumentException("distinct of Float64 is not supported")
          case Cell.Float32(_) =>
            throw new IllegalArgumentException("distinct of Float32 is not supported")
          case Cell.Bool(_) =>
            throw new IllegalArgumentException("distinct of Bool is not supported")
          case Cell.Decimal(_, _, _) =>
            throw new IllegalArgumentException("distinct of Decimal is not supported")
      }

    def distinctCount: Option[Long] = distinctValues.map(seen => seen.size.toLong)

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

    def addText(value: String): Unit =
      if (!seen) then
        seen = true
        textMin = value
        textMax = value
      else
        if (value.compareTo(textMin) < 0) then textMin = value
        if (value.compareTo(textMax) > 0) then textMax = value

    def addSecond(value: String): Unit =
      if (!secondSeen) then
        secondSeen = true
        secondText = value
      else if (value.compareTo(secondText) < 0) then secondText = value

    def addExtra(value: Long): Unit = extraSum += value

  private def fold(
      rows: Vector[WideRow],
      key: Int,
      measure: Int,
      measureKind: ColumnKind,
      fields: Vector[ColumnField],
      distinct: Option[Int],
      second: Option[Int],
      sum2: Option[Int],
  ): Vector[WideRow] =
    val groups = mutable.LinkedHashMap.empty[Cell, Acc]
    rows.foreach { row =>
      cellAt(row, key) match
        case Cell.Null => ()
        case keyCell =>
          val acc = groups.getOrElseUpdate(keyCell, new Acc(distinct.isDefined))
          acc.count += 1L
          addMeasure(acc, cellAt(row, measure))
          noteSecond(acc, row, second)
          noteSum(acc, row, sum2)
          distinct.foreach(ordinal => acc.noteDistinct(cellAt(row, ordinal)))
    }
    groups.iterator.map { (keyCell, acc) =>
      finished(
        fields,
        Vector(keyCell, Cell.Int64(acc.count)) ++ aggregateCells(acc, measureKind, second, sum2),
        acc,
      )
    }.toVector

  private def foldTwo(
      rows: Vector[WideRow],
      key: Int,
      key2: Int,
      measure: Int,
      measureKind: ColumnKind,
      fields: Vector[ColumnField],
      distinct: Option[Int],
      second: Option[Int],
      sum2: Option[Int],
  ): Vector[WideRow] =
    val groups = mutable.LinkedHashMap.empty[(Cell, Cell), Acc]
    rows.foreach { row =>
      (keptKey(row, key), keptKey(row, key2)) match
        case (Some(left), Some(right)) =>
          val acc = groups.getOrElseUpdate((left, right), new Acc(distinct.isDefined))
          acc.count += 1L
          addMeasure(acc, cellAt(row, measure))
          noteSecond(acc, row, second)
          noteSum(acc, row, sum2)
          distinct.foreach(ordinal => acc.noteDistinct(cellAt(row, ordinal)))
        case _ => ()
    }
    groups.iterator.map { case ((left, right), acc) =>
      finished(
        fields,
        Vector(left, right, Cell.Int64(acc.count)) ++ aggregateCells(
          acc,
          measureKind,
          second,
          sum2,
        ),
        acc,
      )
    }.toVector

  private def finished(fields: Vector[ColumnField], cells: Vector[Cell], acc: Acc): WideRow =
    acc.distinctCount match
      case Some(count) => WideRow(fields, cells :+ Cell.Int64(count))
      case None => WideRow(fields, cells)

  private def keptKey(row: WideRow, ordinal: Int): Option[Cell] =
    cellAt(row, ordinal) match
      case Cell.Null => None
      case value @ (Cell.Int32(_) | Cell.Int64(_) | Cell.Utf8(_)) => Some(value)
      case other =>
        throw new IllegalArgumentException(s"group key $other is not int, long, or string")

  private def addMeasure(acc: Acc, cell: Cell): Unit = cell match
    case Cell.Null => ()
    case Cell.Int32(value) => acc.addInt(value)
    case Cell.Int64(value) => acc.addLong(value)
    case Cell.Float64(value) => acc.addDouble(value)
    case Cell.Float32(value) => acc.addFloat(value)
    case Cell.Utf8(value) => acc.addText(value)
    case other =>
      throw new IllegalArgumentException(s"measure $other does not aggregate")

  private def noteSecond(acc: Acc, row: WideRow, second: Option[Int]): Unit =
    second.foreach { ordinal =>
      cellAt(row, ordinal) match
        case Cell.Null => ()
        case Cell.Utf8(value) => acc.addSecond(value)
        case other =>
          throw new IllegalArgumentException(s"measure $other does not aggregate")
    }

  private def noteSum(acc: Acc, row: WideRow, sum2: Option[Int]): Unit =
    sum2.foreach { ordinal =>
      cellAt(row, ordinal) match
        case Cell.Null => ()
        case Cell.Int32(value) => acc.addExtra(value.toLong)
        case Cell.Int64(value) => acc.addExtra(value)
        case other =>
          throw new IllegalArgumentException(s"measure $other does not aggregate")
    }

  private def aggregateCells(
      acc: Acc,
      measureKind: ColumnKind,
      second: Option[Int],
      sum2: Option[Int],
  ): Vector[Cell] =
    val primary = measureKind match
      case ColumnKind.Utf8 =>
        if (!acc.seen) then Vector(Cell.Null, Cell.Null)
        else Vector(Cell.Utf8(acc.textMin), Cell.Utf8(acc.textMax))
      case _ if !acc.seen => Vector(Cell.Null, Cell.Null, Cell.Null)
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
    val extra = second match
      case None => Vector.empty
      case Some(_) if !acc.secondSeen => Vector(Cell.Null)
      case Some(_) => Vector(Cell.Utf8(acc.secondText))
    val added = sum2 match
      case None => Vector.empty
      case Some(_) => Vector(Cell.Int64(acc.extraSum))
    primary ++ extra ++ added

  private def cellAt(row: WideRow, ordinal: Int): Cell =
    if (ordinal >= 0 && ordinal < row.cells.size) then row.cells(ordinal)
    else throw new IllegalArgumentException(s"column $ordinal outside width ${row.cells.size}")
