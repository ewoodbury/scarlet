package com.ewoodbury.scarlet.execution

import scala.reflect.ClassTag

import com.ewoodbury.scarlet.core.*

/**
 * Row-path filter and compute above a join.
 *
 * The stage builder runs a filter of a scan on [[WideColumns]]. A join's row oracle emits
 * [[WideRow]] values, and a later filter cannot stage that join. This packs those rows, runs the
 * same column kernel, and puts the original names back. Duplicate names are legal on a joined row,
 * so the packed columns use a fresh name per ordinal.
 */
private[execution] object WideStepRows:

  def filter(plan: Plan.WideFilterOp): Seq[Partition[WideRow]] =
    val fields = schema(plan.source, "wide filter")
    val kind = fields.lift(plan.ordinal).map(_.kind).getOrElse {
      throw new IllegalArgumentException(
        s"column ${plan.ordinal} outside width ${fields.size}",
      )
    }
    if (!plan.compare.allows(kind)) then
      throw new IllegalArgumentException(
        s"${plan.compare} does not apply to $kind column ${fields(plan.ordinal).name}",
      )
    mapParts(plan.source, fields, fields, columns => columns.filter(plan.ordinal, plan.compare))

  def compute(plan: Plan.WideComputeOp): Seq[Partition[WideRow]] =
    val fields = schema(plan.source, "wide compute")
    val kind = ColumnExpr.checked(plan.expr, fields, plan.name)
    mapParts(
      plan.source,
      fields,
      fields :+ ColumnField(plan.name, kind),
      columns => columns.compute(plan.expr, plan.name),
    )

  private def mapParts(
      source: Plan[WideRow],
      sourceFields: Vector[ColumnField],
      outFields: Vector[ColumnField],
      step: WideColumns => WideColumns,
  ): Seq[Partition[WideRow]] =
    ExecutionService.get.executePartitions(source).map { part =>
      val columns = part.data match
        case existing: WideColumns => existing
        case other => pack(other.iterator.toVector, sourceFields)
      Partition(relabel(step(columns).rows, outFields))
    }

  private def schema(plan: Plan[WideRow], what: String): Vector[ColumnField] =
    WideFields.known(plan).getOrElse {
      throw new IllegalArgumentException(s"$what has no schema")
    }

  private def relabel(rows: Vector[WideRow], fields: Vector[ColumnField]): Vector[WideRow] =
    rows.map { row =>
      if (row.cells.size != fields.size) then
        throw new IllegalArgumentException(
          s"width ${row.cells.size} does not match schema ${fields.size}",
        )
      WideRow(fields, row.cells)
    }

  private def pack(rows: Vector[WideRow], fields: Vector[ColumnField]): WideColumns =
    val inputs = fields.zipWithIndex.map { (field, ordinal) =>
      val cells = rows.map { row =>
        row.cells.lift(ordinal).getOrElse {
          throw new IllegalArgumentException(
            s"column $ordinal outside width ${row.cells.size}",
          )
        }
      }
      input(field.copy(name = s"c$ordinal"), cells)
    }
    WideColumns(inputs)

  private def input(field: ColumnField, cells: Vector[Cell]): WideInput =
    val present = Array.ofDim[Boolean](cells.length)
    def fill[A: ClassTag](read: Cell => Option[A], empty: A): Array[A] =
      val values = new Array[A](cells.length)
      cells.iterator.zipWithIndex.foreach { (cell, row) =>
        read(cell) match
          case Some(value) =>
            present(row) = true
            values(row) = value
          case None =>
            cell match
              case Cell.Null => values(row) = empty
              case other => refuse(field, other)
      }
      values

    field.kind match
      case ColumnKind.Int32 =>
        WideInput.nullableInts(field.name, fill(intCell, 0), present)
      case ColumnKind.Int64 =>
        WideInput.nullableLongs(field.name, fill(longCell, 0L), present)
      case ColumnKind.Float64 =>
        WideInput.nullableDoubles(field.name, fill(doubleCell, 0.0), present)
      case ColumnKind.Float32 =>
        WideInput.nullableFloats(field.name, fill(floatCell, 0.0f), present)
      case ColumnKind.Bool =>
        WideInput.nullableBools(field.name, fill(boolCell, false), present)
      case ColumnKind.Utf8 =>
        WideInput.nullableStrings(field.name, fill(textCell, ""), present)
      case ColumnKind.Decimal(precision, scale) =>
        WideInput.nullableDecimals(field.name, precision, scale, fill(decimalCell, 0L), present)

  private def intCell(cell: Cell): Option[Int] = cell match
    case Cell.Int32(value) => Some(value)
    case _ => None

  private def longCell(cell: Cell): Option[Long] = cell match
    case Cell.Int64(value) => Some(value)
    case _ => None

  private def doubleCell(cell: Cell): Option[Double] = cell match
    case Cell.Float64(value) => Some(value)
    case _ => None

  private def floatCell(cell: Cell): Option[Float] = cell match
    case Cell.Float32(value) => Some(value)
    case _ => None

  private def boolCell(cell: Cell): Option[Boolean] = cell match
    case Cell.Bool(value) => Some(value)
    case _ => None

  private def textCell(cell: Cell): Option[String] = cell match
    case Cell.Utf8(value) => Some(value)
    case _ => None

  private def decimalCell(cell: Cell): Option[Long] = cell match
    case Cell.Decimal(_, _, value) => Some(value)
    case _ => None

  private def refuse(field: ColumnField, cell: Cell): Nothing =
    throw new IllegalArgumentException(s"${field.name} is $cell, expected ${field.kind}")
