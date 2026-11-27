package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.{
  Cell,
  ColumnCompare,
  ColumnField,
  ColumnKind,
  ColumnLiteral,
  CompareOp,
  WideBool,
  WideColumn,
  WideColumns,
  WideFloat32,
  WideFloat64,
  WideInt32,
  WideInt64,
  WideRow,
  WideUtf8,
}

/**
 * Scan, filter, and project of a wide row.
 *
 * Filter uses the existing per-column kernels. A null in the predicate column is dropped and the
 * predicate is not called. A null in another column stays null. Project drops or reorders columns
 * and does not compute. Float32 and string compares are not in the closed set:
 * [[ColumnCompare.allows]] is false for them, and this object refuses the call.
 */
object WideBatch:

  def encode(table: WideColumns): ColumnBatch =
    val columns = table.columns.map(encodeColumn)
    new ColumnBatch(columns.map(_.logicalType), columns, table.length)

  def filter(batch: ColumnBatch, ordinal: Int, compare: ColumnCompare): ColumnBatch =
    val kind = kindOf(batch, ordinal)
    require(
      compare.allows(kind),
      s"$compare does not apply to $kind column $ordinal",
    )
    (kind, compare.literal) match
      case (ColumnKind.Int32, ColumnLiteral.Int32(constant)) =>
        ColumnKernel.filterInt32(batch, ordinal, intPredicate(compare.op, constant))
      case (ColumnKind.Int64, ColumnLiteral.Int64(constant)) =>
        ColumnKernel.filterInt64(batch, ordinal, longPredicate(compare.op, constant))
      case (ColumnKind.Float64, ColumnLiteral.Float64(constant)) =>
        ColumnKernel.filterFloat64(batch, ordinal, doublePredicate(compare.op, constant))
      case (ColumnKind.Bool, ColumnLiteral.Bool(constant)) =>
        ColumnKernel.filterBool(batch, ordinal, boolPredicate(compare.op, constant))
      case _ =>
        throw new IllegalArgumentException(s"no columnar compare for $kind column $ordinal")

  /** Selects columns in `ordinals` order. Names stay with the caller. */
  def project(batch: ColumnBatch, ordinals: Vector[Int]): ColumnBatch =
    require(ordinals.nonEmpty, "project needs a column")
    require(ordinals.distinct.size == ordinals.size, "project ordinals must be unique")
    val columns = ordinals.map(columnAt(batch, _))
    new ColumnBatch(columns.map(_.logicalType), columns, batch.length)

  def decode(batch: ColumnBatch, fields: Vector[ColumnField]): Vector[WideRow] =
    require(
      batch.columns.size == fields.size,
      s"batch width ${batch.columns.size} does not match ${fields.size} fields",
    )
    fields.zipWithIndex.foreach { (field, ordinal) =>
      require(
        sameType(batch.schema(ordinal), field.kind),
        s"column $ordinal is ${batch.schema(ordinal)}, field ${field.name} is ${field.kind}",
      )
    }
    Vector.tabulate(batch.length) { row =>
      WideRow(fields, Vector.tabulate(fields.size)(ordinal => cellAt(batch, fields, ordinal, row)))
    }

  private def encodeColumn(column: WideColumn): Column = column match
    case int32: WideInt32 =>
      Int32Column.of(int32.values.clone(), bits(int32), int32.length)
    case int64: WideInt64 =>
      Int64Column.of(int64.values.clone(), bits(int64), int64.length)
    case float64: WideFloat64 =>
      Float64Column.of(float64.values.clone(), bits(float64), float64.length)
    case float32: WideFloat32 =>
      Float32Column.of(float32.values.clone(), bits(float32), float32.length)
    case bool: WideBool =>
      BoolColumn.of(boolBytes(bool), bits(bool), bool.length)
    case utf8: WideUtf8 =>
      DictUtf8Column.fromNullable(stringCells(utf8))

  private def bits(column: WideColumn): Validity =
    Validity.pack(column.length, row => column.isPresent(row))

  private def stringCells(column: WideUtf8): Vector[Option[String]] =
    Vector.tabulate(column.length) { row =>
      if (column.isPresent(row)) then Some(column.values(row)) else None
    }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def boolBytes(column: WideBool): Array[Byte] =
    val out = new Array[Byte](column.length)
    var row = 0
    while (row < column.length) {
      if (column.isPresent(row) && column.values(row)) then out(row) = 1.toByte
      row += 1
    }
    out

  private def columnAt(batch: ColumnBatch, ordinal: Int): Column =
    require(
      ordinal >= 0 && ordinal < batch.columns.size,
      s"column $ordinal outside width ${batch.columns.size}",
    )
    batch.columns(ordinal)

  private def kindOf(batch: ColumnBatch, ordinal: Int): ColumnKind =
    kindOfType(columnAt(batch, ordinal).logicalType)

  private def kindOfType(logical: LogicalType): ColumnKind = logical match
    case LogicalType.Int32 => ColumnKind.Int32
    case LogicalType.Int64 => ColumnKind.Int64
    case LogicalType.Float64 => ColumnKind.Float64
    case LogicalType.Bool => ColumnKind.Bool
    case LogicalType.Utf8Dict => ColumnKind.Utf8
    case LogicalType.Float32 => ColumnKind.Float32

  private def sameType(logical: LogicalType, kind: ColumnKind): Boolean =
    (logical, kind) match
      case (LogicalType.Int32, ColumnKind.Int32) => true
      case (LogicalType.Int64, ColumnKind.Int64) => true
      case (LogicalType.Float64, ColumnKind.Float64) => true
      case (LogicalType.Bool, ColumnKind.Bool) => true
      case (LogicalType.Utf8Dict, ColumnKind.Utf8) => true
      case (LogicalType.Float32, ColumnKind.Float32) => true
      case _ => false

  private def cellAt(
      batch: ColumnBatch,
      fields: Vector[ColumnField],
      ordinal: Int,
      row: Int,
  ): Cell =
    val column = batch.columns(ordinal)
    if (!column.isValid(row)) then Cell.Null
    else
      (column, fields(ordinal).kind) match
        case (int32: Int32Column, ColumnKind.Int32) => Cell.Int32(int32.values(row))
        case (int64: Int64Column, ColumnKind.Int64) => Cell.Int64(int64.values(row))
        case (float64: Float64Column, ColumnKind.Float64) => Cell.Float64(float64.values(row))
        case (float32: Float32Column, ColumnKind.Float32) => Cell.Float32(float32.values(row))
        case (bool: BoolColumn, ColumnKind.Bool) => Cell.Bool(boolBit(bool.values(row)))
        case (utf8: DictUtf8Column, ColumnKind.Utf8) =>
          Cell.Utf8(utf8.dictionary(utf8.codes(row)))
        case _ =>
          throw new IllegalArgumentException(
            s"column $ordinal is ${column.logicalType}, field is ${fields(ordinal).kind}",
          )

  private def boolBit(byte: Byte): Boolean = byte match
    case 1 => true
    case 0 => false
    case other => throw new IllegalArgumentException(s"bool byte $other is not 0 or 1")

  private def intPredicate(op: CompareOp, constant: Int): ColumnKernel.Int32Predicate = op match
    case CompareOp.Gt => value => value > constant
    case CompareOp.Ge => value => value >= constant
    case CompareOp.Lt => value => value < constant
    case CompareOp.Le => value => value <= constant
    case CompareOp.Eq => value => value == constant
    case CompareOp.Ne => value => value != constant

  private def longPredicate(op: CompareOp, constant: Long): ColumnKernel.Int64Predicate = op match
    case CompareOp.Gt => value => value > constant
    case CompareOp.Ge => value => value >= constant
    case CompareOp.Lt => value => value < constant
    case CompareOp.Le => value => value <= constant
    case CompareOp.Eq => value => value == constant
    case CompareOp.Ne => value => value != constant

  private def doublePredicate(op: CompareOp, constant: Double): ColumnKernel.Float64Predicate =
    op match
      case CompareOp.Gt => value => value > constant
      case CompareOp.Ge => value => value >= constant
      case CompareOp.Lt => value => value < constant
      case CompareOp.Le => value => value <= constant
      case CompareOp.Eq => value => value == constant
      case CompareOp.Ne => value => value != constant

  private def boolPredicate(op: CompareOp, constant: Boolean): ColumnKernel.BoolPredicate =
    op match
      case CompareOp.Eq => value => sameBool(value, constant)
      case CompareOp.Ne => value => !sameBool(value, constant)
      case _ =>
        throw new IllegalArgumentException(s"bool compare does not support $op")

  private def sameBool(left: Boolean, right: Boolean): Boolean = (left, right) match
    case (true, true) | (false, false) => true
    case _ => false
