package com.ewoodbury.scarlet.core

/**
 * Row-path evaluation of one [[ColumnExpr]].
 *
 * A null input yields a null result. Arithmetic uses [[ColumnExpr.wider]]. A decimal add,
 * subtract, or multiply stays exact. The caller has already rejected an expression the closed set
 * does not name.
 */
private[scarlet] object WideCompute:

  def column(
      columns: Vector[WideColumn],
      expr: ColumnExpr,
      field: ColumnField,
  ): WideColumn =
    val length = columns(0).length
    field.kind match
      case ColumnKind.Int32 => ints(columns, expr, field, length)
      case ColumnKind.Int64 => longs(columns, expr, field, length)
      case ColumnKind.Float32 => floats(columns, expr, field, length)
      case ColumnKind.Float64 => doubles(columns, expr, field, length)
      case ColumnKind.Bool => bools(columns, expr, field, length)
      case ColumnKind.Utf8 =>
        throw new IllegalArgumentException(s"${field.name} cannot be a string")
      case ColumnKind.Decimal(_, _) =>
        decimals(columns, expr, field, length)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def ints(
      columns: Vector[WideColumn],
      expr: ColumnExpr,
      field: ColumnField,
      length: Int,
  ): WideColumn =
    val values = new Array[Int](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      eval(columns, expr, field.kind, row) match
        case Cell.Int32(value) =>
          present(row) = true
          values(row) = value
        case Cell.Null => ()
        case other => reject(field, other)
      row += 1
    }
    new WideInt32(field, values, present)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def longs(
      columns: Vector[WideColumn],
      expr: ColumnExpr,
      field: ColumnField,
      length: Int,
  ): WideColumn =
    val values = new Array[Long](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      eval(columns, expr, field.kind, row) match
        case Cell.Int64(value) =>
          present(row) = true
          values(row) = value
        case Cell.Null => ()
        case other => reject(field, other)
      row += 1
    }
    new WideInt64(field, values, present)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def decimals(
      columns: Vector[WideColumn],
      expr: ColumnExpr,
      field: ColumnField,
      length: Int,
  ): WideColumn =
    val values = new Array[Long](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      eval(columns, expr, field.kind, row) match
        case Cell.Decimal(precision, scale, value) =>
          if (field.kind != ColumnKind.Decimal(precision, scale)) then
            reject(field, Cell.Decimal(precision, scale, value))
          present(row) = true
          values(row) = value
        case Cell.Null => ()
        case other => reject(field, other)
      row += 1
    }
    new WideInt64(field, values, present)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def floats(
      columns: Vector[WideColumn],
      expr: ColumnExpr,
      field: ColumnField,
      length: Int,
  ): WideColumn =
    val values = new Array[Float](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      eval(columns, expr, field.kind, row) match
        case Cell.Float32(value) =>
          present(row) = true
          values(row) = value
        case Cell.Null => ()
        case other => reject(field, other)
      row += 1
    }
    new WideFloat32(field, values, present)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def doubles(
      columns: Vector[WideColumn],
      expr: ColumnExpr,
      field: ColumnField,
      length: Int,
  ): WideColumn =
    val values = new Array[Double](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      eval(columns, expr, field.kind, row) match
        case Cell.Float64(value) =>
          present(row) = true
          values(row) = value
        case Cell.Null => ()
        case other => reject(field, other)
      row += 1
    }
    new WideFloat64(field, values, present)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def bools(
      columns: Vector[WideColumn],
      expr: ColumnExpr,
      field: ColumnField,
      length: Int,
  ): WideColumn =
    val values = new Array[Boolean](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      eval(columns, expr, field.kind, row) match
        case Cell.Bool(value) =>
          present(row) = true
          values(row) = value
        case Cell.Null => ()
        case other => reject(field, other)
      row += 1
    }
    new WideBool(field, values, present)

  private def eval(
      columns: Vector[WideColumn],
      expr: ColumnExpr,
      kind: ColumnKind,
      row: Int,
  ): Cell =
    if (!ready(columns, expr, row)) then Cell.Null
    else
      expr match
        case ColumnExpr.Add(left, right) =>
          kind match
            case ColumnKind.Decimal(_, _) =>
              decimalCell(columns, left, right, row, DecimalRules.add)
            case _ =>
              binary(kind, columns(left), columns(right), row, _ + _, _ + _, _ + _, _ + _)
        case ColumnExpr.Sub(left, right) =>
          kind match
            case ColumnKind.Decimal(_, _) =>
              decimalCell(columns, left, right, row, DecimalRules.subtract)
            case _ =>
              binary(kind, columns(left), columns(right), row, _ - _, _ - _, _ - _, _ - _)
        case ColumnExpr.Mul(left, right) =>
          kind match
            case ColumnKind.Decimal(_, _) =>
              decimalCell(columns, left, right, row, DecimalRules.multiply)
            case _ =>
              binary(kind, columns(left), columns(right), row, _ * _, _ * _, _ * _, _ * _)
        case ColumnExpr.Div(left, right) =>
          binary(kind, columns(left), columns(right), row, divInt, divLong, divFloat, divDouble)
        case ColumnExpr.Lit(value) =>
          literal(value)
        case ColumnExpr.Compare(op, left, right) =>
          (columns(left).field.kind, columns(right).field.kind) match
            case (ColumnKind.Decimal(_, leftScale), ColumnKind.Decimal(_, rightScale)) =>
              val (leftValue, _, _) = decimalAt(columns(left), row)
              val (rightValue, _, _) = decimalAt(columns(right), row)
              val order = DecimalRules.compare(leftValue, leftScale, rightValue, rightScale)
              Cell.Bool(decimalOrder(op, order))
            case _ =>
              val wider = ColumnExpr
                .wider(columns(left).field.kind, columns(right).field.kind)
                .getOrElse(throw new IllegalArgumentException(s"$op is not numeric"))
              Cell.Bool(compare(op, wider, columns(left), columns(right), row))
        case ColumnExpr.And(left, right) =>
          Cell.Bool(boolAt(columns(left), row) && boolAt(columns(right), row))
        case ColumnExpr.Or(left, right) =>
          Cell.Bool(boolAt(columns(left), row) || boolAt(columns(right), row))
        case ColumnExpr.Not(input) =>
          Cell.Bool(!boolAt(columns(input), row))
        case ColumnExpr.Cast(input, to) =>
          cast(columns(input).cell(row), to)

  private def binary(
      kind: ColumnKind,
      left: WideColumn,
      right: WideColumn,
      row: Int,
      ints: (Int, Int) => Int,
      longs: (Long, Long) => Long,
      floats: (Float, Float) => Float,
      doubles: (Double, Double) => Double,
  ): Cell = kind match
    case ColumnKind.Int32 => Cell.Int32(ints(intAt(left, row), intAt(right, row)))
    case ColumnKind.Int64 => Cell.Int64(longs(longAt(left, row), longAt(right, row)))
    case ColumnKind.Float32 => Cell.Float32(floats(floatAt(left, row), floatAt(right, row)))
    case ColumnKind.Float64 => Cell.Float64(doubles(doubleAt(left, row), doubleAt(right, row)))
    case _ => throw new IllegalArgumentException(s"arithmetic result $kind")

  private def compare(
      op: CompareOp,
      kind: ColumnKind,
      left: WideColumn,
      right: WideColumn,
      row: Int,
  ): Boolean = kind match
    case ColumnKind.Int32 =>
      op match
        case CompareOp.Gt => intAt(left, row) > intAt(right, row)
        case CompareOp.Ge => intAt(left, row) >= intAt(right, row)
        case CompareOp.Lt => intAt(left, row) < intAt(right, row)
        case CompareOp.Le => intAt(left, row) <= intAt(right, row)
        case CompareOp.Eq => intAt(left, row) == intAt(right, row)
        case CompareOp.Ne => intAt(left, row) != intAt(right, row)
    case ColumnKind.Int64 =>
      op match
        case CompareOp.Gt => longAt(left, row) > longAt(right, row)
        case CompareOp.Ge => longAt(left, row) >= longAt(right, row)
        case CompareOp.Lt => longAt(left, row) < longAt(right, row)
        case CompareOp.Le => longAt(left, row) <= longAt(right, row)
        case CompareOp.Eq => longAt(left, row) == longAt(right, row)
        case CompareOp.Ne => longAt(left, row) != longAt(right, row)
    case ColumnKind.Float32 =>
      op match
        case CompareOp.Gt => floatAt(left, row) > floatAt(right, row)
        case CompareOp.Ge => floatAt(left, row) >= floatAt(right, row)
        case CompareOp.Lt => floatAt(left, row) < floatAt(right, row)
        case CompareOp.Le => floatAt(left, row) <= floatAt(right, row)
        case CompareOp.Eq => floatAt(left, row) == floatAt(right, row)
        case CompareOp.Ne => floatAt(left, row) != floatAt(right, row)
    case ColumnKind.Float64 =>
      op match
        case CompareOp.Gt => doubleAt(left, row) > doubleAt(right, row)
        case CompareOp.Ge => doubleAt(left, row) >= doubleAt(right, row)
        case CompareOp.Lt => doubleAt(left, row) < doubleAt(right, row)
        case CompareOp.Le => doubleAt(left, row) <= doubleAt(right, row)
        case CompareOp.Eq => doubleAt(left, row) == doubleAt(right, row)
        case CompareOp.Ne => doubleAt(left, row) != doubleAt(right, row)
    case _ => throw new IllegalArgumentException(s"compare result $kind")

  private def cast(cell: Cell, to: ColumnKind): Cell = (to, cell) match
    case (ColumnKind.Int32, Cell.Int32(value)) => Cell.Int32(value)
    case (ColumnKind.Int32, Cell.Int64(value)) => Cell.Int32(value.toInt)
    case (ColumnKind.Int32, Cell.Float32(value)) => Cell.Int32(value.toInt)
    case (ColumnKind.Int32, Cell.Float64(value)) => Cell.Int32(value.toInt)
    case (ColumnKind.Int64, Cell.Int32(value)) => Cell.Int64(value.toLong)
    case (ColumnKind.Int64, Cell.Int64(value)) => Cell.Int64(value)
    case (ColumnKind.Int64, Cell.Float32(value)) => Cell.Int64(value.toLong)
    case (ColumnKind.Int64, Cell.Float64(value)) => Cell.Int64(value.toLong)
    case (ColumnKind.Float32, Cell.Int32(value)) => Cell.Float32(value.toFloat)
    case (ColumnKind.Float32, Cell.Int64(value)) => Cell.Float32(value.toFloat)
    case (ColumnKind.Float32, Cell.Float32(value)) => Cell.Float32(value)
    case (ColumnKind.Float32, Cell.Float64(value)) => Cell.Float32(value.toFloat)
    case (ColumnKind.Float64, Cell.Int32(value)) => Cell.Float64(value.toDouble)
    case (ColumnKind.Float64, Cell.Int64(value)) => Cell.Float64(value.toDouble)
    case (ColumnKind.Float64, Cell.Float32(value)) => Cell.Float64(value.toDouble)
    case (ColumnKind.Float64, Cell.Float64(value)) => Cell.Float64(value)
    case (ColumnKind.Decimal(precision, scale), Cell.Int32(value)) =>
      Cell.Decimal(precision, scale, DecimalRules.fromLong(value.toLong, precision, scale))
    case (ColumnKind.Decimal(precision, scale), Cell.Int64(value)) =>
      Cell.Decimal(precision, scale, DecimalRules.fromLong(value, precision, scale))
    case (ColumnKind.Int64, Cell.Decimal(_, scale, value)) =>
      Cell.Int64(DecimalRules.toLong(value, scale))
    case _ => throw new IllegalArgumentException(s"cannot cast $cell to $to")

  private def ready(columns: Vector[WideColumn], expr: ColumnExpr, row: Int): Boolean =
    expr match
      case ColumnExpr.Add(left, right) => on(columns, left, row) && on(columns, right, row)
      case ColumnExpr.Sub(left, right) => on(columns, left, row) && on(columns, right, row)
      case ColumnExpr.Mul(left, right) => on(columns, left, row) && on(columns, right, row)
      case ColumnExpr.Div(left, right) => on(columns, left, row) && on(columns, right, row)
      case ColumnExpr.Lit(_) => true
      case ColumnExpr.Compare(_, left, right) => on(columns, left, row) && on(columns, right, row)
      case ColumnExpr.And(left, right) => on(columns, left, row) && on(columns, right, row)
      case ColumnExpr.Or(left, right) => on(columns, left, row) && on(columns, right, row)
      case ColumnExpr.Not(input) => on(columns, input, row)
      case ColumnExpr.Cast(input, _) => on(columns, input, row)

  private def divInt(left: Int, right: Int): Int =
    if (right == 0) then refuseZero() else left / right

  private def divLong(left: Long, right: Long): Long =
    if (right == 0L) then refuseZero() else left / right

  private def divFloat(left: Float, right: Float): Float =
    if (right == 0.0f) then refuseZero() else left / right

  private def divDouble(left: Double, right: Double): Double =
    if (right == 0.0) then refuseZero() else left / right

  private def refuseZero(): Nothing =
    throw new IllegalArgumentException("divide by zero")

  private def literal(value: ColumnLiteral): Cell = value match
    case ColumnLiteral.Int32(number) => Cell.Int32(number)
    case ColumnLiteral.Int64(number) => Cell.Int64(number)
    case ColumnLiteral.Float64(number) => Cell.Float64(number)
    case ColumnLiteral.Bool(number) => Cell.Bool(number)
    case ColumnLiteral.Decimal(precision, scale, number) =>
      Cell.Decimal(precision, scale, number)

  private def decimalCell(
      columns: Vector[WideColumn],
      left: Int,
      right: Int,
      row: Int,
      op: (Long, Int, Int, Long, Int, Int) => (Long, ColumnKind),
  ): Cell =
    val (leftValue, leftPrecision, leftScale) = decimalAt(columns(left), row)
    val (rightValue, rightPrecision, rightScale) = decimalAt(columns(right), row)
    val (value, kind) = op(
      leftValue,
      leftPrecision,
      leftScale,
      rightValue,
      rightPrecision,
      rightScale,
    )
    kind match
      case ColumnKind.Decimal(precision, scale) => Cell.Decimal(precision, scale, value)
      case other => throw new IllegalArgumentException(s"decimal result $other")

  private def decimalAt(column: WideColumn, row: Int): (Long, Int, Int) = column.cell(row) match
    case Cell.Decimal(precision, scale, value) => (value, precision, scale)
    case other => throw new IllegalArgumentException(s"expected a decimal, found $other")

  private def decimalOrder(op: CompareOp, order: Int): Boolean = op match
    case CompareOp.Gt => order > 0
    case CompareOp.Ge => order >= 0
    case CompareOp.Lt => order < 0
    case CompareOp.Le => order <= 0
    case CompareOp.Eq => order == 0
    case CompareOp.Ne => order != 0

  private def on(columns: Vector[WideColumn], ordinal: Int, row: Int): Boolean =
    columns(ordinal).isPresent(row)

  private def intAt(column: WideColumn, row: Int): Int = column.cell(row) match
    case Cell.Int32(value) => value
    case other => throw new IllegalArgumentException(s"expected an int, found $other")

  private def longAt(column: WideColumn, row: Int): Long = column.cell(row) match
    case Cell.Int32(value) => value.toLong
    case Cell.Int64(value) => value
    case other => throw new IllegalArgumentException(s"expected an int or long, found $other")

  private def floatAt(column: WideColumn, row: Int): Float = column.cell(row) match
    case Cell.Int32(value) => value.toFloat
    case Cell.Int64(value) => value.toFloat
    case Cell.Float32(value) => value
    case other => throw new IllegalArgumentException(s"expected a float input, found $other")

  private def doubleAt(column: WideColumn, row: Int): Double = column.cell(row) match
    case Cell.Int32(value) => value.toDouble
    case Cell.Int64(value) => value.toDouble
    case Cell.Float32(value) => value.toDouble
    case Cell.Float64(value) => value
    case other => throw new IllegalArgumentException(s"expected a numeric input, found $other")

  private def boolAt(column: WideColumn, row: Int): Boolean = column.cell(row) match
    case Cell.Bool(value) => value
    case other => throw new IllegalArgumentException(s"expected a bool, found $other")

  private def reject(field: ColumnField, cell: Cell): Nothing =
    throw new IllegalArgumentException(s"${field.name} produced $cell, expected ${field.kind}")
