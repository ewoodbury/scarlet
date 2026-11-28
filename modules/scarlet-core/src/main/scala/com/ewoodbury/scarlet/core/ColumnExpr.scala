package com.ewoodbury.scarlet.core

/**
 * One new column computed from columns already in the row.
 *
 * Arithmetic uses the wider input: double, then float, then long, then int. A compare yields bool.
 * Null in an input yields null, including `true OR null`. Divide uses that same widening and
 * refuses a present zero divisor. A decimal add, subtract, or multiply stays exact. Decimal divide
 * is not in this set. `length` of a string is that string's UTF-16 length. Other string
 * expressions are not in this set. A date compare is a long compare.
 */
enum ColumnExpr:
  case Add(left: Int, right: Int)
  case Sub(left: Int, right: Int)
  case Mul(left: Int, right: Int)
  case Div(left: Int, right: Int)
  case Lit(value: ColumnLiteral)
  case Compare(op: CompareOp, left: Int, right: Int)
  case And(left: Int, right: Int)
  case Or(left: Int, right: Int)
  case Not(input: Int)
  case Cast(input: Int, to: ColumnKind)
  case Len(input: Int)

object ColumnExpr:

  /** Kind of the column `expr` appends, or [[None]] when `expr` does not apply. */
  def resultKind(expr: ColumnExpr, fields: Vector[ColumnField]): Option[ColumnKind] = expr match
    case ColumnExpr.Add(left, right) => arithmetic(fields, left, right, DecimalRules.addKind)
    case ColumnExpr.Sub(left, right) => arithmetic(fields, left, right, DecimalRules.addKind)
    case ColumnExpr.Mul(left, right) => arithmetic(fields, left, right, DecimalRules.mulKind)
    case ColumnExpr.Div(left, right) => numeric(fields, left, right)
    case ColumnExpr.Lit(_: ColumnLiteral.Utf8) => None
    case ColumnExpr.Lit(value) => Some(value.kind)
    case ColumnExpr.Compare(_, left, right) =>
      compared(fields, left, right)
    case ColumnExpr.And(left, right) => bools(fields, left, right)
    case ColumnExpr.Or(left, right) => bools(fields, left, right)
    case ColumnExpr.Not(input) =>
      field(fields, input).filter(column => boolKind(column.kind)).map(_ => ColumnKind.Bool)
    case ColumnExpr.Cast(input, to) =>
      field(fields, input).flatMap(column => castKind(column.kind, to))
    case ColumnExpr.Len(input) =>
      field(fields, input)
        .filter(column => column.kind == ColumnKind.Utf8)
        .map(_ => ColumnKind.Int32)

  /**
   * Kind `expr` appends. Throws when the name is empty, already used, or `expr` does not apply.
   */
  def checked(expr: ColumnExpr, fields: Vector[ColumnField], name: String): ColumnKind =
    require(name.nonEmpty, "column name must not be empty")
    require(!fields.exists(column => column.name.equals(name)), s"column $name already exists")
    resultKind(expr, fields).getOrElse(
      throw new IllegalArgumentException(
        s"$expr does not apply to ${fields.map(_.kind).mkString("[", ",", "]")}",
      ),
    )

  /** Wider of two numeric kinds. [[None]] when either kind is not numeric. */
  def wider(left: ColumnKind, right: ColumnKind): Option[ColumnKind] =
    (rank(left), rank(right)) match
      case (0, _) => None
      case (_, 0) => None
      case (leftRank, rightRank) if leftRank >= rightRank => Some(left)
      case _ => Some(right)

  private def arithmetic(
      fields: Vector[ColumnField],
      left: Int,
      right: Int,
      decimals: (Int, Int, Int, Int) => Option[ColumnKind],
  ): Option[ColumnKind] =
    for
      leftField <- field(fields, left)
      rightField <- field(fields, right)
      kind <- (leftField.kind, rightField.kind) match
        case (
              ColumnKind.Decimal(leftPrecision, leftScale),
              ColumnKind.Decimal(rightPrecision, rightScale),
            ) =>
          decimals(leftPrecision, leftScale, rightPrecision, rightScale)
        case _ => wider(leftField.kind, rightField.kind)
    yield kind

  private def compared(
      fields: Vector[ColumnField],
      left: Int,
      right: Int,
  ): Option[ColumnKind] =
    for
      leftField <- field(fields, left)
      rightField <- field(fields, right)
      if decimalPair(leftField.kind, rightField.kind) || wider(
        leftField.kind,
        rightField.kind,
      ).nonEmpty
    yield ColumnKind.Bool

  private def castKind(from: ColumnKind, to: ColumnKind): Option[ColumnKind] = (from, to) match
    case (ColumnKind.Int32 | ColumnKind.Int64, ColumnKind.Decimal(precision, scale)) =>
      DecimalRules.check(precision, scale)
      Some(to)
    case (ColumnKind.Decimal(_, _), ColumnKind.Int64) => Some(ColumnKind.Int64)
    case _ if numericKind(from) && numericKind(to) => Some(to)
    case _ => None

  private def decimalPair(left: ColumnKind, right: ColumnKind): Boolean = (left, right) match
    case (ColumnKind.Decimal(_, _), ColumnKind.Decimal(_, _)) => true
    case _ => false

  private def numeric(
      fields: Vector[ColumnField],
      left: Int,
      right: Int,
  ): Option[ColumnKind] =
    for
      leftField <- field(fields, left)
      rightField <- field(fields, right)
      kind <- wider(leftField.kind, rightField.kind)
    yield kind

  private def bools(fields: Vector[ColumnField], left: Int, right: Int): Option[ColumnKind] =
    for
      leftField <- field(fields, left)
      rightField <- field(fields, right)
      if boolKind(leftField.kind) && boolKind(rightField.kind)
    yield ColumnKind.Bool

  private def field(fields: Vector[ColumnField], ordinal: Int): Option[ColumnField] =
    if (ordinal >= 0 && ordinal < fields.size) then Some(fields(ordinal)) else None

  private def numericKind(kind: ColumnKind): Boolean = rank(kind) > 0

  private def boolKind(kind: ColumnKind): Boolean = kind match
    case ColumnKind.Bool => true
    case _ => false

  private def rank(kind: ColumnKind): Int = kind match
    case ColumnKind.Int32 => 1
    case ColumnKind.Int64 => 2
    case ColumnKind.Float32 => 3
    case ColumnKind.Float64 => 4
    case _ => 0
