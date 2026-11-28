package com.ewoodbury.scarlet.core

/**
 * Schema of one wide row.
 *
 * A wide row is an ordered list of columns. Each column has a name and one kind that already
 * exists. The name is for the job and for error text. Kernels address columns by ordinal. There is
 * no nested row and no array column.
 *
 * A compare is a closed set: a numeric column against a constant of the same kind, equality on a
 * bool, a decimal against a decimal constant, or a string column against a string literal. String
 * order is JVM `compareTo`. `LIKE` uses `%` and `_` and is case sensitive. Scales align by an
 * exact power of ten. A null does not pass. Float32 compares are not in this set. A null string
 * literal is refused.
 */
enum ColumnKind:
  case Int32, Int64, Float64, Bool, Utf8, Float32
  case Decimal(precision: Int, scale: Int)

final case class ColumnField(name: String, kind: ColumnKind):
  require(name.nonEmpty, "column name must not be empty")
  kind match
    case ColumnKind.Decimal(precision, scale) => DecimalRules.check(precision, scale)
    case _ => ()

enum CompareOp:
  case Gt, Ge, Lt, Le, Eq, Ne, Like, Unlike

object CompareOp:

  /** True when `order` is a numeric comparison. A pattern has no order. */
  def signed(op: CompareOp, order: Int): Boolean = op match
    case Gt => order > 0
    case Ge => order >= 0
    case Lt => order < 0
    case Le => order <= 0
    case Eq => order == 0
    case Ne => order != 0
    case Like | Unlike =>
      throw new IllegalArgumentException("a pattern is not ordered")

  /** [[TextRule]] order, or a [[LikeMatch]]. `"é"` and `"e\u0301"` stay different. */
  def text(op: CompareOp, value: String, constant: String): Boolean = op match
    case Gt => TextRule.compare(value, constant) > 0
    case Ge => TextRule.compare(value, constant) >= 0
    case Lt => TextRule.compare(value, constant) < 0
    case Le => TextRule.compare(value, constant) <= 0
    case Eq => value == constant
    case Ne => value != constant
    case Like => LikeMatch.matches(value, constant)
    case Unlike => !LikeMatch.matches(value, constant)

enum ColumnLiteral:
  case Int32(value: Int)
  case Int64(value: Long)
  case Float64(value: Double)
  case Bool(value: Boolean)
  case Decimal(precision: Int, scale: Int, value: Long)
  case Utf8(value: String)

  def kind: ColumnKind = this match
    case _: Int32 => ColumnKind.Int32
    case _: Int64 => ColumnKind.Int64
    case _: Float64 => ColumnKind.Float64
    case _: Bool => ColumnKind.Bool
    case Utf8(value) =>
      require(value != null, "a string literal must not be null")
      ColumnKind.Utf8
    case Decimal(precision, scale, value) =>
      DecimalRules.check(precision, scale)
      if (!DecimalRules.fits(value, precision)) then
        throw new IllegalArgumentException(s"decimal value does not fit precision $precision")
      ColumnKind.Decimal(precision, scale)

final case class ColumnCompare(op: CompareOp, literal: ColumnLiteral):
  literal match
    case ColumnLiteral.Utf8(value) =>
      require(value != null, "a string literal must not be null")
    case _ => ()

  /** True when this compare is legal for a column of `kind`. */
  def allows(kind: ColumnKind): Boolean = (kind, literal, op) match
    case (ColumnKind.Int32, _: ColumnLiteral.Int32, operator) if numeric(operator) => true
    case (ColumnKind.Int64, _: ColumnLiteral.Int64, operator) if numeric(operator) => true
    case (ColumnKind.Float64, _: ColumnLiteral.Float64, operator) if numeric(operator) => true
    case (ColumnKind.Bool, _: ColumnLiteral.Bool, CompareOp.Eq | CompareOp.Ne) => true
    case (ColumnKind.Decimal(_, _), _: ColumnLiteral.Decimal, operator) if numeric(operator) =>
      true
    case (ColumnKind.Utf8, _: ColumnLiteral.Utf8, _) => true
    case _ => false

  /**
   * True when a present `cell` passes. A null does not pass, and the comparison is not applied.
   * The caller has already checked [[allows]] for the column kind.
   */
  def keep(cell: Cell): Boolean = cell match
    case Cell.Null => false
    case other => apply(other)

  /**
   * [[keep]] for a cell this compare may not name. A null does not pass. A cell of another kind
   * does not pass. [[keep]] still throws for that cell, so a direct filter can refuse it.
   */
  def matches(cell: Cell): Boolean = cell match
    case Cell.Null => false
    case other =>
      try apply(other)
      catch case _: IllegalArgumentException => false

  private def apply(cell: Cell): Boolean = (op, literal, cell) match
    case (CompareOp.Gt, ColumnLiteral.Int32(constant), Cell.Int32(value)) => value > constant
    case (CompareOp.Ge, ColumnLiteral.Int32(constant), Cell.Int32(value)) => value >= constant
    case (CompareOp.Lt, ColumnLiteral.Int32(constant), Cell.Int32(value)) => value < constant
    case (CompareOp.Le, ColumnLiteral.Int32(constant), Cell.Int32(value)) => value <= constant
    case (CompareOp.Eq, ColumnLiteral.Int32(constant), Cell.Int32(value)) => value == constant
    case (CompareOp.Ne, ColumnLiteral.Int32(constant), Cell.Int32(value)) => value != constant
    case (CompareOp.Gt, ColumnLiteral.Int64(constant), Cell.Int64(value)) => value > constant
    case (CompareOp.Ge, ColumnLiteral.Int64(constant), Cell.Int64(value)) => value >= constant
    case (CompareOp.Lt, ColumnLiteral.Int64(constant), Cell.Int64(value)) => value < constant
    case (CompareOp.Le, ColumnLiteral.Int64(constant), Cell.Int64(value)) => value <= constant
    case (CompareOp.Eq, ColumnLiteral.Int64(constant), Cell.Int64(value)) => value == constant
    case (CompareOp.Ne, ColumnLiteral.Int64(constant), Cell.Int64(value)) => value != constant
    case (CompareOp.Gt, ColumnLiteral.Float64(constant), Cell.Float64(value)) => value > constant
    case (CompareOp.Ge, ColumnLiteral.Float64(constant), Cell.Float64(value)) =>
      value >= constant
    case (CompareOp.Lt, ColumnLiteral.Float64(constant), Cell.Float64(value)) => value < constant
    case (CompareOp.Le, ColumnLiteral.Float64(constant), Cell.Float64(value)) =>
      value <= constant
    case (CompareOp.Eq, ColumnLiteral.Float64(constant), Cell.Float64(value)) =>
      value == constant
    case (CompareOp.Ne, ColumnLiteral.Float64(constant), Cell.Float64(value)) =>
      value != constant
    case (CompareOp.Eq, ColumnLiteral.Bool(constant), Cell.Bool(value)) =>
      sameBool(constant, value)
    case (CompareOp.Ne, ColumnLiteral.Bool(constant), Cell.Bool(value)) =>
      !sameBool(constant, value)
    case (_, ColumnLiteral.Utf8(constant), Cell.Utf8(value)) =>
      CompareOp.text(op, value, constant)
    case (_, ColumnLiteral.Decimal(_, scale, constant), Cell.Decimal(_, valueScale, value)) =>
      val order = DecimalRules.compare(value, valueScale, constant, scale)
      CompareOp.signed(op, order)
    case _ =>
      throw new IllegalArgumentException(s"$this does not apply to $cell")

  private def numeric(operator: CompareOp): Boolean = operator match
    case CompareOp.Like | CompareOp.Unlike => false
    case CompareOp.Gt | CompareOp.Ge | CompareOp.Lt | CompareOp.Le | CompareOp.Eq | CompareOp.Ne =>
      true

  private def sameBool(left: Boolean, right: Boolean): Boolean = (left, right) match
    case (true, true) | (false, false) => true
    case _ => false

/** One cell of a wide row. [[Cell.Null]] is a clear validity bit, not a stored zero. */
enum Cell:
  case Int32(value: Int)
  case Int64(value: Long)
  case Float64(value: Double)
  case Bool(value: Boolean)
  case Utf8(value: String)
  case Float32(value: Float)
  case Decimal(precision: Int, scale: Int, value: Long)
  case Null

final case class WideRow(fields: Vector[ColumnField], cells: Vector[Cell])
