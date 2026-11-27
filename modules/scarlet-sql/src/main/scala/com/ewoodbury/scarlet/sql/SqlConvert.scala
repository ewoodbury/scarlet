package com.ewoodbury.scarlet.sql

import scala.jdk.CollectionConverters.*

import net.sf.jsqlparser.expression.*
import net.sf.jsqlparser.expression.operators.arithmetic.{
  Addition,
  Division,
  Multiplication,
  Subtraction,
}
import net.sf.jsqlparser.expression.operators.conditional.{AndExpression, OrExpression}
import net.sf.jsqlparser.expression.operators.relational.*
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.{Column, Table}
import net.sf.jsqlparser.statement.select.{AllColumns, FromItem, PlainSelect, SelectItem}

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*

/**
 * One SELECT: a registered table, comparisons combined with AND, OR, and NOT, arithmetic, and
 * count, sum, min, and max. A group is one or two numeric keys. Anything else names its node.
 */
private object SqlConvert:

  def run(statement: String, tables: Map[String, DistCollection[WideRow]]): SqlResult =
    val parsed =
      try CCJSqlParserUtil.parse(statement)
      catch
        case failed: net.sf.jsqlparser.JSQLParserException =>
          throw new IllegalArgumentException(failed.getMessage, failed)
    parsed match
      case plain: PlainSelect => convert(plain, tables)
      case other => unsupported(other)

  private def convert(
      plain: PlainSelect,
      tables: Map[String, DistCollection[WideRow]],
  ): SqlResult =
    if plain.getDistinct != null then unsupported(plain.getDistinct)
    if plain.getJoins != null && !plain.getJoins.isEmpty then unsupported(plain.getJoins.get(0))
    if plain.getLimit != null then unsupported(plain.getLimit)
    if plain.getOffset != null then unsupported(plain.getOffset)
    if plain.getFetch != null then unsupported(plain.getFetch)
    if plain.getTop != null then unsupported(plain.getTop)
    if plain.getOrderByElements != null && !plain.getOrderByElements.isEmpty then
      unsupported(plain.getOrderByElements.get(0))
    if plain.getHaving != null then unsupported(plain.getHaving)
    val from = plain.getFromItem
    if from == null then unsupported(plain)
    val frame = Frame(resolve(tables, tableName(from)), 0)
    val filtered = if plain.getWhere == null then frame else predicate(plain.getWhere, frame)
    val grouped = plain.getGroupBy
    if grouped == null then select(plain, filtered)
    else group(plain, keys(grouped.getGroupByExpressionList, filtered), filtered)

  private def select(plain: PlainSelect, frame: Frame): SqlResult =
    val items = itemsOf(plain)
    if items.isEmpty then unsupported(plain)
    else if items.size == 1 && aggregate(items.head).isDefined then
      scalar(aggregate(items.head).get, frame)
    else if items.exists(item => aggregate(item).isDefined) then
      throw new IllegalArgumentException("several aggregates need a group")
    else SqlResult.Rows(project(items, frame).rows)

  private def group(plain: PlainSelect, keyOrdinals: List[Int], frame: Frame): SqlResult =
    if keyOrdinals.isEmpty || keyOrdinals.size > 2 then
      throw new IllegalArgumentException("a group needs one or two numeric columns")
    val items = itemsOf(plain)
    val measure = groupedSelect(items, keyOrdinals, frame)
    val rows =
      if keyOrdinals.size == 1 then frame.rows.groupColumns(keyOrdinals.head, measure)
      else frame.rows.groupColumns(keyOrdinals.head, keyOrdinals(1), measure)
    SqlResult.Rows(rows)

  /** Keys, then count(*), sum, min, and max of one measure. */
  private def groupedSelect(items: List[SelectItem[?]], keys: List[Int], frame: Frame): Int =
    val expected = keys.size + 4
    if items.size != expected then
      throw new IllegalArgumentException(
        "a grouped select lists the keys, then count, sum, min, and max",
      )
    keys.zipWithIndex.foreach { (ordinal, index) =>
      plainColumn(expressionOf(items(index)), frame) match
        case Some(found) if found == ordinal => ()
        case _ =>
          throw new IllegalArgumentException(
            "a grouped select lists the keys, then count, sum, min, and max",
          )
    }
    val functions = items.drop(keys.size).map(item => aggregate(item).getOrElse(unsupported(item)))
    functions.head match
      case Aggregate.Count => ()
      case _ =>
        throw new IllegalArgumentException(
          "a grouped select lists the keys, then count, sum, min, and max",
        )
    val measure = functions.tail.map {
      case Aggregate.Of("sum" | "min" | "max", expr) =>
        plainColumn(expr, frame).getOrElse(
          throw new IllegalArgumentException("a grouped measure must be a column"),
        )
      case _ =>
        throw new IllegalArgumentException(
          "a grouped select lists the keys, then count, sum, min, and max",
        )
    }
    if measure.distinct.size != 1 then
      throw new IllegalArgumentException("sum, min, and max must use one column")
    measure.head

  private def scalar(kind: Aggregate, frame: Frame): SqlResult = kind match
    case Aggregate.Count =>
      SqlResult.Value(frame.rows.count(), frame.rows.plan)
    case Aggregate.Of(name, expr) =>
      val (placed, ordinal) = side(expr, frame)
      val kindOf = kindAt(placed, ordinal)
      val (built, column, resultKind) = name match
        case "sum" => summed(placed, ordinal, kindOf)
        case "min" => (placed, ordinal, kindOf)
        case "max" => (placed, ordinal, kindOf)
        case other => throw new IllegalArgumentException(s"$other is not supported")
      val value = name match
        case "sum" => sumValue(built, column, resultKind)
        case "min" => extremum(built, column, resultKind, least = true)
        case "max" => extremum(built, column, resultKind, least = false)
        case other => throw new IllegalArgumentException(s"$other is not supported")
      SqlResult.Value(value, built.rows.plan)

  private def summed(frame: Frame, ordinal: Int, kind: ColumnKind): (Frame, Int, ColumnKind) =
    kind match
      case ColumnKind.Int32 =>
        val (cast, column) = frame.bind(ColumnExpr.Cast(ordinal, ColumnKind.Int64))
        (cast, column, ColumnKind.Int64)
      case ColumnKind.Int64 => (frame, ordinal, kind)
      case ColumnKind.Decimal(_, _) => (frame, ordinal, kind)
      case other => throw new IllegalArgumentException(s"sum of $other is not supported")

  private def sumValue(frame: Frame, ordinal: Int, kind: ColumnKind): Long = kind match
    case ColumnKind.Int64 => frame.rows.sumLongColumn(ordinal)
    case ColumnKind.Decimal(_, _) => frame.rows.sumDecimalColumn(ordinal)
    case other => throw new IllegalArgumentException(s"sum of $other is not supported")

  private def extremum(frame: Frame, ordinal: Int, kind: ColumnKind, least: Boolean): Long =
    kind match
      case ColumnKind.Int32 =>
        val value =
          if least then frame.rows.minIntColumn(ordinal) else frame.rows.maxIntColumn(ordinal)
        value.toLong
      case ColumnKind.Int64 =>
        if least then frame.rows.minLongColumn(ordinal) else frame.rows.maxLongColumn(ordinal)
      case other => throw new IllegalArgumentException(s"min of $other is not supported")

  private def project(items: List[SelectItem[?]], frame: Frame): Frame =
    val (built, spec) = items.foldLeft((frame, Vector.empty[(Int, String)])) {
      case ((current, spec), item) =>
        val (next, ordinal) = side(expressionOf(item), current)
        val name = outputName(item, next, ordinal)
        (next, spec :+ (ordinal, name))
    }
    built.copy(rows = built.rows.projectColumns(spec))

  private def predicate(expr: Expression, frame: Frame): Frame = unwrap(expr) match
    case and: AndExpression =>
      predicate(and.getRightExpression, predicate(and.getLeftExpression, frame))
    case or: OrExpression =>
      val (built, ordinal) = boolExpr(or, frame)
      built.filter(ordinal, ColumnCompare(CompareOp.Eq, ColumnLiteral.Bool(true)))
    case not: NotExpression => negate(not.getExpression, frame)
    case other => compare(other, frame)

  private def negate(expr: Expression, frame: Frame): Frame = unwrap(expr) match
    case eq: EqualsTo =>
      wideOrBool(CompareOp.Ne, eq.getLeftExpression, eq.getRightExpression, frame)
    case ne: NotEqualsTo =>
      wideOrBool(CompareOp.Eq, ne.getLeftExpression, ne.getRightExpression, frame)
    case gt: GreaterThan =>
      wideOrBool(CompareOp.Le, gt.getLeftExpression, gt.getRightExpression, frame)
    case ge: GreaterThanEquals =>
      wideOrBool(CompareOp.Lt, ge.getLeftExpression, ge.getRightExpression, frame)
    case lt: MinorThan =>
      wideOrBool(CompareOp.Ge, lt.getLeftExpression, lt.getRightExpression, frame)
    case le: MinorThanEquals =>
      wideOrBool(CompareOp.Gt, le.getLeftExpression, le.getRightExpression, frame)
    case other =>
      val (built, ordinal) = boolExpr(other, frame)
      val (negated, column) = built.bind(ColumnExpr.Not(ordinal))
      negated.filter(column, ColumnCompare(CompareOp.Eq, ColumnLiteral.Bool(true)))

  private def compare(expr: Expression, frame: Frame): Frame = unwrap(expr) match
    case eq: EqualsTo =>
      wideOrBool(CompareOp.Eq, eq.getLeftExpression, eq.getRightExpression, frame)
    case ne: NotEqualsTo =>
      wideOrBool(CompareOp.Ne, ne.getLeftExpression, ne.getRightExpression, frame)
    case gt: GreaterThan =>
      wideOrBool(CompareOp.Gt, gt.getLeftExpression, gt.getRightExpression, frame)
    case ge: GreaterThanEquals =>
      wideOrBool(CompareOp.Ge, ge.getLeftExpression, ge.getRightExpression, frame)
    case lt: MinorThan =>
      wideOrBool(CompareOp.Lt, lt.getLeftExpression, lt.getRightExpression, frame)
    case le: MinorThanEquals =>
      wideOrBool(CompareOp.Le, le.getLeftExpression, le.getRightExpression, frame)
    case other => unsupported(other)

  private def wideOrBool(
      op: CompareOp,
      left: Expression,
      right: Expression,
      frame: Frame,
  ): Frame =
    wideCompare(op, left, right, frame).getOrElse {
      val (built, ordinal) = compareBool(op, left, right, frame)
      built.filter(ordinal, ColumnCompare(CompareOp.Eq, ColumnLiteral.Bool(true)))
    }

  private def wideCompare(
      op: CompareOp,
      left: Expression,
      right: Expression,
      frame: Frame,
  ): Option[Frame] =
    plainColumn(left, frame) match
      case Some(ordinal) =>
        typedLiteral(right, kindAt(frame, ordinal)).map { literal =>
          frame.filter(ordinal, ColumnCompare(op, literal))
        }
      case None =>
        plainColumn(right, frame).flatMap { ordinal =>
          typedLiteral(left, kindAt(frame, ordinal)).map { literal =>
            frame.filter(ordinal, ColumnCompare(reflect(op), literal))
          }
        }

  /** A constant on the left is the same compare with the column on the left. */
  private def reflect(op: CompareOp): CompareOp = op match
    case CompareOp.Gt => CompareOp.Lt
    case CompareOp.Ge => CompareOp.Le
    case CompareOp.Lt => CompareOp.Gt
    case CompareOp.Le => CompareOp.Ge
    case CompareOp.Eq => CompareOp.Eq
    case CompareOp.Ne => CompareOp.Ne

  private def boolExpr(expr: Expression, frame: Frame): (Frame, Int) = unwrap(expr) match
    case and: AndExpression =>
      val (leftFrame, left) = boolExpr(and.getLeftExpression, frame)
      val (rightFrame, right) = boolExpr(and.getRightExpression, leftFrame)
      rightFrame.bind(ColumnExpr.And(left, right))
    case or: OrExpression =>
      val (leftFrame, left) = boolExpr(or.getLeftExpression, frame)
      val (rightFrame, right) = boolExpr(or.getRightExpression, leftFrame)
      rightFrame.bind(ColumnExpr.Or(left, right))
    case not: NotExpression =>
      val (inner, ordinal) = boolExpr(not.getExpression, frame)
      inner.bind(ColumnExpr.Not(ordinal))
    case eq: EqualsTo =>
      compareBool(CompareOp.Eq, eq.getLeftExpression, eq.getRightExpression, frame)
    case ne: NotEqualsTo =>
      compareBool(CompareOp.Ne, ne.getLeftExpression, ne.getRightExpression, frame)
    case gt: GreaterThan =>
      compareBool(CompareOp.Gt, gt.getLeftExpression, gt.getRightExpression, frame)
    case ge: GreaterThanEquals =>
      compareBool(CompareOp.Ge, ge.getLeftExpression, ge.getRightExpression, frame)
    case lt: MinorThan =>
      compareBool(CompareOp.Lt, lt.getLeftExpression, lt.getRightExpression, frame)
    case le: MinorThanEquals =>
      compareBool(CompareOp.Le, le.getLeftExpression, le.getRightExpression, frame)
    case other => unsupported(other)

  private def compareBool(
      op: CompareOp,
      left: Expression,
      right: Expression,
      frame: Frame,
  ): (Frame, Int) =
    val (leftFrame, leftOrdinal) = side(left, frame)
    val (rightFrame, rightOrdinal) = side(right, leftFrame)
    rightFrame.bind(ColumnExpr.Compare(op, leftOrdinal, rightOrdinal))

  private def side(expr: Expression, frame: Frame): (Frame, Int) = unwrap(expr) match
    case column: Column => (frame, columnOrdinal(frame, column))
    case value: LongValue => frame.bind(ColumnExpr.Lit(intLiteral(value.getValue)))
    case value: DoubleValue =>
      frame.bind(ColumnExpr.Lit(ColumnLiteral.Float64(value.getValue)))
    case value: SignedExpression if value.getSign == '-' =>
      value.getExpression match
        case inner: LongValue => frame.bind(ColumnExpr.Lit(intLiteral(-inner.getValue)))
        case other => unsupported(other)
    case value: StringValue => unsupported(value)
    case add: Addition => binary(add, frame, ColumnExpr.Add.apply)
    case sub: Subtraction => binary(sub, frame, ColumnExpr.Sub.apply)
    case mul: Multiplication => binary(mul, frame, ColumnExpr.Mul.apply)
    case div: Division => binary(div, frame, ColumnExpr.Div.apply)
    case other => unsupported(other)

  private def binary(
      expr: BinaryExpression,
      frame: Frame,
      op: (Int, Int) => ColumnExpr,
  ): (Frame, Int) =
    val (leftFrame, left) = side(expr.getLeftExpression, frame)
    val (rightFrame, right) = side(expr.getRightExpression, leftFrame)
    rightFrame.bind(op(left, right))

  private def aggregate(item: SelectItem[?]): Option[Aggregate] =
    unwrap(expressionOf(item)) match
      case function: Function =>
        if function.isDistinct || function.isUnique then unsupported(function)
        val name = function.getName
        if name == null then unsupported(function)
        else if name.equalsIgnoreCase("count") && countStar(function) then Some(Aggregate.Count)
        else if name.equalsIgnoreCase("count") then
          throw new IllegalArgumentException("count of a column is not supported")
        else if Set("sum", "min", "max").contains(name.toLowerCase) then
          val args = arguments(function)
          if args.size != 1 then unsupported(function)
          else Some(Aggregate.Of(name.toLowerCase, args.head))
        else unsupported(function)
      case _ => None

  private enum Aggregate:
    case Count
    case Of(name: String, expr: Expression)

  private def countStar(function: Function): Boolean =
    val args = arguments(function)
    function.isAllColumns || args.isEmpty || args.forall {
      case _: AllColumns => true
      case _ => false
    }

  private def arguments(function: Function): List[Expression] =
    val parameters = function.getParameters
    if parameters == null then Nil
    else parameters.asScala.toList.collect { case expression: Expression => expression }

  private def keys(
      list: net.sf.jsqlparser.expression.operators.relational.ExpressionList[?],
      frame: Frame,
  ): List[Int] =
    if list == null then Nil
    else
      list.asScala.toList.map {
        case expression: Expression =>
          plainColumn(expression, frame) match
            case Some(ordinal) =>
              kindAt(frame, ordinal) match
                case ColumnKind.Int32 | ColumnKind.Int64 => ordinal
                case other =>
                  throw new IllegalArgumentException(s"group key $other is not supported")
            case None => unsupported(expression)
        case other => unsupported(other)
      }

  private def plainColumn(expr: Expression, frame: Frame): Option[Int] = unwrap(expr) match
    case column: Column => Some(columnOrdinal(frame, column))
    case _ => None

  private def typedLiteral(expr: Expression, kind: ColumnKind): Option[ColumnLiteral] =
    unwrap(expr) match
      case value: LongValue => Some(literalFor(value.getValue, kind))
      case value: SignedExpression if value.getSign == '-' =>
        value.getExpression match
          case inner: LongValue => Some(literalFor(-inner.getValue, kind))
          case _ => None
      case value: DoubleValue =>
        kind match
          case ColumnKind.Float64 => Some(ColumnLiteral.Float64(value.getValue))
          case _ => None
      case _: StringValue => unsupported(expr)
      case _ => None

  private def literalFor(value: Long, kind: ColumnKind): ColumnLiteral = kind match
    case ColumnKind.Int32 =>
      if value < Int.MinValue || value > Int.MaxValue then
        throw new IllegalArgumentException(s"$value does not fit an int column")
      ColumnLiteral.Int32(value.toInt)
    case ColumnKind.Int64 => ColumnLiteral.Int64(value)
    case ColumnKind.Float64 => ColumnLiteral.Float64(value.toDouble)
    case ColumnKind.Decimal(precision, scale) =>
      if scale != 0 then
        throw new IllegalArgumentException("a decimal literal with a scale is not supported")
      DecimalRules.check(precision, scale)
      if !DecimalRules.fits(value, precision) then
        throw new IllegalArgumentException(s"$value does not fit decimal($precision, $scale)")
      ColumnLiteral.Decimal(precision, scale, value)
    case other => throw new IllegalArgumentException(s"$other is not supported")

  private def intLiteral(value: Long): ColumnLiteral =
    if value >= Int.MinValue && value <= Int.MaxValue then ColumnLiteral.Int32(value.toInt)
    else ColumnLiteral.Int64(value)

  private def unwrap(expr: Expression): Expression = expr match
    case wrapped: Parenthesis =>
      val inner = wrapped.getExpression
      if inner == null then expr else unwrap(inner)
    case wrapped: ParenthesedExpressionList[?] if wrapped.size == 1 =>
      wrapped.get(0) match
        case inner: Expression => unwrap(inner)
        case _ => expr
    case _ => expr

  private def itemsOf(plain: PlainSelect): List[SelectItem[?]] =
    val items = plain.getSelectItems
    if items == null then Nil else items.asScala.toList

  private def expressionOf(item: SelectItem[?]): Expression =
    val expression = item.getExpression
    if expression == null then unsupported(item) else expression

  private def outputName(item: SelectItem[?], frame: Frame, ordinal: Int): String =
    val alias = item.getAlias
    if alias != null && alias.getName != null && alias.getName.nonEmpty then alias.getName
    else kindName(frame, ordinal)

  private def kindName(frame: Frame, ordinal: Int): String =
    schema(frame).lift(ordinal).map(_.name).getOrElse(s"e${frame.next}")

  private def columnOrdinal(frame: Frame, column: Column): Int =
    val name = column.getColumnName
    if name == null then unsupported(column)
    else
      val found = schema(frame).indexWhere(field => field.name.equalsIgnoreCase(name))
      if found < 0 then throw new IllegalArgumentException(s"column $name is not in the table")
      else found

  private def kindAt(frame: Frame, ordinal: Int): ColumnKind =
    schema(frame)
      .lift(ordinal)
      .map(_.kind)
      .getOrElse(
        throw new IllegalArgumentException(s"column $ordinal is not in the table"),
      )

  private def schema(frame: Frame): Vector[ColumnField] =
    WideFields
      .known(frame.rows.plan)
      .getOrElse(
        throw new IllegalArgumentException("the table has no wide schema"),
      )

  private def tableName(from: FromItem): String = from match
    case table: Table =>
      val name = table.getName
      if name == null || name.isEmpty then unsupported(table) else name
    case other => unsupported(other)

  private def resolve(
      tables: Map[String, DistCollection[WideRow]],
      name: String,
  ): DistCollection[WideRow] =
    tables
      .collectFirst {
        case (table, rows) if table.equalsIgnoreCase(name) => rows
      }
      .getOrElse(throw new IllegalArgumentException(s"table $name is not registered"))

  private def unsupported(node: AnyRef): Nothing =
    throw new IllegalArgumentException(s"${node.getClass.getSimpleName} is not supported")

  private final case class Frame(rows: DistCollection[WideRow], next: Int):
    def bind(expr: ColumnExpr): (Frame, Int) =
      val updated = rows.compute(expr, s"e$next")
      (copy(rows = updated, next = next + 1), nextColumn(updated))

    def filter(ordinal: Int, compare: ColumnCompare): Frame =
      val kind = kindAt(this, ordinal)
      if !compare.allows(kind) then
        throw new IllegalArgumentException(s"${compare.op} on $kind is not supported")
      copy(rows = rows.filterColumn(ordinal, compare))

    private def nextColumn(updated: DistCollection[WideRow]): Int =
      WideFields
        .known(updated.plan)
        .map(_.size - 1)
        .getOrElse(
          throw new IllegalArgumentException("a computed column did not append"),
        )
