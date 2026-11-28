package com.ewoodbury.scarlet.sql

import net.sf.jsqlparser.expression.{Expression, Function}
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.statement.select.{ParenthesedSelect, PlainSelect}

import com.ewoodbury.scarlet.core.*

/**
 * One equality with the minimum of one inner column.
 *
 * The inner rows share one outer key. Local filters run first. The grouped minimum is the build,
 * and the outer value has to equal it. Any other correlated shape stays out.
 */
private object MinMatch:

  def apply(expr: Expression, frame: SqlConvert.Frame, outerValue: Int): Option[SqlConvert.Frame] =
    select(expr).flatMap { plain =>
      argument(plain).flatMap { measure =>
        opened(plain, frame).flatMap { inner =>
          linked(plain.getWhere, frame, inner).flatMap { (outerKey, innerKey, local) =>
            ordinal(measure, inner).flatMap { measureOrdinal =>
              if ready(frame, inner, outerKey, innerKey, outerValue, measureOrdinal) then
                Some(joined(frame, inner, outerKey, innerKey, outerValue, measureOrdinal, local))
              else None
            }
          }
        }
      }
    }

  private def select(expr: Expression): Option[PlainSelect] = SqlConvert.unwrap(expr) match
    case body: ParenthesedSelect =>
      val plain = body.getPlainSelect
      if plain == null || blocked(plain) then None else Some(plain)
    case _ => None

  private def blocked(plain: PlainSelect): Boolean =
    val joins = plain.getJoins
    val ordering = plain.getOrderByElements
    plain.getDistinct != null || plain.getGroupBy != null || plain.getHaving != null ||
    plain.getLimit != null || (joins != null && !joins.isEmpty) ||
    (ordering != null && !ordering.isEmpty)

  private def argument(plain: PlainSelect): Option[Expression] = SqlConvert.itemsOf(plain) match
    case List(item) =>
      SqlConvert.unwrap(SqlConvert.expressionOf(item)) match
        case function: Function if least(function) =>
          val args = SqlConvert.arguments(function)
          if args.size == 1 then Some(args.head) else None
        case _ => None
    case _ => None

  private def least(function: Function): Boolean =
    val name = function.getName
    name != null && name.equalsIgnoreCase("min") && !function.isDistinct && !function.isUnique

  private def opened(plain: PlainSelect, frame: SqlConvert.Frame): Option[SqlConvert.Frame] =
    val from = plain.getFromItem
    if from == null then None
    else
      val name = SqlConvert.tableName(from)
      Some(SqlConvert.Frame(SqlConvert.resolve(frame.tables, name), 0, Set(name), frame.tables))

  private def linked(
      where: Expression,
      frame: SqlConvert.Frame,
      inner: SqlConvert.Frame,
  ): Option[(Int, Int, Vector[Expression])] =
    if where == null then None
    else
      val (links, local) =
        SqlConvert
          .conjuncts(where)
          .foldLeft((Vector.empty[(Int, Int)], Vector.empty[Expression])) {
            case ((found, rest), part) =>
              SqlConvert.equiLink(part, frame, inner) match
                case Some(pair) => (found :+ pair, rest)
                case None if SqlConvert.mentionsOuter(part, frame) => (found :+ ((-1, -1)), rest)
                case None => (found, rest :+ part)
          }
      links match
        case Vector((outerKey, innerKey)) if outerKey >= 0 => Some((outerKey, innerKey, local))
        case _ => None

  private def ordinal(measure: Expression, inner: SqlConvert.Frame): Option[Int] =
    SqlConvert.unwrap(measure) match
      case column: Column =>
        val table = column.getTable
        val qualifier = if table == null then null else table.getName
        val named = inner.names.exists(name => name.equalsIgnoreCase(qualifier))
        if qualifier != null && !named then None else Some(SqlConvert.columnOrdinal(inner, column))
      case _ => None

  private def ready(
      frame: SqlConvert.Frame,
      inner: SqlConvert.Frame,
      outerKey: Int,
      innerKey: Int,
      outerValue: Int,
      measureOrdinal: Int,
  ): Boolean =
    val outerKind = SqlConvert.kindAt(frame, outerValue)
    val measureKind = SqlConvert.kindAt(inner, measureOrdinal)
    SqlConvert.joinKey(SqlConvert.kindAt(frame, outerKey)) &&
    SqlConvert.joinKey(SqlConvert.kindAt(inner, innerKey)) && outerKind == measureKind &&
    (outerKind == ColumnKind.Int32 || outerKind == ColumnKind.Int64)

  private def joined(
      frame: SqlConvert.Frame,
      inner: SqlConvert.Frame,
      outerKey: Int,
      innerKey: Int,
      outerValue: Int,
      measureOrdinal: Int,
      local: Vector[Expression],
  ): SqlConvert.Frame =
    val filtered = local.foldLeft(inner)((current, part) => SqlConvert.predicate(part, current))
    val mins = filtered.rows
      .groupColumns(innerKey, measureOrdinal)
      .projectColumns(Vector((0, "k"), (3, "least")))
    val width = SqlConvert.schema(frame).size
    val compared = frame.rows
      .joinColumns(mins, outerKey, 0)
      .compute(ColumnExpr.Compare(CompareOp.Eq, outerValue, width), "m")
    frame.copy(
      rows = compared.filterColumn(
        width + 1,
        ColumnCompare(CompareOp.Eq, ColumnLiteral.Bool(true)),
      ),
    )
