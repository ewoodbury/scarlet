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
import net.sf.jsqlparser.statement.select.*

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.core.*

/**
 * One SELECT: registered tables, a left-deep chain of int or long equi-joins in the written order.
 * A left outer join keeps an unmatched left row. A right or full join names the node. An exists
 * joins the distinct inner key. A scalar subquery becomes the value it returns. A correlated min
 * joins that grouped min. Not exists and any other correlated scalar name that pattern.
 * Comparisons combine with AND, OR, and NOT. Arithmetic, count, sum, min, max, and an exact count
 * of distinct values. A string compares with JVM order. `LIKE` uses `%` and `_`, and it combines
 * with AND and NOT. OR of `LIKE` names the node. `length` is the UTF-16 length. A group is one or
 * two keys. A key is an int, a long, a string, or arithmetic on an int or a long.
 * `extract(minute from` an int64 epoch`)` and `date_trunc('minute',` that epoch`)` are computes. A
 * string measure emits count, min, and max. Order is one int, long, or string column, then a
 * limit. Anything else names its node.
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
    val joins = plain.getJoins
    if plain.getOffset != null then unsupported(plain.getOffset)
    if plain.getFetch != null then unsupported(plain.getFetch)
    if plain.getTop != null then unsupported(plain.getTop)
    if plain.getHaving != null then unsupported(plain.getHaving)
    val from = plain.getFromItem
    if from == null then unsupported(plain)
    val opened = openFrom(from, joins, tables)
    val filtered = if plain.getWhere == null then opened else predicate(plain.getWhere, opened)
    val grouped = plain.getGroupBy
    val selected =
      if grouped == null then select(plain, filtered)
      else
        val (keyed, ordinals, exprs) = keys(grouped.getGroupByExpressionList, filtered)
        group(plain, ordinals, exprs, keyed)
    finish(plain, selected)

  /** One int, long, or string order, then a row count. A scalar names the node that asked. */
  private def finish(plain: PlainSelect, result: SqlResult): SqlResult =
    limit(plain, orderBy(plain, result))

  private def orderBy(plain: PlainSelect, result: SqlResult): SqlResult =
    val elements = plain.getOrderByElements
    if elements == null || elements.isEmpty then result
    else if elements.size > 1 then unsupported(elements.get(1))
    else
      val element = elements.get(0)
      if element.getNullOrdering != null || element.isMysqlWithRollup then unsupported(element)
      result match
        case SqlResult.Value(_, _) => unsupported(element)
        case SqlResult.Rows(rows) =>
          val ordinal = sortOrdinal(element, rows)
          SqlResult.Rows(rows.sortColumn(ordinal, descending = !element.isAsc))

  private def limit(plain: PlainSelect, result: SqlResult): SqlResult =
    val node = plain.getLimit
    if node == null then result
    else
      result match
        case SqlResult.Value(_, _) => unsupported(node)
        case SqlResult.Rows(rows) =>
          SqlResult.Rows(rows.limitColumns(limitCount(node)))

  private def sortOrdinal(element: OrderByElement, rows: DistCollection[WideRow]): Int =
    val expr = element.getExpression
    if expr == null then unsupported(element)
    val fields = WideFields.known(rows.plan).getOrElse(unsupported(element))
    val ordinal = unwrap(expr) match
      case column: Column =>
        val name = column.getColumnName
        if name == null then unsupported(element)
        else fields.indexWhere(field => field.name.equalsIgnoreCase(name))
      case function: Function =>
        aggregateColumn(function) match
          case Some(name) => fields.indexWhere(field => field.name.equalsIgnoreCase(name))
          case None => unsupported(element)
      case _ => unsupported(element)
    if ordinal < 0 then unsupported(element)
    val field = fields.lift(ordinal).getOrElse(unsupported(element))
    field.kind match
      case ColumnKind.Int32 | ColumnKind.Int64 | ColumnKind.Utf8 => ordinal
      case _ => unsupported(element)

  /** `count`, `sum`, `min`, `max`, or `distinct` when that name is an output column. */
  private def aggregateColumn(function: Function): Option[String] =
    if function.isUnique && !function.isDistinct then None
    else if function.isDistinct then
      val name = function.getName
      if name != null && name.equalsIgnoreCase("count") && !countStar(function) &&
        arguments(function).size == 1
      then Some("distinct")
      else None
    else
      val name = function.getName
      if name == null then None
      else if name.equalsIgnoreCase("count") && countStar(function) then Some("count")
      else if name.equalsIgnoreCase("count") then None
      else if Set("sum", "min", "max").contains(name.toLowerCase) then
        val args = arguments(function)
        if args.size == 1 then Some(name.toLowerCase) else None
      else None

  private def limitCount(node: Limit): Int =
    if node.getOffset != null || node.isLimitAll || node.isLimitNull then unsupported(node)
    val by = node.getByExpressions
    if by != null && by.size > 0 then unsupported(node)
    node.getRowCount match
      case value: LongValue =>
        val count = value.getValue
        if count < 0L || count > Int.MaxValue.toLong then unsupported(node)
        else count.toInt
      case _ => unsupported(node)

  private def select(plain: PlainSelect, frame: Frame): SqlResult =
    val items = itemsOf(plain)
    if items.isEmpty then unsupported(plain)
    else if items.size == 1 && aggregate(items.head).isDefined then
      scalar(aggregate(items.head).get, frame)
    else if items.exists(item => aggregate(item).isDefined) then
      throw new IllegalArgumentException("several aggregates need a group")
    else SqlResult.Rows(project(items, frame).rows)

  private def group(
      plain: PlainSelect,
      keyOrdinals: List[Int],
      keyExprs: List[Expression],
      frame: Frame,
  ): SqlResult =
    if keyOrdinals.isEmpty || keyOrdinals.size > 2 then
      throw new IllegalArgumentException("a group needs one or two columns")
    val items = itemsOf(plain)
    val (measure, names, distinctOrdinal) = groupedSelect(items, keyOrdinals, keyExprs, frame)
    val grouped = (keyOrdinals.size, distinctOrdinal) match
      case (1, None) => frame.rows.groupColumns(keyOrdinals.head, measure)
      case (1, Some(column)) => frame.rows.groupDistinct(keyOrdinals.head, measure, column)
      case (_, None) => frame.rows.groupColumns(keyOrdinals.head, keyOrdinals(1), measure)
      case (_, Some(column)) =>
        frame.rows.groupDistinct(keyOrdinals.head, keyOrdinals(1), measure, column)
    SqlResult.Rows(projectGroup(grouped, keyOrdinals.size, names))

  /** Keys, then count, sum, min, max, and count(distinct), in that order, any suffix omitted. */
  private val aggregateOrder: Vector[String] = Vector("count", "sum", "min", "max", "distinct")

  private def groupedSelect(
      items: List[SelectItem[?]],
      keys: List[Int],
      keyExprs: List[Expression],
      frame: Frame,
  ): (Int, Vector[String], Option[Int]) =
    if items.size <= keys.size then groupList()
    keys.zip(keyExprs).zipWithIndex.foreach { (pair, index) =>
      val (ordinal, keyExpr) = pair
      if !selectsKey(expressionOf(items(index)), ordinal, keyExpr, frame) then groupList()
    }
    var cursor = 0
    val chosen = items.drop(keys.size).map { item =>
      val kind = aggregate(item).getOrElse(groupList())
      val name = kind match
        case Aggregate.Count => "count"
        case Aggregate.Of(function, _) => function
        case Aggregate.Distinct(_) =>
          if cursor > aggregateOrder.indexOf("distinct") then
            throw new IllegalArgumentException("a group has one distinct count")
          "distinct"
      val at = aggregateOrder.indexWhere(candidate => candidate == name, cursor)
      if at < 0 then groupList()
      cursor = at + 1
      (name, kind)
    }
    val measures = chosen.flatMap {
      case (_, Aggregate.Of(_, expr)) =>
        Some(
          plainColumn(expr, frame).getOrElse(
            throw new IllegalArgumentException("a grouped measure must be a column"),
          ),
        )
      case _ => None
    }
    val distincts = chosen.flatMap {
      case (_, Aggregate.Distinct(expr)) =>
        plainColumn(expr, frame) match
          case Some(ordinal) => Some(ordinal)
          case None => unsupported(unwrap(expr))
      case _ => None
    }
    if distincts.size > 1 then throw new IllegalArgumentException("a group has one distinct count")
    distincts.foreach { ordinal =>
      kindAt(frame, ordinal) match
        case ColumnKind.Int32 | ColumnKind.Int64 | ColumnKind.Utf8 => ()
        case other => throw new IllegalArgumentException(s"distinct of $other is not supported")
    }
    val measure =
      if measures.isEmpty then keys.head
      else if measures.distinct.size == 1 then measures.head
      else throw new IllegalArgumentException("sum, min, and max must use one column")
    if chosen.exists(item => item._1 == "sum") && kindAt(frame, measure) == ColumnKind.Utf8 then
      throw new IllegalArgumentException("sum of a string is not supported")
    (measure, chosen.map(_._1).toVector, distincts.headOption)

  private def groupList(): Nothing =
    throw new IllegalArgumentException(
      "a grouped select lists the keys, then count, sum, min, max, and count(distinct)",
    )

  /**
   * The kernel emits its aggregate columns, including distinct when the select asks for it. A
   * string measure has no sum. A shorter list drops the rest. Columns are found by name, so min
   * stays min when sum is absent.
   */
  private def projectGroup(
      rows: DistCollection[WideRow],
      keyCount: Int,
      names: Vector[String],
  ): DistCollection[WideRow] =
    val fields = WideFields.known(rows.plan).getOrElse(groupList())
    val emitted = fields.drop(keyCount).map(_.name)
    if names == emitted then rows
    else
      val keys = (0 until keyCount).toVector.map(ordinal => (ordinal, fields(ordinal).name))
      val kept = names.map { name =>
        val ordinal = fields.indexWhere(field => field.name == name, keyCount)
        if ordinal < 0 then groupList()
        (ordinal, fields(ordinal).name)
      }
      rows.projectColumns(keys ++ kept)

  private def scalar(kind: Aggregate, frame: Frame): SqlResult = kind match
    case Aggregate.Count =>
      SqlResult.Value(frame.rows.count(), frame.rows.plan)
    case Aggregate.Distinct(expr) =>
      plainColumn(expr, frame) match
        case Some(ordinal) =>
          kindAt(frame, ordinal) match
            case ColumnKind.Int32 | ColumnKind.Int64 | ColumnKind.Utf8 =>
              SqlResult.Value(frame.rows.countDistinctColumn(ordinal), frame.rows.plan)
            case other =>
              throw new IllegalArgumentException(s"distinct of $other is not supported")
        case None => unsupported(unwrap(expr))
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

  private[sql] def predicate(expr: Expression, frame: Frame): Frame = unwrap(expr) match
    case and: AndExpression =>
      predicate(and.getRightExpression, predicate(and.getLeftExpression, frame))
    case or: OrExpression =>
      val (built, ordinal) = boolExpr(or, frame)
      built.filter(ordinal, ColumnCompare(CompareOp.Eq, ColumnLiteral.Bool(true)))
    case not: NotExpression => negate(not.getExpression, frame)
    case exists: ExistsExpression => existsFrame(exists, frame)
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
    case like: LikeExpression =>
      val flipped = if like.isNot then CompareOp.Like else CompareOp.Unlike
      likeFrame(flipped, like, frame)
    case _: ExistsExpression =>
      throw new IllegalArgumentException("not exists is not supported")
    case other =>
      val (built, ordinal) = boolExpr(other, frame)
      val (negated, column) = built.bind(ColumnExpr.Not(ordinal))
      negated.filter(column, ColumnCompare(CompareOp.Eq, ColumnLiteral.Bool(true)))

  private final case class Side(name: String, ordinals: Vector[Int])

  private final case class Chain(frame: Frame, sides: Vector[Side])

  /** Left-deep equi-joins. Each right key is dropped. Earlier columns keep their ordinals. */
  private def openFrom(
      from: FromItem,
      joins: java.util.List[Join],
      tables: Map[String, DistCollection[WideRow]],
  ): Frame =
    val leftName = tableName(from)
    val left = resolve(tables, leftName)
    val start = Chain(
      Frame(left, 0, Set(leftName), tables),
      Vector(Side(leftName, fieldsOf(left).indices.toVector)),
    )
    val list = if joins == null then Nil else joins.asScala.toList
    list.foldLeft(start)((chain, join) => link(chain, join, tables)).frame

  private def link(
      chain: Chain,
      join: Join,
      tables: Map[String, DistCollection[WideRow]],
  ): Chain =
    if join.isRight || join.isFull || join.isCross || join.isSemi || join.isNatural ||
      join.isApply
    then unsupported(join)
    val rightItem = join.getRightItem
    if rightItem == null || join.getOnExpression == null then unsupported(join)
    val rightName = tableName(rightItem)
    if chain.sides.exists(side => side.name.equalsIgnoreCase(rightName)) then
      throw new IllegalArgumentException(s"table $rightName is already joined")
    val right = resolve(tables, rightName)
    val rightFields = fieldsOf(right)
    val (leftKey, rightKey) = unwrap(join.getOnExpression) match
      case eq: EqualsTo => equiKeys(eq, chain, rightName, rightFields)
      case other => unsupported(other)
    val leftFields = fieldsOf(chain.frame.rows)
    val leftKind = leftFields(leftKey).kind
    val rightKind = rightFields(rightKey).kind
    if !joinKey(leftKind) || !joinKey(rightKind) then
      throw new IllegalArgumentException("a join key must be an int or a long")
    val outer = join.isLeft || join.isOuter
    val joined =
      if (outer) then chain.frame.rows.leftJoinColumns(right, leftKey, rightKey)
      else chain.frame.rows.joinColumns(right, leftKey, rightKey)
    val width = leftFields.size
    val rightOrdinals = rightFields.indices
      .foldLeft((width, Vector.empty[Int])) { (state, index) =>
        val (cursor, found) = state
        if index == rightKey then state else (cursor + 1, found :+ cursor)
      }
      ._2
    Chain(
      Frame(joined, chain.frame.next, chain.frame.names + rightName, chain.frame.tables),
      chain.sides :+ Side(rightName, rightOrdinals),
    )

  private def equiKeys(
      eq: EqualsTo,
      chain: Chain,
      rightName: String,
      rightFields: Vector[ColumnField],
  ): (Int, Int) =
    val leftExpr = unwrap(eq.getLeftExpression)
    val rightExpr = unwrap(eq.getRightExpression)
    (leftExpr, rightExpr) match
      case (leftColumn: Column, rightColumn: Column) =>
        val leftPlace = locate(leftColumn, chain, rightName, rightFields)
        val rightPlace = locate(rightColumn, chain, rightName, rightFields)
        (leftPlace, rightPlace) match
          case (Some((true, leftKey)), Some((false, rightKey))) => (leftKey, rightKey)
          case (Some((false, rightKey)), Some((true, leftKey))) => (leftKey, rightKey)
          case _ => unsupported(eq)
      case _ => unsupported(eq)

  /** True means the column is on the accumulated left input. */
  private def locate(
      column: Column,
      chain: Chain,
      rightName: String,
      rightFields: Vector[ColumnField],
  ): Option[(Boolean, Int)] =
    val table = column.getTable
    val qualified = table != null && table.getName != null && table.getName.nonEmpty
    if qualified && table.getName.equalsIgnoreCase(rightName) then
      Some((false, ordinalIn(rightFields, column)))
    else if qualified then
      chain.sides.find(side => side.name.equalsIgnoreCase(table.getName)) match
        case Some(side) => Some((true, ordinalOn(chain, side, column)))
        case None => unsupported(column)
    else
      val name = column.getColumnName
      if name == null then unsupported(column)
      else
        val leftHits = chain.sides.flatMap { side =>
          ordinalNamed(chain, side, name).map(ordinal => (true, ordinal))
        }
        val rightHit = rightFields.zipWithIndex.collectFirst {
          case (field, ordinal) if field.name.equalsIgnoreCase(name) => (false, ordinal)
        }
        val hits = leftHits ++ rightHit.toList
        hits match
          case Vector((onLeft, ordinal)) => Some((onLeft, ordinal))
          case more if more.size > 1 =>
            throw new IllegalArgumentException(s"column $name is on both sides of the join")
          case _ => throw new IllegalArgumentException(s"column $name is not in the table")

  private def ordinalOn(chain: Chain, side: Side, column: Column): Int =
    val name = column.getColumnName
    if name == null then unsupported(column)
    else
      ordinalNamed(chain, side, name).getOrElse(
        throw new IllegalArgumentException(s"column $name is not in the table"),
      )

  private def ordinalNamed(chain: Chain, side: Side, name: String): Option[Int] =
    val fields = fieldsOf(chain.frame.rows)
    side.ordinals.find { ordinal =>
      fields.lift(ordinal).exists(field => field.name.equalsIgnoreCase(name))
    }

  private def ordinalIn(fields: Vector[ColumnField], column: Column): Int =
    val name = column.getColumnName
    val found = fields.indexWhere(field => field.name.equalsIgnoreCase(name))
    if found < 0 then throw new IllegalArgumentException(s"column $name is not in the table")
    else found

  private def fieldsOf(rows: DistCollection[WideRow]): Vector[ColumnField] =
    WideFields
      .known(rows.plan)
      .getOrElse(
        throw new IllegalArgumentException("the table has no wide schema"),
      )

  private[sql] def joinKey(kind: ColumnKind): Boolean = kind match
    case ColumnKind.Int32 | ColumnKind.Int64 => true
    case _ => false

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
    case like: LikeExpression =>
      val op = if like.isNot then CompareOp.Unlike else CompareOp.Like
      likeFrame(op, like, frame)
    case other => unsupported(other)

  private def likeFrame(op: CompareOp, like: LikeExpression, frame: Frame): Frame =
    if like.getEscape != null || like.isCaseInsensitive || like.isUseBinary then unsupported(like)
    if like.getLikeKeyWord != LikeExpression.KeyWord.LIKE then unsupported(like)
    wideOrBool(op, like.getLeftExpression, like.getRightExpression, frame)

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
        minOn(op, right, frame, ordinal).orElse {
          comparedLiteral(right, kindAt(frame, ordinal), frame).map { literal =>
            frame.filter(ordinal, ColumnCompare(op, literal))
          }
        }
      case None =>
        plainColumn(right, frame).flatMap { ordinal =>
          minOn(reflect(op), left, frame, ordinal).orElse {
            comparedLiteral(left, kindAt(frame, ordinal), frame).map { literal =>
              frame.filter(ordinal, ColumnCompare(reflect(op), literal))
            }
          }
        }

  /** Equality with one correlated minimum. Every other comparison stays a literal. */
  private def minOn(
      op: CompareOp,
      expr: Expression,
      frame: Frame,
      ordinal: Int,
  ): Option[Frame] =
    if op == CompareOp.Eq then MinMatch(expr, frame, ordinal) else None

  /** A literal, or the one value of an uncorrelated scalar subquery. */
  private def comparedLiteral(
      expr: Expression,
      kind: ColumnKind,
      frame: Frame,
  ): Option[ColumnLiteral] =
    typedLiteral(expr, kind).orElse(scalarLiteral(expr, kind, frame))

  private def scalarLiteral(
      expr: Expression,
      kind: ColumnKind,
      frame: Frame,
  ): Option[ColumnLiteral] = unwrap(expr) match
    case select: ParenthesedSelect => Some(literalFor(scalarValue(select, frame), kind))
    case _ => None

  private def scalarValue(select: ParenthesedSelect, outer: Frame): Long =
    val plain = select.getPlainSelect
    if plain == null then unsupported(select)
    else if correlated(plain, outer) then
      throw new IllegalArgumentException("a correlated subquery is not supported")
    else
      convert(plain, outer.tables) match
        case SqlResult.Value(result, _) => result
        case SqlResult.Rows(_) =>
          throw new IllegalArgumentException("a scalar subquery must be one aggregate")

  /** An outer column that the inner query does not own. A join is that pattern too. */
  private def correlated(plain: PlainSelect, outer: Frame): Boolean =
    val from = plain.getFromItem
    val joins = plain.getJoins
    if from == null then false
    else if joins != null && !joins.isEmpty then true
    else
      val name = tableName(from)
      val inner = Frame(resolve(outer.tables, name), 0, Set(name), outer.tables)
      val selected = itemsOf(plain).flatMap { item =>
        val expression = item.getExpression
        if expression == null then Nil else columnsIn(expression)
      }
      val columns = columnsIn(plain.getWhere) ++ columnsIn(plain.getHaving) ++ selected
      columns.exists(column => owned(outer, column).isDefined && owned(inner, column).isEmpty)

  /**
   * Exists keeps each outer row whose key is in the inner rows. The inner key is one group, so a
   * repeated match does not multiply the outer row.
   */
  private def existsFrame(exists: ExistsExpression, frame: Frame): Frame =
    if exists.isNot then throw new IllegalArgumentException("not exists is not supported")
    val right = exists.getRightExpression
    if right == null then unsupported(exists)
    val plain = unwrap(right) match
      case select: ParenthesedSelect =>
        val body = select.getPlainSelect
        if body == null then unsupported(select) else body
      case other => unsupported(other)
    if plain.getDistinct != null then unsupported(plain.getDistinct)
    val joins = plain.getJoins
    if joins != null && !joins.isEmpty then unsupported(joins.get(0))
    if plain.getGroupBy != null then unsupported(plain.getGroupBy)
    if plain.getHaving != null then unsupported(plain.getHaving)
    val ordering = plain.getOrderByElements
    if ordering != null && !ordering.isEmpty then unsupported(ordering.get(0))
    if plain.getLimit != null then unsupported(plain.getLimit)
    val from = plain.getFromItem
    if from == null then unsupported(plain)
    val innerName = tableName(from)
    val inner = Frame(resolve(frame.tables, innerName), 0, Set(innerName), frame.tables)
    if itemsOf(plain).exists(item => aggregate(item).isDefined) then
      throw new IllegalArgumentException("exists does not aggregate")
    val where = plain.getWhere
    if where == null then throw new IllegalArgumentException("exists needs one outer column")
    val (links, local) =
      conjuncts(where).foldLeft((Vector.empty[(Int, Int)], Vector.empty[Expression])) {
        case ((found, rest), part) =>
          equiLink(part, frame, inner) match
            case Some(pair) => (found :+ pair, rest)
            case None =>
              if mentionsOuter(part, frame) then
                throw new IllegalArgumentException("exists needs one outer column")
              else (found, rest :+ part)
      }
    val (outerKey, innerKey) = links match
      case Vector((outerOrdinal, innerOrdinal)) => (outerOrdinal, innerOrdinal)
      case _ => throw new IllegalArgumentException("exists needs one outer column")
    if !joinKey(kindAt(frame, outerKey)) || !joinKey(kindAt(inner, innerKey)) then
      throw new IllegalArgumentException("a join key must be an int or a long")
    val filtered = local.foldLeft(inner)((current, part) => predicate(part, current))
    val keyName = schema(inner).lift(innerKey).map(_.name).getOrElse("k")
    val built = filtered.rows
      .groupColumns(innerKey, innerKey)
      .projectColumns(Vector((0, keyName)))
    frame.copy(rows = frame.rows.joinColumns(built, outerKey, 0))

  private[sql] def conjuncts(expr: Expression): List[Expression] = unwrap(expr) match
    case and: AndExpression =>
      conjuncts(and.getLeftExpression) ++ conjuncts(and.getRightExpression)
    case other => List(other)

  /** Outer ordinal, then inner ordinal, when the equality is the exists key. */
  private[sql] def equiLink(expr: Expression, outer: Frame, inner: Frame): Option[(Int, Int)] =
    unwrap(expr) match
      case eq: EqualsTo =>
        (unwrap(eq.getLeftExpression), unwrap(eq.getRightExpression)) match
          case (left: Column, right: Column) => linkColumns(left, right, outer, inner)
          case _ => None
      case _ => None

  private def linkColumns(
      left: Column,
      right: Column,
      outer: Frame,
      inner: Frame,
  ): Option[(Int, Int)] =
    val leftOuter = owned(outer, left)
    val rightOuter = owned(outer, right)
    val leftInner = owned(inner, left)
    val rightInner = owned(inner, right)
    (leftOuter, rightInner, rightOuter, leftInner) match
      case (Some(outerKey), Some(innerKey), None, None) => Some((outerKey, innerKey))
      case (None, None, Some(outerKey), Some(innerKey)) => Some((outerKey, innerKey))
      case _ =>
        val shared = (leftOuter.isDefined && leftInner.isDefined) ||
          (rightOuter.isDefined && rightInner.isDefined)
        if shared then
          val name = Option(left.getColumnName).getOrElse(right.getColumnName)
          throw new IllegalArgumentException(s"column $name is on both sides of the exists")
        else None

  private[sql] def mentionsOuter(expr: Expression, outer: Frame): Boolean =
    columnsIn(expr).exists(column => owned(outer, column).isDefined)

  private def columnsIn(expr: Expression): List[Column] =
    if expr == null then Nil
    else
      unwrap(expr) match
        case column: Column => List(column)
        case function: Function => arguments(function).flatMap(columnsIn)
        case casted: CastExpression => columnsIn(casted.getLeftExpression)
        case extract: ExtractExpression => columnsIn(extract.getExpression)
        case signed: SignedExpression => columnsIn(signed.getExpression)
        case choice: CaseExpression =>
          val clauses = choice.getWhenClauses
          columnsIn(choice.getSwitchExpression) ++ columnsIn(choice.getElseExpression) ++
            (if clauses == null then Nil else clauses.asScala.toList.flatMap(columnsIn))
        case when: WhenClause =>
          columnsIn(when.getWhenExpression) ++ columnsIn(when.getThenExpression)
        case between: Between =>
          columnsIn(between.getLeftExpression) ++
            columnsIn(between.getBetweenExpressionStart) ++
            columnsIn(between.getBetweenExpressionEnd)
        case included: InExpression =>
          columnsIn(included.getLeftExpression) ++ columnsIn(included.getRightExpression)
        case binary: BinaryExpression =>
          columnsIn(binary.getLeftExpression) ++ columnsIn(binary.getRightExpression)
        case not: NotExpression => columnsIn(not.getExpression)
        case _ => Nil

  private def owned(frame: Frame, column: Column): Option[Int] =
    val name = column.getColumnName
    if name == null then None
    else
      val table = column.getTable
      val qualifier = if table == null then null else table.getName
      if qualifier != null && !frame.names.exists(known => known.equalsIgnoreCase(qualifier)) then
        None
      else
        val found = schema(frame).indexWhere(field => field.name.equalsIgnoreCase(name))
        if found < 0 then None else Some(found)

  /** A constant on the left is the same compare with the column on the left. */
  private def reflect(op: CompareOp): CompareOp = op match
    case CompareOp.Gt => CompareOp.Lt
    case CompareOp.Ge => CompareOp.Le
    case CompareOp.Lt => CompareOp.Gt
    case CompareOp.Le => CompareOp.Ge
    case CompareOp.Eq => CompareOp.Eq
    case CompareOp.Ne => CompareOp.Ne
    case CompareOp.Like | CompareOp.Unlike =>
      throw new IllegalArgumentException("a pattern is not ordered")

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
    case extract: ExtractExpression => minuteOf(extract, frame)
    case function: Function =>
      if dateTrunc(function) then truncMinute(function, frame)
      else if exactHost(function) then replaceHost(function, frame)
      else lengthOf(function, frame)
    case other => unsupported(other)

  private def minuteOf(extract: ExtractExpression, frame: Frame): (Frame, Int) =
    val name = extract.getName
    if name == null || !name.equalsIgnoreCase("minute") then unsupported(extract)
    val inner = extract.getExpression
    if inner == null then unsupported(extract)
    val (placed, ordinal) = side(inner, frame)
    kindAt(placed, ordinal) match
      case ColumnKind.Int64 => placed.bind(ColumnExpr.Minute(ordinal))
      case other => throw new IllegalArgumentException(s"minute of $other is not supported")

  private def dateTrunc(function: Function): Boolean =
    val name = function.getName
    name != null && name.equalsIgnoreCase("date_trunc")

  private def truncMinute(function: Function, frame: Frame): (Frame, Int) =
    if function.isDistinct || function.isUnique then unsupported(function)
    val args = arguments(function)
    if args.size != 2 then unsupported(function)
    args.headOption match
      case Some(text: StringValue) if text.getValue.equalsIgnoreCase("minute") => ()
      case Some(other) => unsupported(other)
      case None => unsupported(function)
    val column = args.drop(1).headOption.getOrElse(unsupported(function))
    val (placed, ordinal) = side(column, frame)
    kindAt(placed, ordinal) match
      case ColumnKind.Int64 => placed.bind(ColumnExpr.TruncMinute(ordinal))
      case other => throw new IllegalArgumentException(s"minute of $other is not supported")

  private def exactHost(function: Function): Boolean =
    val name = function.getName
    if name == null || !name.equalsIgnoreCase("regexp_replace") then false
    else if function.isDistinct || function.isUnique then false
    else
      val args = arguments(function)
      args.size == 3 &&
      textIs(args.lift(1), HostText.pattern) &&
      textIs(args.lift(2), HostText.replacement)

  private def textIs(expr: Option[Expression], text: String): Boolean = expr match
    case Some(value: StringValue) => value.getValue.equals(text)
    case _ => false

  private def replaceHost(function: Function, frame: Frame): (Frame, Int) =
    val column = arguments(function).headOption.getOrElse(unsupported(function))
    val (placed, ordinal) = side(column, frame)
    kindAt(placed, ordinal) match
      case ColumnKind.Utf8 => placed.bind(ColumnExpr.Host(ordinal))
      case other => throw new IllegalArgumentException(s"replace of $other is not supported")

  private def lengthOf(function: Function, frame: Frame): (Frame, Int) =
    val name = function.getName
    if function.isDistinct || function.isUnique || name == null || !name.equalsIgnoreCase("length")
    then unsupported(function)
    val args = arguments(function)
    if args.size != 1 then unsupported(function)
    val (placed, ordinal) = side(args.head, frame)
    placed.bind(ColumnExpr.Len(ordinal))

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
      case function: Function if dateTrunc(function) || exactHost(function) => None
      case function: Function =>
        if function.isDistinct then Some(distinctAggregate(function))
        else if function.isUnique then unsupported(function)
        else Some(plainAggregate(function))
      case _ => None

  private def distinctAggregate(function: Function): Aggregate =
    val name = function.getName
    if name == null || !name.equalsIgnoreCase("count") then unsupported(function)
    else if countStar(function) then unsupported(function)
    else
      val args = arguments(function)
      if args.size != 1 then unsupported(function)
      else Aggregate.Distinct(args.head)

  private def plainAggregate(function: Function): Aggregate =
    val name = function.getName
    if name == null then unsupported(function)
    else if name.equalsIgnoreCase("count") && countStar(function) then Aggregate.Count
    else if name.equalsIgnoreCase("count") then
      throw new IllegalArgumentException("count of a column is not supported")
    else if Set("sum", "min", "max").contains(name.toLowerCase) then
      val args = arguments(function)
      if args.size != 1 then unsupported(function)
      else Aggregate.Of(name.toLowerCase, args.head)
    else unsupported(function)

  private enum Aggregate:
    case Count
    case Of(name: String, expr: Expression)
    case Distinct(expr: Expression)

  private def countStar(function: Function): Boolean =
    val args = arguments(function)
    function.isAllColumns || args.isEmpty || args.forall {
      case _: AllColumns => true
      case _ => false
    }

  private[sql] def arguments(function: Function): List[Expression] =
    val parameters = function.getParameters
    if parameters == null then Nil
    else parameters.asScala.toList.collect { case expression: Expression => expression }

  private def keys(
      list: net.sf.jsqlparser.expression.operators.relational.ExpressionList[?],
      frame: Frame,
  ): (Frame, List[Int], List[Expression]) =
    if list == null then (frame, Nil, Nil)
    else
      val (bound, reversedOrdinals, reversedExprs) =
        list.asScala.toList.foldLeft((frame, List.empty[Int], List.empty[Expression])) {
          case ((current, ordinals, exprs), item) =>
            item match
              case expression: Expression =>
                val (next, ordinal) = keyOrdinal(expression, current)
                (next, ordinal :: ordinals, expression :: exprs)
              case other => unsupported(other)
        }
      (bound, reversedOrdinals.reverse, reversedExprs.reverse)

  /** A column key stays put. Arithmetic on an int or a long appends the compute, then groups. */
  private def keyOrdinal(expression: Expression, frame: Frame): (Frame, Int) =
    plainColumn(expression, frame) match
      case Some(ordinal) =>
        kindAt(frame, ordinal) match
          case ColumnKind.Int32 | ColumnKind.Int64 | ColumnKind.Utf8 => (frame, ordinal)
          case other => throw new IllegalArgumentException(s"group key $other is not supported")
      case None =>
        val bare = unwrap(expression)
        val (bound, ordinal) = bare match
          case extract: ExtractExpression => minuteOf(extract, frame)
          case function: Function if dateTrunc(function) => truncMinute(function, frame)
          case _ =>
            if !referencesColumn(bare) then unsupported(bare)
            arithmeticKey(bare, frame)
        kindAt(bound, ordinal) match
          case ColumnKind.Int32 | ColumnKind.Int64 => (bound, ordinal)
          case other => throw new IllegalArgumentException(s"group key $other is not supported")

  private def arithmeticKey(expr: Expression, frame: Frame): (Frame, Int) = expr match
    case add: Addition => binaryKey(add, frame, ColumnExpr.Add.apply)
    case sub: Subtraction => binaryKey(sub, frame, ColumnExpr.Sub.apply)
    case mul: Multiplication => binaryKey(mul, frame, ColumnExpr.Mul.apply)
    case div: Division => binaryKey(div, frame, ColumnExpr.Div.apply)
    case other => unsupported(other)

  private def binaryKey(
      expr: BinaryExpression,
      frame: Frame,
      op: (Int, Int) => ColumnExpr,
  ): (Frame, Int) =
    val (leftFrame, left) = keyLeaf(expr.getLeftExpression, frame)
    val (rightFrame, right) = keyLeaf(expr.getRightExpression, leftFrame)
    rightFrame.bind(op(left, right))

  private def keyLeaf(expr: Expression, frame: Frame): (Frame, Int) = unwrap(expr) match
    case column: Column =>
      val ordinal = columnOrdinal(frame, column)
      kindAt(frame, ordinal) match
        case ColumnKind.Int32 | ColumnKind.Int64 => (frame, ordinal)
        case other => throw new IllegalArgumentException(s"group key $other is not supported")
    case value: LongValue => frame.bind(ColumnExpr.Lit(intLiteral(value.getValue)))
    case value: SignedExpression if value.getSign == '-' =>
      value.getExpression match
        case inner: LongValue => frame.bind(ColumnExpr.Lit(intLiteral(-inner.getValue)))
        case other => unsupported(other)
    case other => arithmeticKey(other, frame)

  private def referencesColumn(expr: Expression): Boolean = unwrap(expr) match
    case _: Column => true
    case binary: BinaryExpression =>
      referencesColumn(binary.getLeftExpression) || referencesColumn(binary.getRightExpression)
    case _ => false

  private def selectsKey(
      expr: Expression,
      ordinal: Int,
      keyExpr: Expression,
      frame: Frame,
  ): Boolean =
    plainColumn(expr, frame) match
      case Some(found) => found == ordinal
      case None => sameExpression(expr, keyExpr)

  private def sameExpression(left: Expression, right: Expression): Boolean =
    (unwrap(left), unwrap(right)) match
      case (leftColumn: Column, rightColumn: Column) =>
        val leftName = leftColumn.getColumnName
        val rightName = rightColumn.getColumnName
        leftName != null && rightName != null && leftName.equalsIgnoreCase(rightName)
      case (leftValue: LongValue, rightValue: LongValue) =>
        leftValue.getValue == rightValue.getValue
      case (leftSigned: SignedExpression, rightSigned: SignedExpression) =>
        leftSigned.getSign == rightSigned.getSign && sameExpression(
          leftSigned.getExpression,
          rightSigned.getExpression,
        )
      case (leftAdd: Addition, rightAdd: Addition) => sameBinary(leftAdd, rightAdd)
      case (leftSub: Subtraction, rightSub: Subtraction) => sameBinary(leftSub, rightSub)
      case (leftMul: Multiplication, rightMul: Multiplication) => sameBinary(leftMul, rightMul)
      case (leftDiv: Division, rightDiv: Division) => sameBinary(leftDiv, rightDiv)
      case (leftExtract: ExtractExpression, rightExtract: ExtractExpression) =>
        val leftName = leftExtract.getName
        val rightName = rightExtract.getName
        val leftInner = leftExtract.getExpression
        val rightInner = rightExtract.getExpression
        leftName != null && rightName != null && leftName.equalsIgnoreCase(rightName) &&
        leftInner != null && rightInner != null && sameExpression(leftInner, rightInner)
      case (leftFunction: Function, rightFunction: Function)
          if dateTrunc(leftFunction) && dateTrunc(rightFunction) =>
        sameTrunc(leftFunction, rightFunction)
      case _ => false

  private def sameTrunc(left: Function, right: Function): Boolean =
    val leftArgs = arguments(left)
    val rightArgs = arguments(right)
    if leftArgs.length != 2 || rightArgs.length != 2 then false
    else
      val units = (leftArgs.headOption, rightArgs.headOption) match
        case (Some(leftText: StringValue), Some(rightText: StringValue)) =>
          val leftUnit = leftText.getValue
          val rightUnit = rightText.getValue
          leftUnit != null && rightUnit != null && leftUnit.equalsIgnoreCase(rightUnit)
        case _ => false
      val columns = (leftArgs.lift(1), rightArgs.lift(1)) match
        case (Some(leftColumn), Some(rightColumn)) => sameExpression(leftColumn, rightColumn)
        case _ => false
      units && columns

  private def sameBinary(left: BinaryExpression, right: BinaryExpression): Boolean =
    sameExpression(left.getLeftExpression, right.getLeftExpression) &&
      sameExpression(left.getRightExpression, right.getRightExpression)

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
      case value: StringValue =>
        kind match
          case ColumnKind.Utf8 => Some(ColumnLiteral.Utf8(value.getValue))
          case _ => None
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

  private[sql] def unwrap(expr: Expression): Expression = expr match
    case wrapped: Parenthesis =>
      val inner = wrapped.getExpression
      if inner == null then expr else unwrap(inner)
    case wrapped: ParenthesedExpressionList[?] if wrapped.size == 1 =>
      wrapped.get(0) match
        case inner: Expression => unwrap(inner)
        case _ => expr
    case _ => expr

  private[sql] def itemsOf(plain: PlainSelect): List[SelectItem[?]] =
    val items = plain.getSelectItems
    if items == null then Nil else items.asScala.toList

  private[sql] def expressionOf(item: SelectItem[?]): Expression =
    val expression = item.getExpression
    if expression == null then unsupported(item) else expression

  private def outputName(item: SelectItem[?], frame: Frame, ordinal: Int): String =
    val alias = item.getAlias
    if alias != null && alias.getName != null && alias.getName.nonEmpty then alias.getName
    else kindName(frame, ordinal)

  private def kindName(frame: Frame, ordinal: Int): String =
    schema(frame).lift(ordinal).map(_.name).getOrElse(s"e${frame.next}")

  private[sql] def columnOrdinal(frame: Frame, column: Column): Int =
    val name = column.getColumnName
    if name == null then unsupported(column)
    else
      val found = schema(frame).indexWhere(field => field.name.equalsIgnoreCase(name))
      if found < 0 then throw new IllegalArgumentException(s"column $name is not in the table")
      else found

  private[sql] def kindAt(frame: Frame, ordinal: Int): ColumnKind =
    schema(frame)
      .lift(ordinal)
      .map(_.kind)
      .getOrElse(
        throw new IllegalArgumentException(s"column $ordinal is not in the table"),
      )

  private[sql] def schema(frame: Frame): Vector[ColumnField] =
    WideFields
      .known(frame.rows.plan)
      .getOrElse(
        throw new IllegalArgumentException("the table has no wide schema"),
      )

  private[sql] def tableName(from: FromItem): String = from match
    case table: Table =>
      val name = table.getName
      if name == null || name.isEmpty then unsupported(table) else name
    case other => unsupported(other)

  private[sql] def resolve(
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

  private[sql] final case class Frame(
      rows: DistCollection[WideRow],
      next: Int,
      names: Set[String],
      tables: Map[String, DistCollection[WideRow]],
  ):
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
