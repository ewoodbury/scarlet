package com.ewoodbury.scarlet.execution.schema

import com.ewoodbury.scarlet.columnar.LogicalType
import com.ewoodbury.scarlet.execution.{PhysicalOp, PhysicalSchema}

/**
 * Output columns of a [[PhysicalOp]], beside [[PhysicalOp.schema]].
 *
 * This is not [[com.ewoodbury.scarlet.execution.Layout]], the batch enum. `schema` and
 * [[PhysicalSchema.isIntLongAggregate]] stay the admission path. The planner does not read this
 * report. [[known]] reads the case class. A filter and an exchange copy the child. A pipeline, a
 * union, and a dedupe report [[PhysicalOp.kind]]. A join's three columns are the columns of its
 * kind. An outer join marks the missing value [[NullPolicy.Absence]]. A triple
 * [[PhysicalOp.Project]] is [[Layout.AssumedIntLong]]. [[PhysicalOp.GroupedDecode]] is opaque.
 * [[PhysicalOp.MapKey]] rewrites the key to [[ColumnType.Int32]] and copies the value.
 * [[PhysicalOp.MapValue]] reports the kind it already copies.
 */
private[execution] object Layout:

  /** `[Int32, Int64]` assumed for a project of triples. The function is not read. */
  val AssumedIntLong: Output =
    Output.Columns(
      Vector(
        ColumnLayout(ColumnType.Int32, NullPolicy.Nullable),
        ColumnLayout(ColumnType.Int64, NullPolicy.Nullable),
      ),
    )

  /** What `op` already determines. Does not call [[ColumnarPlanner.lower]]. */
  def known(op: PhysicalOp): Output = op match
    case PhysicalOp.Scan(kind, _) =>
      columns(kind)
    case PhysicalOp.Filter(child, _) =>
      known(child)
    case PhysicalOp.Exchange(child) =>
      known(child)
    case PhysicalOp.Project(child) if triples(child) =>
      AssumedIntLong
    case PhysicalOp.Project(child) =>
      known(child)
    case PhysicalOp.MapKey(child) =>
      mapKey(child)
    case _: PhysicalOp.GroupedDecode =>
      Output.Opaque("grouped values")
    case node: PhysicalOp.Pipeline =>
      columns(node.kind)
    case node: PhysicalOp.Union =>
      columns(node.kind)
    case node: PhysicalOp.Dedupe =>
      columns(node.kind)
    case node: PhysicalOp.Column =>
      columns(node.kind)
    case node: PhysicalOp.MapValue =>
      columns(node.kind)
    case node: PhysicalOp.GroupByKey =>
      columns(node.kind)
    case node: PhysicalOp.CountByKey =>
      columns(node.kind)
    case node: PhysicalOp.CountSum =>
      columns(node.kind)
    case node: PhysicalOp.HashAggregate =>
      columns(node.kind)
    case node: PhysicalOp.LongHashAggregate =>
      columns(node.kind)
    case node: PhysicalOp.StringHashAggregate =>
      columns(node.kind)
    case node: PhysicalOp.JoinProbeSum =>
      columns(node.kind)
    case node: PhysicalOp.SortByIntKey =>
      columns(node.kind)
    case node: PhysicalOp.SortByIntValueDesc =>
      columns(node.kind)
    case node: PhysicalOp.SortByLongKey =>
      columns(node.kind)
    case node: PhysicalOp.SortByStringKey =>
      columns(node.kind)
    case node @ (_: PhysicalOp.HashJoin | _: PhysicalOp.SortMergeJoin |
        _: PhysicalOp.BroadcastJoin) =>
      join(node.kind, OuterSide.Inner)
    case node @ (_: PhysicalOp.LongHashJoin | _: PhysicalOp.LongBroadcastJoin |
        _: PhysicalOp.LongSortMergeJoin) =>
      join(node.kind, OuterSide.Inner)
    case node @ (_: PhysicalOp.StringHashJoin | _: PhysicalOp.StringBroadcastJoin |
        _: PhysicalOp.StringSortMergeJoin) =>
      join(node.kind, OuterSide.Inner)
    case node @ (_: PhysicalOp.HashLeftOuter | _: PhysicalOp.SortMergeLeftOuter |
        _: PhysicalOp.BroadcastLeftOuter) =>
      join(node.kind, OuterSide.Left)
    case node @ (_: PhysicalOp.HashRightOuter | _: PhysicalOp.SortMergeRightOuter |
        _: PhysicalOp.BroadcastRightOuter) =>
      join(node.kind, OuterSide.Right)
    case node @ (_: PhysicalOp.HashFullOuter | _: PhysicalOp.SortMergeFullOuter |
        _: PhysicalOp.BroadcastFullOuter) =>
      join(node.kind, OuterSide.Full)
    case node @ (_: PhysicalOp.LongHashLeftOuter | _: PhysicalOp.LongBroadcastLeftOuter |
        _: PhysicalOp.LongSortMergeLeftOuter) =>
      join(node.kind, OuterSide.Left)
    case node @ (_: PhysicalOp.LongHashRightOuter | _: PhysicalOp.LongBroadcastRightOuter |
        _: PhysicalOp.LongSortMergeRightOuter) =>
      join(node.kind, OuterSide.Right)
    case node @ (_: PhysicalOp.LongHashFullOuter | _: PhysicalOp.LongBroadcastFullOuter |
        _: PhysicalOp.LongSortMergeFullOuter) =>
      join(node.kind, OuterSide.Full)
    case node @ (_: PhysicalOp.StringHashLeftOuter | _: PhysicalOp.StringBroadcastLeftOuter |
        _: PhysicalOp.StringSortMergeLeftOuter) =>
      join(node.kind, OuterSide.Left)
    case node @ (_: PhysicalOp.StringHashRightOuter | _: PhysicalOp.StringBroadcastRightOuter |
        _: PhysicalOp.StringSortMergeRightOuter) =>
      join(node.kind, OuterSide.Right)
    case node @ (_: PhysicalOp.StringHashFullOuter | _: PhysicalOp.StringBroadcastFullOuter |
        _: PhysicalOp.StringSortMergeFullOuter) =>
      join(node.kind, OuterSide.Full)

  private def columns(kind: PhysicalOp.Kind): Output =
    Output.Columns(
      PhysicalSchema
        .ofKind(kind)
        .map { tpe =>
          ColumnLayout(columnType(tpe), NullPolicy.Nullable)
        }
        .toVector,
    )

  private def columnType(tpe: LogicalType): ColumnType = tpe match
    case LogicalType.Int32 => ColumnType.Int32
    case LogicalType.Int64 => ColumnType.Int64
    case LogicalType.Float64 => ColumnType.Float64
    case LogicalType.Bool => ColumnType.Bool
    case LogicalType.Utf8Dict => ColumnType.Utf8Dict

  private def triples(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Triples => true
    case _ => false

  /** Key becomes [[ColumnType.Int32]]. The value column is the child's. */
  private def mapKey(child: PhysicalOp): Output = known(child) match
    case Output.Columns(Vector(key, value, _*)) =>
      Output.Columns(Vector(ColumnLayout(ColumnType.Int32, key.nullPolicy), value))
    case Output.Opaque(reason) =>
      Output.Opaque(reason)
    case Output.Columns(_) =>
      Output.Opaque("map key child has fewer than 2 columns")

  /** Three columns. `side` marks the value that a missing partner leaves absent. */
  private def join(kind: PhysicalOp.Kind, side: OuterSide): Output = columns(kind) match
    case Output.Columns(Vector(key, left, right)) =>
      Output.Columns(mark(key, left, right, side))
    case other =>
      throw new IllegalStateException(s"join kind $kind reported $other")

  private def mark(
      key: ColumnLayout,
      left: ColumnLayout,
      right: ColumnLayout,
      side: OuterSide,
  ): Vector[ColumnLayout] = side match
    case OuterSide.Inner => Vector(key, left, right)
    case OuterSide.Left => Vector(key, left, absent(right))
    case OuterSide.Right => Vector(key, absent(left), right)
    case OuterSide.Full => Vector(key, absent(left), absent(right))

  private def absent(column: ColumnLayout): ColumnLayout =
    column.copy(nullPolicy = NullPolicy.Absence)

private[execution] enum ColumnType:
  case Int32, Int64, Float64, Bool, Utf8Dict

/**
 * `Absence` is a missing outer-join partner. It is not [[NullPolicy.Nullable]].
 */
private[execution] enum NullPolicy:
  case Present, Nullable, Absence

private[execution] final case class ColumnLayout(columnType: ColumnType, nullPolicy: NullPolicy)

private[execution] enum Output:
  case Columns(columns: Vector[ColumnLayout])
  case Opaque(reason: String)

private enum OuterSide:
  case Inner, Left, Right, Full
