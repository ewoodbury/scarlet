package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.ColumnReduce
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns

/**
 * Group count of `groupByKey` without value lists.
 *
 * The shape stays `GroupByKey(Exchange(...))`. `collect` still runs
 * [[com.ewoodbury.scarlet.columnar.GroupByKey]] and builds one value array per group. This count
 * is the occupied slots [[CountByKeyArms]] already publishes for that child:
 * [[com.ewoodbury.scarlet.columnar.FoldedCount]] or [[com.ewoodbury.scarlet.columnar.HashCount]]
 * on int pairs, and the long or string count table otherwise. That cardinality is the number of
 * keys [[com.ewoodbury.scarlet.columnar.GroupByKeyKeys]] would emit. A null key is dropped. A null
 * value still leaves its key. [[DistinctKeyCount]] adds a kept null back for `distinct`; this
 * count does not.
 */
private[execution] object GroupByKeyCount:

  /** Both pair columns. A value filter stays on ordinal 1. */
  private val ordinals: Set[Int] = Set(0, 1)

  def of(plan: Plan[_], op: PhysicalOp): Long = (plan, op) match
    case (
          Plan.GroupByKeyOp(source),
          PhysicalOp.GroupByKey(PhysicalOp.Exchange(child)),
        ) =>
      occupied(source, child)
    case _ =>
      throw new IllegalStateException("groupByKey count lost its exchange")

  private def occupied(source: Plan[_], child: PhysicalOp): Long = (source, child) match
    case (Plan.MapValuesOp(input, _), PhysicalOp.IntLongMapValue(under)) =>
      // The lists are not built. The int key under the map is the group count. The function
      // does not run. A null key is dropped.
      CountByKeyArms.groupRows(input, under, ordinals)
    case _ =>
      occupiedKind(source, child)

  private def occupiedKind(source: Plan[_], child: PhysicalOp): Long = child.kind match
    case PhysicalOp.Kind.Pairs =>
      CountByKeyArms.groupRows(source, child, ordinals)
    case PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.StringPairs =>
      rows(CountByKeyArms.columns(source, child, None, false, ordinals))
    case other =>
      throw new IllegalStateException(
        s"groupByKey count expected int, long, or string pairs, found $other",
      )

  private def rows(columns: Columns): Long = columns match
    case Columns.LongCounts(batches) => ColumnReduce.count(batches)
    case Columns.StringPairs(batches) => ColumnReduce.count(batches)
    case other =>
      throw new IllegalStateException(s"groupByKey count expected key counts, found $other")
