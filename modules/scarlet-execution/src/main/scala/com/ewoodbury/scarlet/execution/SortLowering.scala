package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan

/**
 * Lowers a typed key sort and reports its layout.
 *
 * [[Plan.SortByIntKeyOp]] lowers when its child is a scan of int pairs, or filters over that scan,
 * to [[PhysicalOp.SortByIntKey]]. A map, aggregate, or join under the sort stays on the row path.
 * [[Plan.SortByLongKeyOp]] lowers only when its child is a scan of long pairs, to
 * [[PhysicalOp.SortByLongKey]]. [[Plan.SortByStringKeyOp]] lowers only when its child is a scan of
 * string pairs, to [[PhysicalOp.SortByStringKey]]. [[acceptsAsChild]] refuses every sort, so a
 * filter, join, aggregate, or projection above a sort stays on the row path. A long sort spills
 * through the same budget as an int sort. The physical op stays [[PhysicalOp.SortByLongKey]]. A
 * string sort spills through that same budget. The physical op stays
 * [[PhysicalOp.SortByStringKey]].
 *
 * The sorts share one ordering tag: range distribution on the row key, ascending by effective key.
 * A null int or long key orders as zero and stays null. A null string key orders as `""` and stays
 * null. The caller supplies the columnar layout. Int pairs and long pairs use the same layout
 * today. String pairs use [[Layout.StringPair]].
 */
private[execution] object SortLowering:

  /** A parent cannot wrap a sort. The sort itself is lowered by [[ColumnarPlanner.lower]]. */
  def acceptsAsChild(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.SortByIntKey | _: PhysicalOp.SortByLongKey |
        _: PhysicalOp.SortByStringKey =>
      false
    case _ => true

  def lowerInt(
      source: Plan[(Int, Int)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerPlan(source) match
      case Some(child) if acceptsIntSortChild(child) =>
        Some(PhysicalOp.SortByIntKey(child))
      case _ => None

  /** A pair scan, or filters over one. A map, aggregate, or join is not a sort child. */
  private def acceptsIntSortChild(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _) => true
    case PhysicalOp.Filter(child, _) => acceptsIntSortChild(child)
    case _ => false

  def lowerLong(
      source: Plan[(Long, Long)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerPlan(source) match
      case Some(scan @ PhysicalOp.Scan(PhysicalOp.Kind.LongPairs, _)) =>
        Some(PhysicalOp.SortByLongKey(scan))
      case _ => None

  def lowerString(
      source: Plan[(String, Int)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerPlan(source) match
      case Some(scan @ PhysicalOp.Scan(PhysicalOp.Kind.StringPairs, _)) =>
        Some(PhysicalOp.SortByStringKey(scan))
      case _ => None

  def info(shuffleWidth: Int, layout: Layout): PartitioningInfo =
    PartitioningInfo(
      Distribution.Range(shuffleWidth, PartitioningInfo.RowKeyOrdinals),
      OrderingTag.Sorted(PartitioningInfo.RowKeyOrdinals, Seq(true)),
      layout,
    )
