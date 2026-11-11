package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan

/**
 * Lowers a typed key sort and reports its layout.
 *
 * [[Plan.SortByIntKeyOp]] lowers when its child is a scan of int pairs, or filters over that scan,
 * to [[PhysicalOp.SortByIntKey]]. [[Plan.SortByLongKeyOp]] and [[Plan.SortByStringKeyOp]] lower
 * for the same shape on long pairs and string pairs, to [[PhysicalOp.SortByLongKey]] and
 * [[PhysicalOp.SortByStringKey]]. A map, aggregate, or join under any of these sorts stays on the
 * row path. A filter whose child is not that scan does not lower. [[acceptsAsChild]] refuses every
 * sort, so a join, aggregate, or projection above a sort stays on the row path. One row filter
 * above an int sort is admitted only for a fully present int key column. A slot filter, including
 * a long-pair row filter and a whole-row string filter, sorts the survivor index. Over the budget,
 * with spill on, that index is spilled. A long sort spills through the same budget as an int sort.
 * The physical op stays [[PhysicalOp.SortByLongKey]]. A string sort spills through that same
 * budget. The physical op stays [[PhysicalOp.SortByStringKey]].
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
      case Some(child) if acceptsScanOrFilters(child, PhysicalOp.Kind.Pairs) =>
        Some(PhysicalOp.SortByIntKey(child))
      case _ => None

  def lowerLong(
      source: Plan[(Long, Long)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerPlan(source) match
      case Some(child) if acceptsScanOrFilters(child, PhysicalOp.Kind.LongPairs) =>
        Some(PhysicalOp.SortByLongKey(child))
      case _ => None

  def lowerString(
      source: Plan[(String, Int)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerPlan(source) match
      case Some(child) if acceptsScanOrFilters(child, PhysicalOp.Kind.StringPairs) =>
        Some(PhysicalOp.SortByStringKey(child))
      case _ => None

  /**
   * A scan of `kind`, or filters over that scan. A map, aggregate, or join is not a sort child.
   */
  private def acceptsScanOrFilters(op: PhysicalOp, kind: PhysicalOp.Kind): Boolean = op match
    case PhysicalOp.Scan(scanned, _) if scanned eq kind => true
    case PhysicalOp.Filter(child, _) => acceptsScanOrFilters(child, kind)
    case _ => false

  def info(shuffleWidth: Int, layout: Layout): PartitioningInfo =
    PartitioningInfo(
      Distribution.Range(shuffleWidth, PartitioningInfo.RowKeyOrdinals),
      OrderingTag.Sorted(PartitioningInfo.RowKeyOrdinals, Seq(true)),
      layout,
    )
