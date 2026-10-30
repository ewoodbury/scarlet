package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan

/**
 * Lowers a typed key sort and reports its layout.
 *
 * [[Plan.SortByIntKeyOp]] lowers only when its child is a scan of int pairs, to
 * [[PhysicalOp.SortByIntKey]]. [[Plan.SortByLongKeyOp]] lowers only when its child is a scan of
 * long pairs, to [[PhysicalOp.SortByLongKey]]. Any other child stays on the row path.
 * [[acceptsAsChild]] refuses either sort, so a filter, join, aggregate, or projection above a sort
 * stays on the row path. A long sort spills through the same budget as an int sort. The physical
 * op stays [[PhysicalOp.SortByLongKey]].
 *
 * Both sorts share one ordering tag: range distribution on the row key, ascending by effective
 * key. A null key orders as zero and stays null. The caller supplies the columnar layout. Int
 * pairs and long pairs use the same layout today.
 */
private[execution] object SortLowering:

  /** A parent cannot wrap a sort. The sort itself is lowered by [[ColumnarPlanner.lower]]. */
  def acceptsAsChild(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.SortByIntKey | _: PhysicalOp.SortByLongKey => false
    case _ => true

  def lowerInt(
      source: Plan[(Int, Int)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerPlan(source) match
      case Some(scan @ PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _)) =>
        Some(PhysicalOp.SortByIntKey(scan))
      case _ => None

  def lowerLong(
      source: Plan[(Long, Long)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerPlan(source) match
      case Some(scan @ PhysicalOp.Scan(PhysicalOp.Kind.LongPairs, _)) =>
        Some(PhysicalOp.SortByLongKey(scan))
      case _ => None

  def info(shuffleWidth: Int, layout: Layout): PartitioningInfo =
    PartitioningInfo(
      Distribution.Range(shuffleWidth, PartitioningInfo.RowKeyOrdinals),
      OrderingTag.Sorted(PartitioningInfo.RowKeyOrdinals, Seq(true)),
      layout,
    )
