package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan

/**
 * Lowers a typed key sort and reports its layout.
 *
 * [[Plan.SortByIntKeyOp]] lowers when its child is a scan of int pairs, filters over that scan, or
 * one int-pair [[PhysicalOp.Project]] on that scan or filter stack, to
 * [[PhysicalOp.SortByIntKey]]. The project function stays on the plan and is not read.
 * [[Plan.SortByIntValueDescOp]] lowers for a scan or filters only, to
 * [[PhysicalOp.SortByIntValueDesc]]. [[Plan.SortByLongKeyOp]] and [[Plan.SortByStringKeyOp]] lower
 * for that same scan-or-filter shape on long pairs and string pairs, to
 * [[PhysicalOp.SortByLongKey]] and [[PhysicalOp.SortByStringKey]]. `mapValues`, a second map, a
 * long or string project, a map under a value sort, and any other map, aggregate, or join under a
 * sort stay on the row path. A filter whose child is not that scan does not lower.
 * [[acceptsAsChild]] refuses every sort, so a join, aggregate, or projection above a sort stays on
 * the row path. One row filter above an int, long, or string key sort is admitted only for a fully
 * present key column. A slot filter, including a long-pair row filter and a whole-row string
 * filter, sorts the survivor index. Over the budget, with spill on, that index is spilled. A long
 * sort spills through the same budget as an int sort. The physical op stays
 * [[PhysicalOp.SortByLongKey]]. A string sort spills through that same budget. The physical op
 * stays [[PhysicalOp.SortByStringKey]].
 *
 * The sorts share one ordering tag: range distribution on the row key, ascending by effective key.
 * A null int or long key orders as zero and stays null. A null string key orders as `""` and stays
 * null. The caller supplies the columnar layout. Int pairs and long pairs use the same layout
 * today. String pairs use [[Layout.StringPair]].
 */
private[execution] object SortLowering:

  /** A parent cannot wrap a sort. The sort itself is lowered by [[ColumnarPlanner.lower]]. */
  def acceptsAsChild(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.SortByIntKey | _: PhysicalOp.SortByIntValueDesc |
        _: PhysicalOp.SortByLongKey | _: PhysicalOp.SortByStringKey =>
      false
    case _ => true

  def lowerInt(
      source: Plan[(Int, Int)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerPlan(source) match
      case Some(child) if acceptsIntKeyChild(child) =>
        Some(PhysicalOp.SortByIntKey(child))
      case Some(child) =>
        LoweringTrace.note("SortByIntKeyOp", child.kind, "SortLowering")
        None
      case None =>
        LoweringTrace.noteIfEmpty("SortByIntKeyOp", "SortLowering")
        None

  def lowerIntValueDesc(
      source: Plan[(Int, Int)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerPlan(source) match
      case Some(child) if acceptsScanOrFilters(child, PhysicalOp.Kind.Pairs) =>
        Some(PhysicalOp.SortByIntValueDesc(child))
      case Some(child) =>
        LoweringTrace.note("SortByIntValueDescOp", child.kind, "SortLowering")
        None
      case None =>
        LoweringTrace.noteIfEmpty("SortByIntValueDescOp", "SortLowering")
        None

  def lowerLong(
      source: Plan[(Long, Long)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerPlan(source) match
      case Some(child) if acceptsScanOrFilters(child, PhysicalOp.Kind.LongPairs) =>
        Some(PhysicalOp.SortByLongKey(child))
      case Some(child) =>
        LoweringTrace.note("SortByLongKeyOp", child.kind, "SortLowering")
        None
      case None =>
        LoweringTrace.noteIfEmpty("SortByLongKeyOp", "SortLowering")
        None

  def lowerString(
      source: Plan[(String, Int)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerPlan(source) match
      case Some(child) if acceptsScanOrFilters(child, PhysicalOp.Kind.StringPairs) =>
        Some(PhysicalOp.SortByStringKey(child))
      case Some(child) =>
        LoweringTrace.note("SortByStringKeyOp", child.kind, "SortLowering")
        None
      case None =>
        LoweringTrace.noteIfEmpty("SortByStringKeyOp", "SortLowering")
        None

  /**
   * A scan of int pairs, filters over that scan, or one pair [[PhysicalOp.Project]] on that scan
   * or filter stack. The project function is not read. A second project is not a scan or a filter.
   * A long or string project has another kind. `mapValues` is not a project.
   */
  private def acceptsIntKeyChild(op: PhysicalOp): Boolean =
    acceptsScanOrFilters(op, PhysicalOp.Kind.Pairs) || pairProjectOnScan(op)

  /** One int-pair project whose child is a scan or an int-pair filter stack. */
  private def pairProjectOnScan(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Project(child)
        if (op.kind eq PhysicalOp.Kind.Pairs) &&
          acceptsScanOrFilters(child, PhysicalOp.Kind.Pairs) =>
      true
    case _ => false

  /**
   * A scan of `kind`, or filters over that scan. A map, aggregate, or join is not a sort child.
   * [[acceptsIntKeyChild]] is the only sort that also admits one pair project.
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

  /** Range width is the shuffle width. Order is value descending, then key ascending. */
  def infoValueDesc(shuffleWidth: Int, layout: Layout): PartitioningInfo =
    PartitioningInfo(
      Distribution.Range(shuffleWidth, Seq(1, 0)),
      OrderingTag.Sorted(Seq(1, 0), Seq(false, true)),
      layout,
    )
