package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan

/**
 * Lowers a typed key sort and reports its layout.
 *
 * [[Plan.SortByIntKeyOp]] lowers when its child is a scan of int pairs, filters over that scan,
 * one int-pair [[PhysicalOp.Project]] on that scan or filter stack, or one key or value filter
 * immediately above that project, to [[PhysicalOp.SortByIntKey]]. Neither function is read.
 * [[Plan.SortByIntValueDescOp]] lowers for a scan, filters over that scan, one int-pair
 * [[PhysicalOp.Project]] on that scan or filter stack, one [[PhysicalOp.MapKey]] on that scan or
 * filter stack, or one key or value filter immediately above that map, to
 * [[PhysicalOp.SortByIntValueDesc]]. [[pairProjectOnScan]] names that project.
 * [[pairMapKeyOnScan]] names that map. [[slotAboveMapKey]] names that filter. Neither function is
 * read. [[com.ewoodbury.scarlet.columnar.IntTopN]] orders the published pairs, value descending
 * then key ascending. A null input key stays null, is not passed to the key function, and orders
 * as zero. A null value orders as zero and stays null. [[Plan.SortByLongKeyOp]] lowers for a scan
 * of long pairs, filters over that scan, or one long-pair [[PhysicalOp.Project]] on that scan or
 * filter stack, to [[PhysicalOp.SortByLongKey]]. [[LongPairSlot.oneProject]] names that child. The
 * function is not read. The sort reads the projected batches through
 * [[com.ewoodbury.scarlet.columnar.RankSplitLong]] and does not call `visitKeys`.
 * [[Plan.SortByStringKeyOp]] lowers for a scan of string pairs, filters over that scan, one
 * string-pair [[PhysicalOp.Project]] on that scan or filter stack, or one key or value filter
 * immediately above that project, to [[PhysicalOp.SortByStringKey]]. Neither function is read. The
 * sort reads the published batches through [[com.ewoodbury.scarlet.columnar.StringRankSplit]] and
 * does not call `visitKeys`. `mapValues`, a second map, a second slot, a whole-row filter above
 * the int project, a slot or whole-row filter above the long project, [[PhysicalOp.MapKey]], and a
 * second slot or a whole-row filter above the string project stay on the row path. A second slot,
 * a whole-row filter, a second map, `mapValues`, and a project under the value-sort map do too. A
 * slot above [[PhysicalOp.MapKey]] under a key sort stays on the row path. Any other map,
 * aggregate, or join under a sort stays on the row path. A filter above a second project, a map,
 * or a join does not lower. [[acceptsAsChild]] refuses every sort, so a join, aggregate, or
 * projection above a sort stays on the row path. One row filter above an int, long, or string key
 * sort is admitted only for a fully present key column. A slot filter, including a long-pair row
 * filter and a whole-row string filter, sorts the survivor index. Over the budget, with spill on,
 * that index is spilled. A long sort spills through the same budget as an int sort. The physical
 * op stays [[PhysicalOp.SortByLongKey]]. A string sort spills through that same budget. The
 * physical op stays [[PhysicalOp.SortByStringKey]].
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
      case Some(child) if acceptsValueDescChild(child) =>
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
      case Some(child) if acceptsLongKeyChild(child) =>
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
      case Some(child) if acceptsStringKeyChild(child) =>
        Some(PhysicalOp.SortByStringKey(child))
      case Some(child) =>
        LoweringTrace.note("SortByStringKeyOp", child.kind, "SortLowering")
        None
      case None =>
        LoweringTrace.noteIfEmpty("SortByStringKeyOp", "SortLowering")
        None

  /**
   * A scan of int pairs, filters over that scan, one pair [[PhysicalOp.Project]] on that scan or
   * filter stack, or one key or value filter immediately above that project. Neither function is
   * read. A second project is not a scan or a filter. A second slot, a whole-row filter, and
   * [[PhysicalOp.MapKey]] are not this shape. A long or string project has another kind.
   * `mapValues` is not a project.
   */
  private def acceptsIntKeyChild(op: PhysicalOp): Boolean =
    acceptsScanOrFilters(op, PhysicalOp.Kind.Pairs) || pairProjectOnScan(op) ||
      intSlotAboveProject(op)

  /**
   * A scan of int pairs, filters over that scan, one int-pair project on that scan or filter
   * stack, one [[PhysicalOp.MapKey]] on that scan or filter stack, or one key or value filter
   * immediately above that map. [[pairProjectOnScan]] names the project. [[pairMapKeyOnScan]]
   * names the map. [[slotAboveMapKey]] names the filter. Neither function is read. A second slot,
   * a whole-row filter, a second project, a second map, `mapValues`, and a project under the map
   * are not this shape. [[PhysicalOp.MapKey]] under a key sort is not [[acceptsIntKeyChild]].
   */
  private def acceptsValueDescChild(op: PhysicalOp): Boolean =
    acceptsScanOrFilters(op, PhysicalOp.Kind.Pairs) || pairProjectOnScan(op) ||
      pairMapKeyOnScan(op) || slotAboveMapKey(op)

  /**
   * One int-pair project whose child is a scan or an int-pair filter stack. The function is not
   * read. [[FilterChain.selected]] returns `None` for this shape so a value sort does not index
   * the scan under the project.
   */
  private[execution] def pairProjectOnScan(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Project(child)
        if (op.kind eq PhysicalOp.Kind.Pairs) &&
          acceptsScanOrFilters(child, PhysicalOp.Kind.Pairs) =>
      true
    case _ => false

  /**
   * One [[PhysicalOp.MapKey]] whose child is a scan or an int-pair filter stack. The function is
   * not read. [[FilterChain.selected]] returns `None` because [[MapIntKey.inTree]], so a value
   * sort does not index the scan under the map. A second map and a project under the map are not
   * this shape. One key or value filter above the map is [[slotAboveMapKey]].
   */
  private[execution] def pairMapKeyOnScan(op: PhysicalOp): Boolean = op match
    case PhysicalOp.MapKey(child) if acceptsScanOrFilters(child, PhysicalOp.Kind.Pairs) =>
      true
    case _ => false

  /**
   * One key or value slot whose child is [[pairMapKeyOnScan]]. The predicate is not read. A
   * whole-row slot, a second slot, and a filter above anything else are not this shape.
   */
  private[execution] def slotAboveMapKey(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Filter(child, PhysicalOp.Slot.Key | PhysicalOp.Slot.Value) =>
      pairMapKeyOnScan(child)
    case _ => false

  /**
   * One key or value slot whose child is [[pairProjectOnScan]]. The predicate is not read. A
   * whole-row slot, a second slot, and a filter above anything else are not this shape.
   */
  private def intSlotAboveProject(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Filter(child, PhysicalOp.Slot.Key | PhysicalOp.Slot.Value) =>
      pairProjectOnScan(child)
    case _ => false

  /**
   * A scan of long pairs, filters over that scan, or one long-pair project on that scan or filter
   * stack. [[LongPairSlot.oneProject]] names the project. The function is not read. A second
   * project is not a scan or a filter. `mapValues` is not a project. A slot or whole-row filter
   * above the project, and [[PhysicalOp.MapKey]], are not this shape.
   */
  private def acceptsLongKeyChild(op: PhysicalOp): Boolean =
    acceptsScanOrFilters(op, PhysicalOp.Kind.LongPairs) || LongPairSlot.oneProject(op)

  /**
   * A scan of string pairs, filters over that scan, one string-pair project on that scan or filter
   * stack, or one key or value filter immediately above that project. Neither function is read. A
   * second project is not a scan or a filter. `mapValues` is not a project. A second slot, a
   * whole-row filter above the project, and a filter above anything else are not this shape.
   */
  private def acceptsStringKeyChild(op: PhysicalOp): Boolean =
    acceptsScanOrFilters(op, PhysicalOp.Kind.StringPairs) || StringPairSlot.oneProject(op) ||
      slotFilterAboveProject(op)

  /**
   * One key or value slot whose child is [[StringPairSlot.oneProject]]. The predicate is not read.
   */
  private def slotFilterAboveProject(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Filter(child, PhysicalOp.Slot.Key | PhysicalOp.Slot.Value) =>
      StringPairSlot.oneProject(child)
    case _ => false

  /**
   * A scan of `kind`, or filters over that scan. A map, aggregate, or join is not a sort child.
   * [[acceptsIntKeyChild]], [[acceptsLongKeyChild]], and [[acceptsStringKeyChild]] are the sorts
   * that also admit one pair project.
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
