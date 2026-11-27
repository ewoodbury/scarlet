package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.{Operator, ReducerWitness}
import com.ewoodbury.scarlet.execution.schema.{Bound, ColumnBinding, KernelChoice, Output}

/**
 * Lowers a typed key sort and reports its layout.
 *
 * [[Plan.SortByIntKeyOp]] lowers when its child is a scan of int pairs, filters over that scan,
 * one int-pair [[PhysicalOp.Project]] on that scan or filter stack, one key or value filter
 * immediately above that project, one [[PhysicalOp.MapKey]] on that scan or filter stack, or one
 * key or value filter immediately above that map, to [[PhysicalOp.SortByIntKey]]. Neither function
 * is read. A null input key stays null, is not passed to the key function or a key predicate, and
 * orders as zero. [[pairMapKeyOnScan]] names that map. [[slotAboveMapKey]] names that filter.
 * [[FilterChain.selected]] returns `None` for it, and [[com.ewoodbury.scarlet.columnar.RankSplit]]
 * sorts the published pairs. [[Plan.SortByIntValueDescOp]] lowers for a scan, filters over that
 * scan, one int-pair [[PhysicalOp.Project]] on that scan or filter stack, one key or value filter
 * immediately above that project, one [[PhysicalOp.MapKey]] on that scan or filter stack, one key
 * or value filter immediately above that map, or one int [[PhysicalOp.MapValue]] on that scan or
 * filter stack, to [[PhysicalOp.SortByIntValueDesc]]. [[pairProjectOnScan]] names that project.
 * [[intSlotAboveProject]] names the slot above it. [[pairMapKeyOnScan]] names the key map.
 * [[slotAboveMapKey]] names that filter. [[pairMapValueOnScan]] names the value map. Neither
 * function is read. [[com.ewoodbury.scarlet.columnar.IntTopN]] orders the published pairs, value
 * descending then key ascending. One key or value filter immediately above that project keeps the
 * index when it names the projected rows, and [[IntTopN]] orders those rows. A null input key
 * stays null, is not passed to the key function, and orders as zero. A null value stays null, is
 * not passed to `mapValues`, and orders as zero. [[Plan.SortByLongKeyOp]] lowers for a scan of
 * long pairs, filters over that scan, one long-pair [[PhysicalOp.Project]] on that scan or filter
 * stack, or one key or value filter immediately above that project, to
 * [[PhysicalOp.SortByLongKey]]. [[LongPairSlot.oneProject]] names that project. Neither function
 * is read. A bare project publishes both columns and
 * [[com.ewoodbury.scarlet.columnar.RankSplitLong]] sorts those batches. One key or value filter
 * above that project sorts the survivor index of the projected batches. The slot is not applied to
 * the scan under the project. [[com.ewoodbury.scarlet.columnar.RankSplitLong]] sorts that index
 * and does not call `visitKeys`. [[Plan.SortByStringKeyOp]] lowers for a scan of string pairs,
 * filters over that scan, one string-pair [[PhysicalOp.Project]] on that scan or filter stack, or
 * one key or value filter immediately above that project, to [[PhysicalOp.SortByStringKey]].
 * Neither function is read. The sort reads the published batches through
 * [[com.ewoodbury.scarlet.columnar.StringRankSplit]] and does not call `visitKeys`. A second map,
 * a second slot, a whole-row filter above the int project, a second slot or a whole-row filter
 * above the long project, and a second slot or a whole-row filter above the string project stay on
 * the row path. `mapValues` under an int, long, or string key sort stays on the row path. A second
 * map, a slot above that `mapValues`, a second slot, a whole-row filter, and a project under the
 * value-sort key map stay on the row path. A second slot, a whole-row filter, and a second map
 * above [[PhysicalOp.MapKey]] under a key sort stay on the row path. [[PhysicalOp.MapKey]] under a
 * long key sort stays on the row path. [[PhysicalOp.MapValue]] under a long or string key sort
 * stays on the row path. Any other map, aggregate, or join under a sort stays on the row path. A
 * filter above a second project, a map, or a join does not lower. [[acceptsAsChild]] refuses every
 * sort, so a join, aggregate, or projection above a sort stays on the row path. One row filter
 * above an int, long, or string key sort is admitted only for a fully present key column. A slot
 * filter, including a long-pair row filter and a whole-row string filter, sorts the survivor
 * index. Over the budget, with spill on, that index is spilled. A long sort spills through the
 * same budget as an int sort. The physical op stays [[PhysicalOp.SortByLongKey]]. A string sort
 * spills through that same budget. The physical op stays [[PhysicalOp.SortByStringKey]].
 *
 * The sorts share one ordering tag: range distribution on the row key, ascending by effective key.
 * A null int or long key orders as zero and stays null. A null string key orders as `""` and stays
 * null. The caller supplies the columnar layout. Int pairs and long pairs use the same layout
 * today. String pairs use [[Layout.StringPair]].
 *
 * Each lower asks [[com.ewoodbury.scarlet.execution.schema.ColumnBinding]] on the child layout
 * before it builds the sort. [[scala.None]] from the binder is [[scala.None]] from lower. A layout
 * the sort already accepts still builds the same [[PhysicalOp]] class. The shape checks stay: a
 * second map, a second slot, and a whole-row filter are not a different layout, and they stay on
 * the row path. [[com.ewoodbury.scarlet.execution.schema.Describe.tag]] is not this decision.
 */
private[execution] object SortLowering:

  /** A parent cannot wrap a sort. The sort itself is lowered by [[ColumnarPlanner.lower]]. */
  def acceptsAsChild(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.SortByIntKey | _: PhysicalOp.SortByIntValueDesc |
        _: PhysicalOp.SortByLongKey | _: PhysicalOp.SortByStringKey |
        _: PhysicalOp.SortByLongValueDesc | _: PhysicalOp.SortByStringValueDesc =>
      false
    case _ => true

  def lowerInt(
      source: Plan[(Int, Int)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerKeyed(
      lowerPlan(source),
      Operator.SortByIntKey,
      "SortByIntKeyOp",
      acceptsIntKeyChild,
      PhysicalOp.SortByIntKey(_),
    )

  def lowerIntValueDesc(
      source: Plan[(Int, Int)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerKeyed(
      lowerPlan(source),
      Operator.SortByIntValueDesc,
      "SortByIntValueDescOp",
      acceptsValueDescChild,
      PhysicalOp.SortByIntValueDesc(_),
    )

  def lowerLong(
      source: Plan[(Long, Long)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerKeyed(
      lowerPlan(source),
      Operator.SortByLongKey,
      "SortByLongKeyOp",
      acceptsLongKeyChild,
      PhysicalOp.SortByLongKey(_),
    )

  def lowerString(
      source: Plan[(String, Int)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerKeyed(
      lowerPlan(source),
      Operator.SortByStringKey,
      "SortByStringKeyOp",
      acceptsStringKeyChild,
      PhysicalOp.SortByStringKey(_),
    )

  def lowerLongValue(
      source: Plan[(Long, Long)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerKeyed(
      lowerPlan(source),
      Operator.SortByLongValueDesc,
      "SortByLongValueDescOp",
      acceptsLongValueChild,
      PhysicalOp.SortByLongValueDesc(_),
    )

  def lowerStringValue(
      source: Plan[(String, Int)],
      lowerPlan: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerKeyed(
      lowerPlan(source),
      Operator.SortByStringValueDesc,
      "SortByStringValueDescOp",
      acceptsStringValueChild,
      PhysicalOp.SortByStringValueDesc(_),
    )

  /**
   * Ask [[ColumnBinding]] before building. [[scala.None]] from the binder stays [[scala.None]]. A
   * tag the sort accepts still has to pass the shape check, which refuses a second map, a second
   * slot, and a whole-row filter. Those shapes share a layout with a scan the sort already runs.
   */
  private def lowerKeyed(
      lowered: Option[PhysicalOp],
      operator: Operator,
      node: String,
      accepts: PhysicalOp => Boolean,
      build: PhysicalOp => PhysicalOp,
  ): Option[PhysicalOp] =
    lowered match
      case Some(child) if tagged(child, operator) && accepts(child) =>
        Some(build(child))
      case Some(child) =>
        LoweringTrace.note(node, child.kind, "SortLowering")
        None
      case None =>
        LoweringTrace.noteIfEmpty(node, "SortLowering")
        None

  /** Binder tag for `child`, or false when the layout is one this sort refuses. */
  private def tagged(child: PhysicalOp, operator: Operator): Boolean =
    asked(child, operator) match
      case Some(tag) => sameSort(operator, tag)
      case None => false

  /**
   * [[ColumnBinding.choose]] on the child columns. An opaque child is [[scala.None]] and does not
   * call [[ColumnBinding.choose]]. The reducer witness is not a sort input.
   */
  private def asked(child: PhysicalOp, operator: Operator): Option[KernelChoice] =
    val bound = Bound.of(child, Vector.empty, ReducerWitness.NotApplicable)
    bound.output match
      case Output.Columns(columns) =>
        ColumnBinding.choose(
          columns,
          operator,
          ReducerWitness.NotApplicable,
          Bound.nulls(bound),
          Bound.side(bound),
        )
      case Output.IntLongPairs(_) | Output.Opaque(_) => None

  /** True when `tag` is the sort `operator` names. Any other tag is a refusal. */
  private def sameSort(operator: Operator, tag: KernelChoice): Boolean =
    (operator, tag) match
      case (Operator.SortByIntKey, KernelChoice.SortByIntKey) => true
      case (Operator.SortByIntValueDesc, KernelChoice.SortByIntValueDesc) => true
      case (Operator.SortByLongKey, KernelChoice.SortByLongKey) => true
      case (Operator.SortByStringKey, KernelChoice.SortByStringKey) => true
      case (Operator.SortByLongValueDesc, KernelChoice.SortByLongValueDesc) => true
      case (Operator.SortByStringValueDesc, KernelChoice.SortByStringValueDesc) => true
      case _ => false

  /**
   * A scan of int pairs, filters over that scan, one pair [[PhysicalOp.Project]] on that scan or
   * filter stack, one key or value filter immediately above that project, one
   * [[PhysicalOp.MapKey]] on that scan or filter stack, or one key or value filter immediately
   * above that map. [[pairMapKeyOnScan]] names the map. [[slotAboveMapKey]] names that filter.
   * Neither function is read. A second project is not a scan or a filter. A second slot, a
   * whole-row filter, a second map, and `mapValues` are not this shape. A long or string project
   * has another kind.
   */
  private def acceptsIntKeyChild(op: PhysicalOp): Boolean =
    acceptsScanOrFilters(op, PhysicalOp.Kind.Pairs) || pairProjectOnScan(op) ||
      intSlotAboveProject(op) || pairMapKeyOnScan(op) || slotAboveMapKey(op)

  /**
   * A scan of int pairs, filters over that scan, one int-pair project on that scan or filter
   * stack, one key or value filter immediately above that project, one [[PhysicalOp.MapKey]] on
   * that scan or filter stack, one key or value filter immediately above that map, or one int
   * [[PhysicalOp.MapValue]] on that scan or filter stack. [[pairProjectOnScan]] names the project.
   * [[intSlotAboveProject]] names the slot above it. [[pairMapKeyOnScan]] names the key map.
   * [[slotAboveMapKey]] names the filter. [[pairMapValueOnScan]] names the value map. Neither
   * function is read. A second slot, a whole-row filter, a second project, a second map, a slot
   * above the value map, and a project under the map are not this shape.
   */
  private def acceptsValueDescChild(op: PhysicalOp): Boolean =
    acceptsScanOrFilters(op, PhysicalOp.Kind.Pairs) || pairProjectOnScan(op) ||
      intSlotAboveProject(op) || pairMapKeyOnScan(op) || slotAboveMapKey(op) ||
      pairMapValueOnScan(op)

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
   * not read. [[FilterChain.selected]] returns `None` because [[MapIntKey.inTree]], so a key sort
   * and a value sort do not index the scan under the map.
   * [[com.ewoodbury.scarlet.columnar.RankSplit]] orders the published pairs for the key sort. A
   * second map and a project under the map are not this shape. One key or value filter above the
   * map is [[slotAboveMapKey]], and that slot is a key-sort child.
   */
  private[execution] def pairMapKeyOnScan(op: PhysicalOp): Boolean = op match
    case PhysicalOp.MapKey(child) if acceptsScanOrFilters(child, PhysicalOp.Kind.Pairs) =>
      true
    case _ => false

  /**
   * One int [[PhysicalOp.MapValue]] whose child is a scan or an int-pair filter stack. The
   * function is not read. [[FilterChain.selected]] returns `None` for this shape so a value sort
   * does not index the scan under the map. A slot above the map is not this shape.
   */
  private[execution] def pairMapValueOnScan(op: PhysicalOp): Boolean = op match
    case PhysicalOp.MapValue(child)
        if (op.kind eq PhysicalOp.Kind.Pairs) &&
          acceptsScanOrFilters(child, PhysicalOp.Kind.Pairs) =>
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
   * whole-row slot, a second slot, and a filter above anything else are not this shape. A key sort
   * and a value sort both admit this slot. The value sort keeps the index when it names the
   * projected rows.
   */
  private def intSlotAboveProject(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Filter(child, PhysicalOp.Slot.Key | PhysicalOp.Slot.Value) =>
      pairProjectOnScan(child)
    case _ => false

  /**
   * A scan of long pairs, filters over that scan, one long-pair project on that scan or filter
   * stack, or one key or value filter immediately above that project. [[LongPairSlot.oneProject]]
   * names the project. Neither function is read. A second project is not a scan or a filter.
   * `mapValues` is not a project. A second slot, a whole-row filter above the project, and
   * [[PhysicalOp.MapKey]] are not this shape.
   */
  private def acceptsLongKeyChild(op: PhysicalOp): Boolean =
    acceptsScanOrFilters(op, PhysicalOp.Kind.LongPairs) || LongPairSlot.oneProject(op) ||
      longSlotAboveProject(op)

  /**
   * A scan of long pairs, filters over that scan, one long-pair project, one key or value filter
   * above that project, or one long [[PhysicalOp.MapValue]] on that scan or filter stack. A slot
   * above the value map, a second map, and a whole-row filter above the project are not this
   * shape.
   */
  private def acceptsLongValueChild(op: PhysicalOp): Boolean =
    acceptsScanOrFilters(op, PhysicalOp.Kind.LongPairs) || LongPairSlot.oneProject(op) ||
      longSlotAboveProject(op) || longMapValueOnScan(op)

  /** One long [[PhysicalOp.MapValue]] on a scan or a long-pair filter stack. */
  private def longMapValueOnScan(op: PhysicalOp): Boolean = op match
    case PhysicalOp.MapValue(child)
        if (op.kind eq PhysicalOp.Kind.LongPairs) &&
          acceptsScanOrFilters(child, PhysicalOp.Kind.LongPairs) =>
      true
    case _ => false

  /**
   * One key or value slot whose child is [[LongPairSlot.oneProject]]. The predicate is not read.
   */
  private def longSlotAboveProject(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Filter(child, PhysicalOp.Slot.Key | PhysicalOp.Slot.Value) =>
      LongPairSlot.oneProject(child)
    case _ => false

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
   * A scan of string pairs, filters over that scan, one string-pair project, one key or value
   * filter above that project, or one string [[PhysicalOp.MapValue]] on that scan or filter stack.
   * A slot above the value map, a second map, and a whole-row filter above the project are not
   * this shape.
   */
  private def acceptsStringValueChild(op: PhysicalOp): Boolean =
    acceptsScanOrFilters(op, PhysicalOp.Kind.StringPairs) || StringPairSlot.oneProject(op) ||
      slotFilterAboveProject(op) || stringMapValueOnScan(op)

  /** One string [[PhysicalOp.MapValue]] on a scan or a string-pair filter stack. */
  private def stringMapValueOnScan(op: PhysicalOp): Boolean = op match
    case PhysicalOp.MapValue(child)
        if (op.kind eq PhysicalOp.Kind.StringPairs) &&
          acceptsScanOrFilters(child, PhysicalOp.Kind.StringPairs) =>
      true
    case _ => false

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
