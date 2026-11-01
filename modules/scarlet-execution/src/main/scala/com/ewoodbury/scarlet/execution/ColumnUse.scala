package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan

/**
 * Ordinals a pair or triple scan has to encode.
 *
 * The walk starts at every ordinal of the root kind. Child demand is the parent set union the
 * ordinals this node reads. A projection replaces that set, including a projection that is not the
 * root. A map over a pair or a triple is opaque and reads every ordinal of that input. A node this
 * walk cannot follow demands every ordinal of its kind, which is wider than the true set and still
 * correct. One column is the optimization. [[ColumnarInterpreter]] reads the result from
 * [[encodePairs]] only. Grouped sum, long pairs, and the absence-preserving copy do not.
 */
private[execution] object ColumnUse:

  /** Ordinals to encode at the scan. `0` is the key, `1` the value, `2` the right join value. */
  def ordinals(plan: Plan[_], op: PhysicalOp): Set[Int] =
    walk(plan, op, fullWidth(op))

  private def walk(plan: Plan[_], op: PhysicalOp, parent: Set[Int]): Set[Int] = (plan, op) match
    case (Plan.Source(_, _), _: PhysicalOp.Scan) => parent
    case (Plan.KeysOp(source), PhysicalOp.Column(child, 0)) =>
      walk(source, child, Set(0))
    case (Plan.ValuesOp(source), PhysicalOp.Column(child, 1)) =>
      walk(source, child, Set(1))
    case (_, PhysicalOp.Column(child, ordinal)) =>
      planChild(plan).fold(fullWidth(op))(source => walk(source, child, Set(ordinal)))
    case (Plan.FilterKeysOp(source, _), PhysicalOp.Filter(child, PhysicalOp.Slot.Key)) =>
      walk(source, child, parent union Set(0))
    case (Plan.FilterValuesOp(source, _), PhysicalOp.Filter(child, PhysicalOp.Slot.Value)) =>
      walk(source, child, parent union Set(1))
    case (Plan.FilterOp(source, _), PhysicalOp.Filter(child, slot)) =>
      walk(source, child, parent union slotDemand(slot))
    case (Plan.MapValuesOp(source, _), PhysicalOp.MapValue(child)) =>
      walk(source, child, parent union Set(0, 1))
    case (Plan.MapOp(source, _), PhysicalOp.Project(child)) =>
      // The mapper sees the whole input record. An element project reads no pair ordinal.
      walk(source, child, parent union consumedOrdinals(child.kind))
    case (logical, PhysicalOp.Pipeline(child, length)) =>
      val (source, steps) = ColumnarPlanner.elementSteps(logical, length)
      // elementSteps collects a MapOp with no element-type check, so a pipeline over pairs keeps
      // every input ordinal. A short step list is a malformed tree and uses the same full width.
      if (steps.length < length) then fullWidth(op)
      else walk(source, child, parent union consumedOrdinals(child.kind))
    case (plan, PhysicalOp.Exchange(child)) =>
      walk(plan, child, parent union Set(0, 1))
    case (Plan.ReduceByKeyOp(source, _), PhysicalOp.HashAggregate(child)) =>
      walk(source, child, parent union Set(0, 1))
    case (Plan.ReduceByKeyOp(source, _), PhysicalOp.LongHashAggregate(child)) =>
      walk(source, child, parent union Set(0, 1))
    case (Plan.SortByIntKeyOp(source), PhysicalOp.SortByIntKey(child)) =>
      walk(source, child, parent union Set(0, 1))
    case (Plan.SortByLongKeyOp(source), PhysicalOp.SortByLongKey(child)) =>
      walk(source, child, parent union Set(0, 1))
    case _ =>
      joinSides(plan, op) match
        case Some((left, leftOp, right, rightOp)) =>
          // A join reads both columns of each pair input. A triple slot above the join does not
          // replace that demand with the slot's ordinal.
          walk(left, leftOp, Set(0, 1)) union walk(right, rightOp, Set(0, 1))
        case None =>
          fullWidth(op)

  /**
   * Ordinals an opaque map reads from `kind`. Element columns are not pair ordinals, so an int,
   * long, float, boolean, or string map adds nothing.
   */
  private def consumedOrdinals(kind: PhysicalOp.Kind): Set[Int] = kind match
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.LongPairs => Set(0, 1)
    case PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples => Set(0, 1, 2)
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Strings | PhysicalOp.Kind.Bools =>
      Set.empty[Int]

  private def slotDemand(slot: PhysicalOp.Slot): Set[Int] = slot match
    case PhysicalOp.Slot.Element => Set.empty[Int]
    case PhysicalOp.Slot.Key => Set(0)
    case PhysicalOp.Slot.Value => Set(1)
    case PhysicalOp.Slot.Row => Set(0, 1)
    case PhysicalOp.Slot.TripleKey | PhysicalOp.Slot.TripleLeftValue |
        PhysicalOp.Slot.TripleRightValue =>
      Set(0, 1, 2)

  private def fullWidth(op: PhysicalOp): Set[Int] = op.kind match
    case PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples => Set(0, 1, 2)
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Pairs | PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.Strings |
        PhysicalOp.Kind.Bools =>
      Set(0, 1)

  private def planChild(plan: Plan[_]): Option[Plan[_]] = plan match
    case Plan.KeysOp(source) => Some(source)
    case Plan.ValuesOp(source) => Some(source)
    case Plan.FilterOp(source, _) => Some(source)
    case Plan.FilterKeysOp(source, _) => Some(source)
    case Plan.FilterValuesOp(source, _) => Some(source)
    case Plan.MapOp(source, _) => Some(source)
    case Plan.MapValuesOp(source, _) => Some(source)
    case Plan.FlatMapOp(source, _) => Some(source)
    case Plan.FlatMapValuesOp(source, _) => Some(source)
    case Plan.MapPartitionsOp(source, _) => Some(source)
    case Plan.DistinctOp(source) => Some(source)
    case Plan.RepartitionOp(source, _) => Some(source)
    case Plan.CoalesceOp(source, _) => Some(source)
    case Plan.PartitionByOp(source, _) => Some(source)
    case Plan.GroupByKeyOp(source) => Some(source)
    case Plan.ReduceByKeyOp(source, _) => Some(source)
    case Plan.SortByOp(source, _, _) => Some(source)
    case Plan.SortByIntKeyOp(source) => Some(source)
    case Plan.SortByLongKeyOp(source) => Some(source)
    case Plan.Source(_, _) | Plan.UnionOp(_, _) | Plan.JoinOp(_, _, _) |
        Plan.LeftOuterJoinOp(_, _, _) | Plan.RightOuterJoinOp(_, _, _) |
        Plan.FullOuterJoinOp(_, _, _) | Plan.CoGroupOp(_, _) =>
      None

  private def joinSides(
      plan: Plan[_],
      op: PhysicalOp,
  ): Option[(Plan[_], PhysicalOp, Plan[_], PhysicalOp)] =
    def pair(left: Plan[_], right: Plan[_], leftOp: PhysicalOp, rightOp: PhysicalOp) =
      Some((left, leftOp, right, rightOp))

    (plan, op) match
      case (Plan.JoinOp(left, right, _), PhysicalOp.HashJoin(leftOp, rightOp)) =>
        pair(left, right, leftOp, rightOp)
      case (Plan.JoinOp(left, right, _), PhysicalOp.LongHashJoin(leftOp, rightOp)) =>
        pair(left, right, leftOp, rightOp)
      case (Plan.JoinOp(left, right, _), PhysicalOp.SortMergeJoin(leftOp, rightOp)) =>
        pair(left, right, leftOp, rightOp)
      case (Plan.JoinOp(left, right, _), PhysicalOp.BroadcastJoin(leftOp, rightOp, _)) =>
        pair(left, right, leftOp, rightOp)
      case (Plan.LeftOuterJoinOp(left, right, _), PhysicalOp.HashLeftOuter(leftOp, rightOp)) =>
        pair(left, right, leftOp, rightOp)
      case (
            Plan.LeftOuterJoinOp(left, right, _),
            PhysicalOp.BroadcastLeftOuter(leftOp, rightOp),
          ) =>
        pair(left, right, leftOp, rightOp)
      case (
            Plan.LeftOuterJoinOp(left, right, _),
            PhysicalOp.SortMergeLeftOuter(leftOp, rightOp),
          ) =>
        pair(left, right, leftOp, rightOp)
      case (Plan.RightOuterJoinOp(left, right, _), PhysicalOp.HashRightOuter(leftOp, rightOp)) =>
        pair(left, right, leftOp, rightOp)
      case (
            Plan.RightOuterJoinOp(left, right, _),
            PhysicalOp.BroadcastRightOuter(leftOp, rightOp),
          ) =>
        pair(left, right, leftOp, rightOp)
      case (
            Plan.RightOuterJoinOp(left, right, _),
            PhysicalOp.SortMergeRightOuter(leftOp, rightOp),
          ) =>
        pair(left, right, leftOp, rightOp)
      case (Plan.FullOuterJoinOp(left, right, _), PhysicalOp.HashFullOuter(leftOp, rightOp)) =>
        pair(left, right, leftOp, rightOp)
      case (
            Plan.FullOuterJoinOp(left, right, _),
            PhysicalOp.BroadcastFullOuter(leftOp, rightOp),
          ) =>
        pair(left, right, leftOp, rightOp)
      case (
            Plan.FullOuterJoinOp(left, right, _),
            PhysicalOp.SortMergeFullOuter(leftOp, rightOp),
          ) =>
        pair(left, right, leftOp, rightOp)
      case _ => None
