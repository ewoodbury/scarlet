package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, longPairsOf, pairsOf}

/**
 * Int, long, and string `countByKey`.
 *
 * An int-pair stack of key and value slot atoms is counted by [[FoldedCount]]: the predicates run
 * inside the key scan and no survivor index is built. One [[Plan.MapValuesOp]] above that source
 * is skipped when its child is not a map: the keys are unchanged, so the scan reads the source key
 * column and does not run the value function. A filter above that map must be an int key slot. A
 * value or row filter above the map, a second map, a [[Plan.MapOp]], a flatMap, and a
 * mapPartitions stay on the old path. The plan node decides that. The function is not inspected.
 * No filters and no such map, a whole-row or string step, a non-int slot, and a null component the
 * encoder throws on stay on the old path. That path partial-counts survivor ids, including one
 * row, key, or value filter. A scan and a non-pair child stay on the dense count. The value column
 * is not read, so a null value still counts. A long count publishes [[Columns.LongCounts]]. A
 * string count publishes [[Columns.StringPairs]]. The stored count is an `Int`. A null string key
 * is dropped and `""` is kept.
 */
private[execution] object CountByKeyArms:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    child.kind match
      case PhysicalOp.Kind.Pairs =>
        val counted = tryFold(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
          width,
          workers,
          batchSize,
        ) match
          case Some(folded) => folded
          case None =>
            // A boxed pair source whose first row witnesses `Kind.Pairs` can still hold a null
            // component later. The scan keeps that null absent, and this count does not read the
            // value column, so a null value still counts. The catch stays for a null the selection
            // itself throws.
            val selectedParts =
              try
                FilterChain.selected(
                  source,
                  child,
                  selectedPartition,
                  preserveAbsence,
                  ordinals,
                )
              catch
                case _: NullPointerException =>
                  throw new ColumnarFallback("countByKey source has a null pair component")
            selectedParts match
              case Some(parts) => HashCount.byKeySelected(parts, width, workers, batchSize)
              case None =>
                val batches =
                  try
                    pairsOf(
                      interpretAt(source, child, selectedPartition, preserveAbsence, ordinals),
                    )
                  catch
                    case _: NullPointerException =>
                      throw new ColumnarFallback("countByKey source has a null pair component")
                HashCount.byKey(batches, width, workers, batchSize)
        Columns.Pairs(counted)
      case PhysicalOp.Kind.LongPairs =>
        val counted = FilterChain.selected(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
        ) match
          case Some(parts) => LongHashCount.byKeySelected(parts, width, workers, batchSize)
          case None =>
            val batches =
              longPairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals))
            LongHashCount.byKey(batches, width, workers, batchSize)
        Columns.LongCounts(counted)
      case PhysicalOp.Kind.StringPairs =>
        // A null int is not unboxed: string-pair encode reads productElement. This arm does not
        // turn that into a fallback. The value column is dropped before the count.
        val counted = FilterChain.selected(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
        ) match
          case Some(parts) => StringHashCount.byKeySelected(parts, width, workers, batchSize)
          case None =>
            val batches = StringPairWalk.batches(
              interpretAt(source, child, selectedPartition, preserveAbsence, ordinals),
            )
            StringHashCount.byKey(batches, width, workers, batchSize)
        Columns.StringPairs(counted)
      case other =>
        throw new IllegalStateException(
          s"countByKey column is $other, expected int, long, or string pairs",
        )

  /**
   * True when [[tryFold]] has a source scan. That is a non-empty int slot stack whose child is not
   * a map, or one [[Plan.MapValuesOp]] whose child is not a map. A value or row filter above that
   * map, a second map, and a dense scan with no map are false. Budget, a null sort key, and a null
   * the encoder throws are decided in [[columns]], which returns to the index path.
   */
  private[execution] def folds(source: Plan[_], child: PhysicalOp, ordinals: Set[Int]): Boolean =
    foldTarget(source, child, ordinals).nonEmpty

  private def tryFold(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    foldTarget(source, child, ordinals).flatMap { (plan, op, steps) =>
      scanned(plan, op, selectedPartition, preserveAbsence, ordinals).flatMap { (under, batches) =>
        if (sortDropsNullKey(under, batches)) then None
        else if (steps.isEmpty) then
          FoldedCount.countUnmapped(batches, steps, width, workers, batchSize)
        else FoldedCount.count(batches, steps, width, workers, batchSize)
      }
    }

  /**
   * Plan under the filters, the physical child to scan, and the steps that apply to that scan. One
   * [[Plan.MapValuesOp]] is removed when its child is not a map and every filter above it is an
   * int key slot. The steps under the map stay, in source order, ahead of those key slots.
   */
  private def foldTarget(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[(Plan[_], PhysicalOp, List[Selection.Step])] =
    val (underPlan, underOp) = PredicatePlan.childOf(source, child)
    preserved(underPlan, underOp, source, child, ordinals).orElse(
      direct(underPlan, underOp, source, child, ordinals),
    )

  private def direct(
      underPlan: Plan[_],
      underOp: PhysicalOp,
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[(Plan[_], PhysicalOp, List[Selection.Step])] =
    if (pastMap(underPlan)) then None
    else admitted(source, child, ordinals).map(steps => (underPlan, underOp, steps))

  private def preserved(
      underPlan: Plan[_],
      underOp: PhysicalOp,
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[(Plan[_], PhysicalOp, List[Selection.Step])] =
    keyPreserving(underPlan, underOp).flatMap { (basePlan, baseOp) =>
      keySteps(source, child, ordinals).flatMap { above =>
        stacked(basePlan, baseOp, above, ordinals)
      }
    }

  /** One `mapValues` whose child is not itself a map. The function is not read. */
  private def keyPreserving(plan: Plan[_], op: PhysicalOp): Option[(Plan[_], PhysicalOp)] =
    (plan, op) match
      case (Plan.MapValuesOp(inner, _), PhysicalOp.MapValue(mapped)) if !pastMap(inner) =>
        Some((inner, mapped))
      case _ => None

  /**
   * Int key slots above the map, nearest the source first. `Some(Nil)` when nothing sits above the
   * map. `None` when a step would see the mapped value.
   */
  private def keySteps(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.compile(source, child) match
      case None => Some(Nil)
      case Some(compiled) =>
        slotList(compiled, ordinals).filter(steps =>
          steps.forall(keySlot) && FoldedCount.admits(steps),
        )

  private def stacked(
      basePlan: Plan[_],
      baseOp: PhysicalOp,
      above: List[Selection.Step],
      ordinals: Set[Int],
  ): Option[(Plan[_], PhysicalOp, List[Selection.Step])] =
    val (deepPlan, deepOp) = PredicatePlan.childOf(basePlan, baseOp)
    if (pastMap(deepPlan)) then None
    else
      underSteps(basePlan, baseOp, ordinals).map { below =>
        (deepPlan, deepOp, below ++ above)
      }

  /** Steps under the map. `Some(Nil)` when the child is not a filter. */
  private def underSteps(
      plan: Plan[_],
      op: PhysicalOp,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.compile(plan, op) match
      case None => Some(Nil)
      case Some(compiled) =>
        slotList(compiled, ordinals).filter(steps => FoldedCount.admits(steps))

  private def admitted(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.compile(source, child).flatMap { compiled =>
      slotList(compiled, ordinals).filter(steps => FoldedCount.admits(steps))
    }

  private def slotList(
      compiled: PredicatePlan.Compiled,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.selectionSteps(PredicatePlan.encoded(compiled, ordinals)).map(_.toList)

  /** Column 0 only. A value or row step sees the mapped value, so the map has to run. */
  private def keySlot(step: Selection.Step): Boolean = step match
    case Selection.Step.Int32(column, _) => column == 0
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
      false

  /** A map under the filters is interpreted by the index path. The steps would see its output. */
  private def pastMap(plan: Plan[_]): Boolean = plan match
    case Plan.MapOp(_, _) | Plan.MapValuesOp(_, _) | Plan.FlatMapOp(_, _) |
        Plan.FlatMapValuesOp(_, _) | Plan.MapPartitionsOp(_, _) =>
      true
    case _ => false

  private def scanned(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Option[(PhysicalOp, Vector[ColumnBatch])] =
    try
      interpretAt(plan, op, selectedPartition, preserveAbsence, ordinals) match
        case Columns.Pairs(batches) => Some((op, batches))
        case _ => None
    catch case _: NullPointerException => None

  /**
   * The pair-row kernel drops a clear key bit. The row path keeps that row, so a sorted batch with
   * a null key stays on the index path, which falls back before the kernel.
   */
  private def sortDropsNullKey(op: PhysicalOp, batches: Vector[ColumnBatch]): Boolean =
    op match
      case _: PhysicalOp.SortByIntKey => FoldedCount.absentIntKey(batches)
      case _ => false
