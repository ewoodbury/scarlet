package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, longPairsOf, pairsOf}

/**
 * Int, long, and string `countByKey`.
 *
 * An int-pair stack of key and value slot atoms is counted by [[FoldedCount]]: the predicates run
 * inside the key scan and no survivor index is built. The exchange still sees dense `(key, count)`
 * partials. No filters, a whole-row or string step, a step that is not an int slot, a step past a
 * map, and a null component the encoder throws on stay on the old path. That path partial-counts
 * survivor ids, including one row, key, or value filter. A map, a scan, and a non-pair child stay
 * on the dense count. The value column is not read, so a null value still counts. A long count
 * publishes [[Columns.LongCounts]]. A string count publishes [[Columns.StringPairs]]. The stored
 * count is an `Int`. A null string key is dropped and `""` is kept.
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
   * True when the filter stack is a non-empty run of int key or value slots and the peeled child
   * is not a map. A dense scan, a whole-row or string step, a non-int slot, and a filter past a
   * map are false. Budget, a null sort key, and a null the encoder throws are decided in
   * [[columns]], which returns to the index path.
   */
  private[execution] def folds(source: Plan[_], child: PhysicalOp, ordinals: Set[Int]): Boolean =
    acceptedSteps(source, child, ordinals).nonEmpty

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
    acceptedSteps(source, child, ordinals).flatMap { steps =>
      childBatches(source, child, selectedPartition, preserveAbsence, ordinals).flatMap {
        (under, batches) =>
          if (sortDropsNullKey(under, batches)) then None
          else FoldedCount.count(batches, steps, width, workers, batchSize)
      }
    }

  private def acceptedSteps(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    val (under, _) = PredicatePlan.childOf(source, child)
    if (pastMap(under)) then None
    else
      PredicatePlan.compile(source, child).flatMap { compiled =>
        PredicatePlan
          .selectionSteps(PredicatePlan.encoded(compiled, ordinals))
          .map(_.toList)
          .filter(steps => FoldedCount.admits(steps))
      }

  /** A map under the filters is interpreted by the index path. The steps would see its output. */
  private def pastMap(plan: Plan[_]): Boolean = plan match
    case Plan.MapOp(_, _) | Plan.MapValuesOp(_, _) | Plan.FlatMapOp(_, _) |
        Plan.FlatMapValuesOp(_, _) | Plan.MapPartitionsOp(_, _) =>
      true
    case _ => false

  private def childBatches(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Option[(PhysicalOp, Vector[ColumnBatch])] =
    val (underPlan, underOp) = PredicatePlan.childOf(source, child)
    try
      interpretAt(underPlan, underOp, selectedPartition, preserveAbsence, ordinals) match
        case Columns.Pairs(batches) => Some((underOp, batches))
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
