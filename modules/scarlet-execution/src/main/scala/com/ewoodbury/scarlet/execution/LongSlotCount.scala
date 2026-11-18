package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt}

/**
 * `countByKey` after one long slot above a long-pair project.
 *
 * The project's input is a scan or a long-pair filter stack. Each source morsel is handed to
 * [[LongPairProject]], including a null key. The slot then runs on the projected pair. A null
 * projected key is dropped. A key slot still counts a null projected value. A value slot drops a
 * null projected value and does not call its predicate. The pair batch is not published.
 * [[LongHashCount.byKey]] is not called. A second slot, a whole-row filter, and a second map do
 * not reach this object: they do not lower.
 */
private[execution] object LongSlotCount:

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def columns(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    prepared(source, child).map { (inner, under, fn, keySlot, keep) =>
      val adapted = LongPairProject.adapt(fn)
      // A filter stack under the project is an id list. Interpreting it would compact each morsel
      // to the parent batch length and retain every copy.
      FilterChain.selected(inner, under, selectedPartition, preserveAbsence, ordinals) match
        case Some(parts) =>
          ProjectedLongPairCount.countSelected(
            parts,
            adapted,
            keySlot,
            keep,
            width,
            workers,
            batchSize,
          )
        case None =>
          val columns =
            try interpretAt(inner, under, selectedPartition, preserveAbsence, ordinals)
            catch
              case _: NullPointerException =>
                throw new ColumnarFallback("countByKey source has a null pair component")
          columns match
            case Columns.LongPairs(batches) =>
              ProjectedLongPairCount.countSlot(
                batches,
                adapted,
                keySlot,
                keep,
                width,
                workers,
                batchSize,
              )
            case _ =>
              throw new IllegalStateException("slot above a long project expected long pairs")
    }

  /**
   * The plan under the project, the function, and the one slot above it. `None` when this is not
   * one key or value filter over one long-pair map on a scan or a long-pair filter stack.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def prepared(
      source: Plan[_],
      child: PhysicalOp,
  ): Option[(Plan[_], PhysicalOp, Any, Boolean, ColumnKernel.Int64Predicate)] =
    (source, child) match
      case (
            Plan.FilterKeysOp(Plan.MapOp(inner, fn), _),
            PhysicalOp.Filter(PhysicalOp.Project(under), PhysicalOp.Slot.Key),
          ) if LongPairSlot.scanOrFilterStack(under) =>
        Some((inner, under, fn, true, keyPredicate(source, child)))
      case (
            Plan.FilterValuesOp(Plan.MapOp(inner, fn), _),
            PhysicalOp.Filter(PhysicalOp.Project(under), PhysicalOp.Slot.Value),
          ) if LongPairSlot.scanOrFilterStack(under) =>
        Some((inner, under, fn, false, valuePredicate(source, child)))
      case _ => None

  /**
   * The one key slot. A missing atom is a planner bug, not a fall-through to [[LongHashCount]].
   */
  private def keyPredicate(source: Plan[_], child: PhysicalOp): ColumnKernel.Int64Predicate =
    PredicatePlan.compile(source, child) match
      case Some(PredicatePlan.Compiled(Vector(PredicatePlan.Atom.LongPairKey(keep)))) => keep
      case _ =>
        throw new IllegalStateException("key slot above a long project did not compile")

  /**
   * The one value slot. A missing atom is a planner bug, not a fall-through to [[LongHashCount]].
   */
  private def valuePredicate(source: Plan[_], child: PhysicalOp): ColumnKernel.Int64Predicate =
    PredicatePlan.compile(source, child) match
      case Some(PredicatePlan.Compiled(Vector(PredicatePlan.Atom.LongPairValue(_, keep)))) => keep
      case _ =>
        throw new IllegalStateException("value slot above a long project did not compile")
