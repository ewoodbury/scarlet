package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.columnar.{
  ClosedKernelRefusal,
  ColumnBatch,
  ColumnHash,
  Int32Column,
  Selection,
}
import com.ewoodbury.scarlet.core.Plan

/**
 * Distinct count of int keys taken from an int-pair filter stack.
 *
 * The physical shape stays `Dedupe(Exchange(Key(...)))`. `collect` of `distinct` still runs
 * [[com.ewoodbury.scarlet.columnar.DistinctKernel]]. This count uses the groups [[CountByKeyArms]]
 * already publishes for that stack: one occupied slot of
 * [[com.ewoodbury.scarlet.columnar.HashCount]] or [[com.ewoodbury.scarlet.columnar.FoldedCount]]
 * per present key. A null key is one more value when a value filter keeps it, the same rule as
 * [[com.ewoodbury.scarlet.columnar.DistinctKernel]]. A key or row step drops a null before the
 * distinct. A string key and a bare scan stay on the published path. An over-budget table returns
 * `None`, and that published path runs.
 */
private[execution] object DistinctKeyCount:

  private val ordinals: Set[Int] = Set(0, 1)

  def of(plan: Plan[_], op: PhysicalOp): Option[Long] =
    stack(plan, op) match
      case None => None
      case Some((source, child)) =>
        try Some(counted(source, child))
        catch case _: ClosedKernelRefusal => None

  private def stack(plan: Plan[_], op: PhysicalOp): Option[(Plan[_], PhysicalOp)] =
    (plan, op) match
      case (
            Plan.DistinctOp(Plan.KeysOp(source)),
            PhysicalOp.Dedupe(PhysicalOp.Exchange(PhysicalOp.Column(child, 0))),
          ) if admits(source, child) =>
        Some((source, child))
      case _ =>
        None

  private def admits(source: Plan[_], child: PhysicalOp): Boolean =
    child.kind match
      case PhysicalOp.Kind.Pairs => pairStack(source, child)
      case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
          PhysicalOp.Kind.Strings | PhysicalOp.Kind.Bools | PhysicalOp.Kind.Triples |
          PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples |
          PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.StringTriples |
          PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
          PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter |
          PhysicalOp.Kind.CountSums | PhysicalOp.Kind.IntLongSums |
          PhysicalOp.Kind.GroupedIntLongs =>
        false

  private def pairStack(source: Plan[_], child: PhysicalOp): Boolean =
    PredicatePlan.compile(source, child) match
      case Some(compiled) =>
        PredicatePlan.selectionSteps(compiled) match
          case Some(steps) if steps.nonEmpty && steps.forall(pairSlot) =>
            PredicatePlan.childOf(source, child) match
              case (_, PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _)) => true
              case _ => false
          case _ =>
            false
      case None =>
        false

  private def pairSlot(step: Selection.Step): Boolean = step match
    case Selection.Step.Int32(_, _) | Selection.Step.IntPair(_) => true
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.LongPair(_) | Selection.Step.StringPair(_) |
        Selection.Step.IntTriple(_, _) =>
      false

  private def counted(source: Plan[_], child: PhysicalOp): Long =
    CountByKeyArms.groupRows(source, child, ordinals) + nullKey(source, child)

  /** One when a survivor key is null. A key step, a row step, or an owned scan has none. */
  private def nullKey(source: Plan[_], child: PhysicalOp): Long =
    if (readsKey(source, child) || ownedScan(source, child)) then 0L
    else if (survivorNull(source, child)) then 1L
    else 0L

  private def readsKey(source: Plan[_], child: PhysicalOp): Boolean =
    PredicatePlan.compile(source, child) match
      case Some(compiled) =>
        PredicatePlan.selectionSteps(compiled) match
          case Some(steps) => steps.exists(keyStep)
          case None => true
      case None =>
        true

  private def keyStep(step: Selection.Step): Boolean = step match
    case Selection.Step.Int32(column, _) => column == 0
    case Selection.Step.IntPair(_) => true
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.LongPair(_) | Selection.Step.StringPair(_) |
        Selection.Step.IntTriple(_, _) =>
      false

  private def ownedScan(source: Plan[_], child: PhysicalOp): Boolean =
    import com.ewoodbury.scarlet.core.IntPairArrays

    PredicatePlan.childOf(source, child) match
      case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Pairs, _)) =>
        partitions.forall { partition =>
          partition.data match
            case _: IntPairArrays => true
            case _ => false
        }
      case _ =>
        false

  private def survivorNull(source: Plan[_], child: PhysicalOp): Boolean =
    ColumnarInterpreter.interpretAt(source, child, None, false, ordinals) match
      case ColumnarInterpreter.Columns.Pairs(batches) => batches.exists(nullKeyBatch)
      case _ => false

  private def nullKeyBatch(batch: ColumnBatch): Boolean =
    nullAt(ColumnHash.int32At(batch, 0), 0)

  @tailrec
  private def nullAt(keys: Int32Column, row: Int): Boolean =
    if (row >= keys.length || row >= keys.validity.capacity) then false
    else if (!keys.validity.isValid(row)) then true
    else nullAt(keys, row + 1)
