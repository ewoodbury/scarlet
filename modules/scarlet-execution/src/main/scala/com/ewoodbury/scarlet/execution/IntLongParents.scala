package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.columnar.{ColumnBatch, ColumnBatches, ColumnKernel, IntLongRow}
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns

/**
 * Keys, values, filters, and `Long => Long` `mapValues` of an int-to-long widening.
 *
 * [[ColumnBinding.asked]] does not call [[ColumnBinding.choose]] on
 * [[com.ewoodbury.scarlet.execution.schema.Output.IntLongPairs]]. Those columns match
 * [[com.ewoodbury.scarlet.execution.schema.Layout.AssumedIntLong]] when both are nullable, and
 * feeding them to [[choose]] would tag a count or a project sum. Keys and values are
 * [[com.ewoodbury.scarlet.execution.schema.KernelChoice.Column]] from that short path. A filter
 * and a long map are built here, not from a binder tag.
 *
 * The key column stays `Int32`. The value column stays `Int64`. A null key follows the int-pair
 * slot rule. A null long follows the long column rule: the predicate or the function is not
 * called, and the bit stays clear. This does not reuse
 * [[com.ewoodbury.scarlet.columnar.IntLongMap.widen]]. That kernel casts `Int => Long`.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw",
  ),
)
private[execution] object IntLongParents:

  /** The widening, or a filter or long `mapValues` stacked on it. */
  def widened(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.IntLongMapValue => true
    case PhysicalOp.Filter(child, _) => widened(child)
    case PhysicalOp.MapValue(child) => widened(child)
    case _ => false

  /** One column of the widened batches. Ordinal 0 is the int key. Ordinal 1 is the long value. */
  def project(
      source: Plan[_],
      child: PhysicalOp,
      ordinal: Int,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    val projected =
      ColumnarInterpreter.interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)
    val parts = ColumnarInterpreter.pairsOf(projected).map { batch =>
      ColumnBatches.takeColumn(batch, encodedIndex(batch, ordinal))
    }
    if (ordinal == 0) then Columns.Ints(parts) else Columns.Longs(parts)

  /**
   * `Long => Long` on the widened value. A null value stays null and the function is not called.
   * The key column is copied. The schema stays `[Int32, Int64]`.
   */
  def mapLong(input: Columns, mapper: Any): Columns =
    val fn = longValue(mapper)
    Columns.Pairs(
      ColumnarInterpreter.onEachMorsel(ColumnarInterpreter.pairsOf(input)) { morsel =>
        ColumnKernel.projectInt64(
          morsel.batch,
          1,
          morsel.start,
          morsel.length,
          value => fn(value),
        )
      },
    )

  /**
   * One filter stack on the widening. The child under the stack is interpreted once, then each
   * predicate runs on that batch. A null key is dropped by a key slot and kept by a value slot. A
   * null long is dropped by a value slot and kept by a key slot. A whole-row predicate drops a row
   * when either bit is clear.
   */
  def filter(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    val (source, child, steps) = peel(plan, op)
    if (steps.isEmpty) then throw new IllegalStateException("int-long filter has no predicate")
    else
      val input =
        ColumnarInterpreter.interpretAt(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
        )
      Columns.Pairs(
        ColumnarInterpreter.onEachMorsel(ColumnarInterpreter.pairsOf(input)) { morsel =>
          applySteps(morsel.batch, morsel.start, morsel.length, steps)
        },
      )

  /** Named erasure boundary. The planner checked `apply$mcJJ$sp`, so this cast is safe. */
  private def longValue(mapper: Any): Long => Long =
    mapper.asInstanceOf[Long => Long]

  private def encodedIndex(batch: ColumnBatch, ordinal: Int): Int =
    if (batch.columns.length == 1) then 0 else ordinal

  private enum Step:
    case Key(keep: Int => Boolean)
    case Value(keep: Long => Boolean)
    case Row(keep: ((Int, Long)) => Boolean)

  /** Innermost predicate first. A node that is not a filter of this widening stops the walk. */
  private def peel(
      plan: Plan[_],
      op: PhysicalOp,
  ): (Plan[_], PhysicalOp, Vector[Step]) =
    @tailrec
    def loop(
        currentPlan: Plan[_],
        currentOp: PhysicalOp,
        acc: List[Step],
    ): (Plan[_], PhysicalOp, List[Step]) =
      split(currentPlan, currentOp) match
        case Some((source, child, step)) => loop(source, child, step :: acc)
        case None => (currentPlan, currentOp, acc)

    val (source, child, steps) = loop(plan, op, Nil)
    (source, child, steps.toVector)

  private def split(
      plan: Plan[_],
      op: PhysicalOp,
  ): Option[(Plan[_], PhysicalOp, Step)] =
    (plan, op) match
      case (Plan.FilterKeysOp(source, predicate), PhysicalOp.Filter(child, PhysicalOp.Slot.Key)) =>
        Some((source, child, Step.Key(predicate.asInstanceOf[Int => Boolean])))
      case (
            Plan.FilterValuesOp(source, predicate),
            PhysicalOp.Filter(child, PhysicalOp.Slot.Value),
          ) =>
        Some((source, child, Step.Value(predicate.asInstanceOf[Long => Boolean])))
      case (Plan.FilterOp(source, predicate), PhysicalOp.Filter(child, PhysicalOp.Slot.Row)) =>
        Some((source, child, Step.Row(predicate.asInstanceOf[((Int, Long)) => Boolean])))
      case _ => None

  private def applySteps(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      steps: Vector[Step],
  ): ColumnBatch =
    val first = applyStep(batch, start, length, steps.head)
    steps.tail.foldLeft(first) { (current, step) =>
      applyStep(current, 0, current.length, step)
    }

  private def applyStep(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      step: Step,
  ): ColumnBatch =
    step match
      case Step.Key(keep) =>
        ColumnKernel.filterInt32(batch, 0, start, length, value => keep(value), length)
      case Step.Value(keep) =>
        ColumnKernel.filterInt64(batch, 1, start, length, value => keep(value))
      case Step.Row(keep) =>
        IntLongRow.filter(batch, start, length, (key, value) => keep((key, value)))
