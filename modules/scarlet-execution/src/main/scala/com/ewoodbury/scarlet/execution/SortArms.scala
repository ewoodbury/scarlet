package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{BatchCodec, TopN}
import com.ewoodbury.scarlet.core.Plan

/**
 * Take of a value sort.
 *
 * The child is scanned once. A filter stack contributes survivor ids and is not gathered. A bare
 * scan keeps a null. `n <= 0` returns empty and does not scan. The decoded rows are the heap
 * prefix: value descending, key ascending, input order on a tie.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
private[execution] object SortArms:

  def take(plan: Plan[_], op: PhysicalOp, n: Int): Seq[Any] =
    if (n <= 0) then Seq.empty
    else
      (plan, op) match
        case (sort: Plan.SortByOp[?, ?], PhysicalOp.TopN(child)) =>
          decode(sort.source, child, plan, op, n)
        case _ =>
          throw new IllegalStateException("top-n take expected a value sort")

  private def decode(
      source: Plan[_],
      child: PhysicalOp,
      plan: Plan[_],
      op: PhysicalOp,
      n: Int,
  ): Seq[Any] =
    val ordinals = ColumnUse.ordinals(plan, op)
    val batch = FilterChain.selected(
      source,
      child,
      selectedPartition = None,
      preserveAbsence = true,
      ordinals,
    ) match
      case Some(parts) => TopN.ofSelected(parts, n)
      case None =>
        TopN.ofBatches(
          ColumnarInterpreter.pairsOf(
            ColumnarInterpreter.interpretAt(
              source,
              child,
              selectedPartition = None,
              preserveAbsence = true,
              ordinals,
            ),
          ),
          n,
        )
    BatchCodec.decodeIntPairsPreservingAbsence(batch, n)
