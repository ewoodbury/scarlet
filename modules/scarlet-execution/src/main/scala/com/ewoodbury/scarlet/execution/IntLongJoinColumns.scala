package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.IntLongHashJoin
import com.ewoodbury.scarlet.core.{Partition, Plan}

import ColumnarInterpreter.Columns

/**
 * Runs the inner hash join of two `(Int, Long)` sides.
 *
 * Both children are already lowered. A null key is dropped. A null long stays a clear bit and
 * still matches. Decode goes through [[IntLongHashJoin.decode]], not the int-triple decoder. There
 * is no spill.
 */
private[execution] object IntLongJoinColumns:

  def accepts(plan: Plan[_], op: PhysicalOp): Boolean = (plan, op) match
    case (Plan.JoinOp(_, _, strategy), _: PhysicalOp.IntLongHashJoin)
        if JoinLowering.acceptsShuffleHash(strategy) =>
      true
    case _ => false

  def decodes(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.IntLongHashJoin => true
    case _ => false

  def inner(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run a join")
    val (left, right, leftOp, rightOp) = (plan, op) match
      case (
            Plan.JoinOp(left, right, _),
            PhysicalOp.IntLongHashJoin(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (left, right, leftOp, rightOp)
      case _ =>
        throw new IllegalStateException("int-long hash join expected two exchanges")
    val leftBatches = ColumnarInterpreter.pairsOf(
      ColumnarInterpreter.interpretAt(
        left,
        leftOp,
        selectedPartition = None,
        preserveAbsence,
        ordinals,
      ),
    )
    val rightBatches = ColumnarInterpreter.pairsOf(
      ColumnarInterpreter.interpretAt(
        right,
        rightOp,
        selectedPartition = None,
        preserveAbsence,
        ordinals,
      ),
    )
    Columns.Triples(
      IntLongHashJoin.shuffled(
        leftBatches,
        rightBatches,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
      ),
    )

  def partitions(columns: Columns): Seq[Partition[_]] = columns match
    case Columns.Triples(parts) =>
      parts.map(batch => Partition(IntLongHashJoin.decode(batch, batch.length)))
    case _ =>
      throw new IllegalStateException("int-long hash join expected triple batches")

  def prefix(columns: Columns, limit: Int): Seq[Any] =
    if (limit <= 0) then Seq.empty[Any]
    else
      columns match
        case Columns.Triples(parts) =>
          ColumnarPlanner.takeChunks(
            limit,
            parts.iterator.map { batch => (remaining: Int) =>
              IntLongHashJoin.decode(batch, remaining)
            },
          )
        case _ =>
          throw new IllegalStateException("int-long hash join expected triple batches")
