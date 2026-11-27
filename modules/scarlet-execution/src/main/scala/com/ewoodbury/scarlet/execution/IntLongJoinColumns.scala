package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{AbsentSide, ColumnBatch, IntLongHashJoin, IntLongHashOuter}
import com.ewoodbury.scarlet.core.{Partition, Plan}

import ColumnarInterpreter.Columns

/**
 * Runs hash joins of two `(Int, Long)` sides.
 *
 * Both children are already lowered. A null key is dropped on an inner join. An outer join keeps a
 * null key on the preserved side and marks the missing partner absent. A null long stays a clear
 * bit and is not absence. Decode does not use the int-triple decoder. There is no spill.
 */
private[execution] object IntLongJoinColumns:

  def accepts(plan: Plan[_], op: PhysicalOp): Boolean = (plan, op) match
    case (Plan.JoinOp(_, _, strategy), _: PhysicalOp.IntLongHashJoin)
        if JoinLowering.acceptsShuffleHash(strategy) =>
      true
    case (Plan.LeftOuterJoinOp(_, _, strategy), _: PhysicalOp.IntLongHashLeftOuter)
        if JoinLowering.acceptsShuffleHash(strategy) =>
      true
    case (Plan.RightOuterJoinOp(_, _, strategy), _: PhysicalOp.IntLongHashRightOuter)
        if JoinLowering.acceptsShuffleHash(strategy) =>
      true
    case (Plan.FullOuterJoinOp(_, _, strategy), _: PhysicalOp.IntLongHashFullOuter)
        if JoinLowering.acceptsShuffleHash(strategy) =>
      true
    case _ => false

  def decodes(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.IntLongHashJoin | _: PhysicalOp.IntLongHashLeftOuter |
        _: PhysicalOp.IntLongHashRightOuter | _: PhysicalOp.IntLongHashFullOuter =>
      true
    case _ => false

  def inner(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run a join")
    val (left, right, leftOp, rightOp, side, kernel) = (plan, op) match
      case (
            Plan.JoinOp(left, right, _),
            PhysicalOp.IntLongHashJoin(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (left, right, leftOp, rightOp, None, IntLongHashJoin.shuffled)
      case (
            Plan.LeftOuterJoinOp(left, right, _),
            PhysicalOp.IntLongHashLeftOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (left, right, leftOp, rightOp, Some(AbsentSide.Right), IntLongHashOuter.hashLeft)
      case (
            Plan.RightOuterJoinOp(left, right, _),
            PhysicalOp.IntLongHashRightOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (left, right, leftOp, rightOp, Some(AbsentSide.Left), IntLongHashOuter.hashRight)
      case (
            Plan.FullOuterJoinOp(left, right, _),
            PhysicalOp.IntLongHashFullOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (left, right, leftOp, rightOp, Some(AbsentSide.Both), IntLongHashOuter.hashFull)
      case _ =>
        throw new IllegalStateException("int-long hash join expected two exchanges")
    val leftBatches = pairs(left, leftOp, preserveAbsence, ordinals)
    val rightBatches = pairs(right, rightOp, preserveAbsence, ordinals)
    val parts = kernel(
      leftBatches,
      rightBatches,
      ColumnarPlanner.shuffleWidth,
      ColumnarPlanner.parallelism,
    )
    side match
      case None => Columns.Triples(parts)
      case Some(absent) => Columns.Outer(parts, absent)

  def partitions(columns: Columns): Seq[Partition[_]] = columns match
    case Columns.Triples(parts) =>
      parts.map(batch => Partition(IntLongHashJoin.decode(batch, batch.length)))
    case Columns.Outer(parts, AbsentSide.Right) =>
      parts.map(batch => Partition(IntLongHashOuter.decodeLeft(batch, batch.length)))
    case Columns.Outer(parts, AbsentSide.Left) =>
      parts.map(batch => Partition(IntLongHashOuter.decodeRight(batch, batch.length)))
    case Columns.Outer(parts, AbsentSide.Both) =>
      parts.map(batch => Partition(IntLongHashOuter.decodeFull(batch, batch.length)))
    case _ =>
      throw new IllegalStateException("int-long hash join expected triple or outer batches")

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
        case Columns.Outer(parts, AbsentSide.Right) =>
          ColumnarPlanner.takeChunks(
            limit,
            parts.iterator.map { batch => (remaining: Int) =>
              IntLongHashOuter.decodeLeft(batch, remaining)
            },
          )
        case Columns.Outer(parts, AbsentSide.Left) =>
          ColumnarPlanner.takeChunks(
            limit,
            parts.iterator.map { batch => (remaining: Int) =>
              IntLongHashOuter.decodeRight(batch, remaining)
            },
          )
        case Columns.Outer(parts, AbsentSide.Both) =>
          ColumnarPlanner.takeChunks(
            limit,
            parts.iterator.map { batch => (remaining: Int) =>
              IntLongHashOuter.decodeFull(batch, remaining)
            },
          )
        case _ =>
          throw new IllegalStateException("int-long hash join expected triple or outer batches")

  private def pairs(
      plan: Plan[_],
      op: PhysicalOp,
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    ColumnarInterpreter.pairsOf(
      ColumnarInterpreter.interpretAt(
        plan,
        op,
        selectedPartition = None,
        preserveAbsence,
        ordinals,
      ),
    )
