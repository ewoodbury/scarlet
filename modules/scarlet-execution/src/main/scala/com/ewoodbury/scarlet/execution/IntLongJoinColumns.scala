package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan}

import ColumnarInterpreter.Columns

/**
 * Runs joins of two `(Int, Long)` sides.
 *
 * Hash and sort-merge children are exchanges. Broadcast children are the lowered sides. A null key
 * is dropped on an inner join. An outer join keeps a null key on the preserved side and marks the
 * missing partner absent. A null long stays a clear bit and is not absence. Decode does not use
 * the int-triple decoder. There is no spill.
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
    case (Plan.JoinOp(_, _, strategy), _: PhysicalOp.IntLongBroadcastJoin)
        if JoinLowering.acceptsBroadcast(strategy) =>
      true
    case (Plan.LeftOuterJoinOp(_, _, strategy), _: PhysicalOp.IntLongBroadcastLeftOuter)
        if JoinLowering.acceptsBroadcast(strategy) =>
      true
    case (Plan.RightOuterJoinOp(_, _, strategy), _: PhysicalOp.IntLongBroadcastRightOuter)
        if JoinLowering.acceptsBroadcast(strategy) =>
      true
    case (Plan.FullOuterJoinOp(_, _, strategy), _: PhysicalOp.IntLongBroadcastFullOuter)
        if JoinLowering.acceptsBroadcast(strategy) =>
      true
    case (Plan.JoinOp(_, _, strategy), _: PhysicalOp.IntLongSortMergeJoin)
        if JoinLowering.acceptsSortMerge(strategy) =>
      true
    case (Plan.LeftOuterJoinOp(_, _, strategy), _: PhysicalOp.IntLongSortMergeLeftOuter)
        if JoinLowering.acceptsSortMerge(strategy) =>
      true
    case (Plan.RightOuterJoinOp(_, _, strategy), _: PhysicalOp.IntLongSortMergeRightOuter)
        if JoinLowering.acceptsSortMerge(strategy) =>
      true
    case (Plan.FullOuterJoinOp(_, _, strategy), _: PhysicalOp.IntLongSortMergeFullOuter)
        if JoinLowering.acceptsSortMerge(strategy) =>
      true
    case _ => false

  def decodes(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.IntLongHashJoin | _: PhysicalOp.IntLongHashLeftOuter |
        _: PhysicalOp.IntLongHashRightOuter | _: PhysicalOp.IntLongHashFullOuter |
        _: PhysicalOp.IntLongBroadcastJoin | _: PhysicalOp.IntLongBroadcastLeftOuter |
        _: PhysicalOp.IntLongBroadcastRightOuter | _: PhysicalOp.IntLongBroadcastFullOuter |
        _: PhysicalOp.IntLongSortMergeJoin | _: PhysicalOp.IntLongSortMergeLeftOuter |
        _: PhysicalOp.IntLongSortMergeRightOuter | _: PhysicalOp.IntLongSortMergeFullOuter =>
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
    val (side, parts) = (plan, op) match
      case (
            Plan.JoinOp(left, right, _),
            PhysicalOp.IntLongHashJoin(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (
          None,
          shuffled(
            left,
            leftOp,
            right,
            rightOp,
            preserveAbsence,
            ordinals,
            IntLongHashJoin.shuffled,
          ),
        )
      case (
            Plan.LeftOuterJoinOp(left, right, _),
            PhysicalOp.IntLongHashLeftOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (
          Some(AbsentSide.Right),
          shuffled(
            left,
            leftOp,
            right,
            rightOp,
            preserveAbsence,
            ordinals,
            IntLongHashOuter.hashLeft,
          ),
        )
      case (
            Plan.RightOuterJoinOp(left, right, _),
            PhysicalOp.IntLongHashRightOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (
          Some(AbsentSide.Left),
          shuffled(
            left,
            leftOp,
            right,
            rightOp,
            preserveAbsence,
            ordinals,
            IntLongHashOuter.hashRight,
          ),
        )
      case (
            Plan.FullOuterJoinOp(left, right, _),
            PhysicalOp.IntLongHashFullOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (
          Some(AbsentSide.Both),
          shuffled(
            left,
            leftOp,
            right,
            rightOp,
            preserveAbsence,
            ordinals,
            IntLongHashOuter.hashFull,
          ),
        )
      case (
            Plan.JoinOp(left, right, _),
            PhysicalOp.IntLongBroadcastJoin(leftOp, rightOp, buildSide),
          ) =>
        require(
          buildSide == PhysicalOp.BuildSide.Right,
          "int-long broadcast builds the right side",
        )
        (
          None,
          broadcast(
            left,
            leftOp,
            right,
            rightOp,
            preserveAbsence,
            ordinals,
            IntLongBroadcast.inner,
          ),
        )
      case (
            Plan.LeftOuterJoinOp(left, right, _),
            PhysicalOp.IntLongBroadcastLeftOuter(leftOp, rightOp),
          ) =>
        (
          Some(AbsentSide.Right),
          broadcast(
            left,
            leftOp,
            right,
            rightOp,
            preserveAbsence,
            ordinals,
            IntLongBroadcast.leftOuter,
          ),
        )
      case (
            Plan.RightOuterJoinOp(left, right, _),
            PhysicalOp.IntLongBroadcastRightOuter(leftOp, rightOp),
          ) =>
        (
          Some(AbsentSide.Left),
          broadcast(
            left,
            leftOp,
            right,
            rightOp,
            preserveAbsence,
            ordinals,
            IntLongBroadcast.rightOuter,
          ),
        )
      case (
            Plan.FullOuterJoinOp(left, right, _),
            PhysicalOp.IntLongBroadcastFullOuter(leftOp, rightOp),
          ) =>
        (
          Some(AbsentSide.Both),
          broadcast(
            left,
            leftOp,
            right,
            rightOp,
            preserveAbsence,
            ordinals,
            IntLongBroadcast.fullOuter,
          ),
        )
      case (
            Plan.JoinOp(left, right, _),
            PhysicalOp.IntLongSortMergeJoin(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (
          None,
          shuffled(
            left,
            leftOp,
            right,
            rightOp,
            preserveAbsence,
            ordinals,
            IntLongSortMerge.inner,
          ),
        )
      case (
            Plan.LeftOuterJoinOp(left, right, _),
            PhysicalOp.IntLongSortMergeLeftOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (
          Some(AbsentSide.Right),
          shuffled(
            left,
            leftOp,
            right,
            rightOp,
            preserveAbsence,
            ordinals,
            IntLongSortMerge.leftOuter,
          ),
        )
      case (
            Plan.RightOuterJoinOp(left, right, _),
            PhysicalOp.IntLongSortMergeRightOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (
          Some(AbsentSide.Left),
          shuffled(
            left,
            leftOp,
            right,
            rightOp,
            preserveAbsence,
            ordinals,
            IntLongSortMerge.rightOuter,
          ),
        )
      case (
            Plan.FullOuterJoinOp(left, right, _),
            PhysicalOp.IntLongSortMergeFullOuter(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ) =>
        (
          Some(AbsentSide.Both),
          shuffled(
            left,
            leftOp,
            right,
            rightOp,
            preserveAbsence,
            ordinals,
            IntLongSortMerge.fullOuter,
          ),
        )
      case _ =>
        throw new IllegalStateException("int-long join expected its lowered children")
    packed(parts, side)

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
      throw new IllegalStateException("int-long join expected triple or outer batches")

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
          throw new IllegalStateException("int-long join expected triple or outer batches")

  private def shuffled(
      left: Plan[_],
      leftOp: PhysicalOp,
      right: Plan[_],
      rightOp: PhysicalOp,
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      kernel: (Vector[ColumnBatch], Vector[ColumnBatch], Int, Int) => Vector[ColumnBatch],
  ): Vector[ColumnBatch] =
    kernel(
      pairs(left, leftOp, preserveAbsence, ordinals),
      pairs(right, rightOp, preserveAbsence, ordinals),
      ColumnarPlanner.shuffleWidth,
      ColumnarPlanner.parallelism,
    )

  private def broadcast(
      left: Plan[_],
      leftOp: PhysicalOp,
      right: Plan[_],
      rightOp: PhysicalOp,
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      kernel: (Vector[ColumnBatch], Vector[ColumnBatch], Int) => Vector[ColumnBatch],
  ): Vector[ColumnBatch] =
    kernel(
      pairs(left, leftOp, preserveAbsence, ordinals),
      pairs(right, rightOp, preserveAbsence, ordinals),
      ColumnarPlanner.parallelism,
    )

  private def packed(parts: Vector[ColumnBatch], side: Option[AbsentSide]): Columns = side match
    case None => Columns.Triples(parts)
    case Some(absent) => Columns.Outer(parts, absent)

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
