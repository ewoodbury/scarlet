package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.execution.schema.ColumnBinding.{Operator, ReducerWitness}
import com.ewoodbury.scarlet.execution.schema.{Bound, ColumnBinding, KernelChoice, Output}

/**
 * `countByKey` of an int inner join.
 *
 * The child [[Bound]] reports three columns, `(key, left, right)`. [[ColumnBinding]] returns
 * [[KernelChoice.CountThroughProbe]] for that layout. This builds [[PhysicalOp.CountByKey]] of a
 * hash join of two exchanges, with no extra exchange and no project. A hinted sort-merge or
 * broadcast is rewritten to that hash join. A filter above the join, a project, and a non-int join
 * do not match.
 *
 * An outer join and a null policy [[ColumnBinding]] refuses are [[scala.None]] from that policy.
 * They are not a missing shape. [[JoinProbeAggregate]] is a different plan and is not built here.
 */
private[execution] object JoinAggregateBinding:

  /**
   * Count through the probe when the binder tags `child` [[KernelChoice.CountThroughProbe]].
   *
   * `child` is the lowered join. The selection on this [[Bound]] is empty: a slot above the join
   * stays on the child [[CountByKeyLowering]] sees, and this does not drop it.
   */
  def count(child: PhysicalOp): Option[PhysicalOp] =
    val bound = Bound.of(child, Vector.empty, ReducerWitness.NotApplicable)
    choose(bound) match
      case Some(KernelChoice.CountThroughProbe) =>
        hashed(child).map(PhysicalOp.CountByKey(_))
      case _ => None

  /** Tag [[ColumnBinding]] returns for a join [[Bound]]. [[Output.Opaque]] is no tag. */
  private def choose(bound: Bound): Option[KernelChoice] = bound.output match
    case Output.Columns(columns) =>
      ColumnBinding
        .choose(
          columns,
          Operator.CountByKey,
          bound.reducer,
          Bound.nulls(bound),
          Bound.side(bound),
          Set.empty,
          bound.selection,
        )
        .map(_.kernel)
    case Output.Opaque(_) => None

  /**
   * An int inner hash join of two exchanges, or a hinted sort-merge or broadcast rewritten to one.
   *
   * A filter above the join does not match, so the predicate is not dropped. One side that is not
   * an exchange does not match. Outer classes do not match; the binder already refused them from
   * [[com.ewoodbury.scarlet.execution.schema.NullPolicy.Absence]].
   */
  private def hashed(op: PhysicalOp): Option[PhysicalOp] = op match
    case join @ PhysicalOp.HashJoin(PhysicalOp.Exchange(_), PhysicalOp.Exchange(_)) =>
      Some(join)
    case PhysicalOp.SortMergeJoin(PhysicalOp.Exchange(left), PhysicalOp.Exchange(right)) =>
      Some(PhysicalOp.HashJoin(PhysicalOp.Exchange(left), PhysicalOp.Exchange(right)))
    case PhysicalOp.BroadcastJoin(left, right, _) =>
      Some(PhysicalOp.HashJoin(exchanged(left), exchanged(right)))
    case _ => None

  private def exchanged(op: PhysicalOp): PhysicalOp = op match
    case present: PhysicalOp.Exchange => present
    case other => PhysicalOp.Exchange(other)
