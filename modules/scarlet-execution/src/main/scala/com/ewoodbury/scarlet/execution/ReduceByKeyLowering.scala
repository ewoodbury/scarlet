package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.JoinSumProjection
import com.ewoodbury.scarlet.core.Plan

/**
 * Lowers `reduceByKey` for the pair kinds the columnar kernels accept.
 *
 * An `(Int, Int)` child is [[PhysicalOp.HashAggregate]]. A `(Long, Long)` child is
 * [[PhysicalOp.LongHashAggregate]]. A `(String, Int)` child is [[PhysicalOp.StringHashAggregate]].
 * A triple project reports `[Int32, Int64]` and is also [[PhysicalOp.HashAggregate]]. The exchange
 * always wraps that project: the map may replace the join key. An explicit int inner sort-merge or
 * broadcast under that project is rewritten to a hash join of two exchanges before the aggregate
 * is built. [[JoinSumProjection]] over one of those hints stays out: that witness belongs to
 * [[JoinProbeAggregate]], and broadcast is not admitted there. The hinted join itself is
 * unchanged, so collecting it still runs that strategy. Every other kind stays on the row path.
 * The exchange is the child of the aggregate, and the kernel performs the shuffle. A null string
 * value is not this kind: classification keeps that source on the row path.
 */
private[execution] object ReduceByKeyLowering:

  def apply(child: PhysicalOp, logical: Plan[_]): Option[PhysicalOp] =
    hashedIntLong(child, logical) match
      case Some(project) => Some(PhysicalOp.HashAggregate(PhysicalOp.Exchange(project)))
      case None =>
        child.kind match
          case PhysicalOp.Kind.Pairs =>
            Some(PhysicalOp.HashAggregate(PhysicalOp.Exchange(child)))
          case PhysicalOp.Kind.LongPairs =>
            Some(PhysicalOp.LongHashAggregate(PhysicalOp.Exchange(child)))
          case PhysicalOp.Kind.StringPairs =>
            Some(PhysicalOp.StringHashAggregate(PhysicalOp.Exchange(child)))
          case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
              PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.Strings |
              PhysicalOp.Kind.Bools | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.LongCounts |
              PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
              PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter |
              PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums |
              PhysicalOp.Kind.IntLongSums =>
            LoweringTrace.note("ReduceByKeyOp", child.kind, "ReduceByKeyLowering")
            None

  /**
   * A triple project whose schema is `[Int32, Int64]`. Kind stays triples.
   *
   * An int inner [[PhysicalOp.SortMergeJoin]] or [[PhysicalOp.BroadcastJoin]] anywhere under that
   * project is replaced by [[PhysicalOp.HashJoin]] of two exchanges. A filter or a nested project
   * stays wrapped around that hash join. A sort-merge whose children are not exchanges is refused.
   * [[JoinSumProjection]] over a remaining hint is refused, so a broadcast `sumJoinLeft` stays on
   * the row path. Collect of the join, with no aggregate, still lowers to the hinted node.
   */
  private def hashedIntLong(child: PhysicalOp, logical: Plan[_]): Option[PhysicalOp] =
    if (!isIntLongTriple(child) || witnessedHint(logical, child)) then None
    else hashPartitioned(child).filterNot(stillHinted)

  /** The witness, not the lambda body. An unhinted join is not this case. */
  private def witnessedHint(logical: Plan[_], child: PhysicalOp): Boolean = logical match
    case Plan.MapOp(_, _: JoinSumProjection[?]) if stillHinted(child) => true
    case _ => false

  private def isIntLongTriple(child: PhysicalOp): Boolean = child.kind match
    case PhysicalOp.Kind.Triples => PhysicalSchema.isIntLong(child.schema)
    case _ => false

  private def hashPartitioned(op: PhysicalOp): Option[PhysicalOp] = op match
    case PhysicalOp.SortMergeJoin(PhysicalOp.Exchange(left), PhysicalOp.Exchange(right)) =>
      Some(PhysicalOp.HashJoin(PhysicalOp.Exchange(left), PhysicalOp.Exchange(right)))
    case PhysicalOp.SortMergeJoin(_, _) =>
      None
    case PhysicalOp.BroadcastJoin(left, right, _) =>
      Some(PhysicalOp.HashJoin(PhysicalOp.Exchange(left), PhysicalOp.Exchange(right)))
    case PhysicalOp.Filter(inner, slot) =>
      hashPartitioned(inner).map(child => PhysicalOp.Filter(child, slot))
    case PhysicalOp.Project(inner) =>
      hashPartitioned(inner).map(child => PhysicalOp.Project(child))
    case PhysicalOp.Exchange(inner) =>
      hashPartitioned(inner).map(child => PhysicalOp.Exchange(child))
    case other =>
      Some(other)

  private def stillHinted(op: PhysicalOp): Boolean = op match
    case PhysicalOp.SortMergeJoin(_, _) | PhysicalOp.BroadcastJoin(_, _, _) => true
    case other => other.children.exists(stillHinted)
