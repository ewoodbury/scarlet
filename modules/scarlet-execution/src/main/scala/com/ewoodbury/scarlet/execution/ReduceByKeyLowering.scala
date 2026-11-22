package com.ewoodbury.scarlet.execution

/**
 * Lowers `reduceByKey` for the pair kinds the columnar kernels accept.
 *
 * An `(Int, Int)` child is [[PhysicalOp.HashAggregate]]. A `(Long, Long)` child is
 * [[PhysicalOp.LongHashAggregate]]. A `(String, Int)` child is [[PhysicalOp.StringHashAggregate]].
 * A triple project reports `[Int32, Int64]` and is also [[PhysicalOp.HashAggregate]]. The exchange
 * always wraps that project: the map may replace the join key. An int inner sort-merge or
 * broadcast under that project is rewritten to a hash join of two exchanges. Those nodes are not
 * emitted under the aggregate. Collect of the join itself is unchanged. One filter of the triple
 * row, the triple key, or either triple value, between the project and that join, stays in place
 * and the join under it is rewritten the same way. A second filter stays on the row path. Every
 * other kind stays on the row path. The exchange is the child of the aggregate, and the kernel
 * performs the shuffle. A null string value is not this kind: classification keeps that source on
 * the row path.
 */
private[execution] object ReduceByKeyLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] =
    hashPartitionedProject(child) match
      case Some(projected) => Some(aggregated(projected))
      case None if intLongProject(child) => Some(aggregated(child))
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

  private def aggregated(child: PhysicalOp): PhysicalOp =
    PhysicalOp.HashAggregate(PhysicalOp.Exchange(child))

  /**
   * A triple project over an int inner sort-merge or broadcast join.
   *
   * Both sides are hash exchanges, then [[PhysicalOp.HashJoin]], and the project stays above that
   * join. One triple-slot filter between the project and the join is kept above the hash join. A
   * second filter is refused. A sort-merge whose children are not both exchanges is refused. The
   * function body is not read. [[JoinProbeAggregate]] is a different plan and is not built here.
   */
  private def hashPartitionedProject(child: PhysicalOp): Option[PhysicalOp] = child match
    case PhysicalOp.Project(under) =>
      hashedUnder(under).map(joined => PhysicalOp.Project(joined))
    case _ => None

  /** At most one triple filter, then the hinted join. The filter predicate is not read. */
  private def hashedUnder(op: PhysicalOp): Option[PhysicalOp] = op match
    case PhysicalOp.Filter(inner, slot) if tripleSlot(slot) =>
      hashedInner(inner).map(joined => PhysicalOp.Filter(joined, slot))
    case other => hashedInner(other)

  private def tripleSlot(slot: PhysicalOp.Slot): Boolean = slot match
    case PhysicalOp.Slot.TripleKey | PhysicalOp.Slot.TripleLeftValue |
        PhysicalOp.Slot.TripleRightValue | PhysicalOp.Slot.TripleRow =>
      true
    case PhysicalOp.Slot.Element | PhysicalOp.Slot.Row | PhysicalOp.Slot.Key |
        PhysicalOp.Slot.Value =>
      false

  private def hashedInner(op: PhysicalOp): Option[PhysicalOp] = op match
    case PhysicalOp.SortMergeJoin(PhysicalOp.Exchange(left), PhysicalOp.Exchange(right)) =>
      Some(PhysicalOp.HashJoin(PhysicalOp.Exchange(left), PhysicalOp.Exchange(right)))
    case PhysicalOp.BroadcastJoin(left, right, _) =>
      Some(PhysicalOp.HashJoin(exchanged(left), exchanged(right)))
    case _ => None

  private def exchanged(op: PhysicalOp): PhysicalOp = op match
    case present: PhysicalOp.Exchange => present
    case other => PhysicalOp.Exchange(other)

  /**
   * A triple [[PhysicalOp.Project]] reports `[Int32, Int64]`. Kind stays triples.
   *
   * A hinted join that [[hashPartitionedProject]] did not rewrite stays out. That is a second
   * filter, a filter that is not a triple slot, or a sort-merge whose children are not both
   * exchanges.
   */
  private def intLongProject(child: PhysicalOp): Boolean = child match
    // [[IntLongReduce]] accepts only a bare project. A filter here would lower into that throw.
    case PhysicalOp.Filter(_, _) => false
    case PhysicalOp.Project(under) if hintedJoin(under) => false
    case _ =>
      child.kind match
        case PhysicalOp.Kind.Triples => PhysicalSchema.isIntLong(child.schema)
        case _ => false

  private def hintedJoin(op: PhysicalOp): Boolean = op match
    case PhysicalOp.SortMergeJoin(_, _) | PhysicalOp.BroadcastJoin(_, _, _) => true
    case PhysicalOp.Filter(inner, _) => hintedJoin(inner)
    case _ => false
