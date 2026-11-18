package com.ewoodbury.scarlet.execution

/**
 * Lowers `reduceByKey` for the pair kinds the columnar kernels accept.
 *
 * An `(Int, Int)` child is [[PhysicalOp.HashAggregate]]. A `(Long, Long)` child is
 * [[PhysicalOp.LongHashAggregate]]. A `(String, Int)` child is [[PhysicalOp.StringHashAggregate]].
 * A triple project reports `[Int32, Int64]` and is also [[PhysicalOp.HashAggregate]]. The exchange
 * always wraps that project: the map may replace the join key. Every other kind stays on the row
 * path. The exchange is the child of the aggregate, and the kernel performs the shuffle. A null
 * string value is not this kind: classification keeps that source on the row path.
 */
private[execution] object ReduceByKeyLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] =
    if (intLongProject(child)) then Some(PhysicalOp.HashAggregate(PhysicalOp.Exchange(child)))
    else
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
   * A triple [[PhysicalOp.Project]] reports `[Int32, Int64]`. Kind stays triples.
   *
   * Sort-merge and broadcast stay out. [[JoinProbeAggregate]] does not own those strategies, and
   * this lowering must not pick them up.
   */
  private def intLongProject(child: PhysicalOp): Boolean = child match
    case PhysicalOp.Project(under) if hintedJoin(under) => false
    case _ =>
      child.kind match
        case PhysicalOp.Kind.Triples => PhysicalSchema.isIntLong(child.schema)
        case _ => false

  private def hintedJoin(op: PhysicalOp): Boolean = op match
    case PhysicalOp.SortMergeJoin(_, _) | PhysicalOp.BroadcastJoin(_, _, _) => true
    case PhysicalOp.Filter(inner, _) => hintedJoin(inner)
    case _ => false
