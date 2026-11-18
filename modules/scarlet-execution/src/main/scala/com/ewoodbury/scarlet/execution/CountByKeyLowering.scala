package com.ewoodbury.scarlet.execution

/**
 * Lowers int, long, and string `countByKey`, and `countByKey` of a triple project.
 *
 * The child is an exchange, and the kernel performs the shuffle. A null key is dropped. The value
 * column is not read, so a null value still counts. An int child publishes
 * [[PhysicalOp.Kind.Pairs]]. A long child publishes [[PhysicalOp.Kind.LongCounts]], schema
 * `[Int64, Int32]`. A string child publishes [[PhysicalOp.Kind.StringPairs]]. A triple
 * [[PhysicalOp.Project]] reports `[Int32, Int64]` and is also [[PhysicalOp.CountByKey]]. The
 * exchange always wraps that project: the map may replace the join key. The count is an `Int` and
 * wraps. The long estimate charges 8-byte keys. The string estimate charges dictionary bytes.
 * [[PhysicalOp.Kind.Pairs]], [[PhysicalOp.Kind.LongPairs]], and [[PhysicalOp.Kind.StringPairs]]
 * lower. A raw triple, including a join that was not projected, stays on the row path, as does
 * every other kind, including [[PhysicalOp.Kind.LongCounts]]. With spill off, an input that does
 * not fit the budget throws [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]] and the action
 * fails instead of rerunning on rows. With spill on, an over-budget post-exchange bucket spills. A
 * count table that still does not fit throws
 * [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]].
 */
private[execution] object CountByKeyLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] =
    if (intLongProject(child)) then Some(PhysicalOp.CountByKey(PhysicalOp.Exchange(child)))
    else
      child.kind match
        case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.StringPairs =>
          Some(PhysicalOp.CountByKey(PhysicalOp.Exchange(child)))
        case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
            PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.Strings |
            PhysicalOp.Kind.Bools | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.LongCounts |
            PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
            PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter |
            PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums |
            PhysicalOp.Kind.IntLongSums =>
          LoweringTrace.note("CountByKeyOp", child.kind, "CountByKeyLowering")
          None

  /**
   * A triple [[PhysicalOp.Project]] reports `[Int32, Int64]`. Kind stays triples. The function
   * body is not read. A join that is not a project does not match: its schema is three ints.
   */
  private def intLongProject(child: PhysicalOp): Boolean = child.kind match
    case PhysicalOp.Kind.Triples => PhysicalSchema.isIntLong(child.schema)
    case _ => false
