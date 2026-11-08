package com.ewoodbury.scarlet.execution

/**
 * Lowers int, long, and string `countByKey`.
 *
 * The child is an exchange, and the kernel performs the shuffle. A null key is dropped. The value
 * column is not read, so a null value still counts. An int child publishes
 * [[PhysicalOp.Kind.Pairs]]. A long child publishes [[PhysicalOp.Kind.LongCounts]], schema
 * `[Int64, Int32]`. A string child publishes [[PhysicalOp.Kind.StringPairs]]. The count is an
 * `Int` and wraps. The long estimate charges 8-byte keys. The string estimate charges dictionary
 * bytes. [[PhysicalOp.Kind.Pairs]], [[PhysicalOp.Kind.LongPairs]], and
 * [[PhysicalOp.Kind.StringPairs]] lower. Every other kind, including
 * [[PhysicalOp.Kind.LongCounts]], stays on the row path. With spill off, an input that does not
 * fit the budget throws [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]] and the action
 * fails instead of rerunning on rows. With spill on, an over-budget post-exchange bucket spills. A
 * count table that still does not fit throws
 * [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]].
 */
private[execution] object CountByKeyLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] = child.kind match
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.StringPairs =>
      Some(PhysicalOp.CountByKey(PhysicalOp.Exchange(child)))
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.Strings |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.LongCounts |
        PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
        PhysicalOp.Kind.GroupedLongs =>
      None
