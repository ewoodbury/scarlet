package com.ewoodbury.scarlet.execution

/**
 * Lowers int, long, and string `groupByKey`.
 *
 * The child is an exchange, and the kernel performs the shuffle. A null key is dropped. A null
 * value is not dropped: a clear value bit is a planner bug. One grouped batch is published per
 * shuffle partition, including an empty bucket. There is no partial combine and no sort. Spill off
 * keeps the in-memory groups and writes nothing. Spill on slices one over-budget post-exchange
 * bucket in input order. A morsel whose group table does not fit throws
 * [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]] before that table. The appended groups
 * are not bounded by the budget. Key order inside a partition is first-seen. Value order inside a
 * key is input order. `keys`, `values`, `take`, and a map above the group stay on the row path.
 * `count` is the number of groups. [[PhysicalOp.Kind.Pairs]], [[PhysicalOp.Kind.LongPairs]], and
 * [[PhysicalOp.Kind.StringPairs]] lower. Every other kind stays on the row path.
 */
private[execution] object GroupByKeyLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] = child.kind match
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.LongPairs =>
      Some(PhysicalOp.GroupByKey(PhysicalOp.Exchange(child)))
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.Strings |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.LongCounts |
        PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
        PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter =>
      None
