package com.ewoodbury.scarlet.execution

/**
 * Lowers int `groupByKey`.
 *
 * The child is an exchange, and the kernel performs the shuffle. A null key is dropped. A null
 * value is not dropped: a clear value bit is a planner bug. One grouped batch is published per
 * shuffle partition, including an empty bucket. There is no partial combine and no spill. Key
 * order inside a partition is first-seen. Value order inside a key is input order. `keys`,
 * `values`, `take`, and a map above the group stay on the row path. `count` is the number of
 * groups. Only [[PhysicalOp.Kind.Pairs]] lowers. Every other kind stays on the row path.
 */
private[execution] object GroupByKeyLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] = child.kind match
    case PhysicalOp.Kind.Pairs =>
      Some(PhysicalOp.GroupByKey(PhysicalOp.Exchange(child)))
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongTriples |
        PhysicalOp.Kind.Strings | PhysicalOp.Kind.Bools | PhysicalOp.Kind.StringPairs |
        PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.GroupedInts =>
      None
