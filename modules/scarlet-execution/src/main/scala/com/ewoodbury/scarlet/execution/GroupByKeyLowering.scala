package com.ewoodbury.scarlet.execution

/**
 * Lowers int and string `groupByKey`.
 *
 * The child is an exchange, and the kernel performs the shuffle. A null key is dropped. A null
 * value is not dropped: a clear value bit is a planner bug. One grouped batch is published per
 * shuffle partition, including an empty bucket. There is no partial combine, no spill, and no
 * sort. Key order inside a partition is first-seen. Value order inside a key is input order.
 * `keys`, `values`, `take`, and a map above the group stay on the row path. `count` is the number
 * of groups. [[PhysicalOp.Kind.Pairs]] and [[PhysicalOp.Kind.StringPairs]] lower. Every other kind
 * stays on the row path.
 */
private[execution] object GroupByKeyLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] = child.kind match
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.StringPairs =>
      Some(PhysicalOp.GroupByKey(PhysicalOp.Exchange(child)))
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.LongTriples |
        PhysicalOp.Kind.Strings | PhysicalOp.Kind.Bools | PhysicalOp.Kind.StringTriples |
        PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings =>
      None
