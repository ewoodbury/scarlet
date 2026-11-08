package com.ewoodbury.scarlet.execution

/**
 * Lowers int `countByKey`.
 *
 * The child is an exchange, and the kernel performs the shuffle. A null key is dropped. The value
 * column is not read, so a null value still counts. Output kind is [[PhysicalOp.Kind.Pairs]]: one
 * `(key, count)` row per distinct key. The count is an `Int` and wraps. [[PhysicalOp.Kind.Pairs]]
 * lowers. Every other kind stays on the row path. With spill off, an input that does not fit the
 * budget throws [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]] and the action fails
 * instead of rerunning on rows. With spill on, an over-budget post-exchange bucket spills. A
 * count table that still does not fit throws
 * [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]].
 */
private[execution] object CountByKeyLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] = child.kind match
    case PhysicalOp.Kind.Pairs =>
      Some(PhysicalOp.CountByKey(PhysicalOp.Exchange(child)))
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.Strings |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.StringTriples |
        PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
        PhysicalOp.Kind.GroupedLongs =>
      None
