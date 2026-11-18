package com.ewoodbury.scarlet.execution

/**
 * Lowers int `countSumByKey`.
 *
 * The child is an exchange, and the kernel performs the shuffle. A null key is dropped. A null
 * value is counted and not added. Only [[PhysicalOp.Kind.Pairs]] lowers. Every other kind stays on
 * the row path. Output kind is [[PhysicalOp.Kind.CountSums]], schema `[Int32, Int64, Int64]` for
 * key, count, and sum.
 */
private[execution] object CountSumLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] = child.kind match
    case PhysicalOp.Kind.Pairs =>
      Some(PhysicalOp.CountSum(PhysicalOp.Exchange(child)))
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.Strings |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.LongCounts |
        PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.GroupedInts |
        PhysicalOp.Kind.GroupedStrings | PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter |
        PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums | PhysicalOp.Kind.IntLongSums =>
      None
