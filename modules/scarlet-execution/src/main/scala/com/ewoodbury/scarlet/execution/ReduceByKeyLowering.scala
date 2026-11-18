package com.ewoodbury.scarlet.execution

/**
 * Lowers `reduceByKey` for the pair kinds the columnar kernels accept.
 *
 * An `(Int, Int)` child is [[PhysicalOp.HashAggregate]]. A `(Long, Long)` child is
 * [[PhysicalOp.LongHashAggregate]]. A `(String, Int)` child is [[PhysicalOp.StringHashAggregate]].
 * A triple project whose schema is `(Int32, Int64)`, reduced by long addition, is
 * [[PhysicalOp.HashAggregate]] of that project. Ordinals 0 and 1 are the key and the sum.
 * [[HashAggregate.sumInt64]] is the kernel. The exchange is skipped when a [[PhysicalOp.HashJoin]]
 * is already under the project. Every other kind stays on the row path. The exchange is the child
 * of the other aggregates, and those kernels perform the shuffle. A null string value is not this
 * kind: classification keeps that source on the row path.
 */
private[execution] object ReduceByKeyLowering:

  def apply(child: PhysicalOp, reduce: Any): Option[PhysicalOp] =
    if (PhysicalOp.isIntLong(child.schema) && DeclaredPair.addsLongs(reduce)) then
      val input = if (IntLongPairs.hashedJoin(child)) then child else PhysicalOp.Exchange(child)
      Some(PhysicalOp.HashAggregate(input))
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
          None
