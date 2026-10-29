package com.ewoodbury.scarlet.execution

/**
 * Lowers `reduceByKey` for the pair kinds the columnar kernels accept.
 *
 * An `(Int, Int)` child is [[PhysicalOp.HashAggregate]]. A `(Long, Long)` child is
 * [[PhysicalOp.LongHashAggregate]]. Every other kind stays on the row path. The exchange is the
 * child of the aggregate, and the kernel performs the shuffle.
 */
private[execution] object ReduceByKeyLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] = child.kind match
    case PhysicalOp.Kind.Pairs =>
      Some(PhysicalOp.HashAggregate(PhysicalOp.Exchange(child)))
    case PhysicalOp.Kind.LongPairs =>
      Some(PhysicalOp.LongHashAggregate(PhysicalOp.Exchange(child)))
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.Strings |
        PhysicalOp.Kind.Bools =>
      None
