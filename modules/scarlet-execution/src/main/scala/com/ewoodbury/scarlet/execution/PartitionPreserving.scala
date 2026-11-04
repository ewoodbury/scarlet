package com.ewoodbury.scarlet.execution

/**
 * How many partitions a narrow plan keeps from its scan.
 *
 * A scan keeps its own width. Filter, project, pipeline, column, and map-value pass it through. A
 * wide op (exchange, aggregate, join, key sort, union) re-partitions, so it is `None`; `take` must
 * run the whole op instead of reading one input partition and presenting it as the result.
 */
private[execution] object PartitionPreserving:

  def of(op: PhysicalOp): Option[Int] = op match
    case PhysicalOp.Scan(_, partitions) => Some(partitions.length)
    case PhysicalOp.Filter(child, _) => of(child)
    case PhysicalOp.Project(child) => of(child)
    case PhysicalOp.Pipeline(child, _) => of(child)
    case PhysicalOp.Column(child, _) => of(child)
    case PhysicalOp.MapValue(child) => of(child)
    // Every other op re-partitions. Unlisted future ops land here too, so a new op refuses take
    // instead of silently reading one input partition.
    case _ => None
