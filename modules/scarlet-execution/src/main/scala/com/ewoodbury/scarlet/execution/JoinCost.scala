package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Picks broadcast for an unhinted inner join when the build side's row count is already known.
 *
 * The count is the sum of [[scala.collection.Iterable.knownSize]] on a scan. A negative size is
 * unknown, and the join stays a hash join. A filter, an aggregate, and any other operator are
 * unknown. This does not sample and does not estimate a predicate. The build side is the right
 * child, the same side [[JoinClass]] broadcasts.
 */
private[execution] object JoinCost:

  /** True when `build` is a scan whose stored row count is at or under the broadcast threshold. */
  def preferBroadcast(build: PhysicalOp): Boolean = knownRows(build) match
    case Some(rows) => rows <= ScarletConf.get.broadcastJoinThreshold
    case None => false

  /** Row count of a scan whose every partition stores its size. Anything else is unknown. */
  private def knownRows(op: PhysicalOp): Option[Long] = op match
    case PhysicalOp.Scan(_, partitions) =>
      val sizes = partitions.iterator
      var total = 0L
      var known = true
      while (known && sizes.hasNext) {
        val size = sizes.next().data.knownSize
        if (size < 0) then known = false
        else total += size.toLong
      }
      if (known) Some(total) else None
    case _ => None
