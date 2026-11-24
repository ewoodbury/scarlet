package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Partition

/**
 * Row function over partitions a child already produced.
 *
 * `mapPartitions` does not lower. The child runs on its own, so a bound child has published its
 * batches and a refused child has published none. The function then runs once per partition, the
 * same way [[Stage.mapPartitions]] does. This is not a kernel and it does not build a
 * [[PhysicalOp]].
 */
private[execution] object MixedTail:

  def partitions[A, B](
      parts: Seq[Partition[A]],
      f: Iterator[A] => Iterator[B],
  ): Seq[Partition[B]] =
    parts.map { partition =>
      Partition(IterUtil.iterableOf(f(partition.data.iterator)))
    }
