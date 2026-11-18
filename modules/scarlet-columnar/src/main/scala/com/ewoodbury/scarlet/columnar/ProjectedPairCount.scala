package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * `countByKey` over an int-pair [[IntPairProject]].
 *
 * The project's input morsels are already filtered. Each row is handed to the same adapter the
 * interpreter uses, including a null key and a null value. Only a present projected key is
 * inserted. The value cell is checked and dropped, so the pair batch is never published and
 * [[HashCount.byKey]] does not see it. The function is not inspected.
 *
 * Partials are one count per morsel, merged per input batch, then exchanged like [[HashCount]].
 * Spill stays on the post-exchange bucket. A morsel whose row estimate does not fit throws
 * [[ClosedKernelRefusal]] before its table exists. With spill off, the whole input is charged the
 * same way [[HashCount.byKey]] charges it.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object ProjectedPairCount:

  private val CallerRows = 8192

  def count(
      batches: Vector[ColumnBatch],
      mapper: IntPairProject.IntPairMap,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    try
      if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
      else
        batches.foreach(HashCount.requireKey)
        val rows = batches.iterator.map(_.length.toLong).sum
        if (HashCount.refuseBeforeCombine) then HashCount.refuse(rows)
        val groups = batches.map(batch => Morsels.of(batch, batchSize))
        val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
          partial(morsel, mapper)
        }
        val sizes = groups.map(_.length)
        val combined =
          if (shortEnough(partials)) then
            PartialCombine.mergeOnCaller(sizes, partials, HashCount.addCounts)
          else PartialCombine.mergeMorsels(sizes, partials, parallelism, HashCount.addCounts)
        finish(combined, numPartitions, parallelism, batchSize)
    finally HashCount.releaseWorkers()

  private def partial(morsel: Morsel, mapper: IntPairProject.IntPairMap): ColumnBatch =
    HashCount.partialKeys(morsel.length) { add =>
      IntPairProject.visitKeys(morsel.batch, morsel.start, morsel.length, mapper, add)
      ColumnMetrics.fused()
    }

  private def finish(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    if (combined.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      val onCaller = shortEnough(combined)
      val buckets =
        if (onCaller) then partitionHere(combined, numPartitions)
        else HashExchange.partitionAll(combined, keyOrdinal = 0, numPartitions, parallelism)
      val conf = ScarletConf.get
      if (buckets.exists(bucket => HashCountSpill.exceeds(bucket, conf))) then
        buckets.map(bucket => HashCountSpill.finish(bucket, parallelism, batchSize, conf))
      else if (onCaller) then combineHere(buckets)
      else MorselScheduler.map(buckets, parallelism)((_, bucket) => HashCount.addCounts(bucket))

  private def shortEnough(batches: Vector[ColumnBatch]): Boolean =
    var total = 0
    val rows = batches.iterator
    var fits = true
    while (fits && rows.hasNext) {
      total += rows.next().length
      if (total > CallerRows) then fits = false
    }
    fits

  private def partitionHere(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
  ): Vector[ColumnBatch] =
    val scattered = batches.map { batch =>
      ColumnMetrics.visit()
      HashExchange.partitionInt32(batch, keyOrdinal = 0, numPartitions)
    }
    Vector.tabulate(numPartitions) { part =>
      val pieces = scattered.map { one =>
        one.lift(part).getOrElse {
          throw new IllegalArgumentException(s"missing partition $part")
        }
      }
      ColumnBatches.concat(pieces)
    }

  private def combineHere(buckets: Vector[ColumnBatch]): Vector[ColumnBatch] =
    buckets.map { bucket =>
      ColumnMetrics.visit()
      HashCount.addCounts(bucket)
    }
