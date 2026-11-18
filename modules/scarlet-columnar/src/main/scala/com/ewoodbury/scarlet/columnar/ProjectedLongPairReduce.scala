package com.ewoodbury.scarlet.columnar

/**
 * `reduceByKey` over a long-pair [[LongPairProject]].
 *
 * The project's input morsels are already filtered. Each row is handed to the same adapter the
 * interpreter uses, including a null key and a null value. A present projected key and value are
 * inserted into the [[LongHashReduce]] table. A null projected key is dropped. A null projected
 * value is a null group until another value for that key arrives, and a later combine unboxes that
 * null to `0`. The pair batch is never published, and [[LongHashReduce.byKey]] does not see it.
 * The function is not inspected.
 *
 * Partials are one reduce per morsel, merged per input batch, then exchanged like
 * [[LongHashReduce]]. Spill stays on the post-exchange bucket.
 */
private[scarlet] object ProjectedLongPairReduce:

  def reduce(
      batches: Vector[ColumnBatch],
      mapper: LongPairProject.LongPairMap,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: LongHashReduce.LongBinOp,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongPairs)
    else
      try
        val groups = batches.map(batch => Morsels.of(batch, batchSize))
        val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
          partial(morsel, mapper, op)
        }
        val combined = PartialCombine.mergeMorsels(
          groups.map(_.length),
          partials,
          parallelism,
          batch => LongHashReduce.int64(batch, keyOrdinal = 0, valueOrdinal = 1, op),
        )
        LongHashReduce.exchangeReduced(combined, numPartitions, parallelism, batchSize, op)
      finally LongHashReduce.releaseOversized()

  private def partial(
      morsel: Morsel,
      mapper: LongPairProject.LongPairMap,
      op: LongHashReduce.LongBinOp,
  ): ColumnBatch =
    LongHashReduce.partialPairs(morsel.length, op) { sink =>
      LongPairProject.visitPairs(morsel.batch, morsel.start, morsel.length, mapper, sink)
      ColumnMetrics.fused()
    }
