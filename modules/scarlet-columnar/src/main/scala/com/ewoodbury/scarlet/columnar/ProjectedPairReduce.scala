package com.ewoodbury.scarlet.columnar

/**
 * `reduceByKey` over an int-pair [[IntPairProject]].
 *
 * The project's input morsels are already filtered. Each row is handed to the same adapter the
 * interpreter uses, including a null key and a null value. A present projected key and value are
 * inserted into the [[HashReduce]] table. A null projected key is dropped. A null projected value
 * is dropped and does not create a group. The pair batch is never published, and
 * [[HashReduce.byKey]] does not see it. The function is not inspected.
 *
 * Partials are one reduce per morsel, merged per input batch, then exchanged like [[HashReduce]].
 * Spill stays on the post-exchange bucket.
 */
private[scarlet] object ProjectedPairReduce:

  def reduce(
      batches: Vector[ColumnBatch],
      mapper: IntPairProject.IntPairMap,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: HashReduce.IntBinOp,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      val groups = batches.map(batch => Morsels.of(batch, batchSize))
      val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
        partial(morsel, mapper, op)
      }
      val combined = PartialCombine.mergeMorsels(
        groups.map(_.length),
        partials,
        parallelism,
        batch => HashReduce.int32(batch, keyOrdinal = 0, valueOrdinal = 1, op),
      )
      HashReduce.reduceExchanged(combined, numPartitions, parallelism, batchSize, op)

  private def partial(
      morsel: Morsel,
      mapper: IntPairProject.IntPairMap,
      op: HashReduce.IntBinOp,
  ): ColumnBatch =
    HashReduce.partialPairs(morsel.length, op) { sink =>
      IntPairProject.visitPairs(morsel.batch, morsel.start, morsel.length, mapper, sink)
      ColumnMetrics.fused()
    }
