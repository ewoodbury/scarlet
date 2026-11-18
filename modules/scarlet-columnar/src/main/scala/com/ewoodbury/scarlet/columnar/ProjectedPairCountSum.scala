package com.ewoodbury.scarlet.columnar

/**
 * `countSumByKey` over an int-pair [[IntPairProject]].
 *
 * The project's input morsels are already filtered. Each row is handed to the same adapter the
 * interpreter uses, including a null key and a null value. A present projected key and value are
 * observed into the [[CountSum]] table. A null projected key is dropped. A null projected value is
 * counted and not added. The pair batch is never published, and [[CountSum.byKey]] does not see
 * it. The function is not inspected.
 *
 * Partials are one count-sum per morsel, merged per input batch, then exchanged like [[CountSum]].
 * There is no spill path: a partial is `O(distinct keys)`.
 */
private[scarlet] object ProjectedPairCountSum:

  def sum(
      batches: Vector[ColumnBatch],
      mapper: IntPairProject.IntPairMap,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then CountSum.emptyCountSums(numPartitions)
    else
      val groups = batches.map(batch => Morsels.of(batch, batchSize))
      val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
        partial(morsel, mapper)
      }
      val combined = PartialCombine.mergeMorsels(
        groups.map(_.length),
        partials,
        parallelism,
        CountSum.mergeCountSums,
      )
      CountSum.exchangeCountSums(combined, numPartitions, parallelism)

  private def partial(morsel: Morsel, mapper: IntPairProject.IntPairMap): ColumnBatch =
    CountSum.suppliedPartial(morsel.length) { sink =>
      IntPairProject.visitCountSums(morsel.batch, morsel.start, morsel.length, mapper, sink)
      ColumnMetrics.fused()
    }
