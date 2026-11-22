package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * `countByKey` of a triple project over an int inner hash join.
 *
 * Both sides are exchanged the way [[ColumnPipeline.innerJoin]] exchanges them. Each probe bucket
 * offers its projected keys through [[TripleProbeSum.count]]. A triple predicate uses
 * [[TripleProbeSum.countKept]] and still does not write the match columns. Those partials are
 * exchanged again by [[HashCount.exchange]], because the map may replace the join key.
 * [[JoinWrite]] is not called, and [[HashCount.byKey]] is not called. A build bucket over the
 * spill budget declines before a table is allocated, and the row path runs the plan.
 */
@SuppressWarnings(Array("org.wartremover.warts.Throw"))
private[scarlet] object ProjectedJoinCount:

  /**
   * [[keep]] is `None` for the unfiltered probe. A present [[keep]] drops hits that fail it.
   * [[dropNull]] is the whole-row filter: a null component is skipped and [[keep]] is not called.
   * A slot filter passes `dropNull = false` and declines on that null.
   */
  def count(
      build: ColumnPipeline.JoinInput,
      probe: ColumnPipeline.JoinInput,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      slice: Boolean,
      mapper: TripleToIntLong.TripleMap,
      keep: Option[PairKernel.IntTriplePredicate],
      dropNull: Boolean,
  ): Vector[ColumnBatch] =
    try
      val (buildBuckets, probeBuckets) =
        buckets(build, probe, numPartitions, parallelism, batchSize, slice)
      val partials = MorselScheduler.map(probeBuckets, parallelism) { (part, probeBatch) =>
        val buildSide = side(buildBuckets, part)
        val probeSide = side(probeBatch)
        keep match
          case None => TripleProbeSum.count(buildSide, probeSide, mapper)
          case Some(predicate) =>
            TripleProbeSum.countKept(buildSide, probeSide, mapper, predicate, dropNull)
      }
      HashCount.exchange(partials, numPartitions, parallelism, batchSize)
    finally HashCount.releaseWorkers()

  private def buckets(
      build: ColumnPipeline.JoinInput,
      probe: ColumnPipeline.JoinInput,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      slice: Boolean,
  ): (Vector[ColumnBatch], Vector[ColumnBatch]) =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val buildBuckets = exchange(prepared(build, slice, batchSize), numPartitions, parallelism)
    val probeBuckets = exchange(prepared(probe, slice, batchSize), numPartitions, parallelism)
    val conf = ScarletConf.get
    if (buildBuckets.exists(bucket => HashJoinSpill.exceeds(bucket, conf))) then
      throw new ColumnarFallback(
        "a projected join count over the spill budget stays on the row path",
      )
    (buildBuckets, probeBuckets)

  private def prepared(
      input: ColumnPipeline.JoinInput,
      slice: Boolean,
      batchSize: Int,
  ): ColumnPipeline.JoinInput =
    if (!slice) then input
    else
      input match
        case ColumnPipeline.JoinInput.Dense(batches) if batches.isEmpty => input
        case ColumnPipeline.JoinInput.Dense(batches) =>
          ColumnPipeline.JoinInput.Dense(ColumnBatches.sliceAll(batches, batchSize))
        case selected: ColumnPipeline.JoinInput.Selected => selected

  private def exchange(
      input: ColumnPipeline.JoinInput,
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    input match
      case ColumnPipeline.JoinInput.Dense(batches) if batches.isEmpty =>
        Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
      case ColumnPipeline.JoinInput.Dense(batches) =>
        HashExchange.partitionAll(batches, keyOrdinal = 0, numPartitions, parallelism)
      case ColumnPipeline.JoinInput.Selected(parts) if parts.isEmpty =>
        Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
      case ColumnPipeline.JoinInput.Selected(parts) =>
        HashExchange.partitionAllSelected(parts, keyOrdinal = 0, numPartitions, parallelism)

  private def side(buckets: Vector[ColumnBatch], part: Int): HashJoin.JoinSide =
    HashJoin.JoinSide(ColumnBatches.bucketAt(buckets, part), key = 0, value = 1)

  private def side(batch: ColumnBatch): HashJoin.JoinSide =
    HashJoin.JoinSide(batch, key = 0, value = 1)
