package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * `reduceByKey(_ + _)` of a triple project over an int inner hash join.
 *
 * Both sides are exchanged the way [[ColumnPipeline.innerJoin]] exchanges them. Each probe bucket
 * is summed by [[TripleProbeSum]] under the projected key. Those partials are exchanged again by
 * [[TripleIntLongSum.exchange]], because the map may replace the join key. [[JoinWrite]] is not
 * called. A build bucket over the spill budget declines before a table is allocated, and the row
 * path runs the plan. [[visit]] walks the same hits so a non-addition reducer can still name a
 * cell that is not `(Int, Long)`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Throw"))
private[scarlet] object ProjectedJoinSum:

  def reduce(
      build: ColumnPipeline.JoinInput,
      probe: ColumnPipeline.JoinInput,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      slice: Boolean,
      mapper: TripleToIntLong.TripleMap,
  ): Vector[ColumnBatch] =
    val (buildBuckets, probeBuckets) =
      buckets(build, probe, numPartitions, parallelism, batchSize, slice)
    val partials = MorselScheduler.map(probeBuckets, parallelism) { (part, probeBatch) =>
      TripleProbeSum.sum(side(buildBuckets, part), side(probeBatch), mapper)
    }
    TripleIntLongSum.exchange(partials, numPartitions, parallelism)

  /** Hits [[reduce]] would sum. Nothing is published. */
  def visit(
      build: ColumnPipeline.JoinInput,
      probe: ColumnPipeline.JoinInput,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      slice: Boolean,
      mapper: TripleToIntLong.TripleMap,
  ): Unit =
    val (buildBuckets, probeBuckets) =
      buckets(build, probe, numPartitions, parallelism, batchSize, slice)
    val _ = MorselScheduler.map(probeBuckets, parallelism) { (part, probeBatch) =>
      TripleProbeSum.visit(side(buildBuckets, part), side(probeBatch), mapper)
    }

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
        "a projected join sum over the spill budget stays on the row path",
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
        case ColumnPipeline.JoinInput.Dense(batches) =>
          ColumnPipeline.JoinInput.Dense(ColumnBatches.sliceAll(batches, batchSize))
        case selected: ColumnPipeline.JoinInput.Selected => selected

  private def exchange(
      input: ColumnPipeline.JoinInput,
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    input match
      case ColumnPipeline.JoinInput.Dense(batches) =>
        HashExchange.partitionAll(batches, keyOrdinal = 0, numPartitions, parallelism)
      case ColumnPipeline.JoinInput.Selected(parts) =>
        HashExchange.partitionAllSelected(parts, keyOrdinal = 0, numPartitions, parallelism)

  private def side(buckets: Vector[ColumnBatch], part: Int): HashJoin.JoinSide =
    HashJoin.JoinSide(ColumnBatches.bucketAt(buckets, part), key = 0, value = 1)

  private def side(batch: ColumnBatch): HashJoin.JoinSide =
    HashJoin.JoinSide(batch, key = 0, value = 1)
