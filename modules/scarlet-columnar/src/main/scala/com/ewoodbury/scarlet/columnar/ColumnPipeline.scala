package com.ewoodbury.scarlet.columnar

/**
 * Multi-batch sum and inner join.
 *
 * Sums read each input batch in ranges of `batchSize` rows and leave those columns in place.
 * [[HashAggregate.sumRange]] scans each range, `sumInt64` merges that batch's partials, and
 * `sumInt64` runs again after the exchange, so a partial sum does not wrap at `Int`. A
 * `keep`/`map` or [[PairStep]] overload applies that work inside the range scan, so the exchange
 * still sees one partial per input batch. Each output batch is an `Int32` key and an `Int64` sum.
 * Fold the sum with [[ColumnReduce.sumInt64]] at ordinal 1; the batch has two columns. An empty
 * input is `numPartitions` empty sum batches, the same convention as [[HashReduce.byKey]]. The
 * join reads key column 0 and value column 1 on both sides, exchanges each side, and probes one
 * partition at a time.
 */
object ColumnPipeline:

  def sumByKey(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      valueOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    runSum(batches, numPartitions, parallelism, batchSize) { rows =>
      GroupedSumKernel.run(
        rows,
        keyOrdinal,
        valueOrdinal,
        numPartitions,
        parallelism,
        batchSize,
        Vector.empty,
      )
    }

  /**
   * Filters and projects the value inside the partial range scan. The merge and the final sum
   * still read the `Int64` partials.
   */
  def sumByKey(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      valueOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): Vector[ColumnBatch] =
    runSum(batches, numPartitions, parallelism, batchSize) { rows =>
      GroupedSumKernel.run(
        rows,
        keyOrdinal,
        valueOrdinal,
        numPartitions,
        parallelism,
        batchSize,
        keep,
        map,
      )
    }

  /**
   * Runs `steps` inside the partial range scan. A chain of value filters uses the `keep`/`map`
   * overload before [[GroupedSumKernel.run]] sees those steps, so it can take the direct-index
   * sum. Any other chain, including a key filter, a row filter, or a pair map, uses the pair scan.
   * That scan stays on the hash table: a [[PairStep.MapPair]] can rewrite the key, and the direct
   * index classifies the key it sums.
   */
  def sumByKey(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      valueOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      steps: Vector[PairStep],
  ): Vector[ColumnBatch] =
    if (steps.isEmpty) then
      sumByKey(batches, keyOrdinal, valueOrdinal, numPartitions, parallelism, batchSize)
    else
      PairStep.valueKeeps(steps) match
        case Some(predicates) =>
          sumByKey(
            batches,
            keyOrdinal,
            valueOrdinal,
            numPartitions,
            parallelism,
            batchSize,
            PairStep.keepValues(predicates),
            PairStep.PassValue,
          )
        case None =>
          runSum(batches, numPartitions, parallelism, batchSize) { rows =>
            GroupedSumKernel.run(
              rows,
              keyOrdinal,
              valueOrdinal,
              numPartitions,
              parallelism,
              batchSize,
              steps,
            )
          }

  /**
   * Exchanges one combined partial vector. `combine` runs once. An empty input returns before that
   * call, so the kernel is not asked to build partials.
   */
  private def runSum(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  )(combine: Vector[ColumnBatch] => Vector[ColumnBatch]): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(emptySum)
    else
      val combined = combine(batches)
      val buckets = HashExchange.partitionAll(combined, keyOrdinal = 0, numPartitions, parallelism)
      MorselScheduler.map(buckets, parallelism) { (_, bucket) =>
        HashAggregate.sumInt64(bucket, keyOrdinal = 0, valueOrdinal = 1)
      }

  def innerJoin(
      build: Vector[ColumnBatch],
      probe: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    val buildBuckets = HashExchange.partitionAll(
      sliceAll(build, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
    )
    val probeBuckets = HashExchange.partitionAll(
      sliceAll(probe, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
    )
    MorselScheduler.map(probeBuckets, parallelism) { (part, probeBatch) =>
      HashJoin.innerInt32(
        HashJoin.JoinSide(bucketAt(buildBuckets, part), key = 0, value = 1),
        HashJoin.JoinSide(probeBatch, key = 0, value = 1),
      )
    }

  private def emptySum: ColumnBatch =
    val keys = Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0)
    val values = Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0)
    new ColumnBatch(Vector(LogicalType.Int32, LogicalType.Int64), Vector(keys, values), 0)

  private def bucketAt(buckets: Vector[ColumnBatch], part: Int): ColumnBatch =
    buckets.lift(part).getOrElse {
      throw new IllegalArgumentException(s"missing partition $part")
    }

  private def sliceAll(batches: Vector[ColumnBatch], batchSize: Int): Vector[ColumnBatch] =
    require(batches.nonEmpty, "pipeline needs at least one batch")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    batches.flatMap(batch => ColumnBatches.slice(batch, batchSize))
