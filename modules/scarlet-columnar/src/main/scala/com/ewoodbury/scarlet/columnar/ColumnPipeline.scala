package com.ewoodbury.scarlet.columnar

/**
 * Multi-batch sum and inner join.
 *
 * Sums read each input batch in ranges of `batchSize` rows and leave those columns in place.
 * `sumInt32` runs on the range, `sumInt64` merges that batch's partials, and `sumInt64` runs again
 * after the exchange, so a partial sum does not wrap at `Int`. A `keep`/`map` or [[PairStep]]
 * overload applies that work inside the partial `sumInt32`, so the exchange still sees one partial
 * per input batch. Each output batch is an `Int32` key and an `Int64` sum. Fold the sum with
 * [[ColumnReduce.sumInt64]] at ordinal 1; the batch has two columns. An empty input is
 * `numPartitions` empty sum batches, the same convention as [[HashReduce.byKey]]. The join reads
 * key column 0 and value column 1 on both sides, exchanges each side, and probes one partition at
 * a time.
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
    runSum(batches, numPartitions, parallelism, batchSize) {
      GroupedSumKernel.run(
        batches,
        keyOrdinal,
        valueOrdinal,
        numPartitions,
        parallelism,
        batchSize,
        Vector.empty,
      )
    }

  /**
   * Filters and projects the value inside the partial `sumInt32`. The merge and the final sum
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
    runSum(batches, numPartitions, parallelism, batchSize) {
      GroupedSumKernel.run(
        batches,
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
   * Runs `steps` inside the partial `sumInt32`. A chain of value filters uses the `keep`/`map`
   * overload. Any other chain, including a key filter, a row filter, or a pair map, uses the pair
   * scan.
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
      valueKeeps(steps) match
        case Some(predicates) =>
          sumByKey(
            batches,
            keyOrdinal,
            valueOrdinal,
            numPartitions,
            parallelism,
            batchSize,
            keepValues(predicates),
            PassValue,
          )
        case None =>
          runSum(batches, numPartitions, parallelism, batchSize) {
            GroupedSumKernel.run(
              batches,
              keyOrdinal,
              valueOrdinal,
              numPartitions,
              parallelism,
              batchSize,
              steps,
            )
          }

  private def runSum(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  )(combinedOf: => Vector[ColumnBatch]): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(emptySum)
    else
      val combined = combinedOf
      val buckets = HashExchange.partitionAll(combined, keyOrdinal = 0, numPartitions, parallelism)
      MorselScheduler.map(buckets, parallelism) { (_, bucket) =>
        HashAggregate.sumInt64(bucket, keyOrdinal = 0, valueOrdinal = 1)
      }

  /** `Some` when every step is a value filter. The predicates stay in source order. */
  private def valueKeeps(steps: Vector[PairStep]): Option[Vector[Int => Boolean]] =
    val seed: Option[Vector[Int => Boolean]] = Some(Vector.empty[Int => Boolean])
    steps.foldLeft(seed) { (acc, step) =>
      (acc, step) match
        case (Some(predicates), PairStep.KeepValue(predicate)) => Some(predicates :+ predicate)
        case _ => None
    }

  private def keepValues(predicates: Vector[Int => Boolean]): ColumnKernel.Int32Predicate =
    value => predicates.forall(predicate => predicate(value))

  private object PassValue extends ColumnKernel.Int32Map:
    def apply(value: Int): Int = value

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
