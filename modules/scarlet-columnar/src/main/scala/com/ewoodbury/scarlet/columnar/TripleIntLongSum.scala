package com.ewoodbury.scarlet.columnar

/**
 * Sums an int-triple batch after projecting each morsel to `[Int32, Int64]`.
 *
 * Each morsel is projected, then [[HashAggregate.sumInt64]] binds ordinals 0 and 1. The morsel
 * partials of one input batch are folded with [[HashAggregate.mergeSumPartials]], not
 * concatenated. The combined partials are exchanged on the projected key and summed again, so a
 * map that changes the join key still meets in one group. An empty input is `numPartitions` empty
 * sum batches. [[projectAll]] only runs the function, so a decline can name a non-`(Int, Long)`
 * cell before a sum is published.
 */
object TripleIntLongSum:

  def reduce(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      mapper: TripleToIntLong.TripleMap,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    checked(parallelism, batchSize)
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntLongSum)
    else
      val groups = batches.map(batch => Morsels.of(batch, batchSize))
      val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
        val projected =
          TripleToIntLong.project(morsel.batch, morsel.start, morsel.length, mapper)
        HashAggregate.sumInt64(projected, keyOrdinal = 0, valueOrdinal = 1)
      }
      val combined = mergeGroups(groups.map(_.length), partials, parallelism)
      exchange(combined, numPartitions, parallelism)

  /**
   * Exchange summed partials on the projected key and sum each bucket.
   *
   * A probe that already summed its own matches uses this same finish. An empty vector is
   * `numPartitions` empty sum batches. A non-empty vector is [[HashExchange.partitionAll]] then
   * [[HashAggregate.sumInt64]], including a vector of empty partials.
   */
  private[scarlet] def exchange(
      partials: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    if (partials.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntLongSum)
    else
      val buckets =
        HashExchange.partitionAll(partials, keyOrdinal = 0, numPartitions, parallelism)
      MorselScheduler.map(buckets, parallelism) { (_, bucket) =>
        HashAggregate.sumInt64(bucket, keyOrdinal = 0, valueOrdinal = 1)
      }

  /** Calls the function on every present triple. The projected batches are discarded. */
  def projectAll(
      batches: Vector[ColumnBatch],
      parallelism: Int,
      batchSize: Int,
      mapper: TripleToIntLong.TripleMap,
  ): Unit =
    checked(parallelism, batchSize)
    val groups = batches.map(batch => Morsels.of(batch, batchSize))
    val _ = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
      TripleToIntLong.project(morsel.batch, morsel.start, morsel.length, mapper)
    }

  /** One [[HashAggregate.mergeSumPartials]] per input batch. A single morsel is still folded. */
  private def mergeGroups(
      sizes: Vector[Int],
      partials: Vector[ColumnBatch],
      parallelism: Int,
  ): Vector[ColumnBatch] =
    val grouped = PartialCombine.groupPartials(sizes, partials)
    MorselScheduler.map(grouped, parallelism) { (_, group) =>
      HashAggregate.mergeSumPartials(group, keyOrdinal = 0, valueOrdinal = 1)
    }

  private def checked(parallelism: Int, batchSize: Int): Unit =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
