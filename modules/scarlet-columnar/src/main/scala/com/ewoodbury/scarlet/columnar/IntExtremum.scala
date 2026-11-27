package com.ewoodbury.scarlet.columnar

/**
 * Min or max of `(Int, Int)` pairs.
 *
 * Each morsel is aggregated with the filtered int table, which drops a null value and omits a key
 * whose rows are all dropped. Those partials are hashed by key and aggregated again. The table
 * does not merge min or max itself, so the second pass is min-of-mins or max-of-maxes. An empty
 * input is `numPartitions` empty pair batches. Output order inside a bucket is first-seen.
 */
object IntExtremum:

  private val keepAll: ColumnKernel.Int32Predicate = _ => true

  private val identity: ColumnKernel.Int32Map = value => value

  private[scarlet] def byKey(
      batches: Vector[ColumnBatch],
      minimum: Boolean,
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    val measures =
      if (minimum) then
        HashAggregate.Int32Aggs(sum = false, count = false, min = true, max = false)
      else HashAggregate.Int32Aggs(sum = false, count = false, min = false, max = true)
    if (batches.isEmpty) then Vector.fill(numPartitions)(emptyPairs())
    else
      val partials = batches.map { batch =>
        HashAggregate.aggregateInt32(batch, 0, 1, measures, keepAll, identity)
      }
      val exchanged = HashExchange.partitionAll(partials, 0, numPartitions, parallelism)
      exchanged.map { bucket =>
        HashAggregate.aggregateInt32(bucket, 0, 1, measures, keepAll, identity)
      }

  private def emptyPairs(): ColumnBatch =
    val column = Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0): Column
    val again = Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0): Column
    new ColumnBatch(Vector(LogicalType.Int32, LogicalType.Int32), Vector(column, again), 0)
