package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Multi-batch sum and inner join.
 *
 * Sums read each input batch in ranges of `batchSize` rows and leave those columns in place.
 * [[HashAggregate.sumRange]] scans each range, [[HashAggregate.mergeSumPartials]] folds that
 * batch's partials, and `sumInt64` runs again after the exchange, so a partial sum does not wrap
 * at `Int`. A `keep`/`map` or [[PairStep]] overload applies that work inside the range scan, so
 * the exchange still sees one partial per input batch. Each output batch is an `Int32` key and an
 * `Int64` sum. Fold the sum with [[ColumnReduce.sumInt64]] at ordinal 1; the batch has two
 * columns. An empty input is `numPartitions` empty sum batches, the same convention as
 * [[HashReduce.byKey]]. The hash join reads key column 0 and value column 1 on both sides,
 * exchanges each side, and probes one partition at a time. Those exchanges leave `carryNullKeys`
 * off, so a null key is dropped. The broadcast join builds one table from the batches chosen as
 * the build side and probes the other batches in place. When spill is on and that table's estimate
 * exceeds the spill budget, the build and the probe are partitioned and each build partition is
 * spilled. Outer joins do not. The left outer broadcast join uses that table and keeps the probe
 * side. The shuffle-hash left outer join exchanges both sides, carries a null key on the probe
 * side only, and builds one table per partition. A right outer join calls those same entry points
 * with the right input as the probe and the left input as the build, then swaps the value columns
 * back to `(key, left, right)`. A probe-side row with no partner is kept, and the build value's
 * validity bit is clear. That bit is absence. A null value is not absence. Match order is a kernel
 * detail. The published multiset is multiplicity, values, and null semantics. A full outer join
 * probes the left input and also emits build rows no probe matched. Broadcast gives each probe
 * thread its own bitmap and ORs them after the probes join. Shuffle keeps one bitmap per partition
 * and finishes that partition immediately. Null build keys are carried outside the table and
 * emitted once. Those bitmaps are not part of the build budget.
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
   *
   * Each bucket is summed with [[HashAggregate.sumInt64]]. Spill is off unless
   * [[ScarletConf.spillEnabled]] is set. A bucket whose [[SumSpill.retainedBytes]] estimate is
   * above [[ScarletConf.spillBudgetBytes]] is sub-partitioned, written morsel by morsel, and
   * folded with [[HashAggregate.mergeSumPartials]]. That path runs one bucket at a time, so the
   * bucket's files are deleted before the next bucket opens a store. A run whose buckets all fit
   * stays on [[MorselScheduler]], including when spill is enabled and none of them exceed the
   * budget. `reduceByKey` and min/max do not reach this method.
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
      val buckets =
        HashExchange.partitionAll(combined, keyOrdinal = 0, numPartitions, parallelism)
      val conf = ScarletConf.get
      if (buckets.exists(bucket => SumSpill.exceeds(bucket, conf))) then
        buckets.map(bucket => SumSpill.finish(bucket, parallelism, batchSize, conf))
      else
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

  /**
   * Inner join with no exchange. `probe` and `build` are already chosen. One table is filled from
   * every build batch, then each probe batch is probed in place. The result is one batch per probe
   * batch. Output columns are `(key, probe value, build value)`. The caller restores
   * `(key, left, right)` when the logical left input is the build side. Multiplicity, values, and
   * null semantics are the result. Order inside a batch is unspecified here. The kernel walks each
   * build chain newest-first, which is a kernel detail. An empty probe returns no batches and does
   * not allocate a table. An empty build returns one empty batch per probe batch.
   * `buildBudgetBytes` bounds the build table only. An estimate above that bound throws
   * `IllegalStateException` before the table is allocated.
   *
   * Spill is a separate budget. When [[ScarletConf.spillEnabled]] is set and
   * [[HashJoin.retainedBuildBytes]] is above [[ScarletConf.spillBudgetBytes]], [[JoinSpill]]
   * partitions both sides, writes one file per build partition, and probes each partition under
   * that spill budget. The concatenated batches are returned only after every partition is probed.
   * A partition that is still over the budget throws. An empty probe still returns no batches and
   * writes nothing. Outer joins do not call this method.
   */
  def broadcastJoin(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
      parallelism: Int,
      buildBudgetBytes: Long,
  ): Vector[ColumnBatch] =
    val conf = ScarletConf.get
    if (probe.nonEmpty && JoinSpill.exceeds(build, conf)) then
      JoinSpill.finish(probe, build, parallelism, conf)
    else broadcast(probe, build, parallelism, buildBudgetBytes, preserveProbe = false)

  /**
   * Left outer join with no exchange. `probe` is the probe side. A probe-side row with no partner
   * is appended once, and the build value's validity is clear. That clear bit is absence. A null
   * value is not absence and is not representable on this path. Call this only when the probe and
   * build value columns are fully valid. [[valuesFullyPresent]] reads the validity words encoding
   * already wrote. A null probe key does not match and is kept. A null build key is not inserted,
   * so it does not match probe key 0. Output columns are `(key, probe value, build value)`. An
   * empty probe returns no batches. An empty build emits every probe-side row with the build value
   * absent. `buildBudgetBytes` is the same budget as [[broadcastJoin]].
   */
  def broadcastLeftOuterJoin(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
      parallelism: Int,
      buildBudgetBytes: Long,
  ): Vector[ColumnBatch] =
    require(
      valuesFullyPresent(probe) && valuesFullyPresent(build),
      "a left outer join needs fully present value columns",
    )
    broadcast(probe, build, parallelism, buildBudgetBytes, preserveProbe = true)

  /**
   * Shuffle-hash left outer join. `probe` is the left input. `build` is the right input. The probe
   * exchange carries a null key to partition 0. The build exchange drops a null key, so it is not
   * inserted and does not match probe key 0. Each partition builds one table and probes with the
   * same left-outer kernel as [[broadcastLeftOuterJoin]]. Call this only when both value columns
   * are fully valid. [[valuesFullyPresent]] reads the validity words encoding already wrote.
   * Output columns are `(key, probe value, build value)`. An empty probe bucket emits nothing. An
   * empty build bucket emits every probe row in that partition with the build value absent.
   * `buildBudgetBytes` is the same per-table budget as [[broadcastLeftOuterJoin]], applied to each
   * partition's build table.
   */
  def hashLeftOuterJoin(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      buildBudgetBytes: Long,
  ): Vector[ColumnBatch] =
    require(
      valuesFullyPresent(probe) && valuesFullyPresent(build),
      "a left outer join needs fully present value columns",
    )
    val probeBuckets = HashExchange.partitionAll(
      sliceAll(probe, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
      carryNullKeys = true,
    )
    // The build side keeps the default. A null build key is dropped before the table is filled.
    val buildBuckets = HashExchange.partitionAll(
      sliceAll(build, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
    )
    MorselScheduler.map(probeBuckets, parallelism) { (part, probeBatch) =>
      leftOuterPartition(probeBatch, bucketAt(buildBuckets, part), buildBudgetBytes)
    }

  /**
   * One exchanged partition of a left outer join. `probe` and `build` are already in the same
   * bucket. The build bucket fills one table. The probe uses that table's left-outer probe, the
   * same kernel [[broadcastLeftOuterJoin]] runs. `buildBudgetBytes` bounds this table only.
   */
  private[scarlet] def leftOuterPartition(
      probe: ColumnBatch,
      build: ColumnBatch,
      buildBudgetBytes: Long,
  ): ColumnBatch =
    val table = HashJoin.openInt32(
      Vector(HashJoin.JoinSide(build, key = 0, value = 1)),
      buildBudgetBytes,
    )
    table.probe(HashJoin.JoinSide(probe, key = 0, value = 1), preserveProbe = true)

  /**
   * Full outer join with no exchange. `probe` is the left input and `build` is the right input.
   * Output columns are `(key, left, right)`. A probe row with no partner clears the right value's
   * validity bit. A build row with no partner clears the left value's validity bit and is emitted
   * once, in table order. Each probe thread owns one bitmap of the present-key build rows, sized
   * once for the table. The threads use plain stores. After they join, the bitmaps are OR'd. A
   * clear bit is an unmatched build row. Null build keys are not inserted and are emitted once as
   * `(key, absent left, build value)`. An empty probe does not return early: every build row,
   * including a null key, is emitted. An empty table emits nothing from the table. Call this only
   * when both value columns are fully valid. A null value is not absence. `buildBudgetBytes`
   * bounds the build table only. The matched-row bitmaps sit beside that table and are not part of
   * the budget. A build with a null key also copies its present-key rows once, so the table never
   * sees a null key; that copy is outside the budget too.
   */
  def broadcastFullOuterJoin(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
      parallelism: Int,
      buildBudgetBytes: Long,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(
      valuesFullyPresent(probe) && valuesFullyPresent(build),
      "a full outer join needs fully present value columns",
    )
    val (present, nullKeys) = HashJoin.separateNullBuildKeys(buildSides(build))
    val table = HashJoin.openInt32(present, buildBudgetBytes)
    val bitsets =
      if (probe.isEmpty) then Array(Validity.allocate(table.buildRows))
      else Array.fill(parallelism)(Validity.allocate(table.buildRows))
    val probed: Vector[ColumnBatch] =
      if (probe.isEmpty) then Vector.empty
      else
        MorselScheduler.mapByThread(probe, parallelism) { (worker, probeBatch) =>
          table.probeFull(HashJoin.JoinSide(probeBatch, key = 0, value = 1), bitsets(worker))
        }
    val matched = HashJoin.orMatched(bitsets)
    withLeftovers(
      probed,
      table.unmatchedBuild(matched).toVector ++ nullKeys.toVector.map(emitNull),
    )

  /**
   * Shuffle-hash full outer join. `probe` is the left input. `build` is the right input. Both
   * exchanges carry a null key to partition 0. Each partition builds one table from its present
   * keys, probes with one bitmap, and emits that partition's unmatched build rows before the next
   * partition is joined. There is no cross-partition OR. A null build key is carried out of that
   * partition's table and emitted once. A partition with build rows and an empty probe still emits
   * every build row. Call this only when both value columns are fully valid. Output columns are
   * `(key, left, right)`. `buildBudgetBytes` bounds each partition's table only, not the bitmap.
   */
  def hashFullOuterJoin(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      buildBudgetBytes: Long,
  ): Vector[ColumnBatch] =
    require(
      valuesFullyPresent(probe) && valuesFullyPresent(build),
      "a full outer join needs fully present value columns",
    )
    val probeBuckets = HashExchange.partitionAll(
      sliceAll(probe, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
      carryNullKeys = true,
    )
    val buildBuckets = HashExchange.partitionAll(
      sliceAll(build, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
      carryNullKeys = true,
    )
    MorselScheduler.map(probeBuckets, parallelism) { (part, probeBatch) =>
      fullOuterPartition(probeBatch, bucketAt(buildBuckets, part), buildBudgetBytes)
    }

  /**
   * One exchanged partition of a full outer join. The bitmap lives only for this partition and is
   * consumed before the call returns.
   */
  private[scarlet] def fullOuterPartition(
      probe: ColumnBatch,
      build: ColumnBatch,
      buildBudgetBytes: Long,
  ): ColumnBatch =
    val (present, nullKeys) = HashJoin.separateNullBuildKeys(
      Vector(HashJoin.JoinSide(build, key = 0, value = 1)),
    )
    val table = HashJoin.openInt32(present, buildBudgetBytes)
    val matched = Validity.allocate(table.buildRows)
    val probed = table.probeFull(HashJoin.JoinSide(probe, key = 0, value = 1), matched)
    val pieces =
      probed +: (table.unmatchedBuild(matched).toVector ++ nullKeys.toVector.map(emitNull))
    val live = pieces.filter(_.length > 0)
    live match
      case Vector() => probed
      case Vector(only) => only
      case many => ColumnBatches.concat(many)

  private def emitNull(batch: ColumnBatch): ColumnBatch =
    HashJoin.emitNullBuildKeys(batch)

  private def buildSides(build: Vector[ColumnBatch]): Vector[HashJoin.JoinSide] =
    build.map(batch => HashJoin.JoinSide(batch, key = 0, value = 1))

  private def withLeftovers(
      probed: Vector[ColumnBatch],
      leftovers: Vector[ColumnBatch],
  ): Vector[ColumnBatch] =
    val tail = leftovers.filter(_.length > 0)
    if (tail.isEmpty) then probed
    else
      val extra = tail match
        case Vector(only) => only
        case many => ColumnBatches.concat(many)
      if (probed.isEmpty) then Vector(extra) else probed :+ extra

  /**
   * True when every live value cell is present. Pair batches store the value at ordinal 1. The
   * check reads validity words that are already materialised and does not walk the rows again.
   */
  private[scarlet] def valuesFullyPresent(batches: Vector[ColumnBatch]): Boolean =
    batches.forall { batch =>
      val values = ColumnHash.int32At(batch, 1)
      Validity.livePresent(values.validity, values.length)
    }

  private def broadcast(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
      parallelism: Int,
      buildBudgetBytes: Long,
      preserveProbe: Boolean,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (probe.isEmpty) then Vector.empty
    else
      val table = HashJoin.openInt32(
        build.map { batch =>
          HashJoin.JoinSide(batch, key = 0, value = 1)
        },
        buildBudgetBytes,
      )
      MorselScheduler.map(probe, parallelism) { (_, probeBatch) =>
        table.probe(HashJoin.JoinSide(probeBatch, key = 0, value = 1), preserveProbe)
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
