package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Outer joins of `(Long, Long)` pairs.
 *
 * Hash and broadcast build one table per call to [[LongHashOuter.open]] and do not spill. A null
 * key on the preserved side is carried to partition 0. A null build key is not inserted.
 * Sort-merge hash-partitions both sides and runs [[LongSortMerge]] in memory. With spill on, a
 * partition whose present-key estimate is above `spillBudgetBytes` throws before any directory is
 * created. [[sortMergeInnerJoin]] uses that same gate and keeps a null value. An outer method
 * refuses a null value instead.
 *
 * A null value is not absence. Each outer entry checks [[longValuesFullyPresent]] and throws
 * `IllegalArgumentException` when a value bit is clear. The arm throws [[ColumnarFallback]] before
 * those methods see that batch. [[sortMergeInnerJoin]] does not. It copies the clear bit.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object LongOuterPipeline:

  /** True when every live value cell of a long pair is present. An empty batch is present. */
  private[scarlet] def longValuesFullyPresent(batches: Vector[ColumnBatch]): Boolean =
    batches.forall { batch =>
      val values = ColumnHash.int64At(batch, 1)
      Validity.livePresent(values.validity, values.length)
    }

  /**
   * Left outer join with no exchange. `probe` is the left input. An empty probe returns no batches
   * and does not allocate a table. An empty build emits every probe row with the build value
   * absent. Output columns are `(key, probe value, build value)`.
   */
  def broadcastLeftOuterJoin(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
      parallelism: Int,
      buildBudgetBytes: Long,
  ): Vector[ColumnBatch] =
    require(
      longValuesFullyPresent(probe) && longValuesFullyPresent(build),
      "a left outer join needs fully present value columns",
    )
    broadcast(probe, build, parallelism, buildBudgetBytes)

  /**
   * Shuffle-hash left outer join. The probe exchange carries a null key. The build exchange drops
   * one. Each partition builds one table. `buildBudgetBytes` bounds that table only.
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
      longValuesFullyPresent(probe) && longValuesFullyPresent(build),
      "a left outer join needs fully present value columns",
    )
    val probeBuckets = HashExchange.partitionAllInt64(
      sliceAll(probe, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
      carryNullKeys = true,
    )
    // The build side drops a null key, so it is not inserted and does not match probe key 0.
    val buildBuckets = HashExchange.partitionAllInt64(
      sliceAll(build, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
    )
    MorselScheduler.map(probeBuckets, parallelism) { (part, probeBatch) =>
      leftOuterPartition(probeBatch, bucketAt(buildBuckets, part), buildBudgetBytes)
    }

  /**
   * Full outer join with no exchange. An empty probe does not return early: every build row,
   * including a null key, is emitted. The matched-row bitmaps and the present-key copy are not
   * part of `buildBudgetBytes`.
   */
  def broadcastFullOuterJoin(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
      parallelism: Int,
      buildBudgetBytes: Long,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(
      longValuesFullyPresent(probe) && longValuesFullyPresent(build),
      "a full outer join needs fully present value columns",
    )
    val (present, nullKeys) = LongHashOuter.separateNullBuildKeys(buildSides(build))
    val table = LongHashOuter.open(present, buildBudgetBytes)
    val bitsets =
      if (probe.isEmpty) then Array(Validity.allocate(table.buildRows))
      else Array.fill(parallelism)(Validity.allocate(table.buildRows))
    val probed: Vector[ColumnBatch] =
      if (probe.isEmpty) then Vector.empty
      else
        MorselScheduler.mapByThread(probe, parallelism) { (worker, probeBatch) =>
          table.probeFull(HashJoin.JoinSide(probeBatch, key = 0, value = 1), bitsets(worker))
        }
    val matched = LongHashOuter.orMatched(bitsets)
    withLeftovers(
      probed,
      table.unmatchedBuild(matched).toVector ++ nullKeys.toVector.map(emitNull),
    )

  /**
   * Shuffle-hash full outer join. Both exchanges carry a null key. Each partition keeps its own
   * bitmap. There is no cross-partition OR.
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
      longValuesFullyPresent(probe) && longValuesFullyPresent(build),
      "a full outer join needs fully present value columns",
    )
    val probeBuckets = HashExchange.partitionAllInt64(
      sliceAll(probe, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
      carryNullKeys = true,
    )
    val buildBuckets = HashExchange.partitionAllInt64(
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
   * Inner sort-merge. Both exchanges drop a null key. A null value stays on the batch. Over the
   * budget, with spill on, this throws and writes nothing. At or under the budget, and whenever
   * spill is off, the partition stays in memory.
   */
  def sortMergeInnerJoin(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    sortMergeOuter(
      left,
      right,
      numPartitions,
      parallelism,
      batchSize,
      carryLeft = false,
      carryRight = false,
      label = "long inner sort-merge",
      (leftBatch, rightBatch) => LongSortMerge.inner(leftBatch, rightBatch),
    )

  /**
   * Left outer sort-merge. The left exchange carries a null key. The right exchange drops one.
   * Over the budget, with spill on, this throws and writes nothing.
   */
  def sortMergeLeftOuterJoin(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(
      longValuesFullyPresent(left) && longValuesFullyPresent(right),
      "a left outer join needs fully present value columns",
    )
    sortMergeOuter(
      left,
      right,
      numPartitions,
      parallelism,
      batchSize,
      carryLeft = true,
      carryRight = false,
      label = "long outer sort-merge",
      (leftBatch, rightBatch) => LongSortMerge.leftOuter(leftBatch, rightBatch),
    )

  /**
   * Right outer sort-merge. [[LongSortMerge.rightOuter]] swaps the value columns back. This method
   * does not swap again.
   */
  def sortMergeRightOuterJoin(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(
      longValuesFullyPresent(left) && longValuesFullyPresent(right),
      "a right outer join needs fully present value columns",
    )
    sortMergeOuter(
      left,
      right,
      numPartitions,
      parallelism,
      batchSize,
      carryLeft = false,
      carryRight = true,
      label = "long outer sort-merge",
      (leftBatch, rightBatch) => LongSortMerge.rightOuter(leftBatch, rightBatch),
    )

  /**
   * Full outer sort-merge. Both exchanges carry a null key. An empty side emits every row of the
   * other side.
   */
  def sortMergeFullOuterJoin(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(
      longValuesFullyPresent(left) && longValuesFullyPresent(right),
      "a full outer join needs fully present value columns",
    )
    sortMergeOuter(
      left,
      right,
      numPartitions,
      parallelism,
      batchSize,
      carryLeft = true,
      carryRight = true,
      label = "long outer sort-merge",
      (leftBatch, rightBatch) => LongSortMerge.fullOuter(leftBatch, rightBatch),
    )

  private[scarlet] def leftOuterPartition(
      probe: ColumnBatch,
      build: ColumnBatch,
      buildBudgetBytes: Long,
  ): ColumnBatch =
    val table = LongHashOuter.open(
      Vector(HashJoin.JoinSide(build, key = 0, value = 1)),
      buildBudgetBytes,
    )
    table.probe(HashJoin.JoinSide(probe, key = 0, value = 1), preserveProbe = true)

  private[scarlet] def fullOuterPartition(
      probe: ColumnBatch,
      build: ColumnBatch,
      buildBudgetBytes: Long,
  ): ColumnBatch =
    val (present, nullKeys) = LongHashOuter.separateNullBuildKeys(
      Vector(HashJoin.JoinSide(build, key = 0, value = 1)),
    )
    val table = LongHashOuter.open(present, buildBudgetBytes)
    val matched = Validity.allocate(table.buildRows)
    val probed = table.probeFull(HashJoin.JoinSide(probe, key = 0, value = 1), matched)
    val pieces =
      probed +: (table.unmatchedBuild(matched).toVector ++ nullKeys.toVector.map(emitNull))
    val live = pieces.filter(_.length > 0)
    live match
      case Vector() => probed
      case Vector(only) => only
      case many => ColumnBatches.concat(many)

  private def sortMergeOuter(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      carryLeft: Boolean,
      carryRight: Boolean,
      label: String,
      join: (ColumnBatch, ColumnBatch) => ColumnBatch,
  ): Vector[ColumnBatch] =
    val leftBuckets = HashExchange.partitionAllInt64(
      sliceAll(left, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
      carryNullKeys = carryLeft,
    )
    val rightBuckets = HashExchange.partitionAllInt64(
      sliceAll(right, batchSize),
      keyOrdinal = 0,
      numPartitions,
      parallelism,
      carryNullKeys = carryRight,
    )
    val conf = ScarletConf.get
    MorselScheduler.map(leftBuckets, parallelism) { (part, leftBatch) =>
      val rightBatch = bucketAt(rightBuckets, part)
      if (overBudget(leftBatch, rightBatch, conf)) then
        refuseSortMerge(leftBatch, rightBatch, conf, label)
      else join(leftBatch, rightBatch)
    }

  /** Present keys only. A null key is not charged and is not a match. */
  private def presentKeys(batch: ColumnBatch): Int =
    val keys = ColumnHash.int64At(batch, 0)
    val words = keys.validity.words
    val width = keys.length
    var count = 0
    var row = 0
    while (row < width) {
      if (Validity.isSet(words, row)) then count += 1
      row += 1
    }
    count

  /** Sorted-side bytes for `rows` present keys. Zero rows retain nothing. */
  private def retained(rows: Int): Long =
    if (rows <= 0) then 0L
    else rows.toLong * (8L + 8L + 1L) + 64L

  private def overBudget(left: ColumnBatch, right: ColumnBatch, conf: ScarletConf): Boolean =
    conf.spillEnabled &&
      retained(presentKeys(left)) + retained(presentKeys(right)) > conf.spillBudgetBytes

  private def refuseSortMerge(
      left: ColumnBatch,
      right: ColumnBatch,
      conf: ScarletConf,
      label: String,
  ): Nothing =
    val estimate = retained(presentKeys(left)) + retained(presentKeys(right))
    throw new IllegalStateException(
      s"$label spill retains about $estimate bytes, above the budget of ${conf.spillBudgetBytes} bytes",
    )

  private def broadcast(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
      parallelism: Int,
      buildBudgetBytes: Long,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    // A zero-length batch is an empty probe too. Opening the table would charge the build.
    if (probe.isEmpty || probe.forall(_.length == 0)) then Vector.empty
    else
      val table = LongHashOuter.open(buildSides(build), buildBudgetBytes)
      MorselScheduler.map(probe, parallelism) { (_, probeBatch) =>
        table.probe(HashJoin.JoinSide(probeBatch, key = 0, value = 1), preserveProbe = true)
      }

  private def emitNull(batch: ColumnBatch): ColumnBatch =
    LongHashOuter.emitNullBuildKeys(batch)

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

  private def bucketAt(buckets: Vector[ColumnBatch], part: Int): ColumnBatch =
    buckets.lift(part).getOrElse {
      throw new IllegalArgumentException(s"missing partition $part")
    }

  private def sliceAll(batches: Vector[ColumnBatch], batchSize: Int): Vector[ColumnBatch] =
    require(batches.nonEmpty, "pipeline needs at least one batch")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    batches.flatMap(batch => ColumnBatches.slice(batch, batchSize))
