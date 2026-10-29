package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Spills an inner broadcast join whose build table does not fit in its budget.
 *
 * The trigger is the same estimate [[HashJoin.openInt32]] uses before it allocates. Spill is off
 * unless [[ScarletConf.spillEnabled]] is set, and a build at or under
 * [[ScarletConf.spillBudgetBytes]] stays one in-memory table. A build over that budget is
 * partitioned with [[HashExchange.partitionAll]] on both sides, using one partition count. The
 * bucket is `Math.floorMod`, so a key, including a negative key, lands in the same partition on
 * both sides. Null keys are dropped. A null key is not routed to partition 0 and cannot match key
 * 0.
 *
 * Each non-empty build partition is one spill file. A large partition is sliced into morsels, and
 * those morsels share that file, so the file count follows the partition count. The count is
 * capped by the row count and by `spillMaxFiles - 1`. One store serves the whole join and is
 * deleted before the call returns, on success and on throw.
 *
 * Each partition is read back and opened as one table under the same budget. The probe is that
 * partition's probe rows. The per-partition batches are concatenated after every partition has
 * been probed, and that batch is the only result. A partition that is still over the budget, a hot
 * key or a bad split, throws. The join does not degrade past that.
 *
 * Only [[ColumnPipeline.broadcastJoin]] calls this. Shuffle-hash inner join, outer joins, and
 * sort-merge do not.
 *
 * Both sides are partitioned before any file is written, so peak heap is the two inputs plus one
 * partition's table, not the inputs plus the whole build table. The inputs are already resident
 * when a join is planned; this path bounds the table, not the data.
 */
private[scarlet] object JoinSpill:

  /** Thrown when a test asks the spill probe to fail after the first partition. */
  private[scarlet] val ProbeFailed: String = "join spill probe failed"

  private val filesWritten = new AtomicLong
  private val probeFault = new AtomicBoolean(false)
  private val lastDirectory = new AtomicReference("")

  /**
   * True when spill is on and one build table for `build` is strictly above the spill budget. An
   * empty build is false: the in-memory join allocates nothing.
   */
  private[scarlet] def exceeds(build: Vector[ColumnBatch], conf: ScarletConf): Boolean =
    if (!conf.spillEnabled) then false
    else
      val rows = rowCount(build)
      if (rows <= 0L) then false
      else if (rows > Int.MaxValue.toLong) then true
      else HashJoin.retainedBuildBytes(rows.toInt) > conf.spillBudgetBytes

  /**
   * Partitions `build` and `probe`, spills the build, and returns the concatenated inner join. The
   * caller has already decided that the build is over budget. An empty probe writes nothing.
   */
  private[scarlet] def finish(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
      parallelism: Int,
      conf: ScarletConf,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (probe.isEmpty) then Vector.empty
    else
      val rows = checkedRows(build)
      val parts = SpillStore.splitCount(rows, HashJoin.retainedBuildBytes(rows), conf)
      val batchSize = conf.columnarBatchSize
      require(batchSize > 0, s"batch size must be positive, got $batchSize")
      val directory = SpillStore.directory(conf)
      lastDirectory.set(directory.toString)
      SpillStore.withSpill(directory, conf.spillMaxFiles, conf.spillMaxBytes) { store =>
        val buildParts = exchange(build, parts, parallelism)
        val probeParts = exchange(probe, parts, parallelism)
        val files = buildParts.map(part => spillPartition(store, part, batchSize))
        require(
          probeParts.length == files.length,
          s"join spill produced ${files.length} build partitions and ${probeParts.length} probe partitions",
        )
        // Probe after every build file is published. The vector below is local until every
        // partition has been probed; the concatenated batch is what the caller receives.
        val outputs = probeParts.zip(files).zipWithIndex.map { case ((probePart, spilled), part) =>
          failIfArmed(part)
          val buildBatches = spilled match
            case Some(file) => file.read()
            case None => Vector.empty[ColumnBatch]
          probePartition(buildBatches, probePart, conf.spillBudgetBytes)
        }
        Vector(ColumnBatches.concat(outputs))
      }

  /** Spill files published since the last [[resetForTest]]. */
  private[scarlet] def filesWrittenForTest: Long = filesWritten.get

  /** Directory of the store most recently opened, or empty when none has been. */
  private[scarlet] def lastDirectoryForTest: String = lastDirectory.get

  /** The next spilled join probes its first partition, then throws before the second. */
  private[scarlet] def armProbeFailure(): Unit =
    probeFault.set(true)

  /** Clears the file counter, the recorded directory, and an armed probe failure. */
  private[scarlet] def resetForTest(): Unit =
    filesWritten.set(0L)
    probeFault.set(false)
    lastDirectory.set("")

  private def exchange(
      batches: Vector[ColumnBatch],
      parts: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    HashExchange.partitionAll(
      batches,
      keyOrdinal = 0,
      numPartitions = parts,
      parallelism = parallelism,
      carryNullKeys = false,
    )

  /** One file for a non-empty partition. Morsels of a large partition share that file. */
  private def spillPartition(
      store: SpillStore,
      part: ColumnBatch,
      batchSize: Int,
  ): Option[SpillFile] =
    if (part.length == 0) then None
    else Some(publish(store, ColumnBatches.slice(part, batchSize)))

  private def publish(store: SpillStore, morsels: Vector[ColumnBatch]): SpillFile =
    val file = store.write(morsels)
    val _ = filesWritten.incrementAndGet()
    file

  /**
   * Opens one build table for this partition. A table still over `budget` throws the budget
   * refusal and names the partition, so a hot key is not probed as a partial match.
   */
  private def probePartition(
      buildBatches: Vector[ColumnBatch],
      probePart: ColumnBatch,
      budget: Long,
  ): ColumnBatch =
    val rows = checkedPartitionRows(buildBatches)
    if (rows > 0) then
      val estimate = HashJoin.retainedBuildBytes(rows)
      if (estimate > budget) then refusePartition(rows, estimate, budget)
    val sides = buildBatches.map(batch => HashJoin.JoinSide(batch, key = 0, value = 1))
    val table = HashJoin.openInt32(sides, budget)
    table.probe(HashJoin.JoinSide(probePart, key = 0, value = 1))

  private def refusePartition(rows: Int, estimate: Long, budget: Long): Nothing =
    throw new IllegalStateException(
      s"broadcast build table retains about $estimate bytes for $rows rows, " +
        s"above the budget of $budget bytes; one partition still exceeds the budget",
    )

  /** The second partition is where an armed failure fires, after the first probe returned. */
  private def failIfArmed(part: Int): Unit =
    if (part > 0 && probeFault.compareAndSet(true, false)) then
      throw new IllegalStateException(ProbeFailed)

  private def checkedRows(build: Vector[ColumnBatch]): Int =
    val rows = rowCount(build)
    require(rows <= Int.MaxValue.toLong, s"build side has $rows rows")
    require(rows > 0L, "a spilled join needs a non-empty build")
    rows.toInt

  /** Row count of one partition. Empty is zero and does not allocate a table. */
  private def checkedPartitionRows(batches: Vector[ColumnBatch]): Int =
    val rows = rowCount(batches)
    require(rows <= Int.MaxValue.toLong, s"build side has $rows rows")
    rows.toInt

  private def rowCount(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L)((total, batch) => total + batch.length.toLong)
