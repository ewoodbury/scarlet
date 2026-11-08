package com.ewoodbury.scarlet.columnar

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Spills one string shuffle-hash build bucket that does not fit in its budget.
 *
 * The trigger is [[retainedBytes]] for that bucket. It includes the dictionary, not only the code
 * arrays. Spill is off unless [[ScarletConf.spillEnabled]] is set. A bucket at or under
 * [[ScarletConf.spillBudgetBytes]], and every bucket while spill is off, is joined with
 * [[StringHashJoin.joinBucket]], the kernel [[StringHashJoin.inner]] runs after the exchange.
 * [[StringHashJoin.inner]] keeps that in-memory path when no bucket exceeds, and writes nothing.
 *
 * An empty probe bucket publishes [[ColumnBatches.emptyStringJoin]] and writes nothing, even when
 * the build bucket is over budget. An over-budget bucket opens one [[SpillStore]] and closes it
 * before the next bucket. Under-budget buckets open no store. The file count of a store is that
 * bucket's split, so `spillMaxFiles` is not summed across buckets.
 *
 * The split is [[HashExchange.partitionAllUtf8Within]]. The parent exchange already assigned
 * `floorMod(StringHash.hash(text), parentWidth)`. The piece is
 * `floorMod(floorDiv(StringHash.hash(text), parentWidth), parts)`, so keys that share a bucket
 * still separate when their hash quotients differ. A second `floorMod(hash, parts)` does not, when
 * `parts` divides `parentWidth`. Null keys are dropped. `""` is a present key. Each non-empty
 * build piece is one spill file. Morsels of a large piece share that file. The probe is
 * sub-partitioned in memory and is not spilled. Each build piece is read back, refused when its
 * estimate is still over the budget, and probed with [[StringHashJoin.joinBucket]]. One build
 * table is open at a time. The piece outputs are concatenated into that bucket's batch. The result
 * is one batch per shuffle bucket, in bucket order.
 *
 * The two sides keep independent dictionaries. Matching is string equality, which is what the
 * in-memory join already does. The store writes each build piece with the string-pair codec:
 * validity, then the dictionary, then the codes. A selected input is scattered by
 * [[StringHashJoin]] before this runs, so a spill file holds survivor rows only. Broadcast,
 * sort-merge, and outer string joins do not reach this path. The bucket and its sub-partitioned
 * copy stay resident. The store is deleted before the call returns, on success and on throw.
 */
private[scarlet] object StringHashJoinSpill:

  /** Thrown when a test asks the second over-budget bucket to fail before it probes. */
  private[scarlet] val ProbeFailed: String = "string hash join spill probe failed"

  private val filesWritten = new AtomicLong
  private val probeFault = new AtomicBoolean(false)
  private val lastDirectory = new AtomicReference("")
  private val overBudgetBuckets = new AtomicInteger
  private val openTables = new AtomicInteger
  private val maxOpenTables = new AtomicInteger

  /**
   * Estimated retained bytes of the open string join build table for `rows` bucket rows,
   * `dictionaryEntries` dictionary entries, and `dictionaryBytes` of UTF-8 text.
   *
   * `rows * (4 + 4 + 1)` is a dictionary code, the `Int32` value, and one validity byte per row.
   * Validity is stored in bits; charging a byte rounds it up and covers both bitmaps. The
   * dictionary term is `dictionaryEntries * 8` for the reference array plus `dictionaryBytes` for
   * the UTF-8 payload, once per entry, including an entry no row uses. That is the same row and
   * dictionary accounting as [[StringHashReduceSpill.retainedBytes]].
   *
   * The join table is not a [[ColumnHash]] slot array. [[StringHashJoin]] keeps a `head` int and a
   * `next` int per row, sized to the row count, so those terms are `rows * 4` each. The
   * string-to-id map is charged `min(rows, dictionaryEntries) * (8 + 4)`: one reference and one id
   * per entry it can insert. Object headers are ignored. An empty bucket is `0`. The in-memory
   * join returns before it builds a table, and the dictionary is not added.
   */
  private[scarlet] def retainedBytes(
      rows: Int,
      dictionaryEntries: Int,
      dictionaryBytes: Long,
  ): Long =
    if (rows <= 0) then 0L
    else
      val rowBytes = rows.toLong * (4L + 4L + 1L)
      val dictionaryRefs = dictionaryEntries.toLong * 8L
      val headBytes = rows.toLong * 4L
      val chainBytes = rows.toLong * 4L
      val mapEntries = math.min(rows.toLong, dictionaryEntries.toLong)
      val mapBytes = mapEntries * (8L + 4L)
      rowBytes + dictionaryRefs + dictionaryBytes + headBytes + chainBytes + mapBytes

  /**
   * UTF-8 payload of every dictionary entry in the key column. A missing key column is rejected. A
   * null key is an empty slot, not a dictionary entry.
   */
  private[scarlet] def dictionaryBytes(bucket: ColumnBatch): Long =
    val keys = ColumnKernel.asUtf8(ColumnKernel.columnAt(bucket, 0), 0)
    keys.dictionary.iterator
      .map(entry => entry.getBytes(StandardCharsets.UTF_8).length.toLong)
      .sum

  /** Number of dictionary entries in the key column, including entries no row uses. */
  private[scarlet] def dictionaryEntries(bucket: ColumnBatch): Int =
    ColumnKernel.asUtf8(ColumnKernel.columnAt(bucket, 0), 0).dictionary.length

  /**
   * True when spill is on and one build table for this bucket is strictly above the spill budget.
   * An empty bucket is false: the in-memory join allocates nothing. Spill off returns false before
   * the dictionary is read.
   */
  private[scarlet] def exceeds(bucket: ColumnBatch, conf: ScarletConf): Boolean =
    if (!conf.spillEnabled || bucket.length == 0) then false
    else bucketBytes(bucket) > conf.spillBudgetBytes

  /**
   * Joins each shuffle bucket. Over-budget buckets are spilled one store at a time. The result has
   * one batch per bucket, the same shape [[StringHashJoin.inner]] publishes in memory.
   */
  private[scarlet] def finish(
      buildBuckets: Vector[ColumnBatch],
      probeBuckets: Vector[ColumnBatch],
      parallelism: Int,
      conf: ScarletConf,
  ): Vector[ColumnBatch] =
    require(
      buildBuckets.length == probeBuckets.length,
      s"partition counts ${buildBuckets.length} and ${probeBuckets.length} differ",
    )
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    // The armed-failure hook counts over-budget buckets within this call, not across the JVM.
    overBudgetBuckets.set(0)
    Vector.tabulate(buildBuckets.length) { index =>
      val buildBucket = at(buildBuckets, index)
      val probeBucket = at(probeBuckets, index)
      if (probeBucket.length == 0) then ColumnBatches.emptyStringJoin
      else if (exceeds(buildBucket, conf)) then
        spillBucket(buildBucket, probeBucket, buildBuckets.length, parallelism, conf)
      else joinOne(buildBucket, probeBucket)
    }

  /** Spill files published since the last [[resetForTest]]. */
  private[scarlet] def filesWrittenForTest: Long = filesWritten.get

  /** Directory of the store most recently opened, or empty when none has been. */
  private[scarlet] def lastDirectoryForTest: String = lastDirectory.get

  /**
   * Most build tables open at once since the last [[resetForTest]]. One means each piece was
   * probed and released before the next piece opened a table.
   */
  private[scarlet] def openTablesForTest: Int = maxOpenTables.get

  /** The next spilled join probes its first over-budget bucket, then throws on the second. */
  private[scarlet] def armProbeFailure(): Unit =
    probeFault.set(true)

  /** Clears the file counter, the recorded directory, the table counter, and an armed failure. */
  private[scarlet] def resetForTest(): Unit =
    filesWritten.set(0L)
    probeFault.set(false)
    lastDirectory.set("")
    overBudgetBuckets.set(0)
    openTables.set(0)
    maxOpenTables.set(0)

  private def spillBucket(
      buildBucket: ColumnBatch,
      probeBucket: ColumnBatch,
      parentWidth: Int,
      parallelism: Int,
      conf: ScarletConf,
  ): ColumnBatch =
    val rows = buildBucket.length
    require(rows > 0, "a spilled join bucket needs a non-empty build")
    val estimated = bucketBytes(buildBucket)
    val parts = SpillStore.splitCount(rows, estimated, conf)
    val batchSize = conf.columnarBatchSize
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val directory = SpillStore.directory(conf)
    lastDirectory.set(directory.toString)
    SpillStore.withSpill(directory, conf.spillMaxFiles, conf.spillMaxBytes) { store =>
      val buildPieces = subPartition(buildBucket, parentWidth, parts, parallelism)
      val probePieces = subPartition(probeBucket, parentWidth, parts, parallelism)
      require(
        buildPieces.length == parts && probePieces.length == parts,
        s"join spill produced ${buildPieces.length} build pieces and ${probePieces.length} probe pieces",
      )
      val files = buildPieces.map(piece => spillPiece(store, piece, batchSize))
      // The first over-budget bucket returns. The second throws here, after its files exist and
      // before any of its tables open. The store deletes those files on the way out.
      failIfArmed()
      val outputs = Vector.tabulate(parts) { part =>
        val buildPiece = readBuild(at(files, part), at(buildPieces, part))
        probePiece(buildPiece, at(probePieces, part), conf.spillBudgetBytes)
      }
      ColumnBatches.concat(outputs)
    }

  private def subPartition(
      bucket: ColumnBatch,
      parentWidth: Int,
      parts: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    HashExchange.partitionAllUtf8Within(
      Vector(bucket),
      keyOrdinal = 0,
      parentWidth = parentWidth,
      parts = parts,
      parallelism = parallelism,
    )

  /** One file for a non-empty piece. Morsels of a large piece share that file. */
  private def spillPiece(
      store: SpillStore,
      piece: ColumnBatch,
      batchSize: Int,
  ): Option[SpillFile] =
    if (piece.length == 0) then None
    else Some(publish(store, ColumnBatches.slice(piece, batchSize)))

  private def publish(store: SpillStore, morsels: Vector[ColumnBatch]): SpillFile =
    val file = store.write(morsels)
    val _ = filesWritten.incrementAndGet()
    file

  private def readBuild(spilled: Option[SpillFile], resident: ColumnBatch): ColumnBatch =
    spilled match
      case Some(file) =>
        val batches = file.read()
        require(batches.nonEmpty, "a spilled build piece is empty")
        if (batches.length == 1) then at(batches, 0) else ColumnBatches.concat(batches)
      case None => resident

  /**
   * Probes one piece. A build piece still over `budget` throws before
   * [[StringHashJoin.joinBucket]] allocates, and the message names the piece, the row count, and
   * the budget. An empty piece allocates nothing.
   */
  private def probePiece(
      buildPiece: ColumnBatch,
      probePiece: ColumnBatch,
      budget: Long,
  ): ColumnBatch =
    val rows = buildPiece.length
    if (rows > 0) then
      val estimate = bucketBytes(buildPiece)
      if (estimate > budget) then refusePiece(rows, estimate, budget)
    joinOne(buildPiece, probePiece)

  private def refusePiece(rows: Int, estimate: Long, budget: Long): Nothing =
    throw new IllegalStateException(
      s"string shuffle-hash build piece retains about $estimate bytes for $rows rows, " +
        s"above the budget of $budget bytes",
    )

  /** One build table. The count rises for the call and falls before the next piece. */
  private def joinOne(build: ColumnBatch, probe: ColumnBatch): ColumnBatch =
    val now = openTables.incrementAndGet()
    val _ = maxOpenTables.updateAndGet(seen => math.max(seen, now))
    try StringHashJoin.joinBucket(build, probe)
    finally
      val _ = openTables.decrementAndGet()

  /** The second over-budget bucket is where an armed failure fires, after the first returned. */
  private def failIfArmed(): Unit =
    val seen = overBudgetBuckets.getAndIncrement()
    if (seen > 0 && probeFault.compareAndSet(true, false)) then
      throw new IllegalStateException(ProbeFailed)

  private def bucketBytes(bucket: ColumnBatch): Long =
    retainedBytes(bucket.length, dictionaryEntries(bucket), dictionaryBytes(bucket))

  private def at[A](values: Vector[A], index: Int): A =
    values.lift(index).getOrElse {
      throw new IllegalArgumentException(s"missing partition $index")
    }
