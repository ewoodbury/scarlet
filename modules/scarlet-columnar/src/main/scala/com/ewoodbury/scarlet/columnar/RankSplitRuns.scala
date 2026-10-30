package com.ewoodbury.scarlet.columnar

import java.util.Arrays
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Spills an int-pair rank split whose input does not fit in the budget.
 *
 * [[retainedBytes]] charges each non-empty input batch `rows * (4 + 4 + 1)` for the key, the
 * value, and one validity byte, plus [[HeaderAllowance]] for that batch's array headers. Validity
 * is stored in bits; charging a byte rounds it up. An empty batch is zero. Spill stays off unless
 * [[ScarletConf.spillEnabled]] is set. A vector at or under the budget calls
 * [[RankSplit.sortAndSplit]] and writes nothing. A long-pair sort does not call this path. That
 * sort stays in memory over the budget and can run out of memory.
 *
 * Over the budget, each input batch is one source partition. It is sorted whole, by calling
 * [[RankSplit.sortAndSplit]] on that batch alone, and written as one spill file through the
 * existing int-pair spill codec. The sorted batch is then released. A null key stays null and
 * orders as zero. A null value stays null. Empty batches are checked and not written. A partition
 * longer than `columnarBatchSize` is stored as several batches in that one file. The merge reads
 * one batch at a time from each file.
 *
 * [[SpillStore.splitCount]] is not used. It is the hash fan-out for a grouped sum or a broadcast
 * build. A rank split already has one run per input partition. Hash-splitting a partition would
 * not let that partition be sorted without holding it, and the output boundaries have to stay the
 * rank split's.
 *
 * Runs are reduced the way [[SortMergeRuns]] reduces a side. A pass merges at most
 * [[ScarletConf.spillFanIn]] runs into a new sorted file and deletes that group's input runs once
 * the output file is published. A group of one run is kept. Passes repeat until one run remains.
 * That run is scattered by rank. Global rank `r` is written into output partition
 * `r * partitions / rows`, the same integer division [[RankSplit]] uses. Ties break by input
 * partition, then by the row index inside that partition: each partition is sorted that way, and
 * each pass orders equal effective keys by run position.
 *
 * [[SortMergeRuns]] is not called. Its cursors require a present key, drop null keys, and finish
 * in a join. Opening that machinery would be a new surface on the join object, and it would drop
 * the null keys this split has to keep. The fan-in loop, the one-batch cursor, and the
 * delete-after-publish step are copied here. The run heap is [[OrderedRunHeap]]: this path stores
 * the effective key in the heap's key array before each push, and sort-merge stores the present
 * key. The sift itself does not branch on which one.
 *
 * [[retainedBytes]] is the only charge. It is the sorted input the in-memory path would keep, one
 * batch per non-empty input batch. The index arrays, the one-partition copy, the slice, the merge
 * buffers, and the output partitions are not added to it. The budget decides whether to spill. It
 * does not bound the working set below. The output partitions are the returned result.
 *
 * Peak memory while spilling, apart from the caller's input vector:
 *
 *   - Sorting one partition holds the input batch, an index array of `rows` ints, and a merge
 *     buffer of the same length. The buffer is dropped before the sorted batch is gathered, and
 *     the index array is dropped when that gather returns. Those arrays are not live together with
 *     the slice. The one-partition split then copies the sorted batch into the batch that will be
 *     written, and both copies are live for that copy.
 *   - Writing that batch holds it and one slice of at most `columnarBatchSize` rows. The slice is
 *     dropped when the store finishes that write, before the next slice is copied. A batch no
 *     longer than `columnarBatchSize` is written as itself, with no slice copy. The next partition
 *     is not sorted until the file is published. A partition is sorted whole, in memory, before it
 *     is written, so one partition that does not fit still fails.
 *   - A merge pass holds at most `spillFanIn` input batches, one open batch from each run, the
 *     accumulate arrays of `columnarBatchSize` rows, and the emitted copy of the batch being
 *     written.
 *   - The rank scatter holds one input batch of at most `columnarBatchSize` rows and every output
 *     partition. The partitions are allocated before the scan. Their row counts sum to the input
 *     row count.
 *
 * The caller may still hold the input vector. This path does not drop it. `rows == 0` returns
 * `partitions` empty pair batches and writes nothing. `partitions <= 0`, `parallelism <= 0`, and
 * an empty `batches` vector are rejected. `spillFanIn` below 2 is rejected when a split spills,
 * because a pass would not shrink the runs.
 *
 * One store is closed before the method returns, on success and on throw, and that close deletes
 * every run, including intermediate ones.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object RankSplitRuns:

  /**
   * Thrown when a test asks the next run publish to fail after the file is tracked. Initial runs
   * and merged runs both go through that publish.
   */
  private[scarlet] val WriteFailed: String = "rank-split spill write failed"

  /**
   * Thrown when a test asks one merge pass to fail after its temp file exists and before that run
   * is published.
   */
  private[scarlet] val MergeFailed: String = "rank-split spill failed"

  /**
   * Per-batch allowance for the key array, the value array, and the validity-word arrays. This is
   * header and alignment slack, not another per-row charge. One sorted input batch pays it once.
   */
  private[scarlet] val HeaderAllowance: Long = 64L

  private val filesWritten = new AtomicLong
  private val mergePasses = new AtomicLong

  /** Publishes that succeed before the next one throws. Negative means the hook is off. */
  private val writesUntilFault = new AtomicInteger(-1)
  private val failAtPass = new AtomicInteger(0)
  private val lastDirectory = new AtomicReference("")

  /**
   * Estimated retained bytes of the sorted input. Each non-empty batch is charged on its own,
   * because the in-memory split keeps one sorted batch per input batch. Zero when every batch is
   * empty: the in-memory split returns before it sorts.
   */
  private[scarlet] def retainedBytes(batches: Vector[ColumnBatch]): Long =
    batches.iterator.map(batch => partitionBytes(batch.length)).sum

  /** True when spill is on and the sorted-input estimate is strictly above the budget. */
  private[scarlet] def exceeds(batches: Vector[ColumnBatch], conf: ScarletConf): Boolean =
    conf.spillEnabled && retainedBytes(batches) > conf.spillBudgetBytes

  /**
   * Rank-splits `batches` into `partitions` outputs. Over the budget, spills and closes the store
   * before returning. Otherwise this is [[RankSplit.sortAndSplit]] and it writes nothing. This is
   * the only place that chooses.
   *
   * @param parallelism
   *   matched to [[RankSplit.sortAndSplit]]. Unused on the spill path: that path sorts one
   *   partition at a time, so the spilled rows do not depend on it. The in-memory path still sorts
   *   at most this many runs at once. A non-positive value is rejected on both paths.
   */
  private[scarlet] def sortAndSplit(
      batches: Vector[ColumnBatch],
      partitions: Int,
      parallelism: Int,
      conf: ScarletConf,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"numPartitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batches.nonEmpty, "sortAndSplit needs at least one batch")
    val rows = rowCount(batches)
    if (rows == 0 || !exceeds(batches, conf)) then
      RankSplit.sortAndSplit(batches, partitions, parallelism)
    else spill(batches, partitions, rows, conf)

  /** Spill files published since the last [[resetForTest]], including intermediate runs. */
  private[scarlet] def filesWrittenForTest: Long = filesWritten.get

  /** Completed merge passes since the last [[resetForTest]]. A single run completes none. */
  private[scarlet] def mergePassesForTest: Long = mergePasses.get

  /** Directory of the store most recently opened, or empty when none has been. */
  private[scarlet] def lastDirectoryForTest: String = lastDirectory.get

  /** The next run publish, initial or merged, records the file and then throws. */
  private[scarlet] def armWriteFailure(): Unit =
    writesUntilFault.set(0)

  /**
   * `skip` publishes succeed. The next one, initial or merged, records the file and then throws.
   * `skip == 0` matches [[armWriteFailure]].
   */
  private[scarlet] def armWriteFailureAfter(skip: Int): Unit =
    require(skip >= 0, s"skip must be >= 0, got $skip")
    writesUntilFault.set(skip)

  /**
   * The 1-based merge pass throws inside its first new run, after the temp file exists and before
   * that run is published. Earlier passes have already published their runs.
   */
  private[scarlet] def armPassFailure(pass: Int): Unit =
    require(pass > 0, s"pass must be positive, got $pass")
    failAtPass.set(pass)

  /** Clears the file counter, the pass counter, the recorded directory, and armed failures. */
  private[scarlet] def resetForTest(): Unit =
    filesWritten.set(0L)
    mergePasses.set(0L)
    writesUntilFault.set(-1)
    failAtPass.set(0)
    lastDirectory.set("")

  private def spill(
      batches: Vector[ColumnBatch],
      partitions: Int,
      rows: Int,
      conf: ScarletConf,
  ): Vector[ColumnBatch] =
    // Cleared before open. A throw from the store constructor must not report a directory.
    lastDirectory.set("")
    val fanIn = conf.spillFanIn
    require(fanIn >= 2, s"spillFanIn must be >= 2, got $fanIn")
    val batchSize = conf.columnarBatchSize
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val directory = SpillStore.directory(conf)
    SpillStore.withSpill(directory, conf.spillMaxFiles, conf.spillMaxBytes) { store =>
      lastDirectory.set(store.directory.toString)
      val runs = writeRuns(store, batches, batchSize)
      val merged = reduce(store, runs, fanIn, batchSize)
      scatter(merged, rows, partitions)
    }

  private def rowCount(batches: Vector[ColumnBatch]): Int =
    val total = batches.iterator.map(_.length.toLong).sum
    require(total <= Int.MaxValue.toLong, s"sortAndSplit input reached $total rows")
    total.toInt

  private def partitionBytes(rows: Int): Long =
    if (rows <= 0) then 0L
    else rows.toLong * (4L + 4L + 1L) + HeaderAllowance

  /**
   * Sorts one input batch at a time and publishes it. An empty batch is not a file. The sorted
   * copy is local to this iteration, so the next partition is not sorted while it is still live. A
   * long batch is copied one morsel at a time inside the streaming write.
   */
  private def writeRuns(
      store: SpillStore,
      batches: Vector[ColumnBatch],
      batchSize: Int,
  ): Vector[SpillFile] =
    val stored = new Array[SpillFile](batches.length)
    var count = 0
    val pending = batches.iterator
    while (pending.hasNext) {
      val sorted = sortPartition(pending.next())
      if (sorted.length > 0) then
        stored(count) = publishRun(store, sorted, batchSize)
        count += 1
    }
    Vector.tabulate(count)(slot => stored(slot))

  private def sortPartition(batch: ColumnBatch): ColumnBatch =
    val sorted = RankSplit.sortAndSplit(Vector(batch), partitions = 1, parallelism = 1)
    sorted match
      case Vector(only) =>
        require(
          only.length == batch.length,
          s"sorted partition has ${only.length} rows and the input has ${batch.length}",
        )
        only
      case other =>
        throw new IllegalStateException(s"sorted partition produced ${other.length} batches")

  /**
   * One sorted partition, one file. A batch no longer than `batchSize` is written as itself. A
   * longer batch is copied one morsel at a time, so only that slice is live beside the sorted
   * batch.
   */
  private def publishRun(store: SpillStore, sorted: ColumnBatch, batchSize: Int): SpillFile =
    val file =
      if (sorted.length <= batchSize) then store.write(Vector(sorted))
      else
        store.writeStreaming(RankScatter.pairSchema) { sink =>
          writeSlices(sink, sorted, batchSize)
        }
    recorded(file)

  private def writeSlices(sink: SpillBatchWriter, sorted: ColumnBatch, batchSize: Int): Unit =
    val pending = Morsels.of(sorted, batchSize).iterator
    while (pending.hasNext) {
      val morsel = pending.next()
      sink.write(ColumnBatches.copyRange(morsel.batch, morsel.start, morsel.length))
    }

  /** Counts the file, then honors a write fault. Initial runs and merged runs both call this. */
  private def recorded(file: SpillFile): SpillFile =
    val _ = filesWritten.incrementAndGet()
    val remaining = writesUntilFault.getAndUpdate { current =>
      if (current <= 0) -1 else current - 1
    }
    if (remaining == 0) then throw new IllegalStateException(WriteFailed)
    file

  /**
   * Merges `runs` until one file remains. A single run is already that file. Each completed pass
   * counts for [[mergePassesForTest]].
   */
  private def reduce(
      store: SpillStore,
      runs: Vector[SpillFile],
      fanIn: Int,
      batchSize: Int,
  ): SpillFile =
    require(runs.nonEmpty, "rank split spilled rows and kept no run")
    var current = runs
    while (current.length > 1) {
      val pass = mergePasses.get + 1L
      val merged = mergePass(store, current, fanIn, batchSize, pass)
      require(
        merged.length < current.length,
        s"rank-split pass made no progress from ${current.length} runs",
      )
      current = merged
      val _ = mergePasses.incrementAndGet()
    }
    current match
      case Vector(file) => file
      case other =>
        throw new IllegalStateException(s"rank split reduced to ${other.length} runs")

  private def mergePass(
      store: SpillStore,
      runs: Vector[SpillFile],
      fanIn: Int,
      batchSize: Int,
      pass: Long,
  ): Vector[SpillFile] =
    groupRuns(runs, fanIn).map { group =>
      group match
        case Vector(file) => file
        case many =>
          val merged = mergeGroup(store, many, batchSize, pass)
          // The inputs are consumed. Delete them now so initial and intermediate runs do not
          // coexist against `spillMaxFiles` and `spillMaxBytes`.
          many.foreach(_.delete())
          merged
    }

  private def groupRuns(runs: Vector[SpillFile], fanIn: Int): Vector[Vector[SpillFile]] =
    val groups = (runs.length + fanIn - 1) / fanIn
    Vector.tabulate(groups) { index =>
      val start = index * fanIn
      runs.slice(start, math.min(start + fanIn, runs.length))
    }

  /**
   * K-way merge of at most `fanIn` sorted runs. Cursor ids are positions in this group, so an
   * earlier run stays ahead of a later run when the effective keys are equal. The output file is
   * one sorted run.
   */
  private def mergeGroup(
      store: SpillStore,
      files: Vector[SpillFile],
      batchSize: Int,
      pass: Long,
  ): SpillFile =
    val cursors = openCursors(files)
    try
      val keys = new Array[Int](cursors.length)
      val heap = openHeap(cursors, keys)
      val merged = store.writeStreaming(RankScatter.pairSchema) { sink =>
        failIfPass(pass)
        writeSorted(heap, keys, cursors, sink, batchSize)
      }
      recorded(merged)
    finally closeCursors(cursors)

  private def writeSorted(
      heap: OrderedRunHeap,
      keys: Array[Int],
      cursors: Array[RunCursor],
      sink: SpillBatchWriter,
      batchSize: Int,
  ): Unit =
    val bufKeys = new Array[Int](batchSize)
    val bufValues = new Array[Int](batchSize)
    val bufKeyWords = Validity.allocate(batchSize)
    val bufValueWords = Validity.allocate(batchSize)
    var filled = 0
    while (!heap.isEmpty) {
      val id = heap.pop()
      val cursor = cursors(id)
      if (cursor.keyPresent) then
        bufKeys(filled) = cursor.key
        Validity.setBit(bufKeyWords, filled)
      else bufKeys(filled) = 0
      if (cursor.valuePresent) then
        bufValues(filled) = cursor.value
        Validity.setBit(bufValueWords, filled)
      else bufValues(filled) = 0
      filled += 1
      cursor.advance()
      if (cursor.ready) then
        keys(id) = cursor.effectiveKey
        heap.push(id)
      if (filled == batchSize) then
        sink.write(emitBatch(bufKeys, bufKeyWords, bufValues, bufValueWords, filled))
        clearWords(bufKeyWords, filled)
        clearWords(bufValueWords, filled)
        filled = 0
    }
    if (filled > 0) then
      sink.write(emitBatch(bufKeys, bufKeyWords, bufValues, bufValueWords, filled))

  /** Copies the filled prefix. The validity tail is masked, not copied one bit at a time. */
  private def emitBatch(
      keys: Array[Int],
      keyWords: Array[Long],
      values: Array[Int],
      valueWords: Array[Long],
      filled: Int,
  ): ColumnBatch =
    RankScatter.pairs(
      Arrays.copyOf(keys, filled),
      trimWords(keyWords, filled),
      Arrays.copyOf(values, filled),
      trimWords(valueWords, filled),
      filled,
    )

  private def trimWords(words: Array[Long], rows: Int): Array[Long] =
    val count = (rows + 63) >>> 6
    val copied = Arrays.copyOf(words, count)
    val tail = rows & 63
    if (tail != 0) then copied(count - 1) = copied(count - 1) & Validity.tailMask(tail)
    copied

  private def clearWords(words: Array[Long], filled: Int): Unit =
    val used = (filled + 63) >>> 6
    if (used > 0) then Arrays.fill(words, 0, used, 0L)

  private def openCursors(files: Vector[SpillFile]): Array[RunCursor] =
    val opened = files.iterator.zipWithIndex.foldLeft(Vector.empty[RunCursor]) { (acc, item) =>
      val (file, id) = item
      try acc :+ new RunCursor(id, file)
      catch
        case thrown: Throwable =>
          closeAll(acc)
          throw thrown
    }
    opened.toArray

  /** Effective keys. A null key is stored as zero. The heap reads `keys(id)`. */
  private def openHeap(cursors: Array[RunCursor], keys: Array[Int]): OrderedRunHeap =
    val heap = new OrderedRunHeap(keys)
    var id = 0
    while (id < cursors.length) {
      if (cursors(id).ready) then
        keys(id) = cursors(id).effectiveKey
        heap.push(id)
      id += 1
    }
    heap

  private def closeCursors(cursors: Array[RunCursor]): Unit =
    var id = 0
    while (id < cursors.length) {
      cursors(id).close()
      id += 1
    }

  private def closeAll(cursors: Vector[RunCursor]): Unit =
    cursors.foreach(_.close())

  /** An armed pass failure runs after the temp file exists and before that run is published. */
  private def failIfPass(pass: Long): Unit =
    val armed = failAtPass.get
    if (armed > 0 && armed.toLong == pass) then
      failAtPass.set(0)
      throw new IllegalStateException(MergeFailed)

  private def scatter(file: SpillFile, rows: Int, partitions: Int): Vector[ColumnBatch] =
    val cursor = new RunCursor(0, file)
    try fill(cursor, rows, partitions)
    finally cursor.close()

  private def fill(cursor: RunCursor, rows: Int, partitions: Int): Vector[ColumnBatch] =
    RankScatter.scatter(rows, partitions, () => cursor.ready) { sink =>
      val keyOn = cursor.keyPresent
      val valueOn = cursor.valuePresent
      sink.write(
        keyOn,
        if (keyOn) cursor.key else 0,
        valueOn,
        if (valueOn) cursor.value else 0,
      )
      cursor.advance()
    }

  /**
   * One sorted run, read one batch at a time. [[close]] releases the channel if the caller stops
   * before the run is exhausted. A null key orders as zero and is not read from the payload.
   */
  private final class RunCursor(val id: Int, file: SpillFile):

    private val source: Iterator[ColumnBatch] & AutoCloseable = file.batches()
    private var keys: Array[Int] = Array.emptyIntArray
    private var keyWords: Array[Long] = Array.emptyLongArray
    private var values: Array[Int] = Array.emptyIntArray
    private var valueWords: Array[Long] = Array.emptyLongArray
    private var length: Int = 0
    private var index: Int = 0

    def close(): Unit = source.close()

    def ready: Boolean =
      if (index < length) then true else refill()

    def key: Int =
      require(index < length, "rank-split cursor is exhausted")
      keys(index)

    def keyPresent: Boolean =
      require(index < length, "rank-split cursor is exhausted")
      Validity.isSet(keyWords, index)

    def effectiveKey: Int =
      if (keyPresent) key else 0

    def value: Int =
      require(index < length, "rank-split cursor is exhausted")
      values(index)

    def valuePresent: Boolean =
      require(index < length, "rank-split cursor is exhausted")
      Validity.isSet(valueWords, index)

    def advance(): Unit =
      require(index < length, "rank-split cursor is exhausted")
      index += 1

    private def refill(): Boolean =
      var found = false
      var continue = true
      while (continue) {
        if (!source.hasNext) then continue = false
        else
          install(source.next())
          if (index < length) then
            found = true
            continue = false
      }
      found

    private def install(batch: ColumnBatch): Unit =
      val keyColumn = ColumnHash.int32At(batch, 0)
      val valueColumn = ColumnHash.int32At(batch, 1)
      keys = keyColumn.values
      keyWords = keyColumn.validity.words
      values = valueColumn.values
      valueWords = valueColumn.validity.words
      length = batch.length
      index = 0
