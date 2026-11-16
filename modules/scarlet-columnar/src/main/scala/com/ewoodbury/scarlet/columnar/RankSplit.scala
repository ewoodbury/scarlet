package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

/**
 * Rank-split of int-pair partitions.
 *
 * Each input batch is one source partition. It is sorted by effective key: a present key orders by
 * its value, and a null key orders as zero. The null payload is not read. Ties keep input order,
 * so the total order is (effective key, partition index, row index).
 *
 * The local sort is [[IntIndexSort]], a stable radix of row indexes by effective key. The
 * cross-run merge is a binary min-heap of run indexes, one cursor per input run, not a linear scan
 * of those cursors and not `Ordering`. A heap pop is logarithmic in the number of runs. There is
 * one run per input batch. The heap stores at most one int per run, including a run with no rows,
 * which is never pushed.
 *
 * The row at global rank `r` is written into output partition `r * partitions / rows`. Both
 * factors are non-negative, and `/` is integer division. That quotient is the inverse of the
 * partition start in [[RankScatter]]: partition `i` starts at `ceil(i * rows / partitions)`,
 * computed as `(i * rows + partitions - 1) / partitions`. Output partition `i` is sorted. Sizes
 * differ by at most one. The last effective key of a non-empty partition is at most the first
 * effective key of the next non-empty partition.
 *
 * A null key stays a null key. A null value stays null. Validity bits are copied. Payloads are
 * copied only when the bit is set.
 *
 * During the merge the retained batches are one sorted batch per input run plus the output
 * batches. A run of length 0 or 1 reuses its input batch. A longer run is a fresh batch, and its
 * index array and merge buffer are dropped when that batch is published. At most `parallelism`
 * runs are sorted at once, so those transient buffers are bounded by `parallelism`. The caller may
 * still hold the input vector. No per-row object is allocated.
 *
 * `rows == 0` returns `partitions` empty pair batches and does not sort. `partitions <= 0`,
 * `parallelism <= 0`, and an empty `batches` vector are rejected.
 *
 * [[sortAndSplitSelected]] sorts the live prefix of each [[Selected]] and publishes a dense batch
 * of `count` rows. An empty prefix is an empty run. [[sortAndSplit]] and [[sortBatch]] are
 * unchanged.
 */
object RankSplit:

  private val runsForTestCounter = new AtomicInteger(0)
  private val mergesForTestCounter = new AtomicInteger(0)

  /** Non-empty runs in the most recent [[merge]]. */
  private[scarlet] def runsForTest: Int = runsForTestCounter.get

  /** In-memory k-way merges since [[resetForTest]]. One sort is one merge. */
  private[scarlet] def mergePassesForTest: Int = mergesForTestCounter.get

  /** Clears the run and merge counters. */
  private[scarlet] def resetForTest(): Unit =
    runsForTestCounter.set(0)
    mergesForTestCounter.set(0)

  def sortAndSplit(
      batches: Vector[ColumnBatch],
      partitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"numPartitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batches.nonEmpty, "sortAndSplit needs at least one batch")
    val rows = rowCount(batches)
    if (rows == 0) then
      batches.foreach(checkedPair)
      Vector.fill(partitions)(ColumnBatches.emptyIntPairs)
    else merge(sortRuns(batches, parallelism), rows, partitions)

  /**
   * [[sortAndSplit]] for a survivor index. `rows` is the sum of the live prefixes. An empty
   * selection publishes `partitions` empty int-pair batches and does not sort. Each other prefix
   * is one sorted run of `count` rows. The merge is [[merge]].
   */
  def sortAndSplitSelected(
      selected: Vector[Selected],
      partitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"numPartitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(selected.nonEmpty, "sortAndSplitSelected needs at least one batch")
    val rows = selectedRows(selected)
    if (rows == 0) then
      selected.foreach(one => checkedPair(one.batch))
      Vector.fill(partitions)(ColumnBatches.emptyIntPairs)
    else merge(sortRunsSelected(selected, parallelism), rows, partitions)

  private def selectedRows(selected: Vector[Selected]): Int =
    val total = selected.iterator.map(_.count.toLong).sum
    require(total <= Int.MaxValue.toLong, s"sortAndSplitSelected input reached $total rows")
    total.toInt

  private def rowCount(batches: Vector[ColumnBatch]): Int =
    val total = batches.iterator.map(_.length.toLong).sum
    require(total <= Int.MaxValue.toLong, s"sortAndSplit input reached $total rows")
    total.toInt

  private def sortRuns(batches: Vector[ColumnBatch], parallelism: Int): Vector[ColumnBatch] =
    val started = System.nanoTime()
    val sorted = MorselScheduler.map(batches, parallelism)((_, batch) => sortBatch(batch))
    RankSplitTrace.addRun(System.nanoTime() - started)
    sorted

  private def sortRunsSelected(
      selected: Vector[Selected],
      parallelism: Int,
  ): Vector[ColumnBatch] =
    val started = System.nanoTime()
    val sorted = MorselScheduler.map(selected, parallelism)((_, one) => sortSelected(one))
    RankSplitTrace.addRun(System.nanoTime() - started)
    sorted

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def sortBatch(batch: ColumnBatch): ColumnBatch =
    val (keys, values) = checkedPair(batch)
    val rows = batch.length
    if (rows <= 1) then batch
    else
      val order = new Array[Int](rows)
      var row = 0
      while (row < rows) {
        order(row) = row
        row += 1
      }
      val indexStarted = System.nanoTime()
      sortIndices(keys.values, keys.validity.words, order)
      RankSplitTrace.addIndex(System.nanoTime() - indexStarted)
      val gatherStarted = System.nanoTime()
      val gathered = gather(keys, values, order)
      RankSplitTrace.addGather(System.nanoTime() - gatherStarted)
      gathered

  /**
   * One dense run of the live prefix. `order` holds the absolute ids, so [[sortIndices]] and
   * [[gather]] are the ones [[sortBatch]] uses. Ties keep the smaller id, which is input order. A
   * null key orders as zero and stays null. The batch has `count` rows, including when `count` is
   * 0 or 1: the source still holds the dropped rows, so it is not reused.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def sortSelected(selected: Selected): ColumnBatch =
    val (keys, values) = checkedPair(selected.batch)
    val count = selected.count
    val order = new Array[Int](count)
    var slot = 0
    while (slot < count) {
      order(slot) = selected.rows(slot)
      slot += 1
    }
    val indexStarted = System.nanoTime()
    sortIndices(keys.values, keys.validity.words, order)
    RankSplitTrace.addIndex(System.nanoTime() - indexStarted)
    val gatherStarted = System.nanoTime()
    val gathered = gather(keys, values, order)
    RankSplitTrace.addGather(System.nanoTime() - gatherStarted)
    gathered

  /** Sort `order` by effective key. Equal keys keep the smaller input row index. */
  private def sortIndices(keys: Array[Int], keyWords: Array[Long], order: Array[Int]): Unit =
    if (order.length > 1) then
      IntIndexSort.sort(keys, keyWords, order)
      RankSplitTrace.notePasses(2)

  private def effective(keys: Array[Int], keyWords: Array[Long], row: Int): Int =
    if (Validity.isSet(keyWords, row)) keys(row) else 0

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def gather(keys: Int32Column, values: Int32Column, order: Array[Int]): ColumnBatch =
    val count = order.length
    val outKeys = new Array[Int](count)
    val outValues = new Array[Int](count)
    val keyWords = Validity.allocate(count)
    val valueWords = Validity.allocate(count)
    val srcKeyWords = keys.validity.words
    val srcValueWords = values.validity.words
    var out = 0
    while (out < count) {
      val row = order(out)
      if (Validity.isSet(srcKeyWords, row)) then
        outKeys(out) = keys.values(row)
        Validity.setBit(keyWords, out)
      if (Validity.isSet(srcValueWords, row)) then
        outValues(out) = values.values(row)
        Validity.setBit(valueWords, out)
      out += 1
    }
    RankScatter.pairs(outKeys, keyWords, outValues, valueWords, count)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def merge(runs: Vector[ColumnBatch], rows: Int, partitions: Int): Vector[ColumnBatch] =
    val started = System.nanoTime()
    val opened: Array[Run] = runs.iterator.map(openRun).toArray
    val cursors = new Array[Int](opened.length)
    val keys = new Array[Int](opened.length)
    val heap = new OrderedRunHeap(keys)
    var run = 0
    var live = 0
    while (run < opened.length) {
      if (opened(run).length > 0) then
        pushRun(heap, keys, opened, cursors, run)
        live += 1
      run += 1
    }
    runsForTestCounter.set(live)
    RankSplitTrace.addRuns(live.toLong)
    val _ = mergesForTestCounter.incrementAndGet()
    val scattered = RankScatter.scatter(rows, partitions, () => !heap.isEmpty) { sink =>
      val chosen = heap.pop()
      val cursor = cursors(chosen)
      val source = opened(chosen)
      val keyOn = Validity.isSet(source.keyWords, cursor)
      val valueOn = Validity.isSet(source.valueWords, cursor)
      sink.write(
        keyOn,
        if (keyOn) source.keys(cursor) else 0,
        valueOn,
        if (valueOn) source.values(cursor) else 0,
      )
      cursors(chosen) = cursor + 1
      if (cursors(chosen) < source.length) then pushRun(heap, keys, opened, cursors, chosen)
    }
    RankSplitTrace.addMerge(System.nanoTime() - started)
    scattered

  /**
   * Snapshot the effective key, then push. The heap reads `keys(id)` and does not recompute it.
   */
  private def pushRun(
      heap: OrderedRunHeap,
      keys: Array[Int],
      opened: Array[Run],
      cursors: Array[Int],
      id: Int,
  ): Unit =
    val source = opened(id)
    keys(id) = effective(source.keys, source.keyWords, cursors(id))
    heap.push(id)

  private def openRun(batch: ColumnBatch): Run =
    val (keys, values) = checkedPair(batch)
    new Run(keys.values, keys.validity.words, values.values, values.validity.words, batch.length)

  private def checkedPair(batch: ColumnBatch): (Int32Column, Int32Column) =
    require(
      batch.columns.length == 2,
      s"sortAndSplit expected two Int32 columns, got ${batch.schema.mkString("[", ", ", "]")}",
    )
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(
      keys.length == batch.length && values.length == batch.length,
      s"key length ${keys.length}, value length ${values.length}, and batch length ${batch.length} disagree",
    )
    (keys, values)

  private final class Run(
      val keys: Array[Int],
      val keyWords: Array[Long],
      val values: Array[Int],
      val valueWords: Array[Long],
      val length: Int,
  )
