package com.ewoodbury.scarlet.columnar

/**
 * Rank-split of long-pair partitions.
 *
 * Each input batch is one source partition. It is sorted by effective key: a present key orders by
 * its value, and a null key orders as zero. The null payload is not read. Ties keep input order,
 * so the total order is (effective key, partition index, row index).
 *
 * The local sort is a bottom-up merge of row indexes. The merge is a binary min-heap of run
 * indexes, one cursor per input run, not a linear scan of those cursors and not `Ordering`. A heap
 * pop is logarithmic in the number of runs. There is one run per input batch. The heap stores at
 * most one long per run, including a run with no rows, which is never pushed.
 *
 * The row at global rank `r` is written into output partition `r * partitions / rows`. Both
 * factors are non-negative, and `/` is integer division. That quotient is the inverse of the
 * partition start in [[RankBounds]]: partition `i` starts at `ceil(i * rows / partitions)`,
 * computed as `(i * rows + partitions - 1) / partitions`. Output partition `i` is sorted. Sizes
 * differ by at most one. The last effective key of a non-empty partition is at most the first
 * effective key of the next non-empty partition. [[RankSplit]] uses the same boundaries.
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
 * `rows == 0` returns `partitions` empty long-pair batches and does not sort. `partitions <= 0`,
 * `parallelism <= 0`, and an empty `batches` vector are rejected. This object does not choose
 * spill. [[RankSplitLongRuns]] does, and it calls this split when the input fits.
 */
object RankSplitLong:

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
      Vector.fill(partitions)(ColumnBatches.emptyLongPairs)
    else merge(sortRuns(batches, parallelism), rows, partitions)

  private def rowCount(batches: Vector[ColumnBatch]): Int =
    val total = batches.iterator.map(_.length.toLong).sum
    require(total <= Int.MaxValue.toLong, s"sortAndSplit input reached $total rows")
    total.toInt

  private def sortRuns(batches: Vector[ColumnBatch], parallelism: Int): Vector[ColumnBatch] =
    MorselScheduler.map(batches, parallelism)((_, batch) => sortBatch(batch))

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
      sortIndices(keys.values, keys.validity.words, order)
      gather(keys, values, order)

  /** Sort `order` by effective key, then by the row index stored in `order`. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def sortIndices(keys: Array[Long], keyWords: Array[Long], order: Array[Int]): Unit =
    val rows = order.length
    if (rows > 1) then
      val buffer = new Array[Int](rows)
      var src = order
      var dst = buffer
      var sortedInOrder = true
      var width = 1
      while (width < rows) {
        var start = 0
        while (start < rows) {
          val mid = math.min(start.toLong + width.toLong, rows.toLong).toInt
          val end = math.min(start.toLong + (width.toLong << 1), rows.toLong).toInt
          mergeIndices(keys, keyWords, src, dst, start, mid, end)
          start = end
        }
        val previous = src
        src = dst
        dst = previous
        sortedInOrder = !sortedInOrder
        val grown = width.toLong << 1
        width = if (grown >= rows.toLong) rows else grown.toInt
      }
      if (!sortedInOrder) then System.arraycopy(src, 0, order, 0, rows)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def mergeIndices(
      keys: Array[Long],
      keyWords: Array[Long],
      src: Array[Int],
      dst: Array[Int],
      start: Int,
      mid: Int,
      end: Int,
  ): Unit =
    var left = start
    var right = mid
    var out = start
    while (left < mid && right < end) {
      if (before(keys, keyWords, src(left), src(right))) then
        dst(out) = src(left)
        left += 1
      else
        dst(out) = src(right)
        right += 1
      out += 1
    }
    while (left < mid) {
      dst(out) = src(left)
      left += 1
      out += 1
    }
    while (right < end) {
      dst(out) = src(right)
      right += 1
      out += 1
    }

  /** Earlier effective key first. Equal keys keep the smaller input row index. */
  private def before(
      keys: Array[Long],
      keyWords: Array[Long],
      leftRow: Int,
      rightRow: Int,
  ): Boolean =
    val order = java.lang.Long.compare(
      effective(keys, keyWords, leftRow),
      effective(keys, keyWords, rightRow),
    )
    order < 0 || (order == 0 && leftRow < rightRow)

  private def effective(keys: Array[Long], keyWords: Array[Long], row: Int): Long =
    if (Validity.isSet(keyWords, row)) keys(row) else 0L

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def gather(keys: Int64Column, values: Int64Column, order: Array[Int]): ColumnBatch =
    val count = order.length
    val outKeys = new Array[Long](count)
    val outValues = new Array[Long](count)
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
    RankScatterLong.pairs(outKeys, keyWords, outValues, valueWords, count)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def merge(runs: Vector[ColumnBatch], rows: Int, partitions: Int): Vector[ColumnBatch] =
    val opened: Array[Run] = runs.iterator.map(openRun).toArray
    val cursors = new Array[Int](opened.length)
    val keys = new Array[Long](opened.length)
    val heap = new OrderedRunHeapLong(keys)
    var run = 0
    while (run < opened.length) {
      if (opened(run).length > 0) then pushRun(heap, keys, opened, cursors, run)
      run += 1
    }
    RankScatterLong.scatter(rows, partitions, () => !heap.isEmpty) { sink =>
      val chosen = heap.pop()
      val cursor = cursors(chosen)
      val source = opened(chosen)
      val keyOn = Validity.isSet(source.keyWords, cursor)
      val valueOn = Validity.isSet(source.valueWords, cursor)
      sink.write(
        keyOn,
        if (keyOn) source.keys(cursor) else 0L,
        valueOn,
        if (valueOn) source.values(cursor) else 0L,
      )
      cursors(chosen) = cursor + 1
      if (cursors(chosen) < source.length) then pushRun(heap, keys, opened, cursors, chosen)
    }

  /**
   * Snapshot the effective key, then push. The heap reads `keys(id)` and does not recompute it.
   */
  private def pushRun(
      heap: OrderedRunHeapLong,
      keys: Array[Long],
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

  private def checkedPair(batch: ColumnBatch): (Int64Column, Int64Column) =
    require(
      batch.columns.length == 2,
      s"sortAndSplit expected two Int64 columns, got ${batch.schema.mkString("[", ", ", "]")}",
    )
    val keys = ColumnHash.int64At(batch, 0)
    val values = ColumnHash.int64At(batch, 1)
    require(
      keys.length == batch.length && values.length == batch.length,
      s"key length ${keys.length}, value length ${values.length}, and batch length ${batch.length} disagree",
    )
    (keys, values)

  private final class Run(
      val keys: Array[Long],
      val keyWords: Array[Long],
      val values: Array[Long],
      val valueWords: Array[Long],
      val length: Int,
  )
