package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Rank-split of long-pair partitions.
 *
 * Each input batch is one source partition. It is sorted by effective key: a present key orders by
 * its value, and a null key orders as zero. The null payload is not read. Ties keep input order,
 * so the total order is (effective key, partition index, row index).
 *
 * When every live key and value is present, each run is a stable LSD radix of the pair
 * ([[RadixPass]]) and the merge copies present payloads without a per-row validity branch. A null
 * key or a null value keeps the bottom-up merge of row indexes, which orders a null key as zero
 * and leaves the bit clear. The cross-run merge is a binary min-heap of run indexes, one cursor
 * per input run, not a linear scan of those cursors and not `Ordering`. A heap pop is logarithmic
 * in the number of runs. There is one run per input batch. A dense merge of one or two runs does
 * not use the heap. It has the same tie-break: the earlier run first. The heap stores at most one
 * long per run, including a run with no rows, which is never pushed.
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
 * `parallelism <= 0`, and an empty `batches` vector are rejected. [[sortAndSplitSelected]] sorts
 * the live prefix of each [[Selected]]. An empty prefix is an empty run. A null key orders as zero
 * and stays null. This object does not choose spill. [[RankSplitLongRuns]] does, and it calls this
 * split when the input fits.
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
    else if (densePairs(batches)) then
      mergeDense(sortRunsDense(batches, parallelism), rows, partitions)
    else merge(sortRuns(batches, parallelism), rows, partitions)

  /**
   * [[sortAndSplit]] for a survivor index. `rows` is the sum of the live prefixes. An empty
   * selection publishes `partitions` empty long-pair batches and does not sort. Each other prefix
   * is one sorted run of `count` rows.
   *
   * Same handoff as [[RankSplit.sortAndSplitSelected]]. [[stageSelected]] copies a dense prefix.
   * [[finishSelected]] allocates the [[RadixPass]] scratch and merges. Drop the selections between
   * the two calls so the source batches are not live beside that scratch.
   */
  def sortAndSplitSelected(
      selected: Vector[Selected],
      partitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"numPartitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(selected.nonEmpty, "sortAndSplitSelected needs at least one batch")
    finishSelected(stageSelected(selected, parallelism), partitions)

  /** Copy each live prefix. The selections are not retained. See [[RankSplit.stageSelected]]. */
  private[scarlet] def stageSelected(
      selected: Option[Vector[Selected]],
      parallelism: Int,
  ): SelectedRun =
    selected match
      case Some(parts) => stageSelected(parts, parallelism)
      case None =>
        throw new IllegalArgumentException("sortAndSplitSelected needs at least one batch")

  /** [[stageSelected]] for a vector the caller still holds. */
  private[scarlet] def stageSelected(
      selected: Vector[Selected],
      parallelism: Int,
  ): SelectedRun =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(selected.nonEmpty, "sortAndSplitSelected needs at least one batch")
    val rows = selectedRows(selected)
    if (rows == 0) then
      selected.foreach(one => checkedPair(one.batch))
      new EmptyRun
    else if (denseSelected(selected)) then
      new DenseRun(gatherDense(selected, parallelism), rows, parallelism)
    else new SparseRun(sortRunsSelected(selected, parallelism), rows)

  /** Sort staged dense prefixes with [[RadixPass]] and merge. Sparse runs are already sorted. */
  private[scarlet] def finishSelected(
      staged: SelectedRun,
      partitions: Int,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"numPartitions must be positive, got $partitions")
    staged match
      case _: EmptyRun =>
        Vector.fill(partitions)(ColumnBatches.emptyLongPairs)
      case dense: DenseRun =>
        mergeDense(radixPrefixes(dense.prefixes, dense.parallelism), dense.rows, partitions)
      case sparse: SparseRun =>
        merge(sparse.runs, sparse.rows, partitions)

  private def selectedRows(selected: Vector[Selected]): Int =
    val total = selected.iterator.map(_.count.toLong).sum
    require(total <= Int.MaxValue.toLong, s"sortAndSplitSelected input reached $total rows")
    total.toInt

  private def rowCount(batches: Vector[ColumnBatch]): Int =
    val total = batches.iterator.map(_.length.toLong).sum
    require(total <= Int.MaxValue.toLong, s"sortAndSplit input reached $total rows")
    total.toInt

  /** True when every live key and value is present, so [[RadixPass]] may sort the pairs. */
  private def densePairs(batches: Vector[ColumnBatch]): Boolean =
    batches.forall { batch =>
      val (keys, values) = checkedPair(batch)
      Validity.livePresent(keys.validity, batch.length) &&
      Validity.livePresent(values.validity, batch.length)
    }

  /** True when every selected key and value is present. A dropped null does not disqualify. */
  private def denseSelected(selected: Vector[Selected]): Boolean =
    selected.forall(selectionPresent)

  private def selectionPresent(selected: Selected): Boolean =
    val count = selected.count
    if (count == 0) then true
    else
      val (keys, values) = checkedPair(selected.batch)
      if (
          Validity.livePresent(keys.validity, selected.batch.length) &&
          Validity.livePresent(values.validity, selected.batch.length)
        )
      then true
      else rowsPresent(keys, values, selected.rows, count)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def rowsPresent(
      keys: Int64Column,
      values: Int64Column,
      ids: Array[Int],
      count: Int,
  ): Boolean =
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    var slot = 0
    var dense = true
    while (slot < count && dense) {
      val row = ids(slot)
      dense = Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row)
      slot += 1
    }
    dense

  private def sortRuns(batches: Vector[ColumnBatch], parallelism: Int): Vector[ColumnBatch] =
    MorselScheduler.map(batches, parallelism)((_, batch) => sortBatch(batch))

  private def sortRunsSelected(
      selected: Vector[Selected],
      parallelism: Int,
  ): Vector[ColumnBatch] =
    MorselScheduler.map(selected, parallelism)((_, one) => sortSelected(one))

  private def sortRunsDense(batches: Vector[ColumnBatch], parallelism: Int): Vector[ColumnBatch] =
    MorselScheduler.map(batches, parallelism)((_, batch) => sortBatchDense(batch))

  private def gatherDense(selected: Vector[Selected], parallelism: Int): Vector[LongPrefix] =
    MorselScheduler.map(selected, parallelism)((_, one) => copyPrefix(one))

  private def radixPrefixes(prefixes: Vector[LongPrefix], parallelism: Int): Vector[ColumnBatch] =
    MorselScheduler.map(prefixes, parallelism)((_, prefix) => radixPrefix(prefix))

  private def sortBatchDense(batch: ColumnBatch): ColumnBatch =
    val rows = batch.length
    if (rows <= 1) then batch
    else
      val (keys, values) = checkedPair(batch)
      val copiedKeys = Arrays.copyOf(keys.values, rows)
      val copiedValues = Arrays.copyOf(values.values, rows)
      val sorted = RadixPass.sortLongs(copiedKeys, copiedValues, rows)
      presentLongs(sorted.keys, sorted.values, rows)

  /**
   * One dense prefix. Ties keep the smaller id. The source still holds dropped rows, so it is not
   * reused. The copy is the [[RadixPass]] ping-pong source. This does not allocate the scratch.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def copyPrefix(selected: Selected): LongPrefix =
    val count = selected.count
    if (count == 0) then emptyPrefix
    else
      val (keys, values) = checkedPair(selected.batch)
      val copiedKeys = new Array[Long](count)
      val copiedValues = new Array[Long](count)
      var slot = 0
      while (slot < count) {
        val row = selected.rows(slot)
        copiedKeys(slot) = keys.values(row)
        copiedValues(slot) = values.values(row)
        slot += 1
      }
      new LongPrefix(copiedKeys, copiedValues, count)

  /** Sort one copied prefix. `count <= 1` publishes the copy and does not allocate scratch. */
  private def radixPrefix(prefix: LongPrefix): ColumnBatch =
    val count = prefix.count
    if (count == 0) then ColumnBatches.emptyLongPairs
    else if (count == 1) then presentLongs(prefix.keys, prefix.values, count)
    else
      val sorted = RadixPass.sortLongs(prefix.keys, prefix.values, count)
      presentLongs(sorted.keys, sorted.values, count)

  private def presentLongs(keys: Array[Long], values: Array[Long], rows: Int): ColumnBatch =
    val keyWords = Validity.allValid(rows).words
    val valueWords = Validity.allValid(rows).words
    RankScatterLong.pairs(keys, keyWords, values, valueWords, rows)

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

  /**
   * One dense run of the live prefix. `order` holds the absolute ids, so [[sortIndices]] and
   * [[gather]] are the ones [[sortBatch]] uses. Ties keep the smaller id, which is input order. A
   * null key orders as zero and stays null. A null value stays null. The batch has `count` rows,
   * including when `count` is 0 or 1: the source still holds the dropped rows, so it is not
   * reused.
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
    sortIndices(keys.values, keys.validity.words, order)
    gather(keys, values, order)

  /** Sort `order` by effective key, then by the row index stored in `order`. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def sortIndices(keys: Array[Long], keyWords: Array[Long], order: Array[Int]): Unit =
    IndexMerge.sort(order) { (src, dst, start, mid, end) =>
      mergeIndices(keys, keyWords, src, dst, start, mid, end)
    }

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

  /**
   * Merge of present runs. Equal keys keep the earlier run. One run is sliced. Two runs are a
   * direct merge. More runs use [[OrderedRunHeapLong]].
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def mergeDense(
      runs: Vector[ColumnBatch],
      rows: Int,
      partitions: Int,
  ): Vector[ColumnBatch] =
    val opened: Array[Run] = runs.iterator.map(openRun).toArray
    var live = 0
    var run = 0
    while (run < opened.length) {
      if (opened(run).length > 0) then live += 1
      run += 1
    }
    require(live > 0, s"dense rank split lost $rows rows")
    if (live == 1 && partitions == 1) then onePartition(runs)
    else if (live == 1) then sliceOne(opened, rows, partitions)
    else if (live == 2) then mergeTwo(opened, rows, partitions)
    else mergeHeap(opened, rows, partitions)

  private def onePartition(runs: Vector[ColumnBatch]): Vector[ColumnBatch] =
    runs.iterator.find(_.length > 0) match
      case Some(batch) => Vector(batch)
      case None =>
        throw new IllegalStateException("dense rank split lost its only run")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def sliceOne(opened: Array[Run], rows: Int, partitions: Int): Vector[ColumnBatch] =
    var chosen = 0
    var run = 0
    while (run < opened.length) {
      if (opened(run).length > 0) then chosen = run
      run += 1
    }
    val source = opened(chosen)
    val sizes = RankBounds.outputSizes(rows, partitions)
    val out = new Array[ColumnBatch](partitions)
    var part = 0
    while (part < partitions) {
      val size = sizes(part)
      if (size == 0) then out(part) = ColumnBatches.emptyLongPairs
      else
        val start = RankBounds.firstRank(part, rows, partitions)
        val keys = new Array[Long](size)
        val values = new Array[Long](size)
        System.arraycopy(source.keys, start, keys, 0, size)
        System.arraycopy(source.values, start, values, 0, size)
        out(part) = presentLongs(keys, values, size)
      part += 1
    }
    out.toVector

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def mergeTwo(opened: Array[Run], rows: Int, partitions: Int): Vector[ColumnBatch] =
    var first = -1
    var second = -1
    var run = 0
    while (run < opened.length) {
      if (opened(run).length > 0) then
        if (first < 0) then first = run
        else second = run
      run += 1
    }
    val left = opened(first)
    val right = opened(second)
    val leftKeys = left.keys
    val leftValues = left.values
    val rightKeys = right.keys
    val rightValues = right.values
    val leftRows = left.length
    val rightRows = right.length
    var leftAt = 0
    var rightAt = 0
    val sizes = RankBounds.outputSizes(rows, partitions)
    val out = new Array[ColumnBatch](partitions)
    var part = 0
    while (part < partitions) {
      val size = sizes(part)
      if (size == 0) then out(part) = ColumnBatches.emptyLongPairs
      else
        val keys = new Array[Long](size)
        val values = new Array[Long](size)
        var filled = 0
        while (filled < size) {
          val takeLeft =
            rightAt == rightRows ||
              (leftAt < leftRows && leftKeys(leftAt) <= rightKeys(rightAt))
          if (takeLeft) then
            keys(filled) = leftKeys(leftAt)
            values(filled) = leftValues(leftAt)
            leftAt += 1
          else
            keys(filled) = rightKeys(rightAt)
            values(filled) = rightValues(rightAt)
            rightAt += 1
          filled += 1
        }
        out(part) = presentLongs(keys, values, size)
      part += 1
    }
    require(leftAt + rightAt == rows, s"dense merge wrote ${leftAt + rightAt} of $rows")
    out.toVector

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def mergeHeap(opened: Array[Run], rows: Int, partitions: Int): Vector[ColumnBatch] =
    val cursors = new Array[Int](opened.length)
    val heapKeys = new Array[Long](opened.length)
    val heap = new OrderedRunHeapLong(heapKeys)
    var run = 0
    while (run < opened.length) {
      if (opened(run).length > 0) then
        heapKeys(run) = opened(run).keys(0)
        heap.push(run)
      run += 1
    }
    val sizes = RankBounds.outputSizes(rows, partitions)
    val out = new Array[ColumnBatch](partitions)
    var part = 0
    var written = 0
    while (part < partitions) {
      val size = sizes(part)
      if (size == 0) then out(part) = ColumnBatches.emptyLongPairs
      else
        val keys = new Array[Long](size)
        val values = new Array[Long](size)
        var filled = 0
        while (filled < size) {
          val chosen = heap.pop()
          val cursor = cursors(chosen)
          val source = opened(chosen)
          keys(filled) = source.keys(cursor)
          values(filled) = source.values(cursor)
          val next = cursor + 1
          cursors(chosen) = next
          if (next < source.length) then
            heapKeys(chosen) = source.keys(next)
            heap.push(chosen)
          filled += 1
        }
        written += size
        out(part) = presentLongs(keys, values, size)
      part += 1
    }
    require(written == rows, s"dense heap merge wrote $written of $rows")
    out.toVector

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

  private val emptyPrefix = new LongPrefix(new Array[Long](0), new Array[Long](0), 0)

  /** Gathered dense prefix. [[RadixPass]] uses `keys` and `values` as ping-pong storage. */
  private[scarlet] final class LongPrefix(
      val keys: Array[Long],
      val values: Array[Long],
      val count: Int,
  )

  /** Live prefix copied off the source selections. No field points at a [[Selected]]. */
  private[scarlet] sealed trait SelectedRun

  private[scarlet] final class EmptyRun extends SelectedRun

  private[scarlet] final class DenseRun(
      val prefixes: Vector[LongPrefix],
      val rows: Int,
      val parallelism: Int,
  ) extends SelectedRun

  private[scarlet] final class SparseRun(val runs: Vector[ColumnBatch], val rows: Int)
      extends SelectedRun

  private final class Run(
      val keys: Array[Long],
      val keyWords: Array[Long],
      val values: Array[Long],
      val valueWords: Array[Long],
      val length: Int,
  )
