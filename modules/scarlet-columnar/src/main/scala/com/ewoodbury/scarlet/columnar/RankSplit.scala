package com.ewoodbury.scarlet.columnar

/**
 * Rank-split of int-pair partitions.
 *
 * Each input batch is one source partition. It is sorted by effective key: a present key orders by
 * its value, and a null key orders as zero. The null payload is not read. Ties keep input order,
 * so the total order is (effective key, partition index, row index).
 *
 * The local sort is a bottom-up merge of row indexes. The merge is a binary min-heap of run
 * indexes, one cursor per input run, not a linear scan of those cursors and not `Ordering`. A heap
 * pop is logarithmic in the number of runs. There is one run per input batch. The heap stores at
 * most one int per run, including a run with no rows, which is never pushed.
 *
 * The row at global rank `r` is written into output partition `r * partitions / rows`. Both
 * factors are non-negative, and `/` is integer division. That quotient is the inverse of
 * [[firstRank]]: partition `i` starts at `ceil(i * rows / partitions)`, computed as
 * `(i * rows + partitions - 1) / partitions`. Output partition `i` is sorted. Sizes differ by at
 * most one. The last effective key of a non-empty partition is at most the first effective key of
 * the next non-empty partition.
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
 */
object RankSplit:

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
  private def sortIndices(keys: Array[Int], keyWords: Array[Long], order: Array[Int]): Unit =
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
      keys: Array[Int],
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
      keys: Array[Int],
      keyWords: Array[Long],
      leftRow: Int,
      rightRow: Int,
  ): Boolean =
    val order = Integer.compare(
      effective(keys, keyWords, leftRow),
      effective(keys, keyWords, rightRow),
    )
    order < 0 || (order == 0 && leftRow < rightRow)

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
    pairBatch(outKeys, keyWords, outValues, valueWords, count)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def merge(runs: Vector[ColumnBatch], rows: Int, partitions: Int): Vector[ColumnBatch] =
    val opened: Array[Run] = runs.iterator.map(openRun).toArray
    val cursors = new Array[Int](opened.length)
    val heap = new RunHeap(opened, cursors)
    var run = 0
    while (run < opened.length) {
      if (opened(run).length > 0) then heap.push(run)
      run += 1
    }
    val sizes = outputSizes(rows, partitions)
    val sinks = Array.tabulate(partitions)(bucket => new Sink(sizes(bucket)))
    var rank = 0
    var part = 0
    while (!heap.isEmpty) {
      while (part + 1 < partitions && rank >= firstRank(part + 1, rows, partitions)) {
        part += 1
      }
      val chosen = heap.pop()
      val cursor = cursors(chosen)
      sinks(part).write(opened(chosen), cursor)
      cursors(chosen) = cursor + 1
      if (cursors(chosen) < opened(chosen).length) then heap.push(chosen)
      rank += 1
    }
    require(rank == rows, s"rank split wrote $rank rows and counted $rows")
    Vector.tabulate(partitions) { bucket =>
      if (sizes(bucket) == 0) then ColumnBatches.emptyIntPairs
      else sinks(bucket).toBatch
    }

  /**
   * First global rank of output partition `part`. `part == partitions` is `rows`.
   *
   * The value is `ceil(part * rows / partitions)`, as integer division
   * `(part * rows + partitions - 1) / partitions`. Rank `r` belongs to partition
   * `r * partitions / rows`, also integer division, which is the inverse of this ceiling.
   */
  private def firstRank(part: Int, rows: Int, partitions: Int): Int =
    val rank = (part.toLong * rows.toLong + partitions.toLong - 1L) / partitions.toLong
    rank.toInt

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def outputSizes(rows: Int, partitions: Int): Array[Int] =
    val sizes = new Array[Int](partitions)
    var part = 0
    var total = 0
    while (part < partitions) {
      val size = firstRank(part + 1, rows, partitions) - firstRank(part, rows, partitions)
      sizes(part) = size
      total += size
      part += 1
    }
    require(total == rows, s"rank split sized $total rows and counted $rows")
    sizes

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

  private def pairBatch(
      keys: Array[Int],
      keyWords: Array[Long],
      values: Array[Int],
      valueWords: Array[Long],
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(keys, Validity.fromWords(keyWords, rows), rows),
        Int32Column.of(values, Validity.fromWords(valueWords, rows), rows),
      ),
      rows,
    )

  private final class Run(
      val keys: Array[Int],
      val keyWords: Array[Long],
      val values: Array[Int],
      val valueWords: Array[Long],
      val length: Int,
  )

  /** Min-heap of run indexes. Order is the effective key at the cursor, then the run index. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class RunHeap(runs: Array[Run], cursors: Array[Int]):

    private val nodes = new Array[Int](runs.length)
    private var size: Int = 0

    def isEmpty: Boolean = size == 0

    def pop(): Int =
      val chosen = nodes(0)
      size -= 1
      if (size > 0) then
        nodes(0) = nodes(size)
        siftDown(0)
      chosen

    def push(id: Int): Unit =
      nodes(size) = id
      siftUp(size)
      size += 1

    private def less(leftId: Int, rightId: Int): Boolean =
      val order = Integer.compare(
        effective(runs(leftId).keys, runs(leftId).keyWords, cursors(leftId)),
        effective(runs(rightId).keys, runs(rightId).keyWords, cursors(rightId)),
      )
      order < 0 || (order == 0 && leftId < rightId)

    private def siftUp(start: Int): Unit =
      var at = start
      var moved = true
      while (moved && at > 0) {
        val parent = (at - 1) >>> 1
        if (less(nodes(at), nodes(parent))) then
          swap(at, parent)
          at = parent
        else moved = false
      }

    private def siftDown(start: Int): Unit =
      var at = start
      var moved = true
      while (moved) {
        val left = at * 2 + 1
        val right = left + 1
        var best = at
        if (left < size && less(nodes(left), nodes(best))) then best = left
        if (right < size && less(nodes(right), nodes(best))) then best = right
        if (best == at) then moved = false
        else
          swap(at, best)
          at = best
      }

    private def swap(left: Int, right: Int): Unit =
      val held = nodes(left)
      nodes(left) = nodes(right)
      nodes(right) = held

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class Sink(val size: Int):

    private val keys = new Array[Int](size)
    private val values = new Array[Int](size)
    private val keyWords = Validity.allocate(size)
    private val valueWords = Validity.allocate(size)
    private var filled: Int = 0

    def write(run: Run, row: Int): Unit =
      require(filled < size, s"rank split overflowed a partition of $size rows")
      if (Validity.isSet(run.keyWords, row)) then
        keys(filled) = run.keys(row)
        Validity.setBit(keyWords, filled)
      if (Validity.isSet(run.valueWords, row)) then
        values(filled) = run.values(row)
        Validity.setBit(valueWords, filled)
      filled += 1

    def toBatch: ColumnBatch =
      require(filled == size, s"rank split filled $filled of $size")
      pairBatch(keys, keyWords, values, valueWords, size)
