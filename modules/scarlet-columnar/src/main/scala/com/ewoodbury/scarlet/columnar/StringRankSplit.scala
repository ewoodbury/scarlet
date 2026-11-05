package com.ewoodbury.scarlet.columnar

/**
 * In-memory rank split of `(String, Int)` partitions.
 *
 * Each input batch is one source partition, schema `[Utf8Dict, Int32]`. It is sorted by effective
 * key: a present key orders by `String.compareTo` (UTF-16 code units), and a null key orders as
 * `""`. The null payload is not read. Ties keep input order, so the total order is (effective key,
 * partition index, row index).
 *
 * The local sort is a bottom-up merge of row indexes. It gathers a new batch that keeps the source
 * dictionary and the source codes: the codes are permuted, the dictionary array is not rebuilt,
 * and a null key stays null. A run of length 0 or 1 reuses its input batch.
 *
 * The merge is a binary min-heap of run ids, one cursor per input run, not a boxed row. The heap
 * reads one snapshotted effective string per run. Equal strings break by run id, which is source
 * partition order. Each run is already tie-sorted by row, so equal keys keep global input order.
 *
 * Partition `i` starts at `ceil(i * rows / partitions)` and ends at `ceil((i + 1) * rows /
 * partitions)`, the same boundaries as [[RankBounds]]. Each output partition has its own
 * dictionary: the key text is interned once per distinct string, a null key is a clear bit, and
 * the value bit is copied. Output partition `i` is sorted. Sizes differ by at most one. The last
 * effective key of a non-empty partition is at most the first effective key of the next non-empty
 * partition. Concatenation is globally sorted.
 *
 * `rows == 0` returns `partitions` fresh empty string-pair batches and does not sort, including an
 * empty `batches` vector. `partitions <= 0` and `parallelism <= 0` are rejected. This object does
 * not spill.
 */
object StringRankSplit:

  def sortAndSplit(
      batches: Vector[ColumnBatch],
      partitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(partitions > 0, s"numPartitions must be positive, got $partitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    val rows = rowCount(batches)
    if (rows == 0) then Vector.fill(partitions)(ColumnBatches.emptyStringPairs)
    else merge(sortRuns(batches, parallelism), rows, partitions)

  private def rowCount(batches: Vector[ColumnBatch]): Int =
    val total = batches.iterator.map { batch =>
      val _ = checkedPair(batch)
      batch.length.toLong
    }.sum
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
      sortIndices(keys.dictionary, keys.codes, keys.validity.words, order)
      gather(keys, values, order)

  /** Sort `order` by effective key, then by the row index stored in `order`. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def sortIndices(
      dictionary: Array[String],
      codes: Array[Int],
      keyWords: Array[Long],
      order: Array[Int],
  ): Unit =
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
          mergeIndices(dictionary, codes, keyWords, src, dst, start, mid, end)
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
      dictionary: Array[String],
      codes: Array[Int],
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
      if (before(dictionary, codes, keyWords, src(left), src(right))) then
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
      dictionary: Array[String],
      codes: Array[Int],
      keyWords: Array[Long],
      leftRow: Int,
      rightRow: Int,
  ): Boolean =
    val order = effective(dictionary, codes, keyWords, leftRow).compareTo(
      effective(dictionary, codes, keyWords, rightRow),
    )
    order < 0 || (order == 0 && leftRow < rightRow)

  private def effective(
      dictionary: Array[String],
      codes: Array[Int],
      keyWords: Array[Long],
      row: Int,
  ): String =
    if (Validity.isSet(keyWords, row)) dictionary(codes(row)) else ""

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def gather(keys: DictUtf8Column, values: Int32Column, order: Array[Int]): ColumnBatch =
    val count = order.length
    val outCodes = new Array[Int](count)
    val outValues = new Array[Int](count)
    val keyWords = Validity.allocate(count)
    val valueWords = Validity.allocate(count)
    val srcKeyWords = keys.validity.words
    val srcValueWords = values.validity.words
    var out = 0
    while (out < count) {
      val row = order(out)
      if (Validity.isSet(srcKeyWords, row)) then
        outCodes(out) = keys.codes(row)
        Validity.setBit(keyWords, out)
      if (Validity.isSet(srcValueWords, row)) then
        outValues(out) = values.values(row)
        Validity.setBit(valueWords, out)
      out += 1
    }
    publish(keys.dictionary, outCodes, keyWords, outValues, valueWords, count)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def merge(runs: Vector[ColumnBatch], rows: Int, partitions: Int): Vector[ColumnBatch] =
    val opened: Array[Run] = runs.iterator.map(openRun).toArray
    val cursors = new Array[Int](opened.length)
    val keys = Array.fill(opened.length)("")
    val heap = new StringRunHeap(keys)
    var run = 0
    while (run < opened.length) {
      if (opened(run).length > 0) then pushRun(heap, keys, opened, cursors, run)
      run += 1
    }
    val sizes = RankBounds.outputSizes(rows, partitions)
    val sinks = Array.tabulate(partitions)(part => new Sink(sizes(part)))
    RankBounds.place(rows, partitions, () => !heap.isEmpty) { part =>
      val chosen = heap.pop()
      val cursor = cursors(chosen)
      val source = opened(chosen)
      val keyOn = Validity.isSet(source.keyWords, cursor)
      val valueOn = Validity.isSet(source.valueWords, cursor)
      sinks(part).write(
        keyOn,
        if (keyOn) source.text(cursor) else "",
        valueOn,
        if (valueOn) source.values(cursor) else 0,
      )
      cursors(chosen) = cursor + 1
      if (cursors(chosen) < source.length) then pushRun(heap, keys, opened, cursors, chosen)
    }
    Vector.tabulate(partitions) { part =>
      if (sizes(part) == 0) then ColumnBatches.emptyStringPairs
      else sinks(part).toBatch
    }

  /**
   * Snapshot the effective key, then push. The heap reads `keys(id)` and does not recompute it.
   */
  private def pushRun(
      heap: StringRunHeap,
      keys: Array[String],
      opened: Array[Run],
      cursors: Array[Int],
      id: Int,
  ): Unit =
    val source = opened(id)
    val cursor = cursors(id)
    keys(id) = effective(source.dictionary, source.codes, source.keyWords, cursor)
    heap.push(id)

  private def openRun(batch: ColumnBatch): Run =
    val (keys, values) = checkedPair(batch)
    new Run(
      keys.dictionary,
      keys.codes,
      keys.validity.words,
      values.values,
      values.validity.words,
      batch.length,
    )

  private def checkedPair(batch: ColumnBatch): (DictUtf8Column, Int32Column) =
    require(
      batch.columns.length == 2,
      s"sortAndSplit expected [Utf8Dict, Int32], got ${batch.schema.mkString("[", ", ", "]")}",
    )
    val keys = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
    val values = ColumnHash.int32At(batch, 1)
    require(
      keys.length == batch.length && values.length == batch.length,
      s"key length ${keys.length}, value length ${values.length}, and batch length ${batch.length} disagree",
    )
    (keys, values)

  private def publish(
      dictionary: Array[String],
      codes: Array[Int],
      keyWords: Array[Long],
      values: Array[Int],
      valueWords: Array[Long],
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      pairSchema,
      Vector(
        DictUtf8Column.of(dictionary, codes, Validity.fromWords(keyWords, rows), rows),
        Int32Column.of(values, Validity.fromWords(valueWords, rows), rows),
      ),
      rows,
    )

  private val pairSchema: Vector[LogicalType] =
    Vector(LogicalType.Utf8Dict, LogicalType.Int32)

  private final class Run(
      val dictionary: Array[String],
      val codes: Array[Int],
      val keyWords: Array[Long],
      val values: Array[Int],
      val valueWords: Array[Long],
      val length: Int,
  ):
    def text(row: Int): String = dictionary(codes(row))

  /** Min-heap of run ids. Order is the snapshotted effective string, then the smaller id. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class StringRunHeap(keys: Array[String]):

    private val nodes = new Array[Int](keys.length)
    private var size: Int = 0

    def isEmpty: Boolean = size == 0

    def pop(): Int =
      val chosen = nodes(0)
      size -= 1
      if (size > 0) then
        nodes(0) = nodes(size)
        siftDown(0)
      chosen

    /**
     * `keys(id)` is already the effective string. Leave it unchanged until [[pop]] returns `id`.
     */
    def push(id: Int): Unit =
      nodes(size) = id
      siftUp(size)
      size += 1

    private def less(leftId: Int, rightId: Int): Boolean =
      val order = keys(leftId).compareTo(keys(rightId))
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

  /** One output partition. A null key is not interned. A missing value is not copied. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class Sink(val size: Int):

    private val codes = new Array[Int](size)
    private val values = new Array[Int](size)
    private val keyWords = Validity.allocate(size)
    private val valueWords = Validity.allocate(size)
    private val words = DictUtf8Column.Words.empty
    private var filled: Int = 0

    def write(keyPresent: Boolean, key: String, valuePresent: Boolean, value: Int): Unit =
      require(filled < size, s"rank split overflowed a partition of $size rows")
      if (keyPresent) then
        codes(filled) = words.intern(key)
        Validity.setBit(keyWords, filled)
      if (valuePresent) then
        values(filled) = value
        Validity.setBit(valueWords, filled)
      filled += 1

    def toBatch: ColumnBatch =
      require(filled == size, s"rank split filled $filled of $size")
      publish(words.toArray, codes, keyWords, values, valueWords, size)
