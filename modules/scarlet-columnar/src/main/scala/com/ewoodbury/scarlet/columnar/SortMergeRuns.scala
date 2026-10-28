package com.ewoodbury.scarlet.columnar

import java.nio.file.Path
import java.util.Arrays
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Spills one sort-merge partition whose sorted sides do not fit in the budget.
 *
 * The trigger runs after null keys are dropped. [[retainedBytes]] charges `rows * (4 + 4 + 1)` for
 * the key, the value, and one validity byte, plus [[HeaderAllowance]] for that side's array
 * headers. Validity is stored in bits; charging a byte rounds it up. An empty side is zero. Spill
 * stays off unless [[ScarletConf.spillEnabled]] is set. A pair at or under the budget calls
 * [[SortMerge.join]] and writes nothing.
 *
 * Over the budget, each side is cut into morsels of `batchSize` present rows. Null keys are
 * dropped before the cut. Each morsel is sorted on its own and written to its own spill file, so
 * one file is one sorted run. The runs are not read back as a side.
 *
 * The merge keeps one cursor per run and one batch loaded for that cursor. A cursor reads its file
 * only when its batch is exhausted. A run file holds one morsel, so that read happens once. The
 * smaller key advances. On equality the key is drained across every run on both sides, the
 * cartesian product is written, and those cursors advance. A group whose own estimate exceeds the
 * budget throws and names the key. One store closes before the next partition pair, on success and
 * on throw.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object SortMergeRuns:

  /** Thrown when a test asks the next spilled merge to fail after it has consumed a key. */
  private[scarlet] val MergeFailed: String = "sort-merge spill failed"

  /**
   * Per-side allowance for the key array, the value array, and the validity-word array. This is
   * header and alignment slack, not another per-row charge.
   */
  private[scarlet] val HeaderAllowance: Long = 64L

  private val filesWritten = new AtomicLong
  private val mergeFault = new AtomicBoolean(false)
  private val lastDirectory = new AtomicReference("")

  /**
   * Estimated retained bytes of one sorted side with `rows` present keys. Zero rows retain
   * nothing: the in-memory sort returns before it allocates.
   */
  private[scarlet] def retainedBytes(rows: Int): Long =
    if (rows <= 0) then 0L
    else rows.toLong * (4L + 4L + 1L) + HeaderAllowance

  /**
   * True when spill is on and the two sorted sides together are strictly above the budget. Null
   * keys are not counted. A disabled spill flag is false without further work.
   */
  private[scarlet] def exceeds(left: ColumnBatch, right: ColumnBatch, conf: ScarletConf): Boolean =
    conf.spillEnabled &&
      retainedBytes(presentKeys(left)) + retainedBytes(presentKeys(right)) > conf.spillBudgetBytes

  /**
   * Merges partition `i` of `build` with partition `i` of `probe`. Over the budget, the pair is
   * spilled and the store is closed before the next index. Otherwise this is [[SortMerge.join]].
   */
  private[scarlet] def joinAll(
      build: Vector[ColumnBatch],
      probe: Vector[ColumnBatch],
      batchSize: Int,
      conf: ScarletConf,
  ): Vector[ColumnBatch] =
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    require(
      build.length == probe.length,
      s"partition counts ${build.length} and ${probe.length} differ",
    )
    Vector.tabulate(build.length) { index =>
      val left = build.lift(index).getOrElse {
        throw new IllegalArgumentException(s"missing build partition $index")
      }
      val right = probe.lift(index).getOrElse {
        throw new IllegalArgumentException(s"missing probe partition $index")
      }
      joinOne(left, right, batchSize, conf)
    }

  /** Spill files published since the last [[resetForTest]]. */
  private[scarlet] def filesWrittenForTest: Long = filesWritten.get

  /** Directory of the store most recently opened, or empty when none has been. */
  private[scarlet] def lastDirectoryForTest: String = lastDirectory.get

  /** The next spilled merge consumes one key, then throws before it emits that group. */
  private[scarlet] def armMergeFailure(): Unit =
    mergeFault.set(true)

  /** Clears the file counter, the recorded directory, and an armed merge failure. */
  private[scarlet] def resetForTest(): Unit =
    filesWritten.set(0L)
    mergeFault.set(false)
    lastDirectory.set("")

  private def joinOne(
      left: ColumnBatch,
      right: ColumnBatch,
      batchSize: Int,
      conf: ScarletConf,
  ): ColumnBatch =
    if (!conf.spillEnabled) then SortMerge.join(left, right)
    else if (exceeds(left, right, conf)) then finish(left, right, batchSize, conf)
    else SortMerge.join(left, right)

  private def finish(
      left: ColumnBatch,
      right: ColumnBatch,
      batchSize: Int,
      conf: ScarletConf,
  ): ColumnBatch =
    val directory = directoryOf(conf)
    lastDirectory.set(directory.toString)
    SpillStore.withSpill(directory, conf.spillMaxFiles, conf.spillMaxBytes) { store =>
      val leftRuns = writeRuns(store, left, batchSize)
      val rightRuns = writeRuns(store, right, batchSize)
      merge(leftRuns, rightRuns, conf.spillBudgetBytes)
    }

  /**
   * Present keys only. A null key is dropped before the morsel is cut, so a run never holds one.
   */
  private def presentKeys(batch: ColumnBatch): Int =
    val keys = ColumnHash.int32At(batch, 0)
    val words = keys.validity.words
    val rows = keys.length
    var count = 0
    var row = 0
    while (row < rows) {
      if (Validity.isSet(words, row)) then count += 1
      row += 1
    }
    count

  /**
   * Cuts present rows into morsels, sorts each morsel, and publishes it. The buffer is one morsel.
   * The side is not assembled into a second sorted copy.
   */
  private def writeRuns(store: SpillStore, batch: ColumnBatch, batchSize: Int): Vector[SpillFile] =
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    val rows = keys.length
    val bufKeys = new Array[Int](batchSize)
    val bufValues = new Array[Int](batchSize)
    val bufWords = Validity.allocate(batchSize)
    var filled = 0
    var row = 0
    var files = Vector.empty[SpillFile]
    while (row < rows) {
      if (Validity.isSet(keyWords, row)) then
        bufKeys(filled) = keys.values(row)
        if (Validity.isSet(valueWords, row)) then
          bufValues(filled) = values.values(row)
          Validity.setBit(bufWords, filled)
        else bufValues(filled) = 0
        filled += 1
        if (filled == batchSize) then
          files = files :+ publish(store, morselBatch(bufKeys, bufValues, bufWords, filled))
          Arrays.fill(bufWords, 0L)
          filled = 0
      row += 1
    }
    if (filled > 0) then
      files = files :+ publish(store, morselBatch(bufKeys, bufValues, bufWords, filled))
    files

  private def morselBatch(
      keys: Array[Int],
      values: Array[Int],
      words: Array[Long],
      filled: Int,
  ): ColumnBatch =
    val keyCopy = Arrays.copyOf(keys, filled)
    val valueCopy = Arrays.copyOf(values, filled)
    val validity = Validity.allocate(filled)
    var row = 0
    while (row < filled) {
      if (Validity.isSet(words, row)) then Validity.setBit(validity, row)
      row += 1
    }
    new ColumnBatch(
      pairSchema,
      Vector(
        Int32Column.of(keyCopy, Validity.allValid(filled), filled),
        Int32Column.of(valueCopy, Validity.fromWords(validity, filled), filled),
      ),
      filled,
    )

  private def publish(store: SpillStore, morsel: ColumnBatch): SpillFile =
    val file = store.write(Vector(SortMerge.sortedPairs(morsel)))
    val _ = filesWritten.incrementAndGet()
    file

  private def merge(
      leftFiles: Vector[SpillFile],
      rightFiles: Vector[SpillFile],
      budget: Long,
  ): ColumnBatch =
    val leftHeap = KeyHeap.open(leftFiles)
    val rightHeap = KeyHeap.open(rightFiles)
    val output = new OutputRows()
    val leftGroup = new GroupRows()
    val rightGroup = new GroupRows()
    while (!leftHeap.isEmpty && !rightHeap.isEmpty) {
      val order = Integer.compare(leftHeap.minKey, rightHeap.minKey)
      if (order < 0) then
        skip(leftHeap, leftHeap.minKey)
        failIfArmed()
      else if (order > 0) then
        skip(rightHeap, rightHeap.minKey)
        failIfArmed()
      else
        val key = leftHeap.minKey
        leftGroup.clear()
        rightGroup.clear()
        take(leftHeap, key, leftGroup)
        take(rightHeap, key, rightGroup)
        val estimate = retainedBytes(leftGroup.length) + retainedBytes(rightGroup.length)
        if (estimate > budget) then refuseGroup(key, estimate, budget)
        failIfArmed()
        output.cross(key, leftGroup, rightGroup)
    }
    output.toBatch

  /** Drops every row of `key` on this side. The key is absent from the other side. */
  private def skip(heap: KeyHeap, key: Int): Unit =
    while (!heap.isEmpty && Integer.compare(heap.minKey, key) == 0) {
      val cursor = heap.pop()
      while (cursor.ready && Integer.compare(cursor.key, key) == 0) {
        cursor.advance()
      }
      if (cursor.ready) then heap.push(cursor.id)
    }

  /**
   * Appends every row of `key` from every run. A run is sorted, so the key is a contiguous span at
   * the cursor, and a run whose head is greater stays in the heap until this key is finished.
   */
  private def take(heap: KeyHeap, key: Int, group: GroupRows): Unit =
    while (!heap.isEmpty && Integer.compare(heap.minKey, key) == 0) {
      val cursor = heap.pop()
      while (cursor.ready && Integer.compare(cursor.key, key) == 0) {
        if (cursor.valuePresent) then group.add(cursor.value, present = true)
        else group.add(0, present = false)
        cursor.advance()
      }
      if (cursor.ready) then heap.push(cursor.id)
    }

  /** An armed failure has already read one key from the runs. The group is not emitted. */
  private def failIfArmed(): Unit =
    if (mergeFault.compareAndSet(true, false)) then throw new IllegalStateException(MergeFailed)

  private def refuseGroup(key: Int, estimate: Long, budget: Long): Nothing =
    throw new IllegalStateException(
      s"sort-merge group for key $key retains about $estimate bytes, " +
        s"above the budget of $budget bytes",
    )

  private def directoryOf(conf: ScarletConf): Path =
    if (conf.spillDirectory.isEmpty) then SpillStore.defaultDirectory
    else Path.of(conf.spillDirectory)

  private def validityOf(words: Array[Long], rows: Int): Validity =
    if (rows == 0) then Validity.allValid(0)
    else
      val count = (rows + 63) >>> 6
      val copied = Arrays.copyOf(words, count)
      val tail = rows & 63
      if (tail != 0) then copied(count - 1) = copied(count - 1) & ((1L << tail) - 1L)
      Validity.fromWords(copied, rows)

  private val pairSchema: Vector[LogicalType] =
    Vector(LogicalType.Int32, LogicalType.Int32)

  private val joinSchema: Vector[LogicalType] =
    Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32)

  private def emptyJoin: ColumnBatch =
    new ColumnBatch(
      joinSchema,
      Vector(
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
      ),
      0,
    )

  /** One sorted run. The file is read when the cursor has no loaded row left. */
  private final class RunCursor(val id: Int, file: SpillFile):

    private var keys: Array[Int] = Array.emptyIntArray
    private var values: Array[Int] = Array.emptyIntArray
    private var words: Array[Long] = Array.emptyLongArray
    private var length: Int = 0
    private var index: Int = 0
    private var spent: Boolean = false

    def ready: Boolean =
      if (index < length) then true else refill()

    def key: Int =
      require(index < length, "sort-merge cursor is exhausted")
      keys(index)

    def value: Int =
      require(index < length, "sort-merge cursor is exhausted")
      values(index)

    def valuePresent: Boolean =
      require(index < length, "sort-merge cursor is exhausted")
      Validity.isSet(words, index)

    def advance(): Unit =
      require(index < length, "sort-merge cursor is exhausted")
      index += 1

    private def refill(): Boolean =
      if (index < length) then true
      else if (spent) then false
      else
        spent = true
        install(load(file))
        index < length

    private def load(spilled: SpillFile): ColumnBatch =
      spilled.read() match
        case Vector(batch) => batch
        case other =>
          throw new IllegalStateException(s"spill run holds ${other.length} batches")

    private def install(batch: ColumnBatch): Unit =
      val keyColumn = ColumnHash.int32At(batch, 0)
      val valueColumn = ColumnHash.int32At(batch, 1)
      keys = keyColumn.values
      values = valueColumn.values
      words = valueColumn.validity.words
      length = batch.length
      index = 0

  /** Min-heap of live cursors, ordered by key and then by cursor id. */
  private final class KeyHeap(cursors: Array[RunCursor]):

    private val nodes = new Array[Int](cursors.length)
    private var size: Int = 0

    def isEmpty: Boolean = size == 0

    def minKey: Int =
      require(size > 0, "sort-merge heap is empty")
      cursors(nodes(0)).key

    def pop(): RunCursor =
      val cursor = cursors(nodes(0))
      size -= 1
      if (size > 0) then
        nodes(0) = nodes(size)
        siftDown(0)
      cursor

    def push(id: Int): Unit =
      nodes(size) = id
      siftUp(size)
      size += 1

    private def less(leftId: Int, rightId: Int): Boolean =
      val order = Integer.compare(cursors(leftId).key, cursors(rightId).key)
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

  private object KeyHeap:

    def open(files: Vector[SpillFile]): KeyHeap =
      val cursors = files.iterator.zipWithIndex.map { (file, id) =>
        new RunCursor(id, file)
      }.toArray
      val heap = new KeyHeap(cursors)
      cursors.indices.foreach { id =>
        if (cursors(id).ready) then heap.push(id)
      }
      heap

  /** Rows of one equal-key group. The buffer is cleared before the next key. */
  private final class GroupRows:

    private var values: Array[Int] = new Array[Int](8)
    private var words: Array[Long] = Validity.allocate(8)
    private var count: Int = 0

    def length: Int = count

    def clear(): Unit =
      val used = (count + 63) >>> 6
      if (used > 0) then Arrays.fill(words, 0, used, 0L)
      count = 0

    def add(value: Int, present: Boolean): Unit =
      if (count == values.length) then grow()
      if (present) then
        values(count) = value
        Validity.setBit(words, count)
      else values(count) = 0
      count += 1

    def valueAt(row: Int): Int = values(row)

    def presentAt(row: Int): Boolean = Validity.isSet(words, row)

    private def grow(): Unit =
      val next = count * 2
      val copiedValues = Arrays.copyOf(values, next)
      val copiedWords = Validity.allocate(next)
      var row = 0
      while (row < count) {
        if (Validity.isSet(words, row)) then Validity.setBit(copiedWords, row)
        row += 1
      }
      values = copiedValues
      words = copiedWords

  /** Join rows `(key, left, right)` for one partition pair. */
  private final class OutputRows:

    private var keys: Array[Int] = new Array[Int](8)
    private var leftValues: Array[Int] = new Array[Int](8)
    private var rightValues: Array[Int] = new Array[Int](8)
    private var leftWords: Array[Long] = Validity.allocate(8)
    private var rightWords: Array[Long] = Validity.allocate(8)
    private var count: Int = 0

    def cross(key: Int, left: GroupRows, right: GroupRows): Unit =
      val product = left.length.toLong * right.length.toLong
      val next = count.toLong + product
      if (next > Int.MaxValue.toLong) then
        throw new IllegalStateException(s"sort-merge output reached $next rows")
      ensure(next.toInt)
      var leftRow = 0
      while (leftRow < left.length) {
        var rightRow = 0
        while (rightRow < right.length) {
          keys(count) = key
          if (left.presentAt(leftRow)) then
            leftValues(count) = left.valueAt(leftRow)
            Validity.setBit(leftWords, count)
          else leftValues(count) = 0
          if (right.presentAt(rightRow)) then
            rightValues(count) = right.valueAt(rightRow)
            Validity.setBit(rightWords, count)
          else rightValues(count) = 0
          count += 1
          rightRow += 1
        }
        leftRow += 1
      }

    def toBatch: ColumnBatch =
      if (count == 0) then emptyJoin
      else
        new ColumnBatch(
          joinSchema,
          Vector(
            Int32Column.of(Arrays.copyOf(keys, count), Validity.allValid(count), count),
            Int32Column.of(Arrays.copyOf(leftValues, count), validityOf(leftWords, count), count),
            Int32Column.of(
              Arrays.copyOf(rightValues, count),
              validityOf(rightWords, count),
              count,
            ),
          ),
          count,
        )

    private def ensure(needed: Int): Unit =
      val cap = keys.length
      if (needed > cap) then
        var next = math.max(cap, 1)
        while (next < needed) {
          val grown = next.toLong << 1
          next = if (grown >= Int.MaxValue.toLong) Int.MaxValue else grown.toInt
        }
        keys = Arrays.copyOf(keys, next)
        leftValues = Arrays.copyOf(leftValues, next)
        rightValues = Arrays.copyOf(rightValues, next)
        leftWords = growWords(leftWords, count, next)
        rightWords = growWords(rightWords, count, next)

    private def growWords(words: Array[Long], live: Int, capacity: Int): Array[Long] =
      val copied = Validity.allocate(capacity)
      var row = 0
      while (row < live) {
        if (Validity.isSet(words, row)) then Validity.setBit(copied, row)
        row += 1
      }
      copied
