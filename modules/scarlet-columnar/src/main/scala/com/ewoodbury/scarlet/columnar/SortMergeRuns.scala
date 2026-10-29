package com.ewoodbury.scarlet.columnar

import java.nio.file.Path
import java.util.Arrays
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

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
 * one file is one sorted run.
 *
 * Each side is then reduced to one run. A pass merges at most [[ScarletConf.spillFanIn]] runs (the
 * default is 8) into new sorted run files, reading and writing one batch at a time. While a side
 * has more than one run, the next pass repeats. The join merges the two remaining streams. On
 * equality the key is drained on both streams before the cartesian product is written. A group
 * whose own estimate exceeds the budget throws and names the key, before any row of that group is
 * emitted. One store closes before the next partition pair, on success and on throw, and that
 * close deletes every run, including the intermediate ones.
 *
 * A pass holds at most `fanIn` input batches per side plus the one output batch it is writing, and
 * the final join holds two streams plus the equal-key group until it is emitted or refused. That
 * bounds the merge machinery, not the operator: the exchange has already materialised both sides
 * and the joined result is materialised too, so peak query memory is both inputs plus the result
 * plus those `fanIn` batches. Spill keeps the merge from adding the whole side on top.
 *
 * An outer join uses the same fan-in. After it, one run remains per side, so the cursor walk sees
 * an unmatched group without a cross-run bitmap. Preserved null-key rows are split out before any
 * run is written and appended once, after that walk. They are not written per run or per pass. A
 * group whose own estimate exceeds the budget still throws and names the key. With spill off, or
 * under the budget, the outer path calls [[SortMerge]] and writes nothing.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object SortMergeRuns:

  /** Thrown when a test asks the next spilled merge to fail after it has consumed a key. */
  private[scarlet] val MergeFailed: String = "sort-merge spill failed"

  /**
   * Which side an outer sort-merge keeps. [[OuterKind.Right]] runs the left walk on swapped inputs
   * and swaps the value columns back.
   */
  private[scarlet] enum OuterKind:
    case Left, Right, Full

  /**
   * Per-side allowance for the key array, the value array, and the validity-word array. This is
   * header and alignment slack, not another per-row charge.
   */
  private[scarlet] val HeaderAllowance: Long = 64L

  private val filesWritten = new AtomicLong
  private val mergePasses = new AtomicLong
  private val mergeFault = new AtomicBoolean(false)
  private val failAtPass = new AtomicInteger(0)
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

  /**
   * Outer merge of partition `i` with partition `i`. Spill stays off unless
   * [[ScarletConf.spillEnabled]] is set. A pair at or under the budget calls [[SortMerge]] and
   * writes nothing. Over the budget, preserved null keys are removed first and emitted once after
   * the two reduced runs are joined.
   */
  private[scarlet] def joinOuterAll(
      left: Vector[ColumnBatch],
      right: Vector[ColumnBatch],
      batchSize: Int,
      conf: ScarletConf,
      kind: OuterKind,
  ): Vector[ColumnBatch] =
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    require(
      left.length == right.length,
      s"partition counts ${left.length} and ${right.length} differ",
    )
    left.zip(right).map { (leftBatch, rightBatch) =>
      joinOuterOne(leftBatch, rightBatch, batchSize, conf, kind)
    }

  /** Spill files published since the last [[resetForTest]], including intermediate runs. */
  private[scarlet] def filesWrittenForTest: Long = filesWritten.get

  /** Completed merge passes since the last [[resetForTest]]. Left side passes count first. */
  private[scarlet] def mergePassesForTest: Long = mergePasses.get

  /** Directory of the store most recently opened, or empty when none has been. */
  private[scarlet] def lastDirectoryForTest: String = lastDirectory.get

  /** The next spilled merge consumes one key, then throws before it emits that group. */
  private[scarlet] def armMergeFailure(): Unit =
    mergeFault.set(true)

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
    mergeFault.set(false)
    failAtPass.set(0)
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

  private def joinOuterOne(
      left: ColumnBatch,
      right: ColumnBatch,
      batchSize: Int,
      conf: ScarletConf,
      kind: OuterKind,
  ): ColumnBatch =
    if (!conf.spillEnabled) then inMemoryOuter(left, right, kind)
    else if (exceeds(left, right, conf)) then finishOuter(left, right, batchSize, conf, kind)
    else inMemoryOuter(left, right, kind)

  private def inMemoryOuter(left: ColumnBatch, right: ColumnBatch, kind: OuterKind): ColumnBatch =
    kind match
      case OuterKind.Left => SortMerge.leftOuter(left, right)
      case OuterKind.Right => SortMerge.rightOuter(left, right)
      case OuterKind.Full => SortMerge.fullOuter(left, right)

  /**
   * [[OuterKind.Right]] spills the left-outer walk with the inputs swapped, then swaps the value
   * columns back. Null keys of the preserved side are split before the runs are written.
   */
  private def finishOuter(
      left: ColumnBatch,
      right: ColumnBatch,
      batchSize: Int,
      conf: ScarletConf,
      kind: OuterKind,
  ): ColumnBatch =
    kind match
      case OuterKind.Right =>
        SortMerge.swapValues(spillOuter(right, left, batchSize, conf, preserveRight = false))
      case OuterKind.Left =>
        spillOuter(left, right, batchSize, conf, preserveRight = false)
      case OuterKind.Full =>
        spillOuter(left, right, batchSize, conf, preserveRight = true)

  private def spillOuter(
      left: ColumnBatch,
      right: ColumnBatch,
      batchSize: Int,
      conf: ScarletConf,
      preserveRight: Boolean,
  ): ColumnBatch =
    val directory = directoryOf(conf)
    lastDirectory.set(directory.toString)
    val fanIn = conf.spillFanIn
    require(fanIn >= 2, s"spillFanIn must be >= 2, got $fanIn")
    SpillStore.withSpill(directory, conf.spillMaxFiles, conf.spillMaxBytes) { store =>
      val (leftPresent, leftNulls) = splitNullKeys(left)
      val (rightPresent, rightNulls) =
        if (preserveRight) then splitNullKeys(right) else (right, emptyPairs)
      val leftRuns = writeRuns(store, leftPresent, batchSize)
      val rightRuns = writeRuns(store, rightPresent, batchSize)
      val leftFile = reduce(store, leftRuns, fanIn, batchSize)
      val rightFile = reduce(store, rightRuns, fanIn, batchSize)
      val merged = joinStreams(
        leftFile,
        rightFile,
        conf.spillBudgetBytes,
        preserveLeft = true,
        preserveRight = preserveRight,
      )
      withNullKeys(merged, leftNulls, rightNulls)
    }

  private def finish(
      left: ColumnBatch,
      right: ColumnBatch,
      batchSize: Int,
      conf: ScarletConf,
  ): ColumnBatch =
    val directory = directoryOf(conf)
    lastDirectory.set(directory.toString)
    val fanIn = conf.spillFanIn
    require(fanIn >= 2, s"spillFanIn must be >= 2, got $fanIn")
    SpillStore.withSpill(directory, conf.spillMaxFiles, conf.spillMaxBytes) { store =>
      val leftRuns = writeRuns(store, left, batchSize)
      val rightRuns = writeRuns(store, right, batchSize)
      val leftFile = reduce(store, leftRuns, fanIn, batchSize)
      val rightFile = reduce(store, rightRuns, fanIn, batchSize)
      joinStreams(leftFile, rightFile, conf.spillBudgetBytes)
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

  /**
   * Merges `runs` until one file remains. A single run is already that file. An empty side stays
   * empty. Each completed pass counts for [[mergePassesForTest]].
   */
  private def reduce(
      store: SpillStore,
      runs: Vector[SpillFile],
      fanIn: Int,
      batchSize: Int,
  ): Option[SpillFile] =
    if (runs.isEmpty) then None
    else
      var current = runs
      while (current.length > 1) {
        val pass = mergePasses.get + 1L
        val merged = mergePass(store, current, fanIn, batchSize, pass)
        require(
          merged.length < current.length,
          s"sort-merge pass made no progress from ${current.length} runs",
        )
        current = merged
        val _ = mergePasses.incrementAndGet()
      }
      current match
        case Vector(file) => Some(file)
        case other =>
          throw new IllegalStateException(s"sort-merge reduced to ${other.length} runs")

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

  /** K-way merge of at most `fanIn` sorted runs. The output file is one sorted run. */
  private def mergeGroup(
      store: SpillStore,
      files: Vector[SpillFile],
      batchSize: Int,
      pass: Long,
  ): SpillFile =
    val cursors = openCursors(files)
    try
      val heap = openHeap(cursors)
      val merged = store.writeStreaming(pairSchema) { sink =>
        failIfPass(pass)
        writeSorted(heap, sink, batchSize)
      }
      val _ = filesWritten.incrementAndGet()
      merged
    finally
      var id = 0
      while (id < cursors.length) {
        cursors(id).close()
        id += 1
      }

  private def writeSorted(heap: KeyHeap, sink: SpillBatchWriter, batchSize: Int): Unit =
    val bufKeys = new Array[Int](batchSize)
    val bufValues = new Array[Int](batchSize)
    val bufWords = Validity.allocate(batchSize)
    var filled = 0
    while (!heap.isEmpty) {
      val cursor = heap.pop()
      bufKeys(filled) = cursor.key
      if (cursor.valuePresent) then
        bufValues(filled) = cursor.value
        Validity.setBit(bufWords, filled)
      else bufValues(filled) = 0
      filled += 1
      cursor.advance()
      if (cursor.ready) then heap.push(cursor.id)
      if (filled == batchSize) then
        sink.write(morselBatch(bufKeys, bufValues, bufWords, filled))
        clearWords(bufWords, filled)
        filled = 0
    }
    if (filled > 0) then sink.write(morselBatch(bufKeys, bufValues, bufWords, filled))

  private def clearWords(words: Array[Long], filled: Int): Unit =
    val used = (filled + 63) >>> 6
    if (used > 0) then Arrays.fill(words, 0, used, 0L)

  private def joinStreams(
      leftFile: Option[SpillFile],
      rightFile: Option[SpillFile],
      budget: Long,
      preserveLeft: Boolean = false,
      preserveRight: Boolean = false,
  ): ColumnBatch =
    (leftFile, rightFile) match
      case (Some(left), Some(right)) =>
        val leftCursor = new RunCursor(0, left)
        val rightCursor =
          try new RunCursor(1, right)
          catch
            case thrown: Throwable =>
              leftCursor.close()
              throw thrown
        try joinCursors(leftCursor, rightCursor, budget, preserveLeft, preserveRight)
        finally
          leftCursor.close()
          rightCursor.close()
      case (Some(left), None) =>
        if (preserveLeft) then emitFile(left, onLeft = true, budget) else emptyJoin
      case (None, Some(right)) =>
        if (preserveRight) then emitFile(right, onLeft = false, budget) else emptyJoin
      case (None, None) => emptyJoin

  /**
   * One sorted run, emitted as unmatched rows. A group over the budget names its key and throws.
   */
  private def emitFile(file: SpillFile, onLeft: Boolean, budget: Long): ColumnBatch =
    val cursor = new RunCursor(0, file)
    try
      val output = new OutputRows()
      val group = new GroupRows()
      while (cursor.ready) {
        emitUnmatched(cursor, group, output, budget, onLeft)
      }
      output.toBatch
    finally cursor.close()

  /** Stage-1 merge of two sorted streams. A key is drained on both sides before it is emitted. */
  private def joinCursors(
      left: RunCursor,
      right: RunCursor,
      budget: Long,
      preserveLeft: Boolean,
      preserveRight: Boolean,
  ): ColumnBatch =
    val output = new OutputRows()
    val leftGroup = new GroupRows()
    val rightGroup = new GroupRows()
    while (left.ready && right.ready) {
      val order = Integer.compare(left.key, right.key)
      if (order < 0) then
        if (preserveLeft) then emitUnmatched(left, leftGroup, output, budget, onLeft = true)
        else
          skipCursor(left, left.key)
          failIfArmed()
      else if (order > 0) then
        if (preserveRight) then emitUnmatched(right, rightGroup, output, budget, onLeft = false)
        else
          skipCursor(right, right.key)
          failIfArmed()
      else
        val key = left.key
        leftGroup.clear()
        rightGroup.clear()
        takeCursor(left, key, leftGroup)
        takeCursor(right, key, rightGroup)
        val estimate = retainedBytes(leftGroup.length) + retainedBytes(rightGroup.length)
        if (estimate > budget) then refuseGroup(key, estimate, budget)
        failIfArmed()
        output.cross(key, leftGroup, rightGroup)
    }
    if (preserveLeft) then
      while (left.ready) {
        emitUnmatched(left, leftGroup, output, budget, onLeft = true)
      }
    if (preserveRight) then
      while (right.ready) {
        emitUnmatched(right, rightGroup, output, budget, onLeft = false)
      }
    output.toBatch

  /**
   * Drains `key` into `group` and appends it unmatched. The armed failure runs after the key is
   * consumed and before any row of it is emitted. A group over the budget names that key.
   */
  private def emitUnmatched(
      cursor: RunCursor,
      group: GroupRows,
      output: OutputRows,
      budget: Long,
      onLeft: Boolean,
  ): Unit =
    val key = cursor.key
    group.clear()
    takeCursor(cursor, key, group)
    val estimate = retainedBytes(group.length)
    if (estimate > budget) then refuseGroup(key, estimate, budget)
    failIfArmed()
    output.preserve(key, group, onLeft)

  /** Drops every row of `key` on this stream. The key is absent from the other side. */
  private def skipCursor(cursor: RunCursor, key: Int): Unit =
    while (cursor.ready && Integer.compare(cursor.key, key) == 0) {
      cursor.advance()
    }

  /** Appends every row of `key`. The stream is sorted, so the key is one contiguous span. */
  private def takeCursor(cursor: RunCursor, key: Int, group: GroupRows): Unit =
    while (cursor.ready && Integer.compare(cursor.key, key) == 0) {
      if (cursor.valuePresent) then group.add(cursor.value, present = true)
      else group.add(0, present = false)
      cursor.advance()
    }

  private def openCursors(files: Vector[SpillFile]): Array[RunCursor] =
    val opened = files.iterator.zipWithIndex.foldLeft(Vector.empty[RunCursor]) {
      (acc, openedFile) =>
        val (file, id) = openedFile
        try acc :+ new RunCursor(id, file)
        catch
          case thrown: Throwable =>
            acc.foreach(_.close())
            throw thrown
    }
    opened.toArray

  /** An armed failure has already read one key from the runs. The group is not emitted. */
  private def failIfArmed(): Unit =
    if (mergeFault.compareAndSet(true, false)) then throw new IllegalStateException(MergeFailed)

  /** An armed pass failure runs after the temp file exists and before that run is published. */
  private def failIfPass(pass: Long): Unit =
    val armed = failAtPass.get
    if (armed > 0 && armed.toLong == pass) then
      failAtPass.set(0)
      throw new IllegalStateException(MergeFailed)

  private def refuseGroup(key: Int, estimate: Long, budget: Long): Nothing =
    throw new IllegalStateException(
      s"sort-merge group for key $key retains about $estimate bytes, " +
        s"above the budget of $budget bytes",
    )

  private def directoryOf(conf: ScarletConf): Path =
    if (conf.spillDirectory.isEmpty) then SpillStore.defaultDirectory
    else Path.of(conf.spillDirectory)

  /**
   * Present-key rows, then null-key rows. `None` of the null side is an empty pair batch, and the
   * present side is `batch` itself when every key is present. Null keys are not written as runs.
   */
  private def splitNullKeys(batch: ColumnBatch): (ColumnBatch, ColumnBatch) =
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    val width = keys.length
    var nulls = 0
    var row = 0
    while (row < width) {
      if (!Validity.isSet(keyWords, row)) then nulls += 1
      row += 1
    }
    if (nulls == 0) then (batch, emptyPairs)
    else
      val present = width - nulls
      val presentKeys = new Array[Int](present)
      val presentValues = new Array[Int](present)
      val presentWords = Validity.allocate(present)
      val nullValues = new Array[Int](nulls)
      val nullWords = Validity.allocate(nulls)
      var presentAt = 0
      var nullAt = 0
      row = 0
      while (row < width) {
        if (Validity.isSet(keyWords, row)) then
          presentKeys(presentAt) = keys.values(row)
          if (Validity.isSet(valueWords, row)) then
            presentValues(presentAt) = values.values(row)
            Validity.setBit(presentWords, presentAt)
          presentAt += 1
        else
          if (Validity.isSet(valueWords, row)) then
            nullValues(nullAt) = values.values(row)
            Validity.setBit(nullWords, nullAt)
          nullAt += 1
        row += 1
      }
      val presentBatch = new ColumnBatch(
        pairSchema,
        Vector(
          Int32Column.of(presentKeys, Validity.allValid(present), present),
          Int32Column.of(presentValues, Validity.fromWords(presentWords, present), present),
        ),
        present,
      )
      val nullBatch = new ColumnBatch(
        pairSchema,
        Vector(
          Int32Column.of(
            new Array[Int](nulls),
            Validity.fromWords(Validity.allocate(nulls), nulls),
            nulls,
          ),
          Int32Column.of(nullValues, Validity.fromWords(nullWords, nulls), nulls),
        ),
        nulls,
      )
      (presentBatch, nullBatch)

  /** One outer row per null-key pair. The key bit stays clear. The other value is absent. */
  private def nullSideRows(nullPairs: ColumnBatch, onLeft: Boolean): ColumnBatch =
    if (nullPairs.length == 0) then emptyJoin
    else
      val values = ColumnHash.int32At(nullPairs, 1)
      val count = values.length
      val keys = new Array[Int](count)
      val leftValues = new Array[Int](count)
      val rightValues = new Array[Int](count)
      val keyWords = Validity.allocate(count)
      val leftWords = Validity.allocate(count)
      val rightWords = Validity.allocate(count)
      val valueWords = values.validity.words
      var row = 0
      while (row < count) {
        if (Validity.isSet(valueWords, row)) then
          if (onLeft) then
            leftValues(row) = values.values(row)
            Validity.setBit(leftWords, row)
          else
            rightValues(row) = values.values(row)
            Validity.setBit(rightWords, row)
        row += 1
      }
      new ColumnBatch(
        joinSchema,
        Vector(
          Int32Column.of(keys, Validity.fromWords(keyWords, count), count),
          Int32Column.of(leftValues, Validity.fromWords(leftWords, count), count),
          Int32Column.of(rightValues, Validity.fromWords(rightWords, count), count),
        ),
        count,
      )

  /** Appends preserved null-key rows after the cursor walk. An empty piece is left out. */
  private def withNullKeys(
      merged: ColumnBatch,
      leftNulls: ColumnBatch,
      rightNulls: ColumnBatch,
  ): ColumnBatch =
    val extras =
      Vector(nullSideRows(leftNulls, onLeft = true), nullSideRows(rightNulls, onLeft = false))
        .filter(_.length > 0)
    if (extras.isEmpty) then merged
    else
      val pieces = if (merged.length > 0) then merged +: extras else extras
      pieces match
        case Vector(only) => only
        case many => ColumnBatches.concat(many)

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

  private def emptyPairs: ColumnBatch =
    new ColumnBatch(
      pairSchema,
      Vector(
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
        Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0),
      ),
      0,
    )

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

  /**
   * One sorted run, read one batch at a time. [[close]] releases the channel if the caller stops
   * before the run is exhausted.
   */
  private final class RunCursor(val id: Int, file: SpillFile):

    private val source: Iterator[ColumnBatch] & AutoCloseable = file.batches()
    private var keys: Array[Int] = Array.emptyIntArray
    private var values: Array[Int] = Array.emptyIntArray
    private var words: Array[Long] = Array.emptyLongArray
    private var length: Int = 0
    private var index: Int = 0

    def close(): Unit = source.close()

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

  /** Pushes each cursor that has a row. Callers open at most `fanIn` cursors. */
  private def openHeap(cursors: Array[RunCursor]): KeyHeap =
    val heap = new KeyHeap(cursors)
    var id = 0
    while (id < cursors.length) {
      if (cursors(id).ready) then heap.push(id)
      id += 1
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

    /** One preserved row per group row. The other value's bit stays clear. The key is present. */
    def preserve(key: Int, rows: GroupRows, onLeft: Boolean): Unit =
      val next = count.toLong + rows.length.toLong
      if (next > Int.MaxValue.toLong) then
        throw new IllegalStateException(s"sort-merge output reached $next rows")
      ensure(next.toInt)
      var row = 0
      while (row < rows.length) {
        keys(count) = key
        if (onLeft) then
          if (rows.presentAt(row)) then
            leftValues(count) = rows.valueAt(row)
            Validity.setBit(leftWords, count)
          else leftValues(count) = 0
          rightValues(count) = 0
        else
          leftValues(count) = 0
          if (rows.presentAt(row)) then
            rightValues(count) = rows.valueAt(row)
            Validity.setBit(rightWords, count)
          else rightValues(count) = 0
        count += 1
        row += 1
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
