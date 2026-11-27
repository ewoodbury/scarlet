package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.annotation.tailrec

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * Counts int keys through a filter stack without building a survivor index.
 *
 * The row walk matches [[Selection.count]]. Steps run in order and a failure stops the row. A null
 * int slot fails its step and does not call the predicate. When every ordinal the steps read is
 * present on the morsel, the bit test is skipped. A morsel whose present keys fit in a small span
 * counts with [[DirectCount]], one dense slot per key in first-seen order. Every other morsel
 * inserts with [[ColumnHash.mix]] and the same linear probe as [[HashCount]]. A null key is
 * dropped even when no step reads the key. A null value still counts when no step reads the value.
 * The stored count is an `Int` and wraps on overflow.
 *
 * Each worker takes one contiguous stripe of the morsels. Every morsel of one source batch in that
 * stripe folds into a single table and compacts once, so a repeated key is updated instead of
 * inserted again. The table starts at the longest morsel, not the stripe's row count, and grows
 * only when a new key passes the load limit. A segment whose first keys fit in a small span uses
 * [[DirectCount]], one dense slot per key in that prefix. A later key outside the prefix uses the
 * hash table. Partials whose combined length is at most [[CallerRows]] merge, partition, and
 * combine on the caller. The pool dispatch of a few hundred keys is larger than the combine, and
 * the result order is the same index order the scheduler returns.
 *
 * [[count]] returns `None` before any table when `steps` is empty or is not a stack of
 * [[Selection.Step.Int32]] slot atoms. Spill off also returns `None` when the source-row estimate
 * does not fit the budget. Spill on charges one morsel, so a long stripe of repeated keys stays
 * here. A one-row table that does not fit still charges the whole input. A grow or a merged
 * partial that does not fit throws [[ClosedKernelRefusal]] before that array is allocated.
 * [[countOwned]] is that scan over owned int arrays: the columns alias those arrays for the hash
 * and are not returned. [[countUnmapped]] is the encoded scan after one key-preserving `mapValues`
 * was skipped. An empty stack there counts every present key and still does not read the value
 * column. Survivor rows are not known until the scan, so that estimate is an upper bound on the
 * charge [[HashCount.byKeySelected]] uses. The caller keeps the index path, which still refuses or
 * spills from the survivor count. A folded partial that does fit is exchanged like [[HashCount]],
 * and an over-budget post-exchange bucket still finishes in [[HashCountSpill]].
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object FoldedCount:

  private val passes = new AtomicInteger
  private val insertNs = new AtomicLong
  private val mergeNs = new AtomicLong
  private val exchangeNs = new AtomicLong
  private val encodeNs = new AtomicLong
  private val stripeWorkers = new AtomicInteger
  private val stripePartials = new AtomicInteger
  private val denseSegments = new AtomicInteger

  /** Clears the wall-clock phase counters from the last [[count]] call. */
  private[scarlet] def resetPhaseNanos(): Unit =
    insertNs.set(0L)
    mergeNs.set(0L)
    exchangeNs.set(0L)
    encodeNs.set(0L)
    stripeWorkers.set(0)
    stripePartials.set(0)
    denseSegments.set(0)

  /** Wall time of the last partial fold, in nanoseconds. One table per stripe segment. */
  private[scarlet] def insertNanos: Long = insertNs.get

  /** Wall time of the last cross-worker partial merge, in nanoseconds. */
  private[scarlet] def mergeNanos: Long = mergeNs.get

  /** Wall time of the last key exchange, in nanoseconds. */
  private[scarlet] def exchangeNanos: Long = exchangeNs.get

  /**
   * Wall time of the last post-exchange combine. That combine compacts the survivor keys.
   */
  private[scarlet] def encodeNanos: Long = encodeNs.get

  /** Worker stripes of the last [[count]] call. */
  private[scarlet] def stripesForTest: Int = stripeWorkers.get

  /** Segment partials the last [[count]] call merged. */
  private[scarlet] def partialsForTest: Int = stripePartials.get

  /** Segments that counted with [[DirectCount]] on the last [[count]] call. */
  private[scarlet] def denseSegmentsForTest: Int = denseSegments.get

  /**
   * True when every step is an int key or value slot. Empty, whole-row, and string steps are not.
   */
  def admits(steps: Seq[Selection.Step]): Boolean =
    steps.nonEmpty && slotSteps(steps)

  /**
   * True when `steps` can run on the source key column after a key-preserving `mapValues`. Empty
   * is allowed and counts every present key. A whole-row or string step is not.
   */
  def admitsUnmapped(steps: Seq[Selection.Step]): Boolean =
    slotSteps(steps)

  /**
   * True when a batch key is missing, is not an int, or has a null inside its live prefix. Spare
   * bits past `length` are ignored, matching the sort fallback in the index path.
   */
  def absentIntKey(batches: Vector[ColumnBatch]): Boolean =
    batches.exists { batch =>
      batch.columns.headOption match
        case Some(keys: Int32Column) => !liveKeys(keys)
        case _ => true
    }

  /** Accepted [[count]] and [[countUnmapped]] calls since the last [[resetPassesForTest]]. */
  def passesForTest: Int = passes.get

  /** Clears [[passesForTest]]. */
  def resetPassesForTest(): Unit = passes.set(0)

  /**
   * Bytes [[HashCount]] charges for `rows` keys. The budget tests set the spill limit from this.
   */
  def chargedBytes(rows: Long): Long = HashCount.inputRowBytes(rows)

  /**
   * Partial-count `batches` by applying `steps` inside each morsel, then exchange the partials.
   * `None` means the caller should use [[HashCount.byKey]] or [[HashCount.byKeySelected]]. No id
   * array is allocated on the `Some` path.
   */
  def count(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (!admits(steps)) then None
    else counted(batches, steps, numPartitions, parallelism, batchSize)

  /**
   * [[count]] for a source whose value function was not applied. `steps` may be empty. A non-empty
   * stack must be int slots. The key column is the source key column.
   */
  def countUnmapped(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (!admitsUnmapped(steps)) then None
    else counted(batches, steps, numPartitions, parallelism, batchSize)

  /**
   * [[count]] for arrays that are already fully present.
   *
   * The input columns retain the arrays. They exist only for the scan. The compact publishes new
   * key and count arrays, so a later action can still read the source. `None` has the same meaning
   * as [[count]].
   */
  def countOwned(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (!admits(steps)) then None
    else counted(presentParts(parts), steps, numPartitions, parallelism, batchSize)

  /**
   * [[countOwned]] after an int key function. The steps still see the source key and value.
   * `mapKey` runs once on each present survivor and is not inspected. A null key is not passed in.
   * `None` has the same meaning as [[count]].
   */
  def countOwnedMapped(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      mapKey: Int => Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (!admits(steps)) then None
    else countedMapped(presentParts(parts), steps, mapKey, numPartitions, parallelism, batchSize)

  /**
   * [[count]] after an int key function. See [[countOwnedMapped]]. Encoded batches keep their
   * nulls: a missing key is dropped and `mapKey` is not called.
   */
  def countMapped(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      mapKey: Int => Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (!admits(steps)) then None
    else countedMapped(batches, steps, mapKey, numPartitions, parallelism, batchSize)

  /**
   * [[countUnmapped]] for fully present arrays. `steps` may be empty. The input columns alias the
   * arrays and are not returned. See [[countOwned]].
   */
  def countOwnedUnmapped(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    requirePositive(numPartitions, parallelism, batchSize)
    if (!admitsUnmapped(steps)) then None
    else counted(presentParts(parts), steps, numPartitions, parallelism, batchSize)

  private def counted(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    if (batches.isEmpty) then
      notePass()
      Some(Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs))
    else
      batches.foreach(requireKey)
      if (!withinBudget(batches, batchSize)) then None
      else
        notePass()
        Some(execute(batches, steps, numPartitions, parallelism, batchSize))

  /** [[counted]] with [[MappedKeyFold]]. The dense source-key counter does not apply. */
  private[columnar] def countedMapped(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      mapKey: Int => Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      above: Option[Selection.Step] = None,
  ): Option[Vector[ColumnBatch]] =
    if (batches.isEmpty) then
      notePass()
      Some(Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs))
    else
      batches.foreach(requireKey)
      if (!withinBudget(batches, batchSize)) then None
      else
        notePass()
        Some(
          execute(
            batches,
            steps,
            numPartitions,
            parallelism,
            batchSize,
            mapKey,
            mapping = true,
            above,
          ),
        )

  /** Int key or value slots, including none. A whole-row or string step is false. */
  private def slotSteps(steps: Seq[Selection.Step]): Boolean =
    steps.forall {
      case Selection.Step.Int32(column, _) => column == 0 || column == 1
      case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
          Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
          Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
        false
    }

  private def execute(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    execute(batches, steps, numPartitions, parallelism, batchSize, unchangedKey, mapping = false)

  /**
   * `mapping` is false for a source-key count. [[DirectCount]] indexes that key, so a mapped count
   * skips it and applies `mapKey` inside the hash scan instead. `unchangedKey` is not called.
   */
  private def execute(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      mapKey: Int => Int,
      mapping: Boolean,
      above: Option[Selection.Step] = None,
  ): Vector[ColumnBatch] =
    try
      val (counts, morsels) = pieces(batches, batchSize)
      if (morsels.isEmpty) then
        recordFold(0L, 0L, 0, 0, 0)
        recordExchange(0L, 0L)
        Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
      else
        val width = math.min(parallelism, morsels.length)
        denseSegments.set(0)
        val insertStarted = System.nanoTime()
        val partials = MorselScheduler
          .map(stripes(morsels, width), width) { (_, pieces) =>
            foldStripe(pieces, steps, mapKey, mapping, above)
          }
          .flatten
        val mergeStarted = System.nanoTime()
        val groups = overlapCounts(counts, width)
        val combined =
          if (shortEnough(partials)) then
            PartialCombine.mergeOnCaller(groups, partials, HashCount.addCounts)
          else PartialCombine.mergeMorsels(groups, partials, parallelism, HashCount.addCounts)
        val merged = System.nanoTime()
        recordFold(
          mergeStarted - insertStarted,
          merged - mergeStarted,
          width,
          partials.length,
          denseSegments.get,
        )
        finish(combined, numPartitions, parallelism, batchSize)
    finally
      releaseWorkers()
      HashCount.releaseWorkers()

  private final case class Piece(morsel: Morsel, batchId: Int)

  /** Morsel counts line up with [[PartialCombine.mergeMorsels]]. */
  private def pieces(
      batches: Vector[ColumnBatch],
      batchSize: Int,
  ): (Vector[Int], Vector[Piece]) =
    val grouped = batches.zipWithIndex.map { (batch, batchId) =>
      Morsels.of(batch, batchSize).map(morsel => Piece(morsel, batchId))
    }
    (grouped.map(_.length), grouped.flatten)

  /**
   * Contiguous morsel groups. Earlier workers take the extra row when the count does not divide.
   * Flatten stays in batch order because each stripe is one slice of that order.
   */
  private def stripes(morsels: Vector[Piece], width: Int): Vector[Vector[Piece]] =
    val count = morsels.length
    val base = count / width
    val extra = count % width
    Vector.tabulate(width) { worker =>
      val start = worker * base + math.min(worker, extra)
      val length = base + (if (worker < extra) then 1 else 0)
      morsels.slice(start, start + length)
    }

  /** One compact per contiguous run of a source batch. Runs stay in batch order. */
  private def foldStripe(
      pieces: Vector[Piece],
      steps: List[Selection.Step],
      mapKey: Int => Int,
      mapping: Boolean,
      above: Option[Selection.Step],
  ): Vector[ColumnBatch] =
    val rows = pieces.toArray
    val total = rows.length
    var index = 0
    var acc = Vector.empty[ColumnBatch]
    while (index < total) {
      val batchId = rows(index).batchId
      var end = index + 1
      while (end < total && rows(end).batchId == batchId) {
        end += 1
      }
      acc = acc :+ foldSegment(rows, index, end, steps, mapKey, mapping, above)
      index = end
    }
    acc

  private def recordFold(
      insert: Long,
      merge: Long,
      workers: Int,
      partials: Int,
      dense: Int,
  ): Unit =
    insertNs.set(insert)
    mergeNs.set(merge)
    stripeWorkers.set(workers)
    stripePartials.set(partials)
    denseSegments.set(dense)

  private def recordExchange(exchange: Long, encode: Long): Unit =
    exchangeNs.set(exchange)
    encodeNs.set(encode)

  /**
   * How many stripe partials each input batch owns. A stripe overlaps a batch in at most one run,
   * and flatten emits those runs in batch order, so this is the [[PartialCombine]] grouping.
   */
  private def overlapCounts(counts: Vector[Int], width: Int): Vector[Int] =
    if (width <= 1) then Vector.fill(counts.length)(1)
    else
      val total = counts.foldLeft(0)((sum, count) => sum + count)
      val base = total / width
      val extra = total % width
      var batchStart = 0
      var acc = Vector.empty[Int]
      counts.foreach { count =>
        val batchEnd = batchStart + count
        var worker = 0
        var hits = 0
        while (worker < width) {
          val start = worker * base + math.min(worker, extra)
          val end = start + base + (if (worker < extra) then 1 else 0)
          if (start < batchEnd && batchStart < end) then hits += 1
          worker += 1
        }
        acc = acc :+ hits
        batchStart = batchEnd
      }
      acc

  /** One partial for every morsel of one source batch inside a stripe. */
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  private def foldSegment(
      rows: Array[Piece],
      from: Int,
      until: Int,
      steps: List[Selection.Step],
      mapKey: Int => Int,
      mapping: Boolean,
      above: Option[Selection.Step],
  ): ColumnBatch =
    val count = until - from
    val starts = new Array[Int](count)
    val lengths = new Array[Int](count)
    var cursor = 0
    var maxLength = 0
    while (cursor < count) {
      val morsel = rows(from + cursor).morsel
      starts(cursor) = morsel.start
      lengths(cursor) = morsel.length
      if (morsel.length > maxLength) then maxLength = morsel.length
      cursor += 1
    }
    if (maxLength == 0) then ColumnBatches.emptyIntPairs
    else
      val batch = rows(from).morsel.batch
      if (mapping) then scanMapped(batch, starts, lengths, count, maxLength, steps, mapKey, above)
      else
        DirectCount.aggregateRanges(batch, starts, lengths, steps) match
          case null =>
            val table = borrow(maxLength)
            var index = 0
            while (index < count) {
              val length = lengths(index)
              if (length > 0) then table.scan(batch, starts(index), length, steps)
              index += 1
            }
            table.compact()
          case counted =>
            val _ = denseSegments.incrementAndGet()
            counted

  /**
   * Hash scan of one source batch. [[DirectCount]] is indexed by the source key, so it cannot
   * store `mapKey`. One table covers every morsel of the batch inside this stripe.
   */
  private def scanMapped(
      batch: ColumnBatch,
      starts: Array[Int],
      lengths: Array[Int],
      count: Int,
      maxLength: Int,
      steps: List[Selection.Step],
      mapKey: Int => Int,
      above: Option[Selection.Step],
  ): ColumnBatch =
    val table = borrow(maxLength)
    var index = 0
    while (index < count) {
      val length = lengths(index)
      if (length > 0) then table.scanMapped(batch, starts(index), length, steps, mapKey, above)
      index += 1
    }
    table.compact()

  /** Passed when `mapping` is false. [[foldSegment]] does not call it. */
  private val unchangedKey: Int => Int = key => key

  /**
   * One table per thread. A stripe clears it between source batches and not between morsels of one
   * batch. It starts at the longest morsel and grows for new keys. [[MorselScheduler]] drops the
   * holder when the pool task returns; the caller path drops it when [[execute]] returns.
   */
  private val locals: ThreadLocal[Table | Null] = new ThreadLocal[Table | Null]()
  private val allocatedTables = new AtomicLong
  private val reusedTables = new AtomicLong
  private val peakSlots = new AtomicInteger

  /** Key, count, occupancy byte, and slot index. Matches [[HashCount.inputRowBytes]]. */
  private val SlotBytes = 13L

  private[columnar] def releaseWorkers(): Unit =
    locals.remove()
    DirectCount.release()

  /** Zeros the reuse counters and drops this thread's table. */
  private[columnar] def resetReuseForTest(): Unit =
    allocatedTables.set(0L)
    reusedTables.set(0L)
    peakSlots.set(0)
    DirectCount.resetPeakForTest()
    HashCount.resetPeakForTest()
    releaseWorkers()

  /** Tables constructed since the last [[resetReuseForTest]], on every thread. */
  private[columnar] def tablesAllocatedForTest: Long = allocatedTables.get

  /** Segments that cleared an existing table instead of allocating one. */
  private[columnar] def tablesReusedForTest: Long = reusedTables.get

  /**
   * Largest table since [[resetReuseForTest]], in slots. Includes the dense counter and the
   * [[HashCount]] merge, which is where a per-morsel concat would show up.
   */
  private[scarlet] def peakSlotsForTest: Int =
    math.max(peakSlots.get, math.max(DirectCount.peakSlotsForTest, HashCount.peakSlotsForTest))

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def notePeak(slots: Int): Unit =
    var prev = peakSlots.get
    while (slots > prev && !peakSlots.compareAndSet(prev, slots)) do prev = peakSlots.get

  private def borrow(rows: Int): Table =
    val need = ColumnHash.tableSize(rows)
    locals.get() match
      case current: Table if current.accepts(need) =>
        val _ = reusedTables.incrementAndGet()
        current.clear()
        notePeak(current.slotCount)
        current
      case _ =>
        val created = new Table(need)
        locals.set(created)
        val _ = allocatedTables.incrementAndGet()
        notePeak(need)
        created

  /**
   * Combined partials at or under this length stay on the caller. Above it the pool still merges
   * and partitions. 8192 is one default morsel: a residue count such as `row % 1024` finishes
   * under it, and a wide key domain does not.
   */
  private val CallerRows = 8192

  private def shortEnough(batches: Vector[ColumnBatch]): Boolean =
    var total = 0
    val rows = batches.iterator
    var fits = true
    while (fits && rows.hasNext) {
      total += rows.next().length
      if (total > CallerRows) then fits = false
    }
    fits

  /**
   * [[HashExchange.partitionAll]] without a dispatch. One visit per input batch, then bucket `i`
   * concatenated in batch order, matching the parallel exchange.
   */
  private def partitionHere(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
  ): Vector[ColumnBatch] =
    val scattered = batches.map { batch =>
      ColumnMetrics.visit()
      HashExchange.partitionInt32(batch, keyOrdinal = 0, numPartitions)
    }
    Vector.tabulate(numPartitions) { part =>
      val pieces = scattered.map { one =>
        one.lift(part).getOrElse {
          throw new IllegalArgumentException(s"missing partition $part")
        }
      }
      ColumnBatches.concat(pieces)
    }

  /** [[HashCount.addCounts]] of each bucket, in partition order, with one visit per bucket. */
  private def combineHere(buckets: Vector[ColumnBatch]): Vector[ColumnBatch] =
    buckets.map { bucket =>
      ColumnMetrics.visit()
      HashCount.addCounts(bucket)
    }

  /**
   * Same exchange as [[HashCount]]. Spill stays on the post-exchange bucket. The partial combine
   * above has already refused any table that does not fit.
   */
  private def finish(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    if (combined.isEmpty) then
      recordExchange(0L, 0L)
      Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      val exchangeStarted = System.nanoTime()
      val onCaller = shortEnough(combined)
      val buckets =
        if (onCaller) then partitionHere(combined, numPartitions)
        else HashExchange.partitionAll(combined, keyOrdinal = 0, numPartitions, parallelism)
      val exchanged = System.nanoTime()
      val conf = ScarletConf.get
      val encoded =
        if (buckets.exists(bucket => HashCountSpill.exceeds(bucket, conf))) then
          buckets.map(bucket => HashCountSpill.finish(bucket, parallelism, batchSize, conf))
        else if (onCaller) then combineHere(buckets)
        else MorselScheduler.map(buckets, parallelism)((_, bucket) => HashCount.addCounts(bucket))
      recordExchange(exchanged - exchangeStarted, System.nanoTime() - exchanged)
      encoded

  /**
   * Spill off charges every source row. Spill on charges one morsel: the partial grows with
   * distinct keys, and [[HashCount.addCounts]] still refuses a merge that does not fit. A one-row
   * table larger than the budget charges the whole input, whichever way spill is set.
   */
  private def withinBudget(batches: Vector[ColumnBatch], batchSize: Int): Boolean =
    val conf = ScarletConf.get
    val budget = conf.spillBudgetBytes
    val rows = batches.iterator.map(_.length.toLong).sum
    if (!conf.spillEnabled || HashCount.inputRowBytes(1L) > budget) then
      HashCount.inputRowBytes(rows) <= budget
    else
      batches.forall { batch =>
        val morsel = math.min(batch.length.toLong, batchSize.toLong)
        HashCount.inputRowBytes(morsel) <= budget
      }

  private def notePass(): Unit =
    val _ = passes.incrementAndGet()

  /** Live key bits are all set, and the bitmap covers that prefix. */
  private def liveKeys(keys: Int32Column): Boolean =
    val live = keys.length
    val words = keys.validity.words
    val fullWords = live >>> 6
    val tail = live & 63
    val covered = fullWords + (if (tail == 0) then 0 else 1)
    words.length >= covered && Validity.livePresent(keys.validity, live)

  private[columnar] def requirePositive(
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Unit =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")

  /**
   * One all-valid bitmap per caller thread and row count. Owned pair arrays have no nulls, and the
   * count only reads the bits, so the next action with the same length does not fill them again.
   */
  private val presentValid: ThreadLocal[Validity | Null] = new ThreadLocal[Validity | Null]()

  /**
   * One all-valid batch per partition. The columns keep the caller's arrays and share one bitmap
   * for this length. Do not return this batch: the partition still owns those arrays, and a writer
   * would change the shared bits. The count does not write them.
   */
  private[columnar] def presentParts(
      parts: Vector[(Array[Int], Array[Int])],
  ): Vector[ColumnBatch] =
    parts.map { (keys, values) =>
      require(keys.length == values.length, "key and value arrays differ in length")
      val rows = keys.length
      val present = sharedPresent(rows)
      new ColumnBatch(
        Vector(LogicalType.Int32, LogicalType.Int32),
        Vector(
          Int32Column.of(keys, present, rows),
          Int32Column.of(values, present, rows),
        ),
        rows,
      )
    }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def sharedPresent(rows: Int): Validity =
    presentValid.get() match
      case current: Validity if current.capacity == rows => current
      case _ =>
        val created = Validity.allValid(rows)
        presentValid.set(created)
        created

  private def requireKey(batch: ColumnBatch): Unit =
    val keyType = batch.schema.headOption.getOrElse {
      throw new ColumnarFallback("countByKey expects an Int32 key column")
    }
    keyType match
      case LogicalType.Int32 => ()
      case LogicalType.Int64 | LogicalType.Float64 | LogicalType.Float32 | LogicalType.Bool |
          LogicalType.Utf8Dict | LogicalType.Decimal(_, _) =>
        throw new ColumnarFallback(
          s"countByKey expects an Int32 key column, found $keyType",
        )

  private def ordinalMask(stack: List[Selection.Step], mask: Int = 0): Int =
    stack match
      case Nil => mask
      case Selection.Step.Int32(column, _) :: rest => ordinalMask(rest, mask | (1 << column))
      case _ :: rest => ordinalMask(rest, mask)

  /** One [[Validity.rangePresent]] per ordinal named by `mask`. */
  private def denseRange(batch: ColumnBatch, start: Int, length: Int, mask: Int): Boolean =
    ColumnHash.denseRange(batch, start, length, mask)

  private final class Table(initial: Int):
    private var mask: Int = initial - 1
    private var limit: Int = initial >>> 1
    private var keys: Array[Int] = new Array[Int](initial)
    private var counts: Array[Int] = new Array[Int](initial)
    private var state: Array[Byte] = new Array[Byte](initial)
    private var slots: Array[Int] = new Array[Int](initial)
    private var occupied: Int = 0

    def accepts(need: Int): Boolean = keys.length >= need

    def slotCount: Int = keys.length

    /**
     * Drops every key this morsel inserted. The arrays stay, so the next morsel does not zero
     * them.
     */
    def clear(): Unit =
      var cursor = 0
      val rows = occupied
      while (cursor < rows) {
        state(slots(cursor)) = 0
        cursor += 1
      }
      occupied = 0

    def scanMapped(
        batch: ColumnBatch,
        start: Int,
        length: Int,
        steps: List[Selection.Step],
        mapKey: Int => Int,
        above: Option[Selection.Step] = None,
    ): Unit =
      val add = (key: Int, weight: Int) => this.add(key, weight)
      above match
        case Some(step) => MappedKeyFold.scanAbove(batch, start, length, steps, add, mapKey, step)
        case None => MappedKeyFold.scan(batch, start, length, steps, add, mapKey)

    def scan(batch: ColumnBatch, start: Int, length: Int, steps: List[Selection.Step]): Unit =
      val readMask = ordinalMask(steps)
      val key = ColumnHash.int32At(batch, 0)
      Ranges.check(start, length, key.length)
      val value =
        if ((readMask & 2) != 0) then
          val column = ColumnHash.int32At(batch, 1)
          Ranges.check(start, length, column.length)
          require(key.length == column.length, "key and value columns differ in length")
          column
        else unusedInt32
      val stepsDense = denseRange(batch, start, length, readMask)
      // A key step already drops a null key. Otherwise the key bitmap does, unless the range is
      // dense and there is nothing to drop.
      val skipNullKey = ((readMask & 1) != 0) || Validity.rangePresent(key.validity, start, length)
      val keyValues = key.values
      val valueValues = value.values
      steps match
        case Selection.Step.Int32(left, leftKeep) :: Selection.Step
              .Int32(right, rightKeep) :: Nil =>
          scanTwo(
            start,
            length,
            left,
            leftKeep,
            right,
            rightKeep,
            keyValues,
            key.validity.words,
            valueValues,
            value.validity.words,
            stepsDense,
            skipNullKey,
          )
        case Selection.Step.Int32(ordinal, keep) :: Nil =>
          scanOne(
            start,
            length,
            ordinal,
            keep,
            keyValues,
            key.validity.words,
            valueValues,
            value.validity.words,
            stepsDense,
            skipNullKey,
          )
        case _ =>
          scanEach(
            start,
            length,
            steps,
            keyValues,
            key.validity.words,
            valueValues,
            value.validity.words,
            stepsDense,
            skipNullKey,
          )

    def compact(): ColumnBatch =
      val rows = occupied
      val outKeys = new Array[Int](rows)
      val outCounts = new Array[Int](rows)
      var cursor = 0
      while (cursor < rows) {
        val slot = slots(cursor)
        outKeys(cursor) = keys(slot)
        outCounts(cursor) = counts(slot)
        cursor += 1
      }
      val present = Validity.allValid(rows)
      new ColumnBatch(
        Vector(LogicalType.Int32, LogicalType.Int32),
        Vector(Int32Column.of(outKeys, present, rows), Int32Column.of(outCounts, present, rows)),
        rows,
      )

    private def scanTwo(
        start: Int,
        length: Int,
        leftOrdinal: Int,
        leftKeep: ColumnKernel.Int32Predicate,
        rightOrdinal: Int,
        rightKeep: ColumnKernel.Int32Predicate,
        keyValues: Array[Int],
        keyWords: Array[Long],
        valueValues: Array[Int],
        valueWords: Array[Long],
        stepsDense: Boolean,
        skipNullKey: Boolean,
    ): Unit =
      if (stepsDense && skipNullKey) then
        addTwoDense(
          start,
          length,
          leftOrdinal,
          leftKeep,
          rightOrdinal,
          rightKeep,
          keyValues,
          valueValues,
        )
      else if (stepsDense) then
        addTwoDropNullKey(
          start,
          length,
          leftOrdinal,
          leftKeep,
          rightOrdinal,
          rightKeep,
          keyValues,
          keyWords,
          valueValues,
        )
      else
        addTwoChecked(
          start,
          length,
          leftOrdinal,
          leftKeep,
          rightOrdinal,
          rightKeep,
          keyValues,
          keyWords,
          valueValues,
          valueWords,
          skipNullKey,
        )

    private def addTwoDense(
        start: Int,
        length: Int,
        leftOrdinal: Int,
        leftKeep: ColumnKernel.Int32Predicate,
        rightOrdinal: Int,
        rightKeep: ColumnKernel.Int32Predicate,
        keyValues: Array[Int],
        valueValues: Array[Int],
    ): Unit =
      val leftValues = if (leftOrdinal == 0) keyValues else valueValues
      val rightValues = if (rightOrdinal == 0) keyValues else valueValues
      val end = start + length
      var row = start
      while (row < end) {
        if (leftKeep(leftValues(row)) && rightKeep(rightValues(row))) then add(keyValues(row), 1)
        row += 1
      }

    private def addTwoDropNullKey(
        start: Int,
        length: Int,
        leftOrdinal: Int,
        leftKeep: ColumnKernel.Int32Predicate,
        rightOrdinal: Int,
        rightKeep: ColumnKernel.Int32Predicate,
        keyValues: Array[Int],
        keyWords: Array[Long],
        valueValues: Array[Int],
    ): Unit =
      val leftValues = if (leftOrdinal == 0) keyValues else valueValues
      val rightValues = if (rightOrdinal == 0) keyValues else valueValues
      val end = start + length
      var row = start
      while (row < end) {
        if (
            leftKeep(leftValues(row)) && rightKeep(rightValues(row)) &&
            Validity.isSet(keyWords, row)
          )
        then add(keyValues(row), 1)
        row += 1
      }

    private def addTwoChecked(
        start: Int,
        length: Int,
        leftOrdinal: Int,
        leftKeep: ColumnKernel.Int32Predicate,
        rightOrdinal: Int,
        rightKeep: ColumnKernel.Int32Predicate,
        keyValues: Array[Int],
        keyWords: Array[Long],
        valueValues: Array[Int],
        valueWords: Array[Long],
        skipNullKey: Boolean,
    ): Unit =
      val leftValues = if (leftOrdinal == 0) keyValues else valueValues
      val rightValues = if (rightOrdinal == 0) keyValues else valueValues
      val leftWords = if (leftOrdinal == 0) keyWords else valueWords
      val rightWords = if (rightOrdinal == 0) keyWords else valueWords
      val end = start + length
      var row = start
      if (skipNullKey) then
        while (row < end) {
          if (
              Validity.isSet(leftWords, row) && leftKeep(leftValues(row)) &&
              Validity.isSet(rightWords, row) && rightKeep(rightValues(row))
            )
          then add(keyValues(row), 1)
          row += 1
        }
      else
        while (row < end) {
          if (
              Validity.isSet(leftWords, row) && leftKeep(leftValues(row)) &&
              Validity.isSet(rightWords, row) && rightKeep(rightValues(row)) &&
              Validity.isSet(keyWords, row)
            )
          then add(keyValues(row), 1)
          row += 1
        }

    private def scanOne(
        start: Int,
        length: Int,
        ordinal: Int,
        keep: ColumnKernel.Int32Predicate,
        keyValues: Array[Int],
        keyWords: Array[Long],
        valueValues: Array[Int],
        valueWords: Array[Long],
        stepsDense: Boolean,
        skipNullKey: Boolean,
    ): Unit =
      val values = if (ordinal == 0) keyValues else valueValues
      val words = if (ordinal == 0) keyWords else valueWords
      val end = start + length
      var row = start
      if (stepsDense && skipNullKey) then
        while (row < end) {
          if (keep(values(row))) then add(keyValues(row), 1)
          row += 1
        }
      else if (stepsDense) then
        while (row < end) {
          if (keep(values(row)) && Validity.isSet(keyWords, row)) then add(keyValues(row), 1)
          row += 1
        }
      else if (skipNullKey) then
        while (row < end) {
          if (Validity.isSet(words, row) && keep(values(row))) then add(keyValues(row), 1)
          row += 1
        }
      else
        while (row < end) {
          if (Validity.isSet(words, row) && keep(values(row)) && Validity.isSet(keyWords, row))
          then add(keyValues(row), 1)
          row += 1
        }

    private def scanEach(
        start: Int,
        length: Int,
        steps: List[Selection.Step],
        keyValues: Array[Int],
        keyWords: Array[Long],
        valueValues: Array[Int],
        valueWords: Array[Long],
        stepsDense: Boolean,
        skipNullKey: Boolean,
    ): Unit =
      val end = start + length
      var row = start
      while (row < end) {
        val kept =
          if (stepsDense) then keepsDense(steps, row, keyValues, valueValues)
          else keepsChecked(steps, row, keyValues, keyWords, valueValues, valueWords)
        if (kept && (skipNullKey || Validity.isSet(keyWords, row))) then add(keyValues(row), 1)
        row += 1
      }

    private def add(key: Int, weight: Int): Unit =
      val slot = locateForInsert(key)
      if (state(slot) == 0) then
        state(slot) = 1
        keys(slot) = key
        counts(slot) = weight
        slots(occupied) = slot
        occupied += 1
      else counts(slot) = counts(slot) + weight

    /**
     * Slot for `key`. A new key at the load limit doubles the table and is located again. An
     * existing key does not grow it. [[grow]] reinserts occupied groups in first-seen order.
     */
    private def locateForInsert(key: Int): Int =
      val slot = locate(key)
      if (state(slot) == 0 && occupied >= limit) then
        grow()
        locate(key)
      else slot

    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      while (state(slot) != 0 && keys(slot) != key) slot = (slot + 1) & mask
      slot

    /** Doubles the table and reinserts occupied groups in first-seen order. */
    private def grow(): Unit =
      val next = keys.length << 1
      if (next <= 0 || next > ColumnHash.MaxCapacity) then
        throw new IllegalStateException(s"hash count table is full at $occupied groups")
      val bytes = next.toLong * SlotBytes
      val budget = ScarletConf.get.spillBudgetBytes
      if (bytes > budget) then throw new ClosedKernelRefusal(occupied.toLong, bytes, budget)
      val nextMask = next - 1
      val nextKeys = new Array[Int](next)
      val nextCounts = new Array[Int](next)
      val nextState = new Array[Byte](next)
      val nextSlots = new Array[Int](next)
      var cursor = 0
      val rows = occupied
      while (cursor < rows) {
        val from = slots(cursor)
        val key = keys(from)
        var slot = ColumnHash.mix(key) & nextMask
        while (nextState(slot) != 0) {
          slot = (slot + 1) & nextMask
        }
        nextState(slot) = 1
        nextKeys(slot) = key
        nextCounts(slot) = counts(from)
        nextSlots(cursor) = slot
        cursor += 1
      }
      keys = nextKeys
      counts = nextCounts
      state = nextState
      slots = nextSlots
      mask = nextMask
      limit = next >>> 1
      notePeak(next)

  @tailrec
  private def keepsDense(
      stack: List[Selection.Step],
      row: Int,
      keyValues: Array[Int],
      valueValues: Array[Int],
  ): Boolean =
    stack match
      case Nil => true
      case step :: rest =>
        if (keepsDenseStep(step, row, keyValues, valueValues)) then
          keepsDense(rest, row, keyValues, valueValues)
        else false

  @tailrec
  private def keepsChecked(
      stack: List[Selection.Step],
      row: Int,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
  ): Boolean =
    stack match
      case Nil => true
      case step :: rest =>
        if (keepsCheckedStep(step, row, keyValues, keyWords, valueValues, valueWords)) then
          keepsChecked(rest, row, keyValues, keyWords, valueValues, valueWords)
        else false

  private def keepsDenseStep(
      step: Selection.Step,
      row: Int,
      keyValues: Array[Int],
      valueValues: Array[Int],
  ): Boolean = step match
    case Selection.Step.Int32(ordinal, predicate) =>
      val values = if (ordinal == 0) keyValues else valueValues
      predicate(values(row))
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
      rejectedStep

  private def keepsCheckedStep(
      step: Selection.Step,
      row: Int,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
  ): Boolean = step match
    case Selection.Step.Int32(ordinal, predicate) =>
      val values = if (ordinal == 0) keyValues else valueValues
      val words = if (ordinal == 0) keyWords else valueWords
      Validity.isSet(words, row) && predicate(values(row))
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
      rejectedStep

  private def rejectedStep: Boolean =
    require(false, "folded count steps are int slots")
    false

  private val unusedInt32: Int32Column =
    Int32Column.of(Array.empty[Int], Validity.allValid(0), 0)
