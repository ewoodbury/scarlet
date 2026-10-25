package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Direct-index grouped sum for a bounded `Int` key range.
 *
 * One slot is 8 bytes of sum, one flag byte, 4 bytes in the touched-key list, and 4 bytes in the
 * zero-origin key cache: 17 bytes. The flag holds two bits. The group bit means the key was seen.
 * The value bit means a value was added. A null value sets the group bit and leaves the sum null.
 * A filtered sum sets both bits only when a value is kept, so a key whose rows are all dropped
 * stays absent. The group bit keeps that null sum in the output. The value bit marks the sum
 * present. Those are different results, so the flag needs both bits.
 *
 * `cap` is the largest range that stays inside a 4 MiB budget across the workers:
 * `parallelism * range * 17 <= 4 * 1024 * 1024`. The division floors, so past 246723 workers the
 * cap is 0 and the caller hashes. At 4 workers the cap is 61680 slots, 4194240 bytes. A prefix of
 * 65536 does not fit: `4 * 65536 * 17` is 4456448. [[directLimit]] is `min(cap, 65536)`.
 *
 * The table is the observed window, `max - min + 1`, and never the whole cap. A morsel whose max
 * key is 1023 allocates 1024 slots. The window is used only when that span also fits in the cap
 * and is at most [[DensityFactor]] times the morsel length. 1024 groups spread over `0` until
 * `300000` fail that test and hash. The span is `max.toLong - min.toLong + 1`. An `Int`
 * subtraction overflows on `Int.MinValue` beside `Int.MaxValue` and must not be used. Null keys
 * are skipped, the same rows accumulation skips.
 *
 * Keys come out in ascending order. The hash path still emits first-seen order. Grouped sums are
 * compared as maps. There is no first-touch order array.
 *
 * The arrays live on [[HashAggregate]]'s per-worker holder. [[Accumulator.prepare]] clears the
 * touched slots. Those arrays are indexed by slot, so a new minimum with the same width does not
 * allocate. A later morsel that still fits the same zero-origin window checks presence and the key
 * prefix, writes nothing on a miss, and otherwise uses the unrolled sum. That sum stores the flag
 * on every row. A fresh permutation makes a per-row "already seen" test mispredict, and the same
 * test predicts when every morsel repeats one key sequence. The touched list is rebuilt with one
 * pass over the window after the rows are added. An armed failure still records each new group as
 * it is inserted, so a throw leaves those slots on the touched list.
 * [[HashAggregate.releaseSumWorkers]] drops the holder. Each published batch gets a new key array
 * and a new validity bitmap per column. A worker may keep a private copy of the zero-origin keys
 * and copy that into the batch. The private copy is not published. The cache holds one span, the
 * last length requested, so alternating lengths allocate again.
 *
 * [[Accumulator.compactWindow]] is given slot indexes and adds `minKey`. A zero-origin scan is the
 * case where the slot index is already the key. A shifted window uses the same addition.
 * [[Accumulator.addDirect]] checks that the rows are present, then rejects a key outside the table
 * before any slot is written. A failure after that check clears the slots it did write.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object DenseSum:

  /** Sum long, flag byte, touched-list int, and one int in the zero-origin key cache. */
  val BytesPerSlot: Long = 17L

  /** `parallelism * range *` [[BytesPerSlot]] stays at or under this many bytes. */
  val BudgetBytes: Long = 4L * 1024L * 1024L

  /**
   * Largest zero-based prefix [[directLimit]] will return. Four workers cannot hold this many
   * 17-byte slots inside [[BudgetBytes]]. The bound is `min(cap, DirectPrefix)`.
   */
  val DirectPrefix: Int = 65536

  /**
   * A window is dense when its span is at most this many times the morsel length. Length is the
   * bound that the classify pass already has. Distinct keys cannot exceed it.
   */
  val DensityFactor: Int = 4

  def directLimit(cap: Int): Int =
    math.max(0, math.min(cap, DirectPrefix))

  private val GroupMask: Int = 1
  private val ValueMask: Int = 2
  private val GroupBit: Byte = 1
  private val BothBits: Byte = 3

  sealed trait Fit

  /** No present key in the scanned rows. The sum emits nothing. */
  case object Empty extends Fit

  /** The present keys do not fit in `cap`. The caller uses the hash table. */
  case object Wide extends Fit

  /** Present keys lie in `[min, min + range)`. `range` is at least 1, dense, and at most `cap`. */
  final case class Window(min: Int, range: Int) extends Fit

  /** [[classify]] together with whether both columns are fully present in the scanned rows. */
  final case class Survey(fit: Fit, columnsPresent: Boolean)

  /**
   * Largest range whose tables fit in [[BudgetBytes]] when each of `parallelism` workers holds
   * one. A non-positive parallelism is rejected. The result is 0 when the budget cannot hold one
   * slot per worker. Callers hash in that case.
   */
  def cap(parallelism: Int): Int =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    // The budget is 4 MiB and a slot is 17 bytes, so the quotient fits in an Int.
    (BudgetBytes / (BytesPerSlot * parallelism.toLong)).toInt

  /**
   * `Empty` when `[start, start + length)` has no present key. `Window` when the present keys fit
   * in `cap` and are dense in the row count. `Wide` otherwise. One pass reads that row range.
   */
  def classify(keys: Int32Column, start: Int, length: Int, cap: Int): Fit =
    require(
      start >= 0 && length >= 0 && start <= keys.length && length <= keys.length - start,
      s"range ($start, $length) outside ${keys.length} rows",
    )
    if (length == 0) then Empty
    else if (cap <= 0) then Wide
    else boundKeys(keys.values, keys.validity.words, start, start + length, cap)

  /**
   * [[classify]] plus whether every key and value in the row range is present. One pass. `Wide`
   * may stop early, and then `columnsPresent` is false because the scan did not finish.
   */
  def survey(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      cap: Int,
  ): Survey =
    require(keys.length == values.length, "key and value columns differ in length")
    require(
      start >= 0 && length >= 0 && start <= keys.length && length <= keys.length - start,
      s"range ($start, $length) outside ${keys.length} rows",
    )
    if (length == 0) then Survey(Empty, columnsPresent = true)
    else if (cap <= 0) then Survey(Wide, columnsPresent = false)
    else
      boundRows(
        keys.values,
        keys.validity.words,
        values.validity.words,
        start,
        start + length,
        cap,
      )

  /**
   * Reused direct-index table. `prepare` then one scan then [[compact]] is one range. The next
   * range calls `prepare` again.
   */
  final class Accumulator:

    private var minKey: Int = 0
    private var keyRange: Int = -1
    private var sums: Array[Long] = Array.emptyLongArray
    private var seen: Array[Byte] = Array.emptyByteArray
    private var touched: Array[Int] = Array.emptyIntArray
    private var touchedCount: Int = 0
    private var nullSums: Int = 0
    private var minSeen: Int = 0
    private var maxSeen: Int = -1
    private var identityKeys: Array[Int] = Array.emptyIntArray
    // Groups recorded before the next scan throws. Negative means the scan runs to the end.
    private var failAt: Int = -1

    /**
     * Allocates when `range` changes. A new origin with the same width only clears touched slots:
     * the arrays are indexed by slot, and `min` is added back when the batch is published.
     */
    def prepare(min: Int, range: Int): Unit =
      require(range > 0, s"dense range must be positive, got $range")
      if (range != keyRange) then allocate(min, range)
      else
        minKey = min
        reset()

    /**
     * Table for keys in `[0, limit)`. The next call with the same `limit` clears touched slots.
     */
    def prepareDirect(limit: Int): Unit =
      prepare(0, limit)

    /** Groups recorded by the current window. Zero after [[prepare]] and [[abandonDirect]]. */
    private[columnar] def touchedSlots: Int = touchedCount

    /**
     * The next present scan throws [[ArrayIndexOutOfBoundsException]] once `recorded` groups have
     * been written. The slots stay dirty until [[prepare]] or [[abandonDirect]]. One shot.
     */
    private[columnar] def failAfter(recorded: Int): Unit =
      require(recorded > 0, s"recorded groups must be positive, got $recorded")
      failAt = recorded

    /**
     * Sums present rows by using the key as the index. Returns `false` when a key is outside
     * `[0, sums.length)`, before any slot is written. A throw after that check leaves the written
     * slots in place. [[abandonDirect]] or [[prepare]] of this same window clears them.
     */
    def addDirect(keys: Int32Column, values: Int32Column, start: Int, end: Int): Boolean =
      val width = end - start
      assert(
        Validity.rangePresent(keys.validity, start, width) &&
          Validity.rangePresent(values.validity, start, width),
        "direct sum requires present keys and values",
      )
      val limit = sums.length
      if (!fitsPrefix(keys.values, start, end, limit)) then false
      else
        sumPrefix(keys, values, start, end)
        true

    /**
     * Keys and values are present and every key is in `[0, keyRange)`. Used after [[survey]] has
     * already accepted a zero-origin window of this width.
     */
    private[columnar] def addPresentZero(
        keys: Int32Column,
        values: Int32Column,
        start: Int,
        end: Int,
    ): Unit =
      sumPrefix(keys, values, start, end)

    /**
     * True when this table is a zero-origin window that still satisfies `cap` and the density
     * limit for `length`. Clears the previous morsel's slots. A different width or origin has to
     * be surveyed.
     */
    private[columnar] def reuseZero(cap: Int, length: Int): Boolean =
      val range = keyRange
      val reusable =
        range > 0 && minKey == 0 && range <= cap && range.toLong <= denseLimit(length)
      if (reusable) then reset()
      reusable

    /**
     * Sums rows into the current zero-origin window. Returns false before any slot is written when
     * a row is null or a key is outside the window. The caller then surveys the range.
     */
    private[columnar] def sumReuse(
        keys: Int32Column,
        values: Int32Column,
        start: Int,
        end: Int,
    ): Boolean =
      requireReady(start, end, keys.length, values.length)
      val width = end - start
      val present =
        Validity.rangePresent(keys.validity, start, width) &&
          Validity.rangePresent(values.validity, start, width)
      if (present && fitsPrefix(keys.values, start, end, keyRange)) then
        sumPrefix(keys, values, start, end)
        true
      else false

    /** Every key in `[start, end)` is in `[0, limit)`. Reads keys and writes nothing. */
    private def fitsPrefix(keys: Array[Int], start: Int, end: Int, limit: Int): Boolean =
      var row = start
      var fits = true
      while (fits && row < end) {
        val key = keys(row)
        if (key < 0 || key >= limit) then fits = false
        row += 1
      }
      fits

    /**
     * Keys are in `[0, keyRange)`. The steady path stores the flag on every row and rebuilds the
     * touched list afterwards. An armed failure records each new group before its sum changes, and
     * throws from that record.
     */
    private def sumPrefix(keys: Int32Column, values: Int32Column, start: Int, end: Int): Unit =
      requireReady(start, end, keys.length, values.length)
      if (failAt >= 0) then sumPrefixNoting(keys, values, start, end)
      else sumPrefixOpen(keys, values, start, end)

    private def sumPrefixOpen(
        keys: Int32Column,
        values: Int32Column,
        start: Int,
        end: Int,
    ): Unit =
      val keyValues = keys.values
      val valueValues = values.values
      val sumsLocal = sums
      val seenLocal = seen
      val both = BothBits
      var row = start
      while (row + 8 <= end) {
        val i0 = keyValues(row)
        val i1 = keyValues(row + 1)
        val i2 = keyValues(row + 2)
        val i3 = keyValues(row + 3)
        val i4 = keyValues(row + 4)
        val i5 = keyValues(row + 5)
        val i6 = keyValues(row + 6)
        val i7 = keyValues(row + 7)
        seenLocal(i0) = both
        sumsLocal(i0) += valueValues(row).toLong
        seenLocal(i1) = both
        sumsLocal(i1) += valueValues(row + 1).toLong
        seenLocal(i2) = both
        sumsLocal(i2) += valueValues(row + 2).toLong
        seenLocal(i3) = both
        sumsLocal(i3) += valueValues(row + 3).toLong
        seenLocal(i4) = both
        sumsLocal(i4) += valueValues(row + 4).toLong
        seenLocal(i5) = both
        sumsLocal(i5) += valueValues(row + 5).toLong
        seenLocal(i6) = both
        sumsLocal(i6) += valueValues(row + 6).toLong
        seenLocal(i7) = both
        sumsLocal(i7) += valueValues(row + 7).toLong
        row += 8
      }
      while (row < end) {
        val index = keyValues(row)
        seenLocal(index) = both
        sumsLocal(index) += valueValues(row).toLong
        row += 1
      }
      recordTouched(seenLocal)

    /** Same rows as [[sumPrefixOpen]]. Each new group is recorded before its sum is updated. */
    private def sumPrefixNoting(
        keys: Int32Column,
        values: Int32Column,
        start: Int,
        end: Int,
    ): Unit =
      val keyValues = keys.values
      val valueValues = values.values
      val sumsLocal = sums
      val seenLocal = seen
      val touchedLocal = touched
      val both = BothBits
      minSeen = Int.MaxValue
      maxSeen = Int.MinValue
      var row = start
      while (row < end) {
        val index = keyValues(row)
        if (seenLocal(index) == 0) then note(seenLocal, touchedLocal, index, both)
        sumsLocal(index) += valueValues(row).toLong
        row += 1
      }

    /** One pass over the window. Slots the open sum marked are appended in ascending order. */
    private def recordTouched(seenLocal: Array[Byte]): Unit =
      val touchedLocal = touched
      val limit = keyRange
      var index = 0
      var count = 0
      var low = Int.MaxValue
      var high = Int.MinValue
      while (index < limit) {
        if (seenLocal(index) != 0) then
          touchedLocal(count) = index
          count += 1
          if (index < low) then low = index
          if (index > high) then high = index
        index += 1
      }
      touchedCount = count
      if (count == 0) then
        minSeen = Int.MaxValue
        maxSeen = -1
      else
        minSeen = low
        maxSeen = high

    private def note(
        seenLocal: Array[Byte],
        touchedLocal: Array[Int],
        index: Int,
        both: Byte,
    ): Unit =
      val count = touchedCount
      touchedLocal(count) = index
      touchedCount = count + 1
      seenLocal(index) = both
      if (index < minSeen) then minSeen = index
      if (index > maxSeen) then maxSeen = index
      val armed = failAt
      if (armed >= 0 && touchedCount >= armed) then
        failAt = -1
        throw new ArrayIndexOutOfBoundsException(
          s"direct scan failed after recording $touchedCount slots",
        )

    /**
     * Ascending keys in `[minSeen, maxSeen]`. The slot index is the key because this follows
     * [[prepareDirect]].
     */
    def compactDirect(): ColumnBatch =
      failAt = -1
      require(minKey == 0, "direct compaction requires a zero origin")
      val rows = touchedCount
      if (rows == 0) then emptySum
      else
        val span = maxSeen - minSeen + 1
        if (rows == span) then compactSpan(minSeen, span)
        else compactWindow(minSeen, maxSeen, rows)

    /**
     * Clears the whole window, not only the touched list. The open sum writes slots before it
     * rebuilds that list, so a throw in the middle would leave sums the touched list does not
     * name.
     */
    def abandonDirect(): Unit =
      Arrays.fill(sums, 0L)
      Arrays.fill(seen, 0.toByte)
      touchedCount = 0
      nullSums = 0
      minSeen = Int.MaxValue
      maxSeen = -1

    /**
     * Rows that are not fully present. A fully present window uses [[addPresentZero]] or
     * [[addTrusted]]. This scan still accepts present rows. The fully-present loop used to live
     * here and duplicated [[addTrusted]].
     */
    def scanPlain(keys: Int32Column, values: Int32Column, start: Int, end: Int): Unit =
      scanPlainSparse(keys, values, start, end)

    def scanFiltered(
        keys: Int32Column,
        values: Int32Column,
        start: Int,
        end: Int,
        keep: ColumnKernel.Int32Predicate,
        map: ColumnKernel.Int32Map,
    ): Unit =
      val length = end - start
      if (
          Validity.rangePresent(keys.validity, start, length) &&
          Validity.rangePresent(values.validity, start, length)
        )
      then scanFilteredPresent(keys, values, start, end, keep, map)
      else scanFilteredSparse(keys, values, start, end, keep, map)

    /** Ascending keys. A missing value bit publishes a null sum. An unseen group is omitted. */
    def compact(): ColumnBatch =
      failAt = -1
      val rows = touchedCount
      if (rows == 0) then emptySum
      else if (nullSums == 0 && rows == keyRange) then compactFull(rows)
      else if (nullSums == 0 && maxSeen >= 0) then compactWindow(minSeen, maxSeen, rows)
      else compactPartial(rows)

    private def allocate(min: Int, range: Int): Unit =
      minKey = min
      keyRange = range
      sums = new Array[Long](range)
      seen = new Array[Byte](range)
      touched = new Array[Int](range)
      touchedCount = 0
      nullSums = 0
      minSeen = Int.MaxValue
      maxSeen = -1

    private def reset(): Unit =
      val sumsLocal = sums
      val seenLocal = seen
      val touchedLocal = touched
      var cursor = 0
      val count = touchedCount
      while (cursor < count) {
        val index = touchedLocal(cursor)
        sumsLocal(index) = 0L
        seenLocal(index) = 0
        cursor += 1
      }
      touchedCount = 0
      nullSums = 0
      minSeen = Int.MaxValue
      maxSeen = -1

    /**
     * The slot arrays match the prepared range, and `[start, end)` lies inside both columns.
     * Sparse and filtered scans use the same check a fully present scan used to.
     */
    private def requireReady(start: Int, end: Int, keyRows: Int, valueRows: Int): Unit =
      if (
          sums.length != keyRange || seen.length != keyRange || touched.length != keyRange ||
          start < 0 || end < start || end > keyRows || end > valueRows
        )
      then
        throw new IllegalStateException(
          s"dense table length ${sums.length} does not match range $keyRange for [$start, $end)",
        )

    /**
     * Rows in `[start, end)` are present and already classified into this window. Each key is
     * rejected with [[slot]] before its sum is updated, and the touched index is recorded before
     * the next key. The base-zero arm is unrolled because the next key does not depend on this
     * sum. A key outside the window is a failed classification.
     */
    private[columnar] def addTrusted(
        keys: Int32Column,
        values: Int32Column,
        start: Int,
        end: Int,
    ): Unit =
      requireReady(start, end, keys.length, values.length)
      val keyValues = keys.values
      val valueValues = values.values
      val sumsLocal = sums
      val seenLocal = seen
      val touchedLocal = touched
      val base = minKey
      val limit = sumsLocal.length
      val both = BothBits
      var row = start
      if (base == 0) then
        while (row + 4 <= end) {
          val i0 = keyValues(row)
          val i1 = keyValues(row + 1)
          val i2 = keyValues(row + 2)
          val i3 = keyValues(row + 3)
          trust(sumsLocal, seenLocal, touchedLocal, i0, 0, limit, valueValues(row).toLong, both)
          trust(
            sumsLocal,
            seenLocal,
            touchedLocal,
            i1,
            0,
            limit,
            valueValues(row + 1).toLong,
            both,
          )
          trust(
            sumsLocal,
            seenLocal,
            touchedLocal,
            i2,
            0,
            limit,
            valueValues(row + 2).toLong,
            both,
          )
          trust(
            sumsLocal,
            seenLocal,
            touchedLocal,
            i3,
            0,
            limit,
            valueValues(row + 3).toLong,
            both,
          )
          row += 4
        }
        while (row < end) {
          trust(
            sumsLocal,
            seenLocal,
            touchedLocal,
            keyValues(row),
            0,
            limit,
            valueValues(row).toLong,
            both,
          )
          row += 1
        }
      else
        while (row < end) {
          trust(
            sumsLocal,
            seenLocal,
            touchedLocal,
            keyValues(row),
            base,
            limit,
            valueValues(row).toLong,
            both,
          )
          row += 1
        }

    /** Checks `key`, records the slot when it is new, then adds `value`. */
    private inline def trust(
        sumsLocal: Array[Long],
        seenLocal: Array[Byte],
        touchedLocal: Array[Int],
        key: Int,
        base: Int,
        limit: Int,
        value: Long,
        both: Byte,
    ): Unit =
      val index = if (base == 0) then prefixSlot(key, limit) else slot(key, base, limit)
      if (seenLocal(index) == 0) then note(seenLocal, touchedLocal, index, both)
      sumsLocal(index) += value

    private def scanPlainSparse(
        keys: Int32Column,
        values: Int32Column,
        start: Int,
        end: Int,
    ): Unit =
      requireReady(start, end, keys.length, values.length)
      val keyValues = keys.values
      val keyWords = keys.validity.words
      val valueValues = values.values
      val valueWords = values.validity.words
      val sumsLocal = sums
      val seenLocal = seen
      val touchedLocal = touched
      val base = minKey
      val limit = keyRange
      var count = touchedCount
      var missing = nullSums
      var row = start
      while (row < end) {
        if (Validity.isSet(keyWords, row)) then
          val index = slot(keyValues(row), base, limit)
          val valuePresent = Validity.isSet(valueWords, row)
          val flags = seenLocal(index).toInt
          if (flags == 0) then
            touchedLocal(count) = index
            count += 1
            touchedCount = count
            if (valuePresent) then
              seenLocal(index) = BothBits
              sumsLocal(index) += valueValues(row).toLong
            else
              seenLocal(index) = GroupBit
              missing += 1
              nullSums = missing
          else if (valuePresent) then
            if ((flags & ValueMask) == 0) then
              seenLocal(index) = BothBits
              missing -= 1
              nullSums = missing
            sumsLocal(index) += valueValues(row).toLong
        row += 1
      }
      touchedCount = count
      nullSums = missing

    private def scanFilteredPresent(
        keys: Int32Column,
        values: Int32Column,
        start: Int,
        end: Int,
        keep: ColumnKernel.Int32Predicate,
        map: ColumnKernel.Int32Map,
    ): Unit =
      requireReady(start, end, keys.length, values.length)
      val keyValues = keys.values
      val valueValues = values.values
      val sumsLocal = sums
      val seenLocal = seen
      val touchedLocal = touched
      val base = minKey
      val limit = keyRange
      var count = touchedCount
      var row = start
      while (row < end) {
        val value = valueValues(row)
        if (keep(value)) then
          val index = slot(keyValues(row), base, limit)
          if (seenLocal(index) == 0) then
            seenLocal(index) = BothBits
            touchedLocal(count) = index
            count += 1
            touchedCount = count
          sumsLocal(index) += map(value).toLong
        row += 1
      }
      touchedCount = count

    private def scanFilteredSparse(
        keys: Int32Column,
        values: Int32Column,
        start: Int,
        end: Int,
        keep: ColumnKernel.Int32Predicate,
        map: ColumnKernel.Int32Map,
    ): Unit =
      requireReady(start, end, keys.length, values.length)
      val keyValues = keys.values
      val keyWords = keys.validity.words
      val valueValues = values.values
      val valueWords = values.validity.words
      val sumsLocal = sums
      val seenLocal = seen
      val touchedLocal = touched
      val base = minKey
      val limit = keyRange
      var count = touchedCount
      var row = start
      while (row < end) {
        if (Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row)) then
          val value = valueValues(row)
          if (keep(value)) then
            val index = slot(keyValues(row), base, limit)
            if (seenLocal(index) == 0) then
              seenLocal(index) = BothBits
              touchedLocal(count) = index
              count += 1
              touchedCount = count
            sumsLocal(index) += map(value).toLong
        row += 1
      }
      touchedCount = count

    /** Every index was touched and every sum is present, so the slots are already ascending. */
    private def compactFull(rows: Int): ColumnBatch =
      val outKeys = new Array[Int](rows)
      val outSums = new Array[Long](rows)
      val base = minKey.toLong
      var index = 0
      while (index < rows) {
        outKeys(index) = (base + index.toLong).toInt
        index += 1
      }
      System.arraycopy(sums, 0, outSums, 0, rows)
      publish(outKeys, outSums, Array.emptyLongArray, sparse = false, rows)

    /**
     * Keys are `origin, origin + 1, ...`. The returned array is new. A zero origin may copy an
     * internal buffer, and that buffer is not published.
     */
    private def compactSpan(origin: Int, span: Int): ColumnBatch =
      val outSums = new Array[Long](span)
      System.arraycopy(sums, origin, outSums, 0, span)
      publish(spanKeys(origin, span), outSums, Array.emptyLongArray, sparse = false, rows = span)

    private def spanKeys(origin: Int, span: Int): Array[Int] =
      val created = new Array[Int](span)
      if (origin == 0) then System.arraycopy(identity(span), 0, created, 0, span)
      else
        val base = origin.toLong
        var index = 0
        while (index < span) {
          created(index) = (base + index.toLong).toInt
          index += 1
        }
      created

    /**
     * Keys `0, 1, ..., span - 1`. Stays on this worker. Callers copy it before publication.
     *
     * Only the last span is cached. A different length allocates again, so alternating lengths
     * miss. [[BytesPerSlot]] still counts these 4 bytes, because a full span copies one int each.
     */
    private def identity(span: Int): Array[Int] =
      val cached = identityKeys
      if (cached.length == span) then cached
      else
        val created = new Array[Int](span)
        var index = 0
        while (index < span) {
          created(index) = index
          index += 1
        }
        identityKeys = created
        created

    /**
     * `low` and `high` are slot indexes, not keys. The published key is `minKey + index`, which is
     * `index` only when the window origin is 0.
     */
    private def compactWindow(low: Int, high: Int, rows: Int): ColumnBatch =
      val outKeys = new Array[Int](rows)
      val outSums = new Array[Long](rows)
      val seenLocal = seen
      val sumsLocal = sums
      val origin = minKey.toLong
      var cursor = 0
      var index = low
      while (index <= high) {
        if (seenLocal(index) != 0) then
          outKeys(cursor) = (origin + index.toLong).toInt
          outSums(cursor) = sumsLocal(index)
          cursor += 1
        index += 1
      }
      if (cursor != rows) then
        throw new IllegalStateException(
          s"direct sum counted $rows groups between $low and $high, emitted $cursor",
        )
      publish(outKeys, outSums, Array.emptyLongArray, sparse = false, rows)

    private def compactPartial(rows: Int): ColumnBatch =
      val outKeys = new Array[Int](rows)
      val outSums = new Array[Long](rows)
      val sparse = nullSums != 0
      val words = if (sparse) Validity.allocate(rows) else Array.emptyLongArray
      val seenLocal = seen
      val sumsLocal = sums
      val base = minKey
      val groupMask = GroupMask
      val valueMask = ValueMask
      var cursor = 0
      var missing = 0
      var index = 0
      val limit = keyRange
      while (index < limit) {
        val flags = seenLocal(index).toInt
        if ((flags & groupMask) != 0) then
          outKeys(cursor) = (base.toLong + index.toLong).toInt
          if ((flags & valueMask) != 0) then
            outSums(cursor) = sumsLocal(index)
            if (sparse) then Validity.setBit(words, cursor)
          else missing += 1
          cursor += 1
        index += 1
      }
      if (cursor != rows || missing != nullSums) then
        throw new IllegalStateException(
          s"dense sum counted $rows groups and $nullSums null sums, emitted $cursor and $missing",
        )
      publish(outKeys, outSums, words, sparse, rows)

    private def publish(
        outKeys: Array[Int],
        outSums: Array[Long],
        words: Array[Long],
        sparse: Boolean,
        rows: Int,
    ): ColumnBatch =
      val keyColumn = Int32Column.of(outKeys, Validity.allValid(rows), rows)
      /* A fresh bitmap. The key column's bitmap is not reused, so a later writer cannot clear
       * both. */
      val valueValidity =
        if (sparse) then Validity.fromWords(words, rows) else Validity.allValid(rows)
      val valueColumn = Int64Column.of(outSums, valueValidity, rows)
      new ColumnBatch(
        Vector(LogicalType.Int32, LogicalType.Int64),
        Vector(keyColumn, valueColumn),
        rows,
      )

  /** One pass over key presence and the key bounds. Null keys are skipped. */
  private def boundKeys(
      values: Array[Int],
      words: Array[Long],
      start: Int,
      end: Int,
      cap: Int,
  ): Fit =
    val limit = denseLimit(end - start)
    var row = start
    var seenKey = false
    var minKey = 0
    var maxKey = 0
    var wide = false
    while (row < end && !wide) {
      if ((row & 63) == 0 && row + 64 <= end && words(row >>> 6) == -1L) then
        val stop = row + 64
        while (row < stop && !wide) {
          val key = values(row)
          if (!seenKey) then
            seenKey = true
            minKey = key
            maxKey = key
          else if (key < minKey) then
            minKey = key
            wide = tooWide(minKey, maxKey, cap, limit)
          else if (key > maxKey) then
            maxKey = key
            wide = tooWide(minKey, maxKey, cap, limit)
          row += 1
        }
      else
        if (Validity.isSet(words, row)) then
          val key = values(row)
          if (!seenKey) then
            seenKey = true
            minKey = key
            maxKey = key
          else if (key < minKey) then
            minKey = key
            wide = tooWide(minKey, maxKey, cap, limit)
          else if (key > maxKey) then
            maxKey = key
            wide = tooWide(minKey, maxKey, cap, limit)
        row += 1
    }
    if (!seenKey) then Empty else if (wide) then Wide else window(minKey, maxKey, cap, limit)

  /** One pass over both presence bitmaps and the key bounds. */
  private def boundRows(
      values: Array[Int],
      keyWords: Array[Long],
      valueWords: Array[Long],
      start: Int,
      end: Int,
      cap: Int,
  ): Survey =
    val limit = denseLimit(end - start)
    var row = start
    var seenKey = false
    var keysPresent = true
    var valuesPresent = true
    var minKey = 0
    var maxKey = 0
    var wide = false
    while (row < end && !wide) {
      val aligned = (row & 63) == 0 && row + 64 <= end
      if (aligned && keyWords(row >>> 6) == -1L && valueWords(row >>> 6) == -1L) then
        val stop = row + 64
        while (row < stop && !wide) {
          val key = values(row)
          if (!seenKey) then
            seenKey = true
            minKey = key
            maxKey = key
          else if (key < minKey) then
            minKey = key
            wide = tooWide(minKey, maxKey, cap, limit)
          else if (key > maxKey) then
            maxKey = key
            wide = tooWide(minKey, maxKey, cap, limit)
          row += 1
        }
      else
        if (!Validity.isSet(keyWords, row)) then keysPresent = false
        else
          val key = values(row)
          if (!seenKey) then
            seenKey = true
            minKey = key
            maxKey = key
          else if (key < minKey) then
            minKey = key
            wide = tooWide(minKey, maxKey, cap, limit)
          else if (key > maxKey) then
            maxKey = key
            wide = tooWide(minKey, maxKey, cap, limit)
        if (!Validity.isSet(valueWords, row)) then valuesPresent = false
        row += 1
    }
    if (!seenKey) then Survey(Empty, columnsPresent = false)
    else if (wide) then Survey(Wide, columnsPresent = false)
    else Survey(window(minKey, maxKey, cap, limit), keysPresent && valuesPresent)

  /** `true` when the inclusive span misses the cap or the density limit. The span is a `Long`. */
  private def tooWide(minKey: Int, maxKey: Int, cap: Int, limit: Long): Boolean =
    val span = maxKey.toLong - minKey.toLong + 1L
    span > cap.toLong || span > limit

  private def denseLimit(length: Int): Long =
    DensityFactor.toLong * math.max(length, 1).toLong

  private def window(minKey: Int, maxKey: Int, cap: Int, limit: Long): Fit =
    val span = maxKey.toLong - minKey.toLong + 1L
    if (span <= cap.toLong && span <= limit && span <= Int.MaxValue.toLong) then
      Window(minKey, span.toInt)
    else Wide

  /** `key` used as an index. Rejects a negative key and a key at or past `limit`. */
  private inline def inPrefix(index: Int, limit: Int): Boolean =
    index >= 0 && index < limit

  /** Base-zero [[slot]]. The key is the index, so there is no subtraction to wrap. */
  private inline def prefixSlot(key: Int, limit: Int): Int =
    if (!inPrefix(key, limit)) then outside(key, 0, limit)
    key

  /**
   * Index of `key` in a window that starts at `base`. The difference is a `Long`, so
   * `Int.MinValue - Int.MaxValue` stays negative and is rejected instead of wrapping into range.
   */
  private inline def slot(key: Int, base: Int, limit: Int): Int =
    val index = key.toLong - base.toLong
    if (index < 0L || index >= limit.toLong) then outside(key, base, limit)
    index.toInt

  private def outside(key: Int, base: Int, limit: Int): Nothing =
    throw new IllegalStateException(
      s"key $key outside dense window starting at $base of length $limit",
    )

  private def emptySum: ColumnBatch =
    val keys = Int32Column.of(Array.emptyIntArray, Validity.allValid(0), 0)
    val values = Int64Column.of(Array.emptyLongArray, Validity.allValid(0), 0)
    new ColumnBatch(Vector(LogicalType.Int32, LogicalType.Int64), Vector(keys, values), 0)
