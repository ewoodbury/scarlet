package com.ewoodbury.scarlet.columnar

/**
 * Direct-index grouped sum for a bounded `Int` key range.
 *
 * One slot is 8 bytes of sum, one flag byte, and 4 bytes in the touched-key list: 13 bytes. The
 * flag holds two bits. The group bit means the key was seen. The value bit means a value was
 * added. A null value sets the group bit and leaves the sum null. A filtered sum sets both bits
 * only when a value is kept, so a key whose rows are all dropped stays absent. The group bit keeps
 * that null sum in the output. The value bit marks the sum present. Those are different results,
 * so the flag needs both bits.
 *
 * `cap` is the largest range that stays inside a 4 MiB budget across the workers:
 * `parallelism * range * 13 <= 4 * 1024 * 1024`. At 4 workers a range of 65536 is 3.25 MiB, so it
 * is under the cap. The cap is that budget divided by `parallelism * 13`.
 *
 * The span is `max.toLong - min.toLong + 1`. An `Int` subtraction overflows on `Int.MinValue` to
 * `Int.MaxValue` and must not be used. Null keys are skipped, the same rows accumulation skips.
 *
 * Keys come out in ascending order. The hash path still emits first-seen order. Grouped sums are
 * compared as maps. There is no first-touch order array.
 *
 * The arrays live on [[HashAggregate]]'s per-worker holder. [[Accumulator.prepare]] clears the
 * previous range by walking the touched indexes, and allocates again only when `min` or the range
 * changes. [[HashAggregate.releaseSumWorkers]] drops the holder. Each published batch gets a new
 * key array and a new validity bitmap. A worker may keep a private copy of the zero-origin keys
 * and copy that into the batch. The private copy is not published.
 *
 * A non-negative key in `[0, directLimit)` skips the min/max pass and is indexed by the key.
 * `directLimit` is `min(cap, 65536)`. A key outside that prefix is rejected before any slot is
 * written, and the morsel is classified. A bounds failure after that check is a bug. A window that
 * exceeds `cap`, including `Int.MinValue` together with `Int.MaxValue`, uses the hash table.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object DenseSum:

  /** Sum long, flag byte, and one touched-list int. */
  val BytesPerSlot: Long = 13L

  /** `parallelism * range *` [[BytesPerSlot]] stays at or under this many bytes. */
  val BudgetBytes: Long = 4L * 1024L * 1024L

  /**
   * Prefix indexed by the key itself. `4 * 65536 * 13` is 3.25 MiB, inside [[BudgetBytes]] at four
   * workers. [[directLimit]] never returns more than `cap`.
   */
  val DirectPrefix: Int = 65536

  def directLimit(cap: Int): Int =
    math.min(cap, DirectPrefix)

  private val GroupMask: Int = 1
  private val ValueMask: Int = 2
  private val GroupBit: Byte = 1
  private val BothBits: Byte = 3

  sealed trait Fit

  /** No present key in the scanned rows. The sum emits nothing. */
  case object Empty extends Fit

  /** The present keys do not fit in `cap`. The caller uses the hash table. */
  case object Wide extends Fit

  /** Present keys lie in `[min, min + range)`. `range` is at least 1 and at most `cap`. */
  final case class Window(min: Int, range: Int) extends Fit

  /**
   * Largest range whose tables fit in [[BudgetBytes]] when each of `parallelism` workers holds
   * one. A non-positive parallelism is rejected.
   */
  def cap(parallelism: Int): Int =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    // The budget is 4 MiB and a slot is 13 bytes, so the quotient fits in an Int.
    (BudgetBytes / (BytesPerSlot * parallelism.toLong)).toInt

  /**
   * `Empty` when `[start, start + length)` has no present key. `Window` when the present keys fit
   * in `cap`. `Wide` otherwise. The decision reads only that row range.
   */
  def classify(keys: Int32Column, start: Int, length: Int, cap: Int): Fit =
    require(
      start >= 0 && length >= 0 && start <= keys.length && length <= keys.length - start,
      s"range ($start, $length) outside ${keys.length} rows",
    )
    if (length == 0) then Empty
    else
      val end = start + length
      if (Validity.rangePresent(keys.validity, start, length)) then
        presentWindow(keys.values, start, end, cap)
      else classifySparse(keys, start, end, cap)

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

    /** Allocates when `min` or `range` changed. Otherwise clears only the touched slots. */
    def prepare(min: Int, range: Int): Unit =
      require(range > 0, s"dense range must be positive, got $range")
      if (min != minKey || range != keyRange) then allocate(min, range) else reset()

    /**
     * Table for keys in `[0, limit)`. The next call clears only the touched slots when `limit`
     * matches.
     */
    def prepareDirect(limit: Int): Unit =
      require(limit > 0, s"direct limit must be positive, got $limit")
      if (minKey != 0 || keyRange != limit) then allocate(0, limit) else reset()

    /**
     * Sums present rows by using the key as the index. Returns `false` when a key is outside
     * `[0, sums.length)`, before any slot is written. [[abandonDirect]] clears a partial scan.
     * `minSeen` and `maxSeen` cover every recorded key. A bounds failure after the pre-scan is a
     * bug, not a rejected key.
     */
    def addDirect(keys: Int32Column, values: Int32Column, start: Int, end: Int): Boolean =
      val limit = sums.length
      if (!fitsPrefix(keys.values, start, end, limit)) then false
      else
        sumPrefix(keys, values, start, end)
        true

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

    /** Keys are in range. Records a new slot before adding its value. */
    private def sumPrefix(keys: Int32Column, values: Int32Column, start: Int, end: Int): Unit =
      val keyValues = keys.values
      val valueValues = values.values
      val sumsLocal = sums
      val seenLocal = seen
      val touchedLocal = touched
      val both = BothBits
      minSeen = Int.MaxValue
      maxSeen = Int.MinValue
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
        if (seenLocal(i0) == 0) then note(seenLocal, touchedLocal, i0, both)
        sumsLocal(i0) += valueValues(row).toLong
        if (seenLocal(i1) == 0) then note(seenLocal, touchedLocal, i1, both)
        sumsLocal(i1) += valueValues(row + 1).toLong
        if (seenLocal(i2) == 0) then note(seenLocal, touchedLocal, i2, both)
        sumsLocal(i2) += valueValues(row + 2).toLong
        if (seenLocal(i3) == 0) then note(seenLocal, touchedLocal, i3, both)
        sumsLocal(i3) += valueValues(row + 3).toLong
        if (seenLocal(i4) == 0) then note(seenLocal, touchedLocal, i4, both)
        sumsLocal(i4) += valueValues(row + 4).toLong
        if (seenLocal(i5) == 0) then note(seenLocal, touchedLocal, i5, both)
        sumsLocal(i5) += valueValues(row + 5).toLong
        if (seenLocal(i6) == 0) then note(seenLocal, touchedLocal, i6, both)
        sumsLocal(i6) += valueValues(row + 6).toLong
        if (seenLocal(i7) == 0) then note(seenLocal, touchedLocal, i7, both)
        sumsLocal(i7) += valueValues(row + 7).toLong
        row += 8
      }
      while (row < end) {
        val index = keyValues(row)
        if (seenLocal(index) == 0) then note(seenLocal, touchedLocal, index, both)
        sumsLocal(index) += valueValues(row).toLong
        row += 1
      }

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

    /** Ascending keys in `[minSeen, maxSeen]`. Used after a successful [[addDirect]]. */
    def compactDirect(): ColumnBatch =
      val rows = touchedCount
      if (rows == 0) then emptySum
      else
        val span = maxSeen - minSeen + 1
        if (rows == span) then compactSpan(minSeen, span)
        else compactWindow(minSeen, maxSeen, rows)

    /** Clears every slot a rejected direct scan recorded, so the next range starts empty. */
    def abandonDirect(): Unit = reset()

    def scanPlain(keys: Int32Column, values: Int32Column, start: Int, end: Int): Unit =
      val length = end - start
      if (
          Validity.rangePresent(keys.validity, start, length) &&
          Validity.rangePresent(values.validity, start, length)
        )
      then scanPlainPresent(keys, values, start, end)
      else scanPlainSparse(keys, values, start, end)

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
      val rows = touchedCount
      if (rows == 0) then emptySum
      else if (nullSums == 0 && rows == keyRange) then compactFull(rows)
      else compactPartial(rows)

    private def allocate(min: Int, range: Int): Unit =
      minKey = min
      keyRange = range
      sums = new Array[Long](range)
      seen = new Array[Byte](range)
      touched = new Array[Int](range)
      touchedCount = 0
      nullSums = 0
      minSeen = 0
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
      minSeen = 0
      maxSeen = -1

    /**
     * Every row in `[start, end)` is present. The index is checked with a long difference, and it
     * must sit in `[0, limit)` before the slot is updated. A key outside the window fails the
     * scan. The touched index is recorded before the next key.
     */
    private[columnar] def scanPlainPresent(
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
      val base = minKey
      val limit = sumsLocal.length
      if (
          limit != keyRange || seenLocal.length != limit || touchedLocal.length != limit ||
          start < 0 || end > keyValues.length || end > valueValues.length
        )
      then outside(base, base, keyRange)
      val both = BothBits
      var count = touchedCount
      var row = start
      if (base == 0) then
        while (row < end) {
          val index = keyValues(row)
          if (!inPrefix(index, limit)) then outside(index, 0, limit)
          if (seenLocal(index) == 0) then
            seenLocal(index) = both
            touchedLocal(count) = index
            count += 1
            touchedCount = count
          sumsLocal(index) += valueValues(row).toLong
          row += 1
        }
      else
        while (row < end) {
          val index = slot(keyValues(row), base, limit)
          if (seenLocal(index) == 0) then
            seenLocal(index) = both
            touchedLocal(count) = index
            count += 1
            touchedCount = count
          sumsLocal(index) += valueValues(row).toLong
          row += 1
        }
      touchedCount = count

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

    /** Keys `0, 1, ..., span - 1`. Stays on this worker. Callers copy it before publication. */
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

    private def compactWindow(low: Int, high: Int, rows: Int): ColumnBatch =
      val outKeys = new Array[Int](rows)
      val outSums = new Array[Long](rows)
      val seenLocal = seen
      val sumsLocal = sums
      var cursor = 0
      var index = low
      while (index <= high) {
        if (seenLocal(index) != 0) then
          outKeys(cursor) = index
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
      val keyValidity = Validity.allValid(rows)
      val keyColumn = Int32Column.of(outKeys, keyValidity, rows)
      val valueValidity = if (sparse) then Validity.fromWords(words, rows) else keyValidity
      val valueColumn = Int64Column.of(outSums, valueValidity, rows)
      new ColumnBatch(
        Vector(LogicalType.Int32, LogicalType.Int64),
        Vector(keyColumn, valueColumn),
        rows,
      )

  private def presentWindow(values: Array[Int], start: Int, end: Int, cap: Int): Fit =
    var minKey = values(start)
    var maxKey = minKey
    if (exceeds(minKey, maxKey, cap)) then Wide
    else
      var row = start + 1
      var wide = false
      while (row < end && !wide) {
        val key = values(row)
        if (key < minKey) then
          minKey = key
          wide = exceeds(minKey, maxKey, cap)
        else if (key > maxKey) then
          maxKey = key
          wide = exceeds(minKey, maxKey, cap)
        row += 1
      }
      if (wide) then Wide else window(minKey, maxKey, cap)

  private def classifySparse(keys: Int32Column, start: Int, end: Int, cap: Int): Fit =
    val values = keys.values
    val words = keys.validity.words
    var row = start
    var seenKey = false
    var minKey = 0
    var maxKey = 0
    var wide = false
    while (row < end && !wide) {
      if (Validity.isSet(words, row)) then
        val key = values(row)
        if (!seenKey) then
          seenKey = true
          minKey = key
          maxKey = key
          wide = exceeds(minKey, maxKey, cap)
        else if (key < minKey) then
          minKey = key
          wide = exceeds(minKey, maxKey, cap)
        else if (key > maxKey) then
          maxKey = key
          wide = exceeds(minKey, maxKey, cap)
      row += 1
    }
    if (!seenKey) then Empty else if (wide) then Wide else window(minKey, maxKey, cap)

  /** `true` when the inclusive span does not fit in `cap`. The span is a `Long`. */
  private def exceeds(minKey: Int, maxKey: Int, cap: Int): Boolean =
    maxKey.toLong - minKey.toLong + 1L > cap.toLong

  private def window(minKey: Int, maxKey: Int, cap: Int): Fit =
    val span = maxKey.toLong - minKey.toLong + 1L
    if (span <= cap.toLong && span <= Int.MaxValue.toLong) then Window(minKey, span.toInt)
    else Wide

  /**
   * Present keys of a column the caller already proved has no nulls in `[start, start + length)`.
   */
  private[columnar] def classifyPresent(
      keys: Int32Column,
      start: Int,
      length: Int,
      cap: Int,
  ): Fit =
    require(
      start >= 0 && length >= 0 && start <= keys.length && length <= keys.length - start,
      s"range ($start, $length) outside ${keys.length} rows",
    )
    if (length == 0) then Empty
    else presentWindow(keys.values, start, start + length, cap)

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
