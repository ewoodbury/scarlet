package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

/**
 * Dense partial count for a morsel whose keys sit in a small span.
 *
 * The open-addressed table probes one slot per survivor. On this input each key is repeated, the
 * probe almost never chains, and the table is sized for the row count rather than the key domain.
 * When `max - min` fits in [[SpanLimit]], the counter is `counts(key - min)`. First-seen order is
 * the order the scan first increments a zero slot, which is the hash table's insertion order. A
 * null, a short morsel, or a wider span returns null and the caller keeps the hash table. A morsel
 * shorter than [[MinRows]] stays on that table: the reuse contract is pinned there, and the range
 * scan would not pay for itself.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
private[columnar] object DirectCount:

  /** Below this length the hash table is already a few cache lines. */
  private val MinRows = 64

  /** `max - min` must be strictly below this. The counter array is one longer. */
  private val SpanLimit = 4096

  def release(): Unit = locals.remove()

  /** Largest dense counter since [[resetPeakForTest]], in slots. */
  def peakSlotsForTest: Int = peakSlots.get

  /** Clears [[peakSlotsForTest]]. */
  def resetPeakForTest(): Unit =
    peakSlots.set(0)

  /**
   * One partial, or null when this morsel stays on the hash table. The returned batch is
   * independent of the reused counter.
   */
  def aggregate(morsel: Morsel, steps: List[Selection.Step]): ColumnBatch | Null =
    if (morsel.length < MinRows) then null
    else
      steps match
        case Selection.Step.Int32(left, leftKeep) :: Selection.Step.Int32(
              right,
              rightKeep,
            ) :: Nil =>
          two(morsel, left, leftKeep, right, rightKeep)
        case _ =>
          null

  /**
   * One partial for every range of `batch`, or null when the hash table should own the segment.
   *
   * The ranges are one source batch in scan order. The counter is `counts(key - min)` and its
   * length is the key span, not the row count. A span at or above [[SpanLimit]], a segment shorter
   * than [[MinRows]], a null in a column the steps read, or any step list other than two int slots
   * returns null.
   */
  def aggregateRanges(
      batch: ColumnBatch,
      starts: Array[Int],
      lengths: Array[Int],
      steps: List[Selection.Step],
  ): ColumnBatch | Null =
    require(starts.length == lengths.length, "count ranges disagree")
    steps match
      case Selection.Step.Int32(left, leftKeep) :: Selection.Step.Int32(right, rightKeep) :: Nil =>
        ranges(batch, starts, lengths, left, leftKeep, right, rightKeep)
      case _ =>
        null

  private def ranges(
      batch: ColumnBatch,
      starts: Array[Int],
      lengths: Array[Int],
      leftOrdinal: Int,
      leftKeep: ColumnKernel.Int32Predicate,
      rightOrdinal: Int,
      rightKeep: ColumnKernel.Int32Predicate,
  ): ColumnBatch | Null =
    val count = starts.length
    var total = 0
    var cursor = 0
    var ready = true
    while (cursor < count && ready) {
      val length = lengths(cursor)
      if (length > 0) then
        total += length
        val start = starts(cursor)
        ready = columnReady(batch, 0, start, length) &&
          columnReady(batch, leftOrdinal, start, length) &&
          columnReady(batch, rightOrdinal, start, length)
      cursor += 1
    }
    if (!ready || total < MinRows) then null
    else
      val keys = ColumnHash.int32At(batch, 0).values
      val values =
        if ((leftOrdinal == 1) || (rightOrdinal == 1)) then ColumnHash.int32At(batch, 1).values
        else keys
      var min = 0
      var max = 0
      var seen = false
      var fits = true
      cursor = 0
      while (cursor < count && fits) {
        val end = starts(cursor) + lengths(cursor)
        var row = starts(cursor)
        if (!seen && row < end) then
          min = keys(row)
          max = min
          seen = true
          row += 1
        while (row < end && fits) {
          val key = keys(row)
          if (key < min) then min = key
          else if (key > max) then max = key
          if (max.toLong - min.toLong >= SpanLimit.toLong) then fits = false
          row += 1
        }
        cursor += 1
      }
      if (!fits || !seen) then null
      else
        val span = (max - min) + 1
        val buffer = borrow(span)
        buffer.clear()
        buffer.prepare(min, span)
        notePeak(buffer.slotCount)
        val left = if (leftOrdinal == 0) then keys else values
        val right = if (rightOrdinal == 0) then keys else values
        val keepLeft = function(leftKeep)
        val keepRight = function(rightKeep)
        cursor = 0
        while (cursor < count) {
          val end = starts(cursor) + lengths(cursor)
          var row = starts(cursor)
          while (row < end) {
            if (keepLeft(left(row)) && keepRight(right(row))) then buffer.add(keys(row))
            row += 1
          }
          cursor += 1
        }
        buffer.compact()

  private def two(
      morsel: Morsel,
      leftOrdinal: Int,
      leftKeep: ColumnKernel.Int32Predicate,
      rightOrdinal: Int,
      rightKeep: ColumnKernel.Int32Predicate,
  ): ColumnBatch | Null =
    val batch = morsel.batch
    if (
        !columnReady(batch, 0, morsel.start, morsel.length) ||
        !columnReady(batch, leftOrdinal, morsel.start, morsel.length) ||
        !columnReady(batch, rightOrdinal, morsel.start, morsel.length)
      )
    then null
    else
      val keys = ColumnHash.int32At(batch, 0).values
      val values =
        if ((leftOrdinal == 1) || (rightOrdinal == 1)) then ColumnHash.int32At(batch, 1).values
        else keys
      fill(
        morsel,
        keys,
        values,
        leftOrdinal,
        function(leftKeep),
        rightOrdinal,
        function(rightKeep),
      )

  private def columnReady(batch: ColumnBatch, ordinal: Int, start: Int, length: Int): Boolean =
    ordinal >= 0 && ordinal < batch.columns.length &&
      Validity.rangePresent(ColumnHash.int32At(batch, ordinal).validity, start, length)

  private def function(predicate: ColumnKernel.Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        value => fallback(value)

  private def fill(
      morsel: Morsel,
      keys: Array[Int],
      values: Array[Int],
      leftOrdinal: Int,
      keepLeft: Int => Boolean,
      rightOrdinal: Int,
      keepRight: Int => Boolean,
  ): ColumnBatch | Null =
    val start = morsel.start
    val end = start + morsel.length
    if (sampledWide(keys, start, morsel.length)) then null
    else
      var min = keys(start)
      var max = keys(start)
      var row = start + 1
      var fits = true
      while (row < end && fits) {
        val key = keys(row)
        if (key < min) then min = key
        else if (key > max) then max = key
        if (max.toLong - min.toLong >= SpanLimit.toLong) then fits = false
        row += 1
      }
      if (!fits) then null
      else
        val span = (max - min) + 1
        val buffer = borrow(span)
        buffer.clear()
        buffer.prepare(min, span)
        notePeak(buffer.slotCount)
        val left = if (leftOrdinal == 0) then keys else values
        val right = if (rightOrdinal == 0) then keys else values
        row = start
        while (row < end) {
          if (keepLeft(left(row)) && keepRight(right(row))) then buffer.add(keys(row))
          row += 1
        }
        buffer.compact()

  private def sampledWide(keys: Array[Int], start: Int, length: Int): Boolean =
    val first = keys(start)
    val middle = keys(start + (length >>> 1))
    val last = keys(start + length - 1)
    var min = first
    var max = first
    if (middle < min) then min = middle
    if (middle > max) then max = middle
    if (last < min) then min = last
    if (last > max) then max = last
    max.toLong - min.toLong >= SpanLimit.toLong

  private val locals: ThreadLocal[Dense | Null] = new ThreadLocal[Dense | Null]()
  private val peakSlots = new AtomicInteger

  private def notePeak(slots: Int): Unit =
    var prev = peakSlots.get
    while (slots > prev && !peakSlots.compareAndSet(prev, slots)) do prev = peakSlots.get

  private def borrow(span: Int): Dense =
    locals.get() match
      case current: Dense if current.accepts(span) => current
      case _ =>
        val created = new Dense(span)
        locals.set(created)
        created

  private final class Dense(capacity: Int):
    private var counts: Array[Int] = new Array[Int](capacity)
    private var order: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0
    private var min: Int = 0

    def accepts(span: Int): Boolean = counts.length >= span

    def slotCount: Int = counts.length

    def clear(): Unit =
      var cursor = 0
      val rows = occupied
      while (cursor < rows) {
        counts(order(cursor) - min) = 0
        cursor += 1
      }
      occupied = 0

    def prepare(low: Int, span: Int): Unit =
      if (counts.length < span) then
        counts = new Array[Int](span)
        order = new Array[Int](span)
        occupied = 0
      min = low

    def add(key: Int): Unit =
      val index = key - min
      val current = counts(index)
      if (current == 0) then
        order(occupied) = key
        occupied += 1
        counts(index) = 1
      else counts(index) = current + 1

    def compact(): ColumnBatch =
      val rows = occupied
      if (rows == 0) then ColumnBatches.emptyIntPairs
      else
        val outKeys = new Array[Int](rows)
        val outCounts = new Array[Int](rows)
        var cursor = 0
        while (cursor < rows) {
          val key = order(cursor)
          outKeys(cursor) = key
          outCounts(cursor) = counts(key - min)
          cursor += 1
        }
        val present = Validity.allValid(rows)
        new ColumnBatch(
          Vector(LogicalType.Int32, LogicalType.Int32),
          Vector(Int32Column.of(outKeys, present, rows), Int32Column.of(outCounts, present, rows)),
          rows,
        )
