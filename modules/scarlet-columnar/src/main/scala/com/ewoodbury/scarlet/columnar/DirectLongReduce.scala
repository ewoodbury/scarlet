package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

/**
 * Dense partial reduce for long keys that sit in a small span.
 *
 * [[LongHashReduce]] probes an open-addressed table sized for the longest morsel. On a repeated
 * key that probe is an update, and the table is far larger than the key domain. When `max - min`
 * fits in [[SpanLimit]], the value is `values(key - min)`. First-seen order is the order a zero
 * slot is filled. A null, a short segment, or a wider span returns null and the caller keeps the
 * hash table. A segment shorter than [[MinRows]] stays on that table: the growth tests are pinned
 * there.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
private[scarlet] object DirectLongReduce:

  /** Below this length the hash table is already a few cache lines. */
  private val MinRows = 64

  /** `max - min` must be strictly below this. The value array is one longer. */
  private val SpanLimit = 4096L

  /**
   * Keys read to choose the domain. A later key outside that domain declines to the hash table.
   * 8192 is one default morsel, enough for `row % 1024` to show its whole residue set.
   */
  private val DomainPrefix = 8192

  private val peakSlots = new AtomicInteger
  private val hits = new AtomicInteger

  /** Successful direct partials since the process started. */
  def hitsForTest: Int = hits.get

  /** Largest direct value table since [[resetPeakForTest]], in slots. */
  def peakSlotsForTest: Int = peakSlots.get

  /** Clears [[peakSlotsForTest]] and [[hitsForTest]]. */
  def resetPeakForTest(): Unit =
    peakSlots.set(0)
    hits.set(0)

  /**
   * One partial for every range of `batch`, or null when the hash table should own the segment.
   * `dense` means both columns are fully present. The ranges are one source batch in scan order.
   */
  def aggregateRanges(
      batch: ColumnBatch,
      dense: Boolean,
      starts: Array[Int],
      lengths: Array[Int],
      steps: List[Selection.Step],
      op: LongHashReduce.LongBinOp,
  ): ColumnBatch | Null =
    require(starts.length == lengths.length, "reduce ranges disagree")
    if (!dense) then null
    else
      val total = rowCount(lengths)
      if (total < MinRows) then null
      else
        steps match
          case Selection.Step.Int64(0, keyKeep) :: Selection.Step.Int64(1, valueKeep) :: Nil =>
            reduce(batch, starts, lengths, keyKeep, valueKeep, op)
          case Selection.Step.Int64(ordinal, keep) :: Nil if ordinal == 0 || ordinal == 1 =>
            reduceOne(batch, starts, lengths, ordinal, keep, op)
          case Selection.Step.Int64(left, leftKeep) :: Selection.Step.Int64(right, rightKeep) ::
              Nil =>
            reduceTwo(batch, starts, lengths, left, leftKeep, right, rightKeep, op)
          case Selection.Step.Int64(_, _) :: _ | Selection.Step.Int32(_, _) :: _ |
              Selection.Step.Float64(_, _) :: _ | Selection.Step.Bool(_, _) :: _ |
              Selection.Step.Utf8(_, _) :: _ | Selection.Step.IntPair(_) :: _ |
              Selection.Step.LongPair(_) :: _ | Selection.Step.StringPair(_) :: _ |
              Selection.Step.IntTriple(_, _) :: _ | Nil =>
            null

  private def reduce(
      batch: ColumnBatch,
      starts: Array[Int],
      lengths: Array[Int],
      keyKeep: ColumnKernel.Int64Predicate,
      valueKeep: ColumnKernel.Int64Predicate,
      op: LongHashReduce.LongBinOp,
  ): ColumnBatch | Null =
    val keys = ColumnHash.int64At(batch, 0).values
    val values = ColumnHash.int64At(batch, 1).values
    domain(keys, starts, lengths) match
      case null => null
      case bounds: Bounds =>
        val buffer = borrow(bounds.span, op)
        buffer.prepare(bounds.min, bounds.span)
        notePeak(buffer.slotCount)
        val count = starts.length
        var cursor = 0
        var ok = true
        while (cursor < count && ok) do
          val length = lengths(cursor)
          if (length > 0) then
            ok = buffer.scanKeyValue(
              keys,
              values,
              starts(cursor),
              starts(cursor) + length,
              keyKeep,
              valueKeep,
            )
          cursor += 1
        if (ok) then buffer.compact() else null

  private def reduceOne(
      batch: ColumnBatch,
      starts: Array[Int],
      lengths: Array[Int],
      ordinal: Int,
      keep: ColumnKernel.Int64Predicate,
      op: LongHashReduce.LongBinOp,
  ): ColumnBatch | Null =
    val keys = ColumnHash.int64At(batch, 0).values
    val values = ColumnHash.int64At(batch, 1).values
    val tested = if (ordinal == 0) then keys else values
    domain(keys, starts, lengths) match
      case null => null
      case bounds: Bounds =>
        val buffer = borrow(bounds.span, op)
        buffer.prepare(bounds.min, bounds.span)
        notePeak(buffer.slotCount)
        val count = starts.length
        var cursor = 0
        var ok = true
        while (cursor < count && ok) do
          val length = lengths(cursor)
          if (length > 0) then
            ok = buffer.scanOne(
              keys,
              values,
              tested,
              starts(cursor),
              starts(cursor) + length,
              keep,
            )
          cursor += 1
        if (ok) then buffer.compact() else null

  private def reduceTwo(
      batch: ColumnBatch,
      starts: Array[Int],
      lengths: Array[Int],
      leftOrdinal: Int,
      leftKeep: ColumnKernel.Int64Predicate,
      rightOrdinal: Int,
      rightKeep: ColumnKernel.Int64Predicate,
      op: LongHashReduce.LongBinOp,
  ): ColumnBatch | Null =
    val keys = ColumnHash.int64At(batch, 0).values
    val values = ColumnHash.int64At(batch, 1).values
    val left = if (leftOrdinal == 0) then keys else values
    val right = if (rightOrdinal == 0) then keys else values
    domain(keys, starts, lengths) match
      case null => null
      case bounds: Bounds =>
        val buffer = borrow(bounds.span, op)
        buffer.prepare(bounds.min, bounds.span)
        notePeak(buffer.slotCount)
        val count = starts.length
        var cursor = 0
        var ok = true
        while (cursor < count && ok) do
          val length = lengths(cursor)
          if (length > 0) then
            ok = buffer.scanTwo(
              keys,
              values,
              left,
              right,
              starts(cursor),
              starts(cursor) + length,
              leftKeep,
              rightKeep,
            )
          cursor += 1
        if (ok) then buffer.compact() else null

  private def rowCount(lengths: Array[Int]): Int =
    var cursor = 0
    var total = 0
    while (cursor < lengths.length) do
      total += lengths(cursor)
      cursor += 1
    total

  /**
   * Inclusive key domain of a prefix of the ranges, or null when that prefix is empty or at least
   * [[SpanLimit]] wide. Only [[DomainPrefix]] keys are read. A wrapped `max - min` is too wide.
   * The fill declines if a later key falls outside this domain.
   */
  private def domain(keys: Array[Long], starts: Array[Int], lengths: Array[Int]): Bounds | Null =
    val count = starts.length
    var cursor = 0
    var seenRows = 0
    var min = 0L
    var max = 0L
    var seen = false
    var fits = true
    while (cursor < count && fits && seenRows < DomainPrefix) do
      val end = starts(cursor) + lengths(cursor)
      var row = starts(cursor)
      while (row < end && fits && seenRows < DomainPrefix) do
        val key = keys(row)
        if (!seen) then
          min = key
          max = key
          seen = true
        else
          if (key < min) then min = key
          else if (key > max) then max = key
          val width = max - min
          if (width < 0L || width >= SpanLimit) then fits = false
        if (fits) then seenRows += 1
        row += 1
      cursor += 1
    if (!fits || !seen) then null
    else Bounds(min, (max - min).toInt + 1)

  private final class Bounds(val min: Long, val span: Int)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def notePeak(slots: Int): Unit =
    var prev = peakSlots.get
    while (slots > prev && !peakSlots.compareAndSet(prev, slots)) do prev = peakSlots.get

  private val locals: ThreadLocal[Buffer | Null] = new ThreadLocal[Buffer | Null]()

  private def borrow(span: Int, op: LongHashReduce.LongBinOp): Buffer =
    locals.get() match
      case current: Buffer if current.accepts(span, op) =>
        current.clear()
        current
      case _ =>
        val created = new Buffer(span, op)
        locals.set(created)
        created

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Equals"))
  private final class Buffer(initial: Int, val op: LongHashReduce.LongBinOp):
    private var values: Array[Long] = new Array[Long](initial)
    private var state: Array[Byte] = new Array[Byte](initial)
    private var order: Array[Long] = new Array[Long](initial)
    private var occupied: Int = 0
    private var min: Long = 0L

    def accepts(span: Int, bin: LongHashReduce.LongBinOp): Boolean =
      values.length >= span && (op eq bin)

    def slotCount: Int = values.length

    def clear(): Unit =
      var cursor = 0
      val rows = occupied
      val base = min
      val seen = state
      val keys = order
      while (cursor < rows) do
        seen((keys(cursor) - base).toInt) = 0
        cursor += 1
      occupied = 0

    private var spanLimit: Long = 0L

    def prepare(origin: Long, span: Int): Unit =
      if (values.length < span) then
        values = new Array[Long](span)
        state = new Array[Byte](span)
        order = new Array[Long](span)
        occupied = 0
      min = origin
      spanLimit = span.toLong

    /** False when a key falls outside the prefix domain. Inserts so far stay in this buffer. */
    def scanKeyValue(
        keys: Array[Long],
        inputs: Array[Long],
        start: Int,
        end: Int,
        keyKeep: ColumnKernel.Int64Predicate,
        valueKeep: ColumnKernel.Int64Predicate,
    ): Boolean =
      var row = start
      val origin = min
      val limit = spanLimit
      val sums = values
      val seen = state
      val keysSeen = order
      var ok = true
      while (row < end && ok) do
        val key = keys(row)
        val delta = key - origin
        if (delta < 0L || delta >= limit) then ok = false
        else if (keyKeep(key)) then
          val value = inputs(row)
          if (valueKeep(value)) then add(key, value, origin, sums, seen, keysSeen)
        row += 1
      ok

    def scanOne(
        keys: Array[Long],
        inputs: Array[Long],
        tested: Array[Long],
        start: Int,
        end: Int,
        keep: ColumnKernel.Int64Predicate,
    ): Boolean =
      var row = start
      val origin = min
      val limit = spanLimit
      val sums = values
      val seen = state
      val keysSeen = order
      var ok = true
      while (row < end && ok) do
        val key = keys(row)
        val delta = key - origin
        if (delta < 0L || delta >= limit) then ok = false
        else if (keep(tested(row))) then add(key, inputs(row), origin, sums, seen, keysSeen)
        row += 1
      ok

    def scanTwo(
        keys: Array[Long],
        inputs: Array[Long],
        left: Array[Long],
        right: Array[Long],
        start: Int,
        end: Int,
        leftKeep: ColumnKernel.Int64Predicate,
        rightKeep: ColumnKernel.Int64Predicate,
    ): Boolean =
      var row = start
      val origin = min
      val limit = spanLimit
      val sums = values
      val seen = state
      val keysSeen = order
      var ok = true
      while (row < end && ok) do
        val key = keys(row)
        val delta = key - origin
        if (delta < 0L || delta >= limit) then ok = false
        else if (leftKeep(left(row)) && rightKeep(right(row))) then
          add(key, inputs(row), origin, sums, seen, keysSeen)
        row += 1
      ok

    def compact(): ColumnBatch =
      val _ = hits.incrementAndGet()
      val rows = occupied
      if (rows == 0) then ColumnBatches.emptyLongPairs
      else
        val outKeys = new Array[Long](rows)
        val outValues = new Array[Long](rows)
        val base = min
        val sums = values
        var cursor = 0
        while (cursor < rows) do
          val key = order(cursor)
          outKeys(cursor) = key
          outValues(cursor) = sums((key - base).toInt)
          cursor += 1
        val present = Validity.allValid(rows)
        new ColumnBatch(
          Vector(LogicalType.Int64, LogicalType.Int64),
          Vector(
            Int64Column.of(outKeys, present, rows),
            Int64Column.of(outValues, present, rows),
          ),
          rows,
        )

    private def add(
        key: Long,
        value: Long,
        origin: Long,
        sums: Array[Long],
        seen: Array[Byte],
        keysSeen: Array[Long],
    ): Unit =
      val index = (key - origin).toInt
      if (seen(index) == 0) then
        seen(index) = 1
        sums(index) = value
        keysSeen(occupied) = key
        occupied += 1
      else sums(index) = op(sums(index), value)
