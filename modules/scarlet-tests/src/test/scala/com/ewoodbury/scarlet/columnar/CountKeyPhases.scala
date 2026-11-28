package com.ewoodbury.scarlet.columnar
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.support.Check

/**
 * Component timings for the folded int `countByKey`. Not a test.
 *
 * Each method is one full pass. A pass is not subtracted from another, and a parallel pass
 * includes its morsel dispatch. [[scan]], [[predicate]], and [[insert]] are the row work.
 * [[merge]] exchanges partials that were built once, outside the timer. [[output]] decodes a
 * finished result that was also built once. [[direct]] is the same pipeline with a dense counter
 * per key when the morsel's key span fits in [[SpanLimit]] slots.
 *
 * The dense counter is the streaming aggregate for a tiny repeated domain: the open-addressed
 * table probes one slot per survivor, and a span that fits is `counts(key - min) += 1` in
 * first-seen order. A wider span keeps the hash table. Null keys are not on this path.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Var",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Throw",
  ),
)
private[scarlet] object CountKeyPhases:

  /** Largest key span, inclusive of both ends, that uses a dense counter. */
  val SpanLimit: Int = 4096

  /** Open-addressed table sized for 1024 groups, which is `tableSize(1024)`. */
  private val fitRows = 1024

  final class Work(
      val morsels: Vector[Morsel],
      val scanChecksum: Long,
      val mixChecksum: Long,
      val survivors: Array[Array[Int]],
      val survivorRows: Array[Int],
      val partialSizes: Vector[Int],
      val partials: Vector[ColumnBatch],
      val finished: Vector[ColumnBatch],
      val directSample: Vector[ColumnBatch],
      val extraProbes: Long,
      val inserted: Long,
      val predicateClass: String,
      val directClass: String,
      val steps: List[Selection.Step],
      val numPartitions: Int,
      val batchSize: Int,
  )

  def prepare(
      batches: Vector[ColumnBatch],
      morsels: Vector[Morsel],
      steps: List[Selection.Step],
      numPartitions: Int,
      batchSize: Int,
  ): Work =
    val captured = capture(steps)
    val scanChecksum = scanSerial(morsels)
    val survivors = new Array[Array[Int]](morsels.length)
    val survivorRows = new Array[Int](morsels.length)
    val survivorWalk = morsels.iterator
    var index = 0
    while (survivorWalk.hasNext) {
      val morsel = survivorWalk.next()
      val buffer = new Array[Int](morsel.length)
      val rows = collect(morsel, captured, buffer)
      survivors(index) = buffer
      survivorRows(index) = rows
      index += 1
    }
    val mixChecksum = mixSerial(morsels, survivors, survivorRows)
    val sizes = batches.map(batch => Morsels.of(batch, batchSize).length)
    val partials = morsels.map(morsel => hashPartial(morsel, captured, small = false))
    val finished = combine(sizes, partials, numPartitions, 1, batchSize)
    val directSample = direct(morsels, steps, numPartitions, batchSize, 1)
    val probes = extraSerial(morsels, survivors, survivorRows)
    var inserted = 0L
    index = 0
    while (index < survivorRows.length) {
      inserted += survivorRows(index).toLong
      index += 1
    }
    Work(
      morsels,
      scanChecksum,
      mixChecksum,
      survivors,
      survivorRows,
      sizes,
      partials,
      finished,
      directSample,
      probes,
      inserted,
      captured.leftClass,
      captured.rightClass,
      steps,
      numPartitions,
      batchSize,
    )

  def scan(work: Work, threads: Int): Long =
    val parts = MorselScheduler.map(work.morsels, threads)((_, morsel) => scanOne(morsel))
    parts.foldLeft(0L)(_ ^ _)

  def predicate(work: Work, threads: Int, directCall: Boolean): Long =
    val captured = capture(work.steps)
    val parts = MorselScheduler.map(work.morsels, threads) { (_, morsel) =>
      predicateOne(morsel, captured, directCall)
    }
    parts.foldLeft(0L)(_ + _)

  def mix(work: Work, threads: Int): Long =
    val parts = MorselScheduler.map(work.morsels, threads) { (index, _) =>
      mixOne(work.survivors(index), work.survivorRows(index))
    }
    parts.foldLeft(0L)(_ ^ _)

  /**
   * Hash-insert the survivors. `fit` sizes the table for [[fitRows]] groups instead of the morsel.
   */
  def insert(work: Work, threads: Int, fit: Boolean): Long =
    val parts = MorselScheduler.map(work.morsels, threads) { (index, morsel) =>
      val rows = if (fit) then fitRows else morsel.length
      val table = borrow(rows)
      table.clear()
      val keys = work.survivors(index)
      val live = work.survivorRows(index)
      var cursor = 0
      while (cursor < live) {
        table.add(keys(cursor), 1)
        cursor += 1
      }
      table.total.toLong
    }
    parts.foldLeft(0L)(_ + _)

  def merge(work: Work, threads: Int): Long =
    val combined = combine(
      work.partialSizes,
      work.partials,
      work.numPartitions,
      threads,
      work.batchSize,
    )
    cross(combined)

  def output(work: Work): Long = cross(work.finished)

  def direct(
      morsels: Vector[Morsel],
      steps: List[Selection.Step],
      numPartitions: Int,
      batchSize: Int,
      threads: Int,
  ): Vector[ColumnBatch] =
    val captured = capture(steps)
    val sizes = groupSizes(morsels)
    val partials = MorselScheduler.map(morsels, threads) { (_, morsel) =>
      directPartial(morsel, captured)
    }
    combine(sizes, partials, numPartitions, threads, batchSize)

  private def groupSizes(morsels: Vector[Morsel]): Vector[Int] =
    val sizes = new Array[Int](morsels.length)
    var groups = 0
    var count = 0
    var current: ColumnBatch | Null = null
    val grouped = morsels.iterator
    while (grouped.hasNext) {
      val batch = grouped.next().batch
      if ((Check.isNull(current)) || (Check.ref(current, batch))) then
        current = batch
        count += 1
      else
        sizes(groups) = count
        groups += 1
        current = batch
        count = 1
    }
    if (count > 0) then
      sizes(groups) = count
      groups += 1
    sizes.iterator.take(groups).toVector

  def cross(batches: Vector[ColumnBatch]): Long =
    batches.foldLeft(0L) { (sum, batch) =>
      val keys = ColumnHash.int32At(batch, 0)
      val counts = ColumnHash.int32At(batch, 1)
      var row = 0
      var acc = sum
      val length = batch.length
      while (row < length) {
        acc += keys.values(row).toLong * 31L + counts.values(row).toLong
        row += 1
      }
      acc
    }

  /** Order-sensitive fingerprint of keys and counts, so a first-seen drift fails closed. */
  def fingerprint(batches: Vector[ColumnBatch]): Long =
    var hash = 1L
    batches.foreach { batch =>
      val keys = ColumnHash.int32At(batch, 0)
      val counts = ColumnHash.int32At(batch, 1)
      var row = 0
      val length = batch.length
      while (row < length) {
        hash = hash * 1315423911L + keys.values(row).toLong
        hash = hash * 1315423911L + counts.values(row).toLong
        row += 1
      }
    }
    hash

  private final class Captured(
      val leftOrdinal: Int,
      val rightOrdinal: Int,
      val leftApply: ColumnKernel.Int32Predicate,
      val rightApply: ColumnKernel.Int32Predicate,
      val leftCall: Int => Boolean,
      val rightCall: Int => Boolean,
      val leftClass: String,
      val rightClass: String,
  )

  private def capture(steps: List[Selection.Step]): Captured =
    steps match
      case Selection.Step.Int32(left, leftKeep) :: Selection.Step.Int32(right, rightKeep) :: Nil =>
        Captured(
          left,
          right,
          leftKeep,
          rightKeep,
          function(leftKeep),
          function(rightKeep),
          leftKeep.getClass.getName,
          rightKeep.getClass.getName,
        )
      case _ =>
        throw new IllegalStateException(s"phase split expected two int steps, found $steps")

  private def function(predicate: ColumnKernel.Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        value => fallback(value)

  private def scanSerial(morsels: Vector[Morsel]): Long =
    morsels.foldLeft(0L)((acc, morsel) => acc ^ scanOne(morsel))

  private def scanOne(morsel: Morsel): Long =
    val key = ColumnHash.int32At(morsel.batch, 0).values
    val value = ColumnHash.int32At(morsel.batch, 1).values
    val end = morsel.start + morsel.length
    var row = morsel.start
    var acc = 0
    while (row < end) {
      acc ^= key(row)
      acc ^= value(row)
      row += 1
    }
    acc.toLong

  private def predicateOne(morsel: Morsel, captured: Captured, directCall: Boolean): Long =
    val key = ColumnHash.int32At(morsel.batch, 0).values
    val value = ColumnHash.int32At(morsel.batch, 1).values
    val left = if (captured.leftOrdinal == 0) then key else value
    val right = if (captured.rightOrdinal == 0) then key else value
    val end = morsel.start + morsel.length
    var row = morsel.start
    var total = 0L
    if (directCall) then
      val keepLeft = captured.leftCall
      val keepRight = captured.rightCall
      while (row < end) {
        if (keepLeft(left(row)) && keepRight(right(row))) then total += 1L
        row += 1
      }
    else
      val keepLeft = captured.leftApply
      val keepRight = captured.rightApply
      while (row < end) {
        if (keepLeft(left(row)) && keepRight(right(row))) then total += 1L
        row += 1
      }
    total

  private def collect(morsel: Morsel, captured: Captured, into: Array[Int]): Int =
    val key = ColumnHash.int32At(morsel.batch, 0).values
    val value = ColumnHash.int32At(morsel.batch, 1).values
    val left = if (captured.leftOrdinal == 0) then key else value
    val right = if (captured.rightOrdinal == 0) then key else value
    val keepLeft = captured.leftCall
    val keepRight = captured.rightCall
    val end = morsel.start + morsel.length
    var row = morsel.start
    var live = 0
    while (row < end) {
      if (keepLeft(left(row)) && keepRight(right(row))) then
        into(live) = key(row)
        live += 1
      row += 1
    }
    live

  private def mixSerial(
      morsels: Vector[Morsel],
      survivors: Array[Array[Int]],
      rows: Array[Int],
  ): Long =
    var index = 0
    var acc = 0L
    while (index < morsels.length) {
      acc ^= mixOne(survivors(index), rows(index))
      index += 1
    }
    acc

  private def mixOne(keys: Array[Int], live: Int): Long =
    var cursor = 0
    var acc = 0
    while (cursor < live) {
      acc ^= ColumnHash.mix(keys(cursor))
      cursor += 1
    }
    acc.toLong

  private def extraSerial(
      morsels: Vector[Morsel],
      survivors: Array[Array[Int]],
      rows: Array[Int],
  ): Long =
    val lengths = morsels.iterator
    var index = 0
    var extra = 0L
    while (lengths.hasNext) {
      val table = new HashTable(ColumnHash.tableSize(lengths.next().length))
      val keys = survivors(index)
      val live = rows(index)
      var cursor = 0
      while (cursor < live) {
        extra += table.extra(keys(cursor)).toLong
        table.add(keys(cursor), 1)
        cursor += 1
      }
      index += 1
    }
    extra

  private def hashPartial(morsel: Morsel, captured: Captured, small: Boolean): ColumnBatch =
    val rows = if (small) then fitRows else morsel.length
    val table = new HashTable(ColumnHash.tableSize(rows))
    val key = ColumnHash.int32At(morsel.batch, 0).values
    val value = ColumnHash.int32At(morsel.batch, 1).values
    val left = if (captured.leftOrdinal == 0) then key else value
    val right = if (captured.rightOrdinal == 0) then key else value
    val keepLeft = captured.leftCall
    val keepRight = captured.rightCall
    val end = morsel.start + morsel.length
    var row = morsel.start
    while (row < end) {
      if (keepLeft(left(row)) && keepRight(right(row))) then table.add(key(row), 1)
      row += 1
    }
    table.compact()

  private def directPartial(morsel: Morsel, captured: Captured): ColumnBatch =
    val dense = directFill(morsel, captured)
    if (Check.isNull(dense)) then hashPartial(morsel, captured, small = false) else dense

  private def directFill(morsel: Morsel, captured: Captured): ColumnBatch | Null =
    if (morsel.length == 0) then ColumnBatches.emptyIntPairs
    else
      val keyColumn = ColumnHash.int32At(morsel.batch, 0)
      val valueColumn = ColumnHash.int32At(morsel.batch, 1)
      val present =
        Validity.rangePresent(keyColumn.validity, morsel.start, morsel.length) &&
          Validity.rangePresent(valueColumn.validity, morsel.start, morsel.length)
      if (!present) then null
      else fillDense(morsel, captured, keyColumn.values, valueColumn.values)

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

  private def fillDense(
      morsel: Morsel,
      captured: Captured,
      keys: Array[Int],
      values: Array[Int],
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
        val buffer = borrowDense(span)
        buffer.clear()
        buffer.prepare(min, span)
        val left = if (captured.leftOrdinal == 0) then keys else values
        val right = if (captured.rightOrdinal == 0) then keys else values
        val keepLeft = captured.leftCall
        val keepRight = captured.rightCall
        row = start
        while (row < end) {
          if (keepLeft(left(row)) && keepRight(right(row))) then buffer.add(keys(row))
          row += 1
        }
        buffer.compact()

  private def combine(
      sizes: Vector[Int],
      partials: Vector[ColumnBatch],
      numPartitions: Int,
      threads: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    try
      val merged = PartialCombine.mergeMorsels(sizes, partials, threads, HashCount.addCounts)
      if (merged.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
      else
        val buckets = HashExchange.partitionAll(merged, keyOrdinal = 0, numPartitions, threads)
        val conf = ScarletConf.get
        if (buckets.exists(bucket => HashCountSpill.exceeds(bucket, conf))) then
          buckets.map(bucket => HashCountSpill.finish(bucket, threads, batchSize, conf))
        else MorselScheduler.map(buckets, threads)((_, bucket) => HashCount.addCounts(bucket))
    finally HashCount.releaseWorkers()

  private val tables: ThreadLocal[HashTable | Null] = new ThreadLocal[HashTable | Null]()
  private val denses: ThreadLocal[Dense | Null] = new ThreadLocal[Dense | Null]()

  private def borrow(rows: Int): HashTable =
    val need = ColumnHash.tableSize(rows)
    tables.get() match
      case current: HashTable if current.accepts(need) => current
      case _ =>
        val created = new HashTable(need)
        tables.set(created)
        created

  private def borrowDense(span: Int): Dense =
    denses.get() match
      case current: Dense if current.accepts(span) => current
      case _ =>
        val created = new Dense(span)
        denses.set(created)
        created

  private final class HashTable(capacity: Int):
    private val mask: Int = capacity - 1
    private val limit: Int = capacity >>> 1
    private val keys: Array[Int] = new Array[Int](capacity)
    private val counts: Array[Int] = new Array[Int](capacity)
    private val state: Array[Byte] = new Array[Byte](capacity)
    private val slots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def accepts(need: Int): Boolean = keys.length >= need

    def total: Int =
      var cursor = 0
      var sum = 0
      while (cursor < occupied) {
        sum += counts(slots(cursor))
        cursor += 1
      }
      sum

    def clear(): Unit =
      var cursor = 0
      val rows = occupied
      while (cursor < rows) {
        state(slots(cursor)) = 0
        cursor += 1
      }
      occupied = 0

    /** Extra probes past the first slot. The key is not inserted. */
    def extra(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      var steps = 0
      while (state(slot) != 0 && keys(slot) != key) {
        slot = (slot + 1) & mask
        steps += 1
      }
      steps

    def add(key: Int, weight: Int): Unit =
      val slot = locate(key)
      if (state(slot) == 0) then
        if (occupied >= limit) then
          throw new IllegalStateException(s"phase hash table is full at $occupied groups")
        state(slot) = 1
        keys(slot) = key
        counts(slot) = weight
        slots(occupied) = slot
        occupied += 1
      else counts(slot) = counts(slot) + weight

    def compact(): ColumnBatch =
      val rows = occupied
      if (rows == 0) then ColumnBatches.emptyIntPairs
      else
        val outKeys = new Array[Int](rows)
        val outCounts = new Array[Int](rows)
        var cursor = 0
        while (cursor < rows) {
          val slot = slots(cursor)
          outKeys(cursor) = keys(slot)
          outCounts(cursor) = counts(slot)
          cursor += 1
        }
        pairs(outKeys, outCounts, rows)

    private def locate(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      while (state(slot) != 0 && keys(slot) != key) slot = (slot + 1) & mask
      slot

  private final class Dense(capacity: Int):
    private var counts: Array[Int] = new Array[Int](capacity)
    private var order: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0
    private var min: Int = 0

    def accepts(span: Int): Boolean = counts.length >= span

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
        pairs(outKeys, outCounts, rows)

  private def pairs(keys: Array[Int], counts: Array[Int], rows: Int): ColumnBatch =
    val present = Validity.allValid(rows)
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(Int32Column.of(keys, present, rows), Int32Column.of(counts, present, rows)),
      rows,
    )
