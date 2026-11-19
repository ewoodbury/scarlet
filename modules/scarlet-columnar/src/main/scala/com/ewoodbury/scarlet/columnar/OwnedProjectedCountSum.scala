package com.ewoodbury.scarlet.columnar

/**
 * Count and sum of owned int pairs after a pair project.
 *
 * The walk is [[OwnedCountSum]] with one difference: each survivor is passed through `mapper`
 * before the count-sum table. Filters still see the source key and value. `mapper` is the adapter
 * from [[IntPairProject.adapt]]. It runs once on each survivor and is not inspected. A null
 * projected key is dropped. A null projected value is counted and not added. The pair batch is not
 * published, and [[CountSum.byKey]] is not called.
 *
 * The caller promises every source cell is present, which is true of an
 * [[com.ewoodbury.scarlet.core.IntPairArrays]] partition. A null source stays on the encoded
 * [[ProjectedPairCountSum]] walk. The arrays are aliased. Nothing here copies them, and
 * [[Selection.applySelected]] is not used.
 *
 * [[foldAbove]] is that walk plus one int slot on the projected pair. Filters under the project
 * still see the source. The slot sees the projected key or value. A null projected key is dropped.
 * A null projected value that a key slot keeps is counted and not added.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
private[columnar] object OwnedProjectedCountSum:

  /**
   * One partial per stripe. Empty input, including zero-length arrays, is an empty vector: the
   * caller publishes `numPartitions` empty count-sum batches without an exchange.
   */
  def fold(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      mapper: IntPairProject.IntPairMap,
      parallelism: Int,
      open: () => HashAggregate.GroupTable,
      publish: HashAggregate.GroupTable => ColumnBatch,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    val prepared = prepare(steps)
    if (parts.isEmpty) then Vector.empty
    else
      val sources = parts.toArray
      val prefix = prefixOf(sources)
      val total = prefix(sources.length)
      if (total == 0L) then Vector.empty
      else
        val width = stripeWidth(total, parallelism)
        MorselScheduler.map(bounds(total, width), parallelism) { (_, stripe) =>
          val table = open()
          val sink = new TableSink(table)
          scan(sources, prefix, stripe.from, stripe.until, prepared, mapper, sink)
          ColumnMetrics.fused()
          publish(table)
        }

  /** Observes into one stripe's table. A null projected value counts and is not added. */
  private final class TableSink(table: HashAggregate.GroupTable)
      extends IntPairProject.IntPairCountSink:
    def observe(key: Int, value: Int): Unit =
      table.observe(table.ensure(key), value)
    def countNull(key: Int): Unit =
      table.countNull(table.ensure(key))

  private final class Stripe(val from: Long, val until: Long)

  private sealed trait Prepared

  private case object All extends Prepared

  private final class KeyThenValue(val keepKey: Int => Boolean, val keepValue: Int => Boolean)
      extends Prepared

  private final class ValueThenKey(val keepValue: Int => Boolean, val keepKey: Int => Boolean)
      extends Prepared

  private final class KeyOnly(val keep: Int => Boolean) extends Prepared

  private final class ValueOnly(val keep: Int => Boolean) extends Prepared

  private final class Two(
      val leftOrdinal: Int,
      val keepLeft: Int => Boolean,
      val rightOrdinal: Int,
      val keepRight: Int => Boolean,
  ) extends Prepared

  private final class Slots(val slots: Array[Slot]) extends Prepared

  private final class Slot(val column: Int, val keep: Int => Boolean)

  private def prepare(steps: List[Selection.Step]): Prepared =
    steps.foreach {
      case Selection.Step.Int32(column, _) =>
        require(column == 0 || column == 1, s"count-sum ordinal $column is not a pair column")
      case _ =>
        require(false, "owned count-sum steps are int slots")
    }
    steps match
      case Nil => All
      case Selection.Step.Int32(0, keepKey) :: Selection.Step.Int32(1, keepValue) :: Nil =>
        new KeyThenValue(captured(keepKey), captured(keepValue))
      case Selection.Step.Int32(1, keepValue) :: Selection.Step.Int32(0, keepKey) :: Nil =>
        new ValueThenKey(captured(keepValue), captured(keepKey))
      case Selection.Step.Int32(0, keep) :: Nil =>
        new KeyOnly(captured(keep))
      case Selection.Step.Int32(1, keep) :: Nil =>
        new ValueOnly(captured(keep))
      case Selection.Step.Int32(left, keepLeft) :: Selection.Step.Int32(right, keepRight) :: Nil =>
        new Two(left, captured(keepLeft), right, captured(keepRight))
      case _ =>
        new Slots(slotsOf(steps))

  /** One function for the row loop. A bare trait is wrapped once here, not once per row. */
  private def captured(predicate: ColumnKernel.Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        value => fallback(value)

  private def slotsOf(steps: List[Selection.Step]): Array[Slot] =
    val ordered = steps.toArray
    val slots = new Array[Slot](ordered.length)
    var index = 0
    while (index < ordered.length) {
      ordered(index) match
        case Selection.Step.Int32(column, predicate) =>
          slots(index) = new Slot(column, captured(predicate))
        case _ =>
          require(false, "owned count-sum steps are int slots")
      index += 1
    }
    slots

  private def prefixOf(sources: Array[(Array[Int], Array[Int])]): Array[Long] =
    val prefix = new Array[Long](sources.length + 1)
    var index = 0
    while (index < sources.length) {
      val (keys, values) = sources(index)
      require(keys.length == values.length, "key and value arrays differ in length")
      prefix(index + 1) = prefix(index) + keys.length.toLong
      index += 1
    }
    prefix

  /** At most one stripe per row, and no more stripes than workers. */
  private def stripeWidth(total: Long, parallelism: Int): Int =
    val capped = if (total > Int.MaxValue.toLong) parallelism.toLong else total
    math.min(parallelism, capped.toInt)

  /**
   * Contiguous global ranges. Earlier stripes take the extra row when the count does not divide. A
   * stripe may cross a partition boundary. The scan reads each side in order.
   */
  private def bounds(total: Long, width: Int): Vector[Stripe] =
    val base = total / width
    val extra = (total % width).toInt
    Vector.tabulate(width) { worker =>
      val start = worker.toLong * base + math.min(worker, extra).toLong
      val length = base + (if (worker < extra) then 1L else 0L)
      new Stripe(start, start + length)
    }

  private def scan(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      from: Long,
      until: Long,
      prepared: Prepared,
      mapper: IntPairProject.IntPairMap,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var part = 0
    while (part < sources.length && prefix(part + 1) <= from) do part += 1
    var cursor = from
    while (cursor < until) {
      require(part < sources.length, "count-sum stripe ran past the source")
      val partStart = prefix(part)
      val partEnd = prefix(part + 1)
      if (partEnd > cursor) then
        val localStart = (cursor - partStart).toInt
        val localEnd = (math.min(until, partEnd) - partStart).toInt
        if (localEnd > localStart) then
          val (keys, values) = sources(part)
          apply(prepared, keys, values, localStart, localEnd, mapper, sink)
        val next = math.min(until, partEnd)
        require(next > cursor, "count-sum stripe did not advance")
        cursor = next
      part += 1
    }

  /**
   * [[fold]] plus one int slot on the projected pair. `aboveColumn` is `0` for the key and `1` for
   * the value. `aboveKeep` is not called on a null projected cell. A null source stays on the
   * encoded walk: these arrays are fully present.
   */
  def foldAbove(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      mapper: IntPairProject.IntPairMap,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      parallelism: Int,
      open: () => HashAggregate.GroupTable,
      publish: HashAggregate.GroupTable => ColumnBatch,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(
      aboveColumn == 0 || aboveColumn == 1,
      s"pair slot must be a key or a value, got $aboveColumn",
    )
    val prepared = prepare(steps)
    if (parts.isEmpty) then Vector.empty
    else
      val sources = parts.toArray
      val prefix = prefixOf(sources)
      val total = prefix(sources.length)
      if (total == 0L) then Vector.empty
      else
        val width = stripeWidth(total, parallelism)
        MorselScheduler.map(bounds(total, width), parallelism) { (_, stripe) =>
          val table = open()
          val sink = new TableSink(table)
          scanAbove(
            sources,
            prefix,
            stripe.from,
            stripe.until,
            prepared,
            mapper,
            aboveColumn,
            aboveKeep,
            sink,
          )
          ColumnMetrics.fused()
          publish(table)
        }

  private def scanAbove(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      from: Long,
      until: Long,
      prepared: Prepared,
      mapper: IntPairProject.IntPairMap,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var part = 0
    while (part < sources.length && prefix(part + 1) <= from) do part += 1
    var cursor = from
    while (cursor < until) {
      require(part < sources.length, "count-sum stripe ran past the source")
      val partStart = prefix(part)
      val partEnd = prefix(part + 1)
      if (partEnd > cursor) then
        val localStart = (cursor - partStart).toInt
        val localEnd = (math.min(until, partEnd) - partStart).toInt
        if (localEnd > localStart) then
          val (keys, values) = sources(part)
          applyAbove(
            prepared,
            keys,
            values,
            localStart,
            localEnd,
            mapper,
            aboveColumn,
            aboveKeep,
            sink,
          )
        val next = math.min(until, partEnd)
        require(next > cursor, "count-sum stripe did not advance")
        cursor = next
      part += 1
    }

  private def applyAbove(
      prepared: Prepared,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      mapper: IntPairProject.IntPairMap,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    prepared match
      case All =>
        denseAbove(keys, values, start, end, mapper, aboveColumn, aboveKeep, sink)
      case filtered: KeyThenValue =>
        keyThenValueAbove(
          keys,
          values,
          start,
          end,
          filtered.keepKey,
          filtered.keepValue,
          mapper,
          aboveColumn,
          aboveKeep,
          sink,
        )
      case filtered: ValueThenKey =>
        valueThenKeyAbove(
          keys,
          values,
          start,
          end,
          filtered.keepValue,
          filtered.keepKey,
          mapper,
          aboveColumn,
          aboveKeep,
          sink,
        )
      case filtered: KeyOnly =>
        keyOnlyAbove(
          keys,
          values,
          start,
          end,
          filtered.keep,
          mapper,
          aboveColumn,
          aboveKeep,
          sink,
        )
      case filtered: ValueOnly =>
        valueOnlyAbove(
          keys,
          values,
          start,
          end,
          filtered.keep,
          mapper,
          aboveColumn,
          aboveKeep,
          sink,
        )
      case filtered: Two =>
        twoAbove(
          keys,
          values,
          start,
          end,
          filtered.leftOrdinal,
          filtered.keepLeft,
          filtered.rightOrdinal,
          filtered.keepRight,
          mapper,
          aboveColumn,
          aboveKeep,
          sink,
        )
      case filtered: Slots =>
        slotsAbove(
          keys,
          values,
          start,
          end,
          filtered.slots,
          mapper,
          aboveColumn,
          aboveKeep,
          sink,
        )

  private def denseAbove(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      mapper: IntPairProject.IntPairMap,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      IntPairProject.acceptCountFiltered(
        mapper(keys(row), values(row)),
        aboveColumn,
        aboveKeep,
        sink,
      )
      row += 1
    }

  private def keyThenValueAbove(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keepKey: Int => Boolean,
      keepValue: Int => Boolean,
      mapper: IntPairProject.IntPairMap,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      if (keepKey(key)) then
        val value = values(row)
        if (keepValue(value)) then
          IntPairProject.acceptCountFiltered(
            mapper(key, value),
            aboveColumn,
            aboveKeep,
            sink,
          )
      row += 1
    }

  private def valueThenKeyAbove(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keepValue: Int => Boolean,
      keepKey: Int => Boolean,
      mapper: IntPairProject.IntPairMap,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      val value = values(row)
      if (keepValue(value)) then
        val key = keys(row)
        if (keepKey(key)) then
          IntPairProject.acceptCountFiltered(
            mapper(key, value),
            aboveColumn,
            aboveKeep,
            sink,
          )
      row += 1
    }

  private def keyOnlyAbove(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keep: Int => Boolean,
      mapper: IntPairProject.IntPairMap,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      if (keep(key)) then
        IntPairProject.acceptCountFiltered(
          mapper(key, values(row)),
          aboveColumn,
          aboveKeep,
          sink,
        )
      row += 1
    }

  private def valueOnlyAbove(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keep: Int => Boolean,
      mapper: IntPairProject.IntPairMap,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      val value = values(row)
      if (keep(value)) then
        IntPairProject.acceptCountFiltered(
          mapper(keys(row), value),
          aboveColumn,
          aboveKeep,
          sink,
        )
      row += 1
    }

  private def twoAbove(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      leftOrdinal: Int,
      keepLeft: Int => Boolean,
      rightOrdinal: Int,
      keepRight: Int => Boolean,
      mapper: IntPairProject.IntPairMap,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      val value = values(row)
      val left = if (leftOrdinal == 0) then key else value
      val right = if (rightOrdinal == 0) then key else value
      if (keepLeft(left) && keepRight(right)) then
        IntPairProject.acceptCountFiltered(mapper(key, value), aboveColumn, aboveKeep, sink)
      row += 1
    }

  private def slotsAbove(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      filters: Array[Slot],
      mapper: IntPairProject.IntPairMap,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      val value = values(row)
      if (passes(filters, key, value)) then
        IntPairProject.acceptCountFiltered(mapper(key, value), aboveColumn, aboveKeep, sink)
      row += 1
    }

  private def apply(
      prepared: Prepared,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      mapper: IntPairProject.IntPairMap,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    prepared match
      case All =>
        dense(keys, values, start, end, mapper, sink)
      case filtered: KeyThenValue =>
        keyThenValue(
          keys,
          values,
          start,
          end,
          filtered.keepKey,
          filtered.keepValue,
          mapper,
          sink,
        )
      case filtered: ValueThenKey =>
        valueThenKey(
          keys,
          values,
          start,
          end,
          filtered.keepValue,
          filtered.keepKey,
          mapper,
          sink,
        )
      case filtered: KeyOnly =>
        keyOnly(keys, values, start, end, filtered.keep, mapper, sink)
      case filtered: ValueOnly =>
        valueOnly(keys, values, start, end, filtered.keep, mapper, sink)
      case filtered: Two =>
        two(
          keys,
          values,
          start,
          end,
          filtered.leftOrdinal,
          filtered.keepLeft,
          filtered.rightOrdinal,
          filtered.keepRight,
          mapper,
          sink,
        )
      case filtered: Slots =>
        slots(keys, values, start, end, filtered.slots, mapper, sink)

  private def dense(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      mapper: IntPairProject.IntPairMap,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      IntPairProject.acceptCountSum(mapper(keys(row), values(row)), sink)
      row += 1
    }

  private def keyThenValue(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keepKey: Int => Boolean,
      keepValue: Int => Boolean,
      mapper: IntPairProject.IntPairMap,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      if (keepKey(key)) then
        val value = values(row)
        if (keepValue(value)) then IntPairProject.acceptCountSum(mapper(key, value), sink)
      row += 1
    }

  private def valueThenKey(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keepValue: Int => Boolean,
      keepKey: Int => Boolean,
      mapper: IntPairProject.IntPairMap,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      val value = values(row)
      if (keepValue(value)) then
        val key = keys(row)
        if (keepKey(key)) then IntPairProject.acceptCountSum(mapper(key, value), sink)
      row += 1
    }

  private def keyOnly(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keep: Int => Boolean,
      mapper: IntPairProject.IntPairMap,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      if (keep(key)) then IntPairProject.acceptCountSum(mapper(key, values(row)), sink)
      row += 1
    }

  private def valueOnly(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keep: Int => Boolean,
      mapper: IntPairProject.IntPairMap,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      val value = values(row)
      if (keep(value)) then IntPairProject.acceptCountSum(mapper(keys(row), value), sink)
      row += 1
    }

  private def two(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      leftOrdinal: Int,
      keepLeft: Int => Boolean,
      rightOrdinal: Int,
      keepRight: Int => Boolean,
      mapper: IntPairProject.IntPairMap,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      val value = values(row)
      val left = if (leftOrdinal == 0) then key else value
      val right = if (rightOrdinal == 0) then key else value
      if (keepLeft(left) && keepRight(right)) then
        IntPairProject.acceptCountSum(mapper(key, value), sink)
      row += 1
    }

  private def slots(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      filters: Array[Slot],
      mapper: IntPairProject.IntPairMap,
      sink: IntPairProject.IntPairCountSink,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      val value = values(row)
      if (passes(filters, key, value)) then IntPairProject.acceptCountSum(mapper(key, value), sink)
      row += 1
    }

  private def passes(filters: Array[Slot], key: Int, value: Int): Boolean =
    var index = 0
    var valid = true
    while (valid && index < filters.length) {
      val slot = filters(index)
      val sample = if (slot.column == 0) then key else value
      valid = slot.keep(sample)
      index += 1
    }
    valid
