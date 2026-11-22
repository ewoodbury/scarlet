package com.ewoodbury.scarlet.columnar

/**
 * Count and sum of owned int pairs after an int key function and one slot above it.
 *
 * Filters in `steps` see the source key and value. `mapKey` runs once on each survivor and is not
 * inspected. The slot then sees the mapped key or the original value. The arrays are aliased.
 * Nothing here copies them, and [[Selection.applySelected]] is not used.
 *
 * The caller promises every cell is present, which is true of an
 * [[com.ewoodbury.scarlet.core.IntPairArrays]] partition. A null key is not here to drop and a
 * null value is not here to count. Those rules stay on [[MappedAboveCountSum]], which reads
 * encoded batches. This walk only observes.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object OwnedMappedAboveCountSum:

  /**
   * One partial per stripe. Empty input, including zero-length arrays, is an empty vector: the
   * caller publishes `numPartitions` empty count-sum batches without an exchange.
   */
  def fold(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      mapKey: Int => Int,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      parallelism: Int,
      open: () => HashAggregate.GroupTable,
      publish: HashAggregate.GroupTable => ColumnBatch,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(
      aboveColumn == 0 || aboveColumn == 1,
      s"mapped slot must be a key or a value, got $aboveColumn",
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
          scan(
            sources,
            prefix,
            stripe.from,
            stripe.until,
            prepared,
            mapKey,
            aboveColumn,
            aboveKeep,
            table,
          )
          ColumnMetrics.fused()
          publish(table)
        }

  private final class Stripe(val from: Long, val until: Long)

  private sealed trait Prepared

  private case object Bare extends Prepared

  private final class KeyThenValue(val keepKey: Int => Boolean, val keepValue: Int => Boolean)
      extends Prepared

  private final class ValueThenKey(val keepValue: Int => Boolean, val keepKey: Int => Boolean)
      extends Prepared

  private final class Slots(val slots: Array[Slot]) extends Prepared

  private final class Slot(val column: Int, val keep: Int => Boolean)

  private def prepare(steps: List[Selection.Step]): Prepared =
    steps.foreach {
      case Selection.Step.Int32(column, _) =>
        require(column == 0 || column == 1, s"count-sum ordinal $column is not a pair column")
      case _ =>
        require(false, "owned mapped count-sum steps are int slots")
    }
    steps match
      case Nil => Bare
      case Selection.Step.Int32(0, keepKey) :: Selection.Step.Int32(1, keepValue) :: Nil =>
        new KeyThenValue(captured(keepKey), captured(keepValue))
      case Selection.Step.Int32(1, keepValue) :: Selection.Step.Int32(0, keepKey) :: Nil =>
        new ValueThenKey(captured(keepValue), captured(keepKey))
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
          require(false, "owned mapped count-sum steps are int slots")
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
      mapKey: Int => Int,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      table: HashAggregate.GroupTable,
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
          apply(
            prepared,
            table,
            keys,
            values,
            localStart,
            localEnd,
            mapKey,
            aboveColumn,
            aboveKeep,
          )
        val next = math.min(until, partEnd)
        require(next > cursor, "count-sum stripe did not advance")
        cursor = next
      part += 1
    }

  private def apply(
      prepared: Prepared,
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      mapKey: Int => Int,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
  ): Unit =
    val onKey = aboveColumn == 0
    prepared match
      case Bare =>
        if (onKey) then bareKey(table, keys, values, start, end, mapKey, aboveKeep)
        else bareValue(table, keys, values, start, end, mapKey, aboveKeep)
      case filtered: KeyThenValue =>
        if (onKey) then
          keyThenValueKey(
            table,
            keys,
            values,
            start,
            end,
            filtered.keepKey,
            filtered.keepValue,
            mapKey,
            aboveKeep,
          )
        else
          keyThenValueValue(
            table,
            keys,
            values,
            start,
            end,
            filtered.keepKey,
            filtered.keepValue,
            mapKey,
            aboveKeep,
          )
      case filtered: ValueThenKey =>
        if (onKey) then
          valueThenKeyKey(
            table,
            keys,
            values,
            start,
            end,
            filtered.keepValue,
            filtered.keepKey,
            mapKey,
            aboveKeep,
          )
        else
          valueThenKeyValue(
            table,
            keys,
            values,
            start,
            end,
            filtered.keepValue,
            filtered.keepKey,
            mapKey,
            aboveKeep,
          )
      case filtered: Slots =>
        if (onKey) then
          slotsKey(table, keys, values, start, end, filtered.slots, mapKey, aboveKeep)
        else slotsValue(table, keys, values, start, end, filtered.slots, mapKey, aboveKeep)

  private def bareKey(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
  ): Unit =
    var row = start
    while (row < end) {
      val mapped = mapKey(keys(row))
      if (aboveKeep(mapped)) then table.observe(table.ensure(mapped), values(row))
      row += 1
    }

  private def bareValue(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
  ): Unit =
    var row = start
    while (row < end) {
      val mapped = mapKey(keys(row))
      val value = values(row)
      if (aboveKeep(value)) then table.observe(table.ensure(mapped), value)
      row += 1
    }

  private def keyThenValueKey(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keepKey: Int => Boolean,
      keepValue: Int => Boolean,
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      if (keepKey(key)) then
        val value = values(row)
        if (keepValue(value)) then
          val mapped = mapKey(key)
          if (aboveKeep(mapped)) then table.observe(table.ensure(mapped), value)
      row += 1
    }

  private def keyThenValueValue(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keepKey: Int => Boolean,
      keepValue: Int => Boolean,
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      if (keepKey(key)) then
        val value = values(row)
        if (keepValue(value)) then
          val mapped = mapKey(key)
          if (aboveKeep(value)) then table.observe(table.ensure(mapped), value)
      row += 1
    }

  private def valueThenKeyKey(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keepValue: Int => Boolean,
      keepKey: Int => Boolean,
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
  ): Unit =
    var row = start
    while (row < end) {
      val value = values(row)
      if (keepValue(value)) then
        val key = keys(row)
        if (keepKey(key)) then
          val mapped = mapKey(key)
          if (aboveKeep(mapped)) then table.observe(table.ensure(mapped), value)
      row += 1
    }

  private def valueThenKeyValue(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keepValue: Int => Boolean,
      keepKey: Int => Boolean,
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
  ): Unit =
    var row = start
    while (row < end) {
      val value = values(row)
      if (keepValue(value)) then
        val key = keys(row)
        if (keepKey(key)) then
          val mapped = mapKey(key)
          if (aboveKeep(value)) then table.observe(table.ensure(mapped), value)
      row += 1
    }

  private def slotsKey(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      filters: Array[Slot],
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      val value = values(row)
      if (passes(filters, key, value)) then
        val mapped = mapKey(key)
        if (aboveKeep(mapped)) then table.observe(table.ensure(mapped), value)
      row += 1
    }

  private def slotsValue(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      filters: Array[Slot],
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      val value = values(row)
      if (passes(filters, key, value)) then
        val mapped = mapKey(key)
        if (aboveKeep(value)) then table.observe(table.ensure(mapped), value)
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
