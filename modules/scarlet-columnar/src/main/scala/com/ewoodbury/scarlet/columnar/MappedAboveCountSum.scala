package com.ewoodbury.scarlet.columnar

/**
 * Count and sum after one int slot above `mapIntKey`.
 *
 * `under` may be empty and runs on the source columns, before `mapKey`. A null slot fails that
 * step and does not call the predicate. `mapKey` then runs once on a present survivor and is not
 * inspected. `above` runs after it. A key slot sees the mapped key and is not called on a null. A
 * value slot sees the original value, including a null-key row, and is not called on a null value.
 * A null mapped key is dropped. A null value that a key slot keeps is counted and not added. The
 * mapped pair batch is not built.
 *
 * Owned arrays are fully present, so that walk only observes. Null keys and null values stay on
 * [[scan]], which reads encoded morsels.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object MappedAboveCountSum:

  private[columnar] final class Prepared(
      val under: Array[MappedCountSum.Slot],
      val aboveKey: Boolean,
      val aboveKeep: Int => Boolean,
  )

  /** Int slots under the map, plus one int key or value above it. */
  def prepare(under: List[Selection.Step], above: Selection.Step): Prepared =
    val (aboveKey, aboveKeep) = aboveSlot(above)
    new Prepared(MappedCountSum.prepare(under), aboveKey, aboveKeep)

  /**
   * One batch, from row 0. An empty batch does not call `mapKey` or `above`. `prepared` is not
   * copied.
   */
  def scan(
      batch: ColumnBatch,
      prepared: Prepared,
      mapKey: Int => Int,
      table: HashAggregate.GroupTable,
  ): Unit =
    val rows = batch.length
    if (rows == 0) then ()
    else
      ColumnMetrics.fused()
      val keys = ColumnHash.int32At(batch, 0)
      val values = ColumnHash.int32At(batch, 1)
      require(keys.length == values.length, "key and value columns differ in length")
      val keyValues = keys.values
      val valueValues = values.values
      val keyWords = keys.validity.words
      val valueWords = values.validity.words
      val slots = prepared.under
      val aboveKey = prepared.aboveKey
      val aboveKeep = prepared.aboveKeep
      var row = 0
      while (row < rows) {
        if (kept(slots, row, keyValues, keyWords, valueValues, valueWords)) then
          val keyPresent = Validity.isSet(keyWords, row)
          if (aboveKey) then
            if (keyPresent) then
              val mapped = mapKey(keyValues(row))
              if (aboveKeep(mapped)) then
                val group = table.ensure(mapped)
                if (Validity.isSet(valueWords, row)) then table.observe(group, valueValues(row))
                else table.countNull(group)
          else
            acceptValue(
              keyPresent,
              row,
              keyValues,
              valueValues,
              valueWords,
              mapKey,
              aboveKeep,
              table,
            )
        row += 1
      }

  /**
   * One partial per stripe of fully present arrays. Empty input, including zero-length arrays, is
   * an empty vector: the caller publishes `numPartitions` empty count-sum batches.
   */
  def foldOwned(
      parts: Vector[(Array[Int], Array[Int])],
      under: List[Selection.Step],
      above: Selection.Step,
      mapKey: Int => Int,
      parallelism: Int,
      open: () => HashAggregate.GroupTable,
      publish: HashAggregate.GroupTable => ColumnBatch,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    val prepared = prepare(under, above)
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
          scanOwned(sources, prefix, stripe.from, stripe.until, prepared, mapKey, table)
          ColumnMetrics.fused()
          publish(table)
        }

  private def acceptValue(
      keyPresent: Boolean,
      row: Int,
      keyValues: Array[Int],
      valueValues: Array[Int],
      valueWords: Array[Long],
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
      table: HashAggregate.GroupTable,
  ): Unit =
    val valuePresent = Validity.isSet(valueWords, row)
    if (keyPresent) then
      val mapped = mapKey(keyValues(row))
      if (valuePresent && aboveKeep(valueValues(row))) then
        table.observe(table.ensure(mapped), valueValues(row))
    else if (valuePresent) then
      val _ = aboveKeep(valueValues(row))

  private def aboveSlot(step: Selection.Step): (Boolean, Int => Boolean) = step match
    case Selection.Step.Int32(0, keep) => (true, captured(keep))
    case Selection.Step.Int32(1, keep) => (false, captured(keep))
    case Selection.Step.Int32(column, _) =>
      throw new IllegalStateException(s"count-sum ordinal $column is not a pair column")
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
      throw new IllegalStateException("slot above a mapped key is an int key or value")

  /** One function for the row loop. A bare trait is wrapped once here, not once per row. */
  private def captured(predicate: ColumnKernel.Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        value => fallback(value)

  private def kept(
      slots: Array[MappedCountSum.Slot],
      row: Int,
      keyValues: Array[Int],
      keyWords: Array[Long],
      valueValues: Array[Int],
      valueWords: Array[Long],
  ): Boolean =
    var index = 0
    var valid = true
    while (valid && index < slots.length) {
      val slot = slots(index)
      val values = if (slot.column == 0) then keyValues else valueValues
      val words = if (slot.column == 0) then keyWords else valueWords
      valid = Validity.isSet(words, row) && slot.keep(values(row))
      index += 1
    }
    valid

  private final class Stripe(val from: Long, val until: Long)

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
   * stripe may cross a partition boundary.
   */
  private def bounds(total: Long, width: Int): Vector[Stripe] =
    val base = total / width
    val extra = (total % width).toInt
    Vector.tabulate(width) { worker =>
      val start = worker.toLong * base + math.min(worker, extra).toLong
      val length = base + (if (worker < extra) then 1L else 0L)
      new Stripe(start, start + length)
    }

  private def scanOwned(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      from: Long,
      until: Long,
      prepared: Prepared,
      mapKey: Int => Int,
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
          applyOwned(prepared, table, keys, values, localStart, localEnd, mapKey)
        val next = math.min(until, partEnd)
        require(next > cursor, "count-sum stripe did not advance")
        cursor = next
      part += 1
    }

  private def applyOwned(
      prepared: Prepared,
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      mapKey: Int => Int,
  ): Unit =
    val slots = prepared.under
    val aboveKeep = prepared.aboveKeep
    if (slots.length == 0 && prepared.aboveKey) then
      aboveKeyOnly(table, keys, values, start, end, aboveKeep, mapKey)
    else if (slots.length == 0) then
      aboveValueOnly(table, keys, values, start, end, aboveKeep, mapKey)
    else if (slots.length == 1 && slots(0).column == 0 && prepared.aboveKey) then
      keyThenAboveKey(table, keys, values, start, end, slots(0).keep, aboveKeep, mapKey)
    else if (
        slots.length == 2 && slots(0).column == 0 && slots(1).column == 1 && prepared.aboveKey
      )
    then
      keyValueThenAboveKey(
        table,
        keys,
        values,
        start,
        end,
        slots(0).keep,
        slots(1).keep,
        aboveKeep,
        mapKey,
      )
    else general(table, keys, values, start, end, slots, prepared.aboveKey, aboveKeep, mapKey)

  private def aboveKeyOnly(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      aboveKeep: Int => Boolean,
      mapKey: Int => Int,
  ): Unit =
    var row = start
    while (row < end) {
      val mapped = mapKey(keys(row))
      if (aboveKeep(mapped)) then table.observe(table.ensure(mapped), values(row))
      row += 1
    }

  private def aboveValueOnly(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      aboveKeep: Int => Boolean,
      mapKey: Int => Int,
  ): Unit =
    var row = start
    while (row < end) {
      val value = values(row)
      val mapped = mapKey(keys(row))
      if (aboveKeep(value)) then table.observe(table.ensure(mapped), value)
      row += 1
    }

  private def keyThenAboveKey(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keepKey: Int => Boolean,
      aboveKeep: Int => Boolean,
      mapKey: Int => Int,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      if (keepKey(key)) then
        val mapped = mapKey(key)
        if (aboveKeep(mapped)) then table.observe(table.ensure(mapped), values(row))
      row += 1
    }

  private def keyValueThenAboveKey(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keepKey: Int => Boolean,
      keepValue: Int => Boolean,
      aboveKeep: Int => Boolean,
      mapKey: Int => Int,
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

  private def general(
      table: HashAggregate.GroupTable,
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      slots: Array[MappedCountSum.Slot],
      aboveKey: Boolean,
      aboveKeep: Int => Boolean,
      mapKey: Int => Int,
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      val value = values(row)
      if (passes(slots, key, value)) then
        val mapped = mapKey(key)
        val keep = if (aboveKey) then aboveKeep(mapped) else aboveKeep(value)
        if (keep) then table.observe(table.ensure(mapped), value)
      row += 1
    }

  private def passes(slots: Array[MappedCountSum.Slot], key: Int, value: Int): Boolean =
    var index = 0
    var valid = true
    while (valid && index < slots.length) {
      val slot = slots(index)
      val sample = if (slot.column == 0) then key else value
      valid = slot.keep(sample)
      index += 1
    }
    valid
