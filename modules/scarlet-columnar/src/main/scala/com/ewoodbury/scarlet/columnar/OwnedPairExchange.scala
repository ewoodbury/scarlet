package com.ewoodbury.scarlet.columnar

/**
 * Hash exchange of owned int-pair arrays.
 *
 * [[HashExchange.partitionAll]] scatters each batch into private buckets and
 * [[ColumnBatches.concat]] copies those buckets again. A filter that first materialises survivors
 * adds a third copy. This walk applies an int slot stack, or keeps every row, and writes each
 * survivor once into the shuffle bucket `floorMod(key, numPartitions)`. The arrays that write
 * produces are the partition columns. A dense single-partition source is that column as it stands:
 * nothing calls `copyOf` on it.
 *
 * Order inside a bucket is source order. Owned pairs have no null, so a null key is not here to
 * drop. A boxed partition, a null, or a whole-row filter never reaches this walk. Empty input is
 * `numPartitions` empty pair batches.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object OwnedPairExchange:

  private val PairSchema = Vector(LogicalType.Int32, LogicalType.Int32)

  def buckets(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    val prepared = prepare(steps)
    if (parts.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      val sources = parts.toArray
      val prefix = prefixOf(sources)
      val total = prefix(sources.length)
      if (total == 0L) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
      else if (aliasSource(prepared, sources, numPartitions)) then
        val (keys, values) = sources(0)
        Vector(publish(keys, values, keys.length))
      else
        val width = stripeWidth(total, parallelism)
        val stripes = bounds(total, width)
        val counted = new Array[Tally](stripes.length)
        val _ = MorselScheduler.map(stripes, parallelism) { (index, stripe) =>
          counted(index) =
            countStripe(sources, prefix, stripe.from, stripe.until, prepared, numPartitions)
          0
        }
        val offsets = Array.ofDim[Int](counted.length, numPartitions)
        val totals = new Array[Int](numPartitions)
        var stripe = 0
        while (stripe < counted.length) {
          val row = counted(stripe).counts
          var part = 0
          while (part < numPartitions) {
            offsets(stripe)(part) = totals(part)
            val next = totals(part).toLong + row(part).toLong
            require(next <= Int.MaxValue.toLong, s"partition $part has $next rows")
            totals(part) = next.toInt
            part += 1
          }
          stripe += 1
        }
        if (aliasCounted(sources, numPartitions, totals, total)) then
          val (keys, values) = sources(0)
          Vector(publish(keys, values, keys.length))
        else
          scatter(
            sources,
            prefix,
            stripes,
            prepared,
            numPartitions,
            parallelism,
            counted,
            offsets,
            totals,
          )

  private def aliasSource(
      prepared: Prepared,
      sources: Array[(Array[Int], Array[Int])],
      numPartitions: Int,
  ): Boolean =
    numPartitions == 1 && sources.length == 1 && (prepared match
      case All => true
      case _ => false)

  /** Every row survived into the only bucket, so the source arrays are that bucket. */
  private def aliasCounted(
      sources: Array[(Array[Int], Array[Int])],
      numPartitions: Int,
      totals: Array[Int],
      total: Long,
  ): Boolean =
    numPartitions == 1 && sources.length == 1 && totals(0).toLong == total

  private def scatter(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      stripes: Vector[Stripe],
      prepared: Prepared,
      numPartitions: Int,
      parallelism: Int,
      counted: Array[Tally],
      offsets: Array[Array[Int]],
      totals: Array[Int],
  ): Vector[ColumnBatch] =
    val outKeys = new Array[Array[Int]](numPartitions)
    val outValues = new Array[Array[Int]](numPartitions)
    var part = 0
    while (part < numPartitions) {
      outKeys(part) = new Array[Int](totals(part))
      outValues(part) = new Array[Int](totals(part))
      part += 1
    }
    val _ = MorselScheduler.map(stripes, parallelism) { (index, stripe) =>
      fillStripe(
        sources,
        prefix,
        stripe.from,
        stripe.until,
        prepared,
        numPartitions,
        counted(index),
        offsets(index),
        outKeys,
        outValues,
      )
    }
    Vector.tabulate(numPartitions) { bucket =>
      publish(outKeys(bucket), outValues(bucket), totals(bucket))
    }

  private final class Stripe(val from: Long, val until: Long)

  /** Survivor counts, plus the bits the fill reads so it does not retest the slots. */
  private final class Tally(val counts: Array[Int], val marks: Array[Long])

  private sealed trait Prepared

  private case object All extends Prepared

  private final class KeyThenValue(val keepKey: Int => Boolean, val keepValue: Int => Boolean)
      extends Prepared

  private final class Slots(val slots: Array[Slot]) extends Prepared

  private final class Slot(val column: Int, val keep: Int => Boolean)

  private def prepare(steps: List[Selection.Step]): Prepared =
    steps match
      case Nil => All
      case Selection.Step.Int32(0, keepKey) :: Selection.Step.Int32(1, keepValue) :: Nil =>
        new KeyThenValue(captured(keepKey), captured(keepValue))
      case _ =>
        new Slots(slotsOf(steps))

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
        case Selection.Step.Int32(column, predicate) if column == 0 || column == 1 =>
          slots(index) = new Slot(column, captured(predicate))
        case other =>
          require(false, s"owned exchange step ${other} is not an int pair slot")
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

  private def stripeWidth(total: Long, parallelism: Int): Int =
    val capped = if (total > Int.MaxValue.toLong) parallelism.toLong else total
    math.min(parallelism, capped.toInt)

  private def bounds(total: Long, width: Int): Vector[Stripe] =
    val base = total / width
    val extra = (total % width).toInt
    Vector.tabulate(width) { worker =>
      val start = worker.toLong * base + math.min(worker, extra).toLong
      val length = base + (if (worker < extra) then 1L else 0L)
      new Stripe(start, start + length)
    }

  private def countStripe(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      from: Long,
      until: Long,
      prepared: Prepared,
      numPartitions: Int,
  ): Tally =
    val counts = new Array[Int](numPartitions)
    val span = (until - from).toInt
    require(until - from == span.toLong, "owned exchange stripe does not fit")
    prepared match
      case All =>
        foreachRange(sources, prefix, from, until) { (keys, _, start, end, _) =>
          countAll(keys, start, end, numPartitions, counts)
        }
        new Tally(counts, Array.emptyLongArray)
      case filtered: KeyThenValue =>
        val marks = words(span)
        foreachRange(sources, prefix, from, until) { (keys, values, start, end, bitAt) =>
          countKeyValue(
            keys,
            values,
            start,
            end,
            bitAt,
            numPartitions,
            filtered.keepKey,
            filtered.keepValue,
            counts,
            marks,
          )
        }
        new Tally(counts, marks)
      case filtered: Slots =>
        val marks = words(span)
        foreachRange(sources, prefix, from, until) { (keys, values, start, end, bitAt) =>
          countSlots(
            keys,
            values,
            start,
            end,
            bitAt,
            numPartitions,
            filtered.slots,
            counts,
            marks,
          )
        }
        new Tally(counts, marks)

  private def fillStripe(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      from: Long,
      until: Long,
      prepared: Prepared,
      numPartitions: Int,
      tally: Tally,
      offsets: Array[Int],
      outKeys: Array[Array[Int]],
      outValues: Array[Array[Int]],
  ): Int =
    val cursor = new Array[Int](numPartitions)
    System.arraycopy(offsets, 0, cursor, 0, numPartitions)
    val written = prepared match
      case All =>
        fillAll(sources, prefix, from, until, numPartitions, cursor, outKeys, outValues)
      case _: KeyThenValue =>
        fillMarked(
          sources,
          prefix,
          from,
          until,
          numPartitions,
          tally.marks,
          cursor,
          outKeys,
          outValues,
        )
      case _: Slots =>
        fillMarked(
          sources,
          prefix,
          from,
          until,
          numPartitions,
          tally.marks,
          cursor,
          outKeys,
          outValues,
        )
    var expected = 0
    var part = 0
    val counts = tally.counts
    while (part < counts.length) {
      expected += counts(part)
      part += 1
    }
    require(written == expected, s"owned exchange wrote $written rows, counted $expected")
    written

  private def countAll(
      keys: Array[Int],
      start: Int,
      end: Int,
      numPartitions: Int,
      counts: Array[Int],
  ): Unit =
    if (numPartitions == 1) then counts(0) += end - start
    else
      val mask = powerMask(numPartitions)
      var row = start
      while (row < end) {
        counts(bucket(keys(row), numPartitions, mask)) += 1
        row += 1
      }

  private def words(span: Int): Array[Long] =
    if (span <= 0) then Array.emptyLongArray else new Array[Long]((span + 63) >>> 6)

  private def countKeyValue(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      bitAt: Int,
      numPartitions: Int,
      keepKey: Int => Boolean,
      keepValue: Int => Boolean,
      counts: Array[Int],
      marks: Array[Long],
  ): Unit =
    val mask = powerMask(numPartitions)
    var row = start
    var bit = bitAt
    while (row < end) {
      val key = keys(row)
      if (keepKey(key)) then
        val value = values(row)
        if (keepValue(value)) then
          counts(bucket(key, numPartitions, mask)) += 1
          Validity.setBit(marks, bit)
      row += 1
      bit += 1
    }

  private def countSlots(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      bitAt: Int,
      numPartitions: Int,
      slots: Array[Slot],
      counts: Array[Int],
      marks: Array[Long],
  ): Unit =
    val mask = powerMask(numPartitions)
    var row = start
    var bit = bitAt
    while (row < end) {
      val key = keys(row)
      if (passes(slots, key, values(row))) then
        counts(bucket(key, numPartitions, mask)) += 1
        Validity.setBit(marks, bit)
      row += 1
      bit += 1
    }

  private def fillAll(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      from: Long,
      until: Long,
      numPartitions: Int,
      cursor: Array[Int],
      outKeys: Array[Array[Int]],
      outValues: Array[Array[Int]],
  ): Int =
    var written = 0
    if (numPartitions == 1) then
      foreachRange(sources, prefix, from, until) { (keys, values, start, end, _) =>
        val width = end - start
        val at = cursor(0)
        System.arraycopy(keys, start, outKeys(0), at, width)
        System.arraycopy(values, start, outValues(0), at, width)
        cursor(0) = at + width
        written += width
      }
    else
      foreachRange(sources, prefix, from, until) { (keys, values, start, end, _) =>
        val mask = powerMask(numPartitions)
        var row = start
        while (row < end) {
          val part = bucket(keys(row), numPartitions, mask)
          val at = cursor(part)
          outKeys(part)(at) = keys(row)
          outValues(part)(at) = values(row)
          cursor(part) = at + 1
          written += 1
          row += 1
        }
      }
    written

  /** The count pass set the bit with the same slot predicates, so this pass does not call them. */
  private def fillMarked(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      from: Long,
      until: Long,
      numPartitions: Int,
      marks: Array[Long],
      cursor: Array[Int],
      outKeys: Array[Array[Int]],
      outValues: Array[Array[Int]],
  ): Int =
    var written = 0
    val mask = powerMask(numPartitions)
    foreachRange(sources, prefix, from, until) { (keys, values, start, end, bitAt) =>
      var row = start
      var bit = bitAt
      while (row < end) {
        if (Validity.isSet(marks, bit)) then
          val key = keys(row)
          val part = bucket(key, numPartitions, mask)
          val at = cursor(part)
          outKeys(part)(at) = key
          outValues(part)(at) = values(row)
          cursor(part) = at + 1
          written += 1
        row += 1
        bit += 1
      }
    }
    written

  /** `mask >= 0` when `numPartitions` is a power of two. `key & mask` is then `floorMod`. */
  private inline def powerMask(numPartitions: Int): Int =
    if ((numPartitions & (numPartitions - 1)) == 0) then numPartitions - 1 else -1

  private inline def bucket(key: Int, numPartitions: Int, mask: Int): Int =
    if (mask >= 0) then key & mask else Math.floorMod(key, numPartitions)

  private def passes(slots: Array[Slot], key: Int, value: Int): Boolean =
    var index = 0
    var valid = true
    while (valid && index < slots.length) {
      val slot = slots(index)
      val sample = if (slot.column == 0) then key else value
      valid = slot.keep(sample)
      index += 1
    }
    valid

  private def foreachRange(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      from: Long,
      until: Long,
  )(body: (Array[Int], Array[Int], Int, Int, Int) => Unit): Unit =
    var part = 0
    while (part < sources.length && prefix(part + 1) <= from) do part += 1
    var cursor = from
    while (cursor < until) {
      require(part < sources.length, "owned exchange stripe ran past the source")
      val partStart = prefix(part)
      val partEnd = prefix(part + 1)
      if (partEnd > cursor) then
        val localStart = (cursor - partStart).toInt
        val localEnd = (math.min(until, partEnd) - partStart).toInt
        if (localEnd > localStart) then
          val (keys, values) = sources(part)
          val bitAt = (partStart + localStart - from).toInt
          body(keys, values, localStart, localEnd, bitAt)
        val next = math.min(until, partEnd)
        require(next > cursor, "owned exchange stripe did not advance")
        cursor = next
      part += 1
    }

  /** The value arrays are the column. `rows == 0` publishes an empty pair batch instead. */
  private def publish(keys: Array[Int], values: Array[Int], rows: Int): ColumnBatch =
    if (rows == 0) then ColumnBatches.emptyIntPairs
    else
      new ColumnBatch(
        PairSchema,
        Vector(
          Int32Column.of(keys, Validity.allValid(rows), rows),
          Int32Column.of(values, Validity.allValid(rows), rows),
        ),
        rows,
      )
