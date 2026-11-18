package com.ewoodbury.scarlet.columnar

/**
 * Hash-partitions owned int pairs straight into shuffle buckets.
 *
 * The bucket is `floorMod(key, numPartitions)`, the same rule as [[HashExchange.partitionInt32]].
 * Order inside a bucket is source order. An owned array has no null, so a null key is not dropped
 * here and a null value is not carried. Those sides stay on [[HashExchange.partitionAllSelected]].
 *
 * A slot filter runs once per row. The first sum of a shape counts cache-sized tiles and copies
 * those survivors into the bucket arrays. A later sum of the same shape and the same survivor
 * counts writes those buckets in one pass, at the offsets learned from that count, and does not
 * copy a tile again. A count that does not match is discarded and counted again. The columns alias
 * the bucket arrays. A dense single partition aliases the source arrays when there is one of them,
 * and otherwise copies each source once with `arraycopy`. Nothing here trims a full-width survivor
 * buffer.
 *
 * [[begin]] reuses this thread's bucket arrays on the next sum. The previous buckets must already
 * have been read: the next [[partition]] overwrites them.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object OwnedPairExchange:

  private val Schema = Vector(LogicalType.Int32, LogicalType.Int32)

  /** Rows per tile. Keys and values of one tile stay inside a core's cache while it is filled. */
  private val TileRows = 16384L

  sealed trait Keep

  /** Every row. No predicate runs. */
  private[scarlet] case object AllRows extends Keep

  /**
   * Drops this thread's claim on bucket arrays from the previous sum. The next [[partition]] may
   * overwrite them. Call it before either side is exchanged, and only after the previous buckets
   * have been consumed.
   */
  def begin(): Unit = scratch.get().finals.reset()

  def partition(
      parts: Vector[(Array[Int], Array[Int])],
      keep: Keep,
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (parts.isEmpty) then empties(numPartitions)
    else
      val sources = parts.toArray
      val prefix = prefixOf(sources)
      val total = prefix(sources.length)
      if (total == 0L) then empties(numPartitions)
      else
        require(total <= Int.MaxValue.toLong, s"owned exchange has $total rows")
        keep match
          case AllRows if numPartitions == 1 => dense(sources, total)
          case _ =>
            val tiles = bounds(total, tileCount(total, parallelism))
            val plan = scratch.get().planFor(total, tiles.length, numPartitions)
            if (plan.matches(total, tiles.length, numPartitions)) then
              direct(sources, prefix, tiles, keep, numPartitions, parallelism, plan) match
                case Some(batches) => batches
                case None =>
                  plan.hot = false
                  counted(sources, prefix, tiles, keep, numPartitions, parallelism, plan, total)
            else counted(sources, prefix, tiles, keep, numPartitions, parallelism, plan, total)

  /** `keepKey` then `keepValue`, the same two-slot order the owned count uses. */
  def keyThenValue(keepKey: Int => Boolean, keepValue: Int => Boolean): Keep =
    new KeyThenValue(keepKey, keepValue)

  /** Slot order is the predicate order. Column 0 is the key and column 1 is the value. */
  def generic(columns: Array[Int], keeps: Array[Int => Boolean]): Keep =
    require(columns.length == keeps.length, "slot columns and predicates differ in length")
    val slots = new Array[Slot](columns.length)
    var index = 0
    while (index < columns.length) {
      slots(index) = new Slot(columns(index), keeps(index))
      index += 1
    }
    new GenericSlots(slots)

  private val scratch = new ThreadLocal[Scratch]:
    override def initialValue(): Scratch = new Scratch

  private final class Scratch:
    val finals: Pool = new Pool
    private val plans: Array[Learned] =
      Array(new Learned, new Learned, new Learned, new Learned)

    /** A hot plan for this shape, or a cold slot the caller fills. */
    def planFor(rowCount: Long, tileCount: Int, numPartitions: Int): Learned =
      var index = 0
      var free = -1
      var found = plans(0)
      var hit = false
      while (index < plans.length && !hit) {
        val plan = plans(index)
        if (plan.matches(rowCount, tileCount, numPartitions)) then
          found = plan
          hit = true
        else if (!plan.hot && free < 0) then free = index
        index += 1
      }
      if (hit) then found
      else if (free >= 0) then plans(free)
      else plans(0)

  private final class KeyThenValue(val keepKey: Int => Boolean, val keepValue: Int => Boolean)
      extends Keep

  private final class GenericSlots(val slots: Array[Slot]) extends Keep

  private final class Slot(val column: Int, val keep: Int => Boolean)

  private final class Stripe(val from: Long, val until: Long)

  /** Survivors of one tile, already split by bucket. A zero count has no array. */
  private final class Piece(
      val counts: Array[Int],
      val keys: Array[Array[Int]],
      val values: Array[Array[Int]],
  )

  /** Survivor counts of one shape. Offsets are exclusive ranges inside the bucket arrays. */
  private final class Learned:
    var hot: Boolean = false
    var rows: Long = 0L
    var tiles: Int = 0
    var partitions: Int = 0
    var counts: Array[Array[Int]] = Array.empty[Array[Int]]
    var offsets: Array[Array[Int]] = Array.empty[Array[Int]]
    var totals: Array[Int] = Array.empty[Int]

    def matches(rowCount: Long, tileCount: Int, numPartitions: Int): Boolean =
      hot && rows == rowCount && tiles == tileCount && partitions == numPartitions

    def remember(pieces: Array[Piece], numPartitions: Int, rowCount: Long): Unit =
      val tileCount = pieces.length
      val nextCounts = Array.ofDim[Int](tileCount, numPartitions)
      val nextOffsets = Array.ofDim[Int](tileCount, numPartitions)
      val nextTotals = new Array[Int](numPartitions)
      var tile = 0
      while (tile < tileCount) {
        val pieceCounts = pieces(tile).counts
        var part = 0
        while (part < numPartitions) {
          nextOffsets(tile)(part) = nextTotals(part)
          val survivors = pieceCounts(part)
          nextCounts(tile)(part) = survivors
          val next = nextTotals(part) + survivors
          require(next >= nextTotals(part), "shuffle bucket does not fit")
          nextTotals(part) = next
          part += 1
        }
        tile += 1
      }
      counts = nextCounts
      offsets = nextOffsets
      totals = nextTotals
      rows = rowCount
      tiles = tileCount
      partitions = numPartitions
      hot = true

  private final class Pool:
    private var sizes: Array[Int] = new Array[Int](8)
    private var bins: Array[Bin] = new Array[Bin](8)
    private var binCount: Int = 0

    def reset(): Unit =
      var index = 0
      while (index < binCount) {
        bins(index).cursor = 0
        index += 1
      }

    def take(size: Int): Array[Int] =
      val bin = find(size)
      if (bin.cursor < bin.count) then
        val array = bin.arrays(bin.cursor)
        bin.cursor += 1
        array
      else
        val array = new Array[Int](size)
        bin.add(array)
        array

    private def find(size: Int): Bin =
      var index = 0
      var found = -1
      while (index < binCount && found < 0) {
        if (sizes(index) == size) then found = index
        index += 1
      }
      if (found >= 0) then bins(found)
      else grow(size)

    private def grow(size: Int): Bin =
      if (binCount == sizes.length) then
        val grownSizes = new Array[Int](binCount * 2)
        val grownBins = new Array[Bin](binCount * 2)
        System.arraycopy(sizes, 0, grownSizes, 0, binCount)
        System.arraycopy(bins, 0, grownBins, 0, binCount)
        sizes = grownSizes
        bins = grownBins
      val bin = new Bin
      sizes(binCount) = size
      bins(binCount) = bin
      binCount += 1
      bin

  private final class Bin:
    var cursor: Int = 0
    var count: Int = 0
    var arrays: Array[Array[Int]] = new Array[Array[Int]](4)

    def add(array: Array[Int]): Unit =
      if (count == arrays.length) then
        val grown = new Array[Array[Int]](count * 2)
        System.arraycopy(arrays, 0, grown, 0, count)
        arrays = grown
      arrays(count) = array
      count += 1
      cursor = count

  private def empties(numPartitions: Int): Vector[ColumnBatch] =
    Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)

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

  /** One source is aliased. Several sources are copied once, in source order, at exact size. */
  private def dense(
      sources: Array[(Array[Int], Array[Int])],
      total: Long,
  ): Vector[ColumnBatch] =
    if (sources.length == 1) then
      val (keys, values) = sources(0)
      Vector(columns(keys, values, keys.length))
    else
      val rows = total.toInt
      val keys = new Array[Int](rows)
      val values = new Array[Int](rows)
      var cursor = 0
      var index = 0
      while (index < sources.length) {
        val (partKeys, partValues) = sources(index)
        val width = partKeys.length
        System.arraycopy(partKeys, 0, keys, cursor, width)
        System.arraycopy(partValues, 0, values, cursor, width)
        cursor += width
        index += 1
      }
      Vector(columns(keys, values, rows))

  private def tileCount(total: Long, parallelism: Int): Int =
    val tiles = ((total + TileRows - 1L) / TileRows).toInt
    val least = math.min(parallelism, total.toInt)
    math.max(least, math.min(tiles, total.toInt))

  private def bounds(total: Long, width: Int): Vector[Stripe] =
    val base = total / width
    val extra = (total % width).toInt
    Vector.tabulate(width) { worker =>
      val start = worker.toLong * base + math.min(worker, extra).toLong
      val length = base + (if (worker < extra) then 1L else 0L)
      new Stripe(start, start + length)
    }

  private def cut(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      tile: Stripe,
      keep: Keep,
      numPartitions: Int,
  ): Piece =
    val counts = new Array[Int](numPartitions)
    val length = (tile.until - tile.from).toInt
    val kept = keep match
      case AllRows => Array.emptyLongArray
      case _ => new Array[Long]((length + 63) >>> 6)
    scan(sources, prefix, tile.from, tile.until) { (keys, values, start, end, bitBase) =>
      keep match
        case AllRows =>
          countAll(keys, start, end, numPartitions, counts)
        case filtered: KeyThenValue =>
          countKeyValue(
            keys,
            values,
            start,
            end,
            filtered.keepKey,
            filtered.keepValue,
            numPartitions,
            counts,
            kept,
            bitBase,
          )
        case filtered: GenericSlots =>
          countGeneric(
            keys,
            values,
            start,
            end,
            filtered.slots,
            numPartitions,
            counts,
            kept,
            bitBase,
          )
    }
    val keys = new Array[Array[Int]](numPartitions)
    val values = new Array[Array[Int]](numPartitions)
    var part = 0
    while (part < numPartitions) {
      val rows = counts(part)
      if (rows == 0) then
        keys(part) = Array.emptyIntArray
        values(part) = Array.emptyIntArray
      else
        keys(part) = new Array[Int](rows)
        values(part) = new Array[Int](rows)
      part += 1
    }
    val cursor = new Array[Int](numPartitions)
    fill(sources, prefix, tile, keep, numPartitions, keys, values, kept, cursor)
    part = 0
    while (part < numPartitions) {
      require(cursor(part) == counts(part), "owned exchange wrote a different survivor count")
      part += 1
    }
    new Piece(counts, keys, values)

  private def counted(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      tiles: Vector[Stripe],
      keep: Keep,
      numPartitions: Int,
      parallelism: Int,
      plan: Learned,
      total: Long,
  ): Vector[ColumnBatch] =
    val pieces = MorselScheduler
      .map(tiles, parallelism) { (_, tile) =>
        cut(sources, prefix, tile, keep, numPartitions)
      }
      .toArray
    plan.remember(pieces, numPartitions, total)
    stitch(pieces, plan.totals, numPartitions, parallelism)

  /**
   * One pass into the learned ranges. `None` when a tile does not produce the learned count, in
   * which case the caller counts again. The bucket arrays taken here are left for [[begin]] to
   * reuse; they are not published.
   */
  private def direct(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      tiles: Vector[Stripe],
      keep: Keep,
      numPartitions: Int,
      parallelism: Int,
      plan: Learned,
  ): Option[Vector[ColumnBatch]] =
    val totals = plan.totals
    val (outKeys, outValues) = bucketArrays(totals)
    val misses = MorselScheduler.map(tiles, parallelism) { (index, tile) =>
      val wrote = new Array[Int](numPartitions)
      var overflow = false
      scan(sources, prefix, tile.from, tile.until) { (keys, values, start, end, _) =>
        val spilled = keep match
          case AllRows =>
            writeAll(
              keys,
              values,
              start,
              end,
              numPartitions,
              outKeys,
              outValues,
              plan.offsets(index),
              plan.counts(index),
              wrote,
            )
          case filtered: KeyThenValue =>
            writeKeyValue(
              keys,
              values,
              start,
              end,
              filtered.keepKey,
              filtered.keepValue,
              numPartitions,
              outKeys,
              outValues,
              plan.offsets(index),
              plan.counts(index),
              wrote,
            )
          case filtered: GenericSlots =>
            writeGeneric(
              keys,
              values,
              start,
              end,
              filtered.slots,
              numPartitions,
              outKeys,
              outValues,
              plan.offsets(index),
              plan.counts(index),
              wrote,
            )
        if (spilled) then overflow = true
      }
      var bucket = 0
      var miss = overflow
      while (bucket < numPartitions && !miss) {
        if (wrote(bucket) != plan.counts(index)(bucket)) then miss = true
        bucket += 1
      }
      if (miss) then 1 else 0
    }
    val flags = misses.toArray
    var bad = 0
    var index = 0
    while (index < flags.length) {
      bad += flags(index)
      index += 1
    }
    if (bad == 0) then Some(publish(outKeys, outValues, totals)) else None

  private def stitch(
      pieces: Array[Piece],
      totals: Array[Int],
      numPartitions: Int,
      parallelism: Int,
  ): Vector[ColumnBatch] =
    val (outKeys, outValues) = bucketArrays(totals)
    val buckets = Vector.tabulate(numPartitions)(identity)
    val _ = MorselScheduler.map(buckets, parallelism) { (bucket, _) =>
      var cursor = 0
      var piece = 0
      while (piece < pieces.length) {
        val rows = pieces(piece).counts(bucket)
        if (rows > 0) then
          System.arraycopy(pieces(piece).keys(bucket), 0, outKeys(bucket), cursor, rows)
          System.arraycopy(pieces(piece).values(bucket), 0, outValues(bucket), cursor, rows)
          cursor += rows
        piece += 1
      }
      require(cursor == totals(bucket), "owned exchange stitched a different survivor count")
      0
    }
    publish(outKeys, outValues, totals)

  private def bucketArrays(totals: Array[Int]): (Array[Array[Int]], Array[Array[Int]]) =
    val pool = scratch.get().finals
    val keys = new Array[Array[Int]](totals.length)
    val values = new Array[Array[Int]](totals.length)
    var part = 0
    while (part < totals.length) {
      val rows = totals(part)
      if (rows == 0) then
        keys(part) = Array.emptyIntArray
        values(part) = Array.emptyIntArray
      else
        keys(part) = pool.take(rows)
        values(part) = pool.take(rows)
      part += 1
    }
    (keys, values)

  /** Writes kept rows. Returns true when a bucket would pass its learned range. */
  private def writeAll(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      numPartitions: Int,
      outKeys: Array[Array[Int]],
      outValues: Array[Array[Int]],
      origin: Array[Int],
      allow: Array[Int],
      wrote: Array[Int],
  ): Boolean =
    var row = start
    var overflow = false
    while (row < end) {
      val key = keys(row)
      val bucket = Math.floorMod(key, numPartitions)
      val n = wrote(bucket)
      if (n < allow(bucket)) then
        val at = origin(bucket) + n
        outKeys(bucket)(at) = key
        outValues(bucket)(at) = values(row)
        wrote(bucket) = n + 1
      else overflow = true
      row += 1
    }
    overflow

  private def writeKeyValue(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keepKey: Int => Boolean,
      keepValue: Int => Boolean,
      numPartitions: Int,
      outKeys: Array[Array[Int]],
      outValues: Array[Array[Int]],
      origin: Array[Int],
      allow: Array[Int],
      wrote: Array[Int],
  ): Boolean =
    var row = start
    var overflow = false
    while (row < end) {
      val key = keys(row)
      if (keepKey(key) && keepValue(values(row))) then
        val bucket = Math.floorMod(key, numPartitions)
        val n = wrote(bucket)
        if (n < allow(bucket)) then
          val at = origin(bucket) + n
          outKeys(bucket)(at) = key
          outValues(bucket)(at) = values(row)
          wrote(bucket) = n + 1
        else overflow = true
      row += 1
    }
    overflow

  private def writeGeneric(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      slots: Array[Slot],
      numPartitions: Int,
      outKeys: Array[Array[Int]],
      outValues: Array[Array[Int]],
      origin: Array[Int],
      allow: Array[Int],
      wrote: Array[Int],
  ): Boolean =
    var row = start
    var overflow = false
    while (row < end) {
      val key = keys(row)
      val value = values(row)
      if (passes(slots, key, value)) then
        val bucket = Math.floorMod(key, numPartitions)
        val n = wrote(bucket)
        if (n < allow(bucket)) then
          val at = origin(bucket) + n
          outKeys(bucket)(at) = key
          outValues(bucket)(at) = values(row)
          wrote(bucket) = n + 1
        else overflow = true
      row += 1
    }
    overflow

  private def publish(
      keys: Array[Array[Int]],
      values: Array[Array[Int]],
      totals: Array[Int],
  ): Vector[ColumnBatch] =
    val batches = new Array[ColumnBatch](totals.length)
    var part = 0
    while (part < totals.length) {
      val rows = totals(part)
      batches(part) =
        if (rows == 0) then ColumnBatches.emptyIntPairs
        else columns(keys(part), values(part), rows)
      part += 1
    }
    batches.toVector

  private def columns(keys: Array[Int], values: Array[Int], rows: Int): ColumnBatch =
    new ColumnBatch(
      Schema,
      Vector(
        Int32Column.of(keys, Validity.allValid(rows), rows),
        Int32Column.of(values, Validity.allValid(rows), rows),
      ),
      rows,
    )

  private def scan(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      from: Long,
      until: Long,
  )(
      use: (Array[Int], Array[Int], Int, Int, Int) => Unit,
  ): Unit =
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
          use(keys, values, localStart, localEnd, (cursor - from).toInt)
        val next = math.min(until, partEnd)
        require(next > cursor, "owned exchange stripe did not advance")
        cursor = next
      part += 1
    }

  private def countAll(
      keys: Array[Int],
      start: Int,
      end: Int,
      numPartitions: Int,
      counts: Array[Int],
  ): Unit =
    var row = start
    while (row < end) {
      counts(Math.floorMod(keys(row), numPartitions)) += 1
      row += 1
    }

  private def countKeyValue(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      keepKey: Int => Boolean,
      keepValue: Int => Boolean,
      numPartitions: Int,
      counts: Array[Int],
      kept: Array[Long],
      bitBase: Int,
  ): Unit =
    var row = start
    var bit = bitBase
    while (row < end) {
      val key = keys(row)
      if (keepKey(key) && keepValue(values(row))) then
        Validity.setBit(kept, bit)
        counts(Math.floorMod(key, numPartitions)) += 1
      bit += 1
      row += 1
    }

  private def countGeneric(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      slots: Array[Slot],
      numPartitions: Int,
      counts: Array[Int],
      kept: Array[Long],
      bitBase: Int,
  ): Unit =
    var row = start
    var bit = bitBase
    while (row < end) {
      val key = keys(row)
      val value = values(row)
      if (passes(slots, key, value)) then
        Validity.setBit(kept, bit)
        counts(Math.floorMod(key, numPartitions)) += 1
      bit += 1
      row += 1
    }

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

  private def fill(
      sources: Array[(Array[Int], Array[Int])],
      prefix: Array[Long],
      tile: Stripe,
      keep: Keep,
      numPartitions: Int,
      outKeys: Array[Array[Int]],
      outValues: Array[Array[Int]],
      kept: Array[Long],
      cursor: Array[Int],
  ): Unit =
    scan(sources, prefix, tile.from, tile.until) { (keys, values, start, end, bitBase) =>
      keep match
        case AllRows =>
          fillAll(keys, values, start, end, numPartitions, outKeys, outValues, cursor)
        case _: KeyThenValue | _: GenericSlots =>
          fillKept(
            keys,
            values,
            start,
            end,
            numPartitions,
            outKeys,
            outValues,
            kept,
            bitBase,
            cursor,
          )
    }

  private def fillAll(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      numPartitions: Int,
      outKeys: Array[Array[Int]],
      outValues: Array[Array[Int]],
      cursor: Array[Int],
  ): Unit =
    var row = start
    while (row < end) {
      val key = keys(row)
      val bucket = Math.floorMod(key, numPartitions)
      val at = cursor(bucket)
      outKeys(bucket)(at) = key
      outValues(bucket)(at) = values(row)
      cursor(bucket) = at + 1
      row += 1
    }

  private def fillKept(
      keys: Array[Int],
      values: Array[Int],
      start: Int,
      end: Int,
      numPartitions: Int,
      outKeys: Array[Array[Int]],
      outValues: Array[Array[Int]],
      kept: Array[Long],
      bitBase: Int,
      cursor: Array[Int],
  ): Unit =
    var row = start
    var bit = bitBase
    while (row < end) {
      if (Validity.isSet(kept, bit)) then
        val key = keys(row)
        val bucket = Math.floorMod(key, numPartitions)
        val at = cursor(bucket)
        outKeys(bucket)(at) = key
        outValues(bucket)(at) = values(row)
        cursor(bucket) = at + 1
      bit += 1
      row += 1
    }
