package com.ewoodbury.scarlet.columnar

/**
 * Key multiplicities for an int inner-join count.
 *
 * The count is the sum over keys of build rows times probe rows. A value is never stored, and a
 * probe key that was not built is not inserted. The table starts at 4096 slots, or smaller when
 * constructed that way, and doubles when a new key would pass half full. A duplicate only adds to
 * the stored count.
 *
 * Keys in `0 until Window` never enter that probe. The scan increments `windowCounts(key)` and
 * mixes the key only when a read folds the window into the slots. A negative key, and a key at or
 * above [[Window]], still use the slots. Folding is once per read, not once per row.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object JoinKeyCount:

  /** Direct counters. `key & (Window - 1) == key` is exactly this range, so a negative misses. */
  private val Window: Int = 4096

  def fresh(): Table = new Table(4096)

  /**
   * `build.product(probe)`, and one fused-fold note. The count reads source arrays in place and
   * publishes no batch, so the note is what a metrics sink sees.
   */
  def multiply(build: Table, probe: Table): Long =
    ColumnMetrics.fused()
    build.product(probe)

  /**
   * `(key, build.stored(key) * probe.stored(key))` for keys stored on both sides. A miss is
   * omitted. The count is the low 32 bits, the same wrap an `Int` count uses. This does not sum
   * the products and does not insert into either table.
   */
  def productPairs(build: Table, probe: Table): (Array[Int], Array[Int]) =
    val buildKeys = build.storedKeyCount
    val probeKeys = probe.storedKeyCount
    val cap = math.min(buildKeys, probeKeys)
    if (cap == 0) then (Array.empty[Int], Array.empty[Int])
    else
      val outKeys = new Array[Int](cap)
      val outCounts = new Array[Int](cap)
      val written =
        if (probeKeys < buildKeys) then probe.writeProducts(build, outKeys, outCounts)
        else build.writeProducts(probe, outKeys, outCounts)
      if (written == cap) then (outKeys, outCounts)
      else
        (
          java.util.Arrays.copyOf(outKeys, written),
          java.util.Arrays.copyOf(outCounts, written),
        )

  /** Fold `parts` into the first table. An empty vector is an empty table. */
  def merge(parts: Vector[Table]): Table =
    val pending = parts.iterator
    if (!pending.hasNext) then fresh()
    else
      val into = pending.next()
      while (pending.hasNext) do into.absorb(pending.next())
      into

  /**
   * One open-addressed `key → count` map. Empty slots use a state byte, so a stored key may be
   * zero or negative. `product` multiplies this table by `probe` and does not insert into either.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  final class Table(initial: Int):
    private var capacity: Int = sized(initial)
    private var keys: Array[Int] = new Array[Int](capacity)
    private var counts: Array[Long] = new Array[Long](capacity)
    private var state: Array[Byte] = new Array[Byte](capacity)
    private var occupiedSlots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0
    private val windowCounts: Array[Long] = new Array[Long](Window)
    private var windowDirty: Boolean = false

    def addOne(key: Int): Unit = add(key, 1L)

    /**
     * Count every key in `[start, end)`. Same rules as [[addOne]], with the window array hoisted
     * out of the row loop.
     */
    def addAll(keys: Array[Int], start: Int, end: Int): Unit =
      val local = windowCounts
      val windowMask = local.length - 1
      var row = start
      var saw = false
      while (row < end) {
        val key = keys(row)
        val index = key & windowMask
        if (key == index) then
          local(index) = local(index) + 1L
          saw = true
        else hashAdd(key, 1L)
        row += 1
      }
      if (saw) windowDirty = true

    /**
     * Count pairs in `[start, end)` that pass both keeps. A kept key in the window increments one
     * slot. The mix runs only for a key outside it. Eight independent rows keep the predicate
     * latency off the next load.
     */
    def addFiltered(
        keys: Array[Int],
        values: Array[Int],
        start: Int,
        end: Int,
        keepKey: Int => Boolean,
        keepValue: Int => Boolean,
    ): Unit =
      val local = windowCounts
      val mask = local.length - 1
      var row = start
      val limit = end - 7
      while (row < limit) {
        tally(local, mask, keys(row), values(row), keepKey, keepValue)
        tally(local, mask, keys(row + 1), values(row + 1), keepKey, keepValue)
        tally(local, mask, keys(row + 2), values(row + 2), keepKey, keepValue)
        tally(local, mask, keys(row + 3), values(row + 3), keepKey, keepValue)
        tally(local, mask, keys(row + 4), values(row + 4), keepKey, keepValue)
        tally(local, mask, keys(row + 5), values(row + 5), keepKey, keepValue)
        tally(local, mask, keys(row + 6), values(row + 6), keepKey, keepValue)
        tally(local, mask, keys(row + 7), values(row + 7), keepKey, keepValue)
        row += 8
      }
      while (row < end) {
        tally(local, mask, keys(row), values(row), keepKey, keepValue)
        row += 1
      }
      if (start < end) windowDirty = true

    private inline def tally(
        local: Array[Long],
        mask: Int,
        key: Int,
        value: Int,
        keepKey: Int => Boolean,
        keepValue: Int => Boolean,
    ): Unit =
      if (keepKey(key) && keepValue(value)) then
        val index = key & mask
        if (key == index) then local(index) = local(index) + 1L
        else hashAdd(key, 1L)

    /** Stored count, or zero when `key` was never inserted. */
    def stored(key: Int): Long =
      foldWindow()
      val slot = seek(key)
      if (slot < 0) then 0L else counts(slot)

    def add(key: Int, n: Long): Unit =
      if (n != 0L) then
        val local = windowCounts
        val index = key & (local.length - 1)
        if (key == index) then
          local(index) = local(index) + n
          windowDirty = true
        else hashAdd(key, n)

    def absorb(other: Table): Unit =
      other.foldWindow()
      foldWindow()
      var index = 0
      val rows = other.occupied
      while (index < rows) {
        val slot = other.occupiedSlots(index)
        hashAdd(other.keys(slot), other.counts(slot))
        index += 1
      }

    /** Keys in the slots, after the window is folded. */
    private[JoinKeyCount] def storedKeyCount: Int =
      foldWindow()
      occupied

    /**
     * Write `(key, this * probe)` for keys present on both sides. `intoKeys` and `intoCounts` hold
     * at least [[storedKeyCount]] slots. Returns how many pairs were written. Does not sum and
     * does not insert.
     */
    private[JoinKeyCount] def writeProducts(
        probe: Table,
        intoKeys: Array[Int],
        intoCounts: Array[Int],
    ): Int =
      foldWindow()
      var index = 0
      var written = 0
      val rows = occupied
      while (index < rows) {
        val slot = occupiedSlots(index)
        val hits = probe.stored(keys(slot))
        if (hits != 0L) then
          intoKeys(written) = keys(slot)
          intoCounts(written) = (hits * counts(slot)).toInt
          written += 1
        index += 1
      }
      written

    /**
     * Sum of `build * probe` over keys present on both sides. A key stored on only one side adds
     * nothing and is not written into the other table.
     */
    def product(probe: Table): Long =
      foldWindow()
      probe.foldWindow()
      val probeSmaller = probe.occupied < occupied
      val small = if (probeSmaller) probe else this
      val large = if (probeSmaller) this else probe
      var index = 0
      var sum = 0L
      val rows = small.occupied
      while (index < rows) {
        val slot = small.occupiedSlots(index)
        val hits = large.stored(small.keys(slot))
        if (hits != 0L) then sum += hits * small.counts(slot)
        index += 1
      }
      sum

    /**
     * Move window counters into the slots. Dirty stays set until the copy finishes, so a failed
     * insert can be retried without dropping the counters still in the window.
     */
    private def foldWindow(): Unit =
      if (windowDirty) then
        val local = windowCounts
        var index = 0
        val length = local.length
        while (index < length) {
          val n = local(index)
          if (n != 0L) then
            hashAdd(index, n)
            local(index) = 0L
          index += 1
        }
        windowDirty = false

    /** Open-addressed insert. Mixes `key` once. Does not look at the window. */
    private def hashAdd(key: Int, n: Long): Unit =
      val mixed = ColumnHash.mix(key)
      val found = seekMixed(mixed, key)
      if (found >= 0) then counts(found) += n
      else
        if (occupied >= (capacity >>> 1)) then grow()
        placeMixed(mixed, key, n)

    private def seek(key: Int): Int =
      seekMixed(ColumnHash.mix(key), key)

    private def seekMixed(mixed: Int, key: Int): Int =
      val localState = state
      val localKeys = keys
      val localMask = localKeys.length - 1
      var slot = mixed & localMask
      var found = -1
      var scanning = true
      while (scanning) {
        if (localState(slot) == 0) then scanning = false
        else if (localKeys(slot) == key) then
          found = slot
          scanning = false
        else slot = (slot + 1) & localMask
      }
      found

    /** `key` is absent, and a free slot exists. `mixed` is [[ColumnHash.mix]]. */
    private def placeMixed(mixed: Int, key: Int, n: Long): Unit =
      val localState = state
      val localKeys = keys
      val localCounts = counts
      val localMask = localKeys.length - 1
      var slot = mixed & localMask
      var placed = false
      while (!placed) {
        if (localState(slot) == 0) then
          localState(slot) = 1
          localKeys(slot) = key
          localCounts(slot) = n
          occupiedSlots(occupied) = slot
          occupied += 1
          placed = true
        else slot = (slot + 1) & localMask
      }

    private def grow(): Unit =
      val next = capacity << 1
      if (next <= 0 || next > ColumnHash.MaxCapacity) then
        throw new IllegalStateException(s"join count table is already at capacity $capacity")
      val oldKeys = keys
      val oldCounts = counts
      val oldSlots = occupiedSlots
      val oldRows = occupied
      capacity = next
      keys = new Array[Int](capacity)
      counts = new Array[Long](capacity)
      state = new Array[Byte](capacity)
      occupiedSlots = new Array[Int](capacity)
      occupied = 0
      var index = 0
      while (index < oldRows) {
        val slot = oldSlots(index)
        placeMixed(ColumnHash.mix(oldKeys(slot)), oldKeys(slot), oldCounts(slot))
        index += 1
      }

  private def sized(requested: Int): Int =
    val need = math.max(16, requested)
    if (need >= ColumnHash.MaxCapacity) then ColumnHash.MaxCapacity
    else Integer.highestOneBit(need - 1) << 1
