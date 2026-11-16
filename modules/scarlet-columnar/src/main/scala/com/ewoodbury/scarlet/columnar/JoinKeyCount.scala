package com.ewoodbury.scarlet.columnar

/**
 * Key multiplicities for an int inner-join count.
 *
 * The count is the sum over keys of build rows times probe rows. A value is never stored, and a
 * probe key that was not built is not inserted. The table starts at 4096 slots, or smaller when
 * constructed that way, and doubles when a new key would pass half full. A duplicate only adds to
 * the stored count.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object JoinKeyCount:

  def fresh(): Table = new Table(4096)

  /**
   * `build.product(probe)`, and one fused-fold note. The count reads source arrays in place and
   * publishes no batch, so the note is what a metrics sink sees.
   */
  def multiply(build: Table, probe: Table): Long =
    ColumnMetrics.fused()
    build.product(probe)

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
    private var mask: Int = capacity - 1
    private var keys: Array[Int] = new Array[Int](capacity)
    private var counts: Array[Long] = new Array[Long](capacity)
    private var state: Array[Byte] = new Array[Byte](capacity)
    private var occupiedSlots: Array[Int] = new Array[Int](capacity)
    private var occupied: Int = 0

    def addOne(key: Int): Unit = add(key, 1L)

    /** Stored count, or zero when `key` was never inserted. */
    def stored(key: Int): Long =
      val slot = seek(key)
      if (slot < 0) then 0L else counts(slot)

    def add(key: Int, n: Long): Unit =
      if (n != 0L) then
        val found = seek(key)
        if (found >= 0) then counts(found) += n
        else
          if (occupied >= (capacity >>> 1)) then grow()
          placeNew(key, n)

    def absorb(other: Table): Unit =
      var index = 0
      val rows = other.occupied
      while (index < rows) {
        val slot = other.occupiedSlots(index)
        add(other.keys(slot), other.counts(slot))
        index += 1
      }

    /**
     * Sum of `build * probe` over keys present on both sides. A key stored on only one side adds
     * nothing and is not written into the other table.
     */
    def product(probe: Table): Long =
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

    private def seek(key: Int): Int =
      var slot = ColumnHash.mix(key) & mask
      var found = -1
      var scanning = true
      while (scanning) {
        if (state(slot) == 0) then scanning = false
        else if (keys(slot) == key) then
          found = slot
          scanning = false
        else slot = (slot + 1) & mask
      }
      found

    /** `key` is absent, and a free slot exists. */
    private def placeNew(key: Int, n: Long): Unit =
      var slot = ColumnHash.mix(key) & mask
      var placed = false
      while (!placed) {
        if (state(slot) == 0) then
          state(slot) = 1
          keys(slot) = key
          counts(slot) = n
          occupiedSlots(occupied) = slot
          occupied += 1
          placed = true
        else slot = (slot + 1) & mask
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
      mask = capacity - 1
      keys = new Array[Int](capacity)
      counts = new Array[Long](capacity)
      state = new Array[Byte](capacity)
      occupiedSlots = new Array[Int](capacity)
      occupied = 0
      var index = 0
      while (index < oldRows) {
        val slot = oldSlots(index)
        placeNew(oldKeys(slot), oldCounts(slot))
        index += 1
      }

  private def sized(requested: Int): Int =
    val need = math.max(16, requested)
    if (need >= ColumnHash.MaxCapacity) then ColumnHash.MaxCapacity
    else Integer.highestOneBit(need - 1) << 1
