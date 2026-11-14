package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Open-addressed key index for an int hash join.
 *
 * The table starts at [[HashJoin.InitialBuildSlots]], or at [[ColumnHash.tableSize]] when that is
 * smaller, and doubles when inserting a new key would push occupied slots above half the capacity.
 * A duplicate key links into the existing chain and does not grow the table, so a duplicate-heavy
 * build stays near the initial size. A unique build finishes at [[ColumnHash.tableSize]] of its
 * row count: one geometric rehash, the same final slot count as pre-sizing.
 *
 * Rehash copies each chain head into the new slot array and leaves the row links untouched, so the
 * newest-first walk is the same walk the insert built. A step whose retained estimate exceeds
 * `budget` throws before the new arrays are allocated. The estimate is [[HashJoin.retainedAt]].
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] final class JoinSlots(rowCount: Int, budget: Long):

  private var capacity: Int = JoinSlots.starting(rowCount)
  private var mask: Int = capacity - 1
  private var slotKey: Array[Int] = new Array[Int](capacity)
  private var head: Array[Int] = new Array[Int](capacity)
  private var chain: Array[Int] = new Array[Int](capacity)
  private val links: Array[Int] = new Array[Int](rowCount)
  private var occupiedCount: Int = 0
  private var longest: Int = 0

  Arrays.fill(head, -1)
  Arrays.fill(links, -1)

  def slots: Int = capacity

  /** Longest chain. One means every inserted key is unique. */
  def maxChain: Int = longest

  def insert(key: Int, row: Int): Unit =
    var slot = ColumnHash.mix(key) & mask
    var placed = false
    while (!placed) {
      if (head(slot) == -1) then
        if (occupiedCount >= (capacity >>> 1)) then
          grow()
          slot = ColumnHash.mix(key) & mask
        else
          head(slot) = row
          slotKey(slot) = key
          chain(slot) = 1
          occupiedCount += 1
          if (longest < 1) then longest = 1
          placed = true
      else if (slotKey(slot) == key) then
        links(row) = head(slot)
        head(slot) = row
        chain(slot) += 1
        if (chain(slot) > longest) then longest = chain(slot)
        placed = true
      else slot = (slot + 1) & mask
    }

  /** Slot holding `key`, or `-1` when the key was not inserted. */
  def locate(key: Int): Int =
    var slot = ColumnHash.mix(key) & mask
    var found = -1
    var scanning = true
    while (scanning) {
      if (head(slot) == -1) then scanning = false
      else if (slotKey(slot) == key) then
        found = slot
        scanning = false
      else slot = (slot + 1) & mask
    }
    found

  def headRow(slot: Int): Int = head(slot)

  def chainAt(slot: Int): Int = chain(slot)

  def nextRow(row: Int): Int = links(row)

  /**
   * Match rows a probe will emit. A present key that hits contributes its chain length. A miss,
   * and a null probe key, contribute one row when `keepMisses` is set and nothing otherwise.
   */
  def countOutput(
      probeKeys: Array[Int],
      probeWords: Array[Long],
      probeRows: Int,
      keepMisses: Boolean,
  ): Int =
    var total = 0L
    var row = 0
    while (row < probeRows) {
      if (!Validity.isSet(probeWords, row)) then
        if (keepMisses) then total += 1L
      else
        val slot = locate(probeKeys(row))
        if (slot >= 0) then total += chain(slot).toLong
        else if (keepMisses) then total += 1L
      row += 1
    }
    if (total > ColumnHash.MaxCapacity.toLong) then
      throw new IllegalStateException(s"join output reached $total rows")
    total.toInt

  private def grow(): Unit =
    val nextCapacity = capacity << 1
    if (nextCapacity <= 0 || nextCapacity > ColumnHash.MaxCapacity) then
      throw new IllegalStateException(s"join build table is already at capacity $capacity")
    val estimate = HashJoin.retainedAt(rowCount, nextCapacity)
    if (estimate > budget) then HashJoin.refuseSized(rowCount, estimate, budget)
    val oldHead = head
    val oldKey = slotKey
    val oldChain = chain
    val oldCapacity = capacity
    capacity = nextCapacity
    mask = capacity - 1
    head = new Array[Int](capacity)
    slotKey = new Array[Int](capacity)
    chain = new Array[Int](capacity)
    Arrays.fill(head, -1)
    var slot = 0
    while (slot < oldCapacity) {
      val top = oldHead(slot)
      if (top != -1) then
        val key = oldKey(slot)
        var dest = ColumnHash.mix(key) & mask
        while (head(dest) != -1) dest = (dest + 1) & mask
        head(dest) = top
        slotKey(dest) = key
        chain(dest) = oldChain(slot)
      slot += 1
    }

private[columnar] object JoinSlots:

  /** `tableSize` when the build is already small, otherwise the fixed initial capacity. */
  def starting(rows: Int): Int =
    val sized = ColumnHash.tableSize(math.max(rows, 0))
    if (sized <= HashJoin.InitialBuildSlots) sized else HashJoin.InitialBuildSlots
