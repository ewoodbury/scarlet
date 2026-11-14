package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Dense inner write for [[HashJoin]].
 *
 * A repeated build chain is walked once into a scratch run, then copied to each probe hit. Hits
 * keep probe order: the destination of a probe row is fixed before any chain is copied. An
 * equality hit publishes the probe key. One capacity covers every copy, and a start shorter than
 * the matches grows by doubling before the copies. Output arrays keep that capacity and `length`
 * is the match count. A null on the live prefix stays on the per-row writer.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object JoinWrite:

  /** Larger probes stay on the per-row writer. */
  val MaxProbeRows: Int = 65536

  /** True when every live probe key, probe value, and build value is present. */
  def dense(probeKeys: Int32Column, probeValues: Int32Column, buildValues: Int32Column): Boolean =
    Validity.livePresent(probeKeys.validity, probeKeys.length) &&
      Validity.livePresent(probeValues.validity, probeValues.length) &&
      Validity.livePresent(buildValues.validity, buildValues.length)

  /**
   * Inner matches for an all-present probe whose longest chain is longer than one. `reserved` is
   * the caller's row budget.
   */
  def writeGrouped(
      slots: JoinSlots,
      buildValues: Array[Int],
      probeKeys: Int32Column,
      probeValues: Int32Column,
      reserved: Int,
  ): ColumnBatch =
    val probeKeyValues = probeKeys.values
    val probeValueValues = probeValues.values
    val probeRows = probeKeys.length
    val slotCount = slots.slots
    val destOf = new Array[Int](probeRows)
    val slotOf = new Array[Int](probeRows)
    val hits = new Array[Int](slotCount)
    var row = 0
    var cursor = 0L
    while (row < probeRows) {
      val slot = slots.locate(probeKeyValues(row))
      if (slot >= 0) then
        destOf(row) = cursor.toInt
        slotOf(row) = slot
        hits(slot) += 1
        cursor += slots.chainAt(slot).toLong
      else slotOf(row) = -1
      row += 1
    }
    if (cursor == 0L) then ColumnBatches.emptyIntJoin
    else if (cursor > ColumnHash.MaxCapacity.toLong) then
      throw new IllegalStateException(s"join output reached $cursor rows")
    else
      val matched = cursor.toInt
      val slotStart = new Array[Int](slotCount)
      var slot = 0
      var running = 0
      while (slot < slotCount) {
        slotStart(slot) = running
        running += hits(slot)
        slot += 1
      }
      val ordered = new Array[Int](running)
      val next = Arrays.copyOf(slotStart, slotCount)
      row = 0
      while (row < probeRows) {
        val slot = slotOf(row)
        if (slot >= 0) then
          val at = next(slot)
          ordered(at) = row
          next(slot) = at + 1
        row += 1
      }
      val capacity = capacityFor(reserved, matched)
      val outKeys = new Array[Int](capacity)
      val outBuild = new Array[Int](capacity)
      val outProbe = new Array[Int](capacity)
      val scratch = new Array[Int](math.max(slots.maxChain, 1))
      slot = 0
      while (slot < slotCount) {
        val copies = hits(slot)
        if (copies > 0) then
          val count = slots.chainAt(slot)
          var buildRow = slots.headRow(slot)
          var index = 0
          while (index < count) {
            scratch(index) = buildValues(buildRow)
            buildRow = slots.nextRow(buildRow)
            index += 1
          }
          var copy = 0
          val begin = slotStart(slot)
          while (copy < copies) {
            val probeRow = ordered(begin + copy)
            val dest = destOf(probeRow)
            val end = dest + count
            Arrays.fill(outKeys, dest, end, probeKeyValues(probeRow))
            System.arraycopy(scratch, 0, outBuild, dest, count)
            Arrays.fill(outProbe, dest, end, probeValueValues(probeRow))
            copy += 1
          }
        slot += 1
      }
      // One prefix bitmap. Every live bit is set, and the words are not written after publish.
      val bits = Validity.prefixValid(capacity, matched)
      new ColumnBatch(
        Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
        Vector(
          Int32Column.of(outKeys, bits, matched),
          Int32Column.of(outBuild, bits, matched),
          Int32Column.of(outProbe, bits, matched),
        ),
        matched,
      )

  /** `reserved` when it already holds `rows`, otherwise the next doubling that does. */
  private def capacityFor(reserved: Int, rows: Int): Int =
    var capacity = math.max(reserved, 0)
    if (rows > capacity) then
      var next = if (capacity <= 0) then 1 else capacity
      while (next < rows) do next = doubled(next)
      next
    else capacity

  private def doubled(capacity: Int): Int =
    if (capacity >= ColumnHash.MaxCapacity) then
      throw new IllegalStateException(s"join output reached $capacity rows")
    capacity << 1
