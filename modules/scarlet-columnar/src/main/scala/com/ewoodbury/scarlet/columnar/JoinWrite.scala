package com.ewoodbury.scarlet.columnar

import java.util.Arrays

/**
 * Dense inner write for [[HashJoin]].
 *
 * A repeated build chain is walked once into a scratch run, then copied to each probe hit. Hits
 * keep probe order: the destination of a probe row is fixed before any chain is copied. An
 * equality hit publishes the probe key. One capacity covers every copy, and a start shorter than
 * the matches grows by doubling before the copies. Output arrays keep that capacity and `length`
 * is the match count.
 *
 * [[writeDense]] is the probe that is not grouped. Every live probe key, probe value, and build
 * value is present, so the row loop does not test a bit. A unique build that hits every probe row
 * writes at the probe index. A miss, or a chain longer than one, still compacts: the output slot
 * depends on how many rows were kept. A null on the live prefix stays on the per-row writer.
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

  /**
   * Inner matches when the probe keys, probe values, and build values are all present.
   * `probeBeforeBuild` publishes `(key, probe, build)`. Otherwise the columns are
   * `(key, build, probe)`. The key is the build key. `reserved` is the caller's row budget.
   */
  def writeDense(
      slots: JoinSlots,
      buildKeys: Array[Int],
      buildValues: Array[Int],
      probeKeyValues: Array[Int],
      probeValueValues: Array[Int],
      probeRows: Int,
      reserved: Int,
      probeBeforeBuild: Boolean,
  ): ColumnBatch =
    if (slots.maxChain <= 1) then
      writeUnique(
        slots,
        buildKeys,
        buildValues,
        probeKeyValues,
        probeValueValues,
        probeRows,
        math.max(reserved, probeRows),
        probeBeforeBuild,
      )
    else
      writeChains(
        slots,
        buildKeys,
        buildValues,
        probeKeyValues,
        probeValueValues,
        probeRows,
        reserved,
        probeBeforeBuild,
      )

  /**
   * Broadcast and other raw-word probes. `None` means a null remains on the live prefix, so the
   * caller keeps the per-row writer.
   */
  def writePresent(
      slots: JoinSlots,
      buildKeys: Array[Int],
      buildValues: Array[Int],
      buildValueWords: Array[Long],
      buildRows: Int,
      probeKeys: Int32Column,
      probeValues: Int32Column,
      reserved: Int,
      probeBeforeBuild: Boolean,
  ): Option[ColumnBatch] =
    val probeRows = probeKeys.length
    if (
        Validity.livePresent(probeKeys.validity, probeRows) &&
        Validity.livePresent(probeValues.validity, probeRows) &&
        Validity.livePresent(buildValueWords, buildRows)
      )
    then
      Some(
        writeDense(
          slots,
          buildKeys,
          buildValues,
          probeKeys.values,
          probeValues.values,
          probeRows,
          reserved,
          probeBeforeBuild,
        ),
      )
    else None

  /**
   * Multiplicity one. Rows that hit are stored at the probe index until the first miss. The tail
   * compacts. A full hit list never keeps a separate store index.
   */
  private def writeUnique(
      slots: JoinSlots,
      buildKeys: Array[Int],
      buildValues: Array[Int],
      probeKeyValues: Array[Int],
      probeValueValues: Array[Int],
      probeRows: Int,
      capacity: Int,
      probeBeforeBuild: Boolean,
  ): ColumnBatch =
    val out = new DenseBuffers(capacity)
    var row = 0
    var aligned = true
    while (aligned && row < probeRows) {
      val slot = slots.locate(probeKeyValues(row))
      if (slot < 0) then aligned = false
      else
        val buildRow = slots.headRow(slot)
        out.keys(row) = buildKeys(buildRow)
        out.build(row) = buildValues(buildRow)
        out.probe(row) = probeValueValues(row)
        row += 1
    }
    if (aligned) then
      publishDense(out.keys, out.build, out.probe, out.capacity, probeRows, probeBeforeBuild)
    else
      writeUniqueTail(
        out,
        slots,
        buildKeys,
        buildValues,
        probeKeyValues,
        probeValueValues,
        row + 1,
        row,
        probeRows,
        probeBeforeBuild,
      )

  private def writeUniqueTail(
      out: DenseBuffers,
      slots: JoinSlots,
      buildKeys: Array[Int],
      buildValues: Array[Int],
      probeKeyValues: Array[Int],
      probeValueValues: Array[Int],
      start: Int,
      filledAt: Int,
      probeRows: Int,
      probeBeforeBuild: Boolean,
  ): ColumnBatch =
    var filled = filledAt
    var row = start
    while (row < probeRows) {
      val slot = slots.locate(probeKeyValues(row))
      if (slot >= 0) then
        out.growTo(filled + 1)
        val buildRow = slots.headRow(slot)
        out.keys(filled) = buildKeys(buildRow)
        out.build(filled) = buildValues(buildRow)
        out.probe(filled) = probeValueValues(row)
        filled += 1
      row += 1
    }
    publishDense(out.keys, out.build, out.probe, out.capacity, filled, probeBeforeBuild)

  /**
   * Chain length varies, and a miss drops the probe row, so the destination cannot be the probe
   * index. The hash slot itself depends on the key. Neither is a straight scan.
   */
  private def writeChains(
      slots: JoinSlots,
      buildKeys: Array[Int],
      buildValues: Array[Int],
      probeKeyValues: Array[Int],
      probeValueValues: Array[Int],
      probeRows: Int,
      reserved: Int,
      probeBeforeBuild: Boolean,
  ): ColumnBatch =
    val out = new DenseBuffers(math.max(reserved, 0))
    var row = 0
    var filled = 0
    while (row < probeRows) {
      val slot = slots.locate(probeKeyValues(row))
      if (slot >= 0) then
        var buildRow = slots.headRow(slot)
        val probeValue = probeValueValues(row)
        while (buildRow != -1) {
          out.growTo(filled + 1)
          out.keys(filled) = buildKeys(buildRow)
          out.build(filled) = buildValues(buildRow)
          out.probe(filled) = probeValue
          filled += 1
          buildRow = slots.nextRow(buildRow)
        }
      row += 1
    }
    publishDense(out.keys, out.build, out.probe, out.capacity, filled, probeBeforeBuild)

  private def publishDense(
      keys: Array[Int],
      build: Array[Int],
      probe: Array[Int],
      capacity: Int,
      filled: Int,
      probeBeforeBuild: Boolean,
  ): ColumnBatch =
    if (filled == 0) then ColumnBatches.emptyIntJoin
    else
      val keyBits = Validity.prefixValid(capacity, filled)
      val leftBits = Validity.prefixValid(capacity, filled)
      val rightBits = Validity.prefixValid(capacity, filled)
      val leftValues = if (probeBeforeBuild) probe else build
      val rightValues = if (probeBeforeBuild) build else probe
      new ColumnBatch(
        Vector(LogicalType.Int32, LogicalType.Int32, LogicalType.Int32),
        Vector(
          Int32Column.of(keys, keyBits, filled),
          Int32Column.of(leftValues, leftBits, filled),
          Int32Column.of(rightValues, rightBits, filled),
        ),
        filled,
      )

  /**
   * Output arrays for one dense probe. Growth is a copy of the three arrays, not a row closure.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class DenseBuffers(initial: Int):
    var capacity: Int = math.max(initial, 0)
    var keys: Array[Int] = new Array[Int](capacity)
    var build: Array[Int] = new Array[Int](capacity)
    var probe: Array[Int] = new Array[Int](capacity)

    def growTo(needed: Int): Unit =
      if (needed > capacity) then
        var next = if (capacity <= 0) then 1 else capacity
        while (next < needed) do next = doubled(next)
        keys = Arrays.copyOf(keys, next)
        build = Arrays.copyOf(build, next)
        probe = Arrays.copyOf(probe, next)
        capacity = next

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
