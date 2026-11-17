package com.ewoodbury.scarlet.columnar

/**
 * Grouped sum of one int join column, updated from the probe.
 *
 * The build is the left input and the probe is the right, the same sides as
 * [[HashJoin.innerInt32]]. A null key is not inserted and is not a probe hit. A surviving key's
 * sum is `buildSum * probeCount` or `probeSum * buildCount`. Both products equal adding that
 * column once per inner match. The match columns are not written.
 *
 * A null measured value declines. The row path unboxes that value only for a match; declining the
 * bucket keeps the same answer, including when the null row matches nothing. A null on the other
 * side still matches.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object JoinProbeSum:

  enum Measure:
    case Left, Right

  def sum(build: HashJoin.JoinSide, probe: HashJoin.JoinSide, measure: Measure): ColumnBatch =
    val buildKeys = ColumnHash.int32At(build.batch, build.key)
    val buildValues = ColumnHash.int32At(build.batch, build.value)
    val probeKeys = ColumnHash.int32At(probe.batch, probe.key)
    val probeValues = ColumnHash.int32At(probe.batch, probe.value)
    require(buildKeys.length == buildValues.length, "build key and value columns differ in length")
    require(probeKeys.length == probeValues.length, "probe key and value columns differ in length")
    if (buildKeys.length == 0 || probeKeys.length == 0) then ColumnBatches.emptyIntLongSum
    else
      measure match
        case Measure.Left =>
          refuseNull(buildValues, "a null left value cannot be summed from the join probe")
          sumLeft(buildKeys, buildValues, probeKeys)
        case Measure.Right =>
          refuseNull(probeValues, "a null right value cannot be summed from the join probe")
          sumRight(buildKeys, probeKeys, probeValues)

  private def refuseNull(column: Int32Column, message: String): Unit =
    if (!Validity.livePresent(column.validity, column.length)) then
      throw new ColumnarFallback(message)

  private def sumLeft(
      buildKeys: Int32Column,
      buildValues: Int32Column,
      probeKeys: Int32Column,
  ): ColumnBatch =
    val slots = index(buildKeys)
    val totals = new Array[Long](slots.slots)
    val keys = new Array[Int](slots.slots)
    foldChains(slots, buildKeys.values, buildValues.values, totals, keys)
    val hits = countHits(slots, probeKeys)
    emit(keys, totals, hits)

  private def sumRight(
      buildKeys: Int32Column,
      probeKeys: Int32Column,
      probeValues: Int32Column,
  ): ColumnBatch =
    val slots = index(buildKeys)
    val keys = new Array[Int](slots.slots)
    rememberKeys(slots, buildKeys.values, keys)
    val totals = new Array[Long](slots.slots)
    val weights = new Array[Long](slots.slots)
    foldProbe(slots, probeKeys, probeValues, totals, weights)
    emit(keys, totals, weights)

  /** `weights(slot) != 0` emits `totals(slot) * weights(slot)`, including a zero total. */
  private def emit(keys: Array[Int], totals: Array[Long], weights: Array[Long]): ColumnBatch =
    HashAggregate.consume { add =>
      var slot = 0
      val width = keys.length
      while (slot < width) {
        val weight = weights(slot)
        if (weight != 0L) then add(keys(slot), totals(slot) * weight)
        slot += 1
      }
    }

  private def index(keys: Int32Column): JoinSlots =
    val rows = keys.length
    val slots = new JoinSlots(rows, Long.MaxValue)
    val keyValues = keys.values
    var row = 0
    if (Validity.livePresent(keys.validity, rows)) then
      while (row < rows) {
        slots.insert(keyValues(row), row)
        row += 1
      }
    else
      val words = keys.validity.words
      while (row < rows) {
        if (Validity.isSet(words, row)) then slots.insert(keyValues(row), row)
        row += 1
      }
    slots

  private def foldChains(
      slots: JoinSlots,
      keyValues: Array[Int],
      valueValues: Array[Int],
      totals: Array[Long],
      keys: Array[Int],
  ): Unit =
    var slot = 0
    val width = slots.slots
    while (slot < width) {
      var row = slots.headRow(slot)
      if (row >= 0) then
        keys(slot) = keyValues(row)
        var total = 0L
        while (row >= 0) {
          total += valueValues(row).toLong
          row = slots.nextRow(row)
        }
        totals(slot) = total
      slot += 1
    }

  private def rememberKeys(slots: JoinSlots, keyValues: Array[Int], keys: Array[Int]): Unit =
    var slot = 0
    val width = slots.slots
    while (slot < width) {
      val row = slots.headRow(slot)
      if (row >= 0) then keys(slot) = keyValues(row)
      slot += 1
    }

  private def countHits(slots: JoinSlots, probeKeys: Int32Column): Array[Long] =
    val hits = new Array[Long](slots.slots)
    val keyValues = probeKeys.values
    val rows = probeKeys.length
    var row = 0
    if (Validity.livePresent(probeKeys.validity, rows)) then
      while (row < rows) {
        val slot = slots.locate(keyValues(row))
        if (slot >= 0) then hits(slot) += 1L
        row += 1
      }
    else
      val words = probeKeys.validity.words
      while (row < rows) {
        if (Validity.isSet(words, row)) then
          val slot = slots.locate(keyValues(row))
          if (slot >= 0) then hits(slot) += 1L
        row += 1
      }
    hits

  /**
   * Probe-value sum per hit slot. The weight is the build chain length, or zero when no probe hit.
   */
  private def foldProbe(
      slots: JoinSlots,
      probeKeys: Int32Column,
      probeValues: Int32Column,
      totals: Array[Long],
      weights: Array[Long],
  ): Unit =
    val keyValues = probeKeys.values
    val valueValues = probeValues.values
    val rows = probeKeys.length
    var row = 0
    if (Validity.livePresent(probeKeys.validity, rows)) then
      while (row < rows) {
        val slot = slots.locate(keyValues(row))
        if (slot >= 0) then
          totals(slot) += valueValues(row).toLong
          weights(slot) = slots.chainAt(slot).toLong
        row += 1
      }
    else
      val words = probeKeys.validity.words
      while (row < rows) {
        if (Validity.isSet(words, row)) then
          val slot = slots.locate(keyValues(row))
          if (slot >= 0) then
            totals(slot) += valueValues(row).toLong
            weights(slot) = slots.chainAt(slot).toLong
        row += 1
      }
