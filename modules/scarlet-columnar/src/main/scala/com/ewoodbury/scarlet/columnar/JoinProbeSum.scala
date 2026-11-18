package com.ewoodbury.scarlet.columnar

/**
 * Grouped sum of one int join column, updated from the probe.
 *
 * The build is the left input and the probe is the right, the same sides as
 * [[HashJoin.innerInt32]]. A null key is not inserted and is not a probe hit. The group key is the
 * key column of the probed row, and [[HashAggregate]] stores the sum under that key. A hash slot
 * is only a way to find the build chain. Two keys that share a home slot stay two groups.
 *
 * A homogeneous chain (every build row's key column equals the head key) contributes its sum once
 * per probe hit. A chain whose keys differ is walked, and only rows whose key column equals the
 * probe key are added. The match columns are not written.
 *
 * A null measured value adds as zero, the same unbox as the row path, including a group whose only
 * matches are null. A null on the other side still matches. An empty build or probe publishes an
 * empty sum and does not touch the aggregate table.
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
      val slots = index(buildKeys)
      val width = slots.slots
      val slotKeys = new Array[Int](width)
      val sums = new Array[Long](width)
      val counts = new Array[Int](width)
      val measureBuild = measure match
        case Measure.Left => true
        case Measure.Right => false
      fold(slots, buildKeys, buildValues, measureBuild, slotKeys, sums, counts)
      HashAggregate.consume { add =>
        probeAll(
          add,
          slots,
          probeKeys,
          probeValues,
          measureBuild,
          slotKeys,
          sums,
          counts,
          buildKeys,
          buildValues,
        )
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

  /** One slot summary. `counts < 0` means the chain holds more than one key. */
  private def fold(
      slots: JoinSlots,
      buildKeys: Int32Column,
      buildValues: Int32Column,
      measureBuild: Boolean,
      slotKeys: Array[Int],
      sums: Array[Long],
      counts: Array[Int],
  ): Unit =
    val keyValues = buildKeys.values
    val valueValues = buildValues.values
    val keyWords = buildKeys.validity.words
    val valueWords = buildValues.validity.words
    val keysDense = ColumnHash.fullyPresent(buildKeys)
    val valuesDense = ColumnHash.fullyPresent(buildValues)
    var slot = 0
    val width = slots.slots
    while (slot < width) {
      var row = slots.headRow(slot)
      if (row >= 0) then
        val key = keyValues(row)
        var sum = 0L
        var count = 0
        var homogeneous = true
        while (row >= 0 && homogeneous) {
          val keyOk = keysDense || Validity.isSet(keyWords, row)
          if (!keyOk || keyValues(row) != key) then homogeneous = false
          else
            count += 1
            if (measureBuild) then sum += measured(valueValues, valueWords, valuesDense, row)
            row = slots.nextRow(row)
        }
        if (!homogeneous) then counts(slot) = -1
        else
          slotKeys(slot) = key
          sums(slot) = sum
          counts(slot) = count
      slot += 1
    }

  private def probeAll(
      add: (Int, Long) => Unit,
      slots: JoinSlots,
      probeKeys: Int32Column,
      probeValues: Int32Column,
      measureBuild: Boolean,
      slotKeys: Array[Int],
      sums: Array[Long],
      counts: Array[Int],
      buildKeys: Int32Column,
      buildValues: Int32Column,
  ): Unit =
    val keyValues = probeKeys.values
    val valueValues = probeValues.values
    val keyWords = probeKeys.validity.words
    val valueWords = probeValues.validity.words
    val keysDense = ColumnHash.fullyPresent(probeKeys)
    val valuesDense = ColumnHash.fullyPresent(probeValues)
    val rows = probeKeys.length
    var row = 0
    while (row < rows) {
      val keyOk = keysDense || Validity.isSet(keyWords, row)
      if (keyOk) then
        val key = keyValues(row)
        val slot = slots.locate(key)
        if (slot >= 0) then
          val count = counts(slot)
          if (count < 0 || slotKeys(slot) != key) then
            val hit = matchingBuild(slots, key, measureBuild, buildKeys, buildValues)
            if (hit._1 > 0) then
              if (measureBuild) then add(key, hit._2)
              else addProbe(add, key, hit._1, valueValues, valueWords, valuesDense, row)
          else if (count > 0) then
            if (measureBuild) then add(key, sums(slot))
            else addProbe(add, key, count, valueValues, valueWords, valuesDense, row)
      row += 1
    }

  /**
   * Build rows whose key column equals `key`. The count is the multiplicity and the long is their
   * value sum when the build is the measured side. A null measured value adds as zero.
   */
  private def matchingBuild(
      slots: JoinSlots,
      key: Int,
      measureBuild: Boolean,
      buildKeys: Int32Column,
      buildValues: Int32Column,
  ): (Int, Long) =
    val keyValues = buildKeys.values
    val valueValues = buildValues.values
    val keyWords = buildKeys.validity.words
    val valueWords = buildValues.validity.words
    val keysDense = ColumnHash.fullyPresent(buildKeys)
    val valuesDense = ColumnHash.fullyPresent(buildValues)
    val slot = slots.locate(key)
    var row = if (slot < 0) then -1 else slots.headRow(slot)
    var matched = 0
    var sum = 0L
    while (row >= 0) {
      val keyOk = keysDense || Validity.isSet(keyWords, row)
      if (keyOk && keyValues(row) == key) then
        matched += 1
        if (measureBuild) then sum += measured(valueValues, valueWords, valuesDense, row)
      row = slots.nextRow(row)
    }
    (matched, sum)

  private def addProbe(
      add: (Int, Long) => Unit,
      key: Int,
      buildCount: Int,
      probeValues: Array[Int],
      probeValueWords: Array[Long],
      probeValuesDense: Boolean,
      probeRow: Int,
  ): Unit =
    add(
      key,
      measured(probeValues, probeValueWords, probeValuesDense, probeRow) * buildCount.toLong,
    )

  /** A clear bit is zero. That is the row path's unbox of a null int. */
  private def measured(
      values: Array[Int],
      words: Array[Long],
      dense: Boolean,
      row: Int,
  ): Long =
    if (dense || Validity.isSet(words, row)) then values(row).toLong else 0L
