package com.ewoodbury.scarlet.columnar

/**
 * `countSumByKey` after one int slot above `mapIntKey`, over encoded morsels.
 *
 * Steps run in source order on the source columns. A null slot fails its step and does not call
 * the predicate. A null key that survives those steps is not passed to `mapKey` or to a key slot.
 * A value slot still sees that row, including a null key, and is not called on a null value. A
 * null mapped key is the null input key: it is dropped and does not create a group. A present
 * mapped key that the slot keeps is observed. A null value on that row is counted and not added.
 * Neither function is inspected. The pair batch is not published, and [[CountSum.byKey]] is not
 * called.
 *
 * Partials are one count-sum per morsel, merged per input batch, then exchanged like
 * [[CountSum.byKeyMapped]]. There is no spill path: a partial is `O(distinct keys)`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object MappedAboveCountSum:

  /**
   * One merged partial per input batch. `batches` is not empty. `steps` may be empty. `above` is
   * one int key or value.
   */
  def combine(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      mapKey: Int => Int,
      above: Selection.Step,
      parallelism: Int,
      batchSize: Int,
      open: () => HashAggregate.GroupTable,
      publish: HashAggregate.GroupTable => ColumnBatch,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val (column, keep) = slot(above)
    val under = MappedCountSum.prepare(steps)
    val groups = batches.map(batch => Morsels.of(batch, batchSize))
    val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
      val table = open()
      scan(morsel, under, mapKey, column, keep, table)
      ColumnMetrics.fused()
      publish(table)
    }
    PartialCombine.mergeMorsels(
      groups.map(_.length),
      partials,
      parallelism,
      CountSum.mergeCountSums,
    )

  /** One int key or value. The predicate is the function the planner stored. */
  private[columnar] def slot(step: Selection.Step): (Int, Int => Boolean) = step match
    case Selection.Step.Int32(column, predicate) if column == 0 || column == 1 =>
      (column, captured(predicate))
    case Selection.Step.Int32(column, _) =>
      throw new IllegalStateException(s"mapped slot must be a key or a value, got $column")
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
      throw new IllegalStateException("slot above a mapped key is an int key or value")

  private def captured(predicate: ColumnKernel.Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        value => fallback(value)

  private def scan(
      morsel: Morsel,
      slots: Array[MappedCountSum.Slot],
      mapKey: Int => Int,
      aboveColumn: Int,
      aboveKeep: Int => Boolean,
      table: HashAggregate.GroupTable,
  ): Unit =
    val length = morsel.length
    if (length == 0) then ()
    else
      val batch = morsel.batch
      val keys = ColumnHash.int32At(batch, 0)
      val values = ColumnHash.int32At(batch, 1)
      require(keys.length == values.length, "key and value columns differ in length")
      Ranges.check(morsel.start, length, keys.length)
      val keyDense = Validity.rangePresent(keys.validity, morsel.start, length)
      val valueDense = Validity.rangePresent(values.validity, morsel.start, length)
      if (keyDense && valueDense) then
        if (aboveColumn == 0) then
          denseKey(keys, values, morsel.start, length, slots, mapKey, aboveKeep, table)
        else denseValue(keys, values, morsel.start, length, slots, mapKey, aboveKeep, table)
      else if (aboveColumn == 0) then
        sparseKey(
          keys,
          values,
          morsel.start,
          length,
          slots,
          keyDense,
          valueDense,
          mapKey,
          aboveKeep,
          table,
        )
      else
        sparseValue(
          keys,
          values,
          morsel.start,
          length,
          slots,
          keyDense,
          valueDense,
          mapKey,
          aboveKeep,
          table,
        )

  private def denseKey(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      slots: Array[MappedCountSum.Slot],
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
      table: HashAggregate.GroupTable,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val end = start + length
    var row = start
    while (row < end) {
      if (keptDense(slots, row, keyValues, valueValues)) then
        val mapped = mapKey(keyValues(row))
        if (aboveKeep(mapped)) then table.observe(table.ensure(mapped), valueValues(row))
      row += 1
    }

  private def denseValue(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      slots: Array[MappedCountSum.Slot],
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
      table: HashAggregate.GroupTable,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val end = start + length
    var row = start
    while (row < end) {
      if (keptDense(slots, row, keyValues, valueValues)) then
        val mapped = mapKey(keyValues(row))
        val value = valueValues(row)
        if (aboveKeep(value)) then table.observe(table.ensure(mapped), value)
      row += 1
    }

  private def sparseKey(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      slots: Array[MappedCountSum.Slot],
      keyDense: Boolean,
      valueDense: Boolean,
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
      table: HashAggregate.GroupTable,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    val end = start + length
    var row = start
    while (row < end) {
      if (kept(slots, row, keyValues, keyWords, valueValues, valueWords)) then
        if (present(keyDense, keyWords, row)) then
          val mapped = mapKey(keyValues(row))
          if (aboveKeep(mapped)) then
            if (present(valueDense, valueWords, row)) then
              table.observe(table.ensure(mapped), valueValues(row))
            else table.countNull(table.ensure(mapped))
      row += 1
    }

  private def sparseValue(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      slots: Array[MappedCountSum.Slot],
      keyDense: Boolean,
      valueDense: Boolean,
      mapKey: Int => Int,
      aboveKeep: Int => Boolean,
      table: HashAggregate.GroupTable,
  ): Unit =
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    val end = start + length
    var row = start
    while (row < end) {
      if (kept(slots, row, keyValues, keyWords, valueValues, valueWords)) then
        val valueOn = present(valueDense, valueWords, row)
        if (present(keyDense, keyWords, row)) then
          val mapped = mapKey(keyValues(row))
          if (valueOn && aboveKeep(valueValues(row))) then
            table.observe(table.ensure(mapped), valueValues(row))
        else if (valueOn) then
          val _ = aboveKeep(valueValues(row))
      row += 1
    }

  private def present(dense: Boolean, words: Array[Long], row: Int): Boolean =
    dense || Validity.isSet(words, row)

  /** Under-map slots on a fully present range. A null is not in this range. */
  private def keptDense(
      slots: Array[MappedCountSum.Slot],
      row: Int,
      keyValues: Array[Int],
      valueValues: Array[Int],
  ): Boolean =
    var index = 0
    var valid = true
    while (valid && index < slots.length) {
      val slot = slots(index)
      val sample = if (slot.column == 0) then keyValues(row) else valueValues(row)
      valid = slot.keep(sample)
      index += 1
    }
    valid

  /** A null under-map slot fails and does not call its predicate. */
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
