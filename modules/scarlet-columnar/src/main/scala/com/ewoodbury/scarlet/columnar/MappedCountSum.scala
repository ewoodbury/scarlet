package com.ewoodbury.scarlet.columnar

/**
 * Count and sum of encoded int pairs after an int key function.
 *
 * Each morsel is already a batch. Steps run in source order on the source columns. A failure stops
 * the row, and `mapKey` is not called. A null slot fails its step and does not call the predicate.
 * A null key that no step reads is dropped and is not passed to `mapKey`. A present key is mapped
 * once. A null value on that row is counted and not added: [[HashAggregate.GroupTable.countNull]].
 * A present value is [[HashAggregate.GroupTable.observe]]. The function is not inspected. This
 * does not publish a pair batch.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object MappedCountSum:

  private[columnar] final class Slot(val column: Int, val keep: Int => Boolean)

  /** Int key and value slots, including none. A non-int step is refused. */
  def prepare(steps: List[Selection.Step]): Array[Slot] =
    val ordered = steps.toArray
    val slots = new Array[Slot](ordered.length)
    var index = 0
    while (index < ordered.length) {
      ordered(index) match
        case Selection.Step.Int32(column, predicate) =>
          require(column == 0 || column == 1, s"count-sum ordinal $column is not a pair column")
          slots(index) = new Slot(column, captured(predicate))
        case _ =>
          require(false, "mapped count-sum steps are int slots")
      index += 1
    }
    slots

  /**
   * One batch, from row 0. An empty batch does not call `mapKey`. `slots` is the prepared stack
   * and is not copied.
   */
  def scan(
      batch: ColumnBatch,
      slots: Array[Slot],
      mapKey: Int => Int,
      table: HashAggregate.GroupTable,
  ): Unit =
    val rows = batch.length
    if (rows == 0) then ()
    else
      val keys = ColumnHash.int32At(batch, 0)
      val values = ColumnHash.int32At(batch, 1)
      require(keys.length == values.length, "key and value columns differ in length")
      val keyValues = keys.values
      val valueValues = values.values
      val keyWords = keys.validity.words
      val valueWords = values.validity.words
      var row = 0
      while (row < rows) {
        if (kept(slots, row, keyValues, keyWords, valueValues, valueWords)) then
          if (Validity.isSet(keyWords, row)) then
            val slot = table.ensure(mapKey(keyValues(row)))
            if (Validity.isSet(valueWords, row)) then table.observe(slot, valueValues(row))
            else table.countNull(slot)
        row += 1
      }

  /** One function for the row loop. A bare trait is wrapped once here, not once per row. */
  private def captured(predicate: ColumnKernel.Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        value => fallback(value)

  private def kept(
      slots: Array[Slot],
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
