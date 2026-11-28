package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.{ColumnField, ColumnKind}

/**
 * Appends the sum of one int or long column onto a finished one-key group.
 *
 * A null key is already absent from the group. A null value is skipped. Ints widen to long. Long
 * sums wrap. The sum of no values is zero. This is a second pass, the same shape as a distinct
 * count, and it does not spill.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object WideSecondSum:

  def attach(
      grouped: ColumnBatch,
      inputs: Seq[ColumnBatch],
      fields: Vector[ColumnField],
      key: Int,
      sumOrdinal: Int,
  ): ColumnBatch =
    val sums = new Sums
    inputs.foreach(batch => sums.observe(batch, key, sumOrdinal, fields(sumOrdinal).kind))
    val values = sums.lookup(grouped, fields(key).kind)
    append(grouped, values)

  private def append(batch: ColumnBatch, values: Array[Long]): ColumnBatch =
    val column = Int64Column.of(values, Validity.allValid(batch.length), batch.length)
    val columns = batch.columns :+ column
    new ColumnBatch(batch.schema :+ column.logicalType, columns, batch.length)

  private def number(batch: ColumnBatch, ordinal: Int, row: Int, kind: ColumnKind): Long =
    kind match
      case ColumnKind.Int32 =>
        batch.columns(ordinal) match
          case column: Int32Column => column.values(row).toLong
          case other =>
            throw new IllegalStateException(
              s"column $ordinal is ${other.logicalType}, expected Int32",
            )
      case ColumnKind.Int64 =>
        batch.columns(ordinal) match
          case column: Int64Column => column.values(row)
          case other =>
            throw new IllegalStateException(
              s"column $ordinal is ${other.logicalType}, expected Int64",
            )
      case other =>
        throw new IllegalArgumentException(
          s"a second sum needs an int or long column, found $other",
        )

  private final class Sums:
    private var cap = 16
    private var size = 0
    private var used = new Array[Boolean](cap)
    private var keys = new Array[Long](cap)
    private var totals = new Array[Long](cap)

    def observe(batch: ColumnBatch, key: Int, sumOrdinal: Int, kind: ColumnKind): Unit =
      val keyColumn = batch.columns(key)
      val valueColumn = batch.columns(sumOrdinal)
      var row = 0
      while row < batch.length do
        if keyColumn.isValid(row) then
          val slot = slotOf(number(batch, key, row, keyKind(batch, key)))
          if valueColumn.isValid(row) then totals(slot) += number(batch, sumOrdinal, row, kind)
        row += 1

    def lookup(grouped: ColumnBatch, kind: ColumnKind): Array[Long] =
      val rows = grouped.length
      val out = new Array[Long](rows)
      var row = 0
      while row < rows do
        val slot = find(number(grouped, 0, row, kind))
        out(row) = if slot < 0 then 0L else totals(slot)
        row += 1
      out

    private def slotOf(key: Long): Int =
      if (size + 1) * 10 > cap * 7 then grow()
      place(key, create = true)

    private def find(key: Long): Int =
      if size == 0 then -1 else place(key, create = false)

    private def place(key: Long, create: Boolean): Int =
      var index = mix(key) & (cap - 1)
      var steps = 0
      while steps < cap && used(index) && keys(index) != key do
        index = (index + 1) & (cap - 1)
        steps += 1
      if steps >= cap then
        if create then throw new IllegalStateException("sum table is full") else -1
      else if used(index) then index
      else if !create then -1
      else
        used(index) = true
        keys(index) = key
        size += 1
        index

    private def grow(): Unit =
      val oldCap = cap
      val oldUsed = used
      val oldKeys = keys
      val oldTotals = totals
      cap *= 2
      size = 0
      used = new Array[Boolean](cap)
      keys = new Array[Long](cap)
      totals = new Array[Long](cap)
      var row = 0
      while row < oldCap do
        if oldUsed(row) then
          val slot = place(oldKeys(row), create = true)
          totals(slot) = oldTotals(row)
        row += 1

    private def keyKind(batch: ColumnBatch, ordinal: Int): ColumnKind =
      batch.columns(ordinal) match
        case _: Int32Column => ColumnKind.Int32
        case _: Int64Column => ColumnKind.Int64
        case other =>
          throw new IllegalStateException(
            s"column $ordinal is ${other.logicalType}, expected an int or long key",
          )

    private def mix(key: Long): Int =
      val mixed = key * 0x9e3779b97f4a7c15L
      val folded = mixed ^ (mixed >>> 32)
      folded.toInt
