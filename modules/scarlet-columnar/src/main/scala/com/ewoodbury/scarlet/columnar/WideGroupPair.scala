package com.ewoodbury.scarlet.columnar

import java.util.Arrays

import com.ewoodbury.scarlet.core.{ColumnField, ColumnKind, WideAggregate}

/**
 * Groups two int or long columns and aggregates one numeric column.
 *
 * One pass over each batch. A null in either key is dropped. Measure nulls follow [[WideGroup]].
 * Output order is not a contract. This does not call `groupByKey`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object WideGroupPair:

  def aggregate(
      batches: Seq[ColumnBatch],
      fields: Vector[ColumnField],
      key: Int,
      key2: Int,
      measure: Int,
  ): (Vector[ColumnField], ColumnBatch) =
    val out = WideAggregate.output(fields, key, measure, Some(key2)).getOrElse {
      throw new IllegalArgumentException(
        s"columns $key, $key2, and $measure of ${fields.map(_.kind)} do not group",
      )
    }
    val table = new PairKeys(fields(measure).kind, fields(key).kind, fields(key2).kind)
    table.absorb(batches, key, key2, measure)
    (out, table.emit())

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class PairKeys(
      measureKind: ColumnKind,
      leftKind: ColumnKind,
      rightKind: ColumnKind,
  ):
    private val measures = new WideGroup.Measures(measureKind)
    private var cap = 16
    private var keys0 = new Array[Long](cap)
    private var keys1 = new Array[Long](cap)
    private var used = new Array[Boolean](cap)
    private var size = 0

    def absorb(batches: Seq[ColumnBatch], key: Int, key2: Int, measure: Int): Unit =
      measureKind match
        case ColumnKind.Int32 => absorbInts(batches, key, key2, measure)
        case ColumnKind.Int64 => absorbLongs(batches, key, key2, measure)
        case ColumnKind.Float64 => absorbDoubles(batches, key, key2, measure)
        case ColumnKind.Float32 => absorbFloats(batches, key, key2, measure)
        case other => throw new IllegalArgumentException(s"measure $other does not aggregate")

    def emit(): ColumnBatch =
      val rows = size
      val out0 = new Array[Long](rows)
      val out1 = new Array[Long](rows)
      val slots = new Array[Int](rows)
      var cursor = 0
      var index = 0
      while (index < cap) {
        if (used(index)) then
          out0(cursor) = keys0(index)
          out1(cursor) = keys1(index)
          slots(cursor) = index
          cursor += 1
        index += 1
      }
      val columns = Vector(keyColumn(leftKind, out0, rows), keyColumn(rightKind, out1, rows)) ++
        measures.columns(slots)
      new ColumnBatch(columns.map(_.logicalType), columns, rows)

    private def absorbInts(batches: Seq[ColumnBatch], key: Int, key2: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val left = readKeys(batch, key, leftKind)
        val right = readKeys(batch, key2, rightKind)
        val values = intColumn(batch, measure)
        var row = 0
        while (row < batch.length) {
          if (left.present(row) && right.present(row)) then
            val slot = slotOf(left.values(row), right.values(row))
            measures.count(slot)
            if (values.isValid(row)) then measures.noteInt(slot, values.values(row))
          row += 1
        }
      }

    private def absorbLongs(batches: Seq[ColumnBatch], key: Int, key2: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val left = readKeys(batch, key, leftKind)
        val right = readKeys(batch, key2, rightKind)
        val values = longColumn(batch, measure)
        var row = 0
        while (row < batch.length) {
          if (left.present(row) && right.present(row)) then
            val slot = slotOf(left.values(row), right.values(row))
            measures.count(slot)
            if (values.isValid(row)) then measures.noteLong(slot, values.values(row))
          row += 1
        }
      }

    private def absorbDoubles(batches: Seq[ColumnBatch], key: Int, key2: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val left = readKeys(batch, key, leftKind)
        val right = readKeys(batch, key2, rightKind)
        val values = doubleColumn(batch, measure)
        var row = 0
        while (row < batch.length) {
          if (left.present(row) && right.present(row)) then
            val slot = slotOf(left.values(row), right.values(row))
            measures.count(slot)
            if (values.isValid(row)) then measures.noteDouble(slot, values.values(row))
          row += 1
        }
      }

    private def absorbFloats(batches: Seq[ColumnBatch], key: Int, key2: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val left = readKeys(batch, key, leftKind)
        val right = readKeys(batch, key2, rightKind)
        val values = floatColumn(batch, measure)
        var row = 0
        while (row < batch.length) {
          if (left.present(row) && right.present(row)) then
            val slot = slotOf(left.values(row), right.values(row))
            measures.count(slot)
            if (values.isValid(row)) then measures.noteFloat(slot, values.values(row))
          row += 1
        }
      }

    private def slotOf(left: Long, right: Long): Int =
      if ((size + 1) * 10 > cap * 7) then grow()
      place(left, right)

    private def place(left: Long, right: Long): Int =
      var index = mix(left, right) & (cap - 1)
      while (used(index) && (keys0(index) != left || keys1(index) != right)) {
        index = (index + 1) & (cap - 1)
      }
      if (!used(index)) then
        used(index) = true
        keys0(index) = left
        keys1(index) = right
        size += 1
      index

    private def grow(): Unit =
      val oldCap = cap
      val old0 = keys0
      val old1 = keys1
      val oldUsed = used
      val mapping = new Array[Int](oldCap)
      Arrays.fill(mapping, -1)
      cap *= 2
      keys0 = new Array[Long](cap)
      keys1 = new Array[Long](cap)
      used = new Array[Boolean](cap)
      size = 0
      var row = 0
      while (row < oldCap) {
        if (oldUsed(row)) then mapping(row) = place(old0(row), old1(row))
        row += 1
      }
      measures.reindex(mapping, oldCap, cap)

    private def mix(left: Long, right: Long): Int =
      val mixed = (left * 0x9e3779b97f4a7c15L) ^ (right * 0xbf58476d1ce4e5b9L)
      val folded = mixed ^ (mixed >>> 32)
      folded.toInt

  private final class KeyLongs(val values: Array[Long], val present: Array[Boolean])

  private def readKeys(batch: ColumnBatch, ordinal: Int, kind: ColumnKind): KeyLongs =
    val values = new Array[Long](batch.length)
    val present = new Array[Boolean](batch.length)
    kind match
      case ColumnKind.Int32 =>
        val column = intColumn(batch, ordinal)
        var row = 0
        while (row < batch.length) {
          val valid = column.isValid(row)
          present(row) = valid
          if (valid) then values(row) = column.values(row).toLong
          row += 1
        }
      case ColumnKind.Int64 =>
        val column = longColumn(batch, ordinal)
        var row = 0
        while (row < batch.length) {
          val valid = column.isValid(row)
          present(row) = valid
          if (valid) then values(row) = column.values(row)
          row += 1
        }
      case other =>
        throw new IllegalArgumentException(s"group key $other is not int or long")
    new KeyLongs(values, present)

  private def keyColumn(kind: ColumnKind, values: Array[Long], rows: Int): Column = kind match
    case ColumnKind.Int32 =>
      val ints = new Array[Int](rows)
      var row = 0
      while (row < rows) {
        ints(row) = values(row).toInt
        row += 1
      }
      Int32Column.of(ints, Validity.allValid(rows), rows)
    case ColumnKind.Int64 =>
      Int64Column.of(values, Validity.allValid(rows), rows)
    case other =>
      throw new IllegalArgumentException(s"group key $other is not int or long")

  private def intColumn(batch: ColumnBatch, ordinal: Int): Int32Column =
    columnAt(batch, ordinal) match
      case column: Int32Column => column
      case other =>
        throw new IllegalStateException(s"column $ordinal is ${other.logicalType}, expected Int32")

  private def longColumn(batch: ColumnBatch, ordinal: Int): Int64Column =
    columnAt(batch, ordinal) match
      case column: Int64Column => column
      case other =>
        throw new IllegalStateException(s"column $ordinal is ${other.logicalType}, expected Int64")

  private def doubleColumn(batch: ColumnBatch, ordinal: Int): Float64Column =
    columnAt(batch, ordinal) match
      case column: Float64Column => column
      case other =>
        throw new IllegalStateException(
          s"column $ordinal is ${other.logicalType}, expected Float64",
        )

  private def floatColumn(batch: ColumnBatch, ordinal: Int): Float32Column =
    columnAt(batch, ordinal) match
      case column: Float32Column => column
      case other =>
        throw new IllegalStateException(
          s"column $ordinal is ${other.logicalType}, expected Float32",
        )

  private def columnAt(batch: ColumnBatch, ordinal: Int): Column =
    require(
      ordinal >= 0 && ordinal < batch.columns.size,
      s"column $ordinal outside width ${batch.columns.size}",
    )
    batch.columns(ordinal)
