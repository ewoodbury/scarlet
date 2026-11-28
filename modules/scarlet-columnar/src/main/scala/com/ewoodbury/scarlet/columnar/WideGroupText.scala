package com.ewoodbury.scarlet.columnar

import java.util.Arrays

import com.ewoodbury.scarlet.core.{ColumnField, ColumnKind, WideAggregate}

/**
 * Groups when one key is a string.
 *
 * The hash is of the string contents, so two batches group together even when their dictionary
 * codes differ. A null key is dropped. An empty string is a key. Measure nulls follow
 * [[WideGroup]]. Output order is not a contract.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object WideGroupText:

  def aggregate(
      batches: Seq[ColumnBatch],
      fields: Vector[ColumnField],
      key: Int,
      measure: Int,
      key2: Option[Int],
  ): (Vector[ColumnField], ColumnBatch) =
    val out = WideAggregate.output(fields, key, measure, key2).getOrElse {
      throw new IllegalArgumentException(
        s"columns $key, $key2, and $measure of ${fields.map(_.kind)} do not group",
      )
    }
    val batch = key2 match
      case None =>
        val table = new TextKeys(fields(measure).kind)
        table.absorb(batches, key, measure)
        table.emit()
      case Some(second) =>
        val table = new MixedKeys(fields(measure).kind, fields(key).kind, fields(second).kind)
        table.absorb(batches, key, second, measure)
        table.emit()
    (out, batch)

  private final class TextKeys(measureKind: ColumnKind):
    private val measures = new WideGroup.Measures(measureKind)
    private var cap = 16
    private var keys = new Array[String](cap)
    private var used = new Array[Boolean](cap)
    private var size = 0

    def absorb(batches: Seq[ColumnBatch], key: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val texts = textColumn(batch, key)
        var row = 0
        while row < batch.length do
          if texts.isValid(row) then
            val slot = slotOf(texts.dictionary(texts.codes(row)))
            measures.count(slot)
            note(measures, measureKind, batch, measure, row, slot)
          row += 1
      }

    def emit(): ColumnBatch =
      val rows = size
      val out = new Array[String](rows)
      val slots = new Array[Int](rows)
      var cursor = 0
      var index = 0
      while index < cap do
        if used(index) then
          out(cursor) = keys(index)
          slots(cursor) = index
          cursor += 1
        index += 1
      val keyColumn = DictUtf8Column.allValid(out)
      val columns = keyColumn +: measures.columns(slots)
      new ColumnBatch(columns.map(_.logicalType), columns, rows)

    private def slotOf(key: String): Int =
      if (size + 1) * 10 > cap * 7 then grow()
      place(key)

    private def place(key: String): Int =
      var index = mix(key) & (cap - 1)
      while used(index) && !keys(index).equals(key) do index = (index + 1) & (cap - 1)
      if !used(index) then
        used(index) = true
        keys(index) = key
        size += 1
      index

    private def grow(): Unit =
      val oldCap = cap
      val oldKeys = keys
      val oldUsed = used
      val mapping = new Array[Int](oldCap)
      Arrays.fill(mapping, -1)
      cap *= 2
      keys = new Array[String](cap)
      used = new Array[Boolean](cap)
      size = 0
      var row = 0
      while row < oldCap do
        if oldUsed(row) then mapping(row) = place(oldKeys(row))
        row += 1
      measures.reindex(mapping, oldCap, cap)

  private final class MixedKeys(
      measureKind: ColumnKind,
      leftKind: ColumnKind,
      rightKind: ColumnKind,
  ):
    private val measures = new WideGroup.Measures(measureKind)
    private val leftText = leftKind == ColumnKind.Utf8
    private val rightText = rightKind == ColumnKind.Utf8
    private var cap = 16
    private var text0 = new Array[String](cap)
    private var text1 = new Array[String](cap)
    private var num0 = new Array[Long](cap)
    private var num1 = new Array[Long](cap)
    private var used = new Array[Boolean](cap)
    private var size = 0

    def absorb(batches: Seq[ColumnBatch], key: Int, key2: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val left = read(batch, key, leftKind)
        val right = read(batch, key2, rightKind)
        var row = 0
        while row < batch.length do
          if left.present(row) && right.present(row) then
            val slot = slotOf(left.at(row), right.at(row))
            measures.count(slot)
            note(measures, measureKind, batch, measure, row, slot)
          row += 1
      }

    def emit(): ColumnBatch =
      val rows = size
      val slots = new Array[Int](rows)
      val leftOut = new Array[Held](rows)
      val rightOut = new Array[Held](rows)
      var cursor = 0
      var index = 0
      while index < cap do
        if used(index) then
          leftOut(cursor) = Held(leftText, num0(index), text0(index), present = true)
          rightOut(cursor) = Held(rightText, num1(index), text1(index), present = true)
          slots(cursor) = index
          cursor += 1
        index += 1
      val columns = Vector(emitKey(leftKind, leftOut), emitKey(rightKind, rightOut)) ++
        measures.columns(slots)
      new ColumnBatch(columns.map(_.logicalType), columns, rows)

    private def slotOf(left: Held, right: Held): Int =
      if (size + 1) * 10 > cap * 7 then grow()
      place(left, right)

    private def place(left: Held, right: Held): Int =
      var index = mixPair(left, right) & (cap - 1)
      while used(index) && !same(index, left, right) do index = (index + 1) & (cap - 1)
      if !used(index) then
        used(index) = true
        if leftText then text0(index) = left.text else num0(index) = left.num
        if rightText then text1(index) = right.text else num1(index) = right.num
        size += 1
      index

    private def same(index: Int, left: Held, right: Held): Boolean =
      val leftSame = if leftText then text0(index).equals(left.text) else num0(index) == left.num
      val rightSame =
        if rightText then text1(index).equals(right.text) else num1(index) == right.num
      leftSame && rightSame

    private def grow(): Unit =
      val oldCap = cap
      val oldText0 = text0
      val oldText1 = text1
      val oldNum0 = num0
      val oldNum1 = num1
      val oldUsed = used
      val mapping = new Array[Int](oldCap)
      Arrays.fill(mapping, -1)
      cap *= 2
      text0 = new Array[String](cap)
      text1 = new Array[String](cap)
      num0 = new Array[Long](cap)
      num1 = new Array[Long](cap)
      used = new Array[Boolean](cap)
      size = 0
      var row = 0
      while row < oldCap do
        if oldUsed(row) then
          val left = Held(leftText, oldNum0(row), oldText0(row), present = true)
          val right = Held(rightText, oldNum1(row), oldText1(row), present = true)
          mapping(row) = place(left, right)
        row += 1
      measures.reindex(mapping, oldCap, cap)

  private final class Held(
      val textSide: Boolean,
      val num: Long,
      val text: String,
      val present: Boolean,
  )

  private def read(batch: ColumnBatch, ordinal: Int, kind: ColumnKind): Side =
    kind match
      case ColumnKind.Utf8 => readText(batch, ordinal)
      case ColumnKind.Int32 => readInts(batch, ordinal)
      case ColumnKind.Int64 => readLongs(batch, ordinal)
      case other =>
        throw new IllegalArgumentException(s"group key $other is not int, long, or string")

  private def readText(batch: ColumnBatch, ordinal: Int): Side =
    val column = textColumn(batch, ordinal)
    val held = Array.tabulate(batch.length) { row =>
      if column.isValid(row) then
        Held(textSide = true, 0L, column.dictionary(column.codes(row)), present = true)
      else Held(textSide = true, 0L, "", present = false)
    }
    new Side(held)

  private def readInts(batch: ColumnBatch, ordinal: Int): Side =
    val column = intColumn(batch, ordinal)
    val held = Array.tabulate(batch.length) { row =>
      if column.isValid(row) then
        Held(textSide = false, column.values(row).toLong, "", present = true)
      else Held(textSide = false, 0L, "", present = false)
    }
    new Side(held)

  private def readLongs(batch: ColumnBatch, ordinal: Int): Side =
    val column = longColumn(batch, ordinal)
    val held = Array.tabulate(batch.length) { row =>
      if column.isValid(row) then Held(textSide = false, column.values(row), "", present = true)
      else Held(textSide = false, 0L, "", present = false)
    }
    new Side(held)

  private final class Side(val rows: Array[Held]):
    def present(row: Int): Boolean = rows(row).present
    def at(row: Int): Held = rows(row)

  private def emitKey(kind: ColumnKind, held: Array[Held]): Column =
    val rows = held.length
    kind match
      case ColumnKind.Utf8 =>
        val values = new Array[String](rows)
        var row = 0
        while row < rows do
          values(row) = held(row).text
          row += 1
        DictUtf8Column.allValid(values)
      case ColumnKind.Int32 =>
        val values = new Array[Int](rows)
        var row = 0
        while row < rows do
          values(row) = held(row).num.toInt
          row += 1
        Int32Column.of(values, Validity.allValid(rows), rows)
      case ColumnKind.Int64 =>
        val values = new Array[Long](rows)
        var row = 0
        while row < rows do
          values(row) = held(row).num
          row += 1
        Int64Column.of(values, Validity.allValid(rows), rows)
      case other =>
        throw new IllegalArgumentException(s"group key $other is not int, long, or string")

  private def note(
      measures: WideGroup.Measures,
      kind: ColumnKind,
      batch: ColumnBatch,
      ordinal: Int,
      row: Int,
      slot: Int,
  ): Unit =
    kind match
      case ColumnKind.Int32 =>
        val column = intColumn(batch, ordinal)
        if column.isValid(row) then measures.noteInt(slot, column.values(row))
      case ColumnKind.Int64 =>
        val column = longColumn(batch, ordinal)
        if column.isValid(row) then measures.noteLong(slot, column.values(row))
      case ColumnKind.Float64 =>
        val column = doubleColumn(batch, ordinal)
        if column.isValid(row) then measures.noteDouble(slot, column.values(row))
      case ColumnKind.Float32 =>
        val column = floatColumn(batch, ordinal)
        if column.isValid(row) then measures.noteFloat(slot, column.values(row))
      case other => throw new IllegalArgumentException(s"measure $other does not aggregate")

  private def mix(text: String): Int =
    var hash = 0x811c9dc5
    var index = 0
    while index < text.length do
      hash ^= text.charAt(index).toInt
      hash *= 0x01000193
      index += 1
    hash

  private def mixPair(left: Held, right: Held): Int =
    val first = if left.textSide then mix(left.text) else mixLong(left.num)
    val second = if right.textSide then mix(right.text) else mixLong(right.num)
    first ^ Integer.rotateLeft(second, 13)

  private def mixLong(key: Long): Int =
    val mixed = key * 0x9e3779b97f4a7c15L
    val folded = mixed ^ (mixed >>> 32)
    folded.toInt

  private def textColumn(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    columnAt(batch, ordinal) match
      case column: DictUtf8Column => column
      case other =>
        throw new IllegalStateException(s"column $ordinal is ${other.logicalType}, expected Utf8")

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
    batch.columns.lift(ordinal).getOrElse {
      throw new IllegalArgumentException(s"column $ordinal outside width ${batch.columns.size}")
    }
