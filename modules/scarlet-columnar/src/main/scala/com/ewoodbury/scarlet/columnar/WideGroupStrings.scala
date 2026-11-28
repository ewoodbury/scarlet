package com.ewoodbury.scarlet.columnar

import java.util.Arrays

import com.ewoodbury.scarlet.core.{ColumnField, ColumnKind, WideAggregate}

/**
 * Groups when the measure is a string.
 *
 * Output is the key or keys, then count, min, and max. There is no sum. `textMin` appends the min
 * of one other string column. Min and max use JVM `compareTo`, so dictionary code order is not
 * string order. A null key is dropped. An empty string is a value. A null measure increments count
 * and does not change min or max. A key whose measures are all null still emits a row, with min
 * and max null. Output order is not a contract.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object WideGroupStrings:

  def aggregate(
      batches: Seq[ColumnBatch],
      fields: Vector[ColumnField],
      key: Int,
      measure: Int,
      key2: Option[Int],
      textMin: Option[Int],
  ): (Vector[ColumnField], ColumnBatch) =
    val out = WideAggregate.output(fields, key, measure, key2, None, textMin).getOrElse {
      throw new IllegalArgumentException(
        s"columns $key, $key2, and $measure of ${fields.map(_.kind)} do not group",
      )
    }
    val right = key2.map(second => fields(second).kind)
    val table = new Table(fields(key).kind, right, textMin)
    table.absorb(batches, key, measure, key2)
    (out, table.emit())

  private final class Table(
      leftKind: ColumnKind,
      rightKind: Option[ColumnKind],
      textMin: Option[Int],
  ):
    private var cap = 16
    private var used = new Array[Boolean](cap)
    private var size = 0
    private var leftNum = new Array[Long](cap)
    private var leftText = new Array[String](cap)
    private var rightNum = new Array[Long](cap)
    private var rightText = new Array[String](cap)
    private var counts = new Array[Long](cap)
    private var seen = new Array[Boolean](cap)
    private var mins = new Array[String](cap)
    private var maxs = new Array[String](cap)
    private var secondSeen = new Array[Boolean](cap)
    private var secondText = new Array[String](cap)

    def absorb(batches: Seq[ColumnBatch], key: Int, measure: Int, key2: Option[Int]): Unit =
      batches.foreach { batch =>
        var row = 0
        while row < batch.length do
          if present(batch, key, row) && key2.forall(second => present(batch, second, row)) then
            val slot = slotOf(batch, key, key2, row)
            counts(slot) += 1L
            if present(batch, measure, row) then note(slot, textAt(batch, measure, row))
            textMin.foreach { ordinal =>
              if present(batch, ordinal, row) then noteSecond(slot, textAt(batch, ordinal, row))
            }
          row += 1
      }

    def emit(): ColumnBatch =
      val rows = size
      val slots = new Array[Int](rows)
      var cursor = 0
      var index = 0
      while index < cap do
        if used(index) then
          slots(cursor) = index
          cursor += 1
        index += 1
      val columns = keys(slots) ++ Vector(
        countColumn(slots),
        textColumn(slots, minimum = true),
        textColumn(slots, minimum = false),
      ) ++ secondColumns(slots)
      new ColumnBatch(columns.map(_.logicalType), columns, rows)

    private def keys(slots: Array[Int]): Vector[Column] =
      val left = keyColumn(leftKind, leftNum, leftText, slots)
      rightKind match
        case None => Vector(left)
        case Some(kind) => Vector(left, keyColumn(kind, rightNum, rightText, slots))

    private def slotOf(batch: ColumnBatch, key: Int, key2: Option[Int], row: Int): Int =
      if (size + 1) * 10 > cap * 7 then grow()
      val (leftNumber, leftValue) = read(batch, key, leftKind, row)
      val (rightNumber, rightValue) = key2 match
        case None => (0L, "")
        case Some(second) => read(batch, second, rightKind.getOrElse(leftKind), row)
      place(leftNumber, leftValue, rightNumber, rightValue)

    private def place(
        leftNumber: Long,
        leftValue: String,
        rightNumber: Long,
        rightValue: String,
    ): Int =
      var index = mix(leftNumber, leftValue, rightNumber, rightValue) & (cap - 1)
      var steps = 0
      while used(index) && !same(index, leftNumber, leftValue, rightNumber, rightValue) do
        if steps >= cap then throw new IllegalStateException("string group table is full")
        index = (index + 1) & (cap - 1)
        steps += 1
      if !used(index) then
        used(index) = true
        leftNum(index) = leftNumber
        leftText(index) = leftValue
        rightNum(index) = rightNumber
        rightText(index) = rightValue
        size += 1
      index

    private def grow(): Unit =
      val oldCap = cap
      val oldUsed = used
      val oldLeftNum = leftNum
      val oldLeftText = leftText
      val oldRightNum = rightNum
      val oldRightText = rightText
      val oldCounts = counts
      val oldSeen = seen
      val oldMins = mins
      val oldMaxs = maxs
      val oldSecondSeen = secondSeen
      val oldSecondText = secondText
      val mapping = new Array[Int](oldCap)
      Arrays.fill(mapping, -1)
      cap *= 2
      used = new Array[Boolean](cap)
      leftNum = new Array[Long](cap)
      leftText = new Array[String](cap)
      rightNum = new Array[Long](cap)
      rightText = new Array[String](cap)
      counts = new Array[Long](cap)
      seen = new Array[Boolean](cap)
      mins = new Array[String](cap)
      maxs = new Array[String](cap)
      secondSeen = new Array[Boolean](cap)
      secondText = new Array[String](cap)
      size = 0
      var row = 0
      while row < oldCap do
        if oldUsed(row) then
          mapping(row) =
            place(oldLeftNum(row), oldLeftText(row), oldRightNum(row), oldRightText(row))
        row += 1
      row = 0
      while row < oldCap do
        val dest = mapping(row)
        if dest >= 0 then
          counts(dest) = oldCounts(row)
          seen(dest) = oldSeen(row)
          mins(dest) = oldMins(row)
          maxs(dest) = oldMaxs(row)
          secondSeen(dest) = oldSecondSeen(row)
          secondText(dest) = oldSecondText(row)
        row += 1

    private def note(slot: Int, value: String): Unit =
      if !seen(slot) then
        seen(slot) = true
        mins(slot) = value
        maxs(slot) = value
      else
        if value.compareTo(mins(slot)) < 0 then mins(slot) = value
        if value.compareTo(maxs(slot)) > 0 then maxs(slot) = value

    private def noteSecond(slot: Int, value: String): Unit =
      if !secondSeen(slot) then
        secondSeen(slot) = true
        secondText(slot) = value
      else if value.compareTo(secondText(slot)) < 0 then secondText(slot) = value

    private def same(
        index: Int,
        leftNumber: Long,
        leftValue: String,
        rightNumber: Long,
        rightValue: String,
    ): Boolean =
      sameSide(leftKind, leftNum(index), leftText(index), leftNumber, leftValue) &&
        rightKind.forall { kind =>
          sameSide(kind, rightNum(index), rightText(index), rightNumber, rightValue)
        }

    private def sameSide(
        kind: ColumnKind,
        storedNumber: Long,
        storedText: String,
        number: Long,
        text: String,
    ): Boolean = kind match
      case ColumnKind.Utf8 => storedText.equals(text)
      case ColumnKind.Int32 | ColumnKind.Int64 => storedNumber == number
      case other =>
        throw new IllegalArgumentException(s"group key $other is not int, long, or string")

    private def mix(
        leftNumber: Long,
        leftValue: String,
        rightNumber: Long,
        rightValue: String,
    ): Int =
      val first = mixSide(leftKind, leftNumber, leftValue)
      val second = rightKind match
        case None => 0
        case Some(kind) => mixSide(kind, rightNumber, rightValue)
      first ^ Integer.rotateLeft(second, 13)

    private def mixSide(kind: ColumnKind, number: Long, text: String): Int = kind match
      case ColumnKind.Utf8 => mixText(text)
      case _ => mixLong(number)

    private def countColumn(slots: Array[Int]): Column =
      val values = new Array[Long](slots.length)
      var row = 0
      while row < slots.length do
        values(row) = counts(slots(row))
        row += 1
      Int64Column.of(values, Validity.allValid(slots.length), slots.length)

    private def secondColumns(slots: Array[Int]): Vector[Column] =
      if textMin.isEmpty then Vector.empty
      else
        val values = Vector.tabulate(slots.length) { row =>
          val slot = slots(row)
          if secondSeen(slot) then Some(secondText(slot)) else None
        }
        Vector(DictUtf8Column.fromNullable(values))

    private def textColumn(slots: Array[Int], minimum: Boolean): Column =
      val values = Vector.tabulate(slots.length) { row =>
        val slot = slots(row)
        if seen(slot) then Some(if minimum then mins(slot) else maxs(slot)) else None
      }
      DictUtf8Column.fromNullable(values)

    private def keyColumn(
        kind: ColumnKind,
        numbers: Array[Long],
        texts: Array[String],
        slots: Array[Int],
    ): Column = kind match
      case ColumnKind.Int32 =>
        val values = new Array[Int](slots.length)
        var row = 0
        while row < slots.length do
          values(row) = numbers(slots(row)).toInt
          row += 1
        Int32Column.of(values, Validity.allValid(slots.length), slots.length)
      case ColumnKind.Int64 =>
        val values = new Array[Long](slots.length)
        var row = 0
        while row < slots.length do
          values(row) = numbers(slots(row))
          row += 1
        Int64Column.of(values, Validity.allValid(slots.length), slots.length)
      case ColumnKind.Utf8 =>
        val values = new Array[String](slots.length)
        var row = 0
        while row < slots.length do
          values(row) = texts(slots(row))
          row += 1
        DictUtf8Column.allValid(values)
      case other =>
        throw new IllegalArgumentException(s"group key $other is not int, long, or string")

  private def present(batch: ColumnBatch, ordinal: Int, row: Int): Boolean =
    column(batch, ordinal).isValid(row)

  private def read(batch: ColumnBatch, ordinal: Int, kind: ColumnKind, row: Int): (Long, String) =
    kind match
      case ColumnKind.Int32 => (intAt(batch, ordinal, row).toLong, "")
      case ColumnKind.Int64 => (longAt(batch, ordinal, row), "")
      case ColumnKind.Utf8 => (0L, textAt(batch, ordinal, row))
      case other =>
        throw new IllegalArgumentException(s"group key $other is not int, long, or string")

  private def intAt(batch: ColumnBatch, ordinal: Int, row: Int): Int =
    column(batch, ordinal) match
      case values: Int32Column => values.values(row)
      case other =>
        throw new IllegalArgumentException(s"column $ordinal is ${other.logicalType}")

  private def longAt(batch: ColumnBatch, ordinal: Int, row: Int): Long =
    column(batch, ordinal) match
      case values: Int64Column => values.values(row)
      case other =>
        throw new IllegalArgumentException(s"column $ordinal is ${other.logicalType}")

  private def textAt(batch: ColumnBatch, ordinal: Int, row: Int): String =
    column(batch, ordinal) match
      case values: DictUtf8Column => values.dictionary(values.codes(row))
      case other =>
        throw new IllegalArgumentException(s"column $ordinal is ${other.logicalType}")

  private def column(batch: ColumnBatch, ordinal: Int): Column =
    batch.columns.lift(ordinal).getOrElse {
      throw new IllegalArgumentException(s"column $ordinal outside width ${batch.columns.size}")
    }

  private def mixText(text: String): Int =
    var hash = 0x811c9dc5
    var index = 0
    while index < text.length do
      hash ^= text.charAt(index).toInt
      hash *= 0x01000193
      index += 1
    hash

  private def mixLong(key: Long): Int =
    val mixed = key * 0x9e3779b97f4a7c15L
    val folded = mixed ^ (mixed >>> 32)
    folded.toInt
