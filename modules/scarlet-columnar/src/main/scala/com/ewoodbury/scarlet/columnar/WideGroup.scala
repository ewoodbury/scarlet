package com.ewoodbury.scarlet.columnar

import java.util.Arrays

import com.ewoodbury.scarlet.core.{ColumnField, ColumnKind, WideAggregate}

/**
 * Groups one int, long, or string column. A numeric measure emits count, sum, min, and max. A
 * string measure emits count, min, and max, and has no sum.
 *
 * One pass over each batch. A null key is dropped. A null measure increments count and does not
 * change sum, min, or max. A key whose measures are all null still emits a row, with those
 * aggregate columns null. Int sums widen to long. Long sums wrap. Float and double sums are
 * doubles. This does not build a list of values and it does not call `groupByKey`. Output order is
 * not a contract. Empty input emits no rows. `distinct` appends an exact distinct count and does
 * not change the aggregates already emitted.
 */
object WideGroup:

  def aggregate(
      batches: Seq[ColumnBatch],
      fields: Vector[ColumnField],
      key: Int,
      measure: Int,
      key2: Option[Int] = None,
      distinct: Option[Int] = None,
  ): (Vector[ColumnField], ColumnBatch) =
    val text = textKey(fields, key) || key2.exists(second => textKey(fields, second))
    val stringMeasure = textKey(fields, measure)
    val built =
      if stringMeasure then WideGroupStrings.aggregate(batches, fields, key, measure, key2)
      else if text then WideGroupText.aggregate(batches, fields, key, measure, key2)
      else
        key2 match
          case Some(second) => WideGroupPair.aggregate(batches, fields, key, second, measure)
          case None => aggregateOne(batches, fields, key, measure)
    distinct match
      case None => built
      case Some(ordinal) =>
        val full = WideAggregate.output(fields, key, measure, key2, Some(ordinal)).getOrElse {
          throw new IllegalArgumentException(
            s"column $ordinal of ${fields.map(_.kind)} is not a distinct count",
          )
        }
        val batch = WideDistinct.attach(built._2, batches, fields, key, key2, ordinal)
        (full, batch)

  private def textKey(fields: Vector[ColumnField], ordinal: Int): Boolean =
    ordinal >= 0 && ordinal < fields.size && fields(ordinal).kind == ColumnKind.Utf8

  private def aggregateOne(
      batches: Seq[ColumnBatch],
      fields: Vector[ColumnField],
      key: Int,
      measure: Int,
  ): (Vector[ColumnField], ColumnBatch) =
    val out = WideAggregate.output(fields, key, measure).getOrElse {
      throw new IllegalArgumentException(
        s"columns $key and $measure of ${fields.map(_.kind)} do not group",
      )
    }
    val batch = fields(key).kind match
      case ColumnKind.Int32 =>
        val table = new IntKeys(fields(measure).kind)
        table.absorb(batches, key, measure)
        table.emit()
      case ColumnKind.Int64 =>
        val table = new LongKeys(fields(measure).kind)
        table.absorb(batches, key, measure)
        table.emit()
      case other =>
        throw new IllegalArgumentException(s"group key $other is not int or long")
    (out, batch)

  private enum Family:
    case Ints, Longs, Doubles, Floats

  private def family(kind: ColumnKind): Family = kind match
    case ColumnKind.Int32 => Family.Ints
    case ColumnKind.Int64 => Family.Longs
    case ColumnKind.Float64 => Family.Doubles
    case ColumnKind.Float32 => Family.Floats
    case other => throw new IllegalArgumentException(s"measure $other does not aggregate")

  /** Count, sum, min, and max addressed by the same slot as the key table. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[columnar] final class Measures(kind: ColumnKind):
    private val familyKind = family(kind)
    private var cap = 16
    private var counts = new Array[Long](cap)
    private var seen = new Array[Boolean](cap)
    private var longSums = new Array[Long](cap)
    private var doubleSums = new Array[Double](cap)
    private var intMin = new Array[Int](cap)
    private var intMax = new Array[Int](cap)
    private var longMin = new Array[Long](cap)
    private var longMax = new Array[Long](cap)
    private var doubleMin = new Array[Double](cap)
    private var doubleMax = new Array[Double](cap)
    private var floatMin = new Array[Float](cap)
    private var floatMax = new Array[Float](cap)

    def count(slot: Int): Unit = counts(slot) += 1L

    def noteInt(slot: Int, value: Int): Unit =
      if (!seen(slot)) then
        seen(slot) = true
        intMin(slot) = value
        intMax(slot) = value
        longSums(slot) = value.toLong
      else
        if (value < intMin(slot)) then intMin(slot) = value
        if (value > intMax(slot)) then intMax(slot) = value
        longSums(slot) += value.toLong

    def noteLong(slot: Int, value: Long): Unit =
      if (!seen(slot)) then
        seen(slot) = true
        longMin(slot) = value
        longMax(slot) = value
        longSums(slot) = value
      else
        if (value < longMin(slot)) then longMin(slot) = value
        if (value > longMax(slot)) then longMax(slot) = value
        longSums(slot) += value

    def noteDouble(slot: Int, value: Double): Unit =
      if (!seen(slot)) then
        seen(slot) = true
        doubleMin(slot) = value
        doubleMax(slot) = value
        doubleSums(slot) = value
      else
        if (value < doubleMin(slot)) then doubleMin(slot) = value
        if (value > doubleMax(slot)) then doubleMax(slot) = value
        doubleSums(slot) += value

    def noteFloat(slot: Int, value: Float): Unit =
      if (!seen(slot)) then
        seen(slot) = true
        floatMin(slot) = value
        floatMax(slot) = value
        doubleSums(slot) = value.toDouble
      else
        if (value < floatMin(slot)) then floatMin(slot) = value
        if (value > floatMax(slot)) then floatMax(slot) = value
        doubleSums(slot) += value.toDouble

    /** Moves accumulators into the grown key table. `mapping(old)` is the new slot, or `-1`. */
    def reindex(mapping: Array[Int], oldCap: Int, next: Int): Unit =
      val nextCounts = new Array[Long](next)
      val nextSeen = new Array[Boolean](next)
      val nextLongSums = new Array[Long](next)
      val nextDoubleSums = new Array[Double](next)
      val nextIntMin = new Array[Int](next)
      val nextIntMax = new Array[Int](next)
      val nextLongMin = new Array[Long](next)
      val nextLongMax = new Array[Long](next)
      val nextDoubleMin = new Array[Double](next)
      val nextDoubleMax = new Array[Double](next)
      val nextFloatMin = new Array[Float](next)
      val nextFloatMax = new Array[Float](next)
      var row = 0
      while (row < oldCap) {
        val at = mapping(row)
        if (at >= 0) then
          nextCounts(at) = counts(row)
          nextSeen(at) = seen(row)
          nextLongSums(at) = longSums(row)
          nextDoubleSums(at) = doubleSums(row)
          nextIntMin(at) = intMin(row)
          nextIntMax(at) = intMax(row)
          nextLongMin(at) = longMin(row)
          nextLongMax(at) = longMax(row)
          nextDoubleMin(at) = doubleMin(row)
          nextDoubleMax(at) = doubleMax(row)
          nextFloatMin(at) = floatMin(row)
          nextFloatMax(at) = floatMax(row)
        row += 1
      }
      cap = next
      counts = nextCounts
      seen = nextSeen
      longSums = nextLongSums
      doubleSums = nextDoubleSums
      intMin = nextIntMin
      intMax = nextIntMax
      longMin = nextLongMin
      longMax = nextLongMax
      doubleMin = nextDoubleMin
      doubleMax = nextDoubleMax
      floatMin = nextFloatMin
      floatMax = nextFloatMax

    def columns(slots: Array[Int]): IndexedSeq[Column] =
      val rows = slots.length
      val outCounts = new Array[Long](rows)
      val present = new Array[Boolean](rows)
      var row = 0
      while (row < rows) {
        val slot = slots(row)
        outCounts(row) = counts(slot)
        present(row) = seen(slot)
        row += 1
      }
      val validity = Validity.pack(rows, at => present(at))
      val countColumn = Int64Column.of(outCounts, Validity.allValid(rows), rows)
      Vector(
        countColumn,
        sumColumn(slots, validity),
        minColumn(slots, validity),
        maxColumn(slots, validity),
      )

    private def sumColumn(slots: Array[Int], validity: Validity): Column =
      val rows = slots.length
      familyKind match
        case Family.Ints | Family.Longs =>
          val values = new Array[Long](rows)
          copyLong(slots, longSums, values)
          Int64Column.of(values, validity, rows)
        case Family.Doubles | Family.Floats =>
          val values = new Array[Double](rows)
          copyDouble(slots, doubleSums, values)
          Float64Column.of(values, validity, rows)

    private def minColumn(slots: Array[Int], validity: Validity): Column =
      dense(slots, validity, minimum = true)

    private def maxColumn(slots: Array[Int], validity: Validity): Column =
      dense(slots, validity, minimum = false)

    private def dense(slots: Array[Int], validity: Validity, minimum: Boolean): Column =
      val rows = slots.length
      familyKind match
        case Family.Ints =>
          val values = new Array[Int](rows)
          copyInt(slots, if (minimum) then intMin else intMax, values)
          Int32Column.of(values, validity, rows)
        case Family.Longs =>
          val values = new Array[Long](rows)
          copyLong(slots, if (minimum) then longMin else longMax, values)
          Int64Column.of(values, validity, rows)
        case Family.Doubles =>
          val values = new Array[Double](rows)
          copyDouble(slots, if (minimum) then doubleMin else doubleMax, values)
          Float64Column.of(values, validity, rows)
        case Family.Floats =>
          val values = new Array[Float](rows)
          copyFloat(slots, if (minimum) then floatMin else floatMax, values)
          Float32Column.of(values, validity, rows)

    private def copyInt(slots: Array[Int], source: Array[Int], into: Array[Int]): Unit =
      var row = 0
      while (row < slots.length) {
        into(row) = source(slots(row))
        row += 1
      }

    private def copyLong(slots: Array[Int], source: Array[Long], into: Array[Long]): Unit =
      var row = 0
      while (row < slots.length) {
        into(row) = source(slots(row))
        row += 1
      }

    private def copyDouble(slots: Array[Int], source: Array[Double], into: Array[Double]): Unit =
      var row = 0
      while (row < slots.length) {
        into(row) = source(slots(row))
        row += 1
      }

    private def copyFloat(slots: Array[Int], source: Array[Float], into: Array[Float]): Unit =
      var row = 0
      while (row < slots.length) {
        into(row) = source(slots(row))
        row += 1
      }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class IntKeys(measureKind: ColumnKind):
    private val measures = new Measures(measureKind)
    private var cap = 16
    private var keys = new Array[Int](cap)
    private var used = new Array[Boolean](cap)
    private var size = 0

    def absorb(batches: Seq[ColumnBatch], key: Int, measure: Int): Unit =
      measureKind match
        case ColumnKind.Int32 => absorbInts(batches, key, measure)
        case ColumnKind.Int64 => absorbLongs(batches, key, measure)
        case ColumnKind.Float64 => absorbDoubles(batches, key, measure)
        case ColumnKind.Float32 => absorbFloats(batches, key, measure)
        case other => throw new IllegalArgumentException(s"measure $other does not aggregate")

    def emit(): ColumnBatch =
      val rows = size
      val outKeys = new Array[Int](rows)
      val slots = new Array[Int](rows)
      var cursor = 0
      var index = 0
      while (index < cap) {
        if (used(index)) then
          outKeys(cursor) = keys(index)
          slots(cursor) = index
          cursor += 1
        index += 1
      }
      val keyColumn = Int32Column.of(outKeys, Validity.allValid(rows), rows)
      val rest = measures.columns(slots)
      val columns = keyColumn +: rest
      new ColumnBatch(columns.map(_.logicalType), columns, rows)

    private def absorbInts(batches: Seq[ColumnBatch], key: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val keyColumn = intColumn(batch, key)
        val measureColumn = intColumn(batch, measure)
        var row = 0
        while (row < batch.length) {
          if (keyColumn.isValid(row)) then
            val slot = slotOf(keyColumn.values(row))
            measures.count(slot)
            if (measureColumn.isValid(row)) then measures.noteInt(slot, measureColumn.values(row))
          row += 1
        }
      }

    private def absorbLongs(batches: Seq[ColumnBatch], key: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val keyColumn = intColumn(batch, key)
        val measureColumn = longColumn(batch, measure)
        var row = 0
        while (row < batch.length) {
          if (keyColumn.isValid(row)) then
            val slot = slotOf(keyColumn.values(row))
            measures.count(slot)
            if (measureColumn.isValid(row)) then measures.noteLong(slot, measureColumn.values(row))
          row += 1
        }
      }

    private def absorbDoubles(batches: Seq[ColumnBatch], key: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val keyColumn = intColumn(batch, key)
        val measureColumn = doubleColumn(batch, measure)
        var row = 0
        while (row < batch.length) {
          if (keyColumn.isValid(row)) then
            val slot = slotOf(keyColumn.values(row))
            measures.count(slot)
            if (measureColumn.isValid(row)) then
              measures.noteDouble(slot, measureColumn.values(row))
          row += 1
        }
      }

    private def absorbFloats(batches: Seq[ColumnBatch], key: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val keyColumn = intColumn(batch, key)
        val measureColumn = floatColumn(batch, measure)
        var row = 0
        while (row < batch.length) {
          if (keyColumn.isValid(row)) then
            val slot = slotOf(keyColumn.values(row))
            measures.count(slot)
            if (measureColumn.isValid(row)) then
              measures.noteFloat(slot, measureColumn.values(row))
          row += 1
        }
      }

    private def slotOf(key: Int): Int =
      if ((size + 1) * 10 > cap * 7) then grow()
      place(key)

    private def place(key: Int): Int =
      var index = mix(key) & (cap - 1)
      while (used(index) && keys(index) != key) {
        index = (index + 1) & (cap - 1)
      }
      if (!used(index)) then
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
      keys = new Array[Int](cap)
      used = new Array[Boolean](cap)
      size = 0
      var row = 0
      while (row < oldCap) {
        if (oldUsed(row)) then mapping(row) = place(oldKeys(row))
        row += 1
      }
      measures.reindex(mapping, oldCap, cap)

    private def mix(key: Int): Int =
      val mixed = key * 0x9e3779b9
      mixed ^ (mixed >>> 16)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class LongKeys(measureKind: ColumnKind):
    private val measures = new Measures(measureKind)
    private var cap = 16
    private var keys = new Array[Long](cap)
    private var used = new Array[Boolean](cap)
    private var size = 0

    def absorb(batches: Seq[ColumnBatch], key: Int, measure: Int): Unit =
      measureKind match
        case ColumnKind.Int32 => absorbInts(batches, key, measure)
        case ColumnKind.Int64 => absorbLongs(batches, key, measure)
        case ColumnKind.Float64 => absorbDoubles(batches, key, measure)
        case ColumnKind.Float32 => absorbFloats(batches, key, measure)
        case other => throw new IllegalArgumentException(s"measure $other does not aggregate")

    def emit(): ColumnBatch =
      val rows = size
      val outKeys = new Array[Long](rows)
      val slots = new Array[Int](rows)
      var cursor = 0
      var index = 0
      while (index < cap) {
        if (used(index)) then
          outKeys(cursor) = keys(index)
          slots(cursor) = index
          cursor += 1
        index += 1
      }
      val keyColumn = Int64Column.of(outKeys, Validity.allValid(rows), rows)
      val rest = measures.columns(slots)
      val columns = keyColumn +: rest
      new ColumnBatch(columns.map(_.logicalType), columns, rows)

    private def absorbInts(batches: Seq[ColumnBatch], key: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val keyColumn = longColumn(batch, key)
        val measureColumn = intColumn(batch, measure)
        var row = 0
        while (row < batch.length) {
          if (keyColumn.isValid(row)) then
            val slot = slotOf(keyColumn.values(row))
            measures.count(slot)
            if (measureColumn.isValid(row)) then measures.noteInt(slot, measureColumn.values(row))
          row += 1
        }
      }

    private def absorbLongs(batches: Seq[ColumnBatch], key: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val keyColumn = longColumn(batch, key)
        val measureColumn = longColumn(batch, measure)
        var row = 0
        while (row < batch.length) {
          if (keyColumn.isValid(row)) then
            val slot = slotOf(keyColumn.values(row))
            measures.count(slot)
            if (measureColumn.isValid(row)) then measures.noteLong(slot, measureColumn.values(row))
          row += 1
        }
      }

    private def absorbDoubles(batches: Seq[ColumnBatch], key: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val keyColumn = longColumn(batch, key)
        val measureColumn = doubleColumn(batch, measure)
        var row = 0
        while (row < batch.length) {
          if (keyColumn.isValid(row)) then
            val slot = slotOf(keyColumn.values(row))
            measures.count(slot)
            if (measureColumn.isValid(row)) then
              measures.noteDouble(slot, measureColumn.values(row))
          row += 1
        }
      }

    private def absorbFloats(batches: Seq[ColumnBatch], key: Int, measure: Int): Unit =
      batches.foreach { batch =>
        val keyColumn = longColumn(batch, key)
        val measureColumn = floatColumn(batch, measure)
        var row = 0
        while (row < batch.length) {
          if (keyColumn.isValid(row)) then
            val slot = slotOf(keyColumn.values(row))
            measures.count(slot)
            if (measureColumn.isValid(row)) then
              measures.noteFloat(slot, measureColumn.values(row))
          row += 1
        }
      }

    private def slotOf(key: Long): Int =
      if ((size + 1) * 10 > cap * 7) then grow()
      place(key)

    private def place(key: Long): Int =
      var index = mix(key) & (cap - 1)
      while (used(index) && keys(index) != key) {
        index = (index + 1) & (cap - 1)
      }
      if (!used(index)) then
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
      keys = new Array[Long](cap)
      used = new Array[Boolean](cap)
      size = 0
      var row = 0
      while (row < oldCap) {
        if (oldUsed(row)) then mapping(row) = place(oldKeys(row))
        row += 1
      }
      measures.reindex(mapping, oldCap, cap)

    private def mix(key: Long): Int =
      val mixed = key * 0x9e3779b97f4a7c15L
      val folded = mixed ^ (mixed >>> 32)
      folded.toInt

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
