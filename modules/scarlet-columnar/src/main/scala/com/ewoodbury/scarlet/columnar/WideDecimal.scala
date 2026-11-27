package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.{ColumnKind, CompareOp, DecimalRules}

/**
 * Exact decimal arithmetic on a wide batch.
 *
 * Add and subtract use the larger scale and one extra digit for the carry. Multiply adds the
 * precisions and the scales. A result past 18 digits refuses. A null input stays null. Divide is
 * not in this set. The ungrouped sum skips nulls and refuses a total that needs a 19th digit.
 */
object WideDecimal:

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def arithmetic(
      left: Column,
      right: Column,
      kind: ColumnKind,
      length: Int,
      add: Boolean,
      sub: Boolean,
  ): Column =
    val (precision, scale) = decimals(kind)
    val leftColumn = decimal(left)
    val rightColumn = decimal(right)
    val out = new Array[Long](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      if (leftColumn.isValid(row) && rightColumn.isValid(row)) then
        val (value, produced) =
          if (add) then
            DecimalRules.add(
              leftColumn.values(row),
              leftColumn.precision,
              leftColumn.scale,
              rightColumn.values(row),
              rightColumn.precision,
              rightColumn.scale,
            )
          else if (sub) then
            DecimalRules.subtract(
              leftColumn.values(row),
              leftColumn.precision,
              leftColumn.scale,
              rightColumn.values(row),
              rightColumn.precision,
              rightColumn.scale,
            )
          else
            DecimalRules.multiply(
              leftColumn.values(row),
              leftColumn.precision,
              leftColumn.scale,
              rightColumn.values(row),
              rightColumn.precision,
              rightColumn.scale,
            )
        if (produced != kind) then throw new IllegalArgumentException(s"decimal result $produced")
        present(row) = true
        out(row) = value
      row += 1
    }
    DecimalColumn.of(precision, scale, out, packed(present), length)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def compare(left: Column, right: Column, op: CompareOp, length: Int): Column =
    val leftColumn = decimal(left)
    val rightColumn = decimal(right)
    val out = new Array[Byte](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      if (leftColumn.isValid(row) && rightColumn.isValid(row)) then
        present(row) = true
        val order = DecimalRules.compare(
          leftColumn.values(row),
          leftColumn.scale,
          rightColumn.values(row),
          rightColumn.scale,
        )
        if (keep(op, order)) then out(row) = 1.toByte
      row += 1
    }
    BoolColumn.of(out, packed(present), length)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def castFrom(column: Column, precision: Int, scale: Int, length: Int): Column =
    val out = new Array[Long](length)
    val present = new Array[Boolean](length)
    column match
      case source: Int32Column =>
        var row = 0
        while (row < length) {
          if (source.isValid(row)) then
            present(row) = true
            out(row) = DecimalRules.fromLong(source.values(row).toLong, precision, scale)
          row += 1
        }
      case source: Int64Column =>
        var row = 0
        while (row < length) {
          if (source.isValid(row)) then
            present(row) = true
            out(row) = DecimalRules.fromLong(source.values(row), precision, scale)
          row += 1
        }
      case other =>
        throw new IllegalArgumentException(
          s"cannot cast ${other.logicalType} to decimal($precision, $scale)",
        )
    DecimalColumn.of(precision, scale, out, packed(present), length)

  /** Sum of one decimal column. Nulls contribute nothing. Empty input returns zero. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def sum(batches: Vector[ColumnBatch], ordinal: Int): Long =
    var total = 0L
    batches.foreach { batch =>
      val column = decimal(columnAt(batch, ordinal))
      var row = 0
      while (row < column.length) {
        if (column.isValid(row)) then total = add(total, column.values(row))
        row += 1
      }
    }
    total

  /**
   * Keeps rows whose decimal column passes `op` against `constant`. A null does not pass. Other
   * columns are copied in survivor order.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def filter(
      batch: ColumnBatch,
      ordinal: Int,
      op: CompareOp,
      precision: Int,
      scale: Int,
      constant: Long,
  ): ColumnBatch =
    DecimalRules.check(precision, scale)
    if (!DecimalRules.fits(constant, precision)) then
      throw new IllegalArgumentException(s"decimal value does not fit precision $precision")
    val column = decimal(columnAt(batch, ordinal))
    val kept = new Array[Int](batch.length)
    var count = 0
    var row = 0
    while (row < column.length) {
      if (column.isValid(row)) then
        val order = DecimalRules.compare(column.values(row), column.scale, constant, scale)
        if (keep(op, order)) then
          kept(count) = row
          count += 1
      row += 1
    }
    val rows = java.util.Arrays.copyOf(kept, count)
    val columns = batch.columns.map(source => take(source, rows, count))
    new ColumnBatch(columns.map(_.logicalType), columns, count)

  private def add(total: Long, value: Long): Long =
    val sum =
      try Math.addExact(total, value)
      catch case _: ArithmeticException => refuse()
    if (!DecimalRules.fits(sum, DecimalRules.MaxPrecision)) then refuse()
    sum

  private def refuse(): Nothing =
    throw new IllegalArgumentException(
      s"decimal value does not fit precision ${DecimalRules.MaxPrecision}",
    )

  private def decimals(kind: ColumnKind): (Int, Int) = kind match
    case ColumnKind.Decimal(precision, scale) => (precision, scale)
    case other => throw new IllegalArgumentException(s"decimal result $other")

  private def decimal(column: Column): DecimalColumn = column match
    case values: DecimalColumn => values
    case other =>
      throw new IllegalArgumentException(s"expected a decimal, found ${other.logicalType}")

  private def columnAt(batch: ColumnBatch, ordinal: Int): Column =
    require(
      ordinal >= 0 && ordinal < batch.columns.size,
      s"column $ordinal outside width ${batch.columns.size}",
    )
    batch.columns(ordinal)

  private def keep(op: CompareOp, order: Int): Boolean = op match
    case CompareOp.Gt => order > 0
    case CompareOp.Ge => order >= 0
    case CompareOp.Lt => order < 0
    case CompareOp.Le => order <= 0
    case CompareOp.Eq => order == 0
    case CompareOp.Ne => order != 0

  private def packed(bits: Array[Boolean]): Validity =
    Validity.pack(bits.length, row => bits(row))

  private def take(column: Column, rows: Array[Int], count: Int): Column = column match
    case source: DecimalColumn => DecimalColumn.gather(source, rows, count, count)
    case source: Int32Column =>
      if (source.absence.isDefined) then unsupported(source)
      val (values, validity) = takeInts(source.values, row => source.isValid(row), rows, count)
      Int32Column.of(values, validity, count)
    case source: Int64Column =>
      if (source.absence.isDefined) then unsupported(source)
      val (values, validity) = takeLongs(source.values, row => source.isValid(row), rows, count)
      Int64Column.of(values, validity, count)
    case source: Float64Column =>
      val (values, validity) = takeDoubles(source.values, row => source.isValid(row), rows, count)
      Float64Column.of(values, validity, count)
    case source: Float32Column =>
      val (values, validity) = takeFloats(source.values, row => source.isValid(row), rows, count)
      Float32Column.of(values, validity, count)
    case source: BoolColumn =>
      val (values, validity) = takeBytes(source.values, row => source.isValid(row), rows, count)
      BoolColumn.of(values, validity, count)
    case source: DictUtf8Column =>
      val (codes, validity) = takeInts(source.codes, row => source.isValid(row), rows, count)
      DictUtf8Column.of(source.dictionary, codes, validity, count)

  private def unsupported(column: Column): Nothing =
    throw new IllegalArgumentException(s"${column.logicalType} is not supported here")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def takeLongs(
      values: Array[Long],
      valid: Int => Boolean,
      rows: Array[Int],
      count: Int,
  ): (Array[Long], Validity) =
    val out = new Array[Long](count)
    val present = new Array[Boolean](count)
    var slot = 0
    while (slot < count) {
      val row = rows(slot)
      if (valid(row)) then
        present(slot) = true
        out(slot) = values(row)
      slot += 1
    }
    (out, packed(present))

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def takeInts(
      values: Array[Int],
      valid: Int => Boolean,
      rows: Array[Int],
      count: Int,
  ): (Array[Int], Validity) =
    val out = new Array[Int](count)
    val present = new Array[Boolean](count)
    var slot = 0
    while (slot < count) {
      val row = rows(slot)
      if (valid(row)) then
        present(slot) = true
        out(slot) = values(row)
      slot += 1
    }
    (out, packed(present))

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def takeDoubles(
      values: Array[Double],
      valid: Int => Boolean,
      rows: Array[Int],
      count: Int,
  ): (Array[Double], Validity) =
    val out = new Array[Double](count)
    val present = new Array[Boolean](count)
    var slot = 0
    while (slot < count) {
      val row = rows(slot)
      if (valid(row)) then
        present(slot) = true
        out(slot) = values(row)
      slot += 1
    }
    (out, packed(present))

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def takeFloats(
      values: Array[Float],
      valid: Int => Boolean,
      rows: Array[Int],
      count: Int,
  ): (Array[Float], Validity) =
    val out = new Array[Float](count)
    val present = new Array[Boolean](count)
    var slot = 0
    while (slot < count) {
      val row = rows(slot)
      if (valid(row)) then
        present(slot) = true
        out(slot) = values(row)
      slot += 1
    }
    (out, packed(present))

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def takeBytes(
      values: Array[Byte],
      valid: Int => Boolean,
      rows: Array[Int],
      count: Int,
  ): (Array[Byte], Validity) =
    val out = new Array[Byte](count)
    val present = new Array[Boolean](count)
    var slot = 0
    while (slot < count) {
      val row = rows(slot)
      if (valid(row)) then
        present(slot) = true
        out(slot) = values(row)
      slot += 1
    }
    (out, packed(present))
