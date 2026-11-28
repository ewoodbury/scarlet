package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.*

/**
 * Appends one computed column to a wide batch.
 *
 * Arithmetic uses the wider input: double, then float, then long, then int. A compare yields bool.
 * A null input leaves the result null and the value slot unread. Divide uses that same widening
 * and refuses a present zero divisor. A decimal add, subtract, or multiply stays exact, and
 * decimal divide is not in this set. `length` of a string is that string's UTF-16 length. Other
 * string expressions are not in this set. Casts are the same Java conversions the row path uses. A
 * literal fills every row and is never null.
 */
object WideExpr:

  def append(
      batch: ColumnBatch,
      fields: Vector[ColumnField],
      expr: ColumnExpr,
      name: String,
  ): (Vector[ColumnField], ColumnBatch) =
    val kind = ColumnExpr.checked(expr, fields, name)
    val column = build(batch, expr, kind)
    val outFields = fields :+ ColumnField(name, kind)
    val columns = batch.columns :+ column
    (outFields, new ColumnBatch(columns.map(_.logicalType), columns, batch.length))

  private def build(batch: ColumnBatch, expr: ColumnExpr, kind: ColumnKind): Column =
    val length = batch.length
    expr match
      case ColumnExpr.Add(left, right) =>
        arithmetic(batch, left, right, kind, length, add = true, sub = false, div = false)
      case ColumnExpr.Sub(left, right) =>
        arithmetic(batch, left, right, kind, length, add = false, sub = true, div = false)
      case ColumnExpr.Mul(left, right) =>
        arithmetic(batch, left, right, kind, length, add = false, sub = false, div = false)
      case ColumnExpr.Div(left, right) =>
        arithmetic(batch, left, right, kind, length, add = false, sub = false, div = true)
      case ColumnExpr.Lit(value) =>
        literal(value, length)
      case ColumnExpr.Compare(op, left, right) =>
        compare(batch, op, left, right, length)
      case ColumnExpr.And(left, right) =>
        bools(batch, left, right, length, and = true)
      case ColumnExpr.Or(left, right) =>
        bools(batch, left, right, length, and = false)
      case ColumnExpr.Not(input) =>
        negate(batch, input, length)
      case ColumnExpr.Cast(input, to) =>
        cast(at(batch, input), to, length)
      case ColumnExpr.Len(input) =>
        WideText.length(at(batch, input), length)

  private def arithmetic(
      batch: ColumnBatch,
      left: Int,
      right: Int,
      kind: ColumnKind,
      length: Int,
      add: Boolean,
      sub: Boolean,
      div: Boolean,
  ): Column = kind match
    case ColumnKind.Int32 =>
      int32(at(batch, left), at(batch, right), length, add, sub, div)
    case ColumnKind.Int64 =>
      int64(at(batch, left), at(batch, right), length, add, sub, div)
    case ColumnKind.Float32 =>
      float32(at(batch, left), at(batch, right), length, add, sub, div)
    case ColumnKind.Float64 =>
      float64(at(batch, left), at(batch, right), length, add, sub, div)
    case ColumnKind.Decimal(_, _) =>
      if (div) then throw new IllegalArgumentException("decimal divide is not supported")
      else WideDecimal.arithmetic(at(batch, left), at(batch, right), kind, length, add, sub)
    case _ =>
      throw new IllegalArgumentException(s"arithmetic result $kind")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def int32(
      left: Column,
      right: Column,
      length: Int,
      add: Boolean,
      sub: Boolean,
      div: Boolean,
  ): Column =
    val lv = ints(left, length)
    val rv = ints(right, length)
    val lp = presence(left, length)
    val rp = presence(right, length)
    val out = new Array[Int](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      if (lp(row) && rp(row)) then
        present(row) = true
        out(row) =
          if (add) then lv(row) + rv(row)
          else if (sub) then lv(row) - rv(row)
          else if (div) then divInt(lv(row), rv(row))
          else lv(row) * rv(row)
      row += 1
    }
    Int32Column.of(out, packed(present), length)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def int64(
      left: Column,
      right: Column,
      length: Int,
      add: Boolean,
      sub: Boolean,
      div: Boolean,
  ): Column =
    val lv = longs(left, length)
    val rv = longs(right, length)
    val lp = presence(left, length)
    val rp = presence(right, length)
    val out = new Array[Long](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      if (lp(row) && rp(row)) then
        present(row) = true
        out(row) =
          if (add) then lv(row) + rv(row)
          else if (sub) then lv(row) - rv(row)
          else if (div) then divLong(lv(row), rv(row))
          else lv(row) * rv(row)
      row += 1
    }
    Int64Column.of(out, packed(present), length)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def float32(
      left: Column,
      right: Column,
      length: Int,
      add: Boolean,
      sub: Boolean,
      div: Boolean,
  ): Column =
    val lv = floats(left, length)
    val rv = floats(right, length)
    val lp = presence(left, length)
    val rp = presence(right, length)
    val out = new Array[Float](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      if (lp(row) && rp(row)) then
        present(row) = true
        out(row) =
          if (add) then lv(row) + rv(row)
          else if (sub) then lv(row) - rv(row)
          else if (div) then divFloat(lv(row), rv(row))
          else lv(row) * rv(row)
      row += 1
    }
    Float32Column.of(out, packed(present), length)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def float64(
      left: Column,
      right: Column,
      length: Int,
      add: Boolean,
      sub: Boolean,
      div: Boolean,
  ): Column =
    val lv = doubles(left, length)
    val rv = doubles(right, length)
    val lp = presence(left, length)
    val rp = presence(right, length)
    val out = new Array[Double](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      if (lp(row) && rp(row)) then
        present(row) = true
        out(row) =
          if (add) then lv(row) + rv(row)
          else if (sub) then lv(row) - rv(row)
          else if (div) then divDouble(lv(row), rv(row))
          else lv(row) * rv(row)
      row += 1
    }
    Float64Column.of(out, packed(present), length)

  private def divInt(left: Int, right: Int): Int =
    if (right == 0) then refuseZero() else left / right

  private def divLong(left: Long, right: Long): Long =
    if (right == 0L) then refuseZero() else left / right

  private def divFloat(left: Float, right: Float): Float =
    if (right == 0.0f) then refuseZero() else left / right

  private def divDouble(left: Double, right: Double): Double =
    if (right == 0.0) then refuseZero() else left / right

  private def refuseZero(): Nothing =
    throw new IllegalArgumentException("divide by zero")

  private def literal(value: ColumnLiteral, length: Int): Column =
    val present = Validity.allValid(length)
    value match
      case ColumnLiteral.Int32(number) =>
        Int32Column.of(Array.tabulate(length)(_ => number), present, length)
      case ColumnLiteral.Int64(number) =>
        Int64Column.of(Array.tabulate(length)(_ => number), present, length)
      case ColumnLiteral.Float64(number) =>
        Float64Column.of(Array.tabulate(length)(_ => number), present, length)
      case ColumnLiteral.Bool(flag) =>
        val byte: Byte = if (flag) then 1.toByte else 0.toByte
        BoolColumn.of(Array.tabulate(length)(_ => byte), present, length)
      case ColumnLiteral.Decimal(precision, scale, number) =>
        DecimalColumn.of(precision, scale, Array.tabulate(length)(_ => number), present, length)
      case ColumnLiteral.Utf8(_) =>
        throw new IllegalArgumentException("a string literal is not a computed column")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def compare(
      batch: ColumnBatch,
      op: CompareOp,
      left: Int,
      right: Int,
      length: Int,
  ): Column =
    val leftColumn = at(batch, left)
    val rightColumn = at(batch, right)
    (leftColumn, rightColumn) match
      case (_: DecimalColumn, _: DecimalColumn) =>
        return WideDecimal.compare(leftColumn, rightColumn, op, length)
      case _ => ()
    val kind = ColumnExpr
      .wider(kindOf(leftColumn), kindOf(rightColumn))
      .getOrElse(throw new IllegalArgumentException(s"$op is not numeric"))
    val out = new Array[Byte](length)
    val present = new Array[Boolean](length)
    val lp = presence(leftColumn, length)
    val rp = presence(rightColumn, length)
    kind match
      case ColumnKind.Int32 =>
        val lv = ints(leftColumn, length)
        val rv = ints(rightColumn, length)
        var row = 0
        while (row < length) {
          if (lp(row) && rp(row)) then
            present(row) = true
            if (relate(op, lv(row), rv(row))) then out(row) = 1.toByte
          row += 1
        }
      case ColumnKind.Int64 =>
        val lv = longs(leftColumn, length)
        val rv = longs(rightColumn, length)
        var row = 0
        while (row < length) {
          if (lp(row) && rp(row)) then
            present(row) = true
            if (relate(op, lv(row), rv(row))) then out(row) = 1.toByte
          row += 1
        }
      case ColumnKind.Float32 =>
        val lv = floats(leftColumn, length)
        val rv = floats(rightColumn, length)
        var row = 0
        while (row < length) {
          if (lp(row) && rp(row)) then
            present(row) = true
            if (relate(op, lv(row), rv(row))) then out(row) = 1.toByte
          row += 1
        }
      case ColumnKind.Float64 =>
        val lv = doubles(leftColumn, length)
        val rv = doubles(rightColumn, length)
        var row = 0
        while (row < length) {
          if (lp(row) && rp(row)) then
            present(row) = true
            if (relate(op, lv(row), rv(row))) then out(row) = 1.toByte
          row += 1
        }
      case _ =>
        throw new IllegalArgumentException(s"compare result $kind")
    BoolColumn.of(out, packed(present), length)

  private def relate(op: CompareOp, left: Int, right: Int): Boolean = op match
    case CompareOp.Gt => left > right
    case CompareOp.Ge => left >= right
    case CompareOp.Lt => left < right
    case CompareOp.Le => left <= right
    case CompareOp.Eq => left == right
    case CompareOp.Ne => left != right
    case CompareOp.Like | CompareOp.Unlike =>
      throw new IllegalArgumentException("a pattern is not ordered")

  private def relate(op: CompareOp, left: Long, right: Long): Boolean = op match
    case CompareOp.Gt => left > right
    case CompareOp.Ge => left >= right
    case CompareOp.Lt => left < right
    case CompareOp.Le => left <= right
    case CompareOp.Eq => left == right
    case CompareOp.Ne => left != right
    case CompareOp.Like | CompareOp.Unlike =>
      throw new IllegalArgumentException("a pattern is not ordered")

  private def relate(op: CompareOp, left: Float, right: Float): Boolean = op match
    case CompareOp.Gt => left > right
    case CompareOp.Ge => left >= right
    case CompareOp.Lt => left < right
    case CompareOp.Le => left <= right
    case CompareOp.Eq => left == right
    case CompareOp.Ne => left != right
    case CompareOp.Like | CompareOp.Unlike =>
      throw new IllegalArgumentException("a pattern is not ordered")

  private def relate(op: CompareOp, left: Double, right: Double): Boolean = op match
    case CompareOp.Gt => left > right
    case CompareOp.Ge => left >= right
    case CompareOp.Lt => left < right
    case CompareOp.Le => left <= right
    case CompareOp.Eq => left == right
    case CompareOp.Ne => left != right
    case CompareOp.Like | CompareOp.Unlike =>
      throw new IllegalArgumentException("a pattern is not ordered")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def bools(
      batch: ColumnBatch,
      left: Int,
      right: Int,
      length: Int,
      and: Boolean,
  ): Column =
    val lv = boolBytes(at(batch, left), length)
    val rv = boolBytes(at(batch, right), length)
    val lp = presence(at(batch, left), length)
    val rp = presence(at(batch, right), length)
    val out = new Array[Byte](length)
    val present = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      if (lp(row) && rp(row)) then
        present(row) = true
        val value =
          if (and) then flag(lv(row)) && flag(rv(row)) else flag(lv(row)) || flag(rv(row))
        if (value) then out(row) = 1.toByte
      row += 1
    }
    BoolColumn.of(out, packed(present), length)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def negate(batch: ColumnBatch, input: Int, length: Int): Column =
    val values = boolBytes(at(batch, input), length)
    val live = presence(at(batch, input), length)
    val out = new Array[Byte](length)
    var row = 0
    while (row < length) {
      if (live(row) && !flag(values(row))) then out(row) = 1.toByte
      row += 1
    }
    BoolColumn.of(out, packed(live), length)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def cast(column: Column, to: ColumnKind, length: Int): Column =
    val live = presence(column, length)
    to match
      case ColumnKind.Int32 =>
        val out = new Array[Int](length)
        var row = 0
        while (row < length) {
          if (live(row)) then out(row) = castInt(column, row)
          row += 1
        }
        Int32Column.of(out, packed(live), length)
      case ColumnKind.Int64 =>
        val out = new Array[Long](length)
        var row = 0
        while (row < length) {
          if (live(row)) then out(row) = castLong(column, row)
          row += 1
        }
        Int64Column.of(out, packed(live), length)
      case ColumnKind.Float32 =>
        val out = new Array[Float](length)
        var row = 0
        while (row < length) {
          if (live(row)) then out(row) = castFloat(column, row)
          row += 1
        }
        Float32Column.of(out, packed(live), length)
      case ColumnKind.Float64 =>
        val out = new Array[Double](length)
        var row = 0
        while (row < length) {
          if (live(row)) then out(row) = castDouble(column, row)
          row += 1
        }
        Float64Column.of(out, packed(live), length)
      case ColumnKind.Decimal(precision, scale) =>
        WideDecimal.castFrom(column, precision, scale, length)
      case _ =>
        throw new IllegalArgumentException(s"cannot cast to $to")

  private def castInt(column: Column, row: Int): Int = column match
    case values: Int32Column => values.values(row)
    case values: Int64Column => values.values(row).toInt
    case values: Float32Column => values.values(row).toInt
    case values: Float64Column => values.values(row).toInt
    case _ => throw new IllegalArgumentException(s"cannot cast ${column.logicalType} to int")

  private def castLong(column: Column, row: Int): Long = column match
    case values: Int32Column => values.values(row).toLong
    case values: Int64Column => values.values(row)
    case values: Float32Column => values.values(row).toLong
    case values: Float64Column => values.values(row).toLong
    case values: DecimalColumn => DecimalRules.toLong(values.values(row), values.scale)
    case _ => throw new IllegalArgumentException(s"cannot cast ${column.logicalType} to long")

  private def castFloat(column: Column, row: Int): Float = column match
    case values: Int32Column => values.values(row).toFloat
    case values: Int64Column => values.values(row).toFloat
    case values: Float32Column => values.values(row)
    case values: Float64Column => values.values(row).toFloat
    case _ => throw new IllegalArgumentException(s"cannot cast ${column.logicalType} to float")

  private def castDouble(column: Column, row: Int): Double = column match
    case values: Int32Column => values.values(row).toDouble
    case values: Int64Column => values.values(row).toDouble
    case values: Float32Column => values.values(row).toDouble
    case values: Float64Column => values.values(row)
    case _ => throw new IllegalArgumentException(s"cannot cast ${column.logicalType} to double")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def ints(column: Column, length: Int): Array[Int] = column match
    case values: Int32Column =>
      val out = new Array[Int](length)
      var row = 0
      while (row < length) {
        out(row) = values.values(row)
        row += 1
      }
      out
    case _ =>
      throw new IllegalArgumentException(s"expected an int, found ${column.logicalType}")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def longs(column: Column, length: Int): Array[Long] = column match
    case values: Int64Column =>
      val out = new Array[Long](length)
      var row = 0
      while (row < length) {
        out(row) = values.values(row)
        row += 1
      }
      out
    case values: Int32Column =>
      val out = new Array[Long](length)
      var row = 0
      while (row < length) {
        out(row) = values.values(row).toLong
        row += 1
      }
      out
    case _ =>
      throw new IllegalArgumentException(s"expected an int or long, found ${column.logicalType}")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def floats(column: Column, length: Int): Array[Float] = column match
    case values: Float32Column =>
      val out = new Array[Float](length)
      var row = 0
      while (row < length) {
        out(row) = values.values(row)
        row += 1
      }
      out
    case values: Int32Column =>
      val out = new Array[Float](length)
      var row = 0
      while (row < length) {
        out(row) = values.values(row).toFloat
        row += 1
      }
      out
    case values: Int64Column =>
      val out = new Array[Float](length)
      var row = 0
      while (row < length) {
        out(row) = values.values(row).toFloat
        row += 1
      }
      out
    case _ =>
      throw new IllegalArgumentException(s"expected a float input, found ${column.logicalType}")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def doubles(column: Column, length: Int): Array[Double] = column match
    case values: Float64Column =>
      val out = new Array[Double](length)
      var row = 0
      while (row < length) {
        out(row) = values.values(row)
        row += 1
      }
      out
    case values: Float32Column =>
      val out = new Array[Double](length)
      var row = 0
      while (row < length) {
        out(row) = values.values(row).toDouble
        row += 1
      }
      out
    case values: Int64Column =>
      val out = new Array[Double](length)
      var row = 0
      while (row < length) {
        out(row) = values.values(row).toDouble
        row += 1
      }
      out
    case values: Int32Column =>
      val out = new Array[Double](length)
      var row = 0
      while (row < length) {
        out(row) = values.values(row).toDouble
        row += 1
      }
      out
    case _ =>
      throw new IllegalArgumentException(s"expected a numeric input, found ${column.logicalType}")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def boolBytes(column: Column, length: Int): Array[Byte] = column match
    case values: BoolColumn =>
      val out = new Array[Byte](length)
      var row = 0
      while (row < length) {
        out(row) = values.values(row)
        row += 1
      }
      out
    case _ =>
      throw new IllegalArgumentException(s"expected a bool, found ${column.logicalType}")

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def presence(column: Column, length: Int): Array[Boolean] =
    val bits = new Array[Boolean](length)
    var row = 0
    while (row < length) {
      bits(row) = column.isValid(row)
      row += 1
    }
    bits

  private def packed(bits: Array[Boolean]): Validity =
    Validity.pack(bits.length, row => bits(row))

  private def flag(byte: Byte): Boolean = byte match
    case 1 => true
    case 0 => false
    case other => throw new IllegalArgumentException(s"bool byte $other is not 0 or 1")

  private def at(batch: ColumnBatch, ordinal: Int): Column =
    require(
      ordinal >= 0 && ordinal < batch.columns.size,
      s"column $ordinal outside width ${batch.columns.size}",
    )
    batch.columns(ordinal)

  private def kindOf(column: Column): ColumnKind = column.logicalType match
    case LogicalType.Int32 => ColumnKind.Int32
    case LogicalType.Int64 => ColumnKind.Int64
    case LogicalType.Float64 => ColumnKind.Float64
    case LogicalType.Bool => ColumnKind.Bool
    case LogicalType.Utf8Dict => ColumnKind.Utf8
    case LogicalType.Float32 => ColumnKind.Float32
    case LogicalType.Decimal(precision, scale) => ColumnKind.Decimal(precision, scale)
