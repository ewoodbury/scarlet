package com.ewoodbury.scarlet.core

import java.util.Arrays

/**
 * Owned columns of one wide-row partition.
 *
 * Each column is a private copy of the arrays it was built from. A clear presence bit is a null:
 * the value slot is zero, false, or `""`, and readers must not treat it as a value. Filter drops a
 * null in the predicate column and does not compare it. A null in another column stays null.
 * Project drops, reorders, or renames columns and does not compute. It shares the column buffers.
 */
final class WideColumns private (private val buffers: Vector[WideColumn])
    extends Iterable[WideRow]:
  val fields: Vector[ColumnField] = buffers.map(_.field)
  val length: Int = buffers(0).length

  override def knownSize: Int = length

  def iterator: Iterator[WideRow] =
    Iterator.tabulate(length)(row => WideRow(fields, buffers.map(_.cell(row))))

  private[scarlet] def columns: Vector[WideColumn] = buffers

  def rows: Vector[WideRow] = iterator.toVector

  /**
   * Rows from `from` inclusive to `until` exclusive. The slice keeps this schema, including when
   * it is empty.
   */
  override def slice(from: Int, until: Int): WideColumns =
    require(
      from >= 0 && until >= from && until <= length,
      s"slice $from until $until outside length $length",
    )
    val count = until - from
    val indexes = Array.tabulate(count)(offset => from + offset)
    new WideColumns(buffers.map(_.copyRows(indexes, count)))

  /** Drops rows whose column `ordinal` is null or fails `compare`. Other nulls stay. */
  def filter(ordinal: Int, compare: ColumnCompare): WideColumns =
    val column = at(ordinal)
    require(
      compare.allows(column.field.kind),
      s"$compare does not apply to ${column.field.kind} column ${column.field.name}",
    )
    val keep = select(column, compare)
    new WideColumns(buffers.map(_.copyRows(keep, keep.length)))

  /**
   * Appends one computed column. A null input yields null. An expression the closed set does not
   * name throws.
   */
  def compute(expr: ColumnExpr, name: String): WideColumns =
    val kind = ColumnExpr.checked(expr, fields, name)
    new WideColumns(buffers :+ WideCompute.column(buffers, expr, ColumnField(name, kind)))

  /** `spec` is output order. Each pair is a source ordinal and the output name. */
  def project(spec: Vector[(Int, String)]): WideColumns =
    require(spec.nonEmpty, "project needs a column")
    val names = spec.map(_._2)
    require(names.forall(_.nonEmpty), "column name must not be empty")
    require(names.distinct.size == names.size, "column names must be unique")
    val ordinals = spec.map(_._1)
    require(ordinals.distinct.size == ordinals.size, "project ordinals must be unique")
    new WideColumns(spec.map { (ordinal, name) => at(ordinal).renamed(name) })

  private def at(ordinal: Int): WideColumn =
    require(
      ordinal >= 0 && ordinal < buffers.size,
      s"column $ordinal outside width ${buffers.size}",
    )
    buffers(ordinal)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def select(column: WideColumn, compare: ColumnCompare): Array[Int] =
    val keep = new Array[Int](column.length)
    var written = 0
    var row = 0
    while (row < column.length) {
      if (column.isPresent(row) && compare.keep(column.cell(row))) then
        keep(written) = row
        written += 1
      row += 1
    }
    Arrays.copyOf(keep, written)

object WideColumns:

  def apply(inputs: Vector[WideInput]): WideColumns =
    require(inputs.nonEmpty, "a wide row needs at least one column")
    val buffers = inputs.map(_.toColumn)
    val width = buffers(0).length
    buffers.foreach { buffer =>
      require(
        buffer.length == width,
        s"column ${buffer.field.name} has length ${buffer.length}, expected $width",
      )
    }
    val names = buffers.map(_.field.name)
    require(names.distinct.size == names.size, "column names must be unique")
    new WideColumns(buffers)

/** One named input column. Construction copies the arrays. */
sealed trait WideInput:
  def name: String
  def length: Int
  private[scarlet] def toColumn: WideColumn

object WideInput:

  def ints(name: String, values: Array[Int]): WideInput =
    nullableInts(name, values, allPresent(values.length))

  def nullableInts(name: String, values: Array[Int], present: Array[Boolean]): WideInput =
    requireLength(name, values.length, present.length)
    new IntInput(name, values.clone(), present.clone())

  def longs(name: String, values: Array[Long]): WideInput =
    nullableLongs(name, values, allPresent(values.length))

  def nullableLongs(name: String, values: Array[Long], present: Array[Boolean]): WideInput =
    requireLength(name, values.length, present.length)
    new LongInput(name, values.clone(), present.clone())

  def doubles(name: String, values: Array[Double]): WideInput =
    nullableDoubles(name, values, allPresent(values.length))

  def nullableDoubles(name: String, values: Array[Double], present: Array[Boolean]): WideInput =
    requireLength(name, values.length, present.length)
    new DoubleInput(name, values.clone(), present.clone())

  def bools(name: String, values: Array[Boolean]): WideInput =
    nullableBools(name, values, allPresent(values.length))

  def nullableBools(name: String, values: Array[Boolean], present: Array[Boolean]): WideInput =
    requireLength(name, values.length, present.length)
    new BoolInput(name, values.clone(), present.clone())

  def strings(name: String, values: Array[String]): WideInput =
    nullableStrings(name, values, allPresent(values.length))

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def nullableStrings(name: String, values: Array[String], present: Array[Boolean]): WideInput =
    requireLength(name, values.length, present.length)
    require(values.forall(value => value != null), s"column $name stores null in the value array")
    new StringInput(name, values.clone(), present.clone())

  def floats(name: String, values: Array[Float]): WideInput =
    nullableFloats(name, values, allPresent(values.length))

  def nullableFloats(name: String, values: Array[Float], present: Array[Boolean]): WideInput =
    requireLength(name, values.length, present.length)
    new FloatInput(name, values.clone(), present.clone())

  private def requireLength(name: String, values: Int, present: Int): Unit =
    require(name.nonEmpty, "column name must not be empty")
    require(values == present, s"column $name presence length $present does not match $values")

  private def allPresent(length: Int): Array[Boolean] =
    val bits = new Array[Boolean](length)
    Arrays.fill(bits, true)
    bits

  private final class IntInput(val name: String, values: Array[Int], present: Array[Boolean])
      extends WideInput:
    def length: Int = values.length
    private[scarlet] def toColumn: WideColumn =
      new WideInt32(ColumnField(name, ColumnKind.Int32), values, present)

  private final class LongInput(val name: String, values: Array[Long], present: Array[Boolean])
      extends WideInput:
    def length: Int = values.length
    private[scarlet] def toColumn: WideColumn =
      new WideInt64(ColumnField(name, ColumnKind.Int64), values, present)

  private final class DoubleInput(
      val name: String,
      values: Array[Double],
      present: Array[Boolean],
  ) extends WideInput:
    def length: Int = values.length
    private[scarlet] def toColumn: WideColumn =
      new WideFloat64(ColumnField(name, ColumnKind.Float64), values, present)

  private final class BoolInput(val name: String, values: Array[Boolean], present: Array[Boolean])
      extends WideInput:
    def length: Int = values.length
    private[scarlet] def toColumn: WideColumn =
      new WideBool(ColumnField(name, ColumnKind.Bool), values, present)

  private final class StringInput(
      val name: String,
      values: Array[String],
      present: Array[Boolean],
  ) extends WideInput:
    def length: Int = values.length
    private[scarlet] def toColumn: WideColumn =
      new WideUtf8(ColumnField(name, ColumnKind.Utf8), values, present)

  private final class FloatInput(val name: String, values: Array[Float], present: Array[Boolean])
      extends WideInput:
    def length: Int = values.length
    private[scarlet] def toColumn: WideColumn =
      new WideFloat32(ColumnField(name, ColumnKind.Float32), values, present)

/** One column of a [[WideColumns]] partition. The arrays are owned by the partition. */
sealed trait WideColumn:
  def field: ColumnField
  def length: Int
  def isPresent(row: Int): Boolean
  def cell(row: Int): Cell
  def renamed(name: String): WideColumn
  def copyRows(rows: Array[Int], count: Int): WideColumn

final class WideInt32 private[scarlet] (
    val field: ColumnField,
    private[scarlet] val values: Array[Int],
    private[scarlet] val present: Array[Boolean],
) extends WideColumn:
  def length: Int = values.length
  def isPresent(row: Int): Boolean = present(row)
  def cell(row: Int): Cell = if (present(row)) then Cell.Int32(values(row)) else Cell.Null
  def renamed(name: String): WideColumn = new WideInt32(field.copy(name = name), values, present)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def copyRows(rows: Array[Int], count: Int): WideColumn =
    val out = new Array[Int](count)
    val bits = new Array[Boolean](count)
    var index = 0
    while (index < count) {
      val row = rows(index)
      bits(index) = present(row)
      if (present(row)) then out(index) = values(row)
      index += 1
    }
    new WideInt32(field, out, bits)

final class WideInt64 private[scarlet] (
    val field: ColumnField,
    private[scarlet] val values: Array[Long],
    private[scarlet] val present: Array[Boolean],
) extends WideColumn:
  def length: Int = values.length
  def isPresent(row: Int): Boolean = present(row)
  def cell(row: Int): Cell = if (present(row)) then Cell.Int64(values(row)) else Cell.Null
  def renamed(name: String): WideColumn = new WideInt64(field.copy(name = name), values, present)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def copyRows(rows: Array[Int], count: Int): WideColumn =
    val out = new Array[Long](count)
    val bits = new Array[Boolean](count)
    var index = 0
    while (index < count) {
      val row = rows(index)
      bits(index) = present(row)
      if (present(row)) then out(index) = values(row)
      index += 1
    }
    new WideInt64(field, out, bits)

final class WideFloat64 private[scarlet] (
    val field: ColumnField,
    private[scarlet] val values: Array[Double],
    private[scarlet] val present: Array[Boolean],
) extends WideColumn:
  def length: Int = values.length
  def isPresent(row: Int): Boolean = present(row)
  def cell(row: Int): Cell = if (present(row)) then Cell.Float64(values(row)) else Cell.Null
  def renamed(name: String): WideColumn =
    new WideFloat64(field.copy(name = name), values, present)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def copyRows(rows: Array[Int], count: Int): WideColumn =
    val out = new Array[Double](count)
    val bits = new Array[Boolean](count)
    var index = 0
    while (index < count) {
      val row = rows(index)
      bits(index) = present(row)
      if (present(row)) then out(index) = values(row)
      index += 1
    }
    new WideFloat64(field, out, bits)

final class WideBool private[scarlet] (
    val field: ColumnField,
    private[scarlet] val values: Array[Boolean],
    private[scarlet] val present: Array[Boolean],
) extends WideColumn:
  def length: Int = values.length
  def isPresent(row: Int): Boolean = present(row)
  def cell(row: Int): Cell = if (present(row)) then Cell.Bool(values(row)) else Cell.Null
  def renamed(name: String): WideColumn = new WideBool(field.copy(name = name), values, present)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def copyRows(rows: Array[Int], count: Int): WideColumn =
    val out = new Array[Boolean](count)
    val bits = new Array[Boolean](count)
    var index = 0
    while (index < count) {
      val row = rows(index)
      bits(index) = present(row)
      if (present(row)) then out(index) = values(row)
      index += 1
    }
    new WideBool(field, out, bits)

final class WideUtf8 private[scarlet] (
    val field: ColumnField,
    private[scarlet] val values: Array[String],
    private[scarlet] val present: Array[Boolean],
) extends WideColumn:
  def length: Int = values.length
  def isPresent(row: Int): Boolean = present(row)
  def cell(row: Int): Cell = if (present(row)) then Cell.Utf8(values(row)) else Cell.Null
  def renamed(name: String): WideColumn = new WideUtf8(field.copy(name = name), values, present)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def copyRows(rows: Array[Int], count: Int): WideColumn =
    val out = new Array[String](count)
    val bits = new Array[Boolean](count)
    var index = 0
    while (index < count) {
      val row = rows(index)
      bits(index) = present(row)
      out(index) = if (present(row)) then values(row) else ""
      index += 1
    }
    new WideUtf8(field, out, bits)

final class WideFloat32 private[scarlet] (
    val field: ColumnField,
    private[scarlet] val values: Array[Float],
    private[scarlet] val present: Array[Boolean],
) extends WideColumn:
  def length: Int = values.length
  def isPresent(row: Int): Boolean = present(row)
  def cell(row: Int): Cell = if (present(row)) then Cell.Float32(values(row)) else Cell.Null
  def renamed(name: String): WideColumn =
    new WideFloat32(field.copy(name = name), values, present)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def copyRows(rows: Array[Int], count: Int): WideColumn =
    val out = new Array[Float](count)
    val bits = new Array[Boolean](count)
    var index = 0
    while (index < count) {
      val row = rows(index)
      bits(index) = present(row)
      if (present(row)) then out(index) = values(row)
      index += 1
    }
    new WideFloat32(field, out, bits)
