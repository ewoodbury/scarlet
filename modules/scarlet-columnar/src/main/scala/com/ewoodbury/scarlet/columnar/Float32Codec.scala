package com.ewoodbury.scarlet.columnar

import scala.collection.immutable.ArraySeq

/**
 * Encodes and decodes one `Float32` column.
 *
 * An owned array is copied. Any other iterable fills. A null in [[elements]] stays a clear bit.
 * [[values]] rejects that bit. `NaN` and `-0.0f` keep the bits the caller stored.
 */
object Float32Codec:

  def copied(values: Array[Float]): ColumnBatch =
    ColumnMetrics.primitiveEncode()
    batch(java.util.Arrays.copyOf(values, values.length), Validity.allValid(values.length))

  def fromIterable(values: Iterable[Float]): ColumnBatch = values match
    case array: ArraySeq.ofFloat => copied(array.unsafeArray)
    case other =>
      val filled = fill(other)
      batch(filled, Validity.allValid(filled.length))

  def fromNullable(values: Seq[Option[Float]]): ColumnBatch =
    val column = Float32Column.fromNullable(values)
    new ColumnBatch(Vector(LogicalType.Float32), Vector(column), column.length)

  def empty: ColumnBatch =
    batch(Array.emptyFloatArray, Validity.allValid(0))

  /**
   * One `Float32` column. `preserveNull` keeps a clear bit as a null `Float`. A fully present
   * column still returns the primitive values. `NaN` is present.
   *
   * Named erasure boundary. The nullable cells are `java.lang.Float` references. A `scala.Float`
   * cell would unbox a null to `0.0f`.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  def decode(batch: ColumnBatch, maxRows: Int, preserveNull: Boolean): Seq[Float] =
    val column = float32(batch)
    val rows = decodedLength(maxRows, batch.length)
    if (!preserveNull || Validity.livePresent(column.validity, rows)) then present(column, rows)
    else
      ColumnMetrics.decoded(rows)
      val boxed = Array.tabulate[AnyRef](rows)(row => floatRef(column, row))
      ArraySeq.unsafeWrapArray(boxed).asInstanceOf[Seq[Float]]

  private def present(column: Float32Column, rows: Int): Seq[Float] =
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows) { row =>
      require(column.isValid(row), s"null Float32 at row $row")
      column.values(row)
    }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def floatRef(column: Float32Column, row: Int): java.lang.Float =
    if (column.isValid(row)) java.lang.Float.valueOf(column.values(row)) else null

  private def batch(values: Array[Float], validity: Validity): ColumnBatch =
    val column = Float32Column.of(values, validity, values.length)
    new ColumnBatch(Vector(LogicalType.Float32), Vector(column), values.length)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def fill(values: Iterable[Float]): Array[Float] =
    val known = values.knownSize
    if (known < 0) then values.iterator.toArray
    else
      val out = new Array[Float](known)
      val iterator = values.iterator
      var row = 0
      while (row < known) {
        out(row) = iterator.next()
        row += 1
      }
      out

  private def float32(batch: ColumnBatch): Float32Column = batch.columns match
    case Seq(column: Float32Column) => column
    case _ =>
      throw new IllegalArgumentException(s"expected one Float32 column, got ${batch.schema}")

  private def decodedLength(maxRows: Int, rows: Int): Int =
    require(maxRows >= 0, s"maximum decoded rows must be >= 0, got $maxRows")
    math.min(rows, maxRows)
