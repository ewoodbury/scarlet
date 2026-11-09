package com.ewoodbury.scarlet.columnar

/**
 * Decodes a string outer join. An int triple goes to [[BatchCodec.decodeOuter]]. A long triple is
 * refused. A string batch is a `Utf8Dict` key and two `Int32` values. A clear key bit is null and
 * is not a dictionary lookup. An absence bit on an optional value is `None`. A clear value bit
 * with no absence bit throws.
 */
private[scarlet] object StringOuterDecode:

  /** A `Utf8Dict` key and two `Int32` values. An int or long triple is not this shape. */
  def isStringOuter(batch: ColumnBatch): Boolean = batch.columns match
    case Seq(_: DictUtf8Column, _: Int32Column, _: Int32Column) => true
    case _ => false

  def decode(batch: ColumnBatch, absent: AbsentSide): Seq[Any] =
    decode(batch, absent, batch.length)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decode(batch: ColumnBatch, absent: AbsentSide, maxRows: Int): Seq[Any] =
    batch.columns match
      case Seq(_: Int32Column, _: Int32Column, _: Int32Column) =>
        BatchCodec.decodeOuter(batch, absent, maxRows)
      case Seq(_: Int64Column, _: Int64Column, _: Int64Column) =>
        throw new IllegalArgumentException(
          s"string outer output is not a Utf8Dict key and two Int32 columns: ${batch.schema}",
        )
      case Seq(keys: DictUtf8Column, left: Int32Column, right: Int32Column) =>
        require(
          keys.length == batch.length && left.length == batch.length &&
            right.length == batch.length,
          "outer join columns disagree on length",
        )
        val rows = decodedLength(batch, maxRows)
        ColumnMetrics.decoded(rows)
        absent match
          case AbsentSide.Both =>
            Seq.tabulate(rows) { row =>
              (presentString(keys, row), (optionalInt(left, row), optionalInt(right, row)))
            }
          case AbsentSide.Left =>
            Seq.tabulate(rows) { row =>
              (presentString(keys, row), (optionalInt(left, row), presentInt(right, row)))
            }
          case AbsentSide.Right =>
            Seq.tabulate(rows) { row =>
              (presentString(keys, row), (presentInt(left, row), optionalInt(right, row)))
            }
      case _ =>
        throw new IllegalArgumentException(
          s"string outer output is not a Utf8Dict key and two Int32 columns: ${batch.schema}",
        )

  private def decodedLength(batch: ColumnBatch, maxRows: Int): Int =
    require(maxRows >= 0, s"maximum decoded rows must be >= 0, got $maxRows")
    math.min(batch.length, maxRows)

  private def optionalInt(column: Int32Column, row: Int): Option[Int] =
    if (column.isAbsent(row)) then None
    else if (column.isValid(row)) then Some(column.values(row))
    else
      throw new IllegalArgumentException(
        s"null Int32 at row $row on an optional outer column",
      )

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def presentInt(column: Int32Column, row: Int): Any =
    if (column.isValid(row)) then column.values(row) else null

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def presentString(column: DictUtf8Column, row: Int): Any =
    if (column.isValid(row)) then column.dictionary(column.codes(row)) else null
