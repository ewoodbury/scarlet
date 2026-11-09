package com.ewoodbury.scarlet.columnar

/**
 * Decodes an outer join. An int triple goes to [[BatchCodec.decodeOuter]]. A long triple reads the
 * absence bitmap on an optional value. A clear validity bit with no absence bit is a null value
 * and throws. A clear key bit stays null.
 */
private[scarlet] object LongOuterDecode:

  def decode(batch: ColumnBatch, absent: AbsentSide): Seq[Any] =
    decode(batch, absent, batch.length)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decode(batch: ColumnBatch, absent: AbsentSide, maxRows: Int): Seq[Any] =
    batch.columns match
      case Seq(_: Int32Column, _: Int32Column, _: Int32Column) =>
        BatchCodec.decodeOuter(batch, absent, maxRows)
      case Seq(keys: Int64Column, left: Int64Column, right: Int64Column) =>
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
              (presentLong(keys, row), (optionalLong(left, row), optionalLong(right, row)))
            }
          case AbsentSide.Left =>
            Seq.tabulate(rows) { row =>
              (presentLong(keys, row), (optionalLong(left, row), presentLong(right, row)))
            }
          case AbsentSide.Right =>
            Seq.tabulate(rows) { row =>
              (presentLong(keys, row), (presentLong(left, row), optionalLong(right, row)))
            }
      case _ =>
        throw new IllegalArgumentException(
          s"outer join output is not three Int32 or three Int64 columns: ${batch.schema}",
        )

  private def decodedLength(batch: ColumnBatch, maxRows: Int): Int =
    require(maxRows >= 0, s"maximum decoded rows must be >= 0, got $maxRows")
    math.min(batch.length, maxRows)

  /** Absence is `None` even at payload `0`. A clear bit with no absence mark throws. */
  private def optionalLong(column: Int64Column, row: Int): Option[Long] =
    if (column.isAbsent(row)) then None
    else if (column.isValid(row)) then Some(column.values(row))
    else
      throw new IllegalArgumentException(
        s"null Int64 at row $row on an optional outer column",
      )

  /** A clear bit stays null. A set bit is the stored `Long`. */
  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def presentLong(column: Int64Column, row: Int): Any =
    if (column.isValid(row)) then column.values(row) else null
