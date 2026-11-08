package com.ewoodbury.scarlet.columnar

/**
 * Survivor index for a long-pair row filter and a string-pair row filter.
 *
 * A long-pair row calls the predicate only when both validity bits are set. A clear bit drops the
 * row and does not call the predicate. A string-pair row passes a null key, including `""` as a
 * present string. A null int is not passed to [[StringPairRow.Predicate.apply]] as zero.
 * [[StringPairRow.Predicate.keepNullValue]] decides that row, and a kept row leaves the value bit
 * clear.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object WholeRowSelection:

  def seedLong(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      predicate: LongPairKernel.LongPairPredicate,
      index: Array[Int],
  ): Int =
    val (keys, values) = longColumns(batch)
    Ranges.check(start, length, keys.length)
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (bothPresent(keyWords, valueWords, row) && predicate(keyValues(row), valueValues(row)))
      then
        index(written) = row
        written += 1
      row += 1
    }
    written

  def restrictLong(
      batch: ColumnBatch,
      predicate: LongPairKernel.LongPairPredicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val (keys, values) = longColumns(batch)
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (bothPresent(keyWords, valueWords, row) && predicate(keyValues(row), valueValues(row)))
      then
        index(written) = row
        written += 1
      read += 1
    }
    written

  def seedString(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      predicate: StringPairRow.Predicate,
      index: Array[Int],
  ): Int =
    val (keys, values) = stringColumns(batch)
    Ranges.check(start, length, keys.length)
    val valueValues = values.values
    val valueWords = values.validity.words
    val end = start + length
    var row = start
    var written = 0
    while (row < end) {
      if (keepsString(keys, valueValues, valueWords, predicate, row)) then
        index(written) = row
        written += 1
      row += 1
    }
    written

  def restrictString(
      batch: ColumnBatch,
      predicate: StringPairRow.Predicate,
      index: Array[Int],
      count: Int,
  ): Int =
    val (keys, values) = stringColumns(batch)
    val valueValues = values.values
    val valueWords = values.validity.words
    var read = 0
    var written = 0
    while (read < count) {
      val row = index(read)
      if (keepsString(keys, valueValues, valueWords, predicate, row)) then
        index(written) = row
        written += 1
      read += 1
    }
    written

  private inline def bothPresent(
      keyWords: Array[Long],
      valueWords: Array[Long],
      row: Int,
  ): Boolean =
    Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row)

  private def keepsString(
      keys: DictUtf8Column,
      valueValues: Array[Int],
      valueWords: Array[Long],
      predicate: StringPairRow.Predicate,
      row: Int,
  ): Boolean =
    val key = ColumnKernel.utf8String(keys, row)
    if (Validity.isSet(valueWords, row)) then predicate(key, valueValues(row))
    else predicate.keepNullValue(key)

  private def longColumns(batch: ColumnBatch): (Int64Column, Int64Column) =
    require(
      batch.columns.length == 2,
      s"long pair filter expected 2 columns, got ${batch.columns.length}",
    )
    val keys = ColumnKernel.asInt64(ColumnKernel.columnAt(batch, 0), 0)
    val values = ColumnKernel.asInt64(ColumnKernel.columnAt(batch, 1), 1)
    require(keys.length == values.length, "key and value columns differ in length")
    (keys, values)

  private def stringColumns(batch: ColumnBatch): (DictUtf8Column, Int32Column) =
    require(
      batch.columns.length == 2,
      s"string pair filter expected 2 columns, got ${batch.columns.length}",
    )
    val keys = ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, 0), 0)
    val values = ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 1), 1)
    require(keys.length == values.length, "key and value columns differ in length")
    (keys, values)
}

/**
 * Whole-row predicate for a `(String, Int)` pair.
 *
 * [[Predicate.apply]] sees a present int. A null int is [[Predicate.keepNullValue]], not zero. A
 * null key is a real argument. `""` is present. `"é"` and `"e\u0301"` stay distinct.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Null",
  ),
)
private[scarlet] object StringPairRow:

  trait Predicate:
    def apply(key: String, value: Int): Boolean

    /** A clear value bit. The default drops the row. `key` is not read as a value. */
    def keepNullValue(key: String): Boolean = key match
      case _ => false

  /** One adaptation of `(String, Int) => Boolean`. A null int is not passed as zero. */
  final class Adapted(keep: ((String, Int)) => Boolean) extends Predicate:
    def apply(key: String, value: Int): Boolean = keep((key, value))

    override def keepNullValue(key: String): Boolean = keep(nullRow(key))

  // Named erasure boundary. `productElement` sees null. Reading `_2` unboxes to 0, same as the
  // row path.
  private def nullRow(key: String): (String, Int) =
    Tuple2(key, null).asInstanceOf[(String, Int)]
