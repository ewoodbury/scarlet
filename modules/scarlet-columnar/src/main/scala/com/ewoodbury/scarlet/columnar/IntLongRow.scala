package com.ewoodbury.scarlet.columnar

/**
 * Whole-row filter of an `(Int32, Int64)` batch.
 *
 * A row drops when either validity bit is clear, and the predicate is not called. That is the
 * int-pair row rule and the long-pair row rule. Survivors stay in input order. The output schema
 * is the input schema. An empty range publishes an empty batch and does not call the predicate.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object IntLongRow:

  trait Predicate:
    def apply(key: Int, value: Long): Boolean

  def filter(batch: ColumnBatch, predicate: Predicate): ColumnBatch =
    filter(batch, 0, batch.length, predicate)

  /** Scans `[start, start + length)`. Output columns are sized to the range. */
  def filter(batch: ColumnBatch, start: Int, length: Int, predicate: Predicate): ColumnBatch =
    require(
      batch.columns.length == 2,
      s"int-long row filter expected 2 columns, got ${batch.columns.length}",
    )
    val keys = ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)
    val values = ColumnKernel.asInt64(ColumnKernel.columnAt(batch, 1), 1)
    require(keys.length == values.length, "key and value columns differ in length")
    Ranges.check(start, length, keys.length)
    val outKeys = new Array[Int](length)
    val outValues = new Array[Long](length)
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    val end = start + length
    var written = 0
    var row = start
    while (row < end) {
      if (Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row)) then
        val key = keyValues(row)
        val value = valueValues(row)
        if (predicate(key, value)) then
          outKeys(written) = key
          outValues(written) = value
          written += 1
      row += 1
    }
    publish(batch.schema, outKeys, outValues, length, written)

  private def publish(
      schema: IndexedSeq[LogicalType],
      outKeys: Array[Int],
      outValues: Array[Long],
      capacity: Int,
      written: Int,
  ): ColumnBatch =
    val present = Validity.prefixValid(capacity, written)
    new ColumnBatch(
      schema,
      Vector(
        Int32Column.of(outKeys, present, written),
        Int64Column.of(outValues, present, written),
      ),
      written,
    )
