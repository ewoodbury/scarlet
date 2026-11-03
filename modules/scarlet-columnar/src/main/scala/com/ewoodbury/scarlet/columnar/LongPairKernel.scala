package com.ewoodbury.scarlet.columnar

/**
 * Filter a two-column `Int64` pair.
 *
 * A monomorphic copy of [[PairKernel.filter]]. The int loops stay as they are. A row drops when
 * either validity bit is clear, and the predicate is not called on that row. Survivors stay in
 * input order.
 *
 * The whole-batch method scans every live row. The range method scans `[start, start + length)`
 * and reads validity at those absolute rows. Every column is part of the predicate, so there is
 * nothing else to gather. The output is a new compacted batch of the same width. Its arrays are
 * sized to the range, and its length is the survivor count. An empty range publishes an empty
 * batch and does not call the predicate.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object LongPairKernel:

  trait LongPairPredicate:
    def apply(key: Long, value: Long): Boolean

  def filter(batch: ColumnBatch, predicate: LongPairPredicate): ColumnBatch =
    filter(batch, 0, batch.length, predicate)

  /** Scans `[start, start + length)`. Output columns are sized to the range. */
  def filter(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      predicate: LongPairPredicate,
  ): ColumnBatch =
    require(
      batch.columns.length == 2,
      s"long pair filter expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int64At(batch, 0)
    val values = int64At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    Ranges.check(start, length, keys.length)
    val outKeys = new Array[Long](length)
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
      outKeys: Array[Long],
      outValues: Array[Long],
      capacity: Int,
      written: Int,
  ): ColumnBatch =
    val present = Validity.prefixValid(capacity, written)
    new ColumnBatch(
      schema,
      Vector(
        Int64Column.of(outKeys, present, written),
        Int64Column.of(outValues, present, written),
      ),
      written,
    )

  private def int64At(batch: ColumnBatch, ordinal: Int): Int64Column =
    ColumnKernel.asInt64(ColumnKernel.columnAt(batch, ordinal), ordinal)
