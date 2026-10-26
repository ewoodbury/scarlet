package com.ewoodbury.scarlet.columnar

/**
 * Filter a two-column `Int32` pair or a three-column int join batch.
 *
 * A pair drops a null key or a null value and does not call the predicate on that row. A triple
 * treats a null component as an error and names the absolute row; the predicate is not called.
 * Survivors stay in input order.
 *
 * The whole-batch methods scan every live row. The range methods scan `[start, start + length)`
 * and read validity at those absolute rows. Every column is part of the predicate, so there is
 * nothing else to gather. The output is a new compacted batch of the same width. Its arrays are
 * sized to the range, and its length is the survivor count. An empty range publishes an empty
 * batch and does not call the predicate.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object PairKernel:

  trait IntPairPredicate:
    def apply(key: Int, value: Int): Boolean

  trait IntTriplePredicate:
    def apply(key: Int, left: Int, right: Int): Boolean

  def filter(batch: ColumnBatch, predicate: IntPairPredicate): ColumnBatch =
    filter(batch, 0, batch.length, predicate)

  /** Scans `[start, start + length)`. Output columns are sized to the range. */
  def filter(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      predicate: IntPairPredicate,
  ): ColumnBatch =
    require(
      batch.columns.length == 2,
      s"pair filter expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val values = int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    Ranges.check(start, length, keys.length)
    val outKeys = new Array[Int](length)
    val outValues = new Array[Int](length)
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

  def filterTriple(batch: ColumnBatch, predicate: IntTriplePredicate): ColumnBatch =
    filterTriple(batch, 0, batch.length, predicate)

  /**
   * Scans `[start, start + length)`. A null component in that range is still an error at its
   * absolute row. Output columns are sized to the range.
   */
  def filterTriple(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      predicate: IntTriplePredicate,
  ): ColumnBatch =
    require(
      batch.columns.length == 3,
      s"triple filter expected 3 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val leftValues = int32At(batch, 1)
    val rightValues = int32At(batch, 2)
    require(leftValues.length == keys.length, "key and left value columns differ in length")
    require(rightValues.length == keys.length, "key and right value columns differ in length")
    Ranges.check(start, length, keys.length)
    if (!joinValuesPresent(keys, leftValues, rightValues, start, length)) then
      failNullTriple(keys, leftValues, rightValues, start, length)
    val outKeys = new Array[Int](length)
    val outLeft = new Array[Int](length)
    val outRight = new Array[Int](length)
    val keyArr = keys.values
    val leftArr = leftValues.values
    val rightArr = rightValues.values
    val end = start + length
    var written = 0
    var row = start
    while (row < end) {
      val key = keyArr(row)
      val left = leftArr(row)
      val right = rightArr(row)
      if (predicate(key, left, right)) then
        outKeys(written) = key
        outLeft(written) = left
        outRight(written) = right
        written += 1
      row += 1
    }
    val present = Validity.prefixValid(length, written)
    new ColumnBatch(
      batch.schema,
      Vector(
        Int32Column.of(outKeys, present, written),
        Int32Column.of(outLeft, present, written),
        Int32Column.of(outRight, present, written),
      ),
      written,
    )

  private def publish(
      schema: IndexedSeq[LogicalType],
      outKeys: Array[Int],
      outValues: Array[Int],
      capacity: Int,
      written: Int,
  ): ColumnBatch =
    val present = Validity.prefixValid(capacity, written)
    new ColumnBatch(
      schema,
      Vector(
        Int32Column.of(outKeys, present, written),
        Int32Column.of(outValues, present, written),
      ),
      written,
    )

  private def joinValuesPresent(
      keys: Int32Column,
      leftValues: Int32Column,
      rightValues: Int32Column,
      start: Int,
      length: Int,
  ): Boolean =
    Validity.rangePresent(keys.validity, start, length) &&
      Validity.rangePresent(leftValues.validity, start, length) &&
      Validity.rangePresent(rightValues.validity, start, length)

  private def failNullTriple(
      keys: Int32Column,
      leftValues: Int32Column,
      rightValues: Int32Column,
      start: Int,
      length: Int,
  ): Unit =
    val end = start + length
    var row = start
    while (row < end) {
      val present =
        Validity.isSet(keys.validity.words, row) &&
          Validity.isSet(leftValues.validity.words, row) &&
          Validity.isSet(rightValues.validity.words, row)
      require(present, s"null Int32 join value at row $row")
      row += 1
    }

  private def int32At(batch: ColumnBatch, ordinal: Int): Int32Column =
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, ordinal), ordinal)
