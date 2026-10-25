package com.ewoodbury.scarlet.columnar

/**
 * Filter a two-column `Int32` pair. A null key or a null value is dropped, and the predicate is
 * not called on it. Survivors stay in input order.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object PairKernel:

  trait IntPairPredicate:
    def apply(key: Int, value: Int): Boolean

  trait IntTriplePredicate:
    def apply(key: Int, left: Int, right: Int): Boolean

  def filter(batch: ColumnBatch, predicate: IntPairPredicate): ColumnBatch =
    require(
      batch.columns.length == 2,
      s"pair filter expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val values = int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    val rows = keys.length
    val outKeys = new Array[Int](rows)
    val outValues = new Array[Int](rows)
    val keyValues = keys.values
    val valueValues = values.values
    val keyWords = keys.validity.words
    val valueWords = values.validity.words
    var written = 0
    var row = 0
    while (row < rows) {
      if (Validity.isSet(keyWords, row) && Validity.isSet(valueWords, row)) then
        val key = keyValues(row)
        val value = valueValues(row)
        if (predicate(key, value)) then
          outKeys(written) = key
          outValues(written) = value
          written += 1
      row += 1
    }
    val present = Validity.prefixValid(rows, written)
    new ColumnBatch(
      batch.schema,
      Vector(
        Int32Column.of(outKeys, present, written),
        Int32Column.of(outValues, present, written),
      ),
      written,
    )

  /**
   * Filter and compact a three-column int join batch in one pass. Join rows are non-nullable at
   * the row API, so a null component is an error rather than a row to silently discard. Survivors
   * stay in input order.
   */
  def filterTriple(batch: ColumnBatch, predicate: IntTriplePredicate): ColumnBatch =
    require(
      batch.columns.length == 3,
      s"triple filter expected 3 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val leftValues = int32At(batch, 1)
    val rightValues = int32At(batch, 2)
    val rows = keys.length
    require(leftValues.length == rows, "key and left value columns differ in length")
    require(rightValues.length == rows, "key and right value columns differ in length")
    if (!joinValuesPresent(keys, leftValues, rightValues, rows)) then
      failNullTriple(keys, leftValues, rightValues, rows)
    val outKeys = new Array[Int](rows)
    val outLeft = new Array[Int](rows)
    val outRight = new Array[Int](rows)
    val keyArr = keys.values
    val leftArr = leftValues.values
    val rightArr = rightValues.values
    var written = 0
    var row = 0
    while (row < rows) {
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
    val present = Validity.prefixValid(rows, written)
    new ColumnBatch(
      batch.schema,
      Vector(
        Int32Column.of(outKeys, present, written),
        Int32Column.of(outLeft, present, written),
        Int32Column.of(outRight, present, written),
      ),
      written,
    )

  private def joinValuesPresent(
      keys: Int32Column,
      leftValues: Int32Column,
      rightValues: Int32Column,
      rows: Int,
  ): Boolean =
    Validity.livePresent(keys.validity, rows) &&
      Validity.livePresent(leftValues.validity, rows) &&
      Validity.livePresent(rightValues.validity, rows)

  private def failNullTriple(
      keys: Int32Column,
      leftValues: Int32Column,
      rightValues: Int32Column,
      rows: Int,
  ): Unit =
    var row = 0
    while (row < rows) {
      val present =
        Validity.isSet(keys.validity.words, row) &&
          Validity.isSet(leftValues.validity.words, row) &&
          Validity.isSet(rightValues.validity.words, row)
      require(present, s"null Int32 join value at row $row")
      row += 1
    }

  private def int32At(batch: ColumnBatch, ordinal: Int): Int32Column =
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, ordinal), ordinal)
