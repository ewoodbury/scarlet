package com.ewoodbury.scarlet.columnar

/**
 * Filter a two-column `Int32` pair or a three-column int join batch, and map a join batch.
 *
 * A pair drops a null key or a null value and does not call the predicate on that row. A slot
 * filter on a triple treats a null component as an error and names the absolute row; the predicate
 * is not called. A whole-row triple filter drops that row and does not call the predicate. A
 * triple map copies a row that has a null component and does not call the function. Filter
 * survivors stay in input order. A map keeps every input row.
 *
 * The whole-batch methods scan every live row. The range methods scan `[start, start + length)`
 * and read validity at those absolute rows. Every column is part of the predicate or the map, so
 * there is nothing else to gather. A filter's output is a new compacted batch of the same width.
 * Its arrays are sized to the range, and its length is the survivor count. An empty range
 * publishes an empty batch and does not call the predicate. A map writes `length` rows from index
 * 0. A result that is not `(Int, (Int, Int))` throws [[ColumnarFallback]] before that batch is
 * published. `productElement` is the erasure boundary: it keeps a null component visible, and a
 * null is stored as `0` with a clear bit.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object PairKernel:

  trait IntPairPredicate:
    def apply(key: Int, value: Int): Boolean

  trait IntTriplePredicate:
    def apply(key: Int, left: Int, right: Int): Boolean

  /** Returns `Any` so a type-changing map is not cast before the kernel can decline it. */
  trait IntTripleMap:
    def apply(key: Int, left: Int, right: Int): Any

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
    publishTriple(batch.schema, outKeys, outLeft, outRight, length, written)

  def filterTripleRow(batch: ColumnBatch, predicate: IntTriplePredicate): ColumnBatch =
    filterTripleRow(batch, 0, batch.length, predicate)

  /**
   * Scans `[start, start + length)`. A null component is dropped and the predicate is not called.
   * Output columns are sized to the range. An empty range does not call the predicate.
   */
  def filterTripleRow(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      predicate: IntTriplePredicate,
  ): ColumnBatch =
    val (keys, leftValues, rightValues) = tripleColumns(batch, start, length)
    val outKeys = new Array[Int](length)
    val outLeft = new Array[Int](length)
    val outRight = new Array[Int](length)
    val keyArr = keys.values
    val leftArr = leftValues.values
    val rightArr = rightValues.values
    val keyWords = keys.validity.words
    val leftWords = leftValues.validity.words
    val rightWords = rightValues.validity.words
    val end = start + length
    var written = 0
    var row = start
    while (row < end) {
      if (
          Validity.isSet(keyWords, row) &&
          Validity.isSet(leftWords, row) &&
          Validity.isSet(rightWords, row)
        )
      then
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
    publishTriple(batch.schema, outKeys, outLeft, outRight, length, written)

  def mapTriple(batch: ColumnBatch, mapper: IntTripleMap): ColumnBatch =
    mapTriple(batch, 0, batch.length, mapper)

  /**
   * Scans `[start, start + length)` and writes `length` rows. A row with any clear bit is copied
   * and the function is not called. A fully present row, including a stored zero, is one call. A
   * null component in the result is stored as `0` with a clear bit. A result that is not
   * `(Int, (Int, Int))` throws [[ColumnarFallback]] before the batch is published.
   */
  def mapTriple(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapper: IntTripleMap,
  ): ColumnBatch =
    val (keys, leftValues, rightValues) = tripleColumns(batch, start, length)
    val outKeys = new Array[Int](length)
    val outLeft = new Array[Int](length)
    val outRight = new Array[Int](length)
    val keyWords = Validity.allocate(length)
    val leftWords = Validity.allocate(length)
    val rightWords = Validity.allocate(length)
    val keyArr = keys.values
    val leftArr = leftValues.values
    val rightArr = rightValues.values
    val inKeyWords = keys.validity.words
    val inLeftWords = leftValues.validity.words
    val inRightWords = rightValues.validity.words
    val end = start + length
    var row = start
    var out = 0
    while (row < end) {
      val keyOn = Validity.isSet(inKeyWords, row)
      val leftOn = Validity.isSet(inLeftWords, row)
      val rightOn = Validity.isSet(inRightWords, row)
      if (keyOn && leftOn && rightOn) then
        writeMapped(
          mapper(keyArr(row), leftArr(row), rightArr(row)),
          outKeys,
          outLeft,
          outRight,
          keyWords,
          leftWords,
          rightWords,
          out,
        )
      else
        outKeys(out) = keyArr(row)
        outLeft(out) = leftArr(row)
        outRight(out) = rightArr(row)
        if (keyOn) then Validity.setBit(keyWords, out)
        if (leftOn) then Validity.setBit(leftWords, out)
        if (rightOn) then Validity.setBit(rightWords, out)
      out += 1
      row += 1
    }
    new ColumnBatch(
      batch.schema,
      Vector(
        Int32Column.of(outKeys, Validity.fromWords(keyWords, length), length),
        Int32Column.of(outLeft, Validity.fromWords(leftWords, length), length),
        Int32Column.of(outRight, Validity.fromWords(rightWords, length), length),
      ),
      length,
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

  private def publishTriple(
      schema: IndexedSeq[LogicalType],
      outKeys: Array[Int],
      outLeft: Array[Int],
      outRight: Array[Int],
      capacity: Int,
      written: Int,
  ): ColumnBatch =
    val present = Validity.prefixValid(capacity, written)
    new ColumnBatch(
      schema,
      Vector(
        Int32Column.of(outKeys, present, written),
        Int32Column.of(outLeft, present, written),
        Int32Column.of(outRight, present, written),
      ),
      written,
    )

  private def tripleColumns(
      batch: ColumnBatch,
      start: Int,
      length: Int,
  ): (Int32Column, Int32Column, Int32Column) =
    require(
      batch.columns.length == 3,
      s"triple batch expected 3 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val leftValues = int32At(batch, 1)
    val rightValues = int32At(batch, 2)
    require(leftValues.length == keys.length, "key and left value columns differ in length")
    require(rightValues.length == keys.length, "key and right value columns differ in length")
    Ranges.check(start, length, keys.length)
    (keys, leftValues, rightValues)

  /**
   * `productElement` keeps a null visible. A null component is absence. Any other shape is
   * [[ColumnarFallback]], thrown before the output batch is published.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def writeMapped(
      projected: Any,
      keys: Array[Int],
      left: Array[Int],
      right: Array[Int],
      keyWords: Array[Long],
      leftWords: Array[Long],
      rightWords: Array[Long],
      out: Int,
  ): Unit =
    projected match
      case null => decline("null")
      case row: Product if row.productArity == 2 =>
        row.productElement(1) match
          case null => decline("null")
          case nested: Product if nested.productArity == 2 =>
            writeComponent(row.productElement(0), keys, keyWords, out)
            writeComponent(nested.productElement(0), left, leftWords, out)
            writeComponent(nested.productElement(1), right, rightWords, out)
          case other => decline(foundName(other))
      case other => decline(foundName(other))

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def writeComponent(cell: Any, into: Array[Int], words: Array[Long], out: Int): Unit =
    cell match
      case null => into(out) = 0
      case present: Int =>
        into(out) = present
        Validity.setBit(words, out)
      case other => decline(foundName(other))

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  private def decline(found: String): Nothing =
    throw new ColumnarFallback(s"triple map did not stay (Int, (Int, Int)): found $found")

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def foundName(value: Any): String = value match
    case null => "null"
    case other => other.asInstanceOf[AnyRef].getClass.getName

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
