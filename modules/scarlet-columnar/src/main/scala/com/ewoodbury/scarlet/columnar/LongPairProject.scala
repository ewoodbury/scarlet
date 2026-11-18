package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

/**
 * Projects one long-pair batch through a `(Long, Long) => (Long, Long)`.
 *
 * The function runs once per row in `[start, start + length)`, including a null key and a null
 * value. That is the row path: `map` passes the pair through, and a null key is not a reason to
 * skip the call. [[com.ewoodbury.scarlet.columnar.ColumnKernel.projectInt64]] skips a null and is
 * not this kernel. A result that is not a pair of longs, or of nulls, throws [[ColumnarFallback]]
 * before the batch is published. `productElement` is the erasure boundary: it keeps a null cell
 * visible. The output has one row per input row, written from index 0. An empty range publishes an
 * empty batch and does not call the function.
 *
 * Each column owns its validity words. A present result uses [[Validity.allValid]] and does not
 * read those words. A null cell copies them with [[Validity.allocate]] and clears the copy. The
 * child kind selects this kernel. The function body is not inspected.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Throw",
    "org.wartremover.warts.Var",
  ),
)
object LongPairProject:

  private val published = new AtomicInteger

  /** Successful [[project]] calls since the last [[resetPublishedForTest]]. */
  private[scarlet] def publishedForTest: Int = published.get

  /** Clears [[publishedForTest]]. */
  private[scarlet] def resetPublishedForTest(): Unit =
    published.set(0)

  /** Present cells are unboxed. A null component uses the absent apply. */
  trait LongPairMap:
    def apply(key: Long, value: Long): Any
    def applyNullValue(key: Long): Any
    def applyNullKey(value: Long): Any
    def applyNulls(): Any

  /**
   * Adapts a `Tuple2 => Tuple2`. The cast is not a call. A hidden lambda and an anonymous
   * `Function1` both land here after the declared `apply` check. Each absent apply builds the pair
   * the row path would pass, so a null stays null instead of unboxing to `0`.
   */
  def adapt(mapper: Any): LongPairMap =
    val fn = mapper.asInstanceOf[((Long, Long)) => Any]
    new LongPairMap:
      def apply(key: Long, value: Long): Any = fn((key, value))
      def applyNullValue(key: Long): Any = fn(erased(Long.box(key), null))
      def applyNullKey(value: Long): Any = fn(erased(null, Long.box(value)))
      def applyNulls(): Any = fn(erased(null, null))

  def project(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapper: LongPairMap,
  ): ColumnBatch =
    require(
      batch.columns.length == 2,
      s"pair project expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int64At(batch, 0)
    val values = int64At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    Ranges.check(start, length, keys.length)
    if (length == 0) then
      publish(batch.schema, new Array[Long](0), new Array[Long](0), null, null, 0)
    else if (
        Validity.rangePresent(keys.validity, start, length) &&
        Validity.rangePresent(values.validity, start, length)
      )
    then projectDense(batch.schema, keys, values, start, length, mapper)
    else projectSparse(batch.schema, keys, values, start, length, mapper)

  /** Both columns are present in the range. The loop does not read validity. */
  private def projectDense(
      schema: IndexedSeq[LogicalType],
      keys: Int64Column,
      values: Int64Column,
      start: Int,
      length: Int,
      mapper: LongPairMap,
  ): ColumnBatch =
    val outKeys = new Array[Long](length)
    val outValues = new Array[Long](length)
    val keyArr = keys.values
    val valueArr = values.values
    val end = start + length
    var keyWords: Array[Long] = null
    var valueWords: Array[Long] = null
    var row = start
    var out = 0
    while (row < end) {
      val bits = write(mapper(keyArr(row), valueArr(row)), outKeys, outValues, out)
      if (bits != BothPresent) then
        if (keyWords == null) then
          keyWords = ownedAllValid(length)
          valueWords = ownedAllValid(length)
        if ((bits & KeyBit) == 0) then Validity.clearBit(keyWords, out)
        if ((bits & ValueBit) == 0) then Validity.clearBit(valueWords, out)
      out += 1
      row += 1
    }
    publish(schema, outKeys, outValues, keyWords, valueWords, length)

  /** Every row is one call, including a null key, a null value, or both. */
  private def projectSparse(
      schema: IndexedSeq[LogicalType],
      keys: Int64Column,
      values: Int64Column,
      start: Int,
      length: Int,
      mapper: LongPairMap,
  ): ColumnBatch =
    val outKeys = new Array[Long](length)
    val outValues = new Array[Long](length)
    val keyWords = Validity.allocate(length)
    val valueWords = Validity.allocate(length)
    val keyArr = keys.values
    val valueArr = values.values
    val keyIn = keys.validity.words
    val valueIn = values.validity.words
    val end = start + length
    var row = start
    var out = 0
    while (row < end) {
      val keyOn = Validity.isSet(keyIn, row)
      val valueOn = Validity.isSet(valueIn, row)
      val projected =
        if (keyOn && valueOn) then mapper(keyArr(row), valueArr(row))
        else if (keyOn) then mapper.applyNullValue(keyArr(row))
        else if (valueOn) then mapper.applyNullKey(valueArr(row))
        else mapper.applyNulls()
      val bits = write(projected, outKeys, outValues, out)
      if ((bits & KeyBit) != 0) then Validity.setBit(keyWords, out)
      if ((bits & ValueBit) != 0) then Validity.setBit(valueWords, out)
      out += 1
      row += 1
    }
    publish(schema, outKeys, outValues, keyWords, valueWords, length)

  /** Bit 0 is a present key. Bit 1 is a present value. Any other shape declines. */
  private def write(projected: Any, keys: Array[Long], values: Array[Long], out: Int): Int =
    projected match
      case null => decline("null")
      case row: Product if row.productArity == 2 =>
        writeCell(row.productElement(0), keys, out, KeyBit) |
          writeCell(row.productElement(1), values, out, ValueBit)
      case other => decline(foundName(other))

  private def writeCell(cell: Any, into: Array[Long], out: Int, bit: Int): Int =
    cell match
      case null =>
        into(out) = 0L
        0
      case present: Long =>
        into(out) = present
        bit
      case other => decline(foundName(other))

  private def publish(
      schema: IndexedSeq[LogicalType],
      outKeys: Array[Long],
      outValues: Array[Long],
      keyWords: Array[Long],
      valueWords: Array[Long],
      length: Int,
  ): ColumnBatch =
    val keyValidity =
      if (keyWords == null) then Validity.allValid(length)
      else Validity.fromWords(keyWords, length)
    val valueValidity =
      if (valueWords == null) then Validity.allValid(length)
      else Validity.fromWords(valueWords, length)
    val _ = published.incrementAndGet()
    new ColumnBatch(
      schema,
      Vector(
        Int64Column.of(outKeys, keyValidity, length),
        Int64Column.of(outValues, valueValidity, length),
      ),
      length,
    )

  /**
   * A private copy of an all-valid bitmap. [[Validity.allocate]] owns the array.
   * [[Validity.allValid]] is only the fill source, and its words are not written.
   */
  private def ownedAllValid(length: Int): Array[Long] =
    val words = Validity.allocate(length)
    if (words.length > 0) then
      val filled = Validity.allValid(length).words
      System.arraycopy(filled, 0, words, 0, words.length)
    words

  private def int64At(batch: ColumnBatch, ordinal: Int): Int64Column =
    ColumnKernel.asInt64(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def erased(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  private def decline(found: String): Nothing =
    throw new ColumnarFallback(s"pair map did not stay (Long, Long): found $found")

  private def foundName(value: Any): String = value match
    case null => "null"
    case other => other.asInstanceOf[AnyRef].getClass.getName

  private val KeyBit = 1
  private val ValueBit = 2
  private val BothPresent = KeyBit | ValueBit
