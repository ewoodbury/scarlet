package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

/**
 * Projects one string-pair batch through a `(String, Int) => (String, Int)`.
 *
 * The function runs once per row in `[start, start + length)`, including a null key and a null
 * value. That is the row path: `map` passes the pair through, and a null key is not a reason to
 * skip the call. `""` is a present key. A result that is not a pair of a string and an int, or of
 * nulls, throws [[ColumnarFallback]] before the batch is published. `productElement` is the
 * erasure boundary: it keeps a null cell visible. The output has one row per input row, written
 * from index 0. An empty range publishes an empty batch and does not call the function.
 *
 * Each output batch owns its dictionary. Codes are first-seen in that batch and are not the input
 * batch's codes. Each column owns its validity words. A present result uses [[Validity.allValid]]
 * and does not read those words. A null cell copies them with [[Validity.allocate]] and clears the
 * copy. The child kind selects this kernel. The function body is not inspected.
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
object StringPairProject:

  private val published = new AtomicInteger

  /** Successful [[project]] calls since the last [[resetPublishedForTest]]. */
  private[scarlet] def publishedForTest: Int = published.get

  /** Clears [[publishedForTest]]. */
  private[scarlet] def resetPublishedForTest(): Unit =
    published.set(0)

  /** Present cells are unboxed. A null component uses the absent apply. */
  trait StringPairMap:
    def apply(key: String, value: Int): Any
    def applyNullValue(key: String): Any
    def applyNullKey(value: Int): Any
    def applyNulls(): Any

  /**
   * Adapts a `Tuple2 => Tuple2`. The cast is not a call. A hidden lambda and an anonymous
   * `Function1` both land here after the declared `apply` check. Each absent apply builds the pair
   * the row path would pass, so a null stays null instead of unboxing to `0`.
   */
  def adapt(mapper: Any): StringPairMap =
    val fn = mapper.asInstanceOf[((String, Int)) => Any]
    new StringPairMap:
      def apply(key: String, value: Int): Any = fn((key, value))
      def applyNullValue(key: String): Any = fn(erased(key, null))
      def applyNullKey(value: Int): Any = fn(erased(null, Int.box(value)))
      def applyNulls(): Any = fn(erased(null, null))

  def project(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapper: StringPairMap,
  ): ColumnBatch =
    require(
      batch.columns.length == 2,
      s"pair project expected 2 columns, got ${batch.columns.length}",
    )
    val keys = utf8At(batch, 0)
    val values = int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    Ranges.check(start, length, keys.length)
    if (length == 0) then
      publish(
        batch.schema,
        new Array[String](0),
        new Array[Int](0),
        new Array[Int](0),
        null,
        null,
        0,
      )
    else if (
        Validity.rangePresent(keys.validity, start, length) &&
        Validity.rangePresent(values.validity, start, length)
      )
    then projectDense(batch.schema, keys, values, start, length, mapper)
    else projectSparse(batch.schema, keys, values, start, length, mapper)

  /** Both columns are present in the range. The loop does not read validity. */
  private def projectDense(
      schema: IndexedSeq[LogicalType],
      keys: DictUtf8Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapper: StringPairMap,
  ): ColumnBatch =
    val outCodes = new Array[Int](length)
    val outValues = new Array[Int](length)
    val keyText = keys.dictionary
    val keyCodes = keys.codes
    val valueArr = values.values
    val words = DictUtf8Column.Words.empty
    val end = start + length
    var keyWords: Array[Long] = null
    var valueWords: Array[Long] = null
    var row = start
    var out = 0
    while (row < end) {
      val bits = write(
        mapper(keyText(keyCodes(row)), valueArr(row)),
        outCodes,
        outValues,
        words,
        out,
      )
      if (bits != BothPresent) then
        if (keyWords == null) then
          keyWords = ownedAllValid(length)
          valueWords = ownedAllValid(length)
        if ((bits & KeyBit) == 0) then Validity.clearBit(keyWords, out)
        if ((bits & ValueBit) == 0) then Validity.clearBit(valueWords, out)
      out += 1
      row += 1
    }
    publish(schema, words.toArray, outCodes, outValues, keyWords, valueWords, length)

  /** Every row is one call, including a null key, a null value, or both. */
  private def projectSparse(
      schema: IndexedSeq[LogicalType],
      keys: DictUtf8Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapper: StringPairMap,
  ): ColumnBatch =
    val outCodes = new Array[Int](length)
    val outValues = new Array[Int](length)
    val keyWords = Validity.allocate(length)
    val valueWords = Validity.allocate(length)
    val keyText = keys.dictionary
    val keyCodes = keys.codes
    val valueArr = values.values
    val keyIn = keys.validity.words
    val valueIn = values.validity.words
    val words = DictUtf8Column.Words.empty
    val end = start + length
    var row = start
    var out = 0
    while (row < end) {
      val keyOn = Validity.isSet(keyIn, row)
      val valueOn = Validity.isSet(valueIn, row)
      val projected =
        if (keyOn && valueOn) then mapper(keyText(keyCodes(row)), valueArr(row))
        else if (keyOn) then mapper.applyNullValue(keyText(keyCodes(row)))
        else if (valueOn) then mapper.applyNullKey(valueArr(row))
        else mapper.applyNulls()
      val bits = write(projected, outCodes, outValues, words, out)
      if ((bits & KeyBit) != 0) then Validity.setBit(keyWords, out)
      if ((bits & ValueBit) != 0) then Validity.setBit(valueWords, out)
      out += 1
      row += 1
    }
    publish(schema, words.toArray, outCodes, outValues, keyWords, valueWords, length)

  /** Bit 0 is a present key. Bit 1 is a present value. Any other shape declines. */
  private def write(
      projected: Any,
      codes: Array[Int],
      values: Array[Int],
      words: DictUtf8Column.Words,
      out: Int,
  ): Int =
    projected match
      case null => decline("null")
      case row: Product if row.productArity == 2 =>
        writeKey(row.productElement(0), codes, words, out) |
          writeValue(row.productElement(1), values, out)
      case other => decline(foundName(other))

  /** A null key stores code `0` and stays absent. `""` is interned and stays present. */
  private def writeKey(
      cell: Any,
      codes: Array[Int],
      words: DictUtf8Column.Words,
      out: Int,
  ): Int =
    cell match
      case null =>
        codes(out) = 0
        0
      case present: String =>
        codes(out) = words.intern(present)
        KeyBit
      case other => decline(foundName(other))

  private def writeValue(cell: Any, into: Array[Int], out: Int): Int =
    cell match
      case null =>
        into(out) = 0
        0
      case present: Int =>
        into(out) = present
        ValueBit
      case other => decline(foundName(other))

  private def publish(
      schema: IndexedSeq[LogicalType],
      dictionary: Array[String],
      outCodes: Array[Int],
      outValues: Array[Int],
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
        DictUtf8Column.of(dictionary, outCodes, keyValidity, length),
        Int32Column.of(outValues, valueValidity, length),
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

  private def utf8At(batch: ColumnBatch, ordinal: Int): DictUtf8Column =
    ColumnKernel.asUtf8(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def int32At(batch: ColumnBatch, ordinal: Int): Int32Column =
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def erased(key: String, value: java.lang.Integer): (String, Int) =
    Tuple2(key, value).asInstanceOf[(String, Int)]

  private def decline(found: String): Nothing =
    throw new ColumnarFallback(s"pair map did not stay (String, Int): found $found")

  private def foundName(value: Any): String = value match
    case null => "null"
    case other => other.asInstanceOf[AnyRef].getClass.getName

  private val KeyBit = 1
  private val ValueBit = 2
  private val BothPresent = KeyBit | ValueBit
