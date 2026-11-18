package com.ewoodbury.scarlet.columnar

/**
 * Projects one `(String, Int)` batch through a `Tuple2 => Tuple2`.
 *
 * The function runs once per row in `[start, start + length)`, including a null key and a null
 * int. That is the row path. A result that is not a pair of a string-or-null and an int-or-null
 * throws [[ColumnarFallback]] before the batch is published. Each column owns its validity words.
 *
 * The key column shares the input dictionary. A key equal to the input key keeps that code. Any
 * other present key must already be in the dictionary, and then it reuses that code. A key the
 * dictionary does not hold declines. This kernel does not intern, so a map that invents a string
 * does not materialise a new dictionary. An empty range publishes no rows and does not call the
 * function. The shared dictionary is the input array, including that empty range.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.MutableDataStructures",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Throw",
    "org.wartremover.warts.Var",
  ),
)
object StringPairProject:

  /** Present cells are unboxed. A null component uses the absent apply. */
  trait StringPairMap:
    def apply(key: String, value: Int): Any
    def applyNullValue(key: String): Any
    def applyNullKey(value: Int): Any
    def applyNulls(): Any

  /**
   * Adapts a `Tuple2 => Tuple2`. The cast is not a call. Each absent apply builds the pair the row
   * path would pass, so a null stays null instead of becoming `""` or `0`.
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
      s"string pair project expected 2 columns, got ${batch.columns.length}",
    )
    val keys = utf8At(batch, 0)
    val values = int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    Ranges.check(start, length, keys.length)
    val dictionary = keys.dictionary
    if (length == 0) then
      publish(batch.schema, dictionary, new Array[Int](0), new Array[Int](0), null, null, 0)
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
    val codes = keys.codes
    val dictionary = keys.dictionary
    val valueArr = values.values
    val lookup = new DictLookup(dictionary)
    val end = start + length
    var keyWords: Array[Long] = null
    var valueWords: Array[Long] = null
    var row = start
    var out = 0
    while (row < end) {
      val inKey = dictionary(codes(row))
      val bits = write(
        mapper(inKey, valueArr(row)),
        outCodes,
        outValues,
        out,
        inPresent = true,
        inKey = inKey,
        inCode = codes(row),
        lookup = lookup,
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
    publish(schema, dictionary, outCodes, outValues, keyWords, valueWords, length)

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
    val codes = keys.codes
    val dictionary = keys.dictionary
    val valueArr = values.values
    val keyIn = keys.validity.words
    val valueIn = values.validity.words
    val lookup = new DictLookup(dictionary)
    val end = start + length
    var row = start
    var out = 0
    while (row < end) {
      val keyOn = Validity.isSet(keyIn, row)
      val valueOn = Validity.isSet(valueIn, row)
      val inKey = if (keyOn) then dictionary(codes(row)) else null
      val inCode = if (keyOn) then codes(row) else 0
      val projected =
        if (keyOn && valueOn) then mapper(inKey, valueArr(row))
        else if (keyOn) then mapper.applyNullValue(inKey)
        else if (valueOn) then mapper.applyNullKey(valueArr(row))
        else mapper.applyNulls()
      val bits = write(
        projected,
        outCodes,
        outValues,
        out,
        inPresent = keyOn,
        inKey = inKey,
        inCode = inCode,
        lookup = lookup,
      )
      if ((bits & KeyBit) != 0) then Validity.setBit(keyWords, out)
      if ((bits & ValueBit) != 0) then Validity.setBit(valueWords, out)
      out += 1
      row += 1
    }
    publish(schema, dictionary, outCodes, outValues, keyWords, valueWords, length)

  /** Bit 0 is a present key. Bit 1 is a present value. A new string declines before publish. */
  private def write(
      projected: Any,
      codes: Array[Int],
      values: Array[Int],
      out: Int,
      inPresent: Boolean,
      inKey: String,
      inCode: Int,
      lookup: DictLookup,
  ): Int =
    projected match
      case null => declineType("null")
      case row: Product if row.productArity == 2 =>
        writeKey(row.productElement(0), codes, out, inPresent, inKey, inCode, lookup) |
          writeValue(row.productElement(1), values, out)
      case other => declineType(foundName(other))

  private def writeKey(
      cell: Any,
      codes: Array[Int],
      out: Int,
      inPresent: Boolean,
      inKey: String,
      inCode: Int,
      lookup: DictLookup,
  ): Int =
    cell match
      case null =>
        codes(out) = 0
        0
      case text: String =>
        codes(out) = sharedCode(text, inPresent, inKey, inCode, lookup)
        KeyBit
      case other => declineType(foundName(other))

  /**
   * The input code when the text is that key. Otherwise a code already in the dictionary. A miss
   * declines: the dictionary cannot be shared, and this does not intern the text.
   */
  private def sharedCode(
      text: String,
      inPresent: Boolean,
      inKey: String,
      inCode: Int,
      lookup: DictLookup,
  ): Int =
    if (inPresent && ((text eq inKey) || text == inKey)) then inCode
    else
      val found = lookup.codeOf(text)
      if (found < 0) then declineDictionary()
      else found

  private def writeValue(cell: Any, values: Array[Int], out: Int): Int =
    cell match
      case null =>
        values(out) = 0
        0
      case present: Int =>
        values(out) = present
        ValueBit
      case other => declineType(foundName(other))

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
    new ColumnBatch(
      schema,
      Vector(
        DictUtf8Column.of(dictionary, outCodes, keyValidity, length),
        Int32Column.of(outValues, valueValidity, length),
      ),
      length,
    )

  /** First-seen codes of one input dictionary. Built only when a key is not the input key. */
  private final class DictLookup(dictionary: Array[String]):
    private var index: java.util.HashMap[String, Integer] = null

    def codeOf(text: String): Int =
      val table = ensure()
      val found = table.get(text)
      if (found eq null) then -1 else found.intValue

    private def ensure(): java.util.HashMap[String, Integer] =
      val existing = index
      if (existing ne null) then existing
      else
        val built = new java.util.HashMap[String, Integer](math.max(dictionary.length * 2, 1))
        var code = 0
        while (code < dictionary.length) {
          val known = dictionary(code)
          if (built.get(known) eq null) then
            val _ = built.put(known, Integer.valueOf(code))
          code += 1
        }
        index = built
        built

  /** A private copy of an all-valid bitmap. [[Validity.allValid]] is only the fill source. */
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

  private def declineType(found: String): Nothing =
    throw new ColumnarFallback(s"pair map did not stay (String, Int): found $found")

  private def declineDictionary(): Nothing =
    throw new ColumnarFallback(
      "string pair map cannot share the input dictionary: the key is not in it",
    )

  private def foundName(value: Any): String = value match
    case null => "null"
    case other => other.asInstanceOf[AnyRef].getClass.getName

  private val KeyBit = 1
  private val ValueBit = 2
  private val BothPresent = KeyBit | ValueBit
