package com.ewoodbury.scarlet.columnar

/**
 * Projects one int-pair batch through an `(Int, Int) => (Int, Int)`.
 *
 * The function runs once per present key. A null key is copied and is not passed in. A present key
 * is passed in even when the value is null, so a null value can still be counted. A result that is
 * not a pair of ints, or of nulls, throws [[ColumnarFallback]] before the batch is published.
 * `productElement` is the erasure boundary: it keeps a null cell visible. The output has one row
 * per input row, written from index 0. An empty range publishes an empty batch and does not call
 * the function.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Throw",
    "org.wartremover.warts.Var",
  ),
)
object IntPairProject:

  /** Present cells are unboxed. A null value uses [[IntPairMap.applyNullValue]]. */
  trait IntPairMap:
    def apply(key: Int, value: Int): Any
    def applyNullValue(key: Int): Any

  /**
   * Adapts a `Tuple2 => Tuple2`. The cast is not a call. A hidden lambda and an anonymous
   * `Function1` both land here after [[com.ewoodbury.scarlet.execution]] has checked the declared
   * `apply`.
   */
  def adapt(mapper: Any): IntPairMap =
    val fn = mapper.asInstanceOf[((Int, Int)) => Any]
    new IntPairMap:
      def apply(key: Int, value: Int): Any = fn((key, value))
      def applyNullValue(key: Int): Any = fn(nullValue(key))

  def project(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapper: IntPairMap,
  ): ColumnBatch =
    require(
      batch.columns.length == 2,
      s"pair project expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val values = int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    Ranges.check(start, length, keys.length)
    if (length == 0) then
      publish(batch.schema, new Array[Int](0), new Array[Int](0), null, null, 0)
    else if (
        Validity.rangePresent(keys.validity, start, length) &&
        Validity.rangePresent(values.validity, start, length)
      )
    then projectDense(batch.schema, keys, values, start, length, mapper)
    else projectSparse(batch.schema, keys, values, start, length, mapper)

  /** Both columns are present. The loop does not read validity. */
  private def projectDense(
      schema: IndexedSeq[LogicalType],
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapper: IntPairMap,
  ): ColumnBatch =
    val outKeys = new Array[Int](length)
    val outValues = new Array[Int](length)
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
          keyWords = Validity.allValid(length).words
          valueWords = Validity.allValid(length).words
        if ((bits & KeyBit) == 0) then Validity.clearBit(keyWords, out)
        if ((bits & ValueBit) == 0) then Validity.clearBit(valueWords, out)
      out += 1
      row += 1
    }
    publish(schema, outKeys, outValues, keyWords, valueWords, length)

  /** A clear key bit skips the function. A present key with a null value still calls it. */
  private def projectSparse(
      schema: IndexedSeq[LogicalType],
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapper: IntPairMap,
  ): ColumnBatch =
    val outKeys = new Array[Int](length)
    val outValues = new Array[Int](length)
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
      if (Validity.isSet(keyIn, row)) then
        val projected =
          if (Validity.isSet(valueIn, row)) then mapper(keyArr(row), valueArr(row))
          else mapper.applyNullValue(keyArr(row))
        val bits = write(projected, outKeys, outValues, out)
        if ((bits & KeyBit) != 0) then Validity.setBit(keyWords, out)
        if ((bits & ValueBit) != 0) then Validity.setBit(valueWords, out)
      else
        outKeys(out) = keyArr(row)
        outValues(out) = valueArr(row)
        if (Validity.isSet(valueIn, row)) then Validity.setBit(valueWords, out)
      out += 1
      row += 1
    }
    publish(schema, outKeys, outValues, keyWords, valueWords, length)

  /** Bit 0 is a present key. Bit 1 is a present value. Any other shape declines. */
  private def write(projected: Any, keys: Array[Int], values: Array[Int], out: Int): Int =
    projected match
      case null => decline("null")
      case row: Product if row.productArity == 2 =>
        writeCell(row.productElement(0), keys, out, KeyBit) |
          writeCell(row.productElement(1), values, out, ValueBit)
      case other => decline(foundName(other))

  private def writeCell(cell: Any, into: Array[Int], out: Int, bit: Int): Int =
    cell match
      case null =>
        into(out) = 0
        0
      case present: Int =>
        into(out) = present
        bit
      case other => decline(foundName(other))

  private def publish(
      schema: IndexedSeq[LogicalType],
      outKeys: Array[Int],
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
        Int32Column.of(outKeys, keyValidity, length),
        Int32Column.of(outValues, valueValidity, length),
      ),
      length,
    )

  private def int32At(batch: ColumnBatch, ordinal: Int): Int32Column =
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def nullValue(key: Int): (Int, Int) =
    Tuple2(Int.box(key), null).asInstanceOf[(Int, Int)]

  private def decline(found: String): Nothing =
    throw new ColumnarFallback(s"pair map did not stay (Int, Int): found $found")

  private def foundName(value: Any): String = value match
    case null => "null"
    case other => other.asInstanceOf[AnyRef].getClass.getName

  private val KeyBit = 1
  private val ValueBit = 2
  private val BothPresent = KeyBit | ValueBit
