package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

/**
 * Projects one int-pair batch through an `(Int, Int) => (Int, Int)`.
 *
 * The function runs once per row in `[start, start + length)`, including a null key and a null
 * value. That is the row path: `map` passes the pair through, and a null key is not a reason to
 * skip the call. [[com.ewoodbury.scarlet.columnar.ColumnKernel.projectInt32]] skips a null and is
 * not this kernel. A result that is not a pair of ints, or of nulls, throws [[ColumnarFallback]]
 * before the batch is published. `productElement` is the erasure boundary: it keeps a null cell
 * visible. The output has one row per input row, written from index 0. An empty range publishes an
 * empty batch and does not call the function.
 *
 * Each column owns its validity words. A present result uses [[Validity.allValid]] and does not
 * read those words. A null cell copies them with [[Validity.allocate]] and clears the copy.
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
object IntPairProject:

  private val published = new AtomicInteger

  /** Successful [[project]] calls since the last [[resetPublishedForTest]]. */
  private[scarlet] def publishedForTest: Int = published.get

  /** Clears [[publishedForTest]]. */
  private[scarlet] def resetPublishedForTest(): Unit =
    published.set(0)

  /** Present cells are unboxed. A null component uses the absent apply. */
  trait IntPairMap:
    def apply(key: Int, value: Int): Any
    def applyNullValue(key: Int): Any
    def applyNullKey(value: Int): Any
    def applyNulls(): Any

  /**
   * One present projected pair. A null key or a null value is not passed. The caller owns the
   * group.
   */
  private[scarlet] trait IntPairSink:
    def add(key: Int, value: Int): Unit

  /**
   * Adapts a `Tuple2 => Tuple2`. The cast is not a call. A hidden lambda and an anonymous
   * `Function1` both land here after the declared `apply` check. Each absent apply builds the pair
   * the row path would pass, so a null stays null instead of unboxing to `0`.
   */
  def adapt(mapper: Any): IntPairMap =
    val fn = mapper.asInstanceOf[((Int, Int)) => Any]
    new IntPairMap:
      def apply(key: Int, value: Int): Any = fn((key, value))
      def applyNullValue(key: Int): Any = fn(erased(Int.box(key), null))
      def applyNullKey(value: Int): Any = fn(erased(null, Int.box(value)))
      def applyNulls(): Any = fn(erased(null, null))

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

  /** Both columns are present in the range. The loop does not read validity. */
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

  /**
   * Runs `mapper` on `[start, start + length)` and passes each present projected key to `add`.
   *
   * The calls match [[project]]: a null key and a null value are passed in, and an empty range
   * does not call `mapper`. The value cell is checked and dropped. A null projected key is not
   * passed to `add`. A result that is not a pair of ints throws [[ColumnarFallback]] and publishes
   * nothing. This does not allocate an output column.
   */
  private[scarlet] def visitKeys(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapper: IntPairMap,
      add: Int => Unit,
  ): Unit =
    require(
      batch.columns.length == 2,
      s"pair project expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val values = int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    Ranges.check(start, length, keys.length)
    if (length == 0) then ()
    else if (
        Validity.rangePresent(keys.validity, start, length) &&
        Validity.rangePresent(values.validity, start, length)
      )
    then visitDense(keys, values, start, length, mapper, add)
    else visitSparse(keys, values, start, length, mapper, add)

  private def visitDense(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapper: IntPairMap,
      add: Int => Unit,
  ): Unit =
    val keyArr = keys.values
    val valueArr = values.values
    val end = start + length
    var row = start
    while (row < end) {
      offer(mapper(keyArr(row), valueArr(row)), add)
      row += 1
    }

  private def visitSparse(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapper: IntPairMap,
      add: Int => Unit,
  ): Unit =
    val keyArr = keys.values
    val valueArr = values.values
    val keyIn = keys.validity.words
    val valueIn = values.validity.words
    val end = start + length
    var row = start
    while (row < end) {
      val keyOn = Validity.isSet(keyIn, row)
      val valueOn = Validity.isSet(valueIn, row)
      val projected =
        if (keyOn && valueOn) then mapper(keyArr(row), valueArr(row))
        else if (keyOn) then mapper.applyNullValue(keyArr(row))
        else if (valueOn) then mapper.applyNullKey(valueArr(row))
        else mapper.applyNulls()
      offer(projected, add)
      row += 1
    }

  /**
   * Runs `mapper` on `[start, start + length)` and passes each present projected pair to `sink`.
   *
   * The calls match [[project]]: a null key and a null value are passed in, and an empty range
   * does not call `mapper`. A null projected key is dropped. A null projected value is dropped and
   * does not create a group. A result that is not a pair of ints throws [[ColumnarFallback]] and
   * publishes nothing. This does not allocate an output column. [[visitKeys]] stays count-only.
   */
  private[scarlet] def visitPairs(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapper: IntPairMap,
      sink: IntPairSink,
  ): Unit =
    require(
      batch.columns.length == 2,
      s"pair project expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int32At(batch, 0)
    val values = int32At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    Ranges.check(start, length, keys.length)
    if (length == 0) then ()
    else if (
        Validity.rangePresent(keys.validity, start, length) &&
        Validity.rangePresent(values.validity, start, length)
      )
    then visitPairsDense(keys, values, start, length, mapper, sink)
    else visitPairsSparse(keys, values, start, length, mapper, sink)

  private def visitPairsDense(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapper: IntPairMap,
      sink: IntPairSink,
  ): Unit =
    val keyArr = keys.values
    val valueArr = values.values
    val end = start + length
    var row = start
    while (row < end) {
      offerPair(mapper(keyArr(row), valueArr(row)), sink)
      row += 1
    }

  private def visitPairsSparse(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      mapper: IntPairMap,
      sink: IntPairSink,
  ): Unit =
    val keyArr = keys.values
    val valueArr = values.values
    val keyIn = keys.validity.words
    val valueIn = values.validity.words
    val end = start + length
    var row = start
    while (row < end) {
      val keyOn = Validity.isSet(keyIn, row)
      val valueOn = Validity.isSet(valueIn, row)
      val projected =
        if (keyOn && valueOn) then mapper(keyArr(row), valueArr(row))
        else if (keyOn) then mapper.applyNullValue(keyArr(row))
        else if (valueOn) then mapper.applyNullKey(valueArr(row))
        else mapper.applyNulls()
      offerPair(projected, sink)
      row += 1
    }

  /**
   * Keeps a present key and a present value. A null key is dropped. A null value is dropped and
   * does not create a group. A cell that is not an int declines, and the value is checked first.
   */
  private def offerPair(projected: Any, sink: IntPairSink): Unit =
    projected match
      case null => decline("null")
      case row: Product if row.productArity == 2 =>
        val valueCell = row.productElement(1)
        valueCell match
          case null | _: Int => ()
          case other => decline(foundName(other))
        row.productElement(0) match
          case null => ()
          case key: Int =>
            valueCell match
              case value: Int => sink.add(key, value)
              case _ => ()
          case other => decline(foundName(other))
      case other => decline(foundName(other))

  /** Keeps a present key. A null key is dropped. The value is only a type check. */
  private def offer(projected: Any, add: Int => Unit): Unit =
    projected match
      case null => decline("null")
      case row: Product if row.productArity == 2 =>
        row.productElement(1) match
          case null | _: Int => ()
          case other => decline(foundName(other))
        row.productElement(0) match
          case null => ()
          case present: Int => add(present)
          case other => decline(foundName(other))
      case other => decline(foundName(other))

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
    val _ = published.incrementAndGet()
    new ColumnBatch(
      schema,
      Vector(
        Int32Column.of(outKeys, keyValidity, length),
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

  private def int32At(batch: ColumnBatch, ordinal: Int): Int32Column =
    ColumnKernel.asInt32(ColumnKernel.columnAt(batch, ordinal), ordinal)

  private def erased(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  private def decline(found: String): Nothing =
    throw new ColumnarFallback(s"pair map did not stay (Int, Int): found $found")

  private def foundName(value: Any): String = value match
    case null => "null"
    case other => other.asInstanceOf[AnyRef].getClass.getName

  private val KeyBit = 1
  private val ValueBit = 2
  private val BothPresent = KeyBit | ValueBit
