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
   * One present projected pair, or a present key whose value is null.
   *
   * A null key is not passed. [[addMissing]] is the [[LongHashReduce]] null group: it stays
   * missing until another value arrives, and a later combine unboxes that null to `0`.
   */
  private[scarlet] trait LongPairSink:
    def add(key: Long, value: Long): Unit
    def addMissing(key: Long): Unit

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

  /**
   * Runs `mapper` on `[start, start + length)` and passes each present projected key to `add`.
   *
   * The calls match [[project]]: a null key and a null value are passed in, and an empty range
   * does not call `mapper`. The value cell is checked and dropped. A null projected key is not
   * passed to `add`. A result that is not a pair of longs throws [[ColumnarFallback]] and
   * publishes nothing. This does not allocate an output column.
   */
  private[scarlet] def visitKeys(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapper: LongPairMap,
      add: Long => Unit,
  ): Unit =
    require(
      batch.columns.length == 2,
      s"pair project expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int64At(batch, 0)
    val values = int64At(batch, 1)
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
      keys: Int64Column,
      values: Int64Column,
      start: Int,
      length: Int,
      mapper: LongPairMap,
      add: Long => Unit,
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
      keys: Int64Column,
      values: Int64Column,
      start: Int,
      length: Int,
      mapper: LongPairMap,
      add: Long => Unit,
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
   * Runs `mapper` on `[start, start + length)` and passes each projected pair to `sink`.
   *
   * The calls match [[project]]: a null key and a null value are passed in, and an empty range
   * does not call `mapper`. A null projected key is dropped. A null projected value is
   * [[LongPairSink.addMissing]]. A result that is not a pair of longs throws [[ColumnarFallback]]
   * and publishes nothing. This does not allocate an output column. [[visitKeys]] stays
   * count-only.
   */
  private[scarlet] def visitPairs(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapper: LongPairMap,
      sink: LongPairSink,
  ): Unit =
    require(
      batch.columns.length == 2,
      s"pair project expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int64At(batch, 0)
    val values = int64At(batch, 1)
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
      keys: Int64Column,
      values: Int64Column,
      start: Int,
      length: Int,
      mapper: LongPairMap,
      sink: LongPairSink,
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
      keys: Int64Column,
      values: Int64Column,
      start: Int,
      length: Int,
      mapper: LongPairMap,
      sink: LongPairSink,
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
   * [[visitPairs]] for one long slot above the project.
   *
   * The adapter calls match [[project]], including a null key and a null value. A null projected
   * key is dropped, and a key predicate is not called. A null projected value is
   * [[LongPairSink.addMissing]] when the slot reads the key and keeps it, and a value predicate is
   * not called. A value slot that keeps a present value still drops a null key. A result that is
   * not a pair of longs throws [[ColumnarFallback]] and publishes nothing. This does not allocate
   * an output column. `column` is `0` for the key and `1` for the value.
   */
  private[scarlet] def visitFiltered(
      batch: ColumnBatch,
      start: Int,
      length: Int,
      mapper: LongPairMap,
      column: Int,
      keep: Long => Boolean,
      sink: LongPairSink,
  ): Unit =
    require(column == 0 || column == 1, s"pair slot must be a key or a value, got $column")
    require(
      batch.columns.length == 2,
      s"pair project expected 2 columns, got ${batch.columns.length}",
    )
    val keys = int64At(batch, 0)
    val values = int64At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    Ranges.check(start, length, keys.length)
    if (length == 0) then ()
    else if (
        Validity.rangePresent(keys.validity, start, length) &&
        Validity.rangePresent(values.validity, start, length)
      )
    then visitFilteredDense(keys, values, start, length, mapper, column, keep, sink)
    else visitFilteredSparse(keys, values, start, length, mapper, column, keep, sink)

  private def visitFilteredDense(
      keys: Int64Column,
      values: Int64Column,
      start: Int,
      length: Int,
      mapper: LongPairMap,
      column: Int,
      keep: Long => Boolean,
      sink: LongPairSink,
  ): Unit =
    val keyArr = keys.values
    val valueArr = values.values
    val end = start + length
    var row = start
    while (row < end) {
      offerFiltered(mapper(keyArr(row), valueArr(row)), column, keep, sink)
      row += 1
    }

  private def visitFilteredSparse(
      keys: Int64Column,
      values: Int64Column,
      start: Int,
      length: Int,
      mapper: LongPairMap,
      column: Int,
      keep: Long => Boolean,
      sink: LongPairSink,
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
      offerFiltered(projected, column, keep, sink)
      row += 1
    }

  /**
   * [[visitFiltered]] over absolute ids `ids[from until until)`.
   *
   * `dense` is true when both source columns are present for the whole batch. The caller computes
   * that once, so a survivor index is not scanned again per morsel. The adapter calls match
   * [[project]], including a null key and a null value at a selected id. A null projected key is
   * dropped. A null projected value is [[LongPairSink.addMissing]] when the slot keeps the key.
   * This does not allocate an output column and does not resize the source columns.
   */
  private[scarlet] def visitFilteredSelected(
      batch: ColumnBatch,
      ids: Array[Int],
      from: Int,
      until: Int,
      mapper: LongPairMap,
      column: Int,
      keep: Long => Boolean,
      sink: LongPairSink,
      dense: Boolean,
  ): Unit =
    require(column == 0 || column == 1, s"pair slot must be a key or a value, got $column")
    require(
      batch.columns.length == 2,
      s"pair project expected 2 columns, got ${batch.columns.length}",
    )
    require(from >= 0, s"selection start must be >= 0, got $from")
    require(until >= from, s"selection end $until is before start $from")
    require(until <= ids.length, s"selection end $until exceeds ids ${ids.length}")
    val keys = int64At(batch, 0)
    val values = int64At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    if (until == from) then ()
    else if (dense) then
      visitFilteredIdsDense(keys, values, ids, from, until, mapper, column, keep, sink)
    else visitFilteredIdsSparse(keys, values, ids, from, until, mapper, column, keep, sink)

  /** Both live columns are present. One bitmap scan for a whole source batch. */
  private[scarlet] def sourcesPresent(batch: ColumnBatch): Boolean =
    val keys = int64At(batch, 0)
    val values = int64At(batch, 1)
    Validity.rangePresent(keys.validity, 0, keys.length) &&
    Validity.rangePresent(values.validity, 0, values.length)

  private def visitFilteredIdsDense(
      keys: Int64Column,
      values: Int64Column,
      ids: Array[Int],
      from: Int,
      until: Int,
      mapper: LongPairMap,
      column: Int,
      keep: Long => Boolean,
      sink: LongPairSink,
  ): Unit =
    val keyArr = keys.values
    val valueArr = values.values
    var slot = from
    while (slot < until) {
      val row = ids(slot)
      offerFiltered(mapper(keyArr(row), valueArr(row)), column, keep, sink)
      slot += 1
    }

  private def visitFilteredIdsSparse(
      keys: Int64Column,
      values: Int64Column,
      ids: Array[Int],
      from: Int,
      until: Int,
      mapper: LongPairMap,
      column: Int,
      keep: Long => Boolean,
      sink: LongPairSink,
  ): Unit =
    val keyArr = keys.values
    val valueArr = values.values
    val keyIn = keys.validity.words
    val valueIn = values.validity.words
    var slot = from
    while (slot < until) {
      val row = ids(slot)
      val keyOn = Validity.isSet(keyIn, row)
      val valueOn = Validity.isSet(valueIn, row)
      val projected =
        if (keyOn && valueOn) then mapper(keyArr(row), valueArr(row))
        else if (keyOn) then mapper.applyNullValue(keyArr(row))
        else if (valueOn) then mapper.applyNullKey(valueArr(row))
        else mapper.applyNulls()
      offerFiltered(projected, column, keep, sink)
      slot += 1
    }

  /**
   * Keeps a present key. A null key is dropped. A null value is a null group. A cell that is not a
   * long declines, and the value is checked first.
   */
  private def offerPair(projected: Any, sink: LongPairSink): Unit =
    projected match
      case null => decline("null")
      case row: Product if row.productArity == 2 =>
        val valueCell = row.productElement(1)
        valueCell match
          case null | _: Long => ()
          case other => decline(foundName(other))
        row.productElement(0) match
          case null => ()
          case key: Long =>
            valueCell match
              case value: Long => sink.add(key, value)
              case _ => sink.addMissing(key)
          case other => decline(foundName(other))
      case other => decline(foundName(other))

  /**
   * [[offerPair]] plus one long slot. The value cell is checked first, matching [[offerPair]]. A
   * null slot cell is dropped and `keep` is not called. A key slot that keeps a present key still
   * records a null value. A value slot that keeps a present value still drops a null key.
   */
  private def offerFiltered(
      projected: Any,
      column: Int,
      keep: Long => Boolean,
      sink: LongPairSink,
  ): Unit =
    projected match
      case null => decline("null")
      case row: Product if row.productArity == 2 =>
        val valueCell = row.productElement(1)
        valueCell match
          case null | _: Long => ()
          case other => decline(foundName(other))
        val keyCell = row.productElement(0)
        keyCell match
          case null | _: Long => ()
          case other => decline(foundName(other))
        if (column == 0) then keepKey(keyCell, valueCell, keep, sink)
        else keepValue(keyCell, valueCell, keep, sink)
      case other => decline(foundName(other))

  private def keepKey(
      keyCell: Any,
      valueCell: Any,
      keep: Long => Boolean,
      sink: LongPairSink,
  ): Unit =
    keyCell match
      case key: Long if keep(key) =>
        valueCell match
          case value: Long => sink.add(key, value)
          case _ => sink.addMissing(key)
      case _ => ()

  private def keepValue(
      keyCell: Any,
      valueCell: Any,
      keep: Long => Boolean,
      sink: LongPairSink,
  ): Unit =
    valueCell match
      case value: Long if keep(value) =>
        keyCell match
          case key: Long => sink.add(key, value)
          case _ => ()
      case _ => ()

  /** Keeps a present key. A null key is dropped. The value is only a type check. */
  private def offer(projected: Any, add: Long => Unit): Unit =
    projected match
      case null => decline("null")
      case row: Product if row.productArity == 2 =>
        row.productElement(1) match
          case null | _: Long => ()
          case other => decline(foundName(other))
        row.productElement(0) match
          case null => ()
          case present: Long => add(present)
          case other => decline(foundName(other))
      case other => decline(foundName(other))

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
