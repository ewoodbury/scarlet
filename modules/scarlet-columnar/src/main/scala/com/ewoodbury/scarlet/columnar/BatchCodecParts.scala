package com.ewoodbury.scarlet.columnar

/** Column builders and readers for [[BatchCodec]]. */
private[columnar] trait BatchCodecParts:
  /** Throws once, naming the first clear live bit. Bits at `length` and beyond are not live. */
  protected def requireLive(column: Column): Unit =
    requireLiveRows(column.validity.words, column.length)

  /**
   * Same word check as [[requireLive]], limited to the decoded prefix. The tail word is masked to
   * `rows`, so the scan does not walk one bit at a time.
   */
  protected def requireLivePrefix(column: Column, rows: Int): Unit =
    requireLiveRows(column.validity.words, rows)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected def requireLiveRows(words: Array[Long], live: Int): Unit =
    val fullWords = Validity.liveWords(live)
    var index = 0
    while (index < fullWords) {
      if (words(index) != -1L) then failNull(words, Validity.wordStart(index), live)
      index += 1
    }
    val tail = Validity.tailBits(live)
    if (tail != 0) then
      val mask = Validity.tailMask(tail)
      if ((words(fullWords) & mask) != mask) then
        failNull(words, Validity.wordStart(fullWords), live)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected def failNull(words: Array[Long], from: Int, live: Int): Nothing =
    var row = from
    val limit = math.min(live, from + 64)
    while (row < limit) {
      if (!Validity.isSet(words, row)) then
        throw new IllegalArgumentException(s"null join value at row $row")
      row += 1
    }
    throw new IllegalArgumentException(s"null join value at row $from")

  /** Fills from the iterator. `size` then one pass, so a `Seq` is not copied twice. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected def fillInts(values: Iterable[Int]): Array[Int] =
    val count = values.size
    val filled = new Array[Int](count)
    val rows = values.iterator
    var row = 0
    while (row < count) {
      filled(row) = rows.next()
      row += 1
    }
    filled

  /** Fills from the iterator. `size` then one pass, so a `Seq` is not copied twice. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected def fillLongs(values: Iterable[Long]): Array[Long] =
    val count = values.size
    val filled = new Array[Long](count)
    val rows = values.iterator
    var row = 0
    while (row < count) {
      filled(row) = rows.next()
      row += 1
    }
    filled

  /** Fills from the iterator. `size` then one pass, so a `Seq` is not copied twice. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected def fillBoolBytes(values: Iterable[Boolean]): Array[Byte] =
    val count = values.size
    val filled = new Array[Byte](count)
    val rows = values.iterator
    var row = 0
    while (row < count) {
      filled(row) = if (rows.next()) 1.toByte else 0.toByte
      row += 1
    }
    filled

  /** Fills from the iterator. `size` then one pass, so a `Seq` is not copied twice. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected def fillDoubles(values: Iterable[Double]): Array[Double] =
    val count = values.size
    val filled = new Array[Double](count)
    val rows = values.iterator
    var row = 0
    while (row < count) {
      filled(row) = rows.next()
      row += 1
    }
    filled

  /**
   * Fills both long columns in one pass. A null component is absent. Any other non-long cell
   * fails. The absence bitmap appears only after the first null, so a present column stays valid.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected def fillLongPairs(values: Iterable[(Long, Long)]): ColumnBatch =
    val count = values.size
    val leftValues = new Array[Long](count)
    val rightValues = new Array[Long](count)
    val rows = values.iterator
    var row = 0
    var leftAbsent = Option.empty[Array[Boolean]]
    var rightAbsent = Option.empty[Array[Boolean]]
    while (row < count) {
      val pair = rows.next()
      leftAbsent = writeLongComponent(pair, 0, leftValues, leftAbsent, count, row)
      rightAbsent = writeLongComponent(pair, 1, rightValues, rightAbsent, count, row)
      row += 1
    }
    longPairBatch(
      Int64Column.of(leftValues, componentValidity(leftAbsent, count), count),
      Int64Column.of(rightValues, componentValidity(rightAbsent, count), count),
    )

  /**
   * Writes a present `Long`. A null cell is absent. Any other non-long fails. Named erasure
   * boundary: `productElement` does not unbox a `(Long, Long)` cell, so a null stays absent.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  protected def writeLongComponent(
      pair: Product,
      ordinal: Int,
      into: Array[Long],
      absent: Option[Array[Boolean]],
      width: Int,
      row: Int,
  ): Option[Array[Boolean]] =
    pair.productElement(ordinal) match
      case null =>
        val flags = absent.getOrElse(new Array[Boolean](width))
        flags(row) = true
        Some(flags)
      case value: Long =>
        into(row) = value
        absent
      case other =>
        val found = other.asInstanceOf[AnyRef].getClass.getName
        throw new ColumnarFallback(s"columnar long pair expected a Long, found $found")

  /**
   * A present cell is the stored int. A clear bit is a null reference, not `0`. The return type
   * stays `java.lang.Integer`. A `scala.Int` return unboxes that null to `0`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  protected def intRef(column: Int32Column, row: Int): java.lang.Integer =
    if (column.isValid(row)) java.lang.Integer.valueOf(column.values(row)) else null

  /**
   * Named erasure boundary. The decoded tuple has to carry a null component so a sorted null key
   * matches the row path. Building it from `java.lang.Integer` keeps the null from becoming `0`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  protected def erasedIntPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  /** Named erasure boundary. A null `Integer` stays null instead of unboxing to `0`. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  protected def erasedStringInt(key: String, value: java.lang.Integer): (String, Int) =
    Tuple2(key, value).asInstanceOf[(String, Int)]

  /** Named erasure boundary. A null `Integer` stays null instead of unboxing to `0`. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  protected def erasedStringJoin(
      key: String,
      left: java.lang.Integer,
      right: java.lang.Integer,
  ): (String, (Int, Int)) =
    Tuple2(key, Tuple2(left, right)).asInstanceOf[(String, (Int, Int))]

  /**
   * Named erasure boundary. The decoded triple has to carry a null component so a map that
   * preserves absence matches the column bits. Building it from `java.lang.Integer` keeps the null
   * from becoming `0`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  protected def erasedIntTriple(
      key: java.lang.Integer,
      left: java.lang.Integer,
      right: java.lang.Integer,
  ): (Int, (Int, Int)) =
    Tuple2(key, Tuple2(left, right)).asInstanceOf[(Int, (Int, Int))]

  /**
   * A present cell is the stored long. A clear bit is a null reference, not `0`. The return type
   * stays `java.lang.Long`. A `scala.Long` return unboxes that null to `0`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  protected def longRef(column: Int64Column, row: Int): java.lang.Long =
    if (column.isValid(row)) java.lang.Long.valueOf(column.values(row)) else null

  /**
   * Named erasure boundary. The decoded tuple has to carry a null component so a reduced null
   * value matches the row path. Building it from `java.lang.Long` keeps the null from becoming
   * `0`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  protected def erasedLongPair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  /**
   * Named erasure boundary. The decoded triple has to carry a null component so a matched null
   * value stays null. Building it from `java.lang.Long` keeps the null from becoming `0`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  protected def erasedLongTriple(
      key: java.lang.Long,
      left: java.lang.Long,
      right: java.lang.Long,
  ): (Long, (Long, Long)) =
    Tuple2(key, Tuple2(left, right)).asInstanceOf[(Long, (Long, Long))]

  protected def componentValidity(absent: Option[Array[Boolean]], width: Int): Validity =
    absent match
      case None => Validity.allValid(width)
      case Some(flags) => Validity.pack(width, row => !flags(row))

  protected def int32Batch(buffer: Array[Int]): ColumnBatch =
    batch(Int32Column.of(buffer, Validity.allValid(buffer.length), buffer.length))

  protected def int64Batch(buffer: Array[Long]): ColumnBatch =
    batch(Int64Column.of(buffer, Validity.allValid(buffer.length), buffer.length))

  protected def float64Batch(buffer: Array[Double]): ColumnBatch =
    batch(Float64Column.of(buffer, Validity.allValid(buffer.length), buffer.length))

  protected def batch(column: Column): ColumnBatch =
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  protected def pairBatch(left: Int32Column, right: Int32Column): ColumnBatch =
    new ColumnBatch(
      schema = Vector(LogicalType.Int32, LogicalType.Int32),
      columns = Vector(left, right),
      length = left.length,
    )

  protected def stringPairBatch(keys: DictUtf8Column, values: Int32Column): ColumnBatch =
    new ColumnBatch(
      schema = Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      columns = Vector(keys, values),
      length = keys.length,
    )

  protected def longPairBatch(left: Int64Column, right: Int64Column): ColumnBatch =
    new ColumnBatch(
      schema = Vector(LogicalType.Int64, LogicalType.Int64),
      columns = Vector(left, right),
      length = left.length,
    )

  protected def int32(batch: ColumnBatch): Int32Column =
    batch.columns match
      case Seq(column: Int32Column) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Int32 column, got ${batch.schema}")

  protected def int64(batch: ColumnBatch): Int64Column =
    batch.columns match
      case Seq(column: Int64Column) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Int64 column, got ${batch.schema}")

  protected def float64(batch: ColumnBatch): Float64Column =
    batch.columns match
      case Seq(column: Float64Column) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Float64 column, got ${batch.schema}")

  protected def bool(batch: ColumnBatch): BoolColumn =
    batch.columns match
      case Seq(column: BoolColumn) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Bool column, got ${batch.schema}")

  protected def utf8(batch: ColumnBatch): DictUtf8Column =
    batch.columns match
      case Seq(column: DictUtf8Column) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Utf8Dict column, got ${batch.schema}")

  protected def intPair(batch: ColumnBatch): (Int32Column, Int32Column) =
    batch.columns match
      case Seq(left: Int32Column, right: Int32Column) => (left, right)
      case _ =>
        throw new IllegalArgumentException(s"expected two Int32 columns, got ${batch.schema}")

  protected def stringPair(batch: ColumnBatch): (DictUtf8Column, Int32Column) =
    batch.columns match
      case Seq(keys: DictUtf8Column, values: Int32Column) => (keys, values)
      case _ =>
        throw new IllegalArgumentException(
          s"expected Utf8Dict and Int32 columns, got ${batch.schema}",
        )

  /** A clear bit is a null key. A set bit is the dictionary string, even when the code is `0`. */
  protected def stringKey(column: DictUtf8Column, row: Int): String =
    if (column.isValid(row)) then column.dictionary(column.codes(row))
    else BatchCodec.stringElement(None)

  protected def longPair(batch: ColumnBatch): (Int64Column, Int64Column) =
    batch.columns match
      case Seq(left: Int64Column, right: Int64Column) => (left, right)
      case _ =>
        throw new IllegalArgumentException(s"expected two Int64 columns, got ${batch.schema}")

  protected def longCount(batch: ColumnBatch): (Int64Column, Int32Column) =
    batch.columns match
      case Seq(keys: Int64Column, counts: Int32Column) => (keys, counts)
      case _ =>
        throw new IllegalArgumentException(
          s"expected Int64 and Int32 columns, got ${batch.schema}",
        )

  protected def decodedLength(batch: ColumnBatch, maxRows: Int): Int =
    require(maxRows >= 0, s"maximum decoded rows must be >= 0, got $maxRows")
    math.min(batch.length, maxRows)

  /** `count` is a live prefix of `rows`. Slots at `count` and beyond are not read. */
  protected def selectionCount(rows: Array[Int], count: Int): Int =
    require(count >= 0, s"selection count must be >= 0, got $count")
    require(count <= rows.length, s"selection count $count exceeds capacity ${rows.length}")
    count
