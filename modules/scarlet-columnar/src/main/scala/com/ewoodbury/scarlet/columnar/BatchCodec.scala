package com.ewoodbury.scarlet.columnar

import java.util.Arrays

import scala.collection.immutable.ArraySeq

/** Encode Scala sequences into batches, and read those batches back. */
object BatchCodec:

  /**
   * Same sentence as `IntPairArrays.LengthMismatch`. Columnar does not depend on core, so this
   * literal cannot reference that constant. A test locks the two copies together.
   */
  private val LengthMismatch: String = "key and value arrays differ in length"

  /**
   * An `ArraySeq.ofInt` is copied from its primitive array. Any other iterable is filled from its
   * iterator. An owned partition array is [[copiedInts]]: this module does not see that holder.
   */
  def ints(values: Iterable[Int]): ColumnBatch = values match
    case array: ArraySeq.ofInt => copiedInts(array.unsafeArray)
    case other => int32Batch(fillInts(other))

  def nullableInts(values: Seq[Option[Int]]): ColumnBatch =
    batch(Int32Column.fromNullable(values))

  /**
   * An `ArraySeq.ofLong` is copied from its primitive array. Any other iterable, including a
   * `Seq[Long]`, is filled from its iterator. An owned partition array is [[copiedLongs]]: this
   * module does not see that holder.
   */
  def longs(values: Iterable[Long]): ColumnBatch = values match
    case array: ArraySeq.ofLong => copiedLongs(array.unsafeArray)
    case other => fromOwnedLongs(fillLongs(other))

  /**
   * Copies `values` into a long column. The column does not retain `values`. See [[copiedInts]]
   * for why the column does not adopt the array. The partition that produced `values` stays
   * readable.
   */
  private[scarlet] def copiedLongs(values: Array[Long]): ColumnBatch =
    ColumnMetrics.primitiveEncode()
    fromOwnedLongs(Arrays.copyOf(values, values.length))

  /**
   * Adopts `values`. The caller must not mutate that array and must not retain it. [[copiedLongs]]
   * is the path that copies first. Private, like [[int32Batch]], so a later caller cannot alias a
   * published column.
   */
  private def fromOwnedLongs(values: Array[Long]): ColumnBatch =
    int64Batch(values)

  def nullableLongs(values: Seq[Option[Long]]): ColumnBatch =
    batch(Int64Column.fromNullable(values))

  /**
   * Copies `values` into an int column. The column does not retain `values`.
   *
   * The partition that produced `values` stays readable for the row path and for a later action,
   * so it is not single-use. A column buffer is writable. Sharing the array would let a column
   * write change that partition. Construction already copied the caller's range, so this is the
   * one encode copy.
   */
  private[scarlet] def copiedInts(values: Array[Int]): ColumnBatch =
    ColumnMetrics.primitiveEncode()
    int32Batch(Arrays.copyOf(values, values.length))

  /**
   * An `ArraySeq.ofDouble` is copied from its primitive array. Any other iterable is filled from
   * its iterator. An owned partition array is [[copiedFloat64s]]: this module does not see that
   * holder.
   */
  def float64s(values: Iterable[Double]): ColumnBatch = values match
    case array: ArraySeq.ofDouble => copiedFloat64s(array.unsafeArray)
    case other => float64Batch(fillDoubles(other))

  /** Copies `values`. See [[copiedInts]] for why the column does not adopt the array. */
  private[scarlet] def copiedFloat64s(values: Array[Double]): ColumnBatch =
    ColumnMetrics.primitiveEncode()
    float64Batch(Arrays.copyOf(values, values.length))

  def nullableFloat64s(values: Seq[Option[Double]]): ColumnBatch =
    batch(Float64Column.fromNullable(values))

  /**
   * An `ArraySeq.ofBoolean` is copied from its primitive array. Any other sequence is filled from
   * its iterator. The `Seq` signature stays for callers compiled against it. [[boolsFromIterable]]
   * is the same encoding for a non-sequence iterable. An owned partition array is [[copiedBools]]:
   * this module does not see that holder. The column stores bytes and does not retain `values`.
   */
  def bools(values: Seq[Boolean]): ColumnBatch = boolsFromIterable(values)

  /** Same encoding as the [[Seq]] overload, for a non-sequence iterable. */
  def bools(values: Iterable[Boolean]): ColumnBatch = boolsFromIterable(values)

  private def boolsFromIterable(values: Iterable[Boolean]): ColumnBatch = values match
    case array: ArraySeq.ofBoolean => copiedBools(array.unsafeArray)
    case other => boolBatch(fillBoolBytes(other))

  /**
   * Copies `values` into a boolean column. The column does not retain `values`. See [[copiedInts]]
   * for why the column does not adopt the array. The partition that produced `values` stays
   * readable.
   */
  private[scarlet] def copiedBools(values: Array[Boolean]): ColumnBatch =
    ColumnMetrics.primitiveEncode()
    boolBatch(boolBytes(values))

  private def boolBatch(bytes: Array[Byte]): ColumnBatch =
    batch(BoolColumn.of(bytes, Validity.allValid(bytes.length), bytes.length))

  /** Copies `values` into a new byte buffer. The boolean array is not retained. */
  private def boolBytes(values: Array[Boolean]): Array[Byte] =
    Array.tabulate(values.length) { row =>
      if (values(row)) 1.toByte else 0.toByte
    }

  def nullableBools(values: Seq[Option[Boolean]]): ColumnBatch =
    batch(BoolColumn.fromNullable(values))

  def strings(values: Iterable[String]): ColumnBatch =
    batch(DictUtf8Column.fromIterable(values))

  def nullableStrings(values: Seq[Option[String]]): ColumnBatch =
    batch(DictUtf8Column.fromNullable(values))

  /**
   * Copies `keys` and `values`. The column does not retain either array. Unequal lengths are
   * rejected. See [[copiedInts]] for why the arrays are copied rather than adopted.
   */
  def intPairs(keys: Array[Int], values: Array[Int]): ColumnBatch =
    require(keys.length == values.length, LengthMismatch)
    copiedIntPairs(keys, values)

  /**
   * Copies `keys` and `values`. Unequal lengths are rejected with the same sentence as
   * [[intPairs]]. See [[copiedInts]] for why the arrays are copied rather than adopted.
   */
  private[scarlet] def copiedIntPairs(keys: Array[Int], values: Array[Int]): ColumnBatch =
    require(keys.length == values.length, LengthMismatch)
    val rows = keys.length
    ColumnMetrics.primitiveEncode()
    pairBatch(
      Int32Column.of(Arrays.copyOf(keys, rows), Validity.allValid(rows), rows),
      Int32Column.of(Arrays.copyOf(values, rows), Validity.allValid(rows), rows),
    )

  /**
   * Copies `keys` and `values` into two `Int64` columns. Unequal lengths are rejected with the
   * same sentence as [[intPairs]]. The column does not retain either array. See [[copiedIntPairs]]
   * for why the arrays are copied rather than adopted.
   */
  private[scarlet] def copiedLongPairs(keys: Array[Long], values: Array[Long]): ColumnBatch =
    require(keys.length == values.length, LengthMismatch)
    val rows = keys.length
    ColumnMetrics.primitiveEncode()
    longPairBatch(
      Int64Column.of(Arrays.copyOf(keys, rows), Validity.allValid(rows), rows),
      Int64Column.of(Arrays.copyOf(values, rows), Validity.allValid(rows), rows),
    )

  /** One fill of each column. `size` then the iterator, so a `Seq` is not copied twice. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def intPairs(values: Iterable[(Int, Int)]): ColumnBatch =
    val n = values.size
    val leftValues = new Array[Int](n)
    val rightValues = new Array[Int](n)
    val iterator = values.iterator
    var row = 0
    while (row < n) {
      val pair = iterator.next()
      leftValues(row) = pair._1
      rightValues(row) = pair._2
      row += 1
    }
    pairBatch(
      Int32Column.of(leftValues, Validity.allValid(n), n),
      Int32Column.of(rightValues, Validity.allValid(n), n),
    )

  /**
   * Boxed `(Int, Int)` pairs for grouped sum. [[fillPairs]] records a null component as absent and
   * rejects any other non-int cell. A column with no nulls stays all-valid. Other actions use
   * [[intPairs]], which unboxes.
   */
  private[scarlet] def intPairsPreservingAbsence(values: Iterable[(Int, Int)]): ColumnBatch =
    fillPairs(values)

  def nullableIntPairs(values: Seq[(Option[Int], Option[Int])]): ColumnBatch =
    val n = values.size
    val leftValues = new Array[Int](n)
    val rightValues = new Array[Int](n)
    val leftPresent = new Array[Boolean](n)
    val rightPresent = new Array[Boolean](n)
    values.iterator.zipWithIndex.foreach { (pair, row) =>
      pair._1.foreach { value =>
        leftValues(row) = value
        leftPresent(row) = true
      }
      pair._2.foreach { value =>
        rightValues(row) = value
        rightPresent(row) = true
      }
    }
    pairBatch(
      Int32Column.of(leftValues, Validity.pack(n, row => leftPresent(row)), n),
      Int32Column.of(rightValues, Validity.pack(n, row => rightPresent(row)), n),
    )

  def decodeInts(batch: ColumnBatch): Seq[Int] = decodeInts(batch, batch.length)

  def decodeInts(batch: ColumnBatch, maxRows: Int): Seq[Int] =
    val column = int32(batch)
    val rows = decodedLength(batch, maxRows)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows) { row =>
      require(column.isValid(row), s"null Int32 at row $row")
      column.values(row)
    }

  def decodeNullableInts(batch: ColumnBatch): Seq[Option[Int]] =
    val column = int32(batch)
    Seq.tabulate(column.length) { row =>
      if (column.isValid(row)) Some(column.values(row)) else None
    }

  def decodeLongs(batch: ColumnBatch): Seq[Long] = decodeLongs(batch, batch.length)

  def decodeLongs(batch: ColumnBatch, maxRows: Int): Seq[Long] =
    val column = int64(batch)
    val rows = decodedLength(batch, maxRows)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows) { row =>
      require(column.isValid(row), s"null Int64 at row $row")
      column.values(row)
    }

  def decodeNullableLongs(batch: ColumnBatch): Seq[Option[Long]] =
    val column = int64(batch)
    Seq.tabulate(column.length) { row =>
      if (column.isValid(row)) Some(column.values(row)) else None
    }

  def decodeFloat64s(batch: ColumnBatch): Seq[Double] = decodeFloat64s(batch, batch.length)

  def decodeFloat64s(batch: ColumnBatch, maxRows: Int): Seq[Double] =
    val column = float64(batch)
    val rows = decodedLength(batch, maxRows)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows) { row =>
      require(column.isValid(row), s"null Float64 at row $row")
      column.values(row)
    }

  def decodeNullableFloat64s(batch: ColumnBatch): Seq[Option[Double]] =
    val column = float64(batch)
    Seq.tabulate(column.length) { row =>
      if (column.isValid(row)) Some(column.values(row)) else None
    }

  def decodeBools(batch: ColumnBatch): Seq[Boolean] = decodeBools(batch, batch.length)

  def decodeBools(batch: ColumnBatch, maxRows: Int): Seq[Boolean] =
    val column = bool(batch)
    val rows = decodedLength(batch, maxRows)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows) { row =>
      require(column.isValid(row), s"null Bool at row $row")
      column.values(row) != 0
    }

  /** An empty cell as a `DistCollection[String]` element. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def stringElement(cell: Option[String]): String =
    cell match
      case Some(text) => text
      case None => null

  /** A `DistCollection[String]` element that is an empty cell. */
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Equals"))
  def isAbsent(text: String): Boolean = text eq null

  def decodeStrings(batch: ColumnBatch): Seq[String] = decodeStrings(batch, batch.length)

  def decodeStrings(batch: ColumnBatch, maxRows: Int): Seq[String] =
    val column = utf8(batch)
    val rows = decodedLength(batch, maxRows)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows) { row =>
      if (column.isValid(row)) then column.dictionary(column.codes(row))
      else stringElement(None)
    }

  def decodeNullableStrings(batch: ColumnBatch): Seq[Option[String]] =
    val column = utf8(batch)
    Seq.tabulate(column.length) { row =>
      if (column.isValid(row)) Some(column.dictionary(column.codes(row))) else None
    }

  def decodeNullableBools(batch: ColumnBatch): Seq[Option[Boolean]] =
    val column = bool(batch)
    Seq.tabulate(column.length) { row =>
      if (column.isValid(row)) Some(column.values(row) != 0) else None
    }

  def decodeIntPairs(batch: ColumnBatch): Seq[(Int, Int)] = decodeIntPairs(batch, batch.length)

  def decodeIntPairs(batch: ColumnBatch, maxRows: Int): Seq[(Int, Int)] =
    val (left, right) = intPair(batch)
    val rows = decodedLength(batch, maxRows)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows) { row =>
      require(left.isValid(row) && right.isValid(row), s"null Int32 pair at row $row")
      (left.values(row), right.values(row))
    }

  /**
   * Boxed `(Long, Long)` pairs. A null component stays absent. `productElement` does not unbox, so
   * a null is not stored as `0`. This does not call [[ColumnMetrics.primitiveEncode]].
   */
  private[scarlet] def longPairs(values: Iterable[(Long, Long)]): ColumnBatch =
    fillLongPairs(values)

  def decodeLongPairs(batch: ColumnBatch): Seq[(Long, Long)] =
    decodeLongPairs(batch, batch.length)

  /**
   * Reads a long-pair batch. A clear bit is a null `Long`, including a null value left by
   * `reduceByKey`. This does not call [[decodeIntPairs]]. A clear key bit is also a null, so a
   * source that classified from a later present pair can round-trip.
   */
  def decodeLongPairs(batch: ColumnBatch, maxRows: Int): Seq[(Long, Long)] =
    val (left, right) = longPair(batch)
    val rows = decodedLength(batch, maxRows)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows)(row => erasedLongPair(longRef(left, row), longRef(right, row)))

  /**
   * Join rows are `(key, build value, probe value)`. A null stays an error: the decoded `Int`
   * would otherwise be the stored zero. A full decode checks live bits by word; a prefix decode
   * checks only the rows it returns. Spare capacity past `length` is ignored.
   */
  def decodeIntTriples(batch: ColumnBatch): Seq[(Int, (Int, Int))] =
    decodeIntTriples(batch, batch.length)

  def decodeIntTriples(batch: ColumnBatch, maxRows: Int): Seq[(Int, (Int, Int))] =
    batch.columns match
      case Seq(keys: Int32Column, build: Int32Column, probe: Int32Column) =>
        require(
          keys.length == batch.length && build.length == batch.length && probe.length == batch.length,
          "join columns disagree on length",
        )
        val rows = decodedLength(batch, maxRows)
        if (rows == batch.length) then
          requireLive(keys)
          requireLive(build)
          requireLive(probe)
        else
          requireLivePrefix(keys, rows)
          requireLivePrefix(build, rows)
          requireLivePrefix(probe, rows)
        ColumnMetrics.decoded(rows)
        Seq.tabulate(rows) { row =>
          (keys.values(row), (build.values(row), probe.values(row)))
        }
      case _ =>
        throw new IllegalArgumentException(
          s"join output is not three Int32 columns: ${batch.schema}",
        )

  /**
   * Long-key join rows are `(key, left, right)`. A null stays an error: the decoded `Long` would
   * otherwise be the stored zero. A full decode checks live bits by word; a prefix decode checks
   * only the rows it returns. Spare capacity past `length` is ignored. This does not call
   * [[decodeIntTriples]].
   */
  def decodeLongTriples(batch: ColumnBatch): Seq[(Long, (Long, Long))] =
    decodeLongTriples(batch, batch.length)

  def decodeLongTriples(batch: ColumnBatch, maxRows: Int): Seq[(Long, (Long, Long))] =
    batch.columns match
      case Seq(keys: Int64Column, left: Int64Column, right: Int64Column) =>
        require(
          keys.length == batch.length && left.length == batch.length && right.length == batch.length,
          "join columns disagree on length",
        )
        val rows = decodedLength(batch, maxRows)
        if (rows == batch.length) then
          requireLive(keys)
          requireLive(left)
          requireLive(right)
        else
          requireLivePrefix(keys, rows)
          requireLivePrefix(left, rows)
          requireLivePrefix(right, rows)
        ColumnMetrics.decoded(rows)
        Seq.tabulate(rows) { row =>
          (keys.values(row), (left.values(row), right.values(row)))
        }
      case _ =>
        throw new IllegalArgumentException(
          s"join output is not three Int64 columns: ${batch.schema}",
        )

  /**
   * Outer-join rows are `(key, left, right)`. `absent` names the value column wrapped in `Option`.
   * [[AbsentSide.Left]] returns `(key, (Option[Int], Int))`. [[AbsentSide.Right]] returns
   * `(key, (Int, Option[Int]))`. [[AbsentSide.Both]] returns `(key, (Option[Int], Option[Int]))`.
   * A clear bit on an optional column is `None`. A clear bit on the key, or on a column that is
   * not optional, stays null. A null value is not absence. [[optionalInt]] is the only `Option`
   * construction. `maxRows` limits the prefix the same way as [[decodeIntTriples]].
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decodeOuter(batch: ColumnBatch, absent: AbsentSide): Seq[Any] =
    decodeOuter(batch, absent, batch.length)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def decodeOuter(batch: ColumnBatch, absent: AbsentSide, maxRows: Int): Seq[Any] =
    batch.columns match
      case Seq(keys: Int32Column, left: Int32Column, right: Int32Column) =>
        require(
          keys.length == batch.length && left.length == batch.length && right.length == batch.length,
          "outer join columns disagree on length",
        )
        val rows = decodedLength(batch, maxRows)
        ColumnMetrics.decoded(rows)
        absent match
          case AbsentSide.Both =>
            Seq.tabulate(rows) { row =>
              (presentInt(keys, row), (optionalInt(left, row), optionalInt(right, row)))
            }
          case AbsentSide.Left =>
            Seq.tabulate(rows) { row =>
              (presentInt(keys, row), (optionalInt(left, row), presentInt(right, row)))
            }
          case AbsentSide.Right =>
            Seq.tabulate(rows) { row =>
              (presentInt(keys, row), (presentInt(left, row), optionalInt(right, row)))
            }
      case _ =>
        throw new IllegalArgumentException(
          s"outer join output is not three Int32 columns: ${batch.schema}",
        )

  /** A clear bit is absence. A set bit is the stored `Int`. This is the only `Option` site. */
  private def optionalInt(column: Int32Column, row: Int): Option[Int] =
    if (column.isValid(row)) Some(column.values(row)) else None

  /** A clear bit stays null. A set bit is the stored `Int`. */
  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
  private def presentInt(column: Int32Column, row: Int): Any =
    if (column.isValid(row)) then column.values(row) else null

  def decodeNullableIntPairs(batch: ColumnBatch): Seq[(Option[Int], Option[Int])] =
    val (left, right) = intPair(batch)
    Seq.tabulate(batch.length) { row =>
      val leftValue = if (left.isValid(row)) Some(left.values(row)) else None
      val rightValue = if (right.isValid(row)) Some(right.values(row)) else None
      (leftValue, rightValue)
    }

  /** Throws once, naming the first clear live bit. Bits at `length` and beyond are not live. */
  private def requireLive(column: Column): Unit =
    requireLiveRows(column.validity.words, column.length)

  /**
   * Same word check as [[requireLive]], limited to the decoded prefix. The tail word is masked to
   * `rows`, so the scan does not walk one bit at a time.
   */
  private def requireLivePrefix(column: Column, rows: Int): Unit =
    requireLiveRows(column.validity.words, rows)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def requireLiveRows(words: Array[Long], live: Int): Unit =
    val fullWords = live >>> 6
    var index = 0
    while (index < fullWords) {
      if (words(index) != -1L) then failNull(words, index << 6, live)
      index += 1
    }
    val tail = live & 63
    if (tail != 0 && (words(fullWords) & ((1L << tail) - 1L)) != (1L << tail) - 1L) then
      failNull(words, fullWords << 6, live)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def failNull(words: Array[Long], from: Int, live: Int): Nothing =
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
  private def fillInts(values: Iterable[Int]): Array[Int] =
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
  private def fillLongs(values: Iterable[Long]): Array[Long] =
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
  private def fillBoolBytes(values: Iterable[Boolean]): Array[Byte] =
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
  private def fillDoubles(values: Iterable[Double]): Array[Double] =
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
   * Fills both columns in one pass for [[intPairsPreservingAbsence]]. A null component is absent.
   * Any other non-int cell fails. The absence bitmap is allocated only after the first null, so a
   * fully present pair column stays all-valid.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def fillPairs(values: Iterable[(Int, Int)]): ColumnBatch =
    val count = values.size
    val leftValues = new Array[Int](count)
    val rightValues = new Array[Int](count)
    val rows = values.iterator
    var row = 0
    var leftAbsent = Option.empty[Array[Boolean]]
    var rightAbsent = Option.empty[Array[Boolean]]
    while (row < count) {
      val pair = rows.next()
      leftAbsent = writeComponent(pair, 0, "key", leftValues, leftAbsent, count, row)
      rightAbsent = writeComponent(pair, 1, "value", rightValues, rightAbsent, count, row)
      row += 1
    }
    pairBatch(
      Int32Column.of(leftValues, componentValidity(leftAbsent, count), count),
      Int32Column.of(rightValues, componentValidity(rightAbsent, count), count),
    )

  /**
   * Writes a present `Int`. A null cell marks the row absent. Any other non-int cell fails.
   *
   * Named erasure boundary. The cell is the erased field of an `(Int, Int)`. `productElement` does
   * not unbox, so a null stays absent. A non-null cell is a reference, so the class-name cast is
   * safe.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def writeComponent(
      pair: (Int, Int),
      ordinal: Int,
      role: String,
      into: Array[Int],
      absent: Option[Array[Boolean]],
      width: Int,
      row: Int,
  ): Option[Array[Boolean]] =
    pair.productElement(ordinal) match
      case null =>
        val flags = absent.getOrElse(new Array[Boolean](width))
        flags(row) = true
        Some(flags)
      case value: Int =>
        into(row) = value
        absent
      case other =>
        val found = other.asInstanceOf[AnyRef].getClass.getName
        throw new IllegalArgumentException(s"grouped sum expected an Int $role, found $found")

  /**
   * Fills both long columns in one pass. A null component is absent. Any other non-long cell
   * fails. The absence bitmap is allocated only after the first null, so a fully present pair
   * column stays all-valid.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private def fillLongPairs(values: Iterable[(Long, Long)]): ColumnBatch =
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
   * Writes a present `Long`. A null cell marks the row absent. Any other non-long cell fails.
   *
   * Named erasure boundary. The cell is the erased field of a `(Long, Long)`. `productElement`
   * does not unbox, so a null stays absent. A non-null cell is a reference, so the class test is
   * safe.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def writeLongComponent(
      pair: (Long, Long),
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
        throw new ClassCastException(s"columnar long pair expected a Long, found $found")

  /**
   * A present cell is the stored long. A clear bit is a null reference, not `0`.
   *
   * The return type stays `java.lang.Long`. A `scala.Long` return unboxes that null to `0`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def longRef(column: Int64Column, row: Int): java.lang.Long =
    if (column.isValid(row)) java.lang.Long.valueOf(column.values(row)) else null

  /**
   * Named erasure boundary. The decoded tuple has to carry a null component so a reduced null
   * value matches the row path. Building it from `java.lang.Long` keeps the null from becoming
   * `0`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def erasedLongPair(key: java.lang.Long, value: java.lang.Long): (Long, Long) =
    Tuple2(key, value).asInstanceOf[(Long, Long)]

  private def componentValidity(absent: Option[Array[Boolean]], width: Int): Validity =
    absent match
      case None => Validity.allValid(width)
      case Some(flags) => Validity.pack(width, row => !flags(row))

  private def int32Batch(buffer: Array[Int]): ColumnBatch =
    batch(Int32Column.of(buffer, Validity.allValid(buffer.length), buffer.length))

  private def int64Batch(buffer: Array[Long]): ColumnBatch =
    batch(Int64Column.of(buffer, Validity.allValid(buffer.length), buffer.length))

  private def float64Batch(buffer: Array[Double]): ColumnBatch =
    batch(Float64Column.of(buffer, Validity.allValid(buffer.length), buffer.length))

  private def batch(column: Column): ColumnBatch =
    new ColumnBatch(Vector(column.logicalType), Vector(column), column.length)

  private def pairBatch(left: Int32Column, right: Int32Column): ColumnBatch =
    new ColumnBatch(
      schema = Vector(LogicalType.Int32, LogicalType.Int32),
      columns = Vector(left, right),
      length = left.length,
    )

  private def longPairBatch(left: Int64Column, right: Int64Column): ColumnBatch =
    new ColumnBatch(
      schema = Vector(LogicalType.Int64, LogicalType.Int64),
      columns = Vector(left, right),
      length = left.length,
    )

  private def int32(batch: ColumnBatch): Int32Column =
    batch.columns match
      case Seq(column: Int32Column) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Int32 column, got ${batch.schema}")

  private def int64(batch: ColumnBatch): Int64Column =
    batch.columns match
      case Seq(column: Int64Column) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Int64 column, got ${batch.schema}")

  private def float64(batch: ColumnBatch): Float64Column =
    batch.columns match
      case Seq(column: Float64Column) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Float64 column, got ${batch.schema}")

  private def bool(batch: ColumnBatch): BoolColumn =
    batch.columns match
      case Seq(column: BoolColumn) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Bool column, got ${batch.schema}")

  private def utf8(batch: ColumnBatch): DictUtf8Column =
    batch.columns match
      case Seq(column: DictUtf8Column) => column
      case _ =>
        throw new IllegalArgumentException(s"expected one Utf8Dict column, got ${batch.schema}")

  private def intPair(batch: ColumnBatch): (Int32Column, Int32Column) =
    batch.columns match
      case Seq(left: Int32Column, right: Int32Column) => (left, right)
      case _ =>
        throw new IllegalArgumentException(s"expected two Int32 columns, got ${batch.schema}")

  private def longPair(batch: ColumnBatch): (Int64Column, Int64Column) =
    batch.columns match
      case Seq(left: Int64Column, right: Int64Column) => (left, right)
      case _ =>
        throw new IllegalArgumentException(s"expected two Int64 columns, got ${batch.schema}")

  private def decodedLength(batch: ColumnBatch, maxRows: Int): Int =
    require(maxRows >= 0, s"maximum decoded rows must be >= 0, got $maxRows")
    math.min(batch.length, maxRows)
