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

  /** Copies an `ArraySeq.ofInt`. Any other iterable fills. An owned array is [[copiedInts]]. */
  def ints(values: Iterable[Int]): ColumnBatch = values match
    case array: ArraySeq.ofInt => copiedInts(array.unsafeArray)
    case other => int32Batch(fillInts(other))

  def nullableInts(values: Seq[Option[Int]]): ColumnBatch =
    batch(Int32Column.fromNullable(values))

  /** Copies an `ArraySeq.ofLong`. Any other iterable fills. An owned array is [[copiedLongs]]. */
  def longs(values: Iterable[Long]): ColumnBatch = values match
    case array: ArraySeq.ofLong => copiedLongs(array.unsafeArray)
    case other => fromOwnedLongs(fillLongs(other))

  /** Copies `values`. The column does not retain the array. See [[copiedInts]]. */
  private[scarlet] def copiedLongs(values: Array[Long]): ColumnBatch =
    ColumnMetrics.primitiveEncode()
    fromOwnedLongs(Arrays.copyOf(values, values.length))

  /** Adopts `values`. The caller must not mutate or retain it. [[copiedLongs]] copies first. */
  private def fromOwnedLongs(values: Array[Long]): ColumnBatch =
    int64Batch(values)

  def nullableLongs(values: Seq[Option[Long]]): ColumnBatch =
    batch(Int64Column.fromNullable(values))

  /**
   * Copies `values`. The column does not retain the array. The partition stays readable, so a
   * column must not share it. This is the one encode copy.
   */
  private[scarlet] def copiedInts(values: Array[Int]): ColumnBatch =
    ColumnMetrics.primitiveEncode()
    int32Batch(Arrays.copyOf(values, values.length))

  /**
   * Copies an `ArraySeq.ofDouble`. Any other iterable fills. An owned array is [[copiedFloat64s]].
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
   * Copies an `ArraySeq.ofBoolean`. Any other sequence fills from its iterator. The `Seq`
   * signature stays. See [[copiedBools]]. The column stores bytes and does not retain `values`.
   */
  def bools(values: Seq[Boolean]): ColumnBatch = boolsFromIterable(values)

  /** Same encoding as the [[Seq]] overload, for a non-sequence iterable. */
  def bools(values: Iterable[Boolean]): ColumnBatch = boolsFromIterable(values)

  private def boolsFromIterable(values: Iterable[Boolean]): ColumnBatch = values match
    case array: ArraySeq.ofBoolean => copiedBools(array.unsafeArray)
    case other => boolBatch(fillBoolBytes(other))

  /** Copies `values`. The column does not retain the array. See [[copiedInts]]. */
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
   * UTF-8 key and int value. A null key is an empty slot. `""` is present, including code `0`. A
   * null int is a clear bit. `productElement` does not unbox it. A present column stays all-valid
   * and is not retained.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
      "org.wartremover.warts.Var",
    ),
  )
  def stringIntPairs(values: Iterable[(String, Int)]): ColumnBatch =
    val count = values.size
    val texts = new Array[String](count)
    val ints = new Array[Int](count)
    val rows = values.iterator
    var row = 0
    var absent = Option.empty[Array[Boolean]]
    while (row < count) {
      val pair = rows.next()
      texts(row) = pair._1
      absent = writeStringInt(pair, ints, absent, count, row)
      row += 1
    }
    stringPairBatch(
      DictUtf8Column.fromIterable(ArraySeq.unsafeWrapArray(texts)),
      Int32Column.of(ints, componentValidity(absent, count), count),
    )

  /**
   * The value column of a `(String, Int)` scan. A null int is a clear bit, not zero. A fully
   * present column stays all-valid. The column does not retain the input.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Var",
    ),
  )
  def stringIntValues(values: Iterable[(String, Int)]): ColumnBatch =
    val count = values.size
    val ints = new Array[Int](count)
    val rows = values.iterator
    var row = 0
    var absent = Option.empty[Array[Boolean]]
    while (row < count) {
      absent = writeStringInt(rows.next(), ints, absent, count, row)
      row += 1
    }
    batch(Int32Column.of(ints, componentValidity(absent, count), count))

  /**
   * Writes a present int. A null cell is absent. Named erasure boundary: `productElement` does not
   * unbox a `(String, Int)` cell, so a null stays absent. A non-null cell is a reference.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private def writeStringInt(
      pair: Product,
      into: Array[Int],
      absent: Option[Array[Boolean]],
      width: Int,
      row: Int,
  ): Option[Array[Boolean]] =
    pair.productElement(1) match
      case null =>
        val flags = absent.getOrElse(new Array[Boolean](width))
        flags(row) = true
        Some(flags)
      case value: Int =>
        into(row) = value
        absent
      case other =>
        val found = other.asInstanceOf[AnyRef].getClass.getName
        throw new IllegalArgumentException(s"string pair value expected an Int, found $found")

  /**
   * Copies `keys` and `values`. The column does not retain either array. Unequal lengths are
   * rejected. See [[copiedInts]] for why the arrays are copied rather than adopted.
   */
  def intPairs(keys: Array[Int], values: Array[Int]): ColumnBatch =
    require(keys.length == values.length, LengthMismatch)
    copiedIntPairs(keys, values)

  /** Copies both arrays. Unequal lengths use the [[intPairs]] sentence. See [[copiedInts]]. */
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
   * same sentence as [[intPairs]]. The column does not retain either array.
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
   * Boxed `(Int, Int)` pairs. A null component stays absent and a non-int cell is rejected. A
   * column with no nulls stays all-valid. [[intPairs]] is the dense all-valid constructor and
   * still unboxes. Owned arrays use [[copiedIntPairs]].
   */
  private[scarlet] def intPairsPreservingAbsence(values: Iterable[(Int, Int)]): ColumnBatch =
    IntPairEncode.pairs(values)

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

  /**
   * Reads one `Int64` column and keeps a clear bit as a null `Long`. A fully present column still
   * decodes to the stored longs. [[decodeLongs]] rejects a clear bit. `keys` and `values` of a
   * long pair are the callers that keep those nulls.
   *
   * Named erasure boundary. The cells are `java.lang.Long` references in an `Array[AnyRef]`. A
   * `scala.Long` cell would unbox a null to `0`.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Null",
    ),
  )
  private[scarlet] def decodeLongsPreservingAbsence(
      batch: ColumnBatch,
      maxRows: Int,
  ): Seq[Long] =
    val column = int64(batch)
    val rows = decodedLength(batch, maxRows)
    ColumnMetrics.decoded(rows)
    val boxed = Array.tabulate[AnyRef](rows)(row => longRef(column, row))
    ArraySeq.unsafeWrapArray(boxed).asInstanceOf[Seq[Long]]

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

  /**
   * Reads a string-int batch. A clear key bit is a null string. Code `0` with the bit set is the
   * dictionary string, often `""`. A clear value bit is a null int, not the stored zero. A prefix
   * checks only the rows it returns.
   */
  def decodeStringPairs(batch: ColumnBatch): Seq[(String, Int)] =
    decodeStringPairs(batch, batch.length)

  def decodeStringPairs(batch: ColumnBatch, maxRows: Int): Seq[(String, Int)] =
    val (keys, values) = stringPair(batch)
    val rows = decodedLength(batch, maxRows)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows)(row => erasedStringInt(stringKey(keys, row), intRef(values, row)))

  /**
   * Reads `rows(0 until count)` of a string-int batch. A clear key bit is a null string. A clear
   * value bit is a null int, the same rule as the dense decoder. A clear bit on a row the index
   * does not name is not read. `count` is the live prefix. Slots at `count` and beyond are not
   * decoded.
   */
  def decodeStringPairs(batch: ColumnBatch, rows: Array[Int], count: Int): Seq[(String, Int)] =
    val (keys, values) = stringPair(batch)
    val live = selectionCount(rows, count)
    ColumnMetrics.decoded(live)
    Seq.tabulate(live) { slot =>
      val row = rows(slot)
      erasedStringInt(stringKey(keys, row), intRef(values, row))
    }

  def decodeIntPairs(batch: ColumnBatch): Seq[(Int, Int)] = decodeIntPairs(batch, batch.length)

  /**
   * Reads an int-pair batch. A clear bit is an error: the decoded `Int` would otherwise be the
   * stored zero, or a null that only a sort is allowed to keep. A prefix checks only the rows it
   * returns.
   */
  def decodeIntPairs(batch: ColumnBatch, maxRows: Int): Seq[(Int, Int)] =
    val (left, right) = intPair(batch)
    val rows = decodedLength(batch, maxRows)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows) { row =>
      require(left.isValid(row) && right.isValid(row), s"null Int32 pair at row $row")
      (left.values(row), right.values(row))
    }

  /**
   * Reads `rows(0 until count)` of an int-pair batch. A clear bit on a selected row is an error,
   * the same rule as the dense decoder. A clear bit on any other row is not read. `count` is the
   * live prefix. Slots at `count` and beyond are not decoded.
   */
  def decodeIntPairs(batch: ColumnBatch, rows: Array[Int], count: Int): Seq[(Int, Int)] =
    val (left, right) = intPair(batch)
    val live = selectionCount(rows, count)
    ColumnMetrics.decoded(live)
    Seq.tabulate(live) { slot =>
      val row = rows(slot)
      require(left.isValid(row) && right.isValid(row), s"null Int32 pair at row $row")
      (left.values(row), right.values(row))
    }

  /**
   * Reads an int-pair batch and keeps a clear key or value bit as a null `Int`. A fully present
   * batch still decodes to the stored ints. This matches [[decodeLongPairs]]. [[decodeIntPairs]]
   * rejects a clear bit. A sort of int pairs is the caller that keeps those nulls.
   */
  private[scarlet] def decodeIntPairsPreservingAbsence(
      batch: ColumnBatch,
      maxRows: Int,
  ): Seq[(Int, Int)] =
    val (left, right) = intPair(batch)
    val rows = decodedLength(batch, maxRows)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows)(row => erasedIntPair(intRef(left, row), intRef(right, row)))

  /**
   * Boxed `(Long, Long)` pairs. A null component stays absent. `productElement` does not unbox, so
   * a null is not stored as `0`. This does not call [[ColumnMetrics.primitiveEncode]].
   */
  private[scarlet] def longPairs(values: Iterable[(Long, Long)]): ColumnBatch =
    fillLongPairs(values)

  /**
   * One `Int64` column from boxed long pairs. `ordinal` 0 reads the key and 1 the value. The other
   * component is not read. The argument is a `Product` so the read is `productElement`, not a
   * `Tuple2` cast. A null stays absent. This does not call [[ColumnMetrics.primitiveEncode]] and
   * does not use [[longs]], which would unbox a null.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private[scarlet] def longPairSide(values: Iterable[Product], ordinal: Int): ColumnBatch =
    val count = values.size
    val filled = new Array[Long](count)
    val rows = values.iterator
    var row = 0
    var absent = Option.empty[Array[Boolean]]
    while (row < count) {
      val pair = rows.next()
      absent = writeLongComponent(pair, ordinal, filled, absent, count, row)
      row += 1
    }
    batch(Int64Column.of(filled, componentValidity(absent, count), count))

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
   * Reads `rows(0 until count)` of a long-pair batch. A clear bit is a null `Long`, the same rule
   * as [[decodeLongPairs]]. [[decodeIntPairs]] rejects a clear bit and is not this decoder. A
   * clear bit on a row the index does not name is not read. `count` is the live prefix. Slots at
   * `count` and beyond are not decoded.
   */
  def decodeLongPairs(batch: ColumnBatch, rows: Array[Int], count: Int): Seq[(Long, Long)] =
    val (left, right) = longPair(batch)
    val live = selectionCount(rows, count)
    ColumnMetrics.decoded(live)
    Seq.tabulate(live) { slot =>
      val row = rows(slot)
      erasedLongPair(longRef(left, row), longRef(right, row))
    }

  /** `(Long, Int)` counts. Both columns are live. A null key was dropped before publish. */
  def decodeLongCounts(batch: ColumnBatch): Seq[(Long, Int)] =
    decodeLongCounts(batch, batch.length)

  def decodeLongCounts(batch: ColumnBatch, maxRows: Int): Seq[(Long, Int)] =
    val (keys, counts) = longCount(batch)
    require(
      keys.length == batch.length && counts.length == batch.length,
      "count columns disagree on length",
    )
    val rows = decodedLength(batch, maxRows)
    if (rows == batch.length) then
      requireLive(keys)
      requireLive(counts)
    else
      requireLivePrefix(keys, rows)
      requireLivePrefix(counts, rows)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows)(row => (keys.values(row), counts.values(row)))

  /**
   * A null join int is an error, not stored zero. A prefix checks only returned rows. Spare
   * capacity past `length` is ignored.
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
   * Reads `rows(0 until count)`. A clear bit on a selected row is an error, same as the dense
   * decoder. Any other clear bit is not read. Slots at `count` and beyond are not decoded.
   */
  def decodeIntTriples(
      batch: ColumnBatch,
      rows: Array[Int],
      count: Int,
  ): Seq[(Int, (Int, Int))] =
    batch.columns match
      case Seq(keys: Int32Column, build: Int32Column, probe: Int32Column) =>
        require(
          keys.length == batch.length && build.length == batch.length &&
            probe.length == batch.length,
          "join columns disagree on length",
        )
        val live = selectionCount(rows, count)
        ColumnMetrics.decoded(live)
        Seq.tabulate(live) { slot =>
          val row = rows(slot)
          if (!keys.isValid(row) || !build.isValid(row) || !probe.isValid(row)) then
            throw new IllegalArgumentException(s"null join value at row $row")
          (keys.values(row), (build.values(row), probe.values(row)))
        }
      case _ =>
        throw new IllegalArgumentException(
          s"join output is not three Int32 columns: ${batch.schema}",
        )

  /**
   * Keeps a clear bit as a null `Int`. [[decodeIntTriples]] rejects one. A triple map keeps those
   * nulls. A whole-row filter drops them before decode. A present cell stays the stored int.
   */
  private[scarlet] def decodeIntTriplesPreservingAbsence(
      batch: ColumnBatch,
  ): Seq[(Int, (Int, Int))] =
    decodeIntTriplesPreservingAbsence(batch, batch.length)

  private[scarlet] def decodeIntTriplesPreservingAbsence(
      batch: ColumnBatch,
      maxRows: Int,
  ): Seq[(Int, (Int, Int))] =
    batch.columns match
      case Seq(keys: Int32Column, left: Int32Column, right: Int32Column) =>
        require(
          keys.length == batch.length && left.length == batch.length && right.length == batch.length,
          "join columns disagree on length",
        )
        val rows = decodedLength(batch, maxRows)
        ColumnMetrics.decoded(rows)
        Seq.tabulate(rows) { row =>
          erasedIntTriple(intRef(keys, row), intRef(left, row), intRef(right, row))
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
   * Reads a long-triple batch and keeps a clear bit as a null `Long`. A fully present batch still
   * decodes to the stored longs. [[decodeLongTriples]] rejects a clear bit. A clear validity bit
   * is a null value, not an absence bit.
   */
  private[scarlet] def decodeLongTriplesPreservingAbsence(
      batch: ColumnBatch,
  ): Seq[(Long, (Long, Long))] =
    decodeLongTriplesPreservingAbsence(batch, batch.length)

  private[scarlet] def decodeLongTriplesPreservingAbsence(
      batch: ColumnBatch,
      maxRows: Int,
  ): Seq[(Long, (Long, Long))] =
    batch.columns match
      case Seq(keys: Int64Column, left: Int64Column, right: Int64Column) =>
        require(
          keys.length == batch.length && left.length == batch.length && right.length == batch.length,
          "join columns disagree on length",
        )
        val rows = decodedLength(batch, maxRows)
        ColumnMetrics.decoded(rows)
        Seq.tabulate(rows) { row =>
          erasedLongTriple(longRef(keys, row), longRef(left, row), longRef(right, row))
        }
      case _ =>
        throw new IllegalArgumentException(
          s"join output is not three Int64 columns: ${batch.schema}",
        )

  /**
   * String-key join rows are `(key, left, right)`. A clear bit is an error. A clear value would
   * decode as zero. [[decodeStringJoinKeepingNulls]] keeps that bit as a null int. A clear key bit
   * is still an error. A fully present batch decodes to the same strings and ints.
   */
  def decodeStringJoin(batch: ColumnBatch): Seq[(String, (Int, Int))] =
    decodeStringJoin(batch, batch.length)

  private[scarlet] def decodeStringJoinKeepingNulls(
      batch: ColumnBatch,
      maxRows: Int,
  ): Seq[(String, (Int, Int))] =
    decodeStringJoin(batch, maxRows, preserveValues = true)

  def decodeStringJoin(
      batch: ColumnBatch,
      maxRows: Int,
      preserveValues: Boolean = false,
  ): Seq[(String, (Int, Int))] =
    batch.columns match
      case Seq(keys: DictUtf8Column, left: Int32Column, right: Int32Column) =>
        require(
          keys.length == batch.length && left.length == batch.length && right.length == batch.length,
          "join columns disagree on length",
        )
        val rows = decodedLength(batch, maxRows)
        if (rows == batch.length) then requireLive(keys)
        else requireLivePrefix(keys, rows)
        if (!preserveValues) then
          if (rows == batch.length) then
            requireLive(left)
            requireLive(right)
          else
            requireLivePrefix(left, rows)
            requireLivePrefix(right, rows)
        ColumnMetrics.decoded(rows)
        Seq.tabulate(rows) { row =>
          erasedStringJoin(
            keys.dictionary(keys.codes(row)),
            intRef(left, row),
            intRef(right, row),
          )
        }
      case _ =>
        throw new IllegalArgumentException(
          s"join output is not Utf8Dict and two Int32 columns: ${batch.schema}",
        )

  /**
   * Outer-join rows are `(key, left, right)`. `absent` names the value column wrapped in `Option`.
   * [[AbsentSide.Left]] returns `(key, (Option[Int], Int))`. [[AbsentSide.Right]] returns
   * `(key, (Int, Option[Int]))`. [[AbsentSide.Both]] returns `(key, (Option[Int], Option[Int]))`.
   * An absence bit on an optional column is `None`. A clear bit on the key, or on a column that is
   * not optional, stays null. A clear bit on an optional column is `Some(null)`. [[optionalInt]]
   * is the only `Option` construction. `maxRows` limits the prefix the same way as
   * [[decodeIntTriples]].
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

  /**
   * Absence is `None` even at payload `0`. A set bit is `Some` of the stored int. A clear bit with
   * no absence mark is `Some(null)`. The `Some` payload stays `java.lang.Integer`. A `scala.Int`
   * payload unboxes that null to `0`. This is the only `Option` site.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def optionalInt(column: Int32Column, row: Int): Option[java.lang.Integer] =
    if (column.isAbsent(row)) then None
    else if (column.isValid(row)) then Some(java.lang.Integer.valueOf(column.values(row)))
    else Some(null: java.lang.Integer)

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
   * Fills both long columns in one pass. A null component is absent. Any other non-long cell
   * fails. The absence bitmap appears only after the first null, so a present column stays valid.
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
  private def writeLongComponent(
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
  private def intRef(column: Int32Column, row: Int): java.lang.Integer =
    if (column.isValid(row)) java.lang.Integer.valueOf(column.values(row)) else null

  /**
   * Named erasure boundary. The decoded tuple has to carry a null component so a sorted null key
   * matches the row path. Building it from `java.lang.Integer` keeps the null from becoming `0`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def erasedIntPair(key: java.lang.Integer, value: java.lang.Integer): (Int, Int) =
    Tuple2(key, value).asInstanceOf[(Int, Int)]

  /** Named erasure boundary. A null `Integer` stays null instead of unboxing to `0`. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def erasedStringInt(key: String, value: java.lang.Integer): (String, Int) =
    Tuple2(key, value).asInstanceOf[(String, Int)]

  /** Named erasure boundary. A null `Integer` stays null instead of unboxing to `0`. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def erasedStringJoin(
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
  private def erasedIntTriple(
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

  /**
   * Named erasure boundary. The decoded triple has to carry a null component so a matched null
   * value stays null. Building it from `java.lang.Long` keeps the null from becoming `0`.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def erasedLongTriple(
      key: java.lang.Long,
      left: java.lang.Long,
      right: java.lang.Long,
  ): (Long, (Long, Long)) =
    Tuple2(key, Tuple2(left, right)).asInstanceOf[(Long, (Long, Long))]

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

  private def stringPairBatch(keys: DictUtf8Column, values: Int32Column): ColumnBatch =
    new ColumnBatch(
      schema = Vector(LogicalType.Utf8Dict, LogicalType.Int32),
      columns = Vector(keys, values),
      length = keys.length,
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

  private def stringPair(batch: ColumnBatch): (DictUtf8Column, Int32Column) =
    batch.columns match
      case Seq(keys: DictUtf8Column, values: Int32Column) => (keys, values)
      case _ =>
        throw new IllegalArgumentException(
          s"expected Utf8Dict and Int32 columns, got ${batch.schema}",
        )

  /** A clear bit is a null key. A set bit is the dictionary string, even when the code is `0`. */
  private def stringKey(column: DictUtf8Column, row: Int): String =
    if (column.isValid(row)) then column.dictionary(column.codes(row))
    else stringElement(None)

  private def longPair(batch: ColumnBatch): (Int64Column, Int64Column) =
    batch.columns match
      case Seq(left: Int64Column, right: Int64Column) => (left, right)
      case _ =>
        throw new IllegalArgumentException(s"expected two Int64 columns, got ${batch.schema}")

  private def longCount(batch: ColumnBatch): (Int64Column, Int32Column) =
    batch.columns match
      case Seq(keys: Int64Column, counts: Int32Column) => (keys, counts)
      case _ =>
        throw new IllegalArgumentException(
          s"expected Int64 and Int32 columns, got ${batch.schema}",
        )

  private def decodedLength(batch: ColumnBatch, maxRows: Int): Int =
    require(maxRows >= 0, s"maximum decoded rows must be >= 0, got $maxRows")
    math.min(batch.length, maxRows)

  /** `count` is a live prefix of `rows`. Slots at `count` and beyond are not read. */
  private def selectionCount(rows: Array[Int], count: Int): Int =
    require(count >= 0, s"selection count must be >= 0, got $count")
    require(count <= rows.length, s"selection count $count exceeds capacity ${rows.length}")
    count
