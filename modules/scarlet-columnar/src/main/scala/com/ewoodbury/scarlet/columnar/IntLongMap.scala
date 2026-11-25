package com.ewoodbury.scarlet.columnar

/**
 * `mapValues` from an int value to a long value.
 *
 * The key column is copied. A null key stays null. A null value stays null and the function is not
 * called. Key bits and value bits are separate bitmaps: a null key is not a null value. The output
 * arrays are new. This does not alias the input. An empty range is an empty `(Int32, Int64)` batch
 * and does not call the function.
 *
 * `reduce` widens each morsel, then sums with [[HashAggregate.sumInt64]] and finishes through
 * [[TripleIntLongSum.exchange]]. The caller interprets the child under the map, so the function
 * runs once.
 */
object IntLongMap:

  /** `(Int32, Int64)` for `[start, start + length)`. */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  def widen(batch: ColumnBatch, start: Int, length: Int, mapper: Int => Long): ColumnBatch =
    val keys = ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)
    val values = ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 1), 1)
    require(keys.length == values.length, "key and value columns differ in length")
    Ranges.check(start, length, keys.length)
    if (length == 0) then ColumnBatches.emptyIntLongSum
    else
      val keyOut = new Array[Int](length)
      val valueOut = new Array[Long](length)
      val keyWords = Validity.allocate(length)
      val valueWords = Validity.allocate(length)
      val keyIn = keys.validity.words
      val valueIn = values.validity.words
      val end = start + length
      var row = start
      var written = 0
      while (row < end) {
        if (Validity.isSet(keyIn, row)) then
          keyOut(written) = keys.values(row)
          Validity.setBit(keyWords, written)
        else keyOut(written) = 0
        if (Validity.isSet(valueIn, row)) then
          valueOut(written) = mapper(values.values(row))
          Validity.setBit(valueWords, written)
        else valueOut(written) = 0L
        written += 1
        row += 1
      }
      new ColumnBatch(
        Vector(LogicalType.Int32, LogicalType.Int64),
        Vector(
          Int32Column.of(keyOut, Validity.fromWords(keyWords, length), length),
          Int64Column.of(valueOut, Validity.fromWords(valueWords, length), length),
        ),
        length,
      )

  /**
   * Widen `batches`, sum each morsel, and exchange the partials.
   *
   * An empty input is `numPartitions` empty sum batches. A null value does not update its group.
   * [[HashAggregate.sumInt64]] owns that rule.
   */
  def reduce(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      mapper: Int => Long,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntLongSum)
    else
      val groups = batches.map(batch => Morsels.of(batch, batchSize))
      val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
        val widened = widen(morsel.batch, morsel.start, morsel.length, mapper)
        HashAggregate.sumInt64(widened, keyOrdinal = 0, valueOrdinal = 1)
      }
      val combined = merge(groups.map(_.length), partials, parallelism)
      TripleIntLongSum.exchange(combined, numPartitions, parallelism)

  /**
   * `(Int, Long)` rows. A clear bit is a null reference, not zero. This is not
   * [[BatchCodec.decodeIntPairs]]: that decoder returns `(Int, Int)` and cannot keep a long.
   */
  def decode(batch: ColumnBatch, maxRows: Int): Seq[(Int, Long)] =
    val rows = decodedLength(batch, maxRows)
    val keys = ColumnKernel.asInt32(ColumnKernel.columnAt(batch, 0), 0)
    val values = ColumnKernel.asInt64(ColumnKernel.columnAt(batch, 1), 1)
    ColumnMetrics.decoded(rows)
    Seq.tabulate(rows)(row => erased(intRef(keys, row), longRef(values, row)))

  /** One [[HashAggregate.mergeSumPartials]] per input batch. */
  private def merge(
      sizes: Vector[Int],
      partials: Vector[ColumnBatch],
      parallelism: Int,
  ): Vector[ColumnBatch] =
    val grouped = PartialCombine.groupPartials(sizes, partials)
    MorselScheduler.map(grouped, parallelism) { (_, group) =>
      HashAggregate.mergeSumPartials(group, keyOrdinal = 0, valueOrdinal = 1)
    }

  private def decodedLength(batch: ColumnBatch, maxRows: Int): Int =
    require(maxRows >= 0, s"maximum decoded rows must be >= 0, got $maxRows")
    math.min(batch.length, maxRows)

  /** A present cell is the stored int. A clear bit is a null reference, not `0`. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def intRef(column: Int32Column, row: Int): java.lang.Integer =
    if (column.isValid(row)) java.lang.Integer.valueOf(column.values(row)) else null

  /** A present cell is the stored long. A clear bit is a null reference, not `0`. */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def longRef(column: Int64Column, row: Int): java.lang.Long =
    if (column.isValid(row)) java.lang.Long.valueOf(column.values(row)) else null

  /**
   * Named erasure boundary. The decoded pair has to carry a null component. Building it from
   * `java.lang.Integer` and `java.lang.Long` keeps a null from becoming zero.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Null"))
  private def erased(key: java.lang.Integer, value: java.lang.Long): (Int, Long) =
    Tuple2(key, value).asInstanceOf[(Int, Long)]
