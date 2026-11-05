package com.ewoodbury.scarlet.columnar

/**
 * String-pair scatter. Boundaries are [[RankBounds]], the same rule [[RankScatter]] uses. The key
 * column is `Utf8Dict` and the value column is `Int32`.
 *
 * [[StringRankSplit]] and [[StringRankSplitRuns]] both call [[scatter]]. Each output partition has
 * its own dictionary. A null key is not interned. A missing value is not copied.
 *
 * `ready` is polled before each row. `write` appends that row to the sink it is given. A null
 * payload is the caller's concern: the sink stores a key or a value only when the present flag is
 * set.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object StringRankScatter:

  val pairSchema: Vector[LogicalType] =
    Vector(LogicalType.Utf8Dict, LogicalType.Int32)

  def pairs(
      dictionary: Array[String],
      codes: Array[Int],
      keyWords: Array[Long],
      values: Array[Int],
      valueWords: Array[Long],
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      pairSchema,
      Vector(
        DictUtf8Column.of(dictionary, codes, Validity.fromWords(keyWords, rows), rows),
        Int32Column.of(values, Validity.fromWords(valueWords, rows), rows),
      ),
      rows,
    )

  /**
   * Scatters `rows` ordered rows into `partitions` batches. Every output batch is allocated before
   * the first row. An empty partition is an empty string-pair batch.
   */
  def scatter(
      rows: Int,
      partitions: Int,
      ready: () => Boolean,
  )(write: Sink => Unit): Vector[ColumnBatch] =
    val sizes = RankBounds.outputSizes(rows, partitions)
    val sinks = Array.tabulate(partitions)(part => new Sink(sizes(part)))
    RankBounds.place(rows, partitions, ready)(part => write(sinks(part)))
    Vector.tabulate(partitions) { part =>
      if (sizes(part) == 0) then ColumnBatches.emptyStringPairs
      else sinks(part).toBatch
    }

  /** One output partition. `write` is one row, and it does not read a missing payload. */
  final class Sink(val size: Int):

    private val codes = new Array[Int](size)
    private val values = new Array[Int](size)
    private val keyWords = Validity.allocate(size)
    private val valueWords = Validity.allocate(size)
    private val words = DictUtf8Column.Words.empty
    private var filled: Int = 0

    def write(keyPresent: Boolean, key: String, valuePresent: Boolean, value: Int): Unit =
      require(filled < size, s"rank split overflowed a partition of $size rows")
      if (keyPresent) then
        codes(filled) = words.intern(key)
        Validity.setBit(keyWords, filled)
      if (valuePresent) then
        values(filled) = value
        Validity.setBit(valueWords, filled)
      filled += 1

    def toBatch: ColumnBatch =
      require(filled == size, s"rank split filled $filled of $size")
      pairs(words.toArray, codes, keyWords, values, valueWords, size)
