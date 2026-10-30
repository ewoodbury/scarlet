package com.ewoodbury.scarlet.columnar

/**
 * Long-pair scatter. Boundaries are [[RankBounds]], the same rule [[RankScatter]] uses. The
 * columns are `Int64` instead of `Int32`.
 *
 * `ready` is polled before each row. `write` appends that row to the sink it is given. A null
 * payload is the caller's concern: the sink stores a key or a value only when the present flag is
 * set.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object RankScatterLong:

  val pairSchema: Vector[LogicalType] =
    Vector(LogicalType.Int64, LogicalType.Int64)

  def pairs(
      keys: Array[Long],
      keyWords: Array[Long],
      values: Array[Long],
      valueWords: Array[Long],
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      pairSchema,
      Vector(
        Int64Column.of(keys, Validity.fromWords(keyWords, rows), rows),
        Int64Column.of(values, Validity.fromWords(valueWords, rows), rows),
      ),
      rows,
    )

  /**
   * Scatters `rows` ordered rows into `partitions` batches. Every output batch is allocated before
   * the first row. An empty partition is an empty long-pair batch.
   */
  def scatter(
      rows: Int,
      partitions: Int,
      ready: () => Boolean,
  )(write: Sink => Unit): Vector[ColumnBatch] =
    val sizes = RankBounds.outputSizes(rows, partitions)
    val sinks = Array.tabulate(partitions)(bucket => new Sink(sizes(bucket)))
    RankBounds.place(rows, partitions, ready)(part => write(sinks(part)))
    Vector.tabulate(partitions) { bucket =>
      if (sizes(bucket) == 0) then ColumnBatches.emptyLongPairs
      else sinks(bucket).toBatch
    }

  /** One output partition. `write` is one row, and it does not read a missing payload. */
  final class Sink(val size: Int):

    private val keys = new Array[Long](size)
    private val values = new Array[Long](size)
    private val keyWords = Validity.allocate(size)
    private val valueWords = Validity.allocate(size)
    private var filled: Int = 0

    def write(keyPresent: Boolean, key: Long, valuePresent: Boolean, value: Long): Unit =
      require(filled < size, s"rank split overflowed a partition of $size rows")
      if (keyPresent) then
        keys(filled) = key
        Validity.setBit(keyWords, filled)
      if (valuePresent) then
        values(filled) = value
        Validity.setBit(valueWords, filled)
      filled += 1

    def toBatch: ColumnBatch =
      require(filled == size, s"rank split filled $filled of $size")
      pairs(keys, keyWords, values, valueWords, size)
