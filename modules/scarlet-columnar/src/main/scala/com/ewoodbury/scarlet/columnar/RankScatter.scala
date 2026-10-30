package com.ewoodbury.scarlet.columnar

/**
 * Rank boundaries shared by the in-memory split and the spilled split.
 *
 * Global rank `r` is written into output partition `r * partitions / rows`. Both factors are
 * non-negative, and `/` is integer division. That quotient is the inverse of the partition start:
 * partition `i` starts at `ceil(i * rows / partitions)`, computed as
 * `(i * rows + partitions - 1) / partitions`. `part == partitions` is `rows`. Sizes differ by at
 * most one. [[RankSplit]] and [[RankSplitRuns]] both call [[scatter]], so the boundaries are one
 * piece of code.
 *
 * `ready` is polled before each row. `write` appends that row to the sink it is given. A null
 * payload is the caller's concern: the sink stores a key or a value only when the present flag is
 * set.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object RankScatter:

  val pairSchema: Vector[LogicalType] =
    Vector(LogicalType.Int32, LogicalType.Int32)

  def pairs(
      keys: Array[Int],
      keyWords: Array[Long],
      values: Array[Int],
      valueWords: Array[Long],
      rows: Int,
  ): ColumnBatch =
    new ColumnBatch(
      pairSchema,
      Vector(
        Int32Column.of(keys, Validity.fromWords(keyWords, rows), rows),
        Int32Column.of(values, Validity.fromWords(valueWords, rows), rows),
      ),
      rows,
    )

  /**
   * Scatters `rows` ordered rows into `partitions` batches. Every output batch is allocated before
   * the first row. An empty partition is an empty pair batch.
   */
  def scatter(
      rows: Int,
      partitions: Int,
      ready: () => Boolean,
  )(write: Sink => Unit): Vector[ColumnBatch] =
    val sizes = outputSizes(rows, partitions)
    val sinks = Array.tabulate(partitions)(bucket => new Sink(sizes(bucket)))
    var rank = 0
    var part = 0
    while (ready()) {
      while (part + 1 < partitions && rank >= firstRank(part + 1, rows, partitions)) {
        part += 1
      }
      write(sinks(part))
      rank += 1
    }
    require(rank == rows, s"rank split wrote $rank rows and counted $rows")
    Vector.tabulate(partitions) { bucket =>
      if (sizes(bucket) == 0) then ColumnBatches.emptyIntPairs
      else sinks(bucket).toBatch
    }

  /**
   * First global rank of output partition `part`. `part == partitions` is `rows`.
   *
   * The value is `ceil(part * rows / partitions)`, as integer division
   * `(part * rows + partitions - 1) / partitions`.
   */
  private def firstRank(part: Int, rows: Int, partitions: Int): Int =
    val rank = (part.toLong * rows.toLong + partitions.toLong - 1L) / partitions.toLong
    rank.toInt

  private def outputSizes(rows: Int, partitions: Int): Array[Int] =
    val sizes = new Array[Int](partitions)
    var part = 0
    var total = 0
    while (part < partitions) {
      val size = firstRank(part + 1, rows, partitions) - firstRank(part, rows, partitions)
      sizes(part) = size
      total += size
      part += 1
    }
    require(total == rows, s"rank split sized $total rows and counted $rows")
    sizes

  /** One output partition. `write` is one row, and it does not read a missing payload. */
  final class Sink(val size: Int):

    private val keys = new Array[Int](size)
    private val values = new Array[Int](size)
    private val keyWords = Validity.allocate(size)
    private val valueWords = Validity.allocate(size)
    private var filled: Int = 0

    def write(keyPresent: Boolean, key: Int, valuePresent: Boolean, value: Int): Unit =
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
