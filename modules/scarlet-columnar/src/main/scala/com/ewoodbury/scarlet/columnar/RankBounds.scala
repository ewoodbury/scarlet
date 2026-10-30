package com.ewoodbury.scarlet.columnar

/**
 * Rank boundaries shared by the int split and the long split.
 *
 * Global rank `r` is written into output partition `r * partitions / rows`. Both factors are
 * non-negative, and `/` is integer division. That quotient is the inverse of the partition start:
 * partition `i` starts at `ceil(i * rows / partitions)`, computed as
 * `(i * rows + partitions - 1) / partitions`. `part == partitions` is `rows`. Sizes differ by at
 * most one.
 *
 * [[RankScatter]] and [[RankScatterLong]] both call [[outputSizes]] and [[place]]. The column type
 * is the only difference: each caller writes its own sink. [[RankSplitRuns]] reaches the same
 * boundaries through [[RankScatter]].
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object RankBounds:

  /**
   * First global rank of output partition `part`. `part == partitions` is `rows`.
   *
   * The value is `ceil(part * rows / partitions)`, as integer division
   * `(part * rows + partitions - 1) / partitions`.
   */
  def firstRank(part: Int, rows: Int, partitions: Int): Int =
    val rank = (part.toLong * rows.toLong + partitions.toLong - 1L) / partitions.toLong
    rank.toInt

  def outputSizes(rows: Int, partitions: Int): Array[Int] =
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

  /**
   * Hands each ready row to `write` as its output partition. Partitions are visited in order. An
   * empty partition is never passed.
   */
  def place(rows: Int, partitions: Int, ready: () => Boolean)(write: Int => Unit): Unit =
    var rank = 0
    var part = 0
    while (ready()) {
      while (part + 1 < partitions && rank >= firstRank(part + 1, rows, partitions)) {
        part += 1
      }
      write(part)
      rank += 1
    }
    require(rank == rows, s"rank split wrote $rank rows and counted $rows")
