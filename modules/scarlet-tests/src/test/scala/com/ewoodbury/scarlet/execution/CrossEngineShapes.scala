package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection

/**
 * The four cross-engine shapes that sit beside the original six.
 *
 * `filter-distinct` is `keys.distinct.count`. `filter-twogroup` is one map over `groupByKey`
 * that keeps the count and the sum. `filter-topn` is a generic sort on `(v desc, sk asc)` plus
 * `take`. `filter-keymod` maps the key to `k % 100` and then `countByKey`. The sort and the
 * key-changing map are not columnar plans; the harness reports that decline instead of timing it.
 *
 * The checksum folds output columns left to right with `acc * 31 + field`, then sums those row
 * hashes. One column is the value. Two columns are `a * 31 + b`. Three columns are
 * `(a * 31 + b) * 31 + c`.
 */
object CrossEngineShapes:

  val topLimit: Int = 100
  private val bucket: Int = 100

  def distinct(data: DistCollection[(Int, Int)]): DistCollection[Int] =
    data.keys.distinct()

  /** Global order is `v` descending, then `sk` ascending. Generic `sortBy` does not lower. */
  def sorted(data: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    data.sortBy(topKey)(using topOrdering)

  /**
   * One map above `groupByKey`. The group lowers. The function is not a second aggregate kernel.
   */
  def twoGroup(data: DistCollection[(Int, Int)]): DistCollection[(Int, Int, Long)] =
    data.groupByKey.map { case (key, values) =>
      val (count, sum) = values.iterator.foldLeft((0, 0L)) { case ((count, sum), value) =>
        (count + 1, sum + value.toLong)
      }
      (key, count, sum)
    }

  /** `k % 100` changes the key, so `countByKey` on this map does not lower. */
  def keyMod(data: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    data.map { case (key, value) => (key % bucket, value) }.countByKey

  def pairChecksum(rows: Iterable[(Int, Int)]): Long =
    rows.iterator.foldLeft(0L)((total, row) => total + row._1.toLong * 31L + row._2.toLong)

  def tripleChecksum(rows: Iterable[(Int, Int, Long)]): Long =
    rows.iterator.foldLeft(0L) { (total, row) =>
      val (key, count, sum) = row
      total + (key.toLong * 31L + count.toLong) * 31L + sum
    }

  private def topKey(pair: (Int, Int)): (Int, Int) = (pair._2, pair._1)

  private val topOrdering: Ordering[(Int, Int)] =
    Ordering.Tuple2(Ordering.Int.reverse, Ordering.Int)
