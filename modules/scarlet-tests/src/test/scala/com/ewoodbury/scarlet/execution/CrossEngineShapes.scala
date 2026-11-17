package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.DistCollection

/**
 * The cross-engine shapes that sit beside the original six.
 *
 * `filter-distinct` is `keys.distinct.count`. `filter-twogroup` is one map over `groupByKey` that
 * keeps the count and the sum. `filter-topn` is `sortByIntValueDesc` on `(v desc, sk asc)` plus
 * `take`. `filter-keymod` maps the key with `mapIntKey` to `k % 100` and then `countByKey`. That
 * typed key map lowers. A generic pair `map` of the same arithmetic stays on the row path.
 * `filter-joinagg` filters both int tables, joins on the key, groups by that key, and sums the
 * left value.
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

  /** Global order is `v` descending, then `sk` ascending. */
  def sorted(data: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    data.sortByIntValueDesc

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

  /** `k % 100` changes the key. [[com.ewoodbury.scarlet.api.DistCollection.mapIntKey]] lowers. */
  def keyMod(data: DistCollection[(Int, Int)]): DistCollection[(Int, Int)] =
    data.mapIntKey(key => key % bucket).countByKey

  /**
   * Filter both sides, join on the key, group by that key, and sum the left value.
   *
   * The key filter is `key % 3 != 0` on both sides. With two filters the left value keeps
   * `value >= 0` and the right value keeps `value < 50`. Keys are `row % 1024`, so the group has
   * 682 keys. Each matching right row adds the left value once. `filters` is 1 or 2.
   *
   * The group key is the join key. A map that replaced it with the right value would keep the
   * join's hash partitioner, `reduceByKey` would skip the shuffle, and the sums would not be a
   * global group-by. `reduceByKey` lowers only for a pair child. This child is a map over the
   * join, and a join is a triple, so the aggregate does not lower. The map itself lowers as
   * `Project` and then declines at `tryRun`, because the function does not stay
   * `(Int, (Int, Int))`.
   */
  def joinAgg(
      left: DistCollection[(Int, Int)],
      right: DistCollection[(Int, Int)],
      filters: Int,
  ): DistCollection[(Int, Long)] =
    val filteredLeft = applyFilters(left, filters, (value: Int) => value >= 0)
    val filteredRight = applyFilters(right, filters, (value: Int) => value < 50)
    filteredLeft
      .join(filteredRight)
      .map { case (key, (leftValue, _)) => (key, leftValue.toLong) }
      .reduceByKey(_ + _)

  def pairChecksum(rows: Iterable[(Int, Int)]): Long =
    rows.iterator.foldLeft(0L)((total, row) => total + row._1.toLong * 31L + row._2.toLong)

  /** `(group, sum)` rows. Same fold as [[pairChecksum]], with a `Long` sum. */
  def sumChecksum(rows: Iterable[(Int, Long)]): Long =
    rows.iterator.foldLeft(0L)((total, row) => total + row._1.toLong * 31L + row._2)

  private def applyFilters(
      data: DistCollection[(Int, Int)],
      filters: Int,
      keepValue: Int => Boolean,
  ): DistCollection[(Int, Int)] =
    val keyed = data.filterKeys((key: Int) => key % 3 != 0)
    if (filters == 1) then keyed else keyed.filterValues(keepValue)

  def tripleChecksum(rows: Iterable[(Int, Int, Long)]): Long =
    rows.iterator.foldLeft(0L) { (total, row) =>
      val (key, count, sum) = row
      total + (key.toLong * 31L + count.toLong) * 31L + sum
    }
