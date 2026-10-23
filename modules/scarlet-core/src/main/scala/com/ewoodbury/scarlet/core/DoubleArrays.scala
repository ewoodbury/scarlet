package com.ewoodbury.scarlet.core

/**
 * Primitive `Double` values for one partition.
 *
 * The iterator yields values for the row path. The array is `private[scarlet]`, so a caller who
 * pattern matches [[Partition.data]] on a public plan cannot mutate a constructed collection. The
 * columnar encoder copies it into a column. The partition stays readable for a later action, so
 * the column does not adopt the array.
 */
final class DoubleArrays private (private[scarlet] val values: Array[Double])
    extends Iterable[Double]:

  override def knownSize: Int = values.length

  override def iterator: Iterator[Double] =
    Iterator.tabulate(values.length)(row => values(row))

object DoubleArrays:

  /** Copies `values`. Later writes to that array do not change this partition. */
  def apply(values: Array[Double]): DoubleArrays =
    fromOwned(values.clone())

  /** Owns an array that is already a private copy. The caller must not mutate it. */
  private[scarlet] def fromOwned(values: Array[Double]): DoubleArrays =
    new DoubleArrays(values)
