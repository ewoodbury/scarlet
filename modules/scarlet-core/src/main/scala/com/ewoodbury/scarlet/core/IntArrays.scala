package com.ewoodbury.scarlet.core

/**
 * Primitive `Int` values for one partition.
 *
 * The iterator yields values for the row path. The array is package-private so a caller who
 * pattern matches [[Partition.data]] on a public plan cannot mutate a constructed collection. The
 * columnar encoder copies it into a column. The partition stays readable for a later action, so
 * the column does not adopt the array.
 */
final class IntArrays private (private[scarlet] val values: Array[Int]) extends Iterable[Int]:

  override def knownSize: Int = values.length

  override def iterator: Iterator[Int] =
    Iterator.tabulate(values.length)(row => values(row))

object IntArrays:

  /** Copies `values`. Later writes to that array do not change this partition. */
  def apply(values: Array[Int]): IntArrays =
    fromOwned(values.clone())

  /** Owns an array that is already a private copy. The caller must not mutate it. */
  private[scarlet] def fromOwned(values: Array[Int]): IntArrays =
    new IntArrays(values)
