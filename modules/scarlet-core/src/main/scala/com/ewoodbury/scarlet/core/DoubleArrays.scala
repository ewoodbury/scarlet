package com.ewoodbury.scarlet.core

import scala.collection.AbstractIterator

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

  /** Primitive index. Only the yielded `Double` is boxed, at the `Iterator` boundary. */
  override def iterator: Iterator[Double] = new DoubleArrayIterator

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class DoubleArrayIterator extends AbstractIterator[Double]:

    private var index: Int = 0

    override def knownSize: Int = values.length - index

    override def hasNext: Boolean = index < values.length

    override def next(): Double =
      if (index >= values.length) then throw new NoSuchElementException("next on empty iterator")
      val current = values(index)
      index += 1
      current

object DoubleArrays:

  /** Copies `values`. Later writes to that array do not change this partition. */
  def apply(values: Array[Double]): DoubleArrays =
    fromOwned(values.clone())

  /** Owns an array that is already a private copy. The caller must not mutate it. */
  private[scarlet] def fromOwned(values: Array[Double]): DoubleArrays =
    new DoubleArrays(values)
