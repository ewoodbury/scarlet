package com.ewoodbury.scarlet.core

import scala.collection.AbstractIterator

/**
 * Primitive `Boolean` values for one partition.
 *
 * The iterator yields values for the row path. The array is `private[scarlet]`, so a caller who
 * pattern matches [[Partition.data]] on a public plan cannot mutate a constructed collection. The
 * columnar encoder copies it into a column. The partition stays readable for a later action, so
 * the column does not adopt the array.
 */
final class BoolArrays private (private[scarlet] val values: Array[Boolean])
    extends Iterable[Boolean]:

  override def knownSize: Int = values.length

  /** Primitive index. Only the yielded `Boolean` is boxed, at the `Iterator` boundary. */
  override def iterator: Iterator[Boolean] = new BoolArrayIterator

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class BoolArrayIterator extends AbstractIterator[Boolean]:

    private var index: Int = 0

    override def knownSize: Int = values.length - index

    override def hasNext: Boolean = index < values.length

    override def next(): Boolean =
      if (index >= values.length) then throw new NoSuchElementException("next on empty iterator")
      val current = values(index)
      index += 1
      current

object BoolArrays:

  /** Copies `values`. Later writes to that array do not change this partition. */
  def apply(values: Array[Boolean]): BoolArrays =
    fromOwned(values.clone())

  /** Owns an array that is already a private copy. The caller must not mutate it. */
  private[scarlet] def fromOwned(values: Array[Boolean]): BoolArrays =
    new BoolArrays(values)
