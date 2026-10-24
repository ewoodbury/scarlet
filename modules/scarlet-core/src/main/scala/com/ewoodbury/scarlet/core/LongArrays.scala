package com.ewoodbury.scarlet.core

import scala.collection.AbstractIterator

/**
 * Primitive `Long` values for one partition.
 *
 * The iterator yields values for the row path. The array is `private[scarlet]`, so a caller who
 * pattern matches [[Partition.data]] on a public plan cannot mutate a constructed collection. The
 * columnar encoder copies it into a column. The partition stays readable for a later action, so
 * the column does not adopt the array. `length` and `apply` stay on that array and are not a
 * public mutable surface.
 */
final class LongArrays private (private[scarlet] val values: Array[Long]) extends Iterable[Long]:

  override def knownSize: Int = values.length

  /** Primitive index. Only the yielded `Long` is boxed, at the `Iterator` boundary. */
  override def iterator: Iterator[Long] = new LongArrayIterator

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class LongArrayIterator extends AbstractIterator[Long]:

    private var index: Int = 0

    override def knownSize: Int = values.length - index

    override def hasNext: Boolean = index < values.length

    override def next(): Long =
      if (index >= values.length) then throw new NoSuchElementException("next on empty iterator")
      val current = values(index)
      index += 1
      current

object LongArrays:

  /** Copies `values`. Later writes to that array do not change this partition. */
  def apply(values: Array[Long]): LongArrays =
    fromOwned(values.clone())

  /** Owns an array that is already a private copy. The caller must not mutate it. */
  private[scarlet] def fromOwned(values: Array[Long]): LongArrays =
    new LongArrays(values)
