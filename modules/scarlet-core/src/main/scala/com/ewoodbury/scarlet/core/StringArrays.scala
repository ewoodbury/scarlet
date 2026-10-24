package com.ewoodbury.scarlet.core

import scala.collection.AbstractIterator

/**
 * `String` values for one partition.
 *
 * The iterator yields values for the row path. The array is `private[scarlet]`, so a caller who
 * pattern matches [[Partition.data]] on a public plan cannot mutate a constructed collection. The
 * columnar encoder reads the array into a dictionary column. The partition stays readable for a
 * later action, so the column does not adopt the array.
 */
final class StringArrays private (private[scarlet] val values: Array[String])
    extends Iterable[String]:

  override def knownSize: Int = values.length

  override def iterator: Iterator[String] = new StringArrayIterator

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class StringArrayIterator extends AbstractIterator[String]:

    private var index: Int = 0

    override def knownSize: Int = values.length - index

    override def hasNext: Boolean = index < values.length

    override def next(): String =
      if (index >= values.length) then throw new NoSuchElementException("next on empty iterator")
      val current = values(index)
      index += 1
      current

object StringArrays:

  /** Copies `values`. Later writes to that array do not change this partition. */
  def apply(values: Array[String]): StringArrays =
    fromOwned(values.clone())

  /** Owns an array that is already a private copy. The caller must not mutate it. */
  private[scarlet] def fromOwned(values: Array[String]): StringArrays =
    new StringArrays(values)
