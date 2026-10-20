package com.ewoodbury.scarlet.core

/**
 * Primitive keys and values for one `(Int, Int)` partition.
 *
 * The iterator yields pairs for the row path. The arrays are package-private so a caller who
 * pattern matches [[Partition.data]] on a public plan cannot mutate a constructed collection. The
 * columnar encoder copies them into columns.
 */
final class IntPairArrays private (
    private[scarlet] val keys: Array[Int],
    private[scarlet] val values: Array[Int],
) extends Iterable[(Int, Int)]:

  require(keys.length == values.length, "key and value arrays differ in length")

  override def knownSize: Int = keys.length

  override def iterator: Iterator[(Int, Int)] =
    Iterator.tabulate(keys.length)(row => (keys(row), values(row)))

object IntPairArrays:

  /** Copies `keys` and `values`. Later writes to those arrays do not change this partition. */
  def apply(keys: Array[Int], values: Array[Int]): IntPairArrays =
    fromOwned(keys.clone(), values.clone())

  /** Owns arrays that are already a private copy. The caller must not mutate them. */
  private[scarlet] def fromOwned(keys: Array[Int], values: Array[Int]): IntPairArrays =
    new IntPairArrays(keys, values)
