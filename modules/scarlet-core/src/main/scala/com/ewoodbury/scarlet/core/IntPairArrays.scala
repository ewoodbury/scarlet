package com.ewoodbury.scarlet.core

/**
 * Keys and values of one `(Int, Int)` partition, stored as primitive arrays.
 *
 * Iteration yields pairs, so the row path can read the partition. The arrays stay off the public
 * surface: `DistCollection.plan` and [[Partition.data]] are public, and a caller who pattern
 * matches this value must not be able to change a constructed collection. The columnar encoder
 * reads the arrays through package-scoped accessors and copies them into columns.
 */
final class IntPairArrays private (
    private[scarlet] val keys: Array[Int],
    private[scarlet] val values: Array[Int],
) extends Iterable[(Int, Int)]:

  require(keys.length == values.length, "key and value arrays differ in length")

  override def knownSize: Int = keys.length

  override def size: Int = keys.length

  override def iterator: Iterator[(Int, Int)] =
    Iterator.tabulate(keys.length)(row => (keys(row), values(row)))

object IntPairArrays:

  /**
   * Copies `keys` and `values`. Later writes to the caller's arrays do not change this partition.
   */
  def apply(keys: Array[Int], values: Array[Int]): IntPairArrays =
    fromOwned(keys.clone(), values.clone())

  /**
   * Takes ownership of arrays that are already a private copy. The caller must not mutate them.
   */
  private[scarlet] def fromOwned(keys: Array[Int], values: Array[Int]): IntPairArrays =
    new IntPairArrays(keys, values)
