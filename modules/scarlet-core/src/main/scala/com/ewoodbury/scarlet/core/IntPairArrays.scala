package com.ewoodbury.scarlet.core

/**
 * Primitive keys and values for one `(Int, Int)` partition.
 *
 * The iterator yields pairs for the row path. The arrays are package-private so a caller who
 * pattern matches [[Partition.data]] on a public plan cannot mutate a constructed collection. The
 * columnar encoder copies them into columns. The partition stays readable for a later action, so
 * the columns do not adopt the arrays.
 */
final class IntPairArrays private (
    private[scarlet] val keys: Array[Int],
    private[scarlet] val values: Array[Int],
) extends Iterable[(Int, Int)]:

  override def knownSize: Int = keys.length

  override def iterator: Iterator[(Int, Int)] =
    Iterator.tabulate(keys.length)(row => (keys(row), values(row)))

object IntPairArrays:

  /**
   * Wording for a key/value length mismatch. [[requireSameLength]] is the check.
   *
   * `BatchCodec.intPairs` repeats this sentence. The columnar module does not depend on core, so
   * it cannot reference this constant. A test locks the two copies together.
   */
  private[scarlet] val LengthMismatch: String = "key and value arrays differ in length"

  /** Copies `keys` and `values`. Later writes to those arrays do not change this partition. */
  def apply(keys: Array[Int], values: Array[Int]): IntPairArrays =
    requireSameLength(keys, values)
    fromOwned(keys.clone(), values.clone())

  /**
   * Owns arrays that are already a private copy of the same length. The caller must not mutate
   * them. This does not copy or check again.
   */
  private[scarlet] def fromOwned(keys: Array[Int], values: Array[Int]): IntPairArrays =
    new IntPairArrays(keys, values)

  /** Rejects key and value arrays whose lengths differ. */
  private[scarlet] def requireSameLength(keys: Array[Int], values: Array[Int]): Unit =
    require(keys.length == values.length, LengthMismatch)
