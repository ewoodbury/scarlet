package com.ewoodbury.scarlet.core

/**
 * Primitive keys and values for one `(Long, Long)` partition.
 *
 * The iterator yields pairs for the row path. The arrays are `private[scarlet]`, so a caller who
 * pattern matches [[Partition.data]] on a public plan cannot mutate a constructed collection. The
 * columnar encoder copies them into columns. The partition stays readable for a later action, so
 * the columns do not adopt the arrays.
 */
final class LongPairArrays private (
    private[scarlet] val keys: Array[Long],
    private[scarlet] val values: Array[Long],
) extends Iterable[(Long, Long)]:

  override def knownSize: Int = keys.length

  override def iterator: Iterator[(Long, Long)] =
    Iterator.tabulate(keys.length)(row => (keys(row), values(row)))

object LongPairArrays:

  /**
   * Wording for a key/value length mismatch. [[requireSameLength]] is the check.
   *
   * `BatchCodec` repeats this sentence. The columnar module does not depend on core, so it cannot
   * reference this constant. A test locks the two copies together.
   */
  private[scarlet] val LengthMismatch: String = "key and value arrays differ in length"

  /** Copies `keys` and `values`. Later writes to those arrays do not change this partition. */
  def apply(keys: Array[Long], values: Array[Long]): LongPairArrays =
    requireSameLength(keys, values)
    fromOwned(keys.clone(), values.clone())

  /**
   * Owns arrays that are already a private copy. The caller must not mutate them. This does not
   * copy. Lengths are checked here so a later encode cannot take its row count from the keys
   * alone.
   */
  private[scarlet] def fromOwned(keys: Array[Long], values: Array[Long]): LongPairArrays =
    requireSameLength(keys, values)
    new LongPairArrays(keys, values)

  /** Rejects key and value arrays whose lengths differ. */
  private[scarlet] def requireSameLength(keys: Array[Long], values: Array[Long]): Unit =
    require(keys.length == values.length, LengthMismatch)
