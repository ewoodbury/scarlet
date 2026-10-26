package com.ewoodbury.scarlet.execution

/**
 * Null-key policy for keyed row operators.
 *
 * The shuffle routes a null key to partition 0 and does not drop it. Grouping and inner join drop
 * it here, which matches the columnar path. Outer joins group both sides with this drop, so a null
 * key never matches, then emit each preserved null-key row once.
 */
object NullKeys:

  /** Drops rows whose key is null. */
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Equals"))
  def drop[K, V](rows: Iterable[(K, V)]): Iterable[(K, V)] =
    rows.filter { case (key, _) => key != null }
