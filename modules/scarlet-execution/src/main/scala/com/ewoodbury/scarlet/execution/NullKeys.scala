package com.ewoodbury.scarlet.execution

/**
 * Null-key policy for keyed row operators.
 *
 * The shuffle routes a null key to partition 0 and does not drop it. Grouping and inner join drop
 * it here, which matches the columnar path. Left outer join does not call this on the preserved
 * side: a left null is kept and never matches. Right outer join drops the left side here and keeps
 * a right null, which never matches.
 */
object NullKeys:

  /** Drops rows whose key is null. */
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Equals"))
  def drop[K, V](rows: Iterable[(K, V)]): Iterable[(K, V)] =
    rows.filter { case (key, _) => key != null }
