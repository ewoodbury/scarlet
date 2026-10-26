package com.ewoodbury.scarlet.runtime.local

import com.ewoodbury.scarlet.runtime.api.Partitioner

final class HashPartitioner extends Partitioner:
  /**
   * Assigns a key to a partition using a non-negative modulus, so negative hash codes (including
   * `Int.MinValue`) map to valid partition indexes. A null key uses hash code 0 and does not
   * throw, so the row still reaches the join task.
   */
  def partition(key: Any, numPartitions: Int): Int =
    if numPartitions <= 0 then 0 else Math.floorMod(hashOf(key), numPartitions)

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Equals"))
  private def hashOf(key: Any): Int =
    if (key == null) 0 else key.hashCode()
