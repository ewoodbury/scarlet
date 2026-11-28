package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.EpochMinute

/**
 * Minute of the hour, and the epoch truncated to a minute, for an int64 column of seconds.
 *
 * A null cell stays null. Both results use [[EpochMinute]].
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object WideEpoch:

  def minute(column: Column, count: Int): Int32Column = column match
    case epochs: Int64Column =>
      val values = new Array[Int](count)
      val present = new Array[Boolean](count)
      var row = 0
      while row < count do
        if epochs.isValid(row) then
          present(row) = true
          values(row) = EpochMinute.minute(epochs.values(row))
        row += 1
      Int32Column.of(values, Validity.pack(count, at => present(at)), count)
    case other =>
      throw new IllegalArgumentException(s"minute of ${other.logicalType} is not supported")

  def truncate(column: Column, count: Int): Int64Column = column match
    case epochs: Int64Column =>
      val values = new Array[Long](count)
      val present = new Array[Boolean](count)
      var row = 0
      while row < count do
        if epochs.isValid(row) then
          present(row) = true
          values(row) = EpochMinute.truncate(epochs.values(row))
        row += 1
      Int64Column.of(values, Validity.pack(count, at => present(at)), count)
    case other =>
      throw new IllegalArgumentException(s"minute of ${other.logicalType} is not supported")
