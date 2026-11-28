package com.ewoodbury.scarlet.core

/**
 * Minute arithmetic on an int64 Unix epoch in seconds.
 *
 * There is no time zone and no calendar. Division floors, so one second before the epoch is the
 * previous minute.
 */
object EpochMinute:

  def minute(epoch: Long): Int =
    val minutes = floorDiv(epoch, 60L)
    floorMod(minutes, 60L).toInt

  def truncate(epoch: Long): Long =
    floorDiv(epoch, 60L) * 60L

  private def floorDiv(value: Long, divisor: Long): Long =
    val quotient = value / divisor
    if value % divisor < 0L then quotient - 1L else quotient

  private def floorMod(value: Long, divisor: Long): Long =
    val remainder = value % divisor
    if remainder < 0L then remainder + divisor else remainder
