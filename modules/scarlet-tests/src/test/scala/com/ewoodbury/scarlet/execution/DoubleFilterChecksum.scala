package com.ewoodbury.scarlet.execution

/**
 * Checksum for the double bench: keep values `> 0.0`, then sum.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object DoubleFilterChecksum:

  val shape: String = "Filter(Scan)"

  def expected(values: Array[Int]): Double =
    var index = 0
    var sum = 0.0
    while (index < values.length) {
      val value = values(index).toDouble
      if (value > 0.0) then sum += value
      index += 1
    }
    sum
