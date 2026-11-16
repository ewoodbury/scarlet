package com.ewoodbury.scarlet.execution

/**
 * Checksum for the int-narrow bench: keep values `> 0`, double them, then sum.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object IntNarrowChecksum:

  val shape: String = "Pipeline2(Scan)"

  def expected(values: Array[Int]): Long =
    var index = 0
    var sum = 0L
    while (index < values.length) {
      val value = values(index)
      if (value > 0) then sum += (value * 2).toLong
      index += 1
    }
    sum
