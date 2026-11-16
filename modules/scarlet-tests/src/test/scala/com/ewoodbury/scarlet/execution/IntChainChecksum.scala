package com.ewoodbury.scarlet.execution

/**
 * Checksum for the int-chain and long-chain benches: `steps` maps of `+ 1`, then sum.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object IntChainChecksum:

  val shape: String = "Pipeline4(Scan)"

  def expected(values: Array[Int], steps: Int): Long =
    var index = 0
    var sum = 0L
    val extra = steps.toLong
    while (index < values.length) {
      sum += values(index).toLong + extra
      index += 1
    }
    sum
