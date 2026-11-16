package com.ewoodbury.scarlet.execution

/**
 * Checksum for the boolean bench: count even-index rows, which the source stores as `true`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object BooleanFilterChecksum:

  val shape: String = "Filter(Scan)"

  def expected(rows: Int): Long =
    var index = 0
    var count = 0L
    while (index < rows) {
      if ((index & 1) == 0) then count += 1L
      index += 1
    }
    count
