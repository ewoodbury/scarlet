package com.ewoodbury.scarlet.execution

/**
 * Checksum for the string bench: count rows whose dictionary key is `k42`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object StringEqualsChecksum:

  val shape: String = "Filter(Scan)"

  val distinct: Int = 1000

  val needle: Int = 42

  def expected(rows: Int): Long =
    var index = 0
    var count = 0L
    while (index < rows) {
      if (index % distinct == needle) then count += 1L
      index += 1
    }
    count

  def rows(count: Int): Array[String] =
    val pool = Array.tabulate(distinct)(index => "k" + index.toString)
    require(pool(needle).compareTo("k42") == 0, "string needle drifted")
    Array.tabulate(count)(row => pool(row % distinct))
