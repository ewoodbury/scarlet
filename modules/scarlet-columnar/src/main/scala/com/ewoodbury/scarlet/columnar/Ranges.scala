package com.ewoodbury.scarlet.columnar

/** Range validation shared by the kernels and the fused pipelines. */
private[scarlet] object Ranges:

  /** Rejects a negative start or length and a range that ends past `rows`. */
  def check(start: Int, length: Int, rows: Int): Unit =
    require(start >= 0, s"range start must be >= 0, got $start")
    require(length >= 0, s"range length must be >= 0, got $length")
    require(
      start.toLong + length.toLong <= rows.toLong,
      s"range [$start, ${start.toLong + length.toLong}) outside column length $rows",
    )
