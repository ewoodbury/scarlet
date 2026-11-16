package com.ewoodbury.scarlet.columnar

/**
 * Bottom-up mergesort of row indexes. The caller supplies the merge of one pair of runs; the
 * driver doubles the run width and ping-pongs two index buffers. `merge` stays specialized so the
 * primitive compare is not boxed.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[columnar] object IndexMerge:

  inline def sort(order: Array[Int])(
      inline merge: (Array[Int], Array[Int], Int, Int, Int) => Unit,
  ): Unit =
    val rows = order.length
    if (rows > 1) then
      val buffer = new Array[Int](rows)
      var src = order
      var dst = buffer
      var sortedInOrder = true
      var width = 1
      while (width < rows) {
        var start = 0
        while (start < rows) {
          val mid = math.min(start.toLong + width.toLong, rows.toLong).toInt
          val end = math.min(start.toLong + (width.toLong << 1), rows.toLong).toInt
          merge(src, dst, start, mid, end)
          start = end
        }
        val previous = src
        src = dst
        dst = previous
        sortedInOrder = !sortedInOrder
        val grown = width.toLong << 1
        width = if (grown >= rows.toLong) rows else grown.toInt
      }
      if (!sortedInOrder) then System.arraycopy(src, 0, order, 0, rows)
