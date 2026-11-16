package com.ewoodbury.scarlet.columnar

/**
 * Bounded heap of int pairs. Order is value descending, then key ascending.
 *
 * A null orders as zero and stays null. Ties keep input order: an earlier batch, then a smaller
 * row. The root is the worst row still kept, so a better candidate replaces it. After the scan,
 * extracting that root writes the kept rows best-first. `n` larger than the input keeps every row,
 * in that same order. `n <= 0` is an empty batch and does not read a column.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object TopN:

  def ofBatches(batches: Vector[ColumnBatch], n: Int): ColumnBatch =
    if (n <= 0) then ColumnBatches.emptyIntPairs
    else
      val total = rowCount(batches)
      if (total == 0) then ColumnBatches.emptyIntPairs
      else
        val heap = new Heap(math.min(n, total))
        scanBatches(batches, heap)
        heap.toBatch

  def ofSelected(parts: Vector[Selected], n: Int): ColumnBatch =
    if (n <= 0) then ColumnBatches.emptyIntPairs
    else
      val total = selectedCount(parts)
      if (total == 0) then ColumnBatches.emptyIntPairs
      else
        val heap = new Heap(math.min(n, total))
        scanSelected(parts, heap)
        heap.toBatch

  private def rowCount(batches: Vector[ColumnBatch]): Int =
    val total = batches.iterator.map(_.length.toLong).sum
    require(total <= Int.MaxValue.toLong, s"top-n input reached $total rows")
    total.toInt

  private def selectedCount(parts: Vector[Selected]): Int =
    val total = parts.iterator.map(_.count.toLong).sum
    require(total <= Int.MaxValue.toLong, s"top-n input reached $total rows")
    total.toInt

  private def scanBatches(batches: Vector[ColumnBatch], heap: Heap): Unit =
    val all = batches.toArray
    var batchIndex = 0
    while (batchIndex < all.length) do
      val batch = all(batchIndex)
      if (batch.length > 0) then
        val keys = ColumnHash.int32At(batch, 0)
        val values = ColumnHash.int32At(batch, 1)
        val keyWords = keys.validity.words
        val valueWords = values.validity.words
        val keyPayload = keys.values
        val valuePayload = values.values
        var row = 0
        while (row < batch.length) do
          offer(heap, batchIndex, row, keyWords, keyPayload, valueWords, valuePayload)
          row += 1
      batchIndex += 1

  private def scanSelected(parts: Vector[Selected], heap: Heap): Unit =
    val all = parts.toArray
    var batchIndex = 0
    while (batchIndex < all.length) do
      val selected = all(batchIndex)
      if (selected.count > 0) then
        val keys = ColumnHash.int32At(selected.batch, 0)
        val values = ColumnHash.int32At(selected.batch, 1)
        val keyWords = keys.validity.words
        val valueWords = values.validity.words
        val keyPayload = keys.values
        val valuePayload = values.values
        val rows = selected.rows
        var slot = 0
        while (slot < selected.count) do
          offer(heap, batchIndex, rows(slot), keyWords, keyPayload, valueWords, valuePayload)
          slot += 1
      batchIndex += 1

  private def offer(
      heap: Heap,
      batch: Int,
      row: Int,
      keyWords: Array[Long],
      keyPayload: Array[Int],
      valueWords: Array[Long],
      valuePayload: Array[Int],
  ): Unit =
    val keyOn = Validity.isSet(keyWords, row)
    val valueOn = Validity.isSet(valueWords, row)
    val key = if (keyOn) keyPayload(row) else 0
    val value = if (valueOn) valuePayload(row) else 0
    heap.offer(batch, row, keyOn, key, valueOn, value)

  /** Root is the worst kept slot. Slot arrays outlive the heap index. */
  private final class Heap(cap: Int):
    private val keys = new Array[Int](cap)
    private val values = new Array[Int](cap)
    private val keyOn = new Array[Boolean](cap)
    private val valueOn = new Array[Boolean](cap)
    private val batchAt = new Array[Int](cap)
    private val rowAt = new Array[Int](cap)
    private val heap = new Array[Int](cap)
    private var live = 0
    private var ordered = false

    def offer(
        batch: Int,
        row: Int,
        keyPresent: Boolean,
        key: Int,
        valuePresent: Boolean,
        value: Int,
    ): Unit =
      if (live < cap) then
        store(live, batch, row, keyPresent, key, valuePresent, value)
        heap(live) = live
        live += 1
      else
        if (!ordered) then
          heapify()
          ordered = true
        if (beats(value, key, batch, row)) then
          store(heap(0), batch, row, keyPresent, key, valuePresent, value)
          siftDown(0)

    def toBatch: ColumnBatch =
      if (live == 0) then ColumnBatches.emptyIntPairs
      else
        if (!ordered) then heapify()
        val count = live
        val order = new Array[Int](count)
        var left = count
        while (left > 0) do
          left -= 1
          order(left) = popWorst()
        gather(order)

    private def store(
        slot: Int,
        batch: Int,
        row: Int,
        keyPresent: Boolean,
        key: Int,
        valuePresent: Boolean,
        value: Int,
    ): Unit =
      keys(slot) = key
      values(slot) = value
      keyOn(slot) = keyPresent
      valueOn(slot) = valuePresent
      batchAt(slot) = batch
      rowAt(slot) = row

    /** A candidate stays only when it is strictly better than the worst kept row. */
    private def beats(value: Int, key: Int, batch: Int, row: Int): Boolean =
      val slot = heap(0)
      val valueCmp = Integer.compare(value, values(slot))
      if (valueCmp != 0) then valueCmp > 0
      else
        val keyCmp = Integer.compare(key, keys(slot))
        if (keyCmp != 0) then keyCmp < 0
        else
          val batchCmp = Integer.compare(batch, batchAt(slot))
          if (batchCmp != 0) then batchCmp < 0
          else row < rowAt(slot)

    /** `left` sits above `right` when it should be evicted first. */
    private def worse(left: Int, right: Int): Boolean =
      val valueCmp = Integer.compare(values(left), values(right))
      if (valueCmp != 0) then valueCmp < 0
      else
        val keyCmp = Integer.compare(keys(left), keys(right))
        if (keyCmp != 0) then keyCmp > 0
        else
          val batchCmp = Integer.compare(batchAt(left), batchAt(right))
          if (batchCmp != 0) then batchCmp > 0
          else rowAt(left) > rowAt(right)

    private def heapify(): Unit =
      var node = (live >>> 1) - 1
      while (node >= 0) do
        siftDown(node)
        node -= 1

    private def siftDown(start: Int): Unit =
      var current = start
      var moving = true
      while (moving) do
        val left = (current << 1) + 1
        if (left >= live) then moving = false
        else
          var child = left
          val right = left + 1
          if (right < live && worse(heap(right), heap(left))) then child = right
          if (worse(heap(child), heap(current))) then
            val held = heap(current)
            heap(current) = heap(child)
            heap(child) = held
            current = child
          else moving = false

    private def popWorst(): Int =
      val worst = heap(0)
      live -= 1
      if (live > 0) then
        heap(0) = heap(live)
        siftDown(0)
      worst

    private def gather(order: Array[Int]): ColumnBatch =
      val count = order.length
      val outKeys = new Array[Int](count)
      val outValues = new Array[Int](count)
      val keyWords = Validity.allocate(count)
      val valueWords = Validity.allocate(count)
      var out = 0
      while (out < count) do
        val slot = order(out)
        if (keyOn(slot)) then
          outKeys(out) = keys(slot)
          Validity.setBit(keyWords, out)
        if (valueOn(slot)) then
          outValues(out) = values(slot)
          Validity.setBit(valueWords, out)
        out += 1
      RankScatter.pairs(outKeys, keyWords, outValues, valueWords, count)
