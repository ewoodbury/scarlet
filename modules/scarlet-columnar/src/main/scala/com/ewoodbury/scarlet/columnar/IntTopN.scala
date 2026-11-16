package com.ewoodbury.scarlet.columnar

/**
 * Top-N and full sort of int pairs by effective value descending, then effective key ascending.
 *
 * A null value or key orders as zero and stays null. Ties keep input order: the earlier source
 * batch, then the smaller row id. [[take]] is a bounded heap of `limit` rows. When `limit` covers
 * every live row, [[take]] is the full sort. The full sort is two stable [[IntIndexSort]] passes:
 * key ascending, then bitwise-not of the effective value so a signed descending order matches
 * unsigned radix.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object IntTopN:

  def takeSelected(selected: Vector[Selected], limit: Int): ColumnBatch =
    require(limit >= 0, s"top-N limit must be non-negative, got $limit")
    if (limit == 0) then ColumnBatches.emptyIntPairs
    else
      val total = liveRows(selected)
      if (total == 0) then ColumnBatches.emptyIntPairs
      else if (limit >= total) then sortLive(selected)
      else heapSelect(selected, limit)

  def takeBatches(batches: Vector[ColumnBatch], limit: Int): ColumnBatch =
    takeSelected(denseSelected(batches), limit)

  def sortSelected(selected: Vector[Selected]): Vector[ColumnBatch] =
    Vector(sortLive(selected))

  def sortBatches(batches: Vector[ColumnBatch]): Vector[ColumnBatch] =
    sortSelected(denseSelected(batches))

  private def denseSelected(batches: Vector[ColumnBatch]): Vector[Selected] =
    batches.map { batch =>
      val rows = new Array[Int](batch.length)
      var row = 0
      while (row < batch.length) {
        rows(row) = row
        row += 1
      }
      Selected(batch, rows, batch.length)
    }

  private def liveRows(selected: Vector[Selected]): Int =
    val total = selected.iterator.map(_.count.toLong).sum
    require(total <= Int.MaxValue.toLong, s"int top-N input reached $total rows")
    total.toInt

  private def sortLive(selected: Vector[Selected]): ColumnBatch =
    val total = liveRows(selected)
    if (total == 0) then ColumnBatches.emptyIntPairs
    else
      val keys = new Array[Int](total)
      val values = new Array[Int](total)
      val keyWords = Validity.allocate(total)
      val valueWords = Validity.allocate(total)
      gather(selected, keys, values, keyWords, valueWords)
      val order = new Array[Int](total)
      var row = 0
      while (row < total) {
        order(row) = row
        row += 1
      }
      IntIndexSort.sort(keys, keyWords, order)
      val packed = new Array[Int](total)
      row = 0
      while (row < total) {
        val value = if (Validity.isSet(valueWords, row)) values(row) else 0
        packed(row) = ~value
        row += 1
      }
      IntIndexSort.sort(packed, Validity.allValid(total).words, order)
      permute(keys, values, keyWords, valueWords, order)

  private def gather(
      selected: Vector[Selected],
      keys: Array[Int],
      values: Array[Int],
      keyWords: Array[Long],
      valueWords: Array[Long],
  ): Unit =
    var out = 0
    val parts = selected.iterator
    while (parts.hasNext) {
      val one = parts.next()
      val (srcKeys, srcValues) = checkedPair(one.batch)
      val srcKeyWords = srcKeys.validity.words
      val srcValueWords = srcValues.validity.words
      var slot = 0
      while (slot < one.count) {
        val row = one.rows(slot)
        if (Validity.isSet(srcKeyWords, row)) then
          keys(out) = srcKeys.values(row)
          Validity.setBit(keyWords, out)
        if (Validity.isSet(srcValueWords, row)) then
          values(out) = srcValues.values(row)
          Validity.setBit(valueWords, out)
        out += 1
        slot += 1
      }
    }

  private def permute(
      keys: Array[Int],
      values: Array[Int],
      keyWords: Array[Long],
      valueWords: Array[Long],
      order: Array[Int],
  ): ColumnBatch =
    val rows = order.length
    val outKeys = new Array[Int](rows)
    val outValues = new Array[Int](rows)
    val outKeyWords = Validity.allocate(rows)
    val outValueWords = Validity.allocate(rows)
    var slot = 0
    while (slot < rows) {
      val row = order(slot)
      if (Validity.isSet(keyWords, row)) then
        outKeys(slot) = keys(row)
        Validity.setBit(outKeyWords, slot)
      if (Validity.isSet(valueWords, row)) then
        outValues(slot) = values(row)
        Validity.setBit(outValueWords, slot)
      slot += 1
    }
    RankScatter.pairs(outKeys, outKeyWords, outValues, outValueWords, rows)

  private def heapSelect(selected: Vector[Selected], limit: Int): ColumnBatch =
    val heap = new Bound(limit)
    var seq = 0
    val parts = selected.iterator
    while (parts.hasNext) {
      val one = parts.next()
      val (keys, values) = checkedPair(one.batch)
      val keyWords = keys.validity.words
      val valueWords = values.validity.words
      var slot = 0
      while (slot < one.count) {
        val row = one.rows(slot)
        val keyPresent = Validity.isSet(keyWords, row)
        val valuePresent = Validity.isSet(valueWords, row)
        heap.offer(
          if (keyPresent) keys.values(row) else 0,
          if (valuePresent) values.values(row) else 0,
          seq,
          keyPresent,
          valuePresent,
        )
        seq += 1
        slot += 1
      }
    }
    heap.toBatch

  private def checkedPair(batch: ColumnBatch): (Int32Column, Int32Column) =
    require(
      batch.columns.length == 2,
      s"int top-N expected two Int32 columns, got ${batch.schema.mkString("[", ", ", "]")}",
    )
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int32At(batch, 1)
    require(
      keys.length == batch.length && values.length == batch.length,
      s"key length ${keys.length}, value length ${values.length}, and batch length ${batch.length} disagree",
    )
    (keys, values)

  /**
   * Binary heap of the current top-N. Root is the worst winner (smallest value, then largest key,
   * then largest seq) so a worse candidate is rejected in one compare.
   */
  private final class Bound(limit: Int):
    private val keys = new Array[Int](limit)
    private val values = new Array[Int](limit)
    private val seqs = new Array[Int](limit)
    private val keyPresent = new Array[Boolean](limit)
    private val valuePresent = new Array[Boolean](limit)
    private var size = 0

    def offer(
        key: Int,
        value: Int,
        seq: Int,
        keyOk: Boolean,
        valueOk: Boolean,
    ): Unit =
      if (size < limit) then
        keys(size) = key
        values(size) = value
        seqs(size) = seq
        keyPresent(size) = keyOk
        valuePresent(size) = valueOk
        siftUp(size)
        size += 1
      else if (better(value, key, seq, values(0), keys(0), seqs(0))) then
        keys(0) = key
        values(0) = value
        seqs(0) = seq
        keyPresent(0) = keyOk
        valuePresent(0) = valueOk
        siftDown(0)

    def toBatch: ColumnBatch =
      val count = size
      val outKeys = new Array[Int](count)
      val outValues = new Array[Int](count)
      val outKeyWords = Validity.allocate(count)
      val outValueWords = Validity.allocate(count)
      var slot = count
      while (slot > 0) {
        slot -= 1
        outKeys(slot) = keys(0)
        outValues(slot) = values(0)
        if (keyPresent(0)) then Validity.setBit(outKeyWords, slot)
        if (valuePresent(0)) then Validity.setBit(outValueWords, slot)
        popRoot()
      }
      RankScatter.pairs(outKeys, outKeyWords, outValues, outValueWords, count)

    private def popRoot(): Unit =
      size -= 1
      if (size > 0) then
        keys(0) = keys(size)
        values(0) = values(size)
        seqs(0) = seqs(size)
        keyPresent(0) = keyPresent(size)
        valuePresent(0) = valuePresent(size)
        siftDown(0)

    private def siftUp(start: Int): Unit =
      var child = start
      while (child > 0) {
        val parent = (child - 1) >>> 1
        if (
            worse(
              values(child),
              keys(child),
              seqs(child),
              values(parent),
              keys(parent),
              seqs(parent),
            )
          )
        then
          swap(child, parent)
          child = parent
        else child = 0
      }

    private def siftDown(start: Int): Unit =
      var parent = start
      var walking = true
      while (walking) {
        val left = (parent << 1) + 1
        if (left >= size) then walking = false
        else
          val right = left + 1
          val pick =
            if (
                right < size && worse(
                  values(right),
                  keys(right),
                  seqs(right),
                  values(left),
                  keys(left),
                  seqs(left),
                )
              )
            then right
            else left
          if (
              worse(
                values(pick),
                keys(pick),
                seqs(pick),
                values(parent),
                keys(parent),
                seqs(parent),
              )
            )
          then
            swap(pick, parent)
            parent = pick
          else walking = false
      }

    private def swap(left: Int, right: Int): Unit =
      val key = keys(left)
      keys(left) = keys(right)
      keys(right) = key
      val value = values(left)
      values(left) = values(right)
      values(right) = value
      val seq = seqs(left)
      seqs(left) = seqs(right)
      seqs(right) = seq
      val keyOk = keyPresent(left)
      keyPresent(left) = keyPresent(right)
      keyPresent(right) = keyOk
      val valueOk = valuePresent(left)
      valuePresent(left) = valuePresent(right)
      valuePresent(right) = valueOk

    /** True when `a` should appear after `b` in (value DESC, key ASC, seq ASC). */
    private def worse(
        aValue: Int,
        aKey: Int,
        aSeq: Int,
        bValue: Int,
        bKey: Int,
        bSeq: Int,
    ): Boolean =
      if (aValue != bValue) then aValue < bValue
      else if (aKey != bKey) then aKey > bKey
      else aSeq > bSeq

    /** True when `a` should appear before `b`. */
    private def better(
        aValue: Int,
        aKey: Int,
        aSeq: Int,
        bValue: Int,
        bKey: Int,
        bSeq: Int,
    ): Boolean =
      worse(bValue, bKey, bSeq, aValue, aKey, aSeq)
