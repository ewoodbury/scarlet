package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec

/**
 * Top-N and full sort of int pairs by effective value descending, then effective key ascending.
 *
 * A null value or key orders as zero and stays null. Ties keep input order: the earlier source
 * batch, then the smaller row id. [[take]] is a bounded heap of `limit` rows. When `limit` covers
 * every live row, [[take]] is the full sort. The full sort is two stable [[IntIndexSort]] passes:
 * effective key ascending, then the bitwise-not of the effective value. [[IntIndexSort]] orders
 * signed ints. Bitwise-not is strictly decreasing on those ints, so the second pass is signed
 * value descending, the same compare the heap uses. The sign-bit flip that makes an unsigned radix
 * match `Integer.compare` lives in [[IntIndexSort]], not in `~`.
 *
 * [[takeOwned]] is that heap over fully present int arrays. A stripe aliases the partition arrays
 * and offers each survivor in source order. It does not copy the arrays and it does not build a
 * survivor index. An empty step list keeps every row. Two int-slot steps are the same pair of
 * predicates a folded count applies. Stripes keep their own heaps and merge once, so the cut does
 * not change the winners.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object IntTopN:

  private val batchCalls = new AtomicInteger

  /**
   * [[sortSelected]] and [[takeSelected]] calls since the last [[resetBatchesForTest]].
   * [[sortBatches]] and [[takeBatches]] count through those entries.
   */
  private[scarlet] def batchCallsForTest: Int = batchCalls.get

  /** Clears [[batchCallsForTest]]. */
  private[scarlet] def resetBatchesForTest(): Unit =
    batchCalls.set(0)

  def takeSelected(selected: Vector[Selected], limit: Int): ColumnBatch =
    val _ = batchCalls.incrementAndGet()
    require(limit >= 0, s"top-N limit must be non-negative, got $limit")
    if (limit == 0) then ColumnBatches.emptyIntPairs
    else
      val total = liveRows(selected)
      if (total == 0) then ColumnBatches.emptyIntPairs
      else if (limit >= total) then sortLive(selected)
      else heapSelect(selected, limit)

  def takeBatches(batches: Vector[ColumnBatch], limit: Int): ColumnBatch =
    takeSelected(denseSelected(batches), limit)

  /**
   * Top `limit` rows of fully present `parts`, in source order across the vector.
   *
   * `steps` empty keeps every row. Otherwise every step is an int key or value slot, applied in
   * order, and a failure stops the row. There is no validity bitmap: a null is not representable
   * on these arrays. Each stripe is one contiguous slice and holds those arrays by reference.
   * `parallelism` stripes run at once. Each keeps its own heap of `limit` and the heaps merge on
   * the caller. The sequence number is the row's place in the concatenated source, so a tie still
   * prefers the earlier row.
   */
  def takeOwned(
      parts: Vector[(Array[Int], Array[Int])],
      steps: List[Selection.Step],
      limit: Int,
      parallelism: Int,
  ): ColumnBatch =
    require(limit >= 0, s"top-N limit must be non-negative, got $limit")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    if (limit == 0) then ColumnBatches.emptyIntPairs
    else
      val total = ownedRows(parts)
      if (total == 0) then ColumnBatches.emptyIntPairs
      else
        val cap = math.min(limit, total)
        val stripes = cut(parts, parallelism)
        val heaps = Array.tabulate(stripes.length)(_ => new Bound(cap))
        walkStripes(stripes, steps, heaps, parallelism)
        merge(heaps, cap)

  def sortSelected(selected: Vector[Selected]): Vector[ColumnBatch] =
    val _ = batchCalls.incrementAndGet()
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
        val value = if (valuePresent) values.values(row) else 0
        if (!heap.losesOnValue(value)) then
          heap.offer(
            if (keyPresent) keys.values(row) else 0,
            value,
            seq,
            keyPresent,
            valuePresent,
          )
        seq += 1
        slot += 1
      }
    }
    heap.toBatch

  /** Source rows, after the key and value lengths agree. Zero when `parts` is empty. */
  private def ownedRows(parts: Vector[(Array[Int], Array[Int])]): Int =
    var total = 0L
    val rows = parts.iterator
    while (rows.hasNext) {
      val (keys, values) = rows.next()
      require(keys.length == values.length, "key and value arrays differ in length")
      total += keys.length.toLong
    }
    require(total <= Int.MaxValue.toLong, s"int top-N input reached $total rows")
    total.toInt

  /**
   * Contiguous slices, at most `parallelism` per partition. Earlier slices take the extra row. A
   * stripe's sequence base is the concatenated index of its first row.
   */
  private def cut(
      parts: Vector[(Array[Int], Array[Int])],
      parallelism: Int,
  ): Vector[Stripe] =
    val workers = math.max(parallelism, 1)
    var partBase = 0
    var acc = Vector.empty[Stripe]
    val rows = parts.iterator
    while (rows.hasNext) {
      val (keys, values) = rows.next()
      val length = keys.length
      if (length > 0) then
        val pieces = math.min(workers, length)
        val base = length / pieces
        val extra = length % pieces
        var piece = 0
        var start = 0
        while (piece < pieces) {
          val width = base + (if (piece < extra) then 1 else 0)
          acc = acc :+ new Stripe(keys, values, start, width, partBase + start)
          start += width
          piece += 1
        }
      partBase += length
    }
    acc

  /** One fused note per stripe, then that stripe's survivors enter its heap. */
  private def walkStripes(
      stripes: Vector[Stripe],
      steps: List[Selection.Step],
      heaps: Array[Bound],
      parallelism: Int,
  ): Unit =
    steps match
      case Nil =>
        val _ = MorselScheduler.map(stripes, parallelism) { (index, stripe) =>
          ColumnMetrics.fused()
          walkAll(stripe, heaps(index))
        }
      case Selection.Step.Int32(ordinal, keep) :: Nil =>
        require(ordinal == 0 || ordinal == 1, s"top-N ordinal $ordinal is not a pair column")
        val pred = captured(keep)
        val _ = MorselScheduler.map(stripes, parallelism) { (index, stripe) =>
          ColumnMetrics.fused()
          walkOne(stripe, heaps(index), ordinal, pred)
        }
      case Selection.Step.Int32(left, leftKeep) :: Selection.Step.Int32(right, rightKeep) :: Nil =>
        require(left == 0 || left == 1, s"top-N ordinal $left is not a pair column")
        require(right == 0 || right == 1, s"top-N ordinal $right is not a pair column")
        val keepLeft = captured(leftKeep)
        val keepRight = captured(rightKeep)
        val _ = MorselScheduler.map(stripes, parallelism) { (index, stripe) =>
          ColumnMetrics.fused()
          walkTwo(stripe, heaps(index), left, keepLeft, right, keepRight)
        }
      case other =>
        val prepared = prepare(other)
        val _ = MorselScheduler.map(stripes, parallelism) { (index, stripe) =>
          ColumnMetrics.fused()
          walkEach(stripe, heaps(index), prepared)
        }

  private def walkAll(stripe: Stripe, heap: Bound): Unit =
    val keys = stripe.keys
    val values = stripe.values
    val end = stripe.start + stripe.length
    var row = stripe.start
    var seq = stripe.seqBase
    while (row < end) {
      val value = values(row)
      if (!heap.losesOnValue(value)) then heap.offer(keys(row), value, seq, true, true)
      row += 1
      seq += 1
    }

  private def walkOne(
      stripe: Stripe,
      heap: Bound,
      ordinal: Int,
      keep: Int => Boolean,
  ): Unit =
    val keys = stripe.keys
    val values = stripe.values
    val column = if (ordinal == 0) keys else values
    val end = stripe.start + stripe.length
    var row = stripe.start
    var seq = stripe.seqBase
    while (row < end) {
      if (keep(column(row))) then
        val value = values(row)
        if (!heap.losesOnValue(value)) then heap.offer(keys(row), value, seq, true, true)
      row += 1
      seq += 1
    }

  private def walkTwo(
      stripe: Stripe,
      heap: Bound,
      leftOrdinal: Int,
      keepLeft: Int => Boolean,
      rightOrdinal: Int,
      keepRight: Int => Boolean,
  ): Unit =
    val keys = stripe.keys
    val values = stripe.values
    val left = if (leftOrdinal == 0) keys else values
    val right = if (rightOrdinal == 0) keys else values
    val end = stripe.start + stripe.length
    var row = stripe.start
    var seq = stripe.seqBase
    while (row < end) {
      if (keepLeft(left(row)) && keepRight(right(row))) then
        val value = values(row)
        if (!heap.losesOnValue(value)) then heap.offer(keys(row), value, seq, true, true)
      row += 1
      seq += 1
    }

  private def walkEach(
      stripe: Stripe,
      heap: Bound,
      steps: List[(Int, Int => Boolean)],
  ): Unit =
    val keys = stripe.keys
    val values = stripe.values
    val end = stripe.start + stripe.length
    var row = stripe.start
    var seq = stripe.seqBase
    while (row < end) {
      if (keeps(steps, keys, values, row)) then
        val value = values(row)
        if (!heap.losesOnValue(value)) then heap.offer(keys(row), value, seq, true, true)
      row += 1
      seq += 1
    }

  @tailrec
  private def keeps(
      steps: List[(Int, Int => Boolean)],
      keys: Array[Int],
      values: Array[Int],
      row: Int,
  ): Boolean =
    steps match
      case Nil => true
      case (ordinal, keep) :: rest =>
        val column = if (ordinal == 0) keys else values
        keep(column(row)) && keeps(rest, keys, values, row)

  /** One function per step, not one per row. A bare trait is wrapped once here. */
  private def captured(predicate: ColumnKernel.Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        value => fallback(value)

  private def prepare(steps: List[Selection.Step]): List[(Int, Int => Boolean)] =
    steps.map {
      case Selection.Step.Int32(ordinal, predicate) =>
        require(ordinal == 0 || ordinal == 1, s"top-N ordinal $ordinal is not a pair column")
        (ordinal, captured(predicate))
      case other =>
        require(false, s"owned top-N steps are int slots, found $other")
        (0, (_: Int) => false)
    }

  private def merge(heaps: Array[Bound], cap: Int): ColumnBatch =
    if (heaps.length == 1) then finish(heaps(0))
    else
      val combined = new Bound(cap)
      var index = 0
      while (index < heaps.length) {
        heaps(index).drainInto(combined)
        index += 1
      }
      finish(combined)

  private def finish(heap: Bound): ColumnBatch =
    if (heap.isEmpty) then ColumnBatches.emptyIntPairs else heap.toBatch

  /** One slice of one partition. `keys` and `values` are the partition arrays, not copies. */
  private final class Stripe(
      val keys: Array[Int],
      val values: Array[Int],
      val start: Int,
      val length: Int,
      val seqBase: Int,
  )

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

    /**
     * True when the heap is full and `value` is strictly below the worst winner.
     *
     * An equal value still enters [[offer]], because the key and the source sequence break that
     * tie. A null is already stored as zero, which is the number [[offer]] compares.
     */
    def losesOnValue(value: Int): Boolean =
      size == limit && value < values(0)

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

    def isEmpty: Boolean = size == 0

    /** Offers every occupied slot into `target`. This heap stays as it is. */
    def drainInto(target: Bound): Unit =
      var index = 0
      val rows = size
      while (index < rows) {
        target.offer(
          keys(index),
          values(index),
          seqs(index),
          keyPresent(index),
          valuePresent(index),
        )
        index += 1
      }

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
