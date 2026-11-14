package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicInteger

/**
 * Long `reduceByKey` over a filter stack, without a survivor index.
 *
 * [[Selection]] would write an id per kept row and [[LongHashReduce.int64Selected]] would read
 * those ids back. Both columns are already in hand, and the rows collapse to a few keys, so the
 * predicates run inside the same insert as the partial. A null key is dropped even when no step
 * reads the key. A null value on a kept key stays missing until another value for that key
 * arrives, which is the [[LongHashReduce]] rule. Steps run in order. A failure drops the row.
 *
 * Each worker takes one contiguous stripe of the morsels. Within a stripe, every morsel of one
 * source batch folds into a single table and compacts once, so a key that repeats across morsels
 * is updated instead of inserted again. [[partialSetsForTest]] counts those stripes: one partial
 * set per worker. The merge and the exchange are unchanged, so first-seen order inside a bucket
 * stays the order of the source rows.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object FoldedLongReduce:

  private val partialSets = new AtomicInteger

  /** True when every step is a long key slot, a long value slot, or a long-pair row. */
  def admits(steps: Seq[Selection.Step]): Boolean =
    steps.nonEmpty && steps.forall(longStep)

  /** Stripes produced by the last [[reduce]] calls since [[resetPartialSetsForTest]]. */
  def partialSetsForTest: Int = partialSets.get

  /** Clears [[partialSetsForTest]]. */
  def resetPartialSetsForTest(): Unit =
    partialSets.set(0)

  /**
   * Partial-reduce `batches` by applying `steps` inside each morsel, merge one batch at a time,
   * then exchange. An empty input is `numPartitions` empty long-pair batches and does not count a
   * partial set. Spill matches [[LongHashReduce.byKey]]: only the post-exchange bucket can spill.
   */
  def reduce(
      batches: Vector[ColumnBatch],
      steps: List[Selection.Step],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: LongHashReduce.LongBinOp,
  ): Vector[ColumnBatch] =
    require(admits(steps), "folded long reduce needs long key, value, or row steps")
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongPairs)
    else
      try
        val (counts, morsels) = pieces(batches, batchSize)
        val width = math.min(parallelism, morsels.length)
        val partials = partialsOf(morsels, width, steps, op)
        val combined = PartialCombine.mergeMorsels(
          overlapCounts(counts, width),
          partials,
          parallelism,
          batch => LongHashReduce.int64(batch, keyOrdinal = 0, valueOrdinal = 1, op),
        )
        LongHashReduce.exchangeReduced(combined, numPartitions, parallelism, batchSize, op)
      finally LongHashReduce.releaseWorkers()

  private def longStep(step: Selection.Step): Boolean = step match
    case Selection.Step.Int64(column, _) => column == 0 || column == 1
    case Selection.Step.LongPair(_) => true
    case Selection.Step.Int32(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.StringPair(_) |
        Selection.Step.IntTriple(_, _) =>
      false

  private final case class Piece(morsel: Morsel, dense: Boolean, batchId: Int)

  /** Morsel counts line up with [[PartialCombine.mergeMorsels]]. `dense` is computed once. */
  private def pieces(
      batches: Vector[ColumnBatch],
      batchSize: Int,
  ): (Vector[Int], Vector[Piece]) =
    val grouped = batches.zipWithIndex.map { (batch, batchId) =>
      val dense = densePair(batch)
      Morsels.of(batch, batchSize).map(morsel => Piece(morsel, dense, batchId))
    }
    (grouped.map(_.length), grouped.flatten)

  private def densePair(batch: ColumnBatch): Boolean =
    val keys = ColumnHash.int64At(batch, 0)
    val values = ColumnHash.int64At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    ColumnHash.fullyPresent(keys) && ColumnHash.fullyPresent(values)

  /**
   * One stripe per worker, contiguous so flatten is batch order. Each stripe is one partial set. A
   * stripe compacts once per source batch it touches.
   */
  private def partialsOf(
      morsels: Vector[Piece],
      width: Int,
      steps: List[Selection.Step],
      op: LongHashReduce.LongBinOp,
  ): Vector[ColumnBatch] =
    val grouped = MorselScheduler.map(stripes(morsels, width), width) { (_, pieces) =>
      noteSet()
      foldStripe(pieces, steps, op)
    }
    grouped.flatten

  /** One compact per contiguous run of a source batch. Runs stay in batch order. */
  private def foldStripe(
      pieces: Vector[Piece],
      steps: List[Selection.Step],
      op: LongHashReduce.LongBinOp,
  ): Vector[ColumnBatch] =
    val rows = pieces.toArray
    val total = rows.length
    var index = 0
    var acc = Vector.empty[ColumnBatch]
    while (index < total) do
      val batchId = rows(index).batchId
      var end = index + 1
      while (end < total && rows(end).batchId == batchId) do end += 1
      acc = acc :+ foldSegment(rows, index, end, steps, op)
      index = end
    acc

  private def foldSegment(
      rows: Array[Piece],
      from: Int,
      until: Int,
      steps: List[Selection.Step],
      op: LongHashReduce.LongBinOp,
  ): ColumnBatch =
    val count = until - from
    val starts = new Array[Int](count)
    val lengths = new Array[Int](count)
    var cursor = 0
    while (cursor < count) do
      val morsel = rows(from + cursor).morsel
      starts(cursor) = morsel.start
      lengths(cursor) = morsel.length
      cursor += 1
    val head = rows(from)
    LongHashReduce.int64FilteredGroup(head.morsel.batch, head.dense, starts, lengths, steps, op)

  /**
   * How many stripe partials each input batch owns. A stripe overlaps a batch in at most one run,
   * and flatten emits those runs in batch order, so this is the [[PartialCombine]] grouping.
   */
  private def overlapCounts(counts: Vector[Int], width: Int): Vector[Int] =
    if (width <= 1) then Vector.fill(counts.length)(1)
    else
      val total = counts.foldLeft(0)((sum, count) => sum + count)
      val base = total / width
      val extra = total % width
      var batchStart = 0
      var acc = Vector.empty[Int]
      counts.foreach { count =>
        val batchEnd = batchStart + count
        var worker = 0
        var hits = 0
        while (worker < width) do
          val start = worker * base + math.min(worker, extra)
          val end = start + base + (if (worker < extra) then 1 else 0)
          if (start < batchEnd && batchStart < end) then hits += 1
          worker += 1
        acc = acc :+ hits
        batchStart = batchEnd
      }
      acc

  /**
   * Contiguous morsel groups. Earlier workers take the extra row when the count does not divide.
   */
  private def stripes(morsels: Vector[Piece], width: Int): Vector[Vector[Piece]] =
    val count = morsels.length
    val base = count / width
    val extra = count % width
    Vector.tabulate(width) { worker =>
      val start = worker * base + math.min(worker, extra)
      val length = base + (if (worker < extra) then 1 else 0)
      morsels.slice(start, start + length)
    }

  private def noteSet(): Unit =
    val _ = partialSets.incrementAndGet()
