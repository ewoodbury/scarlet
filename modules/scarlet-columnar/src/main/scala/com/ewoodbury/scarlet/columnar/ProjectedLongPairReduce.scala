package com.ewoodbury.scarlet.columnar

/**
 * `reduceByKey` over a long-pair [[LongPairProject]].
 *
 * The project's input morsels are already in hand. Each row is handed to the same adapter the
 * interpreter uses, including a null key and a null value. A null projected key is dropped. A null
 * projected value stays missing until another value for that key arrives, which is the
 * [[LongHashReduce]] rule. The pair batch is never published and [[LongHashReduce.byKey]] does not
 * see it. The function is not inspected.
 *
 * One worker stripe compacts once per source batch it touches, so a repeated projected key is
 * updated in that table instead of written out and hashed again. Those partials are merged per
 * input batch, then exchanged like [[LongHashReduce]]. Spill stays on the post-exchange bucket.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object ProjectedLongPairReduce:

  private val CallerRows = 8192

  private final case class Piece(morsel: Morsel, batchId: Int)

  def reduce(
      batches: Vector[ColumnBatch],
      mapper: LongPairProject.LongPairMap,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: LongHashReduce.LongBinOp,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongPairs)
    else
      try
        val grouped = batches.zipWithIndex.map { (batch, batchId) =>
          Morsels.of(batch, batchSize).map(morsel => Piece(morsel, batchId))
        }
        val morsels = grouped.flatten
        val width = math.min(parallelism, morsels.length)
        val partials =
          MorselScheduler
            .map(stripes(morsels, width), width) { (_, pieces) =>
              foldStripe(pieces, mapper, op)
            }
            .flatten
        val sizes = overlapCounts(grouped.map(_.length), width)
        val combined =
          if (shortEnough(partials)) then
            PartialCombine.mergeOnCaller(sizes, partials, combine(op))
          else PartialCombine.mergeMorsels(sizes, partials, parallelism, combine(op))
        LongHashReduce.exchangeReduced(combined, numPartitions, parallelism, batchSize, op)
      finally LongHashReduce.releaseOversized()

  /** One compact per contiguous run of a source batch. Runs stay in batch order. */
  private def foldStripe(
      pieces: Vector[Piece],
      mapper: LongPairProject.LongPairMap,
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
      acc = acc :+ foldSegment(rows, index, end, mapper, op)
      index = end
    acc

  private def foldSegment(
      rows: Array[Piece],
      from: Int,
      until: Int,
      mapper: LongPairProject.LongPairMap,
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
      if (morsel.length > 0) then ColumnMetrics.fused()
      cursor += 1
    LongHashReduce.partialProjectedRanges(
      rows(from).morsel.batch,
      starts,
      lengths,
      mapper,
      op,
    )

  private def combine(op: LongHashReduce.LongBinOp)(batch: ColumnBatch): ColumnBatch =
    LongHashReduce.int64(batch, keyOrdinal = 0, valueOrdinal = 1, op)

  private def shortEnough(batches: Vector[ColumnBatch]): Boolean =
    var total = 0
    val rows = batches.iterator
    var fits = true
    while (fits && rows.hasNext) {
      total += rows.next().length
      if (total > CallerRows) then fits = false
    }
    fits

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

  /** Contiguous morsel groups. Earlier workers take the extra when the count does not divide. */
  private def stripes(morsels: Vector[Piece], width: Int): Vector[Vector[Piece]] =
    val count = morsels.length
    val base = count / width
    val extra = count % width
    Vector.tabulate(width) { worker =>
      val start = worker * base + math.min(worker, extra)
      val length = base + (if (worker < extra) then 1 else 0)
      morsels.slice(start, start + length)
    }
