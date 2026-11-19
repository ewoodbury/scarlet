package com.ewoodbury.scarlet.columnar

import com.ewoodbury.scarlet.core.ScarletConf

/**
 * `countByKey` over a long-pair [[LongPairProject]].
 *
 * The project's input morsels are already filtered. Each row is handed to the same adapter the
 * interpreter uses, including a null key and a null value. Only a present projected key is
 * inserted. The value cell is checked and dropped, so the pair batch is never published and
 * [[LongHashCount.byKey]] does not see it. The function is not inspected.
 *
 * Partials are one count per morsel, merged per input batch, then exchanged like
 * [[LongHashCount]]. Spill stays on the post-exchange bucket. A morsel whose row estimate does not
 * fit throws [[ClosedKernelRefusal]] before its table exists. With spill off, the whole input is
 * charged the same way [[LongHashCount.byKey]] charges it.
 *
 * [[countFiltered]] is one long slot above that project. The slot sees the projected pair.
 * [[countSelected]] is the same slot when filters under the project already produced a survivor
 * index. Those ids are visited in place. The source columns are not compacted and their widths are
 * not rewritten.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object ProjectedLongPairCount:

  private val CallerRows = 8192

  def count(
      batches: Vector[ColumnBatch],
      mapper: LongPairProject.LongPairMap,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongCounts)
    else
      try
        batches.foreach(LongHashCount.requireKey)
        val rows = batches.iterator.map(_.length.toLong).sum
        if (LongHashCount.refuseBeforeCombine) then LongHashCount.refuse(rows)
        val groups = batches.map(batch => Morsels.of(batch, batchSize))
        val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
          partial(morsel, mapper)
        }
        exchange(groups.map(_.length), partials, numPartitions, parallelism, batchSize)
      finally LongHashCount.releaseWorkers()

  /**
   * One long key or value slot above the project. `batches` are the project's input, already
   * filtered when the caller interpreted that stack. Each morsel is projected and slotted, and
   * only a present projected key is inserted. A null projected value still counts.
   */
  def countFiltered(
      batches: Vector[ColumnBatch],
      mapper: LongPairProject.LongPairMap,
      above: Selection.Step,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val (column, keep) = slot(above)
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongCounts)
    else
      try
        batches.foreach(LongHashCount.requireKey)
        val rows = batches.iterator.map(_.length.toLong).sum
        if (LongHashCount.refuseBeforeCombine) then LongHashCount.refuse(rows)
        val groups = batches.map(batch => Morsels.of(batch, batchSize))
        val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
          partialFiltered(morsel, mapper, column, keep)
        }
        exchange(groups.map(_.length), partials, numPartitions, parallelism, batchSize)
      finally LongHashCount.releaseWorkers()

  /**
   * [[countFiltered]] for a survivor index under the project. `selected` names source rows. The
   * index is not gathered into a new pair batch. A null projected value still counts.
   */
  def countSelected(
      selected: Vector[Selected],
      mapper: LongPairProject.LongPairMap,
      above: Selection.Step,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val (column, keep) = slot(above)
    if (selected.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongCounts)
    else
      try
        selected.foreach(one => LongHashCount.requireKey(one.batch))
        val rows = selected.iterator.map(_.count.toLong).sum
        if (LongHashCount.refuseBeforeCombine) then LongHashCount.refuse(rows)
        val groups = selected.map { one =>
          val dense = LongPairProject.sourcesPresent(one.batch)
          ColumnBatches.cuts(one.count, batchSize).map { case (start, length) =>
            Span(one, start, start + length, dense)
          }
        }
        val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, span) =>
          partialSelected(span, mapper, column, keep)
        }
        exchange(groups.map(_.length), partials, numPartitions, parallelism, batchSize)
      finally LongHashCount.releaseWorkers()

  /**
   * One long key or value. The predicate is the function the planner stored, not a second wrapper.
   */
  private def slot(step: Selection.Step): (Int, Long => Boolean) = step match
    case Selection.Step.Int64(column, predicate) if column == 0 || column == 1 =>
      (column, value => predicate(value))
    case Selection.Step.Int64(column, _) =>
      throw new IllegalStateException(s"pair slot must be a key or a value, got $column")
    case Selection.Step.Int32(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
      throw new IllegalStateException("slot above a long pair project is a long key or value")

  private def partial(morsel: Morsel, mapper: LongPairProject.LongPairMap): ColumnBatch =
    LongHashCount.partialKeys(morsel.length) { add =>
      LongPairProject.visitKeys(morsel.batch, morsel.start, morsel.length, mapper, add)
      ColumnMetrics.fused()
    }

  private def partialFiltered(
      morsel: Morsel,
      mapper: LongPairProject.LongPairMap,
      column: Int,
      keep: Long => Boolean,
  ): ColumnBatch =
    LongHashCount.partialKeys(morsel.length) { add =>
      LongPairProject.visitFiltered(
        morsel.batch,
        morsel.start,
        morsel.length,
        mapper,
        column,
        keep,
        new KeyCount(add),
      )
      ColumnMetrics.fused()
    }

  private def partialSelected(
      span: Span,
      mapper: LongPairProject.LongPairMap,
      column: Int,
      keep: Long => Boolean,
  ): ColumnBatch =
    val rows = span.until - span.from
    LongHashCount.partialKeys(rows) { add =>
      LongPairProject.visitFilteredSelected(
        span.selected.batch,
        span.selected.rows,
        span.from,
        span.until,
        mapper,
        column,
        keep,
        new KeyCount(add),
        span.dense,
      )
      ColumnMetrics.fused()
    }

  private def exchange(
      sizes: Vector[Int],
      partials: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    val combined =
      if (shortEnough(partials)) then
        PartialCombine.mergeOnCaller(sizes, partials, LongHashCount.addCounts)
      else PartialCombine.mergeMorsels(sizes, partials, parallelism, LongHashCount.addCounts)
    finish(combined, numPartitions, parallelism, batchSize)

  private final case class Span(selected: Selected, from: Int, until: Int, dense: Boolean)

  /** Counts the key. A null projected value is the same increment as a present one. */
  private final class KeyCount(record: Long => Unit) extends LongPairProject.LongPairSink:
    def add(key: Long, value: Long): Unit = record(key)
    def addMissing(key: Long): Unit = record(key)

  private def finish(
      combined: Vector[ColumnBatch],
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    if (combined.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongCounts)
    else
      val onCaller = shortEnough(combined)
      val buckets =
        if (onCaller) then partitionHere(combined, numPartitions)
        else HashExchange.partitionAllInt64(combined, keyOrdinal = 0, numPartitions, parallelism)
      val conf = ScarletConf.get
      if (buckets.exists(bucket => LongHashCountSpill.exceeds(bucket, conf))) then
        buckets.map(bucket => LongHashCountSpill.finish(bucket, parallelism, batchSize, conf))
      else if (onCaller) then combineHere(buckets)
      else
        MorselScheduler.map(buckets, parallelism)((_, bucket) => LongHashCount.addCounts(bucket))

  private def shortEnough(batches: Vector[ColumnBatch]): Boolean =
    var total = 0
    val rows = batches.iterator
    var fits = true
    while (fits && rows.hasNext) {
      total += rows.next().length
      if (total > CallerRows) then fits = false
    }
    fits

  private def partitionHere(
      batches: Vector[ColumnBatch],
      numPartitions: Int,
  ): Vector[ColumnBatch] =
    val scattered = batches.map { batch =>
      ColumnMetrics.visit()
      HashExchange.partitionInt64(batch, keyOrdinal = 0, numPartitions)
    }
    Vector.tabulate(numPartitions) { part =>
      val pieces = scattered.map { one =>
        one.lift(part).getOrElse {
          throw new IllegalArgumentException(s"missing partition $part")
        }
      }
      ColumnBatches.concat(pieces)
    }

  private def combineHere(buckets: Vector[ColumnBatch]): Vector[ColumnBatch] =
    buckets.map { bucket =>
      ColumnMetrics.visit()
      LongHashCount.addCounts(bucket)
    }
