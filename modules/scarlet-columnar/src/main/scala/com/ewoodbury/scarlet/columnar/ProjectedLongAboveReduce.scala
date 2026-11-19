package com.ewoodbury.scarlet.columnar

/**
 * `reduceByKey` over one long slot above a long-pair [[LongPairProject]].
 *
 * The project's input is the scan under that project. A long-pair filter stack between them is
 * applied per morsel, because compacting each morsel of a full partition allocates a
 * partition-sized column. Each row is handed to the same adapter the interpreter uses, including a
 * null key and a null value. The slot then sees the projected pair: a key predicate is not called
 * on a null projected key, and a value predicate is not called on a null projected value. A
 * present projected key and value that the slot keeps are inserted into the [[LongHashReduce]]
 * table. A null projected key is dropped. A null projected value is a null group until another
 * value for that key arrives, and a later combine unboxes that null to `0`. The pair batch is
 * never published, and [[LongHashReduce.byKey]] does not see it. Neither function is inspected.
 *
 * Partials are one reduce per morsel, merged per input batch, then exchanged like
 * [[ProjectedLongPairReduce]]. Spill stays on the post-exchange bucket.
 */
private[scarlet] object ProjectedLongAboveReduce:

  def reduce(
      batches: Vector[ColumnBatch],
      mapper: LongPairProject.LongPairMap,
      under: Vector[Selection.Step],
      above: Selection.Step,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: LongHashReduce.LongBinOp,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val (column, keep) = slot(above)
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyLongPairs)
    else
      try
        val groups = batches.map(batch => Morsels.of(batch, batchSize))
        val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
          partial(morsel, mapper, under, column, keep, op)
        }
        val combined = PartialCombine.mergeMorsels(
          groups.map(_.length),
          partials,
          parallelism,
          batch => LongHashReduce.int64(batch, keyOrdinal = 0, valueOrdinal = 1, op),
        )
        LongHashReduce.exchangeReduced(combined, numPartitions, parallelism, batchSize, op)
      finally LongHashReduce.releaseOversized()

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

  private def partial(
      morsel: Morsel,
      mapper: LongPairProject.LongPairMap,
      under: Vector[Selection.Step],
      column: Int,
      keep: Long => Boolean,
      op: LongHashReduce.LongBinOp,
  ): ColumnBatch =
    LongHashReduce.partialPairs(morsel.length, op) { sink =>
      val filtered =
        if (under.isEmpty) then morsel.batch
        else Selection(morsel.batch, morsel.start, morsel.length, under)
      val start = if (under.isEmpty) then morsel.start else 0
      val length = if (under.isEmpty) then morsel.length else filtered.length
      LongPairProject.visitFiltered(filtered, start, length, mapper, column, keep, sink)
      ColumnMetrics.fused()
    }
