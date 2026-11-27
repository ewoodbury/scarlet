package com.ewoodbury.scarlet.columnar

/**
 * `reduceByKey` over one int slot above an int-pair [[IntPairProject]].
 *
 * The project's input morsels are already filtered. Each row is handed to the same adapter the
 * interpreter uses, including a null key and a null value. The slot then sees the projected pair:
 * a key predicate is not called on a null projected key, and a value predicate is not called on a
 * null projected value. A present projected key and value that the slot keeps are inserted into
 * the [[HashReduce]] table. A null projected key is dropped. A null projected value on a kept key
 * stays a null group. A value slot does not keep that null. The pair batch is never published, and
 * [[HashReduce.byKey]] does not see it. Neither function is inspected.
 *
 * Partials are one reduce per morsel, merged per input batch, then exchanged like
 * [[ProjectedPairReduce]]. Spill stays on the post-exchange bucket.
 */
private[scarlet] object ProjectedAboveReduce:

  def reduce(
      batches: Vector[ColumnBatch],
      mapper: IntPairProject.IntPairMap,
      above: Selection.Step,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      op: HashReduce.IntBinOp,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val (column, keep) = slot(above)
    if (batches.isEmpty) then Vector.fill(numPartitions)(ColumnBatches.emptyIntPairs)
    else
      val groups = batches.map(batch => Morsels.of(batch, batchSize))
      val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
        partial(morsel, mapper, column, keep, op)
      }
      val combined = PartialCombine.mergeMorsels(
        groups.map(_.length),
        partials,
        parallelism,
        batch => HashReduce.int32(batch, keyOrdinal = 0, valueOrdinal = 1, op),
      )
      HashReduce.reduceExchanged(combined, numPartitions, parallelism, batchSize, op)

  /**
   * One int key or value. The predicate is the function the planner stored, not a second wrapper.
   */
  private def slot(step: Selection.Step): (Int, Int => Boolean) = step match
    case Selection.Step.Int32(column, predicate) if column == 0 || column == 1 =>
      (column, captured(predicate))
    case Selection.Step.Int32(column, _) =>
      throw new IllegalStateException(s"pair slot must be a key or a value, got $column")
    case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
        Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
        Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
      throw new IllegalStateException("slot above a pair project is an int key or value")

  private def captured(predicate: ColumnKernel.Int32Predicate): Int => Boolean =
    predicate match
      case direct: Selection.Int32Function => direct.call
      case other =>
        val fallback = other
        value => fallback(value)

  private def partial(
      morsel: Morsel,
      mapper: IntPairProject.IntPairMap,
      column: Int,
      keep: Int => Boolean,
      op: HashReduce.IntBinOp,
  ): ColumnBatch =
    HashReduce.partialPairs(morsel.length, op) { sink =>
      IntPairProject.visitFiltered(
        morsel.batch,
        morsel.start,
        morsel.length,
        mapper,
        column,
        keep,
        sink,
      )
      ColumnMetrics.fused()
    }
