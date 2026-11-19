package com.ewoodbury.scarlet.columnar

/**
 * `countSumByKey` over one int slot above an int-pair [[IntPairProject]].
 *
 * The project's input morsels are already filtered. Each row is handed to the same adapter the
 * interpreter uses, including a null key and a null value. The slot then sees the projected pair:
 * a key predicate is not called on a null projected key, and a value predicate is not called on a
 * null projected value. A present projected key that the slot keeps is observed into the
 * [[CountSum]] table. A null projected value on that row is counted and not added. A null
 * projected key is dropped. The pair batch is never published, and [[CountSum.byKey]] does not see
 * it. Neither function is inspected.
 *
 * Partials are one count-sum per morsel, merged per input batch, then exchanged like
 * [[ProjectedPairCountSum]]. There is no spill path: a partial is `O(distinct keys)`.
 */
private[scarlet] object ProjectedAboveCountSum:

  def sum(
      batches: Vector[ColumnBatch],
      mapper: IntPairProject.IntPairMap,
      above: Selection.Step,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    val (column, keep) = slot(above)
    if (batches.isEmpty) then CountSum.emptyCountSums(numPartitions)
    else
      val groups = batches.map(batch => Morsels.of(batch, batchSize))
      val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, morsel) =>
        partial(morsel, mapper, column, keep)
      }
      val combined = PartialCombine.mergeMorsels(
        groups.map(_.length),
        partials,
        parallelism,
        CountSum.mergeCountSums,
      )
      CountSum.exchangeCountSums(combined, numPartitions, parallelism)

  /** One int key or value. The predicate is the function the planner stored. */
  private[columnar] def slot(step: Selection.Step): (Int, Int => Boolean) = step match
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
  ): ColumnBatch =
    CountSum.suppliedPartial(morsel.length) { sink =>
      IntPairProject.visitCountFiltered(
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
