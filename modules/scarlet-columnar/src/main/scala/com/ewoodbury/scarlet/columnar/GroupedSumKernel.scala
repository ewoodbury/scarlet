package com.ewoodbury.scarlet.columnar

/**
 * Grouped sum of int pairs over whole input batches.
 *
 * Each batch becomes ranges of `batchSize` rows. The key and value columns stay where they are; a
 * worker reads `[start, start + length)` and compacts one partial. One [[HashAggregate]] table per
 * worker is cleared before the next range and dropped when the action returns. The partials of one
 * input batch are folded by [[HashAggregate.mergeSumPartials]] into one partial. A batch that is
 * already a single range keeps that partial. [[ColumnPipeline]] exchanges the partials.
 *
 * An empty input returns an empty vector. `numPartitions` is checked here so a direct caller still
 * rejects a non-positive partition count. One scheduler task covers one range, so a morsel still
 * counts as one visit.
 *
 * A plain range is classified in one pass. That pass reads presence and the key bounds together.
 * The window is used when its span fits [[DenseSum.cap]] and is at most [[DenseSum.DensityFactor]]
 * times the row count. The table has one slot per key in the window. A later morsel that still
 * fits that zero-origin window is summed in one pass. A sparser range, a cap of 0, and a
 * [[PairStep]] chain that can rewrite the key use the hash table. Dense partials emit ascending
 * keys. Hash partials emit first-seen order. Grouped sums are compared as maps. Classification
 * reads only `[start, start + length)` and skips null keys. The span is
 * `max.toLong - min.toLong + 1`.
 */
private[scarlet] object GroupedSumKernel:

  def run(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      valueOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      steps: Vector[PairStep],
  ): Vector[ColumnBatch] =
    finishing(
      merge(
        batches,
        keyOrdinal,
        valueOrdinal,
        numPartitions,
        parallelism,
        batchSize,
        shapeOf(steps),
      ),
    )

  /** `keep` and `map` run inside the partial scan. A null value is dropped before `keep`. */
  def run(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      valueOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): Vector[ColumnBatch] =
    finishing(
      merge(
        batches,
        keyOrdinal,
        valueOrdinal,
        numPartitions,
        parallelism,
        batchSize,
        Filtered(keep, map),
      ),
    )

  /** One partial per range, in input order. The first vector counts the ranges of each batch. */
  def partialMorsels(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      valueOrdinal: Int,
      parallelism: Int,
      batchSize: Int,
      steps: Vector[PairStep],
  ): (Vector[Int], Vector[ColumnBatch]) =
    partialMorsels(
      batches,
      keyOrdinal,
      valueOrdinal,
      parallelism,
      batchSize,
      steps,
      DenseSum.cap(parallelism),
    )

  /**
   * [[partialMorsels]] with an explicit window cap. Tests pass a small cap so the fit-or-hash
   * boundary does not allocate the production tables.
   */
  private[columnar] def partialMorsels(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      valueOrdinal: Int,
      parallelism: Int,
      batchSize: Int,
      steps: Vector[PairStep],
      denseCap: Int,
  ): (Vector[Int], Vector[ColumnBatch]) =
    finishing {
      require(parallelism > 0, s"parallelism must be positive, got $parallelism")
      require(batchSize > 0, s"batch size must be positive, got $batchSize")
      require(denseCap >= 0, s"dense cap must be non-negative, got $denseCap")
      accumulate(
        batches,
        keyOrdinal,
        valueOrdinal,
        parallelism,
        batchSize,
        shapeOf(steps),
        denseCap,
      )
    }

  private sealed trait Shape

  private case object Plain extends Shape

  private final case class Filtered(
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ) extends Shape

  private final case class Stepped(steps: Array[PairStep]) extends Shape

  /** Key column, value column, and the row range to scan. Neither column is copied. */
  private final case class Span(keys: Int32Column, values: Int32Column, start: Int, length: Int)

  /**
   * One partial per input batch. A group of one partial stays as it is. Wider groups fold through
   * [[HashAggregate.mergeSumPartials]].
   */
  private[scarlet] def mergePartials(
      sizes: Vector[Int],
      partials: Vector[ColumnBatch],
      parallelism: Int,
  ): Vector[ColumnBatch] =
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    val groups = PartialCombine.groupPartials(sizes, partials)
    MorselScheduler.map(groups, parallelism)((_, group) => combine(group))

  private def combine(group: Vector[ColumnBatch]): ColumnBatch =
    group match
      case Vector(only) => only
      case many => HashAggregate.mergeSumPartials(many, keyOrdinal = 0, valueOrdinal = 1)

  /**
   * Runs `body` and then drops this thread's worker table. Ranges inside `body` still share it.
   * `MorselScheduler` runs on the calling thread when `parallelism` is 1 or there is one morsel,
   * and that thread outlives the action. The other worker threads are started for the call and
   * exit with it.
   */
  private def finishing[A](body: => A): A =
    try body
    finally HashAggregate.releaseSumWorkers()

  private def merge(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      valueOrdinal: Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
      shape: Shape,
  ): Vector[ColumnBatch] =
    require(numPartitions > 0, s"numPartitions must be positive, got $numPartitions")
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    if (batches.isEmpty) then Vector.empty
    else
      val (sizes, partials) =
        accumulate(
          batches,
          keyOrdinal,
          valueOrdinal,
          parallelism,
          batchSize,
          shape,
          DenseSum.cap(parallelism),
        )
      mergePartials(sizes, partials, parallelism)

  private def accumulate(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      valueOrdinal: Int,
      parallelism: Int,
      batchSize: Int,
      shape: Shape,
      denseCap: Int,
  ): (Vector[Int], Vector[ColumnBatch]) =
    val groups = batches.map(batch => spans(batch, keyOrdinal, valueOrdinal, batchSize))
    val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, span) =>
      scan(span, shape, denseCap)
    }
    (groups.map(_.length), partials)

  private def spans(
      batch: ColumnBatch,
      keyOrdinal: Int,
      valueOrdinal: Int,
      batchSize: Int,
  ): Vector[Span] =
    val keys = ColumnHash.int32At(batch, keyOrdinal)
    val values = ColumnHash.int32At(batch, valueOrdinal)
    require(keys.length == values.length, "key and value columns differ in length")
    val rows = keys.length
    if (rows == 0) then Vector(Span(keys, values, 0, 0))
    else
      val pieces = (rows + batchSize - 1) / batchSize
      Vector.tabulate(pieces) { piece =>
        val start = piece * batchSize
        val length = math.min(batchSize, rows - start)
        Span(keys, values, start, length)
      }

  private def scan(span: Span, shape: Shape, denseCap: Int): ColumnBatch =
    shape match
      case Stepped(steps) =>
        // Steps can rewrite the key. Classifying the raw key would index the wrong group.
        HashAggregate.sumRange(span.keys, span.values, span.start, span.length, steps)
      case Plain =>
        scanPlain(span, denseCap)
      case Filtered(keep, map) =>
        scanFiltered(span, denseCap, keep, map)

  /**
   * Direct index when the present keys in this range are dense. `Stepped` does not call this. A
   * cap of 0 hashes without reading the keys. An empty key set still publishes an empty partial
   * through the hash table, which clears that table.
   */
  private def scanPlain(span: Span, denseCap: Int): ColumnBatch =
    if (span.length == 0) then emptyRange(span)
    else if (denseCap <= 0) then hashPlain(span)
    else
      HashAggregate.tryReuseDirect(
        span.keys,
        span.values,
        span.start,
        span.length,
        denseCap,
      ) match
        case Some(batch) => batch
        case None =>
          surveyPlain(span, denseCap)

  private def surveyPlain(span: Span, denseCap: Int): ColumnBatch =
    val surveyed =
      DenseSum.survey(span.keys, span.values, span.start, span.length, denseCap)
    surveyed.fit match
      case DenseSum.Window(minKey, keyRange) =>
        HashAggregate.indexSumRange(
          span.keys,
          span.values,
          span.start,
          span.length,
          minKey,
          keyRange,
          columnsPresent = surveyed.columnsPresent,
        )
      case DenseSum.Empty => emptyRange(span)
      case DenseSum.Wide => hashPlain(span)

  private def hashPlain(span: Span): ColumnBatch =
    HashAggregate.sumRange(span.keys, span.values, span.start, span.length)

  private def scanFiltered(
      span: Span,
      denseCap: Int,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    if (span.length == 0) then emptyRange(span)
    else if (denseCap <= 0) then
      HashAggregate.sumRange(span.keys, span.values, span.start, span.length, keep, map)
    else
      DenseSum.classify(span.keys, span.start, span.length, denseCap) match
        case DenseSum.Window(minKey, keyRange) =>
          HashAggregate.indexSumRange(
            span.keys,
            span.values,
            span.start,
            span.length,
            minKey,
            keyRange,
            keep,
            map,
          )
        case DenseSum.Empty => emptyRange(span)
        case DenseSum.Wide =>
          HashAggregate.sumRange(span.keys, span.values, span.start, span.length, keep, map)

  private def emptyRange(span: Span): ColumnBatch =
    HashAggregate.sumRange(span.keys, span.values, span.start, 0)

  private def shapeOf(steps: Vector[PairStep]): Shape =
    if (steps.isEmpty) then Plain
    else
      // ColumnPipeline.sumByKey rewrites an all-KeepValue chain to the keep/map overload before
      // run(steps), so a production sum does not reach this branch. partialMorsels and a direct
      // run(steps) still scan that chain with keep and map. Both are eligible for the direct
      // index. Anything else, including MapPair, stays Stepped and uses the hash table.
      PairStep.valueKeeps(steps) match
        case Some(predicates) => Filtered(PairStep.keepValues(predicates), PairStep.PassValue)
        case None => Stepped(steps.toArray)
