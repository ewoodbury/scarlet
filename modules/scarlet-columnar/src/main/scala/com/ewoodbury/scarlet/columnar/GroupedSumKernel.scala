package com.ewoodbury.scarlet.columnar

/**
 * Grouped sum of int pairs over whole input batches.
 *
 * Each batch becomes ranges of `batchSize` rows. The key and value columns stay where they are; a
 * worker reads `[start, start + length)` and compacts one partial. One [[HashAggregate]] table per
 * worker is cleared before the next range and dropped when the action returns. The partials of one
 * input batch are merged with `sumInt64`, matching [[PartialCombine.mergeMorsels]]. The result is
 * one partial per input batch. [[ColumnPipeline]] exchanges those partials.
 *
 * An empty input returns an empty vector. `numPartitions` is checked here so a direct caller still
 * rejects a non-positive partition count. One scheduler task covers one range, so a morsel still
 * counts as one visit.
 *
 * A plain range whose keys are present and lie in `[0, DenseSum.directLimit)` is indexed by the
 * key. Any other plain or keep/map range whose present keys fit in [[DenseSum.cap]] is summed by
 * direct index after a min/max pass. Both emit ascending keys. Every other range, including a
 * [[PairStep]] chain that can rewrite the key, uses the hash table and emits first-seen order.
 * Grouped sums are compared as maps. Classification reads only `[start, start + length)` and skips
 * null keys.
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
    finishing {
      require(parallelism > 0, s"parallelism must be positive, got $parallelism")
      require(batchSize > 0, s"batch size must be positive, got $batchSize")
      accumulate(batches, keyOrdinal, valueOrdinal, parallelism, batchSize, shapeOf(steps))
    }

  private sealed trait Shape

  private case object Plain extends Shape

  private final case class Filtered(
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ) extends Shape

  private final case class Stepped(steps: Array[PairStep]) extends Shape

  /**
   * Key column, value column, and the row range to scan. Neither column is copied. `keysPresent`
   * and `valuesPresent` are the whole column, so every row of this range is present when the flag
   * is set.
   */
  private final case class Span(
      keys: Int32Column,
      values: Int32Column,
      start: Int,
      length: Int,
      keysPresent: Boolean,
      valuesPresent: Boolean,
  )

  private val mergeSum: ColumnBatch => ColumnBatch =
    batch => HashAggregate.sumInt64(batch, keyOrdinal = 0, valueOrdinal = 1)

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
        accumulate(batches, keyOrdinal, valueOrdinal, parallelism, batchSize, shape)
      PartialCombine.mergeMorsels(sizes, partials, parallelism, mergeSum)

  private def accumulate(
      batches: Vector[ColumnBatch],
      keyOrdinal: Int,
      valueOrdinal: Int,
      parallelism: Int,
      batchSize: Int,
      shape: Shape,
  ): (Vector[Int], Vector[ColumnBatch]) =
    val groups = batches.map(batch => spans(batch, keyOrdinal, valueOrdinal, batchSize))
    val denseCap = DenseSum.cap(parallelism)
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
    val keysPresent = Validity.livePresent(keys.validity, rows)
    val valuesPresent = Validity.livePresent(values.validity, rows)
    if (rows == 0) then Vector(Span(keys, values, 0, 0, keysPresent, valuesPresent))
    else
      val pieces = (rows + batchSize - 1) / batchSize
      Vector.tabulate(pieces) { piece =>
        val start = piece * batchSize
        val length = math.min(batchSize, rows - start)
        Span(keys, values, start, length, keysPresent, valuesPresent)
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
   * Direct index when the present keys in this range fit. `Stepped` does not call this. An empty
   * key set still publishes an empty partial through the hash table, which clears that table.
   */
  private def scanPlain(span: Span, denseCap: Int): ColumnBatch =
    if (span.length == 0) then emptyRange(span)
    else
      val direct =
        if (span.keysPresent && span.valuesPresent) then
          HashAggregate.tryDirectSum(
            span.keys,
            span.values,
            span.start,
            span.length,
            denseCap,
          )
        else None
      direct match
        case Some(batch) => batch
        case None => scanPlainClassified(span, denseCap)

  private def scanPlainClassified(span: Span, denseCap: Int): ColumnBatch =
    val fit =
      if (span.keysPresent) then
        DenseSum.classifyPresent(span.keys, span.start, span.length, denseCap)
      else DenseSum.classify(span.keys, span.start, span.length, denseCap)
    fit match
      case DenseSum.Window(minKey, keyRange) =>
        HashAggregate.indexSumRange(
          span.keys,
          span.values,
          span.start,
          span.length,
          minKey,
          keyRange,
          columnsPresent = span.keysPresent && span.valuesPresent,
        )
      case DenseSum.Empty => emptyRange(span)
      case DenseSum.Wide =>
        HashAggregate.sumRange(span.keys, span.values, span.start, span.length)

  private def scanFiltered(
      span: Span,
      denseCap: Int,
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ): ColumnBatch =
    if (span.length == 0) then emptyRange(span)
    else
      val fit =
        if (span.keysPresent) then
          DenseSum.classifyPresent(span.keys, span.start, span.length, denseCap)
        else DenseSum.classify(span.keys, span.start, span.length, denseCap)
      fit match
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
