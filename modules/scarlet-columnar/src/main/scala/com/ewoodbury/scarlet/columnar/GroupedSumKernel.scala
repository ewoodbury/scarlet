package com.ewoodbury.scarlet.columnar

/**
 * Grouped sum of int pairs over whole input batches.
 *
 * Each batch becomes ranges of `batchSize` rows. The key and value columns stay where they are; a
 * worker reads `[start, start + length)` and compacts one partial. One [[HashAggregate]] table per
 * worker is cleared after that partial and reused for the next range. The partials of one input
 * batch are merged with `sumInt64`, matching [[PartialCombine.mergeMorsels]]. The result is one
 * partial per input batch. [[ColumnPipeline]] exchanges those partials.
 *
 * An empty input returns an empty vector. `numPartitions` is checked here so a direct caller still
 * rejects a non-positive partition count. One scheduler task covers one range, so a morsel still
 * counts as one visit.
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
    merge(batches, keyOrdinal, valueOrdinal, numPartitions, parallelism, batchSize, shapeOf(steps))

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
    merge(
      batches,
      keyOrdinal,
      valueOrdinal,
      numPartitions,
      parallelism,
      batchSize,
      Filtered(keep, map),
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
    require(parallelism > 0, s"parallelism must be positive, got $parallelism")
    require(batchSize > 0, s"batch size must be positive, got $batchSize")
    accumulate(batches, keyOrdinal, valueOrdinal, parallelism, batchSize, shapeOf(steps))

  private sealed trait Shape

  private case object Plain extends Shape

  private final case class Filtered(
      keep: ColumnKernel.Int32Predicate,
      map: ColumnKernel.Int32Map,
  ) extends Shape

  private final case class Stepped(steps: Array[PairStep]) extends Shape

  /** Key column, value column, and the row range to scan. Neither column is copied. */
  private final case class Span(keys: Int32Column, values: Int32Column, start: Int, length: Int)

  private val mergeSum: ColumnBatch => ColumnBatch =
    batch => HashAggregate.sumInt64(batch, keyOrdinal = 0, valueOrdinal = 1)

  private object PassValue extends ColumnKernel.Int32Map:
    def apply(value: Int): Int = value

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
    val partials = MorselScheduler.map(groups.flatten, parallelism) { (_, span) =>
      scan(span, shape)
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

  private def scan(span: Span, shape: Shape): ColumnBatch =
    shape match
      case Plain =>
        HashAggregate.sumRange(span.keys, span.values, span.start, span.length)
      case Filtered(keep, map) =>
        HashAggregate.sumRange(span.keys, span.values, span.start, span.length, keep, map)
      case Stepped(steps) =>
        HashAggregate.sumRange(span.keys, span.values, span.start, span.length, steps)

  private def shapeOf(steps: Vector[PairStep]): Shape =
    if (steps.isEmpty) then Plain
    else
      valueKeeps(steps) match
        case Some(predicates) => Filtered(keepValues(predicates), PassValue)
        case None => Stepped(steps.toArray)

  /** `Some` when every step is a value filter. The predicates stay in source order. */
  private def valueKeeps(steps: Vector[PairStep]): Option[Vector[Int => Boolean]] =
    val seed: Option[Vector[Int => Boolean]] = Some(Vector.empty[Int => Boolean])
    steps.foldLeft(seed) { (acc, step) =>
      (acc, step) match
        case (Some(predicates), PairStep.KeepValue(predicate)) => Some(predicates :+ predicate)
        case _ => None
    }

  private def keepValues(predicates: Vector[Int => Boolean]): ColumnKernel.Int32Predicate =
    value => predicates.forall(predicate => predicate(value))
