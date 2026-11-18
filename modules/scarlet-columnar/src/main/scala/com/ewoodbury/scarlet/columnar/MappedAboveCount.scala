package com.ewoodbury.scarlet.columnar

/**
 * `countByKey` after one int slot above `mapIntKey`.
 *
 * `under` may be empty and stays on the source columns. `above` runs after `mapKey`: a key slot
 * sees the mapped key and is not called on a null, and a value slot sees the original value. The
 * mapped pair batch is not built.
 */
private[scarlet] object MappedAboveCount:

  def count(
      batches: Vector[ColumnBatch],
      under: List[Selection.Step],
      above: Selection.Step,
      mapKey: Int => Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    FoldedCount.requirePositive(numPartitions, parallelism, batchSize)
    if (!admits(under, above)) then None
    else
      FoldedCount.countedMapped(
        batches,
        under,
        mapKey,
        numPartitions,
        parallelism,
        batchSize,
        Some(above),
      )

  /** [[count]] for fully present owned arrays. */
  def countOwned(
      parts: Vector[(Array[Int], Array[Int])],
      under: List[Selection.Step],
      above: Selection.Step,
      mapKey: Int => Int,
      numPartitions: Int,
      parallelism: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    count(
      FoldedCount.presentParts(parts),
      under,
      above,
      mapKey,
      numPartitions,
      parallelism,
      batchSize,
    )

  /** Under-map slots may be empty. The slot above the map is one int key or value. */
  private def admits(under: Seq[Selection.Step], above: Selection.Step): Boolean =
    FoldedCount.admitsUnmapped(under) && (above match
      case Selection.Step.Int32(column, _) => column == 0 || column == 1
      case Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) | Selection.Step.Bool(_, _) |
          Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) | Selection.Step.LongPair(_) |
          Selection.Step.StringPair(_) | Selection.Step.IntTriple(_, _) =>
        false)
