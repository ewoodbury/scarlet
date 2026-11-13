package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}

import ColumnarInterpreter.{Columns, interpretAt, longPairsOf, pairsOf}

/**
 * Joins that call [[ColumnPipeline]] and return [[ColumnarInterpreter.Columns]].
 *
 * Long inner, sort-merge inner, broadcast inner, string inner broadcast, and the outer joins live
 * here. Int and string shuffle-hash stay in [[JoinArms]] and [[StringJoinArms]]. The string-hash
 * case stays in [[ColumnarInterpreter]]. An outer arm forces `preserveAbsence` on and does not
 * read the flag the caller passed.
 */
private[execution] object PipelineJoinArms:

  def longInner(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    val leftBatches = longPairsOf(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence, ordinals),
    )
    val rightBatches = longPairsOf(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence, ordinals),
    )
    // A clear validity bit is a null value, not an absence bit. The matched row is kept.
    Columns.LongTriples(
      ColumnPipeline.longInnerJoin(
        leftBatches,
        rightBatches,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
      ),
    )

  def sortMergeInner(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    Columns.Triples(
      ColumnPipeline.sortMergeJoin(
        pairsOf(interpretAt(left, leftOp, selectedPartition, preserveAbsence, ordinals)),
        pairsOf(interpretAt(right, rightOp, selectedPartition, preserveAbsence, ordinals)),
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
      ),
    )

  def broadcastInner(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      buildSide: PhysicalOp.BuildSide,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    val leftBatches = pairsOf(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence, ordinals),
    )
    val rightBatches = pairsOf(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence, ordinals),
    )
    val (probe, build) = buildSide match
      case PhysicalOp.BuildSide.Right => (leftBatches, rightBatches)
      case PhysicalOp.BuildSide.Left => (rightBatches, leftBatches)
    val joined = ColumnPipeline.broadcastJoin(
      probe,
      build,
      ColumnarPlanner.parallelism,
      ScarletConf.get.broadcastBuildBudgetBytes,
    )
    // The kernel writes (key, probe value, build value). Building the left child makes that
    // (key, right, left), so the value columns are swapped back to (key, left, right).
    val ordered = buildSide match
      case PhysicalOp.BuildSide.Right => joined
      case PhysicalOp.BuildSide.Left => swapValueColumns(joined)
    Columns.Triples(ordered)

  def broadcastInnerLong(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      buildSide: PhysicalOp.BuildSide,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    val leftBatches = longPairsOf(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence, ordinals),
    )
    val rightBatches = longPairsOf(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence, ordinals),
    )
    val (probe, build) = buildSide match
      case PhysicalOp.BuildSide.Right => (leftBatches, rightBatches)
      case PhysicalOp.BuildSide.Left => (rightBatches, leftBatches)
    val joined =
      if (probe.isEmpty) then Vector.empty[ColumnBatch]
      else
        val rows = build.iterator.map(_.length.toLong).sum
        if (rows > 0L) then
          require(rows <= Int.MaxValue.toLong, s"build side has $rows rows")
          val count = rows.toInt
          val estimate = LongHashJoin.retainedBuildBytes(count)
          val budget = ScarletConf.get.broadcastBuildBudgetBytes
          if (estimate > budget) then
            throw new IllegalStateException(
              s"broadcast build table retains about $estimate bytes for $count rows, above the budget of $budget bytes",
            )
        val buildBatch = ColumnBatches.concat(build)
        val table = HashJoin.JoinSide(buildBatch, key = 0, value = 1)
        probe.map { batch =>
          LongHashJoin.innerInt64(table, HashJoin.JoinSide(batch, key = 0, value = 1))
        }
    // innerInt64 writes (key, build value, probe value). The int broadcast kernel writes
    // (key, probe value, build value), so this swap is the other way around: building the
    // right child publishes (key, right, left) and has to be swapped back to (key, left, right).
    val ordered = buildSide match
      case PhysicalOp.BuildSide.Right => swapValueColumns(joined)
      case PhysicalOp.BuildSide.Left => joined
    Columns.LongTriples(ordered)

  def broadcastInnerString(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      buildSide: PhysicalOp.BuildSide,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    val leftBatches = StringPairWalk.batches(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence, ordinals),
    )
    val rightBatches = StringPairWalk.batches(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence, ordinals),
    )
    val (probe, build) = buildSide match
      case PhysicalOp.BuildSide.Right => (leftBatches, rightBatches)
      case PhysicalOp.BuildSide.Left => (rightBatches, leftBatches)
    val joined =
      if (probe.isEmpty) then Vector.empty[ColumnBatch]
      else
        val buildBatch = ColumnBatches.concat(build)
        val rows = buildBatch.length
        if (rows > 0) then
          val estimate = StringHashJoinSpill.retainedBytes(
            rows,
            StringHashJoinSpill.dictionaryEntries(buildBatch),
            StringHashJoinSpill.dictionaryBytes(buildBatch),
          )
          val budget = ScarletConf.get.broadcastBuildBudgetBytes
          if (estimate > budget) then
            throw new IllegalStateException(
              s"broadcast build table retains about $estimate bytes for $rows rows, above the budget of $budget bytes",
            )
        probe.map(batch => StringHashJoin.joinBucket(buildBatch, batch))
    // joinBucket writes (key, build value, probe value). Building the right child publishes
    // (key, right, left), so the value columns are swapped back to (key, left, right).
    val ordered = buildSide match
      case PhysicalOp.BuildSide.Right => swapValueColumns(joined)
      case PhysicalOp.BuildSide.Left => joined
    Columns.StringTriples(ordered)

  def broadcastLeft(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // preserveAbsence keeps null keys. It is not the probe side. The right input is the build.
    val probe = pairsOf(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    val build = pairsOf(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    // Validity words are already on the columns. A null value is not a probe-side row with no
    // partner, and this path cannot represent one, so the kernel is not chosen.
    val valuesPresent =
      ColumnPipeline.valuesFullyPresent(probe) && ColumnPipeline.valuesFullyPresent(build)
    if (!valuesPresent) then
      throw new ColumnarFallback(
        "a null value on a left outer join stays on the row path",
      )
    // The kernel writes (key, probe value, build value). The probe side is the left input, so
    // the batch is already (key, left, right). The build value's absence bit is set and its
    // validity bit stays clear.
    val joined = ColumnPipeline.broadcastLeftOuterJoin(
      probe,
      build,
      ColumnarPlanner.parallelism,
      ScarletConf.get.broadcastBuildBudgetBytes,
    )
    Columns.Outer(joined, AbsentSide.Right)

  def hashLeft(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // preserveAbsence keeps null keys. The left input is the probe side. Its exchange carries a
    // null key. The right input is the build side. Its exchange drops a null key.
    val probe = pairsOf(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    val build = pairsOf(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    // A null value is a clear validity bit. The kernel copies it and writes
    // (key, probe value, build value). The probe side is the left input, so the batch is already
    // (key, left, right). The build value's absence bit is set only for a missing partner, and
    // its validity bit stays clear.
    val joined = ColumnPipeline.hashLeftOuterJoin(
      probe,
      build,
      ColumnarPlanner.shuffleWidth,
      ColumnarPlanner.parallelism,
      ColumnarPlanner.batchSize,
      ScarletConf.get.broadcastBuildBudgetBytes,
    )
    Columns.Outer(joined, AbsentSide.Right)

  def broadcastRight(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // preserveAbsence keeps null keys. The left input is the build. The right input is the
    // probe, so unmatched right rows are kept.
    val build = pairsOf(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    val probe = pairsOf(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    // Validity words are already on the columns. A null value is not absence, and this path
    // cannot represent one, so the kernel is not chosen.
    val valuesPresent =
      ColumnPipeline.valuesFullyPresent(probe) && ColumnPipeline.valuesFullyPresent(build)
    if (!valuesPresent) then
      throw new ColumnarFallback(
        "a null value on a right outer join stays on the row path",
      )
    rightOuter(probe, build) { (probeBatches, buildBatches) =>
      ColumnPipeline.broadcastLeftOuterJoin(
        probeBatches,
        buildBatches,
        ColumnarPlanner.parallelism,
        ScarletConf.get.broadcastBuildBudgetBytes,
      )
    }

  def hashRight(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // preserveAbsence keeps null keys. The right input is the probe side. Its exchange carries
    // a null key. The left input is the build side. Its exchange drops a null key.
    val build = pairsOf(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    val probe = pairsOf(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    // A null value is a clear validity bit. The kernel copies it and writes
    // (key, probe value, build value). The probe is the right input and the build is the left,
    // so the columns arrive as (right, left). The swap restores (key, left, right). Absence is
    // the left value's absence bit, set only for a missing partner. Its validity bit stays clear.
    rightOuter(probe, build) { (probeBatches, buildBatches) =>
      ColumnPipeline.hashLeftOuterJoin(
        probeBatches,
        buildBatches,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
        ScarletConf.get.broadcastBuildBudgetBytes,
      )
    }

  def broadcastFull(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    /* preserveAbsence keeps null keys. The left input is the probe. The right input is the build. */
    val probe = pairsOf(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    val build = pairsOf(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    fullOuter(probe, build) { (probeBatches, buildBatches) =>
      ColumnPipeline.broadcastFullOuterJoin(
        probeBatches,
        buildBatches,
        ColumnarPlanner.parallelism,
        ScarletConf.get.broadcastBuildBudgetBytes,
      )
    }

  def hashFull(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // preserveAbsence keeps null keys. Both exchanges carry a null key. The right input is the
    // build side. A null build key is carried outside the table.
    val probe = pairsOf(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    val build = pairsOf(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    fullOuter(probe, build) { (probeBatches, buildBatches) =>
      ColumnPipeline.hashFullOuterJoin(
        probeBatches,
        buildBatches,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
        ScarletConf.get.broadcastBuildBudgetBytes,
      )
    }

  def sortMergeLeft(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // preserveAbsence keeps null keys. The left exchange carries a null key. The right exchange
    // drops one. The kernel publishes (key, left, right).
    val probe = pairsOf(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    val build = pairsOf(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    val valuesPresent =
      ColumnPipeline.valuesFullyPresent(probe) && ColumnPipeline.valuesFullyPresent(build)
    if (!valuesPresent) then
      throw new ColumnarFallback(
        "a null value on a left outer join stays on the row path",
      )
    Columns.Outer(
      ColumnPipeline.sortMergeLeftOuterJoin(
        probe,
        build,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
      ),
      AbsentSide.Right,
    )

  def sortMergeRight(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // preserveAbsence keeps null keys. The kernel swaps the value columns back. Do not swap
    // again here.
    val leftBatches = pairsOf(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    val rightBatches =
      pairsOf(interpretAt(right, rightOp, selectedPartition, preserveAbsence = true, ordinals))
    val valuesPresent =
      ColumnPipeline.valuesFullyPresent(leftBatches) &&
        ColumnPipeline.valuesFullyPresent(rightBatches)
    if (!valuesPresent) then
      throw new ColumnarFallback(
        "a null value on a right outer join stays on the row path",
      )
    Columns.Outer(
      ColumnPipeline.sortMergeRightOuterJoin(
        leftBatches,
        rightBatches,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
      ),
      AbsentSide.Left,
    )

  def sortMergeFull(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // preserveAbsence keeps null keys. Both exchanges carry a null key.
    val probe = pairsOf(
      interpretAt(left, leftOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    val build = pairsOf(
      interpretAt(right, rightOp, selectedPartition, preserveAbsence = true, ordinals),
    )
    fullOuter(probe, build) { (probeBatches, buildBatches) =>
      ColumnPipeline.sortMergeFullOuterJoin(
        probeBatches,
        buildBatches,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
      )
    }

  /**
   * Run the left-outer kernel with `probe` as the right input and `build` as the left input, then
   * restore `(key, left, right)`. The kernel writes `(key, probe value, build value)`, so the
   * value columns arrive as `(right, left)`. The build value's absence bit moves with the left
   * column. A null value is a clear validity bit. Broadcast refuses one before calling this.
   */
  private def rightOuter(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
  )(join: (Vector[ColumnBatch], Vector[ColumnBatch]) => Vector[ColumnBatch]): Columns =
    Columns.Outer(swapValueColumns(join(probe, build)), AbsentSide.Left)

  /**
   * Run a full outer join. The kernel writes `(key, probe value, build value)`. The probe is the
   * left input, so the batch is already `(key, left, right)`. Either value's absence bit marks a
   * missing partner. A null value is not absence, and this path cannot represent one.
   */
  private def fullOuter(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
  )(join: (Vector[ColumnBatch], Vector[ColumnBatch]) => Vector[ColumnBatch]): Columns =
    val valuesPresent =
      ColumnPipeline.valuesFullyPresent(probe) && ColumnPipeline.valuesFullyPresent(build)
    if (!valuesPresent) then
      throw new ColumnarFallback(
        "a null value on a full outer join stays on the row path",
      )
    Columns.Outer(join(probe, build), AbsentSide.Both)

  /**
   * Kernel probe output is `(key, probe value, build value)`. Exchange the two value columns so a
   * left-side build publishes `(key, left, right)`.
   */
  private def swapValueColumns(batches: Vector[ColumnBatch]): Vector[ColumnBatch] =
    batches.map { batch =>
      batch.columns match
        case Seq(key, probeValue, buildValue) =>
          new ColumnBatch(
            Vector(key.logicalType, buildValue.logicalType, probeValue.logicalType),
            Vector(key, buildValue, probeValue),
            batch.length,
          )
        case other =>
          throw new IllegalArgumentException(
            s"join output has ${other.size} columns",
          )
    }
