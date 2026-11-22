package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}

import ColumnPipeline.JoinInput

/**
 * Int inner shuffle-hash join.
 *
 * A slot-filter stack on an owned int-pair scan is copied by [[IntPairFilterEncode]]: one pass,
 * survivors only, no survivor index. The build reads that encoded key column. A boxed source, a
 * null, or any other stack still scatters survivor ids, including a stack of one. An int-pair row
 * filter does the same. The exchange reads those rows and does not slice them. A child with no
 * selection steps stays on the sliced dense exchange. [[FilterChain.selected]] interprets a stack
 * once. The `None` path is the only one that interprets that child.
 *
 * A null value is a clear bit in the output, including a selection. It is not an absence bit. A
 * null key is dropped by the exchange. A selected side skips the slice; both dense sides use the
 * caller's flag and slice. An owned filter copy is already dense and is not sliced. [[count]] of
 * two owned int-pair sides multiplies per-key counts and does not copy values. Any other count
 * uses that same exchange and sums chain lengths. [[sumValuePair]] uses that same exchange and
 * adds `build * 31 + probe` per present match. It does not write the match columns. [[projectSum]]
 * uses that same exchange, projects each hit, and sums under the projected key. [[projectCount]]
 * uses that same exchange, offers each projected key, and counts. [[inner]] still writes the match
 * columns.
 */
private[execution] object JoinArms:

  def inner(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    val build = arm(left, leftOp, selectedPartition, preserveAbsence, ordinals)
    val probe = arm(right, rightOp, selectedPartition, preserveAbsence, ordinals)
    if (build.slice && probe.slice) then
      ColumnPipeline.innerJoin(build.batches, probe.batches, width, workers, batchSize)
    else ColumnPipeline.innerJoin(build.input, probe.input, width, workers)

  /**
   * Rows [[inner]] would publish. The match columns are not written.
   *
   * Owned int pairs, filtered or not, multiply key counts. A boxed source, a null, or a slice of
   * one partition still exchanges and sums chain lengths.
   */
  def count(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Long =
    val multiplied =
      if (selectedPartition.nonEmpty) then None
      else IntPairFilterEncode.tryCount(left, leftOp, right, rightOp, ordinals)
    multiplied match
      case Some(total) => total
      case None =>
        val width = ColumnarPlanner.shuffleWidth
        val workers = ColumnarPlanner.parallelism
        val batchSize = ColumnarPlanner.batchSize
        val build = arm(left, leftOp, selectedPartition, preserveAbsence, ordinals)
        val probe = arm(right, rightOp, selectedPartition, preserveAbsence, ordinals)
        if (build.slice && probe.slice) then
          ColumnPipeline.countInner(build.batches, probe.batches, width, workers, batchSize)
        else ColumnPipeline.countInner(build.input, probe.input, width, workers)

  /**
   * `build * 31 + probe` over the rows [[inner]] would publish. The match columns are not written.
   * An empty side is zero. A null value on either side is skipped. The exchange is the non-product
   * arm of [[count]], including a selected side and a slice.
   */
  def sumValuePair(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Long =
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    val build = arm(left, leftOp, selectedPartition, preserveAbsence, ordinals)
    val probe = arm(right, rightOp, selectedPartition, preserveAbsence, ordinals)
    if (build.slice && probe.slice) then
      ColumnPipeline.sumValuePair(build.batches, probe.batches, width, workers, batchSize)
    else ColumnPipeline.sumValuePair(build.input, probe.input, width, workers)

  /**
   * Grouped sum of a triple project over the exchange [[inner]] uses. Match columns are not
   * written. An over-budget build bucket declines inside [[ProjectedJoinSum.reduce]].
   */
  def projectSum(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      mapper: TripleToIntLong.TripleMap,
  ): Vector[ColumnBatch] =
    val (build, probe, slice) = sides(left, right, leftOp, rightOp, preserveAbsence, ordinals)
    ProjectedJoinSum.reduce(
      build,
      probe,
      ColumnarPlanner.shuffleWidth,
      ColumnarPlanner.parallelism,
      ColumnarPlanner.batchSize,
      slice,
      mapper,
    )

  /**
   * Grouped count of a triple project over the exchange [[inner]] uses. Match columns are not
   * written. Each probe hit offers its projected key into [[HashCount.partialKeys]]. Those
   * partials are exchanged the way [[HashCount.byKey]] exchanges them. An over-budget build bucket
   * declines inside [[ProjectedJoinCount.count]].
   */
  def projectCount(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      mapper: TripleToIntLong.TripleMap,
  ): Vector[ColumnBatch] =
    val (build, probe, slice) = sides(left, right, leftOp, rightOp, preserveAbsence, ordinals)
    ProjectedJoinCount.count(
      build,
      probe,
      ColumnarPlanner.shuffleWidth,
      ColumnarPlanner.parallelism,
      ColumnarPlanner.batchSize,
      slice,
      mapper,
    )

  /** Hits [[projectSum]] would add. Nothing is published. */
  def projectVisit(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      mapper: TripleToIntLong.TripleMap,
  ): Unit =
    val (build, probe, slice) = sides(left, right, leftOp, rightOp, preserveAbsence, ordinals)
    ProjectedJoinSum.visit(
      build,
      probe,
      ColumnarPlanner.shuffleWidth,
      ColumnarPlanner.parallelism,
      ColumnarPlanner.batchSize,
      slice,
      mapper,
    )

  /**
   * Grouped sum of one join column. The exchange matches [[inner]]. Match columns are not written.
   * An over-budget build bucket declines inside [[ColumnPipeline.sumInner]].
   */
  def sum(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      measure: JoinProbeSum.Measure,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    val build = arm(left, leftOp, None, false, ordinals)
    val probe = arm(right, rightOp, None, false, ordinals)
    if (build.slice && probe.slice) then
      ColumnPipeline.sumInner(build.batches, probe.batches, width, workers, batchSize, measure)
    else ColumnPipeline.sumInner(build.input, probe.input, width, workers, measure)

  /**
   * Broadcast grouped sum. The left input is the build and the right input is the probe, so
   * [[JoinProbeSum.Measure.Left]] is the left column. There is no exchange.
   * [[JoinProbeSum.sumBroadcast]] checks `broadcastBuildBudgetBytes` before it concatenates or
   * calls [[JoinProbeSum.sum]]. [[ColumnPipeline.broadcastJoin]] is not called, and the match
   * columns are not written.
   */
  def broadcastSum(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      measure: JoinProbeSum.Measure,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    val build = dense(arm(left, leftOp, None, false, ordinals))
    val probe = dense(arm(right, rightOp, None, false, ordinals))
    Vector(
      JoinProbeSum.sumBroadcast(
        build,
        probe,
        ScarletConf.get.broadcastBuildBudgetBytes,
        measure,
      ),
    )

  private def sides(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): (JoinInput, JoinInput, Boolean) =
    val build = arm(left, leftOp, None, preserveAbsence, ordinals)
    val probe = arm(right, rightOp, None, preserveAbsence, ordinals)
    (build.input, probe.input, build.slice && probe.slice)

  private def dense(armed: Armed): Vector[ColumnBatch] = armed.input match
    case JoinInput.Dense(batches) => batches
    case JoinInput.Selected(parts) => parts.map(Selection.materialize)

  private final case class Armed(
      input: JoinInput,
      batches: Vector[ColumnBatch],
      slice: Boolean,
  )

  private def arm(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Armed =
    val copied =
      if (selectedPartition.nonEmpty) then None
      else IntPairFilterEncode.tryBatches(plan, op, ordinals)
    copied match
      case Some(batches) => Armed(JoinInput.Dense(batches), batches, false)
      case None =>
        selection(plan, op, selectedPartition, ordinals) match
          case Some(parts) =>
            Armed(JoinInput.Selected(parts), Vector.empty[ColumnBatch], false)
          case None =>
            val batches = pairs(plan, op, selectedPartition, preserveAbsence, ordinals)
            Armed(JoinInput.Dense(batches), batches, true)

  private def selection(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      ordinals: Set[Int],
  ): Option[Vector[Selected]] =
    FilterChain.selected(plan, op, selectedPartition, preserveAbsence = true, ordinals)

  private def pairs(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    ColumnarInterpreter.pairsOf(
      ColumnarInterpreter.interpretAt(plan, op, selectedPartition, preserveAbsence, ordinals),
    )
