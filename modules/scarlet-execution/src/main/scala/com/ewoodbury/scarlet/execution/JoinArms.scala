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
 * adds `build * 31 + probe` per present match. It does not write the match columns. [[inner]]
 * still does.
 *
 * [[broadcastSum]] is the int broadcast arm of the same probe sum. The left child is the build and
 * the right child is the probe. There is no exchange. The call is [[JoinProbeSum.sum]], not
 * [[ColumnPipeline.broadcastJoin]]. An over-budget build declines before the sides are
 * concatenated and before the probe runs.
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
   * Grouped sum of one broadcast join column. The left child is the build and the right child is
   * the probe, so [[JoinProbeSum.Measure.Left]] is the left column. Neither side is exchanged.
   * Match columns are not written. A build whose retained estimate exceeds the spill budget, when
   * spill is on, or the broadcast build budget, declines before the batches are concatenated.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def broadcastSum(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      measure: JoinProbeSum.Measure,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    val build = arm(left, leftOp, None, false, ordinals)
    val probe = arm(right, rightOp, None, false, ordinals)
    if (overBudget(liveRows(build))) then
      throw new ColumnarFallback(
        "a broadcast join sum over the build budget stays on the row path",
      )
    Vector(
      JoinProbeSum.sum(
        HashJoin.JoinSide(dense(build), key = 0, value = 1),
        HashJoin.JoinSide(dense(probe), key = 0, value = 1),
        measure,
      ),
    )

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

  /** Rows that would enter the build table. A selection counts survivors, not the source width. */
  private def liveRows(armed: Armed): Long = armed.input match
    case JoinInput.Dense(batches) => batches.iterator.map(_.length.toLong).sum
    case JoinInput.Selected(parts) => parts.iterator.map(_.count.toLong).sum

  /**
   * True when one build table for `rows` is over budget. Spill off ignores the spill budget. An
   * empty build is false: the probe publishes an empty sum and allocates nothing.
   */
  private def overBudget(rows: Long): Boolean =
    if (rows <= 0L) then false
    else if (rows > Int.MaxValue.toLong) then true
    else
      val estimate = HashJoin.retainedBuildBytes(rows.toInt)
      val conf = ScarletConf.get
      val spill = conf.spillEnabled && estimate > conf.spillBudgetBytes
      val table = estimate > conf.broadcastBuildBudgetBytes
      spill || table

  /** One batch for [[JoinProbeSum.sum]]. A selection is gathered here, not by an exchange. */
  private def dense(armed: Armed): ColumnBatch =
    val batches = armed.input match
      case JoinInput.Dense(found) => found
      case JoinInput.Selected(parts) => parts.map(Selection.materialize)
    batches match
      case Vector() => emptyPairs
      case Vector(only) => only
      case many => ColumnBatches.concat(many)

  private def emptyPairs: ColumnBatch =
    new ColumnBatch(
      Vector(LogicalType.Int32, LogicalType.Int32),
      Vector(
        Int32Column.of(Array.empty[Int], Validity.allValid(0), 0),
        Int32Column.of(Array.empty[Int], Validity.allValid(0), 0),
      ),
      0,
    )
