package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicInteger

import com.ewoodbury.scarlet.columnar.{
  ColumnBatch,
  ColumnarFallback,
  TripleIntLongSum,
  TripleToIntLong,
}
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt}

/**
 * `reduceByKey` of a triple project whose schema is `[Int32, Int64]`.
 *
 * An int inner hash join under that project sums each probe hit under the projected key.
 * [[com.ewoodbury.scarlet.columnar.JoinWrite]] is not called. Partials are finished by
 * [[com.ewoodbury.scarlet.columnar.TripleIntLongSum.exchange]], the same exchange
 * [[com.ewoodbury.scarlet.columnar.TripleIntLongSum.reduce]] uses, which calls
 * [[com.ewoodbury.scarlet.columnar.HashAggregate.sumInt64]]. The map may replace the join key, so
 * that second exchange stays. Any other project still interprets the child, and a join there still
 * writes its match columns. Each of those morsels is summed with `sumInt64` and folded with
 * [[com.ewoodbury.scarlet.columnar.HashAggregate.mergeSumPartials]].
 *
 * The reducer has to be addition. Any other `(Long, Long) => Long` declines before a sum is
 * published, after the map has run. A map result that is not `(Int, Long)` declines first, with
 * that name. [[com.ewoodbury.scarlet.execution.JoinProbeAggregate]] is a different plan and is not
 * built here.
 */
private[execution] object IntLongReduce:

  private val columnsCalls = new AtomicInteger

  /** [[columns]] entries since the last [[resetColumnsForTest]]. */
  private[scarlet] def columnsCallsForTest: Int = columnsCalls.get

  /** Clears [[columnsCallsForTest]]. */
  private[scarlet] def resetColumnsForTest(): Unit =
    columnsCalls.set(0)

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      reduceFunc: Any,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    val _ = columnsCalls.incrementAndGet()
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    (source, child) match
      case (Plan.MapOp(input, mapper), PhysicalOp.Project(inputOp)) =>
        val absence = preserveAbsence || child.requestsTripleAbsence
        val adapted = TripleToIntLong.adapt(mapper)
        probedJoin(input, inputOp) match
          case Some((left, right, leftOp, rightOp)) if addsLongs(reduceFunc) =>
            Columns.Pairs(
              JoinArms.projectSum(left, right, leftOp, rightOp, absence, ordinals, adapted),
            )
          case Some((left, right, leftOp, rightOp)) =>
            JoinArms.projectVisit(left, right, leftOp, rightOp, absence, ordinals, adapted)
            throw new ColumnarFallback("int-long reduceByKey is the sum")
          case None if addsLongs(reduceFunc) =>
            Columns.Pairs(
              TripleIntLongSum.reduce(
                triplesOf(input, inputOp, selectedPartition, absence, ordinals),
                ColumnarPlanner.shuffleWidth,
                ColumnarPlanner.parallelism,
                ColumnarPlanner.batchSize,
                adapted,
              ),
            )
          case None =>
            TripleIntLongSum.projectAll(
              triplesOf(input, inputOp, selectedPartition, absence, ordinals),
              ColumnarPlanner.parallelism,
              ColumnarPlanner.batchSize,
              adapted,
            )
            throw new ColumnarFallback("int-long reduceByKey is the sum")
      case _ =>
        throw new ColumnarFallback("int-long reduceByKey expected a triple project")

  /** Int inner hash join of two exchanges. Sort-merge and broadcast are already rewritten. */
  private def probedJoin(
      input: Plan[_],
      inputOp: PhysicalOp,
  ): Option[(Plan[_], Plan[_], PhysicalOp, PhysicalOp)] =
    input match
      case Plan.JoinOp(left, right, strategy)
          if JoinLowering.acceptsShuffleHash(strategy) ||
            JoinLowering.hintedInnerHashed(strategy) =>
        inputOp match
          case PhysicalOp.HashJoin(
                PhysicalOp.Exchange(leftOp),
                PhysicalOp.Exchange(rightOp),
              ) =>
            Some((left, right, leftOp, rightOp))
          case _ => None
      case _ => None

  private def triplesOf(
      input: Plan[_],
      inputOp: PhysicalOp,
      selectedPartition: Option[Int],
      absence: Boolean,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    tripleBatches(interpretAt(input, inputOp, selectedPartition, absence, ordinals))

  private def tripleBatches(columns: Columns): Vector[ColumnBatch] =
    columns match
      case Columns.Triples(parts) => parts
      case _ => throw new ColumnarFallback("int-long reduceByKey expected triples")

  /**
   * Addition, checked by calling the function. The body is not read. `max` and `min` miss one of
   * these points and stay on the row path. A reducer that is not `(Long, Long) => Long` is not
   * addition; the caller then runs the map so a non-`(Int, Long)` cell can decline by name.
   */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Equals"))
  private def addsLongs(reduceFunc: Any): Boolean =
    try
      val fn = reduceFunc.asInstanceOf[(Long, Long) => Long]
      fn(1L, 2L) == 3L && fn(5L, 0L) == 5L && fn(-2L, 2L) == 0L
    catch case _: ClassCastException => false
