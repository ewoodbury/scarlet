package com.ewoodbury.scarlet.execution

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
 * The child is exchanged even when it is a join: the map may replace the join key. Each morsel is
 * summed with [[com.ewoodbury.scarlet.columnar.HashAggregate.sumInt64]] and the morsels of a batch
 * are folded with [[com.ewoodbury.scarlet.columnar.HashAggregate.mergeSumPartials]]. The reducer
 * has to be addition. Any other `(Long, Long) => Long` declines before a sum is published. A map
 * result that is not `(Int, Long)` declines first, with that name. The join under the project is
 * what runs, so the match columns are still written. An explicit int inner sort-merge or broadcast
 * has already been rewritten to that hash join.
 * [[com.ewoodbury.scarlet.execution.JoinProbeAggregate]] is a different plan and is not built
 * here.
 */
private[execution] object IntLongReduce:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      reduceFunc: Any,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    (source, child) match
      case (Plan.MapOp(input, mapper), PhysicalOp.Project(inputOp)) =>
        val absence = preserveAbsence || child.requestsTripleAbsence
        val triples = tripleBatches(
          interpretAt(input, inputOp, selectedPartition, absence, ordinals),
        )
        val adapted = TripleToIntLong.adapt(mapper)
        if (addsLongs(reduceFunc)) then
          Columns.Pairs(
            TripleIntLongSum.reduce(
              triples,
              ColumnarPlanner.shuffleWidth,
              ColumnarPlanner.parallelism,
              ColumnarPlanner.batchSize,
              adapted,
            ),
          )
        else
          TripleIntLongSum.projectAll(
            triples,
            ColumnarPlanner.parallelism,
            ColumnarPlanner.batchSize,
            adapted,
          )
          throw new ColumnarFallback("int-long reduceByKey is the sum")
      case _ =>
        throw new ColumnarFallback("int-long reduceByKey expected a triple project")

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
