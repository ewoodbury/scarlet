package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt}

/**
 * `countByKey` of a triple project whose schema is `[Int32, Int64]`.
 *
 * The child is exchanged even when it is a join: the map may replace the join key. Each morsel is
 * projected with [[TripleToIntLong.project]], then [[HashCount.byKey]] counts ordinal 0. Ordinal 1
 * is not read. The function body is not inspected. A result that is not `(Int, Long)` throws
 * [[ColumnarFallback]] from the project, before a count is published. The join under the project
 * is what runs, so the match columns are still written.
 * [[com.ewoodbury.scarlet.execution.JoinProbeAggregate]] is a different plan and is not built
 * here.
 */
private[execution] object IntLongCount:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
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
        Columns.Pairs(
          HashCount.byKey(
            project(triples, adapted),
            ColumnarPlanner.shuffleWidth,
            ColumnarPlanner.parallelism,
            ColumnarPlanner.batchSize,
          ),
        )
      case _ =>
        throw new ColumnarFallback("int-long countByKey expected a triple project")

  /** One projected batch per morsel. [[HashCount.byKey]] drops the value column. */
  private def project(
      batches: Vector[ColumnBatch],
      mapper: TripleToIntLong.TripleMap,
  ): Vector[ColumnBatch] =
    val groups = batches.map(batch => Morsels.of(batch, ColumnarPlanner.batchSize))
    MorselScheduler.map(groups.flatten, ColumnarPlanner.parallelism) { (_, morsel) =>
      TripleToIntLong.project(morsel.batch, morsel.start, morsel.length, mapper)
    }

  private def tripleBatches(columns: Columns): Vector[ColumnBatch] =
    columns match
      case Columns.Triples(parts) => parts
      case _ => throw new ColumnarFallback("int-long countByKey expected triples")
