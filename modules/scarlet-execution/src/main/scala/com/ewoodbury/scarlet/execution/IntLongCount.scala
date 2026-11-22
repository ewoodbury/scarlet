package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt}

/**
 * `countByKey` of a triple project whose schema is `[Int32, Int64]`.
 *
 * An int inner hash join under that project counts each probe hit under the projected key.
 * [[com.ewoodbury.scarlet.columnar.JoinWrite]] is not called, and [[HashCount.byKey]] is not
 * called. Partials are finished by [[HashCount.exchange]], the same exchange [[HashCount.byKey]]
 * uses after its partial combine. The map may replace the join key, so that second exchange stays.
 * The function body is not inspected. Any other project still interprets the child, projects each
 * morsel with [[TripleToIntLong.project]], then [[HashCount.byKey]] counts ordinal 0. A join on
 * that path still writes its match columns. A result that is not `(Int, Long)` throws
 * [[ColumnarFallback]] from the offer or the project, before a count is published.
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
        val adapted = TripleToIntLong.adapt(mapper)
        probedJoin(input, inputOp) match
          case Some((left, right, leftOp, rightOp)) =>
            Columns.Pairs(
              JoinArms.projectCount(left, right, leftOp, rightOp, absence, ordinals, adapted),
            )
          case None =>
            Columns.Pairs(
              HashCount.byKey(
                project(
                  tripleBatches(
                    interpretAt(input, inputOp, selectedPartition, absence, ordinals),
                  ),
                  adapted,
                ),
                ColumnarPlanner.shuffleWidth,
                ColumnarPlanner.parallelism,
                ColumnarPlanner.batchSize,
              ),
            )
      case _ =>
        throw new ColumnarFallback("int-long countByKey expected a triple project")

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
