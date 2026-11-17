package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, pairsOf}

/**
 * Int `countSumByKey`.
 *
 * A slot-filter stack partial-aggregates survivor ids, including a stack of one. A scan and any
 * other pair child are interpreted, then aggregated. The exchange sees dense partials of
 * `(key, count, sum)`. A null key is dropped. A null value is counted and not added.
 */
private[execution] object CountSumArms:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    child.kind match
      case PhysicalOp.Kind.Pairs =>
        val aggregated = FilterChain.selected(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
        ) match
          case Some(parts) => CountSum.byKeySelected(parts, width, workers, batchSize)
          case None =>
            CountSum.byKey(
              pairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)),
              width,
              workers,
              batchSize,
            )
        Columns.CountSums(aggregated)
      case other =>
        throw new IllegalStateException(
          s"countSumByKey column is $other, expected int pairs",
        )
