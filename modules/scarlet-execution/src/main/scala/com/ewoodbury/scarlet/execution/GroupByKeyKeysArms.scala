package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnBatch, GroupByKeyKeys}
import com.ewoodbury.scarlet.core.Plan

/**
 * Int `keys` of `groupByKey`.
 *
 * A key, value, or row filter scatters survivor ids, including a stack of one, and only the key
 * column is kept. The dense path reads the key column of the child. Neither path calls
 * [[com.ewoodbury.scarlet.columnar.GroupByKey]] or [[CountByKeyArms]], so no value list is built.
 */
private[execution] object GroupByKeyKeysArms:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    child.kind match
      case PhysicalOp.Kind.Pairs => ()
      case _ =>
        throw new IllegalArgumentException("groupByKey keys expects int pairs")
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    FilterChain.selected(
      source,
      child,
      selectedPartition,
      preserveAbsence,
      ordinals,
    ) match
      case Some(parts) =>
        GroupByKeyKeys.int32Selected(parts, width, workers)
      case None =>
        val pairs = ColumnarInterpreter.pairsOf(
          ColumnarInterpreter.interpretAt(
            source,
            child,
            selectedPartition,
            preserveAbsence,
            ordinals,
          ),
        )
        GroupByKeyKeys.int32(pairs, width, workers)
