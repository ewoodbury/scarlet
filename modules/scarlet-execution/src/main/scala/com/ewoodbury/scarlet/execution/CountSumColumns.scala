package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{LongCountSum, StringCountSum}
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, longPairsOf}

/**
 * Long and string `countSum` after the child is published.
 *
 * The int path stays in [[CountSumArms]], including the owned fold. These two read the batches the
 * child already published. A null key is dropped. A null value is counted and not added.
 */
private[execution] object CountSumColumns:

  def longs(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    Columns.LongCountSums(
      LongCountSum.byKey(
        longPairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)),
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
      ),
    )

  def strings(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    Columns.StringCountSums(
      StringCountSum.byKey(
        StringPairWalk.batches(
          interpretAt(source, child, selectedPartition, preserveAbsence, ordinals),
        ),
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
      ),
    )
