package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.GroupByKey
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns

/**
 * Int, long, and string `groupByKey`.
 *
 * A key, value, or row filter on an int pair, a long pair, or a string pair scatters survivor ids,
 * including a stack of one. Long-pair `groupByKey` does not read that index, so a whole-row filter
 * and a key or value filter both still reach the dense exchange. A null value on a present key
 * stays in the group. Spill still refuses that null before a file is opened. The `None` path is
 * the only one that interprets a child that did not publish a
 * [[com.ewoodbury.scarlet.columnar.Selected]].
 */
private[execution] object GroupByArms:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    ExecutionRoute.requirePair(child, "groupByKey") match
      case ExecutionRoute.PairArm.IntPairs =>
        FilterChain.selected(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
        ) match
          case Some(parts) =>
            Columns.GroupedInts(GroupByKey.int32Selected(parts, width, workers))
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
            Columns.GroupedInts(GroupByKey.int32(pairs, width, workers))
      case ExecutionRoute.PairArm.StringPairs =>
        FilterChain.selected(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
        ) match
          case Some(parts) =>
            Columns.GroupedStrings(GroupByKey.stringsSelected(parts, width, workers))
          case None =>
            val input = ColumnarInterpreter.interpretAt(
              source,
              child,
              selectedPartition,
              preserveAbsence,
              ordinals,
            )
            val batches = StringPairWalk.batches(input)
            Columns.GroupedStrings(GroupByKey.strings(batches, width, workers))
      case ExecutionRoute.PairArm.LongPairs =>
        val input = ColumnarInterpreter.interpretAt(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
        )
        Columns.GroupedLongs(
          GroupByKey.int64(ColumnarInterpreter.longPairsOf(input), width, workers),
        )
