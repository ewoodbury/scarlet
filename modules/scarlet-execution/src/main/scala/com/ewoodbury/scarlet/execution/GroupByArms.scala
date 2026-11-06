package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.GroupByKey
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns

/**
 * Int, long, and string `groupByKey`.
 *
 * A two-filter key or value stack scatters survivor ids. A whole-row string stack is not lowered;
 * a single filter, and every other child, stays on the dense exchange. Long pairs have no
 * selection variant, so a whole-row filter and a key or value filter both reach the dense
 * exchange. The `None` path is the only one that interprets a child that did not publish a
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
    child.kind match
      case PhysicalOp.Kind.Pairs =>
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
      case PhysicalOp.Kind.StringPairs =>
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
            Columns.GroupedStrings(
              GroupByKey.strings(StringPairWalk.batches(input), width, workers),
            )
      case PhysicalOp.Kind.LongPairs =>
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
      case other =>
        throw new IllegalStateException(
          s"groupByKey column is $other, expected int, long, or string pairs",
        )
