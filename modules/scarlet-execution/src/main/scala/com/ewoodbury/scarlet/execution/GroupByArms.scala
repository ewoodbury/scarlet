package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.GroupByKey
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns

/**
 * Int and string `groupByKey`.
 *
 * Int pairs with a two-filter stack scatter survivor ids. Every other int-pair child, and every
 * string-pair child, stays on the dense exchange. The `None` path is the only one that interprets
 * that child.
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
        FilterChain.selectedPairs(
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
      case other =>
        throw new IllegalStateException(
          s"groupByKey column is $other, expected int or string pairs",
        )
