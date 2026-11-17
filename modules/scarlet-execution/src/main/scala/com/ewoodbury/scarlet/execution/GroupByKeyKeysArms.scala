package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{GroupByKeyKeys, Selected}
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns

/**
 * Int, long, and string `keys` of `groupByKey`.
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
  ): Columns =
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    FilterChain.selected(
      source,
      child,
      selectedPartition,
      preserveAbsence,
      ordinals,
    ) match
      case Some(parts) => selectedColumns(child, parts, width, workers)
      case None =>
        denseColumns(source, child, selectedPartition, preserveAbsence, ordinals, width, workers)

  private def selectedColumns(
      child: PhysicalOp,
      parts: Vector[Selected],
      width: Int,
      workers: Int,
  ): Columns = child.kind match
    case PhysicalOp.Kind.Pairs =>
      Columns.Ints(GroupByKeyKeys.int32Selected(parts, width, workers))
    case PhysicalOp.Kind.LongPairs =>
      Columns.Longs(GroupByKeyKeys.int64Selected(parts, width, workers))
    case PhysicalOp.Kind.StringPairs =>
      Columns.Strings(GroupByKeyKeys.stringsSelected(parts, width, workers))
    case other => refuse(other)

  private def denseColumns(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
  ): Columns =
    val input = ColumnarInterpreter.interpretAt(
      source,
      child,
      selectedPartition,
      preserveAbsence,
      ordinals,
    )
    child.kind match
      case PhysicalOp.Kind.Pairs =>
        Columns.Ints(
          GroupByKeyKeys.int32(ColumnarInterpreter.pairsOf(input), width, workers),
        )
      case PhysicalOp.Kind.LongPairs =>
        Columns.Longs(
          GroupByKeyKeys.int64(ColumnarInterpreter.longPairsOf(input), width, workers),
        )
      case PhysicalOp.Kind.StringPairs =>
        Columns.Strings(
          GroupByKeyKeys.strings(StringPairWalk.batches(input), width, workers),
        )
      case other => refuse(other)

  private def refuse(kind: PhysicalOp.Kind): Nothing =
    throw new IllegalArgumentException(
      s"groupByKey keys expects int, long, or string pairs, found $kind",
    )
