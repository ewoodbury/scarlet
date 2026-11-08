package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{
  ColumnarFallback,
  HashCount,
  LongHashCount,
  StringHashCount,
}
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, longPairsOf, pairsOf}

/**
 * Int, long, and string `countByKey`.
 *
 * A pair stack whose selection steps are defined partial-counts survivor ids, including one row,
 * key, or value filter. The exchange sees dense `(key, count)` partials. A map, a scan, and a
 * non-pair child stay on the dense count. The `None` path is the only one that interprets that
 * child. The value column is not read. A long count publishes [[Columns.LongCounts]]. A string
 * count publishes [[Columns.StringPairs]]. The stored count is an `Int`. A null string key is
 * dropped and `""` is kept.
 */
private[execution] object CountByKeyArms:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    child.kind match
      case PhysicalOp.Kind.Pairs =>
        // A boxed pair source whose first row witnesses `Kind.Pairs` can still hold a null
        // component later. The pair encode unboxes it. A selection encodes inside
        // `FilterChain.selected`, and the dense path encodes here, so both throws become the
        // documented fallback rather than a failed action.
        val selectedParts =
          try
            FilterChain.selected(
              source,
              child,
              selectedPartition,
              preserveAbsence,
              ordinals,
            )
          catch
            case _: NullPointerException =>
              throw new ColumnarFallback("countByKey source has a null pair component")
        val counted = selectedParts match
          case Some(parts) => HashCount.byKeySelected(parts, width, workers, batchSize)
          case None =>
            val batches =
              try pairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals))
              catch
                case _: NullPointerException =>
                  throw new ColumnarFallback("countByKey source has a null pair component")
            HashCount.byKey(batches, width, workers, batchSize)
        Columns.Pairs(counted)
      case PhysicalOp.Kind.LongPairs =>
        val counted = FilterChain.selected(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
        ) match
          case Some(parts) => LongHashCount.byKeySelected(parts, width, workers, batchSize)
          case None =>
            val batches =
              longPairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals))
            LongHashCount.byKey(batches, width, workers, batchSize)
        Columns.LongCounts(counted)
      case PhysicalOp.Kind.StringPairs =>
        // A null int is not unboxed: string-pair encode reads productElement. This arm does not
        // turn that into a fallback. The value column is dropped before the count.
        val counted = FilterChain.selected(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
        ) match
          case Some(parts) => StringHashCount.byKeySelected(parts, width, workers, batchSize)
          case None =>
            val batches = StringPairWalk.batches(
              interpretAt(source, child, selectedPartition, preserveAbsence, ordinals),
            )
            StringHashCount.byKey(batches, width, workers, batchSize)
        Columns.StringPairs(counted)
      case other =>
        throw new IllegalStateException(
          s"countByKey column is $other, expected int, long, or string pairs",
        )
