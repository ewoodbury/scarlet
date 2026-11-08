package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnarFallback, HashCount}
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, pairsOf}

/**
 * Int `countByKey`.
 *
 * A two-filter key or value stack partial-counts survivor ids. The exchange sees dense `(key,
 * count)` partials. A single filter, and every other child, stays on the dense count. The `None`
 * path is the only one that interprets that child. The value column is not read.
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
        val counted = FilterChain.selected(
          source,
          child,
          selectedPartition,
          preserveAbsence,
          ordinals,
        ) match
          case Some(parts) => HashCount.byKeySelected(parts, width, workers, batchSize)
          case None =>
            // A boxed pair source whose first row witnesses `Kind.Pairs` can still hold a null
            // component later. The pair encode unboxes it and throws before the kernel runs, so
            // turn that into the documented fallback rather than a failed action.
            val batches =
              try pairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals))
              catch
                case _: NullPointerException =>
                  throw new ColumnarFallback("countByKey source has a null pair component")
            HashCount.byKey(batches, width, workers, batchSize)
        Columns.Pairs(counted)
      case other =>
        throw new IllegalStateException(s"countByKey column is $other, expected int pairs")
