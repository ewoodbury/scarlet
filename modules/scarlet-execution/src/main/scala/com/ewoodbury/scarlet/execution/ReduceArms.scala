package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.StringHashReduce
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns

/**
 * String `reduceByKey`.
 *
 * A two-filter key or value stack partial-reduces survivor ids. The exchange sees dense partials.
 * A single filter, a whole-row string filter, and every other child stay on the dense reduce. The
 * `None` path is the only one that interprets that child.
 */
private[execution] object ReduceArms:

  def stringByKey[V](
      source: Plan[_],
      child: PhysicalOp,
      reduceFunc: (V, V) => V,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    // Named erasure boundary. Grouped string-pair reduction declares `(Int, Int) => Int`.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val op = reduceFunc.asInstanceOf[(Int, Int) => Int]
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    FilterChain.selected(
      source,
      child,
      selectedPartition,
      preserveAbsence,
      ordinals,
    ) match
      case Some(parts) =>
        Columns.StringPairs(StringHashReduce.byKeySelected(parts, width, workers, batchSize, op))
      case None =>
        val batches = StringPairWalk.batches(
          ColumnarInterpreter.interpretAt(
            source,
            child,
            selectedPartition,
            preserveAbsence,
            ordinals,
          ),
        )
        Columns.StringPairs(StringHashReduce.byKey(batches, width, workers, batchSize, op))
