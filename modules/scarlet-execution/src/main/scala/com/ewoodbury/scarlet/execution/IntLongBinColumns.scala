package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.IntLongBinReduce
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, pairsOf}

/**
 * Non-adding `reduceByKey` of an int key and a long value.
 *
 * The cast is the erasure boundary. [[AggregateBinding]] already required `(Long, Long) => Long`.
 * Addition does not reach this runner.
 */
private[execution] object IntLongBinColumns:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      reduceFunc: Any,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    // Named erasure boundary. The witness already required `(Long, Long) => Long`.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val fn = reduceFunc.asInstanceOf[(Long, Long) => Long]
    val op = new IntLongBinReduce.LongBinOp:
      def apply(left: Long, right: Long): Long = fn(left, right)
    Columns.Pairs(
      IntLongBinReduce.byKey(
        pairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)),
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
        op,
      ),
    )
