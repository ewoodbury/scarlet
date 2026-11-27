package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{
  ColumnBatch,
  ColumnarFallback,
  DistinctKernel,
  Float32Distinct,
}
import com.ewoodbury.scarlet.core.Plan

/**
 * Runs a lowered `distinct`.
 *
 * A raw long or double scan keeps a null element as a clear bit. Every other child, including a
 * filter above that scan, is encoded by [[ColumnarInterpreter.interpretAt]]. The kernel then
 * hash-scatters and dedupes. A selected partition is refused: distinct is not
 * partition-preserving.
 */
private[execution] object DistinctColumns:

  def run(
      source: Plan[_],
      child: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): ColumnarInterpreter.Columns =
    if (selectedPartition.nonEmpty) then
      throw new ColumnarFallback("a partition selector cannot run a distinct")
    val batches = input(source, child, preserveAbsence, ordinals)
    val width = ColumnarPlanner.shuffleWidth
    ExecutionRoute.distinctArm(child) match
      case ExecutionRoute.DistinctArm.Ints =>
        ColumnarInterpreter.Columns.Ints(DistinctKernel.ints(batches, width))
      case ExecutionRoute.DistinctArm.Longs =>
        ColumnarInterpreter.Columns.Longs(DistinctKernel.longs(batches, width))
      case ExecutionRoute.DistinctArm.Doubles =>
        ColumnarInterpreter.Columns.Floats(DistinctKernel.doubles(batches, width))
      case ExecutionRoute.DistinctArm.Float32s =>
        ColumnarInterpreter.Columns.Floats(Float32Distinct(batches, width))
      case ExecutionRoute.DistinctArm.Strings =>
        ColumnarInterpreter.Columns.Strings(DistinctKernel.strings(batches, width))

  private def input(
      source: Plan[_],
      child: PhysicalOp,
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    (source, child) match
      case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Longs, _)) =>
        SourceEncode.encodeNullableLongs(partitions)
      case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Floats, _)) =>
        SourceEncode.encodeNullableDoubles(partitions)
      case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.Float32s, _)) =>
        SourceEncode.encodeNullableFloat32s(partitions)
      case _ =>
        ColumnarInterpreter.batchesOf(
          ColumnarInterpreter.interpretAt(source, child, None, preserveAbsence, ordinals),
        )
