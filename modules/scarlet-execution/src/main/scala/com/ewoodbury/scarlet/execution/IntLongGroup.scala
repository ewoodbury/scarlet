package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{GroupByKey, IntLongMap}
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns

/**
 * `groupByKey` of an int-to-long `mapValues`.
 *
 * The scan under the map is interpreted, then [[IntLongMap.widen]] runs the function once on each
 * present value. Interpreting the map node as well would run it twice. A null input value stays
 * null and the function is not called.
 * [[com.ewoodbury.scarlet.columnar.HashExchange.partitionAll]] then drops a null key and keeps
 * that null value. The int key table builds the lists. This is not `reduceByKey`, and this group
 * does not spill.
 */
private[execution] object IntLongGroup:

  def columns(
      source: Plan[_],
      child: PhysicalOp,
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns = (source, child) match
    case (Plan.MapValuesOp(input, mapper), PhysicalOp.IntLongMapValue(under)) =>
      val pairs = ColumnarInterpreter.pairsOf(
        ColumnarInterpreter.interpretAt(input, under, None, preserveAbsence, ordinals),
      )
      val fn = longValue(mapper)
      val widened = pairs.map(batch => IntLongMap.widen(batch, 0, batch.length, fn))
      Columns.GroupedIntLongs(
        GroupByKey.intLong(
          widened,
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
        ),
      )
    case _ =>
      throw new IllegalStateException("int-long groupByKey expected mapValues to long")

  /** Named erasure boundary. [[FunctionWitness.declaredIntToLong]] checked `apply$mcJI$sp`. */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def longValue(mapper: Any): Int => Long =
    mapper.asInstanceOf[Int => Long]
