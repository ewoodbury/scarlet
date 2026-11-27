package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.IntExtremum
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, pairsOf}

/**
 * Int min or max after the child is published.
 *
 * A null key is dropped. A null value is skipped. A key whose values are all null emits no row.
 * Partial mins or maxes are exchanged and aggregated again, because the hash table does not merge
 * those measures itself.
 */
private[execution] object ExtremumColumns:

  def accepts(plan: Plan[_], op: PhysicalOp): Boolean = (plan, op) match
    case (
          Plan.ExtremumByKeyOp(_, _),
          PhysicalOp.IntExtremum(PhysicalOp.Exchange(_), _),
        ) =>
      true
    case _ => false

  def dispatch(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns = (plan, op) match
    case (
          Plan.ExtremumByKeyOp(source, minimum),
          PhysicalOp.IntExtremum(PhysicalOp.Exchange(child), _),
        ) =>
      require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
      Columns.Pairs(
        IntExtremum.byKey(
          pairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)),
          minimum,
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
        ),
      )
    case _ =>
      throw new IllegalStateException("extremum dispatch expected an int extremum")
