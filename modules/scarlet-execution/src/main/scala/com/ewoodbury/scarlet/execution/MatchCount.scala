package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.ColumnReduce
import com.ewoodbury.scarlet.core.Plan

/**
 * Scalar count of a finished plan.
 *
 * An int shuffle-hash inner join sums chain lengths and does not write the match columns. Every
 * other plan is interpreted, and the count is the lengths of the batches it publishes.
 */
private[execution] object MatchCount:

  def of(plan: Plan[_], op: PhysicalOp): Long =
    joinMatches(plan, op).getOrElse(published(plan, op))

  private def joinMatches(plan: Plan[_], op: PhysicalOp): Option[Long] =
    (plan, op) match
      case (
            Plan.JoinOp(left, right, strategy),
            PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
          ) if JoinLowering.acceptsShuffleHash(strategy) =>
        Some(
          JoinArms.count(
            left,
            right,
            leftOp,
            rightOp,
            None,
            op.requestsTripleAbsence,
            ColumnUse.ordinals(plan, op),
          ),
        )
      case _ =>
        None

  private def published(plan: Plan[_], op: PhysicalOp): Long =
    ColumnReduce.count(
      ColumnarInterpreter.batchesOf(ColumnarInterpreter.interpret(plan, op)),
    )
