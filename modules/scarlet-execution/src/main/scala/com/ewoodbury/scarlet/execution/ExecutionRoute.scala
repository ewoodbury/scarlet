package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.KernelChoice

/**
 * Runtime route for a hash aggregate.
 *
 * The arm is [[AggregateBinding.choiceFor]] on the child that was lowered. It is not
 * [[Describe.tag]], and it is not [[PhysicalSchema.isIntLongAggregate]]. A node whose layout is
 * `[Int32, Int64]` takes [[Arm.IntLongReduce]] only when the binder's tag is
 * [[KernelChoice.IntLongProjectSum]]. [[KernelChoice.JoinProbeSum]] is a different physical node
 * and stays on its own decoder.
 *
 * [[scala.None]] from the binder is [[Arm.IntHashAggregate]]. That decline must not fall through
 * to the schema test. A tag this hash node does not run fails the action.
 */
private[execution] object ExecutionRoute:

  /** Which hash-aggregate kernel runs. */
  enum Arm:
    case IntLongReduce
    case IntHashAggregate

  /**
   * Arm for `reduceByKey` of `child`.
   *
   * `child` is the lowered child under the exchange, not the [[PhysicalOp.HashAggregate]] node.
   */
  def reduceArm(source: Plan[_], child: PhysicalOp, reducer: Any): Arm =
    AggregateBinding.choiceFor(
      source,
      child,
      Operator.ReduceByKey,
      AggregateBinding.witnessOf(reducer),
    ) match
      case Some(KernelChoice.IntLongProjectSum) => Arm.IntLongReduce
      case Some(KernelChoice.IntHashAggregate) => Arm.IntHashAggregate
      case Some(tag) =>
        throw new IllegalStateException(s"hash aggregate bound $tag")
      case None => Arm.IntHashAggregate

  /**
   * True when collect should decode this root as an int-long sum.
   *
   * Missing `plan`, a non-reduce root, and a hash aggregate the binder did not tag
   * [[KernelChoice.IntLongProjectSum]] are false. [[PhysicalOp.JoinProbeSum]] is false.
   */
  def decodesAsIntLongSum(plan: Option[Plan[_]], op: PhysicalOp): Boolean =
    (plan, op) match
      case (
            Some(Plan.ReduceByKeyOp(source, reducer)),
            PhysicalOp.HashAggregate(PhysicalOp.Exchange(child)),
          ) =>
        reduceArm(source, child, reducer) match
          case Arm.IntLongReduce => true
          case Arm.IntHashAggregate => false
      case _ =>
        false
