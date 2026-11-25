package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, KernelChoice}

/**
 * `mapValues` from an int value to a long value asks [[ColumnBinding]] on the child layout.
 *
 * The tag that comes back is [[KernelChoice.IntLongMapValue]]. A binder [[scala.None]] is
 * [[scala.None]]. [[ColumnarPlanner]] asks only when the child kind is [[PhysicalOp.Kind.Pairs]]
 * and [[FunctionWitness.declaredIntToLong]] is true. [[MapIntKey.inTree]] still refuses before
 * this ask. A string pair and a long pair fail the kind check and do not ask.
 *
 * [[Layout.AssumedIntLong]] is [[scala.None]] here. That layout is not this map and it is not
 * [[KernelChoice.MapValue]].
 */
private[execution] object IntLongMapBinding:

  /**
   * One [[PhysicalOp.IntLongMapValue]]. The tag has to be [[KernelChoice.IntLongMapValue]].
   *
   * A kind match that builds the class without [[ColumnBinding.choose]] leaves
   * [[ColumnBinding.chooseCallsForTest]] at zero. A kind match that calls [[choose]] and ignores
   * [[scala.None]] still builds for a [[PhysicalOp.Kind.Pairs]] child whose layout is
   * [[Layout.AssumedIntLong]]. This returns [[scala.None]] for that child, and the counter moves.
   */
  def value(child: PhysicalOp): Option[PhysicalOp] =
    val asked = ColumnBinding.asked(child, Operator.IntLongMapValue)
    if (asked.contains(KernelChoice.IntLongMapValue)) then Some(PhysicalOp.IntLongMapValue(child))
    else None
