package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, KernelChoice}

/**
 * Same-type `mapValues` asks [[ColumnBinding]] on the child layout.
 *
 * The tag that comes back is [[KernelChoice.MapValue]], the class this parent already builds. A
 * binder [[scala.None]] is [[scala.None]]. [[ColumnarPlanner]] asks only after the witness:
 * `Int => Int` on an int or string pair, or `Long => Long` on a long pair. `Int => Long` fails
 * that witness and does not ask here. [[IntLongMapBinding]] owns that map. [[MapIntKey.inTree]]
 * still refuses before this ask.
 *
 * [[Layout.AssumedIntLong]] is [[scala.None]] here. That layout is not [[KernelChoice.MapValue]]
 * and it is not [[KernelChoice.IntLongProjectSum]].
 */
private[execution] object MapValueBinding:

  /**
   * One [[PhysicalOp.MapValue]]. The tag has to be [[KernelChoice.MapValue]].
   *
   * A kind match that builds the class anyway fails a child whose layout is
   * [[Layout.AssumedIntLong]]. A kind match that never asks leaves
   * [[ColumnBinding.chooseCallsForTest]] at zero.
   */
  def value(child: PhysicalOp): Option[PhysicalOp] =
    if (ColumnBinding.asked(child, Operator.MapValues).contains(KernelChoice.MapValue)) then
      Some(PhysicalOp.MapValue(child))
    else None
