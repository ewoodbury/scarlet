package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, KernelChoice}

/**
 * Lowers `minByKey` and `maxByKey` for `(Int, Int)`.
 *
 * The child is an exchange, and the kernel performs the shuffle. A null key is dropped. A null
 * value is skipped. A key whose values are all null emits no row. The binder tag
 * [[KernelChoice.IntExtremum]] builds [[PhysicalOp.IntExtremum]]. Every other layout stays on the
 * row path.
 */
private[execution] object ExtremumLowering:

  def apply(child: PhysicalOp, minimum: Boolean): Option[PhysicalOp] =
    (ColumnBinding.asked(child, Operator.ExtremumByKey), child.kind) match
      case (Some(KernelChoice.IntExtremum), PhysicalOp.Kind.Pairs) =>
        Some(PhysicalOp.IntExtremum(PhysicalOp.Exchange(child), minimum))
      case _ =>
        LoweringTrace.note("ExtremumByKeyOp", child.kind, "ExtremumLowering")
        None
