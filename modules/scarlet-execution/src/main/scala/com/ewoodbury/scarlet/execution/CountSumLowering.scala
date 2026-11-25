package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, KernelChoice}

/**
 * Lowers int `countSumByKey`.
 *
 * The child is an exchange, and the kernel performs the shuffle. A null key is dropped. A null
 * value is counted and not added. The binder tag [[KernelChoice.CountSum]] builds
 * [[PhysicalOp.CountSum]]. Every other layout stays on the row path. Output kind is
 * [[PhysicalOp.Kind.CountSums]], schema `[Int32, Int64, Int64]` for key, count, and sum.
 */
private[execution] object CountSumLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] =
    ColumnBinding.asked(child, Operator.CountSumByKey) match
      case Some(KernelChoice.CountSum) =>
        Some(PhysicalOp.CountSum(PhysicalOp.Exchange(child)))
      case _ =>
        LoweringTrace.note("CountSumByKeyOp", child.kind, "CountSumLowering")
        None
