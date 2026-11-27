package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, KernelChoice}

/**
 * Lowers `countSumByKey`, `countSumByLongKey`, `countSumByStringKey`, and `countSumByIntLongKey`.
 *
 * The child is an exchange, and the kernel performs the shuffle. A null key is dropped. A null
 * value is counted and not added. The binder tag [[KernelChoice.CountSum]] builds
 * [[PhysicalOp.CountSum]] for int pairs, [[PhysicalOp.LongCountSum]] for long pairs, and
 * [[PhysicalOp.StringCountSum]] for string pairs. A widened `(Int, Long)` map, including one
 * filter or one `Long => Long` map above it, is [[PhysicalOp.IntLongCountSum]]. Every other layout
 * stays on the row path.
 */
private[execution] object CountSumLowering:

  def apply(child: PhysicalOp): Option[PhysicalOp] =
    if (IntLongParents.widened(child)) then
      Some(PhysicalOp.IntLongCountSum(PhysicalOp.Exchange(child)))
    else counted(child)

  private def counted(child: PhysicalOp): Option[PhysicalOp] =
    ColumnBinding.asked(child, Operator.CountSumByKey) match
      case Some(KernelChoice.CountSum) =>
        val exchanged = PhysicalOp.Exchange(child)
        child.kind match
          case PhysicalOp.Kind.Pairs => Some(PhysicalOp.CountSum(exchanged))
          case PhysicalOp.Kind.LongPairs => Some(PhysicalOp.LongCountSum(exchanged))
          case PhysicalOp.Kind.StringPairs => Some(PhysicalOp.StringCountSum(exchanged))
          case _ =>
            LoweringTrace.note("CountSumByKeyOp", child.kind, "CountSumLowering")
            None
      case _ =>
        LoweringTrace.note("CountSumByKeyOp", child.kind, "CountSumLowering")
        None
