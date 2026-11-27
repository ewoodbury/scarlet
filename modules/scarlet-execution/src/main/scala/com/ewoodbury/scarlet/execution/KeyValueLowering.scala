package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, KernelChoice}

/**
 * Lowers `keys` and `values`.
 *
 * Both ask [[ColumnBinding]] on the child layout. The tag for an accepted pair is
 * [[KernelChoice.Column]], and the physical node stays [[PhysicalOp.Column]]. `keys` of
 * `groupByKey` uses [[CountByKeyLowering]] as the typed witness and projects the key. That kernel
 * scans the exchange and does not build value lists. `keys` and `values` of one
 * [[PhysicalOp.MapKey]], or of one key or value filter on it, keep a null. A null input key stays
 * a clear bit and is not passed to the function or a key predicate. The value column is the
 * original value. A wider parent still declines.
 *
 * A grouped child is [[com.ewoodbury.scarlet.execution.schema.Layout.Grouped]], so the binder
 * refuses `values` of `groupByKey`. That projection stays on the row path and is not a columnar
 * project over the group. The kind check still refuses a grouped kind.
 * [[com.ewoodbury.scarlet.execution.schema.Describe.tag]] is not this decision.
 */
private[execution] object KeyValueLowering:

  /**
   * Int, long, and string `keys` of `groupByKey`. [[CountByKeyLowering]] is the typed witness. The
   * interpreter does not run the count kernel and does not build value lists. A long key column is
   * [[PhysicalOp.Kind.Longs]]. A string key column is [[PhysicalOp.Kind.Strings]].
   */
  def groupKeys(
      source: Plan[_],
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    lowerChild(source) match
      case Some(child) if tagged(child, Operator.Keys) && pairKind(child) =>
        CountByKeyLowering(child) match
          case Some(counted) => Some(PhysicalOp.Column(counted, 0))
          case None =>
            LoweringTrace.note("KeysOp", child.kind, "KeyValueLowering")
            None
      case Some(child) =>
        LoweringTrace.note("KeysOp", child.kind, "KeyValueLowering")
        None
      case None =>
        LoweringTrace.noteIfEmpty("KeysOp", "KeyValueLowering")
        None

  /** `keys` when `ordinal` is 0, `values` when `ordinal` is 1. Both use [[tagged]]. */
  def project(
      source: Plan[_],
      ordinal: Int,
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    val operator = operatorFor(ordinal)
    lowerChild(source) match
      case Some(child) if tagged(child, operator) && admits(child, ordinal) =>
        Some(PhysicalOp.Column(child, ordinal))
      case Some(child) =>
        LoweringTrace.note(node(ordinal), child.kind, "KeyValueLowering")
        None
      case None =>
        LoweringTrace.noteIfEmpty(node(ordinal), "KeyValueLowering")
        None

  /** Binder tag [[KernelChoice.Column]], or false when this projection refuses the layout. */
  private def tagged(child: PhysicalOp, operator: Operator): Boolean =
    ColumnBinding.asked(child, operator) match
      case Some(KernelChoice.Column) => true
      case _ => false

  /** Ordinal 0 asks [[Operator.Keys]]. Ordinal 1 asks [[Operator.Values]]. */
  private def operatorFor(ordinal: Int): Operator =
    if ordinal == 0 then Operator.Keys else Operator.Values

  private def node(ordinal: Int): String =
    if ordinal == 0 then "KeysOp" else "ValuesOp"

  /**
   * One [[PhysicalOp.MapKey]], or one slot above it, or an int, long, or string pair that does not
   * still carry a key map. A grouped child is not this shape.
   */
  private def admits(child: PhysicalOp, ordinal: Int): Boolean =
    if mapped(child, ordinal) then true
    else if MapIntKey.inTree(child) then false
    else pairKind(child)

  /** Ordinal 0 or 1 of one [[PhysicalOp.MapKey]] or one slot above it. Neither body is read. */
  private def mapped(child: PhysicalOp, ordinal: Int): Boolean =
    (ordinal == 0 || ordinal == 1) && MapIntKey.admitsColumn(child)

  private def pairKind(child: PhysicalOp): Boolean = child.kind match
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.StringPairs =>
      true
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.Strings | PhysicalOp.Kind.Bools |
        PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.StringTriples |
        PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
        PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter |
        PhysicalOp.Kind.CountSums | PhysicalOp.Kind.LongCountSums |
        PhysicalOp.Kind.StringCountSums | PhysicalOp.Kind.IntLongSums |
        PhysicalOp.Kind.GroupedIntLongs | PhysicalOp.Kind.IntLongTriples =>
      false
