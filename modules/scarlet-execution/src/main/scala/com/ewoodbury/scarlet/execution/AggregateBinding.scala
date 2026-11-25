package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.Selection
import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.*
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.{Operator, ReducerWitness}

/**
 * `countByKey`, `reduceByKey`, and `countSumByKey` ask [[ColumnBinding]] after the child is
 * lowered.
 *
 * A slot filter is peeled only when [[PredicatePlan.selectionSteps]] returns steps. A filter that
 * does not compile stays on today's lowering when that lowering exists, and is [[scala.None]] when
 * it does not. The binder is not asked about the child under that filter, so a kernel is not bound
 * for a predicate it will not run.
 *
 * When the binder's tag is the tag [[Describe.tag]] records for today's root, the result is that
 * root. [[scala.None]] from the binder is [[scala.None]]. A different tag is [[scala.None]] too:
 * the plan does not keep today's root because the binder disagreed. A projected `(Int32, Int64)`
 * whose reducer does not add longs is that [[scala.None]], and the row path runs the whole plan.
 * When today's lowering returns [[scala.None]], [[AggregateAdmission]] may build a root for a slot
 * stack over one map. Every other empty plan stays empty.
 *
 * `countByKey` of an int inner join asks [[ColumnBinding]] on the join's [[Bound]]. The three
 * columns are `(key, left, right)`. [[JoinAggregateBinding]] builds
 * [[KernelChoice.CountThroughProbe]]. A project above that join is [[Layout.AssumedIntLong]], not
 * the join. [[JoinProbeAggregate]] stays in [[ColumnarPlanner]] ahead of [[reduce]].
 */
private[execution] object AggregateBinding:

  def count(
      source: Plan[_],
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    kept(source, lowerChild, Operator.CountByKey, ReducerWitness.NotApplicable)(
      CountByKeyLowering.apply,
    )

  /**
   * Today's `reduceByKey` after [[JoinProbeAggregate]] has declined.
   *
   * `reducer` is the witness input. It is called only to tell long addition from any other
   * function, the same three points the int-long kernel uses. A cast that fails is not addition.
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
    ),
  )
  def reduce(
      source: Plan[_],
      reducer: Any,
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    kept(source, lowerChild, Operator.ReduceByKey, witness(reducer))(ReduceByKeyLowering.apply)

  def countSum(
      source: Plan[_],
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    kept(source, lowerChild, Operator.CountSumByKey, ReducerWitness.NotApplicable)(
      CountSumLowering.apply,
    )

  /**
   * Tag [[ColumnBinding]] returns for `source` after the selection peel.
   *
   * [[scala.None]] means there is nothing to bind: the child did not lower, or a slot filter did
   * not compile. This does not build a [[PhysicalOp]].
   */
  private[execution] def choiceFor(
      source: Plan[_],
      child: PhysicalOp,
      operator: Operator,
      reducer: ReducerWitness,
  ): Option[KernelChoice] =
    bindable(source, child).flatMap { peeled =>
      ask(peeled.op, operator, reducer, peeled.selection)
    }

  /** Tag for an already lowered child. Does not build a [[PhysicalOp]]. */
  private[execution] def choiceOf(
      op: PhysicalOp,
      operator: Operator,
      reducer: ReducerWitness,
  ): Option[KernelChoice] = ask(op, operator, reducer, Vector.empty)

  private def kept(
      source: Plan[_],
      lowerChild: Plan[_] => Option[PhysicalOp],
      operator: Operator,
      reducer: ReducerWitness,
  )(todayOf: PhysicalOp => Option[PhysicalOp]): Option[PhysicalOp] =
    val today = lowerChild(source) match
      case None => None
      case Some(child) =>
        val existing = todayOf(child)
        bindable(source, child) match
          case None => existing
          case Some(peeled) =>
            reconcile(ask(peeled.op, operator, reducer, peeled.selection), existing)
    today.orElse(AggregateAdmission.admit(source, lowerChild, operator, reducer))

  /**
   * Today's root when [[Describe.tag]] is the binder's tag.
   *
   * [[scala.None]] from the binder is [[scala.None]]. A tag that is not today's tag is
   * [[scala.None]] as well, including a binder tag with no root from today's lowering. The plan
   * does not fall back to today's root. [[AggregateAdmission]] is the only later replacement. A
   * projected `(Int32, Int64)` whose reducer does not add longs is that binder [[scala.None]].
   */
  private def reconcile(
      bound: Option[KernelChoice],
      today: Option[PhysicalOp],
  ): Option[PhysicalOp] =
    (bound, today) match
      case (Some(tag), Some(op)) if agrees(tag, op) => Some(op)
      case _ => None

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def agrees(tag: KernelChoice, op: PhysicalOp): Boolean =
    Describe.tag(op) == tag

  /**
   * Child under a slot-filter prefix the selection compiler compiled.
   *
   * No prefix returns `child`. A prefix whose steps are missing returns [[scala.None]]: the filter
   * stays on today's arm, and the child under it is not bound.
   */
  private def bindable(source: Plan[_], child: PhysicalOp): Option[Peeled] =
    if (!slotPrefix(source, child)) then Some(Peeled(child, Vector.empty))
    else
      steps(source, child) match
        case Some(compiled) if compiled.nonEmpty =>
          val (_, under) = PredicatePlan.childOf(source, child)
          Some(Peeled(under, compiled))
        case _ => None

  /** Child under a compiled slot prefix. `selection` is empty when the child had no prefix. */
  private final case class Peeled(op: PhysicalOp, selection: Vector[Selection.Step])

  private def slotPrefix(plan: Plan[_], op: PhysicalOp): Boolean = (plan, op) match
    case (Plan.FilterKeysOp(_, _), PhysicalOp.Filter(_, PhysicalOp.Slot.Key)) => true
    case (Plan.FilterValuesOp(_, _), PhysicalOp.Filter(_, PhysicalOp.Slot.Value)) => true
    case (Plan.FilterOp(_, _), PhysicalOp.Filter(_, _)) => true
    case _ => false

  /** Steps for the filter prefix. A cast the compiler rejects is no steps, not a failed lower. */
  private def steps(plan: Plan[_], op: PhysicalOp): Option[Vector[Selection.Step]] =
    try PredicatePlan.compile(plan, op).flatMap(PredicatePlan.selectionSteps)
    catch
      case _: ClassCastException => None
      case _: IllegalStateException => None

  /**
   * Tag for `op` after the peel.
   *
   * The [[Bound]] carries the child's columns, including [[NullPolicy.Absence]] on an outer join.
   * [[ColumnBinding]] reads that policy. A count-through-probe tag is dropped when `selection` is
   * not empty: that kernel has no slot, and the predicate is not discarded.
   */
  private def ask(
      op: PhysicalOp,
      operator: Operator,
      reducer: ReducerWitness,
      selection: Vector[Selection.Step],
  ): Option[KernelChoice] =
    val bound = Bound.of(op, selection, reducer)
    val choice = bound.output match
      case Output.Columns(columns) =>
        ColumnBinding.choose(columns, operator, reducer, Bound.nulls(bound), Bound.side(bound))
      case Output.IntLongPairs(_) => intLongMap(operator, reducer)
      case Output.Opaque(_) => None
    choice match
      case Some(KernelChoice.CountThroughProbe) if selection.nonEmpty => None
      case other => other

  /**
   * Long addition on an int-to-long `mapValues`. Every other operator, including `countByKey`, is
   * [[scala.None]]. Routing these columns through [[ColumnBinding.choose]] would also tag a count,
   * because they compare equal to [[Layout.AssumedIntLong]].
   */
  private def intLongMap(operator: Operator, reducer: ReducerWitness): Option[KernelChoice] =
    (operator, reducer) match
      case (Operator.ReduceByKey, ReducerWitness.AddsLongs) =>
        Some(KernelChoice.IntLongProjectSum)
      case _ => None

  /** Long addition, or any other reducer. [[witness]] calls the function. */
  private[execution] def witnessOf(reducer: Any): ReducerWitness = witness(reducer)

  /**
   * Long addition, checked by calling `reducer`. The body is not read. Any other function,
   * including one that does not accept two longs, is [[ReducerWitness.Other]].
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Equals",
    ),
  )
  private def witness(reducer: Any): ReducerWitness =
    try
      val fn = reducer.asInstanceOf[(Long, Long) => Long]
      if (fn(1L, 2L) == 3L && fn(5L, 0L) == 5L && fn(-2L, 2L) == 0L) then ReducerWitness.AddsLongs
      else ReducerWitness.Other
    catch case _: ClassCastException => ReducerWitness.Other
