package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.Selection
import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.*
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.{
  NullAdmission,
  Operator,
  ReducerWitness,
  Side,
}

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
 * root. When the binder returns [[scala.None]] and today's lowering returns a root, that root is
 * kept. A tag the binder would add while today's lowering returns [[scala.None]] is not built.
 * [[CountByKeyLowering]] still runs, so its join arm stays on that path. [[JoinProbeAggregate]]
 * stays in [[ColumnarPlanner]] ahead of [[reduce]].
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
    bindable(source, child).flatMap(peeled => ask(peeled, operator, reducer))

  private def kept(
      source: Plan[_],
      lowerChild: Plan[_] => Option[PhysicalOp],
      operator: Operator,
      reducer: ReducerWitness,
  )(todayOf: PhysicalOp => Option[PhysicalOp]): Option[PhysicalOp] =
    lowerChild(source) match
      case None => None
      case Some(child) =>
        val today = todayOf(child)
        bindable(source, child) match
          case None => today
          case Some(peeled) => reconcile(ask(peeled, operator, reducer), today)

  /**
   * Today's root when the binder agrees or declines. A disagreeing tag keeps today's root too, so
   * a plan that already lowers is not dropped. [[scala.None]] from today is not replaced.
   */
  private def reconcile(
      bound: Option[KernelChoice],
      today: Option[PhysicalOp],
  ): Option[PhysicalOp] =
    (bound, today) match
      case (Some(tag), Some(op)) if agrees(tag, op) => Some(op)
      case (None, Some(op)) => Some(op)
      case (Some(_), Some(op)) => Some(op)
      case (_, None) => None

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def agrees(tag: KernelChoice, op: PhysicalOp): Boolean =
    Describe.tag(op) == tag

  /**
   * Child under a slot-filter prefix the selection compiler compiled.
   *
   * No prefix returns `child`. A prefix whose steps are missing returns [[scala.None]]: the filter
   * stays on today's arm, and the child under it is not bound.
   */
  private def bindable(source: Plan[_], child: PhysicalOp): Option[PhysicalOp] =
    if (!slotPrefix(source, child)) then Some(child)
    else
      steps(source, child) match
        case Some(compiled) if compiled.nonEmpty =>
          val (_, under) = PredicatePlan.childOf(source, child)
          Some(under)
        case _ => None

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

  private def ask(
      op: PhysicalOp,
      operator: Operator,
      reducer: ReducerWitness,
  ): Option[KernelChoice] =
    Layout.known(op) match
      case Output.Columns(columns) =>
        ColumnBinding.choose(columns, operator, reducer, admission(columns), side(columns))
      case Output.Opaque(_) => None

  /** Column policy [[Layout.known]] already stored. A refused scan never becomes this child. */
  private def admission(columns: Vector[ColumnLayout]): NullAdmission =
    NullAdmission.Policy(policy(columns))

  private def policy(columns: Vector[ColumnLayout]): NullPolicy =
    if (columns.exists(absent)) then NullPolicy.Absence
    else if (columns.nonEmpty && columns.forall(present)) then NullPolicy.Present
    else NullPolicy.Nullable

  private def side(columns: Vector[ColumnLayout]): Side =
    if (columns.exists(absent)) then Side.Outer else Side.Inner

  private def absent(column: ColumnLayout): Boolean = column.nullPolicy match
    case NullPolicy.Absence => true
    case NullPolicy.Present | NullPolicy.Nullable => false

  private def present(column: ColumnLayout): Boolean = column.nullPolicy match
    case NullPolicy.Present => true
    case NullPolicy.Nullable | NullPolicy.Absence => false

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
