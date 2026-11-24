package com.ewoodbury.scarlet.execution.schema

/**
 * Chooses a [[KernelChoice]] for one parent.
 *
 * The choice is a pure function of the child layout, the operator, the reducer witness, the null
 * policy, and inner or outer. [[scala.None]] is a refusal. Refusal is not a tag. This does not
 * build a [[com.ewoodbury.scarlet.execution.PhysicalOp]]. [[AggregateBinding]] asks it after the
 * peel. The interpreter does not call it.
 *
 * [[NullAdmission.Refused]] means [[com.ewoodbury.scarlet.execution.ScanLowering]] would refuse
 * the source for nulls. The caller passes that policy. Nothing here reads a schema or classifies
 * rows. [[Side.Outer]] and [[NullPolicy.Absence]] are an outer join, and these aggregates refuse
 * it. A generic tuple map is not accepted. An `Int => Long` mapValues is [[scala.None]].
 */
private[execution] object ColumnBinding:

  /**
   * Parent this binder is asked about.
   *
   * [[Operator.TupleMap]] is a generic tuple `map`. It is not accepted. [[Operator.MapValues]] is
   * `Int => Long`, which has no pair kernel.
   */
  enum Operator:
    case CountByKey, ReduceByKey, CountSumByKey, MapValues, TupleMap

  /**
   * Reducer witness the caller already computed.
   *
   * [[ReducerWitness.AddsLongs]] is the only witness that selects
   * [[KernelChoice.IntLongProjectSum]]. Pair aggregates do not read the witness. This does not
   * call the reducer.
   */
  enum ReducerWitness:
    case AddsLongs, Other, NotApplicable

  /** Inner input, or an outer join whose missing partner is [[NullPolicy.Absence]]. */
  enum Side:
    case Inner, Outer

  /**
   * Null policy of the source.
   *
   * [[NullAdmission.Refused]] is not a column policy.
   * [[com.ewoodbury.scarlet.execution.ScanLowering]] would refuse that source for nulls, and the
   * caller passes the fact in.
   */
  enum NullAdmission:
    case Policy(policy: NullPolicy)
    case Refused

  /**
   * Tag for `operator` on `layout`, or [[scala.None]].
   *
   * `layout` is the child columns. `reducer` is the witness result. `nulls` is the source policy,
   * including a scan the classifier would refuse. `side` is inner or outer.
   */
  def choose(
      layout: Vector[ColumnLayout],
      operator: Operator,
      reducer: ReducerWitness,
      nulls: NullAdmission,
      side: Side,
  ): Option[KernelChoice] =
    if declined(operator, layout, nulls, side) then None
    else tag(layout, operator, reducer)

  /**
   * Parents that have no tag, an outer side, or a null policy the kernels do not run.
   *
   * `Int => Long` mapValues stays here so a later edit does not tag it
   * [[KernelChoice.IntLongProjectSum]]. A generic tuple map is not a parent this binder accepts,
   * so no tag is built for it.
   */
  private def declined(
      operator: Operator,
      layout: Vector[ColumnLayout],
      nulls: NullAdmission,
      side: Side,
  ): Boolean =
    operator match
      case Operator.MapValues | Operator.TupleMap => true
      case Operator.CountByKey | Operator.ReduceByKey | Operator.CountSumByKey =>
        outer(side) || refused(nulls) || layout.exists(absent)

  private def refused(nulls: NullAdmission): Boolean = nulls match
    case NullAdmission.Refused => true
    case NullAdmission.Policy(NullPolicy.Absence) => true
    case NullAdmission.Policy(NullPolicy.Present) => false
    case NullAdmission.Policy(NullPolicy.Nullable) => false

  private def outer(side: Side): Boolean = side match
    case Side.Outer => true
    case Side.Inner => false

  private def absent(column: ColumnLayout): Boolean = column.nullPolicy match
    case NullPolicy.Absence => true
    case NullPolicy.Present | NullPolicy.Nullable => false

  /** Pair layouts the kernels already run, or long addition on [[Layout.AssumedIntLong]]. */
  private def tag(
      layout: Vector[ColumnLayout],
      operator: Operator,
      reducer: ReducerWitness,
  ): Option[KernelChoice] =
    if assumedIntLong(layout) then intLong(operator, reducer)
    else pair(layout.map(_.columnType), operator)

  private def assumedIntLong(layout: Vector[ColumnLayout]): Boolean = Layout.AssumedIntLong match
    case Output.Columns(columns) => sameColumns(layout, columns)
    case Output.Opaque(_) => false

  private def sameColumns(left: Vector[ColumnLayout], right: Vector[ColumnLayout]): Boolean =
    (left, right) match
      case (Vector(leftKey, leftValue), Vector(rightKey, rightValue)) =>
        sameColumn(leftKey, rightKey) && sameColumn(leftValue, rightValue)
      case _ =>
        false

  private def sameColumn(left: ColumnLayout, right: ColumnLayout): Boolean =
    sameType(left.columnType, right.columnType) &&
      samePolicy(left.nullPolicy, right.nullPolicy)

  private def sameType(left: ColumnType, right: ColumnType): Boolean = (left, right) match
    case (ColumnType.Int32, ColumnType.Int32) | (ColumnType.Int64, ColumnType.Int64) |
        (ColumnType.Float64, ColumnType.Float64) | (ColumnType.Bool, ColumnType.Bool) |
        (ColumnType.Utf8Dict, ColumnType.Utf8Dict) =>
      true
    case _ =>
      false

  private def samePolicy(left: NullPolicy, right: NullPolicy): Boolean = (left, right) match
    case (NullPolicy.Present, NullPolicy.Present) | (NullPolicy.Nullable, NullPolicy.Nullable) |
        (NullPolicy.Absence, NullPolicy.Absence) =>
      true
    case _ =>
      false

  /** Triple-project `[Int32, Int64]`. Only long addition has a kernel. */
  private def intLong(operator: Operator, reducer: ReducerWitness): Option[KernelChoice] =
    (operator, reducer) match
      case (Operator.ReduceByKey, ReducerWitness.AddsLongs) =>
        Some(KernelChoice.IntLongProjectSum)
      case _ =>
        None

  /**
   * `(Int32, Int32)`, `(Int64, Int64)`, and `(Utf8Dict, Int32)` under `Present` or `Nullable`.
   * Those are the policies the count and reduce kernels already accept.
   */
  private def pair(types: Vector[ColumnType], operator: Operator): Option[KernelChoice] =
    (types, operator) match
      case (Vector(ColumnType.Int32, ColumnType.Int32), Operator.CountByKey) =>
        Some(KernelChoice.CountByKey)
      case (Vector(ColumnType.Int32, ColumnType.Int32), Operator.ReduceByKey) =>
        Some(KernelChoice.IntHashAggregate)
      case (Vector(ColumnType.Int32, ColumnType.Int32), Operator.CountSumByKey) =>
        Some(KernelChoice.CountSum)
      case (Vector(ColumnType.Int64, ColumnType.Int64), Operator.CountByKey) =>
        Some(KernelChoice.CountByKey)
      case (Vector(ColumnType.Int64, ColumnType.Int64), Operator.ReduceByKey) =>
        Some(KernelChoice.LongHashAggregate)
      case (Vector(ColumnType.Utf8Dict, ColumnType.Int32), Operator.CountByKey) =>
        Some(KernelChoice.CountByKey)
      case (Vector(ColumnType.Utf8Dict, ColumnType.Int32), Operator.ReduceByKey) =>
        Some(KernelChoice.StringHashAggregate)
      case _ =>
        None
