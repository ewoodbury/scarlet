package com.ewoodbury.scarlet.execution.schema

import java.util.concurrent.atomic.AtomicInteger

import com.ewoodbury.scarlet.execution.PhysicalOp

/**
 * Chooses a [[KernelChoice]] for one parent.
 *
 * The returned tag is a function of the child layout, the operator, the reducer witness, the null
 * policy, and inner or outer. [[chooseCallsForTest]] counts calls and does not change the tag.
 * [[scala.None]] is a refusal. Refusal is not a tag. This does not build a
 * [[com.ewoodbury.scarlet.execution.PhysicalOp]]. [[AggregateBinding]] asks it after the peel.
 * [[com.ewoodbury.scarlet.execution.SortLowering]] asks it before building a typed sort.
 * [[com.ewoodbury.scarlet.execution.GroupByKeyLowering]],
 * [[com.ewoodbury.scarlet.execution.DistinctLowering]], and
 * [[com.ewoodbury.scarlet.execution.KeyValueLowering]] ask it before building `groupByKey`,
 * `distinct`, `keys`, and `values`. [[com.ewoodbury.scarlet.execution.UnionLowering]] asks it on
 * each child before it builds a union. [[com.ewoodbury.scarlet.execution.FilterBinding]] asks it
 * before it builds a filter. The interpreter does not call it.
 *
 * [[NullAdmission.Refused]] means [[com.ewoodbury.scarlet.execution.ScanLowering]] would refuse
 * the source for nulls. The caller passes that policy. Nothing here reads a schema or classifies
 * rows. [[Side.Outer]] and [[NullPolicy.Absence]] are an outer join, and these aggregates refuse
 * it before a tag is chosen. A generic tuple map is not accepted. An `Int => Long` mapValues is
 * [[scala.None]].
 *
 * An int inner join is three [[ColumnType.Int32]] columns, `(key, left, right)`. `countByKey` on
 * that layout is [[KernelChoice.CountThroughProbe]]. The join is not [[Layout.AssumedIntLong]].
 * `reduceByKey` on those three columns is [[scala.None]], including when the witness adds longs.
 * [[Layout.AssumedIntLong]] is the project above the join. `countByKey` of it is
 * [[KernelChoice.CountByKey]]. It is [[KernelChoice.IntLongProjectSum]] only for
 * [[ReducerWitness.AddsLongs]].
 */
private[execution] object ColumnBinding:

  private val chooseCalls = new AtomicInteger

  /**
   * [[choose]] calls since [[resetChooseCallsForTest]]. A kind-only caller leaves this at zero.
   */
  private[execution] def chooseCallsForTest: Int = chooseCalls.get

  /** Clears [[chooseCallsForTest]]. */
  private[execution] def resetChooseCallsForTest(): Unit =
    chooseCalls.set(0)

  /**
   * Parent this binder is asked about.
   *
   * [[Operator.TupleMap]] is a generic tuple `map`. It is not accepted. [[Operator.MapValues]] is
   * `Int => Long`, which has no pair kernel. The four sort cases are the typed sorts.
   * [[com.ewoodbury.scarlet.execution.SortLowering]] asks before it builds one.
   * [[Operator.GroupByKey]], [[Operator.Distinct]], [[Operator.Keys]], and [[Operator.Values]] are
   * the group family. [[Operator.Keys]] and [[Operator.Values]] both tag [[KernelChoice.Column]].
   * [[Operator.Union]] is one child of a concatenation. [[choose]] returns [[KernelChoice.Union]]
   * for any column layout, including an outer join.
   * [[com.ewoodbury.scarlet.execution.UnionLowering]] compares the two children.
   * [[Operator.Filter]], [[Operator.FilterKeys]], and [[Operator.FilterValues]] are the three
   * filter parents. Each one tags [[KernelChoice.Filter]] on a layout that parent already runs.
   */
  enum Operator:
    case CountByKey, ReduceByKey, CountSumByKey, MapValues, TupleMap
    case SortByIntKey, SortByIntValueDesc, SortByLongKey, SortByStringKey
    case GroupByKey, Distinct, Keys, Values
    case Filter, FilterKeys, FilterValues
    case Union

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
    val _ = chooseCalls.incrementAndGet()
    // Each union child asks here. Absence is a real outer-join column, so this parent is not
    // [[declined]]. [[UnionLowering]] compares the two layouts and the kinds the kernel can run.
    operator match
      case Operator.Union => Some(KernelChoice.Union)
      case _ =>
        if declined(operator, layout, nulls, side) then None
        else tag(layout, operator, reducer)

  /**
   * [[choose]] on the columns of `child`.
   *
   * An opaque child is [[scala.None]] and does not call [[choose]]. The reducer witness is not an
   * input for `groupByKey`, `distinct`, `keys`, or `values`.
   */
  private[execution] def asked(child: PhysicalOp, operator: Operator): Option[KernelChoice] =
    val bound = Bound.of(child, Vector.empty, ReducerWitness.NotApplicable)
    bound.output match
      case Output.Columns(columns) =>
        choose(
          columns,
          operator,
          ReducerWitness.NotApplicable,
          Bound.nulls(bound),
          Bound.side(bound),
        )
      case Output.Opaque(_) => None

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
      case Operator.Union => false
      case Operator.Filter =>
        false
      case Operator.CountByKey | Operator.ReduceByKey | Operator.CountSumByKey |
          Operator.SortByIntKey | Operator.SortByIntValueDesc | Operator.SortByLongKey |
          Operator.SortByStringKey | Operator.GroupByKey | Operator.Distinct | Operator.Keys |
          Operator.Values | Operator.FilterKeys | Operator.FilterValues =>
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

  /**
   * Pair layouts the kernels already run, `countByKey` or long addition on
   * [[Layout.AssumedIntLong]], or [[KernelChoice.CountThroughProbe]] on an int inner join.
   *
   * The join arm is three columns. It is not the two-column project above that join.
   */
  private def tag(
      layout: Vector[ColumnLayout],
      operator: Operator,
      reducer: ReducerWitness,
  ): Option[KernelChoice] =
    // A filter on three int columns is [[KernelChoice.Filter]], not a count through the probe.
    // [[Layout.AssumedIntLong]] still goes to [[intLong]] first, so a filter there is
    // [[scala.None]]. The one slot above that project is kept outside this match.
    if assumedIntLong(layout) then intLong(operator, reducer)
    else if filterParent(operator) then filterChoice(layout)
    else if intJoin(layout) then countThroughProbe(operator)
    else pair(layout.map(_.columnType), operator)

  /** `filter`, `filterKeys`, and `filterValues`. Each one builds [[KernelChoice.Filter]]. */
  private def filterParent(operator: Operator): Boolean = operator match
    case Operator.Filter | Operator.FilterKeys | Operator.FilterValues => true
    case Operator.CountByKey | Operator.ReduceByKey | Operator.CountSumByKey | Operator.MapValues |
        Operator.TupleMap | Operator.SortByIntKey | Operator.SortByIntValueDesc |
        Operator.SortByLongKey | Operator.SortByStringKey | Operator.GroupByKey |
        Operator.Distinct | Operator.Keys | Operator.Values | Operator.Union =>
      false

  /**
   * Layouts a filter already runs: int, long, and string pairs, three int columns, and one int,
   * long, double, string, or boolean column. A whole-row filter is not declined for
   * [[NullPolicy.Absence]]: an outer int join already lowers one named column, and that kernel
   * fails closed. [[Operator.FilterKeys]] and [[Operator.FilterValues]] still refuse that column.
   * [[Layout.AssumedIntLong]] does not reach this match.
   */
  private def filterChoice(layout: Vector[ColumnLayout]): Option[KernelChoice] =
    if (filterLayout(layout.map(_.columnType))) then Some(KernelChoice.Filter) else None

  private def filterLayout(types: Vector[ColumnType]): Boolean = types match
    case Vector(ColumnType.Int32, ColumnType.Int32) => true
    case Vector(ColumnType.Int64, ColumnType.Int64) => true
    case Vector(ColumnType.Utf8Dict, ColumnType.Int32) => true
    case Vector(ColumnType.Int32, ColumnType.Int32, ColumnType.Int32) => true
    case Vector(ColumnType.Int32) => true
    case Vector(ColumnType.Int64) => true
    case Vector(ColumnType.Float64) => true
    case Vector(ColumnType.Utf8Dict) => true
    case Vector(ColumnType.Bool) => true
    case _ => false

  /**
   * `(Int32, Int32, Int32)` under [[NullPolicy.Present]] or [[NullPolicy.Nullable]].
   *
   * [[NullPolicy.Absence]] does not match. [[declined]] already refused that column, so an outer
   * join is not a missing arm of this match.
   */
  private def intJoin(layout: Vector[ColumnLayout]): Boolean = layout match
    case Vector(key, left, right) =>
      intColumn(key) && intColumn(left) && intColumn(right)
    case _ => false

  private def intColumn(column: ColumnLayout): Boolean =
    (column.columnType, column.nullPolicy) match
      case (ColumnType.Int32, NullPolicy.Present) => true
      case (ColumnType.Int32, NullPolicy.Nullable) => true
      case (ColumnType.Int32, NullPolicy.Absence) => false
      case (ColumnType.Int64, _) | (ColumnType.Float64, _) | (ColumnType.Bool, _) |
          (ColumnType.Utf8Dict, _) =>
        false

  /** `countByKey` of the three int columns. `reduceByKey` of a join is not a project sum. */
  private def countThroughProbe(operator: Operator): Option[KernelChoice] = operator match
    case Operator.CountByKey => Some(KernelChoice.CountThroughProbe)
    case Operator.ReduceByKey | Operator.CountSumByKey | Operator.MapValues | Operator.TupleMap |
        Operator.SortByIntKey | Operator.SortByIntValueDesc | Operator.SortByLongKey |
        Operator.SortByStringKey | Operator.GroupByKey | Operator.Distinct | Operator.Keys |
        Operator.Values | Operator.Filter | Operator.FilterKeys | Operator.FilterValues |
        Operator.Union =>
      None

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

  /**
   * Triple-project `[Int32, Int64]`.
   *
   * `countByKey` is [[KernelChoice.CountByKey]], the class that project already publishes.
   * `reduceByKey` is [[KernelChoice.IntLongProjectSum]] only when the witness adds longs. This
   * vector does not show a hinted join the lowering refused, so [[scala.None]] here is not that
   * refusal.
   */
  private def intLong(operator: Operator, reducer: ReducerWitness): Option[KernelChoice] =
    (operator, reducer) match
      case (Operator.ReduceByKey, ReducerWitness.AddsLongs) =>
        Some(KernelChoice.IntLongProjectSum)
      case (Operator.CountByKey, _) =>
        Some(KernelChoice.CountByKey)
      case _ =>
        None

  /**
   * `(Int32, Int32)`, `(Int64, Int64)`, and `(Utf8Dict, Int32)` under `Present` or `Nullable`.
   * Those are the policies the count, reduce, typed-sort, group, key, and value kernels already
   * accept. A sort, group, key, or value on another pair is [[scala.None]]. `distinct` is one
   * [[ColumnType.Int32]] column or one [[ColumnType.Utf8Dict]] column. [[NullPolicy.Absence]]
   * never reaches this match.
   */
  private def pair(types: Vector[ColumnType], operator: Operator): Option[KernelChoice] =
    (types, operator) match
      case (Vector(ColumnType.Int32, ColumnType.Int32), Operator.CountByKey) =>
        Some(KernelChoice.CountByKey)
      case (Vector(ColumnType.Int32, ColumnType.Int32), Operator.ReduceByKey) =>
        Some(KernelChoice.IntHashAggregate)
      case (Vector(ColumnType.Int32, ColumnType.Int32), Operator.CountSumByKey) =>
        Some(KernelChoice.CountSum)
      case (Vector(ColumnType.Int32, ColumnType.Int32), Operator.SortByIntKey) =>
        Some(KernelChoice.SortByIntKey)
      case (Vector(ColumnType.Int32, ColumnType.Int32), Operator.SortByIntValueDesc) =>
        Some(KernelChoice.SortByIntValueDesc)
      case (Vector(ColumnType.Int64, ColumnType.Int64), Operator.CountByKey) =>
        Some(KernelChoice.CountByKey)
      case (Vector(ColumnType.Int64, ColumnType.Int64), Operator.ReduceByKey) =>
        Some(KernelChoice.LongHashAggregate)
      case (Vector(ColumnType.Int64, ColumnType.Int64), Operator.SortByLongKey) =>
        Some(KernelChoice.SortByLongKey)
      case (Vector(ColumnType.Utf8Dict, ColumnType.Int32), Operator.CountByKey) =>
        Some(KernelChoice.CountByKey)
      case (Vector(ColumnType.Utf8Dict, ColumnType.Int32), Operator.ReduceByKey) =>
        Some(KernelChoice.StringHashAggregate)
      case (Vector(ColumnType.Utf8Dict, ColumnType.Int32), Operator.SortByStringKey) =>
        Some(KernelChoice.SortByStringKey)
      case (Vector(ColumnType.Int32, ColumnType.Int32), Operator.GroupByKey) =>
        Some(KernelChoice.GroupByKey)
      case (Vector(ColumnType.Int64, ColumnType.Int64), Operator.GroupByKey) =>
        Some(KernelChoice.GroupByKey)
      case (Vector(ColumnType.Utf8Dict, ColumnType.Int32), Operator.GroupByKey) =>
        Some(KernelChoice.GroupByKey)
      case (Vector(ColumnType.Int32, ColumnType.Int32), Operator.Keys) =>
        Some(KernelChoice.Column)
      case (Vector(ColumnType.Int32, ColumnType.Int32), Operator.Values) =>
        Some(KernelChoice.Column)
      case (Vector(ColumnType.Int64, ColumnType.Int64), Operator.Keys) =>
        Some(KernelChoice.Column)
      case (Vector(ColumnType.Int64, ColumnType.Int64), Operator.Values) =>
        Some(KernelChoice.Column)
      case (Vector(ColumnType.Utf8Dict, ColumnType.Int32), Operator.Keys) =>
        Some(KernelChoice.Column)
      case (Vector(ColumnType.Utf8Dict, ColumnType.Int32), Operator.Values) =>
        Some(KernelChoice.Column)
      case (Vector(ColumnType.Int32), Operator.Distinct) =>
        Some(KernelChoice.Dedupe)
      case (Vector(ColumnType.Utf8Dict), Operator.Distinct) =>
        Some(KernelChoice.Dedupe)
      case _ =>
        None
