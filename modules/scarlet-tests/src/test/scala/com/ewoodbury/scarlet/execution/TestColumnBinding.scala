package com.ewoodbury.scarlet.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.ewoodbury.scarlet.execution.schema.*

import ColumnBinding.{NullAdmission, Operator, ReducerWitness, Side}

/**
 * Truth table for [[ColumnBinding]]. One row is one cell: layout, operator, reducer witness, null
 * policy, and inner or outer.
 *
 * `Present` and `Nullable` are the policies the pair kernels already accept. A refused scan is
 * passed in as [[NullAdmission.Refused]]. That row keeps a normal nullable layout, so the refusal
 * is not read off the columns. This does not call [[ColumnarPlanner.lower]] and does not classify
 * a source.
 */
class TestColumnBinding extends AnyFlatSpec with Matchers:

  private val assumedIntLong: Vector[ColumnLayout] = Layout.AssumedIntLong match
    case Output.Columns(columns) => columns
    case Output.IntLongPairs(_) =>
      throw new IllegalStateException("AssumedIntLong is not an int-long map value")
    case Output.Opaque(reason) => throw new IllegalStateException(reason)

  private val outerAssumed: Vector[ColumnLayout] = assumedIntLong match
    case Vector(key, value) =>
      Vector(key, value.copy(nullPolicy = NullPolicy.Absence))
    case _ =>
      throw new IllegalStateException("AssumedIntLong has a key and a value")

  "ColumnBinding" should "match every truth-table cell" in {
    val labels = table.map(_.label)
    labels.distinct shouldBe labels
    table.foreach { row =>
      withClue(s"${row.label}: ") {
        decide(row) shouldBe row.choice
      }
    }
  }

  it should "build no kernel tag for a generic tuple map" in {
    val row = named("generic tuple map")
    row.operator shouldBe Operator.TupleMap
    row.layout shouldBe assumedIntLong
    row.choice.isEmpty shouldBe true
    decide(row).isEmpty shouldBe true
  }

  it should "refuse Int => Long mapValues" in {
    val row = named("Int => Long mapValues")
    row.operator shouldBe Operator.MapValues
    row.reducer shouldBe ReducerWitness.AddsLongs
    row.layout shouldBe assumedIntLong
    row.choice.isEmpty shouldBe true
    decide(row).isEmpty shouldBe true
  }

  it should "take a refused scan null policy from the caller" in {
    val admitted = named("(Int32, Int32) countByKey Nullable")
    val refused = named("(Int32, Int32) countByKey scan nulls refused")
    refused.layout shouldBe admitted.layout
    refused.operator shouldBe admitted.operator
    refused.reducer shouldBe admitted.reducer
    refused.side shouldBe admitted.side
    refused.nulls shouldBe NullAdmission.Refused
    admitted.choice shouldBe Some(KernelChoice.CountByKey)
    decide(refused).isEmpty shouldBe true
  }

  private def named(label: String): Cell =
    byLabel.getOrElse(label, fail(s"missing truth-table row: $label"))

  private def decide(row: Cell): Option[KernelChoice] =
    ColumnBinding.choose(row.layout, row.operator, row.reducer, row.nulls, row.side)

  private def columns(
      key: ColumnType,
      value: ColumnType,
      policy: NullPolicy,
  ): Vector[ColumnLayout] =
    Vector(ColumnLayout(key, policy), ColumnLayout(value, policy))

  /** Int inner join: key, left, and right. Not [[Layout.AssumedIntLong]]. */
  private def joinColumns(policy: NullPolicy): Vector[ColumnLayout] =
    Vector(
      ColumnLayout(ColumnType.Int32, policy),
      ColumnLayout(ColumnType.Int32, policy),
      ColumnLayout(ColumnType.Int32, policy),
    )

  private def accepted(
      key: ColumnType,
      value: ColumnType,
      operator: Operator,
      reducer: ReducerWitness,
      policy: NullPolicy,
      choice: KernelChoice,
  ): Cell =
    Cell(
      label = s"($key, $value) ${operatorName(operator)} $policy",
      layout = columns(key, value, policy),
      operator = operator,
      reducer = reducer,
      nulls = NullAdmission.Policy(policy),
      side = Side.Inner,
      choice = Some(choice),
    )

  /** Same pair types. The value side is [[NullPolicy.Absence]], and the side is outer. */
  private def outer(
      key: ColumnType,
      value: ColumnType,
      operator: Operator,
      reducer: ReducerWitness,
  ): Cell =
    Cell(
      label = s"($key, $value) ${operatorName(operator)} outer Absence",
      layout = Vector(
        ColumnLayout(key, NullPolicy.Nullable),
        ColumnLayout(value, NullPolicy.Absence),
      ),
      operator = operator,
      reducer = reducer,
      nulls = NullAdmission.Policy(NullPolicy.Nullable),
      side = Side.Outer,
      choice = None,
    )

  /**
   * Layout stays the nullable pair those kernels accept. [[NullAdmission.Refused]] is the input
   * that a scan classifier would refuse for nulls.
   */
  private def refusedScan(
      key: ColumnType,
      value: ColumnType,
      operator: Operator,
      reducer: ReducerWitness,
  ): Cell =
    Cell(
      label = s"($key, $value) ${operatorName(operator)} scan nulls refused",
      layout = columns(key, value, NullPolicy.Nullable),
      operator = operator,
      reducer = reducer,
      nulls = NullAdmission.Refused,
      side = Side.Inner,
      choice = None,
    )

  private def operatorName(operator: Operator): String = operator match
    case Operator.CountByKey => "countByKey"
    case Operator.ReduceByKey => "reduceByKey"
    case Operator.CountSumByKey => "countSumByKey"
    case Operator.MapValues => "mapValues"
    case Operator.IntLongMapValue => "intLongMapValue"
    case Operator.TupleMap => "map"
    case Operator.SortByIntKey => "sortByIntKey"
    case Operator.SortByIntValueDesc => "sortByIntValueDesc"
    case Operator.SortByLongKey => "sortByLongKey"
    case Operator.SortByStringKey => "sortByStringKey"
    case Operator.SortByLongValueDesc => "sortByLongValueDesc"
    case Operator.SortByStringValueDesc => "sortByStringValueDesc"
    case Operator.GroupByKey => "groupByKey"
    case Operator.Distinct => "distinct"
    case Operator.Keys => "keys"
    case Operator.Values => "values"
    case Operator.Filter => "filter"
    case Operator.FilterKeys => "filterKeys"
    case Operator.FilterValues => "filterValues"
    case Operator.Project => "project"
    case Operator.Pipeline => "pipeline"
    case Operator.Union => "union"

  private val table: Vector[Cell] = Vector(
    accepted(
      ColumnType.Int32,
      ColumnType.Int32,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
      NullPolicy.Present,
      KernelChoice.CountByKey,
    ),
    accepted(
      ColumnType.Int32,
      ColumnType.Int32,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
      NullPolicy.Nullable,
      KernelChoice.CountByKey,
    ),
    accepted(
      ColumnType.Int32,
      ColumnType.Int32,
      Operator.ReduceByKey,
      ReducerWitness.Other,
      NullPolicy.Present,
      KernelChoice.IntHashAggregate,
    ),
    accepted(
      ColumnType.Int32,
      ColumnType.Int32,
      Operator.ReduceByKey,
      ReducerWitness.Other,
      NullPolicy.Nullable,
      KernelChoice.IntHashAggregate,
    ),
    accepted(
      ColumnType.Int32,
      ColumnType.Int32,
      Operator.CountSumByKey,
      ReducerWitness.NotApplicable,
      NullPolicy.Present,
      KernelChoice.CountSum,
    ),
    accepted(
      ColumnType.Int32,
      ColumnType.Int32,
      Operator.CountSumByKey,
      ReducerWitness.NotApplicable,
      NullPolicy.Nullable,
      KernelChoice.CountSum,
    ),
    accepted(
      ColumnType.Int64,
      ColumnType.Int64,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
      NullPolicy.Present,
      KernelChoice.CountByKey,
    ),
    accepted(
      ColumnType.Int64,
      ColumnType.Int64,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
      NullPolicy.Nullable,
      KernelChoice.CountByKey,
    ),
    accepted(
      ColumnType.Int64,
      ColumnType.Int64,
      Operator.ReduceByKey,
      ReducerWitness.Other,
      NullPolicy.Present,
      KernelChoice.LongHashAggregate,
    ),
    accepted(
      ColumnType.Int64,
      ColumnType.Int64,
      Operator.ReduceByKey,
      ReducerWitness.Other,
      NullPolicy.Nullable,
      KernelChoice.LongHashAggregate,
    ),
    accepted(
      ColumnType.Utf8Dict,
      ColumnType.Int32,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
      NullPolicy.Present,
      KernelChoice.CountByKey,
    ),
    accepted(
      ColumnType.Utf8Dict,
      ColumnType.Int32,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
      NullPolicy.Nullable,
      KernelChoice.CountByKey,
    ),
    accepted(
      ColumnType.Utf8Dict,
      ColumnType.Int32,
      Operator.ReduceByKey,
      ReducerWitness.Other,
      NullPolicy.Present,
      KernelChoice.StringHashAggregate,
    ),
    accepted(
      ColumnType.Utf8Dict,
      ColumnType.Int32,
      Operator.ReduceByKey,
      ReducerWitness.Other,
      NullPolicy.Nullable,
      KernelChoice.StringHashAggregate,
    ),
    Cell(
      label = "AssumedIntLong addsLongs",
      layout = assumedIntLong,
      operator = Operator.ReduceByKey,
      reducer = ReducerWitness.AddsLongs,
      nulls = NullAdmission.Policy(NullPolicy.Nullable),
      side = Side.Inner,
      choice = Some(KernelChoice.IntLongProjectSum),
    ),
    Cell(
      label = "AssumedIntLong any other reducer",
      layout = assumedIntLong,
      operator = Operator.ReduceByKey,
      reducer = ReducerWitness.Other,
      nulls = NullAdmission.Policy(NullPolicy.Nullable),
      side = Side.Inner,
      choice = None,
    ),
    // The triple-project count publishes CountByKey. Describe agrees with that tag.
    Cell(
      label = "AssumedIntLong countByKey",
      layout = assumedIntLong,
      operator = Operator.CountByKey,
      reducer = ReducerWitness.NotApplicable,
      nulls = NullAdmission.Policy(NullPolicy.Nullable),
      side = Side.Inner,
      choice = Some(KernelChoice.CountByKey),
    ),
    outer(
      ColumnType.Int32,
      ColumnType.Int32,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
    ),
    outer(
      ColumnType.Int32,
      ColumnType.Int32,
      Operator.ReduceByKey,
      ReducerWitness.Other,
    ),
    outer(
      ColumnType.Int32,
      ColumnType.Int32,
      Operator.CountSumByKey,
      ReducerWitness.NotApplicable,
    ),
    outer(ColumnType.Int64, ColumnType.Int64, Operator.CountByKey, ReducerWitness.NotApplicable),
    outer(ColumnType.Int64, ColumnType.Int64, Operator.ReduceByKey, ReducerWitness.Other),
    outer(
      ColumnType.Utf8Dict,
      ColumnType.Int32,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
    ),
    outer(ColumnType.Utf8Dict, ColumnType.Int32, Operator.ReduceByKey, ReducerWitness.Other),
    Cell(
      label = "AssumedIntLong addsLongs outer Absence",
      layout = outerAssumed,
      operator = Operator.ReduceByKey,
      reducer = ReducerWitness.AddsLongs,
      nulls = NullAdmission.Policy(NullPolicy.Nullable),
      side = Side.Outer,
      choice = None,
    ),
    refusedScan(
      ColumnType.Int32,
      ColumnType.Int32,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
    ),
    refusedScan(
      ColumnType.Int64,
      ColumnType.Int64,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
    ),
    refusedScan(
      ColumnType.Utf8Dict,
      ColumnType.Int32,
      Operator.CountByKey,
      ReducerWitness.NotApplicable,
    ),
    Cell(
      label = "AssumedIntLong addsLongs scan nulls refused",
      layout = assumedIntLong,
      operator = Operator.ReduceByKey,
      reducer = ReducerWitness.AddsLongs,
      nulls = NullAdmission.Refused,
      side = Side.Inner,
      choice = None,
    ),
    Cell(
      label = "(Int32, Int32, Int32) countByKey Present",
      layout = joinColumns(NullPolicy.Present),
      operator = Operator.CountByKey,
      reducer = ReducerWitness.NotApplicable,
      nulls = NullAdmission.Policy(NullPolicy.Present),
      side = Side.Inner,
      choice = Some(KernelChoice.CountThroughProbe),
    ),
    Cell(
      label = "(Int32, Int32, Int32) countByKey Nullable",
      layout = joinColumns(NullPolicy.Nullable),
      operator = Operator.CountByKey,
      reducer = ReducerWitness.NotApplicable,
      nulls = NullAdmission.Policy(NullPolicy.Nullable),
      side = Side.Inner,
      choice = Some(KernelChoice.CountThroughProbe),
    ),
    // The join is three columns. Long addition does not make it IntLongProjectSum.
    Cell(
      label = "(Int32, Int32, Int32) reduceByKey AddsLongs",
      layout = joinColumns(NullPolicy.Nullable),
      operator = Operator.ReduceByKey,
      reducer = ReducerWitness.AddsLongs,
      nulls = NullAdmission.Policy(NullPolicy.Nullable),
      side = Side.Inner,
      choice = None,
    ),
    Cell(
      label = "(Int32, Int32, Int32) countByKey outer Absence",
      layout = Vector(
        ColumnLayout(ColumnType.Int32, NullPolicy.Nullable),
        ColumnLayout(ColumnType.Int32, NullPolicy.Nullable),
        ColumnLayout(ColumnType.Int32, NullPolicy.Absence),
      ),
      operator = Operator.CountByKey,
      reducer = ReducerWitness.NotApplicable,
      nulls = NullAdmission.Policy(NullPolicy.Absence),
      side = Side.Outer,
      choice = None,
    ),
    Cell(
      label = "(Int32, Int32, Int32) countByKey scan nulls refused",
      layout = joinColumns(NullPolicy.Nullable),
      operator = Operator.CountByKey,
      reducer = ReducerWitness.NotApplicable,
      nulls = NullAdmission.Refused,
      side = Side.Inner,
      choice = None,
    ),
    Cell(
      label = "(Int64, Int64, Int64) countByKey Nullable",
      layout = Vector(
        ColumnLayout(ColumnType.Int64, NullPolicy.Nullable),
        ColumnLayout(ColumnType.Int64, NullPolicy.Nullable),
        ColumnLayout(ColumnType.Int64, NullPolicy.Nullable),
      ),
      operator = Operator.CountByKey,
      reducer = ReducerWitness.NotApplicable,
      nulls = NullAdmission.Policy(NullPolicy.Nullable),
      side = Side.Inner,
      choice = None,
    ),
    Cell(
      label = "(Utf8Dict, Int32, Int32) countByKey Nullable",
      layout = Vector(
        ColumnLayout(ColumnType.Utf8Dict, NullPolicy.Nullable),
        ColumnLayout(ColumnType.Int32, NullPolicy.Nullable),
        ColumnLayout(ColumnType.Int32, NullPolicy.Nullable),
      ),
      operator = Operator.CountByKey,
      reducer = ReducerWitness.NotApplicable,
      nulls = NullAdmission.Policy(NullPolicy.Nullable),
      side = Side.Inner,
      choice = None,
    ),
    accepted(
      ColumnType.Int32,
      ColumnType.Int32,
      Operator.MapValues,
      ReducerWitness.NotApplicable,
      NullPolicy.Nullable,
      KernelChoice.MapValue,
    ),
    accepted(
      ColumnType.Int64,
      ColumnType.Int64,
      Operator.MapValues,
      ReducerWitness.NotApplicable,
      NullPolicy.Nullable,
      KernelChoice.MapValue,
    ),
    accepted(
      ColumnType.Utf8Dict,
      ColumnType.Int32,
      Operator.MapValues,
      ReducerWitness.NotApplicable,
      NullPolicy.Nullable,
      KernelChoice.MapValue,
    ),
    accepted(
      ColumnType.Int32,
      ColumnType.Int32,
      Operator.IntLongMapValue,
      ReducerWitness.NotApplicable,
      NullPolicy.Nullable,
      KernelChoice.IntLongMapValue,
    ),
    // Not accepted. This cell constructs no KernelChoice.
    Cell(
      label = "generic tuple map",
      layout = assumedIntLong,
      operator = Operator.TupleMap,
      reducer = ReducerWitness.Other,
      nulls = NullAdmission.Policy(NullPolicy.Nullable),
      side = Side.Inner,
      choice = None,
    ),
    // Named so a later edit does not tag Int => Long mapValues.
    Cell(
      label = "Int => Long mapValues",
      layout = assumedIntLong,
      operator = Operator.MapValues,
      reducer = ReducerWitness.AddsLongs,
      nulls = NullAdmission.Policy(NullPolicy.Nullable),
      side = Side.Inner,
      choice = None,
    ),
  )

  private val byLabel: Map[String, Cell] =
    table.iterator.map(row => row.label -> row).toMap

private final case class Cell(
    label: String,
    layout: Vector[ColumnLayout],
    operator: Operator,
    reducer: ReducerWitness,
    nulls: NullAdmission,
    side: Side,
    choice: Option[KernelChoice],
)
