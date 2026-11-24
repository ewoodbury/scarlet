package com.ewoodbury.scarlet.execution.schema

import com.ewoodbury.scarlet.columnar.Selection
import com.ewoodbury.scarlet.execution.PhysicalOp
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.{NullAdmission, ReducerWitness, Side}

/**
 * One node a parent reads.
 *
 * `output` is [[Layout.known]] of `op`. It is not an input to
 * [[com.ewoodbury.scarlet.execution.PhysicalSchema]] or to `op.schema`. `kernel` is
 * [[Describe.tag]] of `op`. `selection` is the compiled slot steps, or empty. `reducer` is the
 * witness for this node, or [[ReducerWitness.NotApplicable]].
 *
 * A hash, broadcast, or sort-merge join reports three columns: key, left, and right. An outer join
 * marks the missing value [[NullPolicy.Absence]]. A triple [[PhysicalOp.Project]] reports
 * [[Layout.AssumedIntLong]], which is the aggregate's `(Int32, Int64)` input. That project is not
 * the join.
 */
private[execution] final case class Bound(
    op: PhysicalOp,
    output: Output,
    kernel: KernelChoice,
    selection: Vector[Selection.Step],
    reducer: ReducerWitness,
)

private[execution] object Bound:

  /** `output` and `kernel` come from `op`. This does not lower a plan. */
  def of(
      op: PhysicalOp,
      selection: Vector[Selection.Step],
      reducer: ReducerWitness,
  ): Bound =
    Bound(op, Layout.known(op), Describe.tag(op), selection, reducer)

  /**
   * Null policy stored on `bound`.
   *
   * [[NullAdmission.Refused]] is an opaque child. A lowered join carries [[NullPolicy]] on its
   * columns. [[NullPolicy.Absence]] is an outer partner.
   */
  def nulls(bound: Bound): NullAdmission = bound.output match
    case Output.Columns(columns) => NullAdmission.Policy(policy(columns))
    case Output.Opaque(_) => NullAdmission.Refused

  /** [[Side.Outer]] when a column is [[NullPolicy.Absence]]. */
  def side(bound: Bound): Side = bound.output match
    case Output.Columns(columns) if columns.exists(absent) => Side.Outer
    case Output.Columns(_) => Side.Inner
    case Output.Opaque(_) => Side.Inner

  private def policy(columns: Vector[ColumnLayout]): NullPolicy =
    if (columns.exists(absent)) then NullPolicy.Absence
    else if (columns.nonEmpty && columns.forall(present)) then NullPolicy.Present
    else NullPolicy.Nullable

  private def absent(column: ColumnLayout): Boolean = column.nullPolicy match
    case NullPolicy.Absence => true
    case NullPolicy.Present | NullPolicy.Nullable => false

  private def present(column: ColumnLayout): Boolean = column.nullPolicy match
    case NullPolicy.Present => true
    case NullPolicy.Nullable | NullPolicy.Absence => false
