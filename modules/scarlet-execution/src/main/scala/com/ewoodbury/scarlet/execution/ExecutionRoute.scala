package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.{
  ColumnLayout,
  ColumnType,
  KernelChoice,
  Layout,
  Output,
}

/**
 * Route from a lowered child to the kernel that runs it.
 *
 * Hash aggregates use the binder tag. [[KernelChoice.IntLongProjectSum]] is [[Arm.IntLongReduce]].
 * [[scala.None]] is [[Arm.IntHashAggregate]]: that decline must not fall through to
 * [[PhysicalSchema.isIntLongAggregate]]. A tag this hash node does not run fails the action.
 * [[KernelChoice.JoinProbeSum]] is a different physical node and stays on its own decoder.
 *
 * Pair counts, groups, keys of a group, distinct, `mapValues`, and key or value projection read
 * [[Layout.known]]. One binder tag covers the int, long, and string kernels, so the tag is not
 * that choice. A count whose schema is `[Int32, Int64]` is [[CountArm.IntLong]] before the pair
 * split. A hash aggregate of a triple project reports pair columns, so
 * [[PhysicalSchema.isIntLong]] is what still names that child. [[Layout.AssumedIntLong]] is the
 * project itself. [[PhysicalSchema.isIntLongAggregate]] is not this test.
 */
private[execution] object ExecutionRoute:

  /** Which hash-aggregate kernel runs. */
  enum Arm:
    case IntLongReduce
    case IntHashAggregate

  /** Int, long, or string pair columns. Absent when [[Layout.known]] is not one of those. */
  enum PairArm:
    case IntPairs
    case LongPairs
    case StringPairs

  /** `countByKey` under an exchange. [[CountArm.IntLong]] is not a pair arm. */
  enum CountArm:
    case IntLong
    case IntPairs
    case LongPairs
    case StringPairs

  /** `distinct` of one column. A long column is not an arm. */
  enum DistinctArm:
    case Ints
    case Strings

  /**
   * `mapValues`. A layout that is not a long pair or a string pair stays on the int kernel, which
   * is the arm a non-pair child already took.
   */
  enum MapValueArm:
    case IntValues
    case LongValues
    case StringValues

  /**
   * Arm for `reduceByKey` of `child`.
   *
   * `child` is the lowered child under the exchange, not the [[PhysicalOp.HashAggregate]] node.
   */
  def reduceArm(source: Plan[_], child: PhysicalOp, reducer: Any): Arm =
    AggregateBinding.choiceFor(
      source,
      child,
      Operator.ReduceByKey,
      AggregateBinding.witnessOf(reducer),
    ) match
      case Some(KernelChoice.IntLongProjectSum) => Arm.IntLongReduce
      case Some(KernelChoice.IntHashAggregate) => Arm.IntHashAggregate
      case Some(tag) =>
        throw new IllegalStateException(s"hash aggregate bound $tag")
      case None => Arm.IntHashAggregate

  /**
   * True when collect should decode this root as an int-long sum.
   *
   * Missing `plan`, a non-reduce root, and a hash aggregate the binder did not tag
   * [[KernelChoice.IntLongProjectSum]] are false. [[PhysicalOp.JoinProbeSum]] is false.
   */
  def decodesAsIntLongSum(plan: Option[Plan[_]], op: PhysicalOp): Boolean =
    (plan, op) match
      case (
            Some(Plan.ReduceByKeyOp(source, reducer)),
            PhysicalOp.HashAggregate(PhysicalOp.Exchange(child), _),
          ) =>
        reduceArm(source, child, reducer) match
          case Arm.IntLongReduce => true
          case Arm.IntHashAggregate => false
      case _ =>
        false

  /**
   * Pair family of `child`, or [[scala.None]] when the columns are not an int, long, or string
   * pair.
   *
   * Grouped values are [[Layout.Grouped]] and are [[scala.None]]. [[Layout.AssumedIntLong]] and
   * [[Layout.ProbeSum]] are [[scala.None]]. Null policy is not part of the family.
   */
  def pairArm(child: PhysicalOp): Option[PairArm] =
    Layout.known(child) match
      case Output.Columns(columns) =>
        columns match
          case Vector(key, value) => pairTypes(key.columnType, value.columnType)
          case _ => None
      case Output.Opaque(_) => None

  /** Pair family, or a failure that names `what`. A grouped child fails here. */
  def requirePair(child: PhysicalOp, what: String): PairArm =
    pairArm(child).getOrElse(
      throw new IllegalStateException(
        s"$what column is ${Layout.known(child)}, expected int, long, or string pairs",
      ),
    )

  /**
   * Count arm for the child under `CountByKey(Exchange(_))`.
   *
   * [[PhysicalSchema.isIntLong]] runs first. The three pair families are [[pairArm]]. A child that
   * is neither fails the action.
   */
  def countArm(child: PhysicalOp): CountArm =
    if (PhysicalSchema.isIntLong(child.schema)) CountArm.IntLong
    else
      requirePair(child, "countByKey") match
        case PairArm.IntPairs => CountArm.IntPairs
        case PairArm.LongPairs => CountArm.LongPairs
        case PairArm.StringPairs => CountArm.StringPairs

  /** Int column or string column. Any other layout fails the action. */
  def distinctArm(child: PhysicalOp): DistinctArm =
    val output = Layout.known(child)
    output match
      case Output.Columns(Vector(ColumnLayout(ColumnType.Int32, _))) => DistinctArm.Ints
      case Output.Columns(Vector(ColumnLayout(ColumnType.Utf8Dict, _))) => DistinctArm.Strings
      case Output.Columns(_) | Output.Opaque(_) =>
        throw new IllegalStateException(
          s"distinct column is $output, expected int or string",
        )

  /** Long pairs and string pairs select their kernels. Every other layout stays on ints. */
  def mapValueArm(child: PhysicalOp): MapValueArm =
    pairArm(child) match
      case Some(PairArm.LongPairs) => MapValueArm.LongValues
      case Some(PairArm.StringPairs) => MapValueArm.StringValues
      case Some(PairArm.IntPairs) | None => MapValueArm.IntValues

  /** String-pair columns. A filter of that layout is not [[FilterChain]]. */
  def stringPairs(child: PhysicalOp): Boolean =
    pairArm(child) match
      case Some(PairArm.StringPairs) => true
      case Some(PairArm.IntPairs) | Some(PairArm.LongPairs) | None => false

  private def pairTypes(key: ColumnType, value: ColumnType): Option[PairArm] =
    (key, value) match
      case (ColumnType.Int32, ColumnType.Int32) => Some(PairArm.IntPairs)
      case (ColumnType.Int64, ColumnType.Int64) => Some(PairArm.LongPairs)
      case (ColumnType.Utf8Dict, ColumnType.Int32) => Some(PairArm.StringPairs)
      case (ColumnType.Int32, ColumnType.Int64) | (ColumnType.Int32, ColumnType.Float64) |
          (ColumnType.Int32, ColumnType.Bool) | (ColumnType.Int32, ColumnType.Utf8Dict) |
          (ColumnType.Int64, ColumnType.Int32) | (ColumnType.Int64, ColumnType.Float64) |
          (ColumnType.Int64, ColumnType.Bool) | (ColumnType.Int64, ColumnType.Utf8Dict) |
          (ColumnType.Float64, _) | (ColumnType.Bool, _) |
          (ColumnType.Utf8Dict, ColumnType.Int64) | (ColumnType.Utf8Dict, ColumnType.Float64) |
          (ColumnType.Utf8Dict, ColumnType.Bool) | (ColumnType.Utf8Dict, ColumnType.Utf8Dict) =>
        None
