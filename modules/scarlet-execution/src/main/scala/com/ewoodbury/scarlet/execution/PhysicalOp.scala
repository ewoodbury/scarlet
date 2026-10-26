package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Partition

/**
 * One columnar operator, lowered from a logical [[com.ewoodbury.scarlet.core.Plan]].
 *
 * User functions stay on the logical plan, so lowering and interpretation have to agree node for
 * node. A straight int, long, float, or string filter/project chain can be one
 * [[PhysicalOp.Pipeline]] node. A boolean filter chain is one [[PhysicalOp.Filter]] per step.
 * Other filters and projects remain one node per step. [[PhysicalOp.Project]] and `Pipeline` keep
 * the child kind. [[PhysicalOp.Column]] narrows a pair to one int column and shares its arrays.
 * [[PhysicalOp.MapValue]] rewrites an int value and keeps the pair. [[PhysicalOp.Exchange]] sits
 * under [[PhysicalOp.HashAggregate]] and on each side of [[PhysicalOp.HashJoin]]. Those kernels
 * perform the shuffle. Interpreting the boundary on its own does not. [[PhysicalOp.BroadcastJoin]]
 * has no exchange. Its right child is the build side and its left child is probed in place.
 */
enum PhysicalOp:
  case Scan(sourceKind: PhysicalOp.Kind, partitions: Seq[Partition[_]])
  case Filter(child: PhysicalOp, slot: PhysicalOp.Slot)
  case Project(child: PhysicalOp)

  /** One column of an int pair. Kind is [[PhysicalOp.Kind.Ints]]. */
  case Column(child: PhysicalOp, columnOrdinal: Int)

  /** `mapValues` whose new value is an unboxed int. Kind stays [[PhysicalOp.Kind.Pairs]]. */
  case MapValue(child: PhysicalOp)

  /**
   * An element pipeline for the outer `length` consecutive logical FilterOp/MapOp nodes.
   * Interpretation re-derives their kinds and reads their functions from Plan.
   */
  case Pipeline(child: PhysicalOp, length: Int)
  case Exchange(child: PhysicalOp)
  case HashAggregate(child: PhysicalOp)
  case HashJoin(left: PhysicalOp, right: PhysicalOp)

  /**
   * Inner join with no exchange. `probe` is the left input and `build` is the right input. The
   * right side is always the build side, including when it is the larger input. Output kind is
   * triples: `(key, left, right)`.
   */
  case BroadcastJoin(probe: PhysicalOp, build: PhysicalOp)

  def kind: PhysicalOp.Kind = this match
    case Scan(sourceKind, _) => sourceKind
    case Filter(child, _) => child.kind
    case Project(child) => child.kind
    case Pipeline(child, _) => child.kind
    case Exchange(child) => child.kind
    case Column(_, _) => PhysicalOp.Kind.Ints
    case MapValue(_) => PhysicalOp.Kind.Pairs
    case HashAggregate(_) => PhysicalOp.Kind.Pairs
    case HashJoin(_, _) => PhysicalOp.Kind.Triples
    case BroadcastJoin(_, _) => PhysicalOp.Kind.Triples

  /** Operator nesting, without data. */
  def shape: String = this match
    case Scan(_, _) => "Scan"
    case Filter(child, slot) => s"${slot.label}(${child.shape})"
    case Project(child) => s"Project(${child.shape})"
    case Pipeline(child, length) => s"Pipeline$length(${child.shape})"
    case Exchange(child) => s"Exchange(${child.shape})"
    case Column(child, columnOrdinal) =>
      val label = columnOrdinal match
        case 0 => "Key"
        case 1 => "Value"
        case other => s"Column$other"
      s"$label(${child.shape})"
    case MapValue(child) => s"MapValue(${child.shape})"
    case HashAggregate(child) => s"HashAggregate(${child.shape})"
    case HashJoin(left, right) => s"HashJoin(${left.shape}, ${right.shape})"
    case BroadcastJoin(probe, build) => s"BroadcastJoin(${probe.shape}, ${build.shape})"

object PhysicalOp:
  enum Kind:
    case Ints, Longs, Floats, Pairs, Triples, Strings, Bools

  /**
   * `Element` is the single column, an int, a long, a double, a boolean, or a string, and its
   * shape label is `Filter`. `Row` is a predicate on both columns of a pair. `Key` and `Value`
   * name which pair column. The triple slots select one of the three int columns in a join result.
   */
  enum Slot:
    case Element, Row, Key, Value, TripleKey, TripleLeftValue, TripleRightValue

    def label: String = this match
      case Element => "Filter"
      case Row => "FilterRow"
      case Slot.Key => "FilterKey"
      case Slot.Value => "FilterValue"
      case TripleKey => "FilterTripleKey"
      case TripleLeftValue => "FilterTripleLeftValue"
      case TripleRightValue => "FilterTripleRightValue"
