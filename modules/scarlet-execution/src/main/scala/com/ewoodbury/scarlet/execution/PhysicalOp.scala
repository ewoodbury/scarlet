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
 * under [[PhysicalOp.HashAggregate]] and [[PhysicalOp.LongHashAggregate]], and on each side of
 * [[PhysicalOp.HashJoin]], [[PhysicalOp.LongHashJoin]], [[PhysicalOp.SortMergeJoin]],
 * [[PhysicalOp.SortMergeLeftOuter]], [[PhysicalOp.SortMergeRightOuter]],
 * [[PhysicalOp.SortMergeFullOuter]], [[PhysicalOp.HashLeftOuter]], [[PhysicalOp.HashRightOuter]],
 * and [[PhysicalOp.HashFullOuter]]. Those kernels perform the shuffle. Interpreting the boundary
 * on its own does not. [[PhysicalOp.BroadcastJoin]], [[PhysicalOp.BroadcastLeftOuter]],
 * [[PhysicalOp.BroadcastRightOuter]], and [[PhysicalOp.BroadcastFullOuter]] have no exchange.
 * [[PhysicalOp.BuildSide]] chooses which logical child of an inner broadcast join is built. The
 * planner records the right child. A later cost-based pass may choose the left child. The other
 * child is probed in place. A hash or broadcast left outer join always builds the right input. The
 * left input is the probe side. A hash or broadcast right outer join always builds the left input
 * and probes the right. A probe-side row with no partner clears the build value's validity bit.
 * That bit is absence. A null value is not absence. A sort-merge outer join has no build table.
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

  /**
   * In-memory hash reduce of `(Long, Long)` pairs. `child` is an [[Exchange]]. The kernel
   * partial-reduces, partitions with `Long.hashCode`, and reduces each bucket. There is no spill
   * path. Output kind is [[PhysicalOp.Kind.LongPairs]].
   */
  case LongHashAggregate(child: PhysicalOp)
  case HashJoin(left: PhysicalOp, right: PhysicalOp)

  /**
   * In-memory shuffle-hash inner join of `(Long, Long)` pairs. `left` and `right` are exchanges
   * with the same partition count. The kernel builds the left bucket and probes the right. A null
   * key is dropped on both sides by the exchange and is not inserted. Every value cell must be
   * present. The interpreter refuses a null value, so the plan stays on the row path. Output
   * columns are `(key, left, right)`. There is no spill and no build budget. Output kind is
   * [[PhysicalOp.Kind.LongTriples]].
   */
  case LongHashJoin(left: PhysicalOp, right: PhysicalOp)

  /**
   * In-memory sort-merge inner join of int pairs. `left` and `right` are exchanges with the same
   * partition count. Each partition is sorted by key and merged. Null keys are dropped on both
   * sides before the sort. A null payload of `0` does not match key `0`. Output columns are
   * `(key, left, right)`. One batch per partition. Outer joins do not use this node.
   */
  case SortMergeJoin(left: PhysicalOp, right: PhysicalOp)

  /**
   * Sort-merge left outer join of int pairs. `left` and `right` are exchanges with the same
   * partition count. The left exchange carries a null key to partition 0. The right exchange drops
   * a null key. Each partition is sorted by present key and merged. A null key does not match. A
   * left row with no right partner is kept, including a null key, and the right value's validity
   * bit is clear. That bit is absence. A preserved null key publishes a clear bit on the key
   * column. A null value is not absence and stays on the row path. Output columns are
   * `(key, left, right)`. One batch per partition.
   */
  case SortMergeLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Sort-merge right outer join of int pairs. `left` and `right` are exchanges. The right exchange
   * carries a null key to partition 0. The left exchange drops a null key. The left-outer walk
   * runs with the inputs swapped, then the value columns are swapped back to `(key, left, right)`.
   * Absence is the left value's clear bit. A null value stays on the row path. Output columns are
   * `(key, left, right)`. One batch per partition.
   */
  case SortMergeRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Sort-merge full outer join of int pairs. `left` and `right` are exchanges. Both exchanges
   * carry a null key to partition 0. A group on only one side is emitted, with the other value
   * absent. A null key on either side is emitted once and does not match. An empty side emits
   * every row of the other side. Either value's clear bit is absence. A null value stays on the
   * row path. Output columns are `(key, left, right)`. One batch per partition.
   */
  case SortMergeFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash left outer join. `left` and `right` are exchanges. The left input is the probe
   * side, and its exchange carries a null key to partition 0. The right input is the build side,
   * and its exchange drops a null key, so a null build key is not inserted. Each partition builds
   * one table and probes with the left-outer kernel. Output columns are `(key, left, right)`.
   * Absence is a probe-side row with no partner, and the right value's validity bit is clear. A
   * null value is not absence and stays on the row path. The build table uses the same budget as a
   * broadcast join. This is a different node from [[HashJoin]], so the inner join does not carry
   * null keys.
   */
  case HashLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash right outer join. `left` and `right` are exchanges. The right input is the probe
   * side, and its exchange carries a null key to partition 0. The left input is the build side,
   * and its exchange drops a null key, so a null build key is not inserted. Each partition builds
   * one table and probes with the left-outer kernel, then swaps the value columns back to
   * `(key, left, right)`. Absence is the left value's clear bit. A null value is not absence and
   * stays on the row path. The build table uses the same budget as a broadcast join.
   */
  case HashRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Inner join with no exchange. `left` and `right` stay the logical inputs. `buildSide` selects
   * which child fills the hash table. The other child is probed in place. The planner records
   * [[BuildSide.Right]]. A later cost-based pass may choose [[BuildSide.Left]]. Output kind is
   * triples `(key, left, right)`. Multiplicity, values, and null semantics are the contract. Order
   * is unspecified.
   */
  case BroadcastJoin(left: PhysicalOp, right: PhysicalOp, buildSide: PhysicalOp.BuildSide)

  /**
   * Left outer join with no exchange. `left` and `right` stay the logical inputs. The right input
   * fills the hash table. The left input is the probe side and is probed in place. There is no
   * build side: building the left input would keep unmatched right rows. Output columns are
   * `(key, left, right)`. Absence is a probe-side row with no partner, and the right value's
   * validity bit is clear. A null value is not absence and is not representable on this path. A
   * null value on either input stays on the row path. This is a different node from
   * [[BroadcastJoin]], so the inner join does not carry an outer flag.
   */
  case BroadcastLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Right outer join with no exchange. `left` and `right` stay the logical inputs. The left input
   * fills the hash table. The right input is the probe side and is probed in place. The left-outer
   * kernel writes `(key, probe value, build value)`, and the value columns are swapped back to
   * `(key, left, right)`. Absence is the left value's clear bit. A null value is not absence and
   * stays on the row path. This is a different node from [[BroadcastJoin]] and from
   * [[BroadcastLeftOuter]].
   */
  case BroadcastRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash full outer join. `left` and `right` are exchanges. Both exchanges carry a null
   * key to partition 0. The left input is the probe side. The right input is the build side. Each
   * partition builds one table, probes with one bitmap, and emits that partition's unmatched build
   * rows. A null build key is carried outside the table and emitted once. Output columns are
   * `(key, left, right)`. Either value's clear bit is absence. A null value is not absence and
   * stays on the row path. The build table uses the same budget as a broadcast join. The bitmap is
   * not part of that budget.
   */
  case HashFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Full outer join with no exchange. `left` and `right` stay the logical inputs. The right input
   * fills the hash table. The left input is the probe side and is probed in place. Each probe
   * thread keeps its own matched-row bitmap. The bitmaps are OR'd after the probes join. A clear
   * bit emits that build row with the left value absent. A null build key is carried outside the
   * table and emitted once. An empty probe still emits every build row. Output columns are
   * `(key, left, right)`. Either value's clear bit is absence. A null value stays on the row path.
   * The bitmap is not part of the build budget.
   */
  case BroadcastFullOuter(left: PhysicalOp, right: PhysicalOp)

  def kind: PhysicalOp.Kind = this match
    case Scan(sourceKind, _) => sourceKind
    case Filter(child, _) => child.kind
    case Project(child) => child.kind
    case Pipeline(child, _) => child.kind
    case Exchange(child) => child.kind
    case Column(_, _) => PhysicalOp.Kind.Ints
    case MapValue(_) => PhysicalOp.Kind.Pairs
    case HashAggregate(_) => PhysicalOp.Kind.Pairs
    case LongHashAggregate(_) => PhysicalOp.Kind.LongPairs
    case HashJoin(_, _) => PhysicalOp.Kind.Triples
    case LongHashJoin(_, _) => PhysicalOp.Kind.LongTriples
    case SortMergeJoin(_, _) => PhysicalOp.Kind.Triples
    case SortMergeLeftOuter(_, _) => PhysicalOp.Kind.Triples
    case SortMergeRightOuter(_, _) => PhysicalOp.Kind.Triples
    case SortMergeFullOuter(_, _) => PhysicalOp.Kind.Triples
    case HashLeftOuter(_, _) => PhysicalOp.Kind.Triples
    case HashRightOuter(_, _) => PhysicalOp.Kind.Triples
    case BroadcastJoin(_, _, _) => PhysicalOp.Kind.Triples
    case BroadcastLeftOuter(_, _) => PhysicalOp.Kind.Triples
    case BroadcastRightOuter(_, _) => PhysicalOp.Kind.Triples
    case HashFullOuter(_, _) => PhysicalOp.Kind.Triples
    case BroadcastFullOuter(_, _) => PhysicalOp.Kind.Triples

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
    case LongHashAggregate(child) => s"LongHashAggregate(${child.shape})"
    case HashJoin(left, right) => s"HashJoin(${left.shape}, ${right.shape})"
    case LongHashJoin(left, right) => s"LongHashJoin(${left.shape}, ${right.shape})"
    case SortMergeJoin(left, right) => s"SortMergeJoin(${left.shape}, ${right.shape})"
    case SortMergeLeftOuter(left, right) => s"SortMergeLeftOuter(${left.shape}, ${right.shape})"
    case SortMergeRightOuter(left, right) => s"SortMergeRightOuter(${left.shape}, ${right.shape})"
    case SortMergeFullOuter(left, right) => s"SortMergeFullOuter(${left.shape}, ${right.shape})"
    case HashLeftOuter(left, right) => s"HashLeftOuter(${left.shape}, ${right.shape})"
    case HashRightOuter(left, right) => s"HashRightOuter(${left.shape}, ${right.shape})"
    case BroadcastJoin(left, right, _) => s"BroadcastJoin(${left.shape}, ${right.shape})"
    case BroadcastLeftOuter(left, right) => s"BroadcastLeftOuter(${left.shape}, ${right.shape})"
    case BroadcastRightOuter(left, right) => s"BroadcastRightOuter(${left.shape}, ${right.shape})"
    case HashFullOuter(left, right) => s"HashFullOuter(${left.shape}, ${right.shape})"
    case BroadcastFullOuter(left, right) => s"BroadcastFullOuter(${left.shape}, ${right.shape})"

object PhysicalOp:
  enum Kind:
    case Ints, Longs, Floats, Pairs, Triples, Strings, Bools, LongPairs, LongTriples

  /** Which logical child of a broadcast join fills the build table. */
  enum BuildSide:
    case Left, Right

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
