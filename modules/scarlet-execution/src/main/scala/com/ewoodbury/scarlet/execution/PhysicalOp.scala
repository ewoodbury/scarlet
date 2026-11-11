package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.Partition

/**
 * One columnar operator, lowered from a logical [[com.ewoodbury.scarlet.core.Plan]].
 *
 * User functions stay on the logical plan, so lowering and interpretation have to agree node for
 * node. A straight int, long, float, or string filter/project chain can be one
 * [[PhysicalOp.Pipeline]] node. A boolean filter chain is one [[PhysicalOp.Filter]] per step.
 * Other filters and projects remain one node per step. [[PhysicalOp.Project]] and `Pipeline` keep
 * the child kind. [[PhysicalOp.Column]] narrows an int or long pair to one column and shares its
 * arrays. [[PhysicalOp.MapValue]] rewrites an int or long value and keeps the pair.
 * [[PhysicalOp.Exchange]] sits under [[PhysicalOp.HashAggregate]],
 * [[PhysicalOp.LongHashAggregate]], [[PhysicalOp.StringHashAggregate]], [[PhysicalOp.GroupByKey]],
 * and [[PhysicalOp.CountByKey]], and on each side of [[PhysicalOp.HashJoin]],
 * [[PhysicalOp.LongHashJoin]], [[PhysicalOp.StringHashJoin]], [[PhysicalOp.SortMergeJoin]],
 * [[PhysicalOp.SortMergeLeftOuter]], [[PhysicalOp.SortMergeRightOuter]],
 * [[PhysicalOp.SortMergeFullOuter]], [[PhysicalOp.HashLeftOuter]], [[PhysicalOp.HashRightOuter]],
 * and [[PhysicalOp.HashFullOuter]]. Those kernels perform the shuffle. Interpreting the boundary
 * on its own does not. [[PhysicalOp.BroadcastJoin]], [[PhysicalOp.BroadcastLeftOuter]],
 * [[PhysicalOp.BroadcastRightOuter]], and [[PhysicalOp.BroadcastFullOuter]] have no exchange.
 * [[PhysicalOp.BuildSide]] chooses which logical child of an inner broadcast join is built. The
 * planner records the right child. A later cost-based pass may choose the left child. The other
 * child is probed in place. A hash or broadcast left outer join always builds the right input. The
 * left input is the probe side. A hash or broadcast right outer join always builds the left input
 * and probes the right. A probe-side row with no partner sets the build value's absence bit. A
 * null value is not absence. A sort-merge outer join has no build table. Long outer joins are the
 * `LongHash*Outer`, `LongBroadcast*Outer`, and `LongSortMerge*Outer` nodes. Their kind is
 * [[PhysicalOp.Kind.LongOuter]]. String outer joins are the `StringHash*Outer`,
 * `StringBroadcast*Outer`, and `StringSortMerge*Outer` nodes. Their kind is
 * [[PhysicalOp.Kind.StringOuter]]. Hash and broadcast do not spill. Sort-merge runs in memory and
 * throws instead of spilling.
 */
enum PhysicalOp:
  case Scan(sourceKind: PhysicalOp.Kind, partitions: Seq[Partition[_]])
  case Filter(child: PhysicalOp, slot: PhysicalOp.Slot)
  case Project(child: PhysicalOp)

  /**
   * One column of an int, long, or string pair. Kind is [[PhysicalOp.Kind.Ints]] or
   * [[PhysicalOp.Kind.Longs]], or [[PhysicalOp.Kind.Strings]] for a string key.
   */
  case Column(child: PhysicalOp, columnOrdinal: Int)

  /**
   * `mapValues` whose new value is an unboxed int or long. Kind stays the pair kind, including
   * [[PhysicalOp.Kind.StringPairs]].
   */
  case MapValue(child: PhysicalOp)

  /**
   * An element pipeline for the outer `length` consecutive logical FilterOp/MapOp nodes.
   * Interpretation re-derives their kinds and reads their functions from Plan.
   */
  case Pipeline(child: PhysicalOp, length: Int)

  /** Concatenation of two same-kind inputs. Ordering is unknown; both sides are encoded. */
  case Union(left: PhysicalOp, right: PhysicalOp)

  /** Whole-row dedupe of an [[Exchange]] child. One row per distinct value, order unspecified. */
  case Dedupe(child: PhysicalOp)
  case Exchange(child: PhysicalOp)
  case HashAggregate(child: PhysicalOp)

  /**
   * In-memory hash reduce of `(Long, Long)` pairs. `child` is an [[Exchange]]. The kernel
   * partial-reduces, partitions with `Long.hashCode`, and reduces each bucket. There is no spill
   * path. Output kind is [[PhysicalOp.Kind.LongPairs]].
   */
  case LongHashAggregate(child: PhysicalOp)

  /**
   * In-memory hash reduce of `(String, Int)` pairs. `child` is an [[Exchange]]. The kernel
   * partial-reduces by dictionary code, partitions with `StringHash`, and reduces each bucket. A
   * null key is dropped. `""` is kept. A null value stays on the row path. There is no spill path.
   * Output kind is [[PhysicalOp.Kind.StringPairs]].
   */
  case StringHashAggregate(child: PhysicalOp)
  case HashJoin(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash inner join of `(Long, Long)` pairs. `left` and `right` are exchanges with the
   * same partition count. The kernel builds the left bucket and probes the right. A null key is
   * dropped on both sides by the exchange and is not inserted. A clear validity bit is a null
   * value and is copied with the matched row. It is not an absence bit. Output columns are
   * `(key, left, right)`. When `spillEnabled` is on and a build bucket's retained estimate is
   * above `spillBudgetBytes`, that bucket is sub-partitioned by the quotient of `Long.hashCode`
   * and the parent width. Each non-empty build piece is one spill file. One piece is probed at a
   * time. A piece still over the budget throws. At or under the budget, and whenever spill is off,
   * the bucket stays in memory and writes nothing. Broadcast, sort-merge, and outer joins on long
   * keys do not use this node. Output kind is [[PhysicalOp.Kind.LongTriples]].
   */
  case LongHashJoin(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash inner join of `(String, Int)` pairs. `left` and `right` are exchanges with the
   * same partition count. The kernel builds the left bucket and probes the right. The two sides
   * keep independent dictionaries, so the table matches strings, not dictionary codes. A null key
   * is dropped on both sides by the exchange. `""` is a present key. A clear validity bit is a
   * null value and is copied with the matched row. It is not an absence bit. A key or value filter
   * on either side, including a stack of one, scatters survivor ids and does not slice them.
   * Output columns are `(key, left, right)`, and the key is the probe row. Spill is off unless
   * `spillEnabled` is set. A build bucket whose retained estimate, including its dictionary, is
   * above `spillBudgetBytes` is sub-partitioned and joined one piece at a time. A bucket at or
   * under the budget, and every bucket while spill is off, stays in memory. Broadcast, sort-merge,
   * and outer joins do not use this node. Output kind is [[PhysicalOp.Kind.StringTriples]].
   */
  case StringHashJoin(left: PhysicalOp, right: PhysicalOp)

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
   * left row with no right partner is kept, including a null key, and the right value's absence
   * bit is set. Its validity bit stays clear. A preserved null key publishes a clear bit on the
   * key column. A null value is not absence and stays on the row path. Output columns are
   * `(key, left, right)`. One batch per partition.
   */
  case SortMergeLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Sort-merge right outer join of int pairs. `left` and `right` are exchanges. The right exchange
   * carries a null key to partition 0. The left exchange drops a null key. The left-outer walk
   * runs with the inputs swapped, then the value columns are swapped back to `(key, left, right)`.
   * Absence is the left value's absence bit. A null value stays on the row path. Output columns
   * are `(key, left, right)`. One batch per partition.
   */
  case SortMergeRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Sort-merge full outer join of int pairs. `left` and `right` are exchanges. Both exchanges
   * carry a null key to partition 0. A group on only one side is emitted, with the other value
   * absent. A null key on either side is emitted once and does not match. An empty side emits
   * every row of the other side. Either value's absence bit is a missing partner. A null value
   * stays on the row path. Output columns are `(key, left, right)`. One batch per partition.
   */
  case SortMergeFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash left outer join. `left` and `right` are exchanges. The left input is the probe
   * side, and its exchange carries a null key to partition 0. The right input is the build side,
   * and its exchange drops a null key, so a null build key is not inserted. Each partition builds
   * one table and probes with the left-outer kernel. Output columns are `(key, left, right)`.
   * Absence is a probe-side row with no partner, and the right value's absence bit is set. A null
   * value is a clear validity bit and stays columnar. The build table uses the same budget as a
   * broadcast join. This is a different node from [[HashJoin]], so the inner join does not carry
   * null keys.
   */
  case HashLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash right outer join. `left` and `right` are exchanges. The right input is the probe
   * side, and its exchange carries a null key to partition 0. The left input is the build side,
   * and its exchange drops a null key, so a null build key is not inserted. Each partition builds
   * one table and probes with the left-outer kernel, then swaps the value columns back to
   * `(key, left, right)`. Absence is the left value's absence bit. A null value is not absence and
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
   * absence bit is set. A null value is not absence and is not representable on this path. A null
   * value on either input stays on the row path. This is a different node from [[BroadcastJoin]],
   * so the inner join does not carry an outer flag.
   */
  case BroadcastLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Right outer join with no exchange. `left` and `right` stay the logical inputs. The left input
   * fills the hash table. The right input is the probe side and is probed in place. The left-outer
   * kernel writes `(key, probe value, build value)`, and the value columns are swapped back to
   * `(key, left, right)`. Absence is the left value's absence bit. A null value is not absence and
   * stays on the row path. This is a different node from [[BroadcastJoin]] and from
   * [[BroadcastLeftOuter]].
   */
  case BroadcastRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash full outer join. `left` and `right` are exchanges. Both exchanges carry a null
   * key to partition 0. The left input is the probe side. The right input is the build side. Each
   * partition builds one table, probes with one bitmap, and emits that partition's unmatched build
   * rows. A null build key is carried outside the table and emitted once. Output columns are
   * `(key, left, right)`. Either value's absence bit is a missing partner. A null value is not
   * absence and stays on the row path. The build table uses the same budget as a broadcast join.
   * The bitmap is not part of that budget.
   */
  case HashFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Full outer join with no exchange. `left` and `right` stay the logical inputs. The right input
   * fills the hash table. The left input is the probe side and is probed in place. Each probe
   * thread keeps its own matched-row bitmap. The bitmaps are OR'd after the probes join. A clear
   * bit emits that build row with the left value absent. A null build key is carried outside the
   * table and emitted once. An empty probe still emits every build row. Output columns are
   * `(key, left, right)`. Either value's absence bit is a missing partner. A null value stays on
   * the row path. The bitmap is not part of the build budget.
   */
  case BroadcastFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash left outer join of `(Long, Long)` pairs. Both children are exchanges. The left
   * exchange carries a null key to partition 0. The right exchange drops one. The build table uses
   * `broadcastBuildBudgetBytes` per partition and does not spill. Output kind is
   * [[PhysicalOp.Kind.LongOuter]]. A null value stays on the row path.
   */
  case LongHashLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash right outer join of `(Long, Long)` pairs. Both children are exchanges. The right
   * side is the probe and carries a null key. The value columns are swapped back to
   * `(key, left, right)`. Output kind is [[PhysicalOp.Kind.LongOuter]].
   */
  case LongHashRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash full outer join of `(Long, Long)` pairs. Both exchanges carry a null key. The
   * matched-row bitmap and the null-key copy are not part of the build budget. Output kind is
   * [[PhysicalOp.Kind.LongOuter]].
   */
  case LongHashFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Broadcast left outer join of `(Long, Long)` pairs. No exchange. Kind
   * [[PhysicalOp.Kind.LongOuter]].
   */
  case LongBroadcastLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Broadcast right outer join of `(Long, Long)` pairs. No exchange. Kind
   * [[PhysicalOp.Kind.LongOuter]].
   */
  case LongBroadcastRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Broadcast full outer join of `(Long, Long)` pairs. No exchange. Kind
   * [[PhysicalOp.Kind.LongOuter]].
   */
  case LongBroadcastFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Sort-merge left outer join of `(Long, Long)` pairs. Both children are exchanges. This runs in
   * memory. Over `spillBudgetBytes` with spill on it throws and writes nothing. Output kind is
   * [[PhysicalOp.Kind.LongOuter]].
   */
  case LongSortMergeLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /** Sort-merge right outer join of `(Long, Long)` pairs. Kind [[PhysicalOp.Kind.LongOuter]]. */
  case LongSortMergeRightOuter(left: PhysicalOp, right: PhysicalOp)

  /** Sort-merge full outer join of `(Long, Long)` pairs. Kind [[PhysicalOp.Kind.LongOuter]]. */
  case LongSortMergeFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash left outer join of `(String, Int)` pairs. Both children are exchanges. The left
   * exchange carries a null key to partition 0. The right exchange drops one. Keys are dictionary
   * UTF-8. The sides keep independent dictionaries. The table matches strings, not codes. `""` is
   * present. `"é"` does not match `"e\u0301"`. A clear key bit is tested before the dictionary is
   * indexed. The build table uses `broadcastBuildBudgetBytes` per partition and does not spill.
   * The estimate is `StringHashJoinSpill.retainedBytes`, which includes the dictionary. The
   * matched-row bitmap is not part of that budget. Output kind is [[PhysicalOp.Kind.StringOuter]].
   * Columns are `(key, left, right)`: `Utf8Dict`, `Int32`, `Int32`. Values use
   * `Int32Column.withAbsence`. The key column has no absence field. A null value stays on the row
   * path.
   */
  case StringHashLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash right outer join of `(String, Int)` pairs. Both children are exchanges. The right
   * side is the probe and carries a null key. The value columns are swapped back to
   * `(key, left, right)`. Output kind is [[PhysicalOp.Kind.StringOuter]].
   */
  case StringHashRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash full outer join of `(String, Int)` pairs. Both exchanges carry a null key. The
   * matched-row bitmap and the null-key copy are not part of the build budget. Output kind is
   * [[PhysicalOp.Kind.StringOuter]].
   */
  case StringHashFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Broadcast left outer join of `(String, Int)` pairs. No exchange. Kind
   * [[PhysicalOp.Kind.StringOuter]].
   */
  case StringBroadcastLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Broadcast right outer join of `(String, Int)` pairs. No exchange. Kind
   * [[PhysicalOp.Kind.StringOuter]].
   */
  case StringBroadcastRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Broadcast full outer join of `(String, Int)` pairs. No exchange. Kind
   * [[PhysicalOp.Kind.StringOuter]].
   */
  case StringBroadcastFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Sort-merge left outer join of `(String, Int)` pairs. Both children are exchanges. This runs in
   * memory. Over `spillBudgetBytes` with spill on it throws and writes nothing. Output kind is
   * [[PhysicalOp.Kind.StringOuter]].
   */
  case StringSortMergeLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Sort-merge right outer join of `(String, Int)` pairs. Kind [[PhysicalOp.Kind.StringOuter]].
   */
  case StringSortMergeRightOuter(left: PhysicalOp, right: PhysicalOp)

  /** Sort-merge full outer join of `(String, Int)` pairs. Kind [[PhysicalOp.Kind.StringOuter]]. */
  case StringSortMergeFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * `groupByKey`. The child is an [[PhysicalOp.Exchange]]. The kernel shuffles. A null key is
   * dropped. A null value is kept, and a clear value bit is a planner bug. One grouped batch is
   * published per shuffle partition, including an empty bucket. Output kind is
   * [[PhysicalOp.Kind.GroupedInts]] for a [[PhysicalOp.Kind.Pairs]] child,
   * [[PhysicalOp.Kind.GroupedStrings]] for a [[PhysicalOp.Kind.StringPairs]] child, and
   * [[PhysicalOp.Kind.GroupedLongs]] for a [[PhysicalOp.Kind.LongPairs]] child. Any other child is
   * a planner bug. There is no partial combine and no sort. Spill off keeps the in-memory groups
   * and writes nothing. Spill on slices one over-budget post-exchange bucket in input order. A
   * morsel whose group table does not fit throws
   * [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]] before that table. The appended groups
   * are not bounded by the budget.
   */
  case GroupByKey(child: PhysicalOp)

  /**
   * `countByKey`. The child is an [[PhysicalOp.Exchange]]. The kernel shuffles. A null key is
   * dropped. The value column is not read, so a null value still counts. One `(key, count)` row is
   * published per distinct key, including an empty bucket. Output kind is
   * [[PhysicalOp.Kind.Pairs]] for a [[PhysicalOp.Kind.Pairs]] child,
   * [[PhysicalOp.Kind.LongCounts]] for a [[PhysicalOp.Kind.LongPairs]] child, and
   * [[PhysicalOp.Kind.StringPairs]] for a [[PhysicalOp.Kind.StringPairs]] child. Any other child
   * is a planner bug. The count is an `Int` and wraps. A long count is `[Int64, Int32]`. The long
   * estimate charges 8-byte keys. The string estimate charges dictionary bytes. With spill off, an
   * input that does not fit the budget throws
   * [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]] before the table is allocated, and the
   * action fails instead of rerunning on rows. With spill on, an over-budget post-exchange bucket
   * spills. A count table that still does not fit throws
   * [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]]. The running partial is `O(distinct
   * keys)`.
   */
  case CountByKey(child: PhysicalOp)

  /**
   * Global sort of an int-pair scan. Each source partition is sorted by effective key, then the
   * sorted runs are rank-merged into `defaultShufflePartitions`. A null key orders as zero and
   * stays null. A null value stays null. Ties keep input order. Output kind is
   * [[PhysicalOp.Kind.Pairs]].
   *
   * The ordering tag is [[OrderingTag.Sorted]] with `Seq(0)` and `Seq(true)`. That is ascending by
   * effective key: a null key orders as zero and stays null in the output. The tag is not a claim
   * that column 0 is non-null.
   */
  case SortByIntKey(child: PhysicalOp)

  /**
   * Global sort of a long-pair scan. Each source partition is sorted by effective key, then the
   * sorted runs are rank-merged into `defaultShufflePartitions`. A null key orders as zero and
   * stays null. A null value stays null. Ties keep input order. Output kind is
   * [[PhysicalOp.Kind.LongPairs]].
   *
   * The ordering tag is [[OrderingTag.Sorted]] with `Seq(0)` and `Seq(true)`. That is ascending by
   * effective key: a null key orders as zero and stays null in the output. The tag is not a claim
   * that column 0 is non-null. Over the budget, with spill on, each input partition is one sorted
   * run and the runs are merged. One input partition is still sorted in memory. At or under the
   * budget, and whenever spill is off, the in-memory split runs and writes nothing.
   */
  case SortByLongKey(child: PhysicalOp)

  /**
   * Global sort of a string-pair scan. Each source partition is sorted by effective key, then the
   * sorted runs are rank-merged into `defaultShufflePartitions`. A present key orders by
   * `String.compareTo` (UTF-16 code units). A null key orders as `""` and stays null. Ties keep
   * input order. Output kind is [[PhysicalOp.Kind.StringPairs]]. Over the budget, with spill on,
   * each input partition is one sorted run and the runs are merged. One input partition is still
   * sorted in memory. At or under the budget, and whenever spill is off, the in-memory split runs
   * and writes nothing.
   *
   * The ordering tag is [[OrderingTag.Sorted]] with `Seq(0)` and `Seq(true)`. That is ascending by
   * effective key: a null key orders as `""` and stays null in the output. The tag is not a claim
   * that column 0 is non-null.
   */
  case SortByStringKey(child: PhysicalOp)

  def kind: PhysicalOp.Kind = this match
    case Scan(sourceKind, _) => sourceKind
    case Filter(child, _) => child.kind
    case Project(child) => child.kind
    case Pipeline(child, _) => child.kind
    case Union(left, _) => left.kind
    case Dedupe(child) => child.kind
    case Exchange(child) => child.kind
    case Column(child, ordinal) =>
      child.kind match
        case PhysicalOp.Kind.LongPairs => PhysicalOp.Kind.Longs
        case PhysicalOp.Kind.StringPairs if ordinal == 0 => PhysicalOp.Kind.Strings
        case PhysicalOp.Kind.StringPairs => PhysicalOp.Kind.Ints
        case _ => PhysicalOp.Kind.Ints
    case MapValue(child) =>
      child.kind match
        case PhysicalOp.Kind.LongPairs => PhysicalOp.Kind.LongPairs
        case PhysicalOp.Kind.StringPairs => PhysicalOp.Kind.StringPairs
        case _ => PhysicalOp.Kind.Pairs
    case SortByIntKey(_) => PhysicalOp.Kind.Pairs
    case SortByLongKey(_) => PhysicalOp.Kind.LongPairs
    case SortByStringKey(_) => PhysicalOp.Kind.StringPairs
    case HashAggregate(_) => PhysicalOp.Kind.Pairs
    case LongHashAggregate(_) => PhysicalOp.Kind.LongPairs
    case StringHashAggregate(_) => PhysicalOp.Kind.StringPairs
    case GroupByKey(child) =>
      child.kind match
        case PhysicalOp.Kind.Pairs => PhysicalOp.Kind.GroupedInts
        case PhysicalOp.Kind.StringPairs => PhysicalOp.Kind.GroupedStrings
        case PhysicalOp.Kind.LongPairs => PhysicalOp.Kind.GroupedLongs
        case other =>
          throw new IllegalStateException(
            s"groupByKey column is $other, expected int, long, or string pairs",
          )
    case CountByKey(child) =>
      child.kind match
        case PhysicalOp.Kind.Pairs => PhysicalOp.Kind.Pairs
        case PhysicalOp.Kind.LongPairs => PhysicalOp.Kind.LongCounts
        case PhysicalOp.Kind.StringPairs => PhysicalOp.Kind.StringPairs
        case other =>
          throw new IllegalStateException(
            s"countByKey column is $other, expected int, long, or string pairs",
          )
    case HashJoin(_, _) => PhysicalOp.Kind.Triples
    case LongHashJoin(_, _) => PhysicalOp.Kind.LongTriples
    case StringHashJoin(_, _) => PhysicalOp.Kind.StringTriples
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
    case LongHashLeftOuter(_, _) | LongHashRightOuter(_, _) | LongHashFullOuter(_, _) |
        LongBroadcastLeftOuter(_, _) | LongBroadcastRightOuter(_, _) |
        LongBroadcastFullOuter(_, _) | LongSortMergeLeftOuter(_, _) |
        LongSortMergeRightOuter(_, _) | LongSortMergeFullOuter(_, _) =>
      PhysicalOp.Kind.LongOuter
    case StringHashLeftOuter(_, _) | StringHashRightOuter(_, _) | StringHashFullOuter(_, _) |
        StringBroadcastLeftOuter(_, _) | StringBroadcastRightOuter(_, _) |
        StringBroadcastFullOuter(_, _) | StringSortMergeLeftOuter(_, _) |
        StringSortMergeRightOuter(_, _) | StringSortMergeFullOuter(_, _) =>
      PhysicalOp.Kind.StringOuter

  /**
   * An int inner join, or a filter or project stacked on one. An outer join is a triple batch with
   * an absent side, so a whole-row filter or a map on it stays on the row path.
   */
  def isInnerTriple: Boolean = this match
    case HashJoin(_, _) | SortMergeJoin(_, _) | BroadcastJoin(_, _, _) => true
    case Filter(child, _) => child.isInnerTriple
    case Project(child) => child.isInnerTriple
    case _ => false

  /**
   * A null value can ride along. A whole-row filter or a map above an inner join still wants that
   * bit. A slot filter does not: its kernel rejects a null component.
   */
  def keepsTripleAbsence: Boolean = this match
    case HashJoin(_, _) | SortMergeJoin(_, _) | BroadcastJoin(_, _, _) => true
    case Filter(child, PhysicalOp.Slot.TripleRow) => child.keepsTripleAbsence
    case Project(child) => child.keepsTripleAbsence
    case _ => false

  /**
   * Root of a whole-row filter or a map. [[ColumnarInterpreter.interpret]] asks the join to keep
   * null values only for this root. A bare join stays on the unboxing encode.
   */
  def requestsTripleAbsence: Boolean = this match
    case Project(child) => child.keepsTripleAbsence
    case Filter(child, PhysicalOp.Slot.TripleRow) => child.keepsTripleAbsence
    case _ => false

  /** Operator nesting, without data. */
  def shape: String = this match
    case Scan(_, _) => "Scan"
    case Filter(child, slot) => s"${slot.label}(${child.shape})"
    case Project(child) => s"Project(${child.shape})"
    case Pipeline(child, length) => s"Pipeline$length(${child.shape})"
    case Union(left, right) => s"Union(${left.shape}, ${right.shape})"
    case Dedupe(child) => s"Dedupe(${child.shape})"
    case Exchange(child) => s"Exchange(${child.shape})"
    case Column(child, columnOrdinal) =>
      val label = columnOrdinal match
        case 0 => "Key"
        case 1 => "Value"
        case other => s"Column$other"
      s"$label(${child.shape})"
    case MapValue(child) => s"MapValue(${child.shape})"
    case SortByIntKey(child) => s"SortByIntKey(${child.shape})"
    case SortByLongKey(child) => s"SortByLongKey(${child.shape})"
    case SortByStringKey(child) => s"SortByStringKey(${child.shape})"
    case HashAggregate(child) => s"HashAggregate(${child.shape})"
    case LongHashAggregate(child) => s"LongHashAggregate(${child.shape})"
    case StringHashAggregate(child) => s"StringHashAggregate(${child.shape})"
    case GroupByKey(child) => s"GroupByKey(${child.shape})"
    case CountByKey(child) => s"CountByKey(${child.shape})"
    case HashJoin(left, right) => s"HashJoin(${left.shape}, ${right.shape})"
    case LongHashJoin(left, right) => s"LongHashJoin(${left.shape}, ${right.shape})"
    case StringHashJoin(left, right) => s"StringHashJoin(${left.shape}, ${right.shape})"
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
    case LongHashLeftOuter(left, right) => s"LongHashLeftOuter(${left.shape}, ${right.shape})"
    case LongHashRightOuter(left, right) => s"LongHashRightOuter(${left.shape}, ${right.shape})"
    case LongHashFullOuter(left, right) => s"LongHashFullOuter(${left.shape}, ${right.shape})"
    case LongBroadcastLeftOuter(left, right) =>
      s"LongBroadcastLeftOuter(${left.shape}, ${right.shape})"
    case LongBroadcastRightOuter(left, right) =>
      s"LongBroadcastRightOuter(${left.shape}, ${right.shape})"
    case LongBroadcastFullOuter(left, right) =>
      s"LongBroadcastFullOuter(${left.shape}, ${right.shape})"
    case LongSortMergeLeftOuter(left, right) =>
      s"LongSortMergeLeftOuter(${left.shape}, ${right.shape})"
    case LongSortMergeRightOuter(left, right) =>
      s"LongSortMergeRightOuter(${left.shape}, ${right.shape})"
    case LongSortMergeFullOuter(left, right) =>
      s"LongSortMergeFullOuter(${left.shape}, ${right.shape})"
    case StringHashLeftOuter(left, right) => s"StringHashLeftOuter(${left.shape}, ${right.shape})"
    case StringHashRightOuter(left, right) =>
      s"StringHashRightOuter(${left.shape}, ${right.shape})"
    case StringHashFullOuter(left, right) => s"StringHashFullOuter(${left.shape}, ${right.shape})"
    case StringBroadcastLeftOuter(left, right) =>
      s"StringBroadcastLeftOuter(${left.shape}, ${right.shape})"
    case StringBroadcastRightOuter(left, right) =>
      s"StringBroadcastRightOuter(${left.shape}, ${right.shape})"
    case StringBroadcastFullOuter(left, right) =>
      s"StringBroadcastFullOuter(${left.shape}, ${right.shape})"
    case StringSortMergeLeftOuter(left, right) =>
      s"StringSortMergeLeftOuter(${left.shape}, ${right.shape})"
    case StringSortMergeRightOuter(left, right) =>
      s"StringSortMergeRightOuter(${left.shape}, ${right.shape})"
    case StringSortMergeFullOuter(left, right) =>
      s"StringSortMergeFullOuter(${left.shape}, ${right.shape})"

object PhysicalOp:
  enum Kind:
    case Ints, Longs, Floats, Pairs, Triples, Strings, Bools, LongPairs, LongCounts, LongTriples,
      StringPairs, StringTriples, GroupedInts, GroupedStrings, GroupedLongs, LongOuter, StringOuter

  /** Which logical child of a broadcast join fills the build table. */
  enum BuildSide:
    case Left, Right

  /**
   * `Element` is the single column, an int, a long, a double, a boolean, or a string, and its
   * shape label is `Filter`. `Row` is a predicate on both columns of a pair. `Key` and `Value`
   * name which pair column. The triple column slots select one of the three int columns in a join
   * result. `TripleRow` is a predicate on the whole `(Int, (Int, Int))` row.
   */
  enum Slot:
    case Element, Row, Key, Value, TripleKey, TripleLeftValue, TripleRightValue, TripleRow

    def label: String = this match
      case Element => "Filter"
      case Row => "FilterRow"
      case Slot.Key => "FilterKey"
      case Slot.Value => "FilterValue"
      case TripleKey => "FilterTripleKey"
      case TripleLeftValue => "FilterTripleLeftValue"
      case TripleRightValue => "FilterTripleRightValue"
      case TripleRow => "FilterTripleRow"
