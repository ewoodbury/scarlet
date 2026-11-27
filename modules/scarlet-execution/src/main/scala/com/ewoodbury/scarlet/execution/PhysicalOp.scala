package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.LogicalType
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
 * [[PhysicalOp.GroupedDecode]] is one map directly above [[PhysicalOp.GroupByKey]]. The function
 * stays on the logical plan. Execution decodes the grouped batch once and applies it to each
 * `(K, Iterable[V])`. [[PhysicalOp.Exchange]] sits under [[PhysicalOp.HashAggregate]],
 * [[PhysicalOp.LongHashAggregate]], [[PhysicalOp.StringHashAggregate]], [[PhysicalOp.GroupByKey]],
 * [[PhysicalOp.CountByKey]], and [[PhysicalOp.CountSum]], and on each side of
 * [[PhysicalOp.HashJoin]], [[PhysicalOp.JoinProbeSum]], [[PhysicalOp.LongHashJoin]],
 * [[PhysicalOp.LongSortMergeJoin]], [[PhysicalOp.StringHashJoin]],
 * [[PhysicalOp.StringSortMergeJoin]], [[PhysicalOp.SortMergeJoin]],
 * [[PhysicalOp.SortMergeLeftOuter]], [[PhysicalOp.SortMergeRightOuter]],
 * [[PhysicalOp.SortMergeFullOuter]], [[PhysicalOp.HashLeftOuter]], [[PhysicalOp.HashRightOuter]],
 * and [[PhysicalOp.HashFullOuter]]. Those kernels perform the shuffle. Interpreting the boundary
 * on its own does not. [[PhysicalOp.BroadcastJoin]], [[PhysicalOp.LongBroadcastJoin]],
 * [[PhysicalOp.StringBroadcastJoin]], [[PhysicalOp.BroadcastLeftOuter]],
 * [[PhysicalOp.BroadcastRightOuter]], and [[PhysicalOp.BroadcastFullOuter]] have no exchange.
 * [[PhysicalOp.BuildSide]] chooses which logical child of an inner broadcast join is built. The
 * planner records the right child. A later cost-based pass may choose the left child. The other
 * child is probed in place. A hash or broadcast left outer join always builds the right input. The
 * left input is the probe side. A hash or broadcast right outer join always builds the left input
 * and probes the right. A probe-side row with no partner sets the build value's absence bit. A
 * null value is not absence. A sort-merge left, right, or full outer join copies a clear validity
 * bit. Right outer swaps the value columns. Left and full do not. A sort-merge outer join has no
 * build table. Long outer joins are the `LongHash*Outer`, `LongBroadcast*Outer`, and
 * `LongSortMerge*Outer` nodes. Their kind is [[PhysicalOp.Kind.LongOuter]]. String outer joins are
 * the `StringHash*Outer`, `StringBroadcast*Outer`, and `StringSortMerge*Outer` nodes. Their kind
 * is [[PhysicalOp.Kind.StringOuter]]. Hash and broadcast do not spill. Sort-merge runs in memory
 * and throws instead of spilling.
 */
enum PhysicalOp:
  case Scan(sourceKind: PhysicalOp.Kind, partitions: Seq[Partition[_]])
  case Filter(child: PhysicalOp, slot: PhysicalOp.Slot)
  case Project(child: PhysicalOp)

  /**
   * One column of an int, long, or string pair, or the key of a `countByKey` witness. Kind is
   * [[PhysicalOp.Kind.Ints]], [[PhysicalOp.Kind.Longs]] for a long pair or the key of a long
   * count, or [[PhysicalOp.Kind.Strings]] for a string key.
   */
  case Column(child: PhysicalOp, columnOrdinal: Int)

  /**
   * `mapValues` whose new value is an unboxed int or long. Kind stays the pair kind, including
   * [[PhysicalOp.Kind.StringPairs]].
   */
  case MapValue(child: PhysicalOp)

  /**
   * `mapValues` whose int value becomes a long. Kind stays the child's kind. The schema is
   * `[Int32, Int64]`. That layout is not the triple-project assumption.
   */
  case IntLongMapValue(child: PhysicalOp)

  /**
   * `mapIntKey` rewrites column 0. Kind stays [[PhysicalOp.Kind.Pairs]]. A null key stays null and
   * the function is not called. The plan node is the witness. The function is not inspected.
   */
  case MapKey(child: PhysicalOp)

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

  /**
   * Int hash reduce. `tag` is the binder tag the builder stored. The child schema does not set it.
   */
  case HashAggregate(child: PhysicalOp, tag: PhysicalOp.HashAggregateTag)

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
   * Sum of one int join column, grouped by the join key, updated from the probe.
   *
   * Shuffle-hash and sort-merge wrap `left` and `right` in exchanges. The kernel builds the left
   * bucket and probes the right. Sort-merge is that hash probe: it does not sort. An explicit
   * broadcast hint leaves both children unexchanged. The left child is still the build, so
   * [[ProbeMeasure.Left]] is the left column. A null key is not a probe hit. Each match adds the
   * measured column as a `Long`. The group key is that match's key column, not the hash slot. The
   * match columns are not written. [[ProbeMeasure]] records which side. Output kind is
   * [[PhysicalOp.Kind.IntLongSums]], schema `(Int32, Int64)`. A null measured value adds as zero.
   * A shuffled build bucket over the spill budget declines and does not write the matches. A
   * broadcast build whose retained estimate is above `broadcastBuildBudgetBytes` declines before
   * the table is allocated and does not write the matches. Outer joins do not use this node.
   */
  case JoinProbeSum(left: PhysicalOp, right: PhysicalOp, measure: PhysicalOp.ProbeMeasure)

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
   * keys do not use this node. Sort-merge is [[LongSortMergeJoin]]. Output kind is
   * [[PhysicalOp.Kind.LongTriples]].
   */
  case LongHashJoin(left: PhysicalOp, right: PhysicalOp)

  /**
   * Inner join of `(Long, Long)` pairs with no exchange. Build the right side. Probe the left in
   * place. A null key is dropped. A null value stays columnar. Output is `(key, left, right)`.
   * [[BuildSide]] records which child fills the table. The planner records [[BuildSide.Right]].
   * Output kind is [[PhysicalOp.Kind.LongTriples]].
   */
  case LongBroadcastJoin(left: PhysicalOp, right: PhysicalOp, buildSide: PhysicalOp.BuildSide)

  /**
   * In-memory sort-merge inner join of `(Long, Long)` pairs. `left` and `right` are exchanges with
   * the same partition count. Each partition is sorted by present key and merged. A null key is
   * dropped on both sides before the sort, so a null payload of `0` does not match key `0`. A
   * clear validity bit is a null value and is copied with the matched row. It is not an absence
   * bit. Equal keys keep input order. Output columns are `(key, left, right)`. One batch per
   * partition. This node does not spill. With spill on, a partition whose present-key estimate is
   * above `spillBudgetBytes` throws and writes nothing. At or under the budget, and whenever spill
   * is off, the partition stays in memory. Output kind is [[PhysicalOp.Kind.LongTriples]].
   */
  case LongSortMergeJoin(left: PhysicalOp, right: PhysicalOp)

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
   * Inner join of `(String, Int)` pairs with no exchange. Build the right side. Probe the left in
   * place. Match by string equality. A null key is dropped. A null value stays columnar. Output is
   * `(key, left, right)`. [[BuildSide]] records which child fills the table. The planner records
   * [[BuildSide.Right]]. Layout is [[com.ewoodbury.scarlet.columnar.Layout.StringTriple]]. Output
   * kind is [[PhysicalOp.Kind.StringTriples]].
   */
  case StringBroadcastJoin(left: PhysicalOp, right: PhysicalOp, buildSide: PhysicalOp.BuildSide)

  /**
   * In-memory sort-merge inner join of `(String, Int)` pairs. `left` and `right` are exchanges
   * with the same partition count. Each partition is sorted by the dictionary-decoded key and
   * merged. Comparison is `String.compareTo`. Dictionary codes are not compared. A null key is
   * dropped on both sides before the sort, so a null key does not match `""`. A clear validity bit
   * is a null value and is copied with the matched row. It is not an absence bit. `""` matches
   * `""`. `"é"` and `"e\u0301"` do not match. Equal keys keep input order. Output columns are
   * `(key, left, right)`. One batch per partition. This node does not spill. With spill on, a
   * partition whose present-key estimate, including the dictionary, is above `spillBudgetBytes`
   * throws and writes nothing. At or under the budget, and whenever spill is off, the partition
   * stays in memory. Output kind is [[PhysicalOp.Kind.StringTriples]].
   */
  case StringSortMergeJoin(left: PhysicalOp, right: PhysicalOp)

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
   * key column. A null value is a clear validity bit and is copied. It is not an absence bit. A
   * stored `0` stays a present zero. Output columns are `(key, left, right)`. One batch per
   * partition. The inputs are not swapped.
   */
  case SortMergeLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Sort-merge right outer join of int pairs. `left` and `right` are exchanges. The right exchange
   * carries a null key to partition 0. The left exchange drops a null key. The left-outer walk
   * runs with the inputs swapped, then the value columns are swapped back to `(key, left, right)`.
   * Absence is the left value's absence bit. A null value is a clear validity bit and is copied.
   * Output columns are `(key, left, right)`. One batch per partition.
   */
  case SortMergeRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Sort-merge full outer join of int pairs. `left` and `right` are exchanges. Both exchanges
   * carry a null key to partition 0. A group on only one side is emitted, with the other value
   * absent. A null key on either side is emitted once and does not match. An empty side emits
   * every row of the other side. Either value's absence bit is a missing partner. A null value is
   * a clear validity bit and is copied. Output columns are `(key, left, right)`. One batch per
   * partition.
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
   * `(key, left, right)`. Absence is the left value's absence bit. A null value is a clear
   * validity bit and stays columnar. The build table uses the same budget as a broadcast join.
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
   * absence bit is set. A null value is a clear validity bit and stays columnar. This is a
   * different node from [[BroadcastJoin]], so the inner join does not carry an outer flag.
   */
  case BroadcastLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Right outer join with no exchange. `left` and `right` stay the logical inputs. The left input
   * fills the hash table. The right input is the probe side and is probed in place. The left-outer
   * kernel writes `(key, probe value, build value)`, and the value columns are swapped back to
   * `(key, left, right)`. Absence is the left value's absence bit. A null value is a clear
   * validity bit and stays columnar. This is a different node from [[BroadcastJoin]] and from
   * [[BroadcastLeftOuter]].
   */
  case BroadcastRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Shuffle-hash full outer join. `left` and `right` are exchanges. Both exchanges carry a null
   * key to partition 0. The left input is the probe side. The right input is the build side. Each
   * partition builds one table, probes with one bitmap, and emits that partition's unmatched build
   * rows. A null build key is carried outside the table and emitted once. Output columns are
   * `(key, left, right)`. Either value's absence bit is a missing partner. A null value is a clear
   * validity bit and stays columnar. The build table uses the same budget as a broadcast join. The
   * bitmap is not part of that budget.
   */
  case HashFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Full outer join with no exchange. `left` and `right` stay the logical inputs. The right input
   * fills the hash table. The left input is the probe side and is probed in place. Each probe
   * thread keeps its own matched-row bitmap. The bitmaps are OR'd after the probes join. A clear
   * bit emits that build row with the left value absent. A null build key is carried outside the
   * table and emitted once. An empty probe still emits every build row. Output columns are
   * `(key, left, right)`. Either value's absence bit is a missing partner. A null value is a clear
   * validity bit and stays columnar. The bitmap is not part of the build budget.
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
   * dropped. One grouped batch is published per shuffle partition, including an empty bucket.
   * Output kind is [[PhysicalOp.Kind.GroupedInts]] for a [[PhysicalOp.Kind.Pairs]] child,
   * [[PhysicalOp.Kind.GroupedStrings]] for a [[PhysicalOp.Kind.StringPairs]] child, and
   * [[PhysicalOp.Kind.GroupedLongs]] for a [[PhysicalOp.Kind.LongPairs]] child. A clear value bit
   * on those three stays in the group. Spill refuses it before a file is opened. An int-to-long
   * [[IntLongMapValue]] child publishes [[PhysicalOp.Kind.GroupedIntLongs]]: the key stays
   * `Int32`, the values stay `Int64`, and a null value stays in the list. That group does not
   * spill. Any other child is a planner bug. There is no partial combine and no sort. Spill off
   * keeps the in-memory groups and writes nothing. Spill on slices one over-budget post-exchange
   * bucket in input order. A morsel whose group table does not fit throws
   * [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]] before that table. The appended groups
   * are not bounded by the budget.
   */
  case GroupByKey(child: PhysicalOp)

  /**
   * One map directly above [[GroupByKey]]. `child` is that group. The user function stays on the
   * logical plan. Execution decodes `child` once through
   * [[com.ewoodbury.scarlet.execution.ColumnarInterpreter.decodeFor]] and runs the function on
   * each `(K, Iterable[V])`. The function is not a columnar map and is not inspected. Output kind
   * stays the grouped kind of `child`. `values` and `take` do not use this node. Int, long, and
   * string `keys` stay a distinct-key scan.
   */
  case GroupedDecode(child: PhysicalOp)

  /**
   * `countByKey`. The child is an [[PhysicalOp.Exchange]], or an int inner [[HashJoin]] of two
   * exchanges. The exchange child is shuffled by the kernel. The hash-join child is not wrapped in
   * another exchange and has no inserted project: the join key is unchanged, and the count is the
   * per-key product of the two sides. A null key is dropped. The value column is not read, so a
   * null value still counts. One `(key, count)` row is published per distinct key, including an
   * empty bucket. Output kind is [[PhysicalOp.Kind.Pairs]] for a [[PhysicalOp.Kind.Pairs]] child,
   * for that hash join, and for a triple [[PhysicalOp.Project]] whose schema is `[Int32, Int64]`.
   * That project keeps kind [[PhysicalOp.Kind.Triples]]; the count still publishes int pairs and
   * reads only the projected key. A [[PhysicalOp.Kind.LongPairs]] child publishes
   * [[PhysicalOp.Kind.LongCounts]], and a [[PhysicalOp.Kind.StringPairs]] child publishes
   * [[PhysicalOp.Kind.StringPairs]]. Any other child is a planner bug. The count is an `Int` and
   * wraps. A long count is `[Int64, Int32]`. The long estimate charges 8-byte keys. The string
   * estimate charges dictionary bytes. With spill off, an input that does not fit the budget
   * throws [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]] before the table is allocated,
   * and the action fails instead of rerunning on rows. With spill on, an over-budget post-exchange
   * bucket spills. A count table that still does not fit throws
   * [[com.ewoodbury.scarlet.columnar.ClosedKernelRefusal]]. The running partial is `O(distinct
   * keys)`.
   */
  case CountByKey(child: PhysicalOp)

  /**
   * Count and sum of `(Int, Int)` pairs. The child is an [[Exchange]]. The kernel shuffles. A null
   * key is dropped. A null value is counted and not added. One `(key, count, sum)` row per
   * distinct key, including an empty bucket. Output kind is [[PhysicalOp.Kind.CountSums]]. The
   * schema is `[Int32, Int64, Int64]`. The count narrows to `Int` at decode and wraps. The table
   * grows from a small capacity with the distinct keys.
   */
  case CountSum(child: PhysicalOp)

  /**
   * Count and sum of `(Long, Long)` pairs. The child is an [[Exchange]]. The kernel shuffles with
   * the long-key exchange. A null key is dropped. A null value is counted and not added. One
   * `(key, count, sum)` row per distinct key, including an empty bucket. Output kind is
   * [[PhysicalOp.Kind.LongCountSums]]. The schema is `[Int64, Int64, Int64]`. The count narrows to
   * `Int` at decode and wraps. There is no spill path.
   */
  case LongCountSum(child: PhysicalOp)

  /**
   * Count and sum of `(String, Int)` pairs. The child is an [[Exchange]]. The kernel shuffles with
   * the string-key exchange. A null key is dropped. An empty string is a present key. A null value
   * is counted and not added. One `(key, count, sum)` row per distinct key, including an empty
   * bucket. Output kind is [[PhysicalOp.Kind.StringCountSums]]. The schema is
   * `[Utf8Dict, Int64, Int64]`. The count narrows to `Int` at decode and wraps. There is no spill
   * path.
   */
  case StringCountSum(child: PhysicalOp)

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
   * Global sort of an int-pair scan, of one int-pair project on that scan or filter stack, of one
   * key map on that scan or filter stack, of one key or value filter immediately above that map,
   * or of one int `mapValues` on that scan or filter stack, by effective value descending, then
   * effective key ascending. Neither function is read. The sort orders the published pairs. A null
   * input key stays null, is not passed to the key function, and orders as zero. A null value
   * stays null, is not passed to `mapValues`, and orders as zero. Ties keep input order. `take(n)`
   * is a bounded heap of `n` rows when `n` is below the live row count. When `n` covers every live
   * row, take is the full sort rather than that heap. Collect is the same full order. Output kind
   * is [[PhysicalOp.Kind.Pairs]].
   *
   * The ordering tag is [[OrderingTag.Sorted]] with `Seq(1, 0)` and `Seq(false, true)`.
   */
  case SortByIntValueDesc(child: PhysicalOp)

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

  /**
   * Global sort of published long pairs by effective value descending, then effective key
   * ascending. A null orders as zero and stays null. Ties keep input order. Output kind is
   * [[PhysicalOp.Kind.LongPairs]]. There is no spill. `take` decodes a prefix of this order.
   *
   * The ordering tag is [[OrderingTag.Sorted]] with `Seq(1, 0)` and `Seq(false, true)`.
   */
  case SortByLongValueDesc(child: PhysicalOp)

  /**
   * Global sort of published string pairs by the int value descending, then the string key
   * ascending. A null key orders as `""` and stays null. A null value orders as zero and stays
   * null. Ties keep input order. Output kind is [[PhysicalOp.Kind.StringPairs]]. There is no
   * spill. `take` decodes a prefix of this order.
   *
   * The ordering tag is [[OrderingTag.Sorted]] with `Seq(1, 0)` and `Seq(false, true)`.
   */
  case SortByStringValueDesc(child: PhysicalOp)

  /**
   * Direct physical children. A scan has none. Every operator stores its children as constructor
   * arguments, so a new case cannot hide a key map from [[MapIntKey.inTree]].
   */
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Any",
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.Product",
    ),
  )
  def children: List[PhysicalOp] =
    this.productIterator.collect { case child: PhysicalOp => child }.toList

  def kind: PhysicalOp.Kind = this match
    case Scan(sourceKind, _) => sourceKind
    case Filter(child, _) => child.kind
    case Project(child) => child.kind
    case Pipeline(child, _) => child.kind
    case Union(left, _) => left.kind
    case Dedupe(child) => child.kind
    case Exchange(child) => child.kind
    case Column(child, ordinal) =>
      if (IntLongParents.widened(child)) then
        if (ordinal == 0) then PhysicalOp.Kind.Ints else PhysicalOp.Kind.Longs
      else
        child.kind match
          case PhysicalOp.Kind.LongPairs => PhysicalOp.Kind.Longs
          case PhysicalOp.Kind.LongCounts if ordinal == 0 => PhysicalOp.Kind.Longs
          case PhysicalOp.Kind.StringPairs if ordinal == 0 => PhysicalOp.Kind.Strings
          case PhysicalOp.Kind.StringPairs => PhysicalOp.Kind.Ints
          case _ => PhysicalOp.Kind.Ints
    case MapValue(child) =>
      child.kind match
        case PhysicalOp.Kind.LongPairs => PhysicalOp.Kind.LongPairs
        case PhysicalOp.Kind.StringPairs => PhysicalOp.Kind.StringPairs
        case _ => PhysicalOp.Kind.Pairs
    case IntLongMapValue(child) => child.kind
    case MapKey(_) => PhysicalOp.Kind.Pairs
    case SortByIntKey(_) => PhysicalOp.Kind.Pairs
    case SortByIntValueDesc(_) => PhysicalOp.Kind.Pairs
    case SortByLongKey(_) => PhysicalOp.Kind.LongPairs
    case SortByStringKey(_) => PhysicalOp.Kind.StringPairs
    case SortByLongValueDesc(_) => PhysicalOp.Kind.LongPairs
    case SortByStringValueDesc(_) => PhysicalOp.Kind.StringPairs
    case HashAggregate(_, _) => PhysicalOp.Kind.Pairs
    case LongHashAggregate(_) => PhysicalOp.Kind.LongPairs
    case StringHashAggregate(_) => PhysicalOp.Kind.StringPairs
    case GroupByKey(child)
        if PhysicalSchema.isIntLong(child.schema) && (child.kind eq PhysicalOp.Kind.Pairs) =>
      PhysicalOp.Kind.GroupedIntLongs
    case GroupByKey(child) =>
      child.kind match
        case PhysicalOp.Kind.Pairs => PhysicalOp.Kind.GroupedInts
        case PhysicalOp.Kind.StringPairs => PhysicalOp.Kind.GroupedStrings
        case PhysicalOp.Kind.LongPairs => PhysicalOp.Kind.GroupedLongs
        case other =>
          throw new IllegalStateException(
            s"groupByKey column is $other, expected int, long, or string pairs",
          )
    case GroupedDecode(child) => child.kind
    case CountByKey(PhysicalOp.HashJoin(_, _)) => PhysicalOp.Kind.Pairs
    case CountByKey(child) =>
      if (PhysicalSchema.isIntLong(child.schema)) then PhysicalOp.Kind.Pairs
      else
        child.kind match
          case PhysicalOp.Kind.Pairs => PhysicalOp.Kind.Pairs
          case PhysicalOp.Kind.LongPairs => PhysicalOp.Kind.LongCounts
          case PhysicalOp.Kind.StringPairs => PhysicalOp.Kind.StringPairs
          case other =>
            throw new IllegalStateException(
              s"countByKey column is $other, expected int, long, or string pairs",
            )
    case CountSum(_) => PhysicalOp.Kind.CountSums
    case LongCountSum(_) => PhysicalOp.Kind.LongCountSums
    case StringCountSum(_) => PhysicalOp.Kind.StringCountSums
    case IntLongHashAggregate(_) => PhysicalOp.Kind.Pairs
    case IntLongCountSum(_) => PhysicalOp.Kind.CountSums
    case SortByIntLongKey(_) | SortByIntLongValueDesc(_) => PhysicalOp.Kind.Pairs
    case IntLongHashJoin(_, _) | IntLongBroadcastJoin(_, _, _) | IntLongSortMergeJoin(_, _) =>
      PhysicalOp.Kind.IntLongTriples
    case IntLongHashLeftOuter(_, _) | IntLongHashRightOuter(_, _) | IntLongHashFullOuter(_, _) |
        IntLongBroadcastLeftOuter(_, _) | IntLongBroadcastRightOuter(_, _) |
        IntLongBroadcastFullOuter(_, _) | IntLongSortMergeLeftOuter(_, _) |
        IntLongSortMergeRightOuter(_, _) | IntLongSortMergeFullOuter(_, _) =>
      PhysicalOp.Kind.IntLongOuter
    case HashJoin(_, _) => PhysicalOp.Kind.Triples
    case JoinProbeSum(_, _, _) => PhysicalOp.Kind.IntLongSums
    case LongHashJoin(_, _) | LongBroadcastJoin(_, _, _) | LongSortMergeJoin(_, _) =>
      PhysicalOp.Kind.LongTriples
    case StringHashJoin(_, _) | StringBroadcastJoin(_, _, _) | StringSortMergeJoin(_, _) =>
      PhysicalOp.Kind.StringTriples
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
   * Output columns. [[kind]] stays the admission enum. A [[Project]] whose child kind is
   * [[PhysicalOp.Kind.Triples]] reports `[Int32, Int64]`.
   */
  def schema: IndexedSeq[LogicalType] = PhysicalSchema.of(this)

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
    case IntLongMapValue(child) => s"IntLongMapValue(${child.shape})"
    case MapKey(child) => s"MapKey(${child.shape})"
    case SortByIntKey(child) => s"SortByIntKey(${child.shape})"
    case SortByIntValueDesc(child) => s"SortByIntValueDesc(${child.shape})"
    case SortByLongKey(child) => s"SortByLongKey(${child.shape})"
    case SortByStringKey(child) => s"SortByStringKey(${child.shape})"
    case SortByLongValueDesc(child) => s"SortByLongValueDesc(${child.shape})"
    case SortByStringValueDesc(child) => s"SortByStringValueDesc(${child.shape})"
    case HashAggregate(child, _) => s"HashAggregate(${child.shape})"
    case LongHashAggregate(child) => s"LongHashAggregate(${child.shape})"
    case StringHashAggregate(child) => s"StringHashAggregate(${child.shape})"
    case GroupByKey(child) => s"GroupByKey(${child.shape})"
    case GroupedDecode(child) => s"GroupedDecode(${child.shape})"
    case CountByKey(child) => s"CountByKey(${child.shape})"
    case CountSum(child) => s"CountSum(${child.shape})"
    case LongCountSum(child) => s"LongCountSum(${child.shape})"
    case StringCountSum(child) => s"StringCountSum(${child.shape})"
    case IntLongHashAggregate(child) => s"IntLongHashAggregate(${child.shape})"
    case IntLongCountSum(child) => s"IntLongCountSum(${child.shape})"
    case SortByIntLongKey(child) => s"SortByIntLongKey(${child.shape})"
    case SortByIntLongValueDesc(child) => s"SortByIntLongValueDesc(${child.shape})"
    case IntLongHashJoin(left, right) => s"IntLongHashJoin(${left.shape}, ${right.shape})"
    case IntLongHashLeftOuter(left, right) =>
      s"IntLongHashLeftOuter(${left.shape}, ${right.shape})"
    case IntLongHashRightOuter(left, right) =>
      s"IntLongHashRightOuter(${left.shape}, ${right.shape})"
    case IntLongHashFullOuter(left, right) =>
      s"IntLongHashFullOuter(${left.shape}, ${right.shape})"
    case IntLongBroadcastJoin(left, right, _) =>
      s"IntLongBroadcastJoin(${left.shape}, ${right.shape})"
    case IntLongBroadcastLeftOuter(left, right) =>
      s"IntLongBroadcastLeftOuter(${left.shape}, ${right.shape})"
    case IntLongBroadcastRightOuter(left, right) =>
      s"IntLongBroadcastRightOuter(${left.shape}, ${right.shape})"
    case IntLongBroadcastFullOuter(left, right) =>
      s"IntLongBroadcastFullOuter(${left.shape}, ${right.shape})"
    case IntLongSortMergeJoin(left, right) =>
      s"IntLongSortMergeJoin(${left.shape}, ${right.shape})"
    case IntLongSortMergeLeftOuter(left, right) =>
      s"IntLongSortMergeLeftOuter(${left.shape}, ${right.shape})"
    case IntLongSortMergeRightOuter(left, right) =>
      s"IntLongSortMergeRightOuter(${left.shape}, ${right.shape})"
    case IntLongSortMergeFullOuter(left, right) =>
      s"IntLongSortMergeFullOuter(${left.shape}, ${right.shape})"
    case HashJoin(left, right) => s"HashJoin(${left.shape}, ${right.shape})"
    case JoinProbeSum(left, right, measure) =>
      val side = measure match
        case PhysicalOp.ProbeMeasure.Left => "Left"
        case PhysicalOp.ProbeMeasure.Right => "Right"
      s"JoinProbeSum$side(${left.shape}, ${right.shape})"
    case LongHashJoin(left, right) => s"LongHashJoin(${left.shape}, ${right.shape})"
    case LongBroadcastJoin(left, right, _) =>
      s"LongBroadcastJoin(${left.shape}, ${right.shape})"
    case LongSortMergeJoin(left, right) =>
      s"LongSortMergeJoin(${left.shape}, ${right.shape})"
    case StringHashJoin(left, right) => s"StringHashJoin(${left.shape}, ${right.shape})"
    case StringBroadcastJoin(left, right, _) =>
      s"StringBroadcastJoin(${left.shape}, ${right.shape})"
    case StringSortMergeJoin(left, right) =>
      s"StringSortMergeJoin(${left.shape}, ${right.shape})"
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

  /**
   * Non-adding `reduceByKey` of an int key and a long value. The child is an [[Exchange]]. The
   * kernel shuffles on the int key. Addition stays [[HashAggregate]] with
   * [[PhysicalOp.HashAggregateTag.IntLongSum]]. A null key is dropped. A null value stays null
   * until another value arrives. Output kind is [[PhysicalOp.Kind.Pairs]]. The schema is
   * `[Int32, Int64]`. There is no spill path.
   */
  case IntLongHashAggregate(child: PhysicalOp)

  /**
   * Count and sum of `(Int, Long)` pairs. The child is an [[Exchange]]. The kernel shuffles on the
   * int key. A null key is dropped. A null value is counted and not added. Output kind is
   * [[PhysicalOp.Kind.CountSums]]. The schema is `[Int32, Int64, Int64]`. There is no spill path.
   */
  case IntLongCountSum(child: PhysicalOp)

  /**
   * Global sort of `(Int, Long)` by the int key ascending. A null key orders as zero and stays
   * null. A null long stays null. One filter of the widening lowers. A second map stays on the row
   * path. Output kind is [[PhysicalOp.Kind.Pairs]]. The schema is `[Int32, Int64]`. There is no
   * spill. The ordering tag is ascending on column 0.
   */
  case SortByIntLongKey(child: PhysicalOp)

  /**
   * Global sort of `(Int, Long)` by the long value descending, then the int key ascending. A null
   * long orders as zero and stays null. A null key orders as zero and stays null. Ties keep input
   * order. Output kind is [[PhysicalOp.Kind.Pairs]]. The schema is `[Int32, Int64]`. There is no
   * spill. `take` is not a separate heap.
   */
  case SortByIntLongValueDesc(child: PhysicalOp)

  /**
   * Inner hash join of two `(Int, Long)` sides. Each child is an [[Exchange]]. The kernel shuffles
   * on the int key. A null key is dropped. A null long stays null and still matches. Output kind
   * is [[PhysicalOp.Kind.IntLongTriples]]. The schema is `[Int32, Int64, Int64]`. Broadcast,
   * sort-merge, and outer joins are not this node. There is no spill.
   */
  case IntLongHashJoin(left: PhysicalOp, right: PhysicalOp)

  /**
   * Left hash outer join of two `(Int, Long)` sides. Each child is an [[Exchange]]. A null key
   * does not match. A left null key is kept and the right value is absent. A right null key is
   * dropped. A null long stays a clear validity bit and is not absence. Output kind is
   * [[PhysicalOp.Kind.IntLongOuter]]. The schema is `[Int32, Int64, Int64]`. There is no spill.
   */
  case IntLongHashLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Right hash outer join of two `(Int, Long)` sides. Each child is an [[Exchange]]. A null key
   * does not match. A right null key is kept and the left value is absent. A left null key is
   * dropped. A null long stays a clear validity bit and is not absence. Output kind is
   * [[PhysicalOp.Kind.IntLongOuter]]. The schema is `[Int32, Int64, Int64]`. There is no spill.
   */
  case IntLongHashRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Full hash outer join of two `(Int, Long)` sides. Each child is an [[Exchange]]. A null key
   * does not match and is kept on its own side with the other value absent. A null long stays a
   * clear validity bit and is not absence. Output kind is [[PhysicalOp.Kind.IntLongOuter]]. The
   * schema is `[Int32, Int64, Int64]`. There is no spill.
   */
  case IntLongHashFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Inner broadcast join of two `(Int, Long)` sides. The children are not exchanges. The right
   * side fills the table. A null key is dropped. A null long stays null and still matches. Output
   * kind is [[PhysicalOp.Kind.IntLongTriples]]. The schema is `[Int32, Int64, Int64]`. There is no
   * spill.
   */
  case IntLongBroadcastJoin(
      left: PhysicalOp,
      right: PhysicalOp,
      buildSide: PhysicalOp.BuildSide,
  )

  /**
   * Left broadcast outer join of two `(Int, Long)` sides. The children are not exchanges. A null
   * key does not match. A left null key is kept and the right value is absent. A right null key is
   * dropped. A null long stays a clear validity bit and is not absence. Output kind is
   * [[PhysicalOp.Kind.IntLongOuter]]. The schema is `[Int32, Int64, Int64]`. There is no spill.
   */
  case IntLongBroadcastLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Right broadcast outer join of two `(Int, Long)` sides. The children are not exchanges. A null
   * key does not match. A right null key is kept and the left value is absent. A left null key is
   * dropped. A null long stays a clear validity bit and is not absence. Output kind is
   * [[PhysicalOp.Kind.IntLongOuter]]. The schema is `[Int32, Int64, Int64]`. There is no spill.
   */
  case IntLongBroadcastRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Full broadcast outer join of two `(Int, Long)` sides. The children are not exchanges. A null
   * key does not match and is kept on its own side with the other value absent. A null long stays
   * a clear validity bit and is not absence. Output kind is [[PhysicalOp.Kind.IntLongOuter]]. The
   * schema is `[Int32, Int64, Int64]`. There is no spill.
   */
  case IntLongBroadcastFullOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Inner sort-merge join of two `(Int, Long)` sides. Each child is an [[Exchange]]. A null key is
   * dropped. A null long stays null and still matches. Output kind is
   * [[PhysicalOp.Kind.IntLongTriples]]. The schema is `[Int32, Int64, Int64]`. There is no spill.
   */
  case IntLongSortMergeJoin(left: PhysicalOp, right: PhysicalOp)

  /**
   * Left sort-merge outer join of two `(Int, Long)` sides. Each child is an [[Exchange]]. A null
   * key does not match. A left null key is kept and the right value is absent. A right null key is
   * dropped. A null long stays a clear validity bit and is not absence. Output kind is
   * [[PhysicalOp.Kind.IntLongOuter]]. The schema is `[Int32, Int64, Int64]`. There is no spill.
   */
  case IntLongSortMergeLeftOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Right sort-merge outer join of two `(Int, Long)` sides. Each child is an [[Exchange]]. A null
   * key does not match. A right null key is kept and the left value is absent. A left null key is
   * dropped. A null long stays a clear validity bit and is not absence. Output kind is
   * [[PhysicalOp.Kind.IntLongOuter]]. The schema is `[Int32, Int64, Int64]`. There is no spill.
   */
  case IntLongSortMergeRightOuter(left: PhysicalOp, right: PhysicalOp)

  /**
   * Full sort-merge outer join of two `(Int, Long)` sides. Each child is an [[Exchange]]. A null
   * key does not match and is kept on its own side with the other value absent. A null long stays
   * a clear validity bit and is not absence. Output kind is [[PhysicalOp.Kind.IntLongOuter]]. The
   * schema is `[Int32, Int64, Int64]`. There is no spill.
   */
  case IntLongSortMergeFullOuter(left: PhysicalOp, right: PhysicalOp)

object PhysicalOp:

  /**
   * Tag the builder stored on [[HashAggregate]].
   *
   * This is not derived from the child schema. An `[Int32, Int64]` child may still be
   * [[HashAggregateTag.IntHash]]. [[HashAggregateTag.IntLongSum]] is what the int-long aggregate
   * path stores.
   */
  enum HashAggregateTag:
    case IntHash
    case IntLongSum

  enum Kind:
    case Ints, Longs, Floats, Pairs, Triples, Strings, Bools, LongPairs, LongCounts, LongTriples,
      StringPairs, StringTriples, GroupedInts, GroupedStrings, GroupedLongs, LongOuter,
      StringOuter,
      CountSums,
      IntLongSums,
      GroupedIntLongs,
      LongCountSums,
      StringCountSums,
      IntLongTriples,
      IntLongOuter

  /** Which value column [[JoinProbeSum]] adds. `Left` is the build side. */
  enum ProbeMeasure:
    case Left, Right

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
