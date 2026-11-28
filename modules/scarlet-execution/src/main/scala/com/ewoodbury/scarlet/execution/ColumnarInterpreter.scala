package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}

/**
 * Runs a lowered [[PhysicalOp]] tree and encodes source partitions into columns.
 *
 * Named erasure boundary, shared with [[ColumnarPlanner]]. [[ColumnarPlanner.lower]] chooses the
 * tree and does not call user functions. [[interpret]] walks that same logical plan and applies
 * the operator recorded on each physical node. A mismatch throws `IllegalStateException`, which
 * [[ColumnarPlanner.tryRun]] does not turn into a row-path fallback. A broadcast build table above
 * `broadcastBuildBudgetBytes` throws `IllegalStateException` the same way: both inputs are already
 * evaluated, and the budget covers that table only. A partition selector on an aggregate, a join,
 * or a sort fails a `require` guard with `IllegalArgumentException`, and that is not a fallback
 * either. A `ClassCastException` from a filter, or from a string map that declares `String` and
 * returns another value, is still a fallback, and that function may already have run.
 * [[ColumnarFallback]] is the named form of a deliberate decline, including a null value on a long
 * or string outer join: the value columns are already encoded, and the kernel is not chosen. An
 * int outer join, shuffle-hash, broadcast, or sort-merge, on the left, right, or full side, keeps
 * the null. A null value on a long inner hash join stays columnar. A clear validity bit is a null
 * value, not an absence bit. A selected null on an int inner join, and a null on the dense side of
 * a mixed int hash join, stay a clear validity bit. That bit is not absence. A null on a dropped
 * row is not a decline. A null value on a string inner hash or broadcast join stays columnar.
 * String `reduceByKey` and string `groupByKey` with a live null value are that fallback. A string
 * outer join still declines a null value. A string-pair filter keeps the null int. A value
 * projection that kept a null int is that fallback too. With spill off, an over-budget int, long,
 * or string `countByKey` throws `ClosedKernelRefusal`, which does not extend `ClassCastException`,
 * so the action fails instead of rerunning on rows. With spill on, an over-budget post-exchange
 * bucket spills. A count table that still does not fit throws `ClosedKernelRefusal`. The long
 * estimate charges 8-byte keys. The string estimate charges dictionary bytes. The running partial
 * is `O(distinct keys)`.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw",
  ),
)
private[execution] object ColumnarInterpreter extends ColumnarInterpreterParts:

  private[execution] enum Columns:
    case Ints(parts: Vector[ColumnBatch])
    case Longs(parts: Vector[ColumnBatch])
    case Floats(parts: Vector[ColumnBatch])
    case Pairs(parts: Vector[ColumnBatch])
    case LongPairs(parts: Vector[ColumnBatch])

    /** `(Long, Int)` counts. Schema is `[Int64, Int32]`. A null key was already dropped. */
    case LongCounts(parts: Vector[ColumnBatch])

    /**
     * `(Int, Int, Long)` count and sum. Schema is `[Int32, Int64, Int64]`: key, count, sum. A null
     * key was already dropped. A null value was counted and not added.
     */
    case CountSums(parts: Vector[ColumnBatch])

    /**
     * `(Long, Int, Long)` count and sum. Schema is `[Int64, Int64, Int64]`: key, count, sum. A
     * null key was already dropped. A null value was counted and not added. The count narrows to
     * `Int` at decode.
     */
    case LongCountSums(parts: Vector[ColumnBatch])

    /**
     * `(String, Int, Long)` count and sum. Schema is `[Utf8Dict, Int64, Int64]`. An empty string
     * is a present key. A null key was already dropped. A null value was counted and not added.
     */
    case StringCountSums(parts: Vector[ColumnBatch])
    case Triples(parts: Vector[ColumnBatch])
    case LongTriples(parts: Vector[ColumnBatch])
    case Strings(parts: Vector[ColumnBatch])
    case Bools(parts: Vector[ColumnBatch])

    /** `(String, Int)` batches. Column 0 is the key, 1 the value; narrowed batch may carry one. */
    case StringPairs(parts: Vector[ColumnBatch])

    /**
     * `(String, (Int, Int))` batches. Schema is `[Utf8Dict, Int32, Int32]`. The key is the probe
     * row's dictionary and code.
     */
    case StringTriples(parts: Vector[ColumnBatch])

    /**
     * Outer-join batches stay `(key, left, right)`. `absent` tells the decoder which value column
     * is the `Option`. A string batch is [[StringOuterDecode]]. A long batch is
     * [[LongOuterDecode]]. An int batch still reaches [[BatchCodec.decodeOuter]] inside
     * [[LongOuterDecode]]. [[AbsentSide.Right]] is a left outer join. [[AbsentSide.Left]] is a
     * right outer join. [[AbsentSide.Both]] is a full outer join.
     */
    case Outer(parts: Vector[ColumnBatch], absent: AbsentSide)

    /** Int `groupByKey` batches. Not a [[ColumnBatch]]. */
    case GroupedInts(parts: Vector[GroupedIntBatch])

    /** String `groupByKey` batches. Not a [[ColumnBatch]]. */
    case GroupedStrings(parts: Vector[GroupedStringBatch])

    /** Long `groupByKey` batches. Not a [[ColumnBatch]]. Values are `Long`. */
    case GroupedLongs(parts: Vector[GroupedLongBatch])

    /**
     * Int-key, long-value `groupByKey`. Not a [[ColumnBatch]]. A clear value bit is a null `Long`.
     */
    case GroupedIntLongs(parts: Vector[GroupedIntLongBatch])

  private[execution] def interpret(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int] = None,
      preserveAbsence: Boolean = false,
  ): Columns =
    // A whole-row filter or a triple map asks the join to keep null values. A bare join does not,
    // and a slot filter does not: its kernel rejects a clear bit.
    val absence = preserveAbsence || op.requestsTripleAbsence
    interpretAt(
      plan,
      op,
      selectedPartition,
      absence,
      ColumnUse.ordinals(plan, op),
    )

  /**
   * `ordinals` is the scan demand of the root plan. Recursive calls pass that same set.
   * Recomputing it on a peeled child would forget a projection or a filter above the peel.
   */
  private[execution] def interpretAt(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns = (plan, op) match
    case (Plan.UnionOp(leftPlan, rightPlan), PhysicalOp.Union(leftOp, rightOp)) =>
      // A union is not partition-preserving, so take never drives it per partition. If a caller
      // ever passes a selected partition here, both sides would read their own partition i, which
      // is not a global union partition.
      ColumnsConcat(
        interpretAt(leftPlan, leftOp, selectedPartition, preserveAbsence, ordinals),
        interpretAt(rightPlan, rightOp, selectedPartition, preserveAbsence, ordinals),
      )
    case (Plan.DistinctOp(source), PhysicalOp.Dedupe(PhysicalOp.Exchange(child))) =>
      DistinctColumns.run(source, child, selectedPartition, preserveAbsence, ordinals)
    case (_: Plan.DistinctOp[_], _: PhysicalOp.Dedupe) =>
      throw new ColumnarFallback("distinct needs an exchange")
    case (Plan.Source(partitions, _), PhysicalOp.Scan(kind, _)) =>
      val selected = selectPartition(partitions, selectedPartition)
      kind match
        case PhysicalOp.Kind.Ints => Columns.Ints(SourceEncode.encodeInts(selected))
        case PhysicalOp.Kind.Longs => Columns.Longs(SourceEncode.encodeLongs(selected))
        case PhysicalOp.Kind.Floats => Columns.Floats(SourceEncode.encodeFloats(selected))
        case PhysicalOp.Kind.Float32s =>
          Columns.Floats(SourceEncode.encodeFloat32s(selected))
        case PhysicalOp.Kind.Pairs =>
          // A boxed pair keeps a null component absent. preserveAbsence still forces both columns.
          // A singleton demand encodes only the ordinal ColumnUse asked for.
          val encoded =
            if (preserveAbsence) then SourceEncode.encodeGroupedPairs(selected)
            else SourceEncode.encodePairs(selected, ordinals)
          Columns.Pairs(encoded)
        case PhysicalOp.Kind.LongPairs =>
          Columns.LongPairs(SourceEncode.encodeLongPairs(selected, ordinals))
        case PhysicalOp.Kind.Strings => Columns.Strings(SourceEncode.encodeStrings(selected))
        case PhysicalOp.Kind.Bools => Columns.Bools(SourceEncode.encodeBools(selected))
        case PhysicalOp.Kind.StringPairs => StringPairWalk.encoded(selected, ordinals)
        case PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongCounts | PhysicalOp.Kind.LongTriples |
            PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.GroupedInts |
            PhysicalOp.Kind.GroupedStrings | PhysicalOp.Kind.GroupedLongs |
            PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums |
            PhysicalOp.Kind.LongCountSums | PhysicalOp.Kind.StringCountSums |
            PhysicalOp.Kind.IntLongSums | PhysicalOp.Kind.GroupedIntLongs |
            PhysicalOp.Kind.IntLongTriples | PhysicalOp.Kind.IntLongOuter |
            PhysicalOp.Kind.Columns =>
          throw new IllegalStateException(
            s"physical tree does not match ${plan.getClass.getSimpleName}",
          )
    // A string-pair filter is not [[FilterChain.accepts]]. A key or value stack stays in
    // [[StringPairWalk]]. A whole-row string stack is one selection pass there. A long-pair stack
    // of two or more atoms is one selection pass in [[FilterChain.run]]. A lone long-pair filter
    // still chains. A sort reads [[FilterChain.selected]], which does not compact.
    // `accepts` walks the logical plan, so it runs only for a filter node.
    case (logical, physical @ PhysicalOp.Filter(child, _)) if ExecutionRoute.stringPairs(child) =>
      StringPairWalk.filter(logical, physical, selectedPartition, preserveAbsence, ordinals)
    case (logical, physical)
        if physical.isInstanceOf[PhysicalOp.Filter] && FilterChain.accepts(logical, physical) =>
      FilterChain.run(logical, physical, selectedPartition, preserveAbsence, ordinals)
    case (Plan.MapOp(source, mapper), PhysicalOp.Project(child)) =>
      project(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals), mapper)
    case (logical, PhysicalOp.Pipeline(child, length)) =>
      interpretPipeline(logical, child, length, selectedPartition, preserveAbsence, ordinals)
    case (
          Plan.KeysOp(Plan.GroupByKeyOp(source)),
          PhysicalOp.Column(PhysicalOp.CountByKey(PhysicalOp.Exchange(child)), 0),
        ) =>
      // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
      // The count node is the typed witness. This arm publishes distinct keys, not counts.
      require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
      GroupByKeyKeysArms.columns(
        source,
        child,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (Plan.KeysOp(source), PhysicalOp.Column(child, 0)) =>
      pairProjection(
        source,
        child,
        ordinal = 0,
        selectedPartition = selectedPartition,
        preserveAbsence = preserveAbsence,
        ordinals = ordinals,
      )
    case (Plan.ValuesOp(source), PhysicalOp.Column(child, 1)) =>
      pairProjection(
        source,
        child,
        ordinal = 1,
        selectedPartition = selectedPartition,
        preserveAbsence = preserveAbsence,
        ordinals = ordinals,
      )
    case (Plan.MapValuesOp(source, mapper), PhysicalOp.MapValue(child)) =>
      val input = interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)
      if (IntLongParents.widened(child)) then IntLongParents.mapLong(input, mapper)
      else
        ExecutionRoute.mapValueArm(child) match
          case ExecutionRoute.MapValueArm.LongValues => mapLongValues(input, mapper)
          case ExecutionRoute.MapValueArm.StringValues => StringPairWalk.mapValues(input, mapper)
          case ExecutionRoute.MapValueArm.IntValues => mapIntValues(input, mapper)
    case (Plan.MapValuesOp(source, mapper), PhysicalOp.IntLongMapValue(child)) =>
      IntLongMapRun.columns(
        interpretAt(source, child, selectedPartition, preserveAbsence, ordinals),
        mapper,
      )
    case (Plan.MapIntKeyOp(source, mapper), PhysicalOp.MapKey(child)) =>
      MapIntKey.columns(
        interpretAt(source, child, selectedPartition, preserveAbsence, ordinals),
        mapper,
      )
    case (
          Plan.ReduceByKeyOp(source, reduceFunc),
          PhysicalOp.HashAggregate(PhysicalOp.Exchange(child), _),
        ) =>
      ExecutionRoute.reduceArm(source, child, reduceFunc) match
        case ExecutionRoute.Arm.IntLongReduce =>
          IntLongReduce.columns(
            source,
            child,
            reduceFunc,
            selectedPartition,
            preserveAbsence,
            ordinals,
          )
        case ExecutionRoute.Arm.IntHashAggregate =>
          ReduceArms.intByKey(
            source,
            child,
            reduceFunc,
            selectedPartition,
            preserveAbsence,
            ordinals,
          )
    case (
          Plan.ReduceByKeyOp(source, reduceFunc),
          PhysicalOp.LongHashAggregate(PhysicalOp.Exchange(child)),
        ) =>
      ReduceArms.longByKey(
        source,
        child,
        reduceFunc,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.ReduceByKeyOp(source, reduceFunc),
          PhysicalOp.StringHashAggregate(PhysicalOp.Exchange(child)),
        ) =>
      ReduceArms.stringByKey(
        source,
        child,
        reduceFunc,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.GroupByKeyOp(source),
          PhysicalOp.GroupByKey(PhysicalOp.Exchange(child)),
        ) =>
      // Boundary guard: take refuses a group, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
      child match
        case _: PhysicalOp.IntLongMapValue =>
          IntLongGroup.columns(source, child, preserveAbsence, ordinals)
        case _ =>
          GroupByArms.columns(source, child, selectedPartition, preserveAbsence, ordinals)
    case (
          Plan.CountByKeyOp(Plan.JoinOp(left, right, strategy)),
          PhysicalOp.CountByKey(
            PhysicalOp.HashJoin(
              PhysicalOp.Exchange(leftOp),
              PhysicalOp.Exchange(rightOp),
            ),
          ),
        )
        if JoinLowering.acceptsShuffleHash(strategy) ||
          JoinLowering.hintedInnerHashed(strategy) =>
      // The sides are already exchanged. There is no user project, so the count is the join key.
      require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
      JoinCountByKey.columns(left, right, leftOp, rightOp, preserveAbsence, ordinals)
    case (
          Plan.CountByKeyOp(source),
          PhysicalOp.CountByKey(PhysicalOp.Exchange(child)),
        ) =>
      ExecutionRoute.countArm(child) match
        case ExecutionRoute.CountArm.IntLong =>
          IntLongCount.columns(
            source,
            child,
            selectedPartition,
            preserveAbsence,
            ordinals,
          )
        case ExecutionRoute.CountArm.IntPairs | ExecutionRoute.CountArm.LongPairs |
            ExecutionRoute.CountArm.StringPairs =>
          CountByKeyArms.columns(source, child, selectedPartition, preserveAbsence, ordinals)
    case (plan, op) if CountSumColumns.accepts(plan, op) =>
      CountSumColumns.dispatch(plan, op, selectedPartition, preserveAbsence, ordinals)
    case (plan, op) if ExtremumColumns.accepts(plan, op) =>
      ExtremumColumns.dispatch(plan, op, selectedPartition, preserveAbsence, ordinals)
    case (
          Plan.ReduceByKeyOp(source, reduceFunc),
          PhysicalOp.IntLongHashAggregate(PhysicalOp.Exchange(child)),
        ) =>
      IntLongBinColumns.columns(
        source,
        child,
        reduceFunc,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.JoinOp(left, right, strategy),
          PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
        ) if JoinLowering.acceptsShuffleHash(strategy) =>
      hashJoinTriples(left, right, leftOp, rightOp, selectedPartition, preserveAbsence, ordinals)
    case (
          Plan.JoinOp(left, right, strategy),
          PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
        ) if JoinLowering.hintedInnerHashed(strategy) =>
      hashJoinTriples(left, right, leftOp, rightOp, selectedPartition, preserveAbsence, ordinals)
    case (plan, op) if IntLongJoinColumns.accepts(plan, op) =>
      IntLongJoinColumns.inner(plan, op, selectedPartition, preserveAbsence, ordinals)
    case (
          Plan.JoinOp(left, right, strategy),
          PhysicalOp.LongHashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
        ) if JoinLowering.acceptsShuffleHash(strategy) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.longInner(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.JoinOp(left, right, strategy),
          PhysicalOp.StringHashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
        ) if JoinLowering.acceptsShuffleHash(strategy) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      // A key or value stack on either side keeps the survivor index, including one filter. Both
      // dense sides stay on the batch exchange.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      Columns.StringTriples(
        StringJoinArms.inner(
          left,
          right,
          leftOp,
          rightOp,
          selectedPartition,
          preserveAbsence,
          ordinals,
        ),
      )
    case (
          Plan.JoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)),
          PhysicalOp.SortMergeJoin(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.sortMergeInner(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.JoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)),
          PhysicalOp.LongSortMergeJoin(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.longSortMerge(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.JoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)),
          PhysicalOp.StringSortMergeJoin(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.stringSortMerge(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.JoinOp(left, right, None | Some(Plan.JoinStrategy.Broadcast)),
          PhysicalOp.BroadcastJoin(leftOp, rightOp, buildSide),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.broadcastInner(
        left,
        right,
        leftOp,
        rightOp,
        buildSide,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.JoinOp(left, right, None | Some(Plan.JoinStrategy.Broadcast)),
          PhysicalOp.LongBroadcastJoin(leftOp, rightOp, buildSide),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.broadcastInnerLong(
        left,
        right,
        leftOp,
        rightOp,
        buildSide,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.JoinOp(left, right, None | Some(Plan.JoinStrategy.Broadcast)),
          PhysicalOp.StringBroadcastJoin(leftOp, rightOp, buildSide),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.broadcastInnerString(
        left,
        right,
        leftOp,
        rightOp,
        buildSide,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
          PhysicalOp.BroadcastLeftOuter(leftOp, rightOp),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.broadcastLeft(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.LeftOuterJoinOp(left, right, strategy),
          PhysicalOp.HashLeftOuter(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) if JoinLowering.acceptsShuffleHash(strategy) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.hashLeft(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
          PhysicalOp.BroadcastRightOuter(leftOp, rightOp),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.broadcastRight(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.RightOuterJoinOp(left, right, strategy),
          PhysicalOp.HashRightOuter(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) if JoinLowering.acceptsShuffleHash(strategy) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.hashRight(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
          PhysicalOp.BroadcastFullOuter(leftOp, rightOp),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.broadcastFull(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.FullOuterJoinOp(left, right, strategy),
          PhysicalOp.HashFullOuter(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) if JoinLowering.acceptsShuffleHash(strategy) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.hashFull(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)),
          PhysicalOp.SortMergeLeftOuter(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.sortMergeLeft(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)),
          PhysicalOp.SortMergeRightOuter(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.sortMergeRight(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (
          Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)),
          PhysicalOp.SortMergeFullOuter(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      PipelineJoinArms.sortMergeFull(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
    case (Plan.SortByIntValueDescOp(source), PhysicalOp.SortByIntValueDesc(child)) =>
      // Boundary guard: take uses the heap arm, so a selector never reaches this collect path.
      require(selectedPartition.isEmpty, "a partition selector cannot run a sort")
      SortArms.sort(source, child, ordinals)
    case (Plan.SortByIntKeyOp(source), PhysicalOp.SortByIntKey(child)) =>
      // Boundary guard: take runs the whole sort, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a sort")
      // The handoff copies the survivor index, drops those source batches, then radix-sorts.
      SelectedSort.intKey(source, child, ordinals)
    case (Plan.SortByLongKeyOp(source), PhysicalOp.SortByLongKey(child)) =>
      // Boundary guard: take runs the whole sort, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a sort")
      // The `None` path is a scan or one long-pair project. Those write both columns. One key or
      // value filter above that project keeps a survivor index of the projected batches. The slot
      // is not applied to the scan under the project. RankSplitLong sorts that index and does not
      // call visitKeys.
      SelectedSort.longKey(source, child, ordinals)
    case (Plan.SortByStringKeyOp(source), PhysicalOp.SortByStringKey(child)) =>
      // Boundary guard: take runs the whole sort, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a sort")
      // String-pair encode keeps a null key. A null key orders as "" and stays null. A filter
      // over a scan sorts survivor ids. The `None` path is a scan, one string-pair project, or
      // one key or value filter above that project. Those write both columns. StringRankSplit
      // sorts the published batches and does not call visitKeys. StringRankSplitRuns spills the
      // survivor index when the budget says so. Take decodes.
      val width = ColumnarPlanner.shuffleWidth
      val workers = ColumnarPlanner.parallelism
      val sorted = FilterChain.selected(
        source,
        child,
        selectedPartition = None,
        preserveAbsence = true,
        ordinals,
      ) match
        case Some(parts) =>
          StringRankSplitRuns.sortAndSplitSelected(parts, width, workers, ScarletConf.get)
        case None =>
          val batches = StringPairWalk.batches(
            interpretAt(
              source,
              child,
              selectedPartition = None,
              preserveAbsence = true,
              ordinals,
            ),
          )
          StringRankSplitRuns.sortAndSplit(batches, width, workers, ScarletConf.get)
      Columns.StringPairs(sorted)
    case (Plan.SortByLongValueDescOp(source), PhysicalOp.SortByLongValueDesc(child)) =>
      require(selectedPartition.isEmpty, "a partition selector cannot run a sort")
      Columns.LongPairs(
        ValueSort.longPairs(
          longPairsOf(
            interpretAt(
              source,
              child,
              selectedPartition = None,
              preserveAbsence = true,
              ordinals,
            ),
          ),
          ColumnarPlanner.shuffleWidth,
        ),
      )
    case (Plan.SortByStringValueDescOp(source), PhysicalOp.SortByStringValueDesc(child)) =>
      require(selectedPartition.isEmpty, "a partition selector cannot run a sort")
      Columns.StringPairs(
        ValueSort.stringPairs(
          StringPairWalk.batches(
            interpretAt(
              source,
              child,
              selectedPartition = None,
              preserveAbsence = true,
              ordinals,
            ),
          ),
          ColumnarPlanner.shuffleWidth,
        ),
      )
    case (plan, op) if IntLongSortColumns.accepts(plan, op) =>
      require(selectedPartition.isEmpty, "a partition selector cannot run a sort")
      IntLongSortColumns.dispatch(plan, op, ordinals)
    case (plan, op) if LongOuterArms.accepts(plan, op) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      LongOuterArms.run(plan, op, selectedPartition, ordinals)
    case (plan, op) if StringOuterArms.accepts(plan, op) =>
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      StringOuterArms.run(plan, op, selectedPartition, ordinals)
    case _ =>
      throw new IllegalStateException(
        s"physical tree does not match ${plan.getClass.getSimpleName}",
      )
