package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan, ScarletConf}

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
private[execution] object ColumnarInterpreter:

  private def pairProjection(
      source: Plan[_],
      child: PhysicalOp,
      ordinal: Int,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    if (IntLongParents.widened(child)) then
      IntLongParents.project(source, child, ordinal, selectedPartition, preserveAbsence, ordinals)
    else
      val projected = interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)
      val columns = projectColumn(pairBatches(child, projected), ordinal)
      StringPairWalk.wrapColumn(child.kind, columns, ordinal)

  /** The pair batches `child` produced. A wrong column type is a planner bug, not a fallback. */
  private def pairBatches(child: PhysicalOp, columns: Columns): Vector[ColumnBatch] =
    ExecutionRoute.pairArm(child) match
      case Some(ExecutionRoute.PairArm.LongPairs) => longPairsOf(columns)
      case Some(ExecutionRoute.PairArm.IntPairs) => pairsOf(columns)
      case Some(ExecutionRoute.PairArm.StringPairs) => StringPairWalk.batches(columns)
      case None =>
        throw new IllegalStateException("columnar projection expected a pair")

  private def projectColumn(parts: Vector[ColumnBatch], ordinal: Int): Vector[ColumnBatch] =
    parts.map { batch =>
      // A singleton encode stores the demanded ordinal at index 0.
      ColumnBatches.takeColumn(batch, encodedIndex(batch, ordinal))
    }

  private def mapIntValues(columns: Columns, mapper: Any): Columns =
    // Named erasure boundary. Lowering checked the declared `Int => Int` shape, so this cast
    // is safe.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val fn = mapper.asInstanceOf[Int => Int]
    Columns.Pairs(
      onEachMorsel(pairsOf(columns)) { morsel =>
        ColumnKernel.projectInt32(
          morsel.batch,
          1,
          morsel.start,
          morsel.length,
          value => fn(value),
        )
      },
    )

  private def mapLongValues(columns: Columns, mapper: Any): Columns =
    // Named erasure boundary. Lowering checked `Long => Long` via FunctionWitness, so this cast
    // is safe. A null value stays null and is not passed to the mapper. A null key is left as it
    // is.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val fn = mapper.asInstanceOf[Long => Long]
    Columns.LongPairs(
      onEachMorsel(longPairsOf(columns)) { morsel =>
        ColumnKernel.projectInt64(
          morsel.batch,
          1,
          morsel.start,
          morsel.length,
          value => fn(value),
        )
      },
    )

  /** Index of `ordinal` after a one-column encode. Both columns keep their indexes. */
  private def encodedIndex(batch: ColumnBatch, ordinal: Int): Int =
    if (batch.columns.length == 1) then 0 else ordinal

  private[execution] def batchesOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Ints(parts) => parts
    case Columns.Longs(parts) => parts
    case Columns.Floats(parts) => parts
    case Columns.Pairs(parts) => parts
    case Columns.LongPairs(parts) => parts
    case Columns.LongCounts(parts) => parts
    case Columns.CountSums(parts) => parts
    case Columns.LongCountSums(parts) => parts
    case Columns.StringCountSums(parts) => parts
    case Columns.Triples(parts) => parts
    case Columns.LongTriples(parts) => parts
    case Columns.Strings(parts) => parts
    case Columns.Bools(parts) => parts
    case Columns.StringPairs(parts) => parts
    case Columns.StringTriples(parts) => parts
    case Columns.Outer(parts, _) => parts
    case Columns.GroupedInts(_) | Columns.GroupedStrings(_) | Columns.GroupedLongs(_) |
        Columns.GroupedIntLongs(_) =>
      throw new IllegalStateException("grouped batches are not column batches")

  /** Number of groups. Other columns are not a grouped result. */
  private[execution] def groupCount(columns: Columns): Long = columns match
    case Columns.GroupedInts(parts) =>
      parts.foldLeft(0L)((total, batch) => total + batch.length.toLong)
    case Columns.GroupedStrings(parts) =>
      parts.foldLeft(0L)((total, batch) => total + batch.length.toLong)
    case Columns.GroupedLongs(parts) =>
      parts.foldLeft(0L)((total, batch) => total + batch.length.toLong)
    case Columns.GroupedIntLongs(parts) =>
      parts.foldLeft(0L)((total, batch) => total + batch.length.toLong)
    case _ =>
      throw new IllegalStateException("group count is only defined for a grouped batch")

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

  private def selectPartition[A](
      partitions: Seq[Partition[A]],
      selectedPartition: Option[Int],
  ): Seq[Partition[A]] = selectedPartition match
    case None => partitions
    case Some(index) => partitions.slice(index, index + 1)

  /**
   * Decode `columns` for collect and take. [[decodeWith]] picks the pair, triple, and long
   * decoders from the physical op. A key map and an int-pair project keep a null. Every other int
   * pair rejects a clear bit. Outer joins are [[Columns.Outer]].
   * [[KernelChoice.IntLongProjectSum]] is [[JoinProbeAggregate.decodeSums]], not the
   * `[Int32, Int64]` schema of a triple project. A grouped batch decodes to `(K, Iterable[V])`. An
   * int-long group keeps a null value.
   */
  private[execution] def decodeFor(columns: Columns, op: PhysicalOp): Seq[Partition[_]] =
    decodeRouted(columns, op, None)

  /** [[decodeFor]] using the binder's tag for this `plan`. */
  private[execution] def decodeFor(
      columns: Columns,
      op: PhysicalOp,
      plan: Plan[_],
  ): Seq[Partition[_]] =
    decodeRouted(columns, op, Some(plan))

  /** Interpret `plan` and decode it with the binder's tag. */
  private[execution] def decodeLowered(plan: Plan[_], op: PhysicalOp): Seq[Partition[_]] =
    decodeFor(interpret(plan, op), op, plan)

  private def decodeRouted(
      columns: Columns,
      op: PhysicalOp,
      plan: Option[Plan[_]],
  ): Seq[Partition[_]] =
    if (ExecutionRoute.decodesAsIntLongSum(plan, op)) then intLongSums(columns)
    else if (IntLongMapRun.decodes(op)) then IntLongMapRun.partitions(columns)
    else
      ColumnDecode.partitions(
        columns,
        ColumnDecoders.ints(op),
        ColumnDecoders.intPairs(op),
        ColumnDecoders.longs(op),
        ColumnDecoders.triples(op),
        ColumnDecoders.floats(op),
      )

  /** Groups of an int-long reduce. A non-pair batch is a bug in the arm, not a row fallback. */
  private def intLongSums(columns: Columns): Seq[Partition[_]] = columns match
    case Columns.Pairs(parts) =>
      parts.map(batch => Partition(JoinProbeAggregate.decodeSums(batch)))
    case _ =>
      throw new IllegalStateException("int-long reduceByKey expected pair batches")

  /**
   * Prefix of [[decodeFor]]. The same op chooses the int, int-pair, long, and triple decoders.
   * `limit <= 0` returns empty without reading a batch.
   */
  private[execution] def decodePrefixFor(columns: Columns, limit: Int, op: PhysicalOp): Seq[Any] =
    if (IntLongMapRun.decodes(op)) then IntLongMapRun.prefix(columns, limit)
    else
      decodePrefixWith(
        columns,
        limit,
        ColumnDecoders.ints(op),
        ColumnDecoders.intPairs(op),
        ColumnDecoders.longs(op),
        ColumnDecoders.triples(op),
        ColumnDecoders.floats(op),
      )

  private def decodePrefixWith(
      columns: Columns,
      limit: Int,
      decodeInts: (ColumnBatch, Int) => Seq[Int],
      decodePairs: (ColumnBatch, Int) => Seq[(Int, Int)],
      decodeLongs: (ColumnBatch, Int) => Seq[Long],
      decodeTriples: (ColumnBatch, Int) => Seq[(Int, (Int, Int))],
      decodeFloats: (ColumnBatch, Int) => Seq[Double],
  ): Seq[Any] =
    if (limit <= 0) then Seq.empty
    else
      columns match
        case Columns.Ints(parts) =>
          prefixRows(parts, limit, decodeInts)
        case Columns.Longs(parts) =>
          prefixRows(parts, limit, decodeLongs)
        case Columns.Floats(parts) =>
          prefixRows(parts, limit, decodeFloats)
        case Columns.Pairs(parts) =>
          prefixRows(parts, limit, decodePairs)
        case Columns.LongCounts(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeLongCounts(batch, rows))
        case Columns.CountSums(parts) =>
          prefixRows(parts, limit, CountSum.rows)
        case Columns.LongCountSums(parts) =>
          prefixRows(parts, limit, LongCountSum.rows)
        case Columns.StringCountSums(parts) =>
          prefixRows(parts, limit, StringCountSum.rows)
        case Columns.LongPairs(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeLongPairs(batch, rows))
        case Columns.Triples(parts) =>
          // Joins are not partition-preserving, so `take` does not reach this arm.
          prefixRows(parts, limit, decodeTriples)
        case Columns.LongTriples(parts) =>
          // Joins are not partition-preserving, so `take` does not reach this arm.
          prefixRows(parts, limit, BatchCodec.decodeLongTriplesPreservingAbsence)
        case Columns.Outer(parts, absent) =>
          prefixRows(parts, limit, (batch, rows) => ColumnDecode.outer(batch, absent, rows))
        case Columns.Strings(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeStrings(batch, rows))
        case Columns.StringPairs(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeStringPairs(batch, rows))
        case Columns.StringTriples(parts) =>
          // Joins are not partition-preserving, so `take` does not reach this arm.
          prefixRows(parts, limit, BatchCodec.decodeStringJoinKeepingNulls)
        case Columns.Bools(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeBools(batch, rows))
        case Columns.GroupedInts(_) | Columns.GroupedStrings(_) | Columns.GroupedLongs(_) |
            Columns.GroupedIntLongs(_) =>
          throw new IllegalStateException("take refuses a grouped batch")

  private def prefixRows[A](
      parts: Vector[ColumnBatch],
      limit: Int,
      decode: (ColumnBatch, Int) => Seq[A],
  ): Seq[Any] =
    ColumnarPlanner.takeChunks(
      limit,
      parts.iterator.map { batch => (remaining: Int) =>
        decode(batch, remaining)
      },
    )

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
            PhysicalOp.Kind.IntLongSums | PhysicalOp.Kind.GroupedIntLongs =>
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
    case (
          Plan.CountSumByKeyOp(source),
          PhysicalOp.CountSum(PhysicalOp.Exchange(child)),
        ) =>
      CountSumArms.columns(source, child, selectedPartition, preserveAbsence, ordinals)
    case (
          Plan.CountSumByLongKeyOp(source),
          PhysicalOp.LongCountSum(PhysicalOp.Exchange(child)),
        ) =>
      CountSumColumns.longs(source, child, selectedPartition, preserveAbsence, ordinals)
    case (
          Plan.CountSumByStringKeyOp(source),
          PhysicalOp.StringCountSum(PhysicalOp.Exchange(child)),
        ) =>
      CountSumColumns.strings(source, child, selectedPartition, preserveAbsence, ordinals)
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
          Plan.JoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
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
          Plan.JoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
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
          Plan.JoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
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

  /** Hash join of two exchanges. A selector never reaches this arm: take refuses a join. */
  private def hashJoinTriples(
      left: Plan[_],
      right: Plan[_],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    require(selectedPartition.isEmpty, "a partition selector cannot run a join")
    Columns.Triples(
      JoinArms.inner(
        left,
        right,
        leftOp,
        rightOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
      ),
    )

  private def interpretPipeline(
      plan: Plan[_],
      child: PhysicalOp,
      length: Int,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    val (source, steps) = ColumnarPlanner.elementSteps(plan, length)
    if (steps.length < length) then
      // Lowering uses this same collector, so this only guards a malformed physical tree.
      throw new IllegalStateException("pipeline does not match logical element steps")
    pipeline(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals), steps)

  private def pipeline(
      columns: Columns,
      steps: List[ColumnarPlanner.ElementStep],
  ): Columns =
    def rejected: Nothing =
      throw new IllegalStateException("pipeline expects an int, long, float, or string column")

    columns match
      case Columns.Ints(parts) =>
        Columns.Ints(onEachMorsel(parts)(morsel => ElementPipeline.ints(morsel, steps)))
      case Columns.Longs(parts) =>
        Columns.Longs(onEachMorsel(parts)(morsel => ElementPipeline.longs(morsel, steps)))
      case Columns.Floats(parts) =>
        Columns.Floats(onEachMorsel(parts)(morsel => ElementPipeline.floats(morsel, steps)))
      case Columns.Strings(parts) =>
        Columns.Strings(onEachMorsel(parts)(morsel => ElementPipeline.strings(morsel, steps)))
      case Columns.Bools(_) | Columns.Pairs(_) | Columns.LongPairs(_) | Columns.LongCounts(_) |
          Columns.CountSums(_) | Columns.LongCountSums(_) | Columns.StringCountSums(_) |
          Columns.Triples(_) | Columns.LongTriples(_) | Columns.StringPairs(_) |
          Columns.StringTriples(_) | Columns.Outer(_, _) | Columns.GroupedInts(_) |
          Columns.GroupedStrings(_) | Columns.GroupedLongs(_) | Columns.GroupedIntLongs(_) =>
        rejected

  private def project(columns: Columns, mapper: Any): Columns = columns match
    case Columns.Ints(parts) =>
      // Named erasure boundary. Lowering already checked the declared `Int => Int` shape.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val fn = mapper.asInstanceOf[Int => Int]
      Columns.Ints(onEachMorsel(parts) { morsel =>
        ColumnKernel.projectInt32(
          morsel.batch,
          0,
          morsel.start,
          morsel.length,
          value => fn(value),
        )
      })
    case Columns.Longs(parts) =>
      // Named erasure boundary. Lowering already checked the declared `Long => Long` shape.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val fn = mapper.asInstanceOf[Long => Long]
      Columns.Longs(onEachMorsel(parts) { morsel =>
        ColumnKernel.projectInt64(
          morsel.batch,
          0,
          morsel.start,
          morsel.length,
          value => fn(value),
        )
      })
    case Columns.Floats(parts) =>
      // Named erasure boundary. Lowering already checked the declared `Double => Double` shape.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val fn = mapper.asInstanceOf[Double => Double]
      Columns.Floats(onEachMorsel(parts) { morsel =>
        ColumnKernel.projectFloat64(
          morsel.batch,
          0,
          morsel.start,
          morsel.length,
          value => fn(value),
        )
      })
    case Columns.Strings(parts) =>
      // Named erasure boundary. A declared string map may return another value; `stringResult`
      // turns that into `ColumnarFallback`, which is the named row-path fallback.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Any"))
      val fn = mapper.asInstanceOf[String => Any]
      Columns.Strings(onEachMorsel(parts) { morsel =>
        ColumnKernel.projectUtf8(
          morsel.batch,
          0,
          morsel.start,
          morsel.length,
          value => ColumnarPlanner.stringResult(fn(value)),
        )
      })
    case Columns.Triples(parts) =>
      // Named erasure boundary. The adapter returns Any. A type-changing triple throws
      // ColumnarFallback inside the kernel, before the output batch is published, instead of a
      // raw ClassCastException from this cast. The cast itself succeeds for any Function1.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.Any"))
      val fn = mapper.asInstanceOf[((Int, (Int, Int))) => Any]
      val adapted: PairKernel.IntTripleMap = (key, left, right) => fn((key, (left, right)))
      Columns.Triples(onEachMorsel(parts) { morsel =>
        PairKernel.mapTriple(morsel.batch, morsel.start, morsel.length, adapted)
      })
    case Columns.Pairs(parts) =>
      // A non-int cell throws ColumnarFallback before publish. The Tuple2 witness is not a call.
      val adapted = IntPairProject.adapt(mapper)
      Columns.Pairs(onEachMorsel(parts) { morsel =>
        IntPairProject.project(morsel.batch, morsel.start, morsel.length, adapted)
      })
    case Columns.LongPairs(parts) =>
      // Child kind selected longs. A non-long cell throws ColumnarFallback before publish.
      val adapted = LongPairProject.adapt(mapper)
      Columns.LongPairs(onEachMorsel(parts) { morsel =>
        LongPairProject.project(morsel.batch, morsel.start, morsel.length, adapted)
      })
    case Columns.StringPairs(parts) => StringPairWalk.project(parts, mapper)
    case Columns.Bools(_) | Columns.LongCounts(_) | Columns.CountSums(_) |
        Columns.LongCountSums(_) | Columns.StringCountSums(_) | Columns.LongTriples(_) |
        Columns.StringTriples(_) | Columns.Outer(_, _) | Columns.GroupedInts(_) |
        Columns.GroupedStrings(_) | Columns.GroupedLongs(_) | Columns.GroupedIntLongs(_) =>
      throw new IllegalStateException(
        s"columnar map expects ${elementColumnList(includeBooleans = false)}",
      )

  private[execution] def pairsOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Pairs(parts) if parts.nonEmpty => parts
    case Columns.Pairs(_) =>
      throw new IllegalStateException("columnar pairs have no partitions")
    case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.LongPairs(_) |
        Columns.LongCounts(_) | Columns.CountSums(_) | Columns.LongCountSums(_) |
        Columns.StringCountSums(_) | Columns.Triples(_) | Columns.LongTriples(_) |
        Columns.Strings(_) | Columns.Bools(_) | Columns.StringPairs(_) | Columns.StringTriples(_) |
        Columns.Outer(_, _) | Columns.GroupedInts(_) | Columns.GroupedStrings(_) |
        Columns.GroupedLongs(_) | Columns.GroupedIntLongs(_) =>
      throw new IllegalStateException("columnar op expected int pairs")

  private[execution] def triplesOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Triples(parts) if parts.nonEmpty => parts
    case Columns.Triples(_) =>
      throw new IllegalStateException("columnar triples have no partitions")
    case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.Pairs(_) |
        Columns.LongPairs(_) | Columns.LongCounts(_) | Columns.CountSums(_) |
        Columns.LongCountSums(_) | Columns.StringCountSums(_) | Columns.LongTriples(_) |
        Columns.Strings(_) | Columns.Bools(_) | Columns.StringPairs(_) | Columns.StringTriples(_) |
        Columns.Outer(_, _) | Columns.GroupedInts(_) | Columns.GroupedStrings(_) |
        Columns.GroupedLongs(_) | Columns.GroupedIntLongs(_) =>
      throw new IllegalStateException("columnar op expected int triples")

  /**
   * Kinds an element filter or map accepts. A filter accepts booleans. A map does not: a boolean
   * project is refused before execution, and the map arm is the guard if one arrives anyway.
   */
  private def elementColumnList(includeBooleans: Boolean): String =
    if (includeBooleans) then "ints, longs, floats, booleans, or strings"
    else "ints, longs, floats, or strings"

  /**
   * Cuts each partition, visits every piece, and regroups one batch per input partition. An empty
   * `parts` returns unchanged and does not call the scheduler. Results keep the flatten order of
   * the cuts, which is what `regroup` depends on.
   */
  private def fanOut[A](
      parts: Vector[ColumnBatch],
  )(cuts: ColumnBatch => Vector[A])(fn: A => ColumnBatch): Vector[ColumnBatch] =
    if (parts.isEmpty) then parts
    else
      val cut = parts.map(cuts)
      val mapped =
        MorselScheduler.map(cut.flatten, ColumnarPlanner.parallelism)((_, piece) => fn(piece))
      regroup(cut.map(_.length), mapped)

  /**
   * One visit per morsel of the shared batch. The function owns its output. Regroup uses the same
   * per-part visit counts as the fan-out above.
   */
  private[execution] def onEachMorsel(
      parts: Vector[ColumnBatch],
  )(fn: Morsel => ColumnBatch): Vector[ColumnBatch] =
    fanOut(parts)(part => Morsels.of(part, ColumnarPlanner.batchSize))(fn)

  private[execution] def regroup(
      counts: Vector[Int],
      flat: Vector[ColumnBatch],
  ): Vector[ColumnBatch] =
    val seed: (Vector[ColumnBatch], Vector[ColumnBatch]) = (flat, Vector.empty[ColumnBatch])
    val (_, grouped) = counts.foldLeft(seed) { (state, count) =>
      val (rest, acc) = state
      val (group, tail) = rest.splitAt(count)
      (tail, acc :+ ColumnBatches.concat(group))
    }
    grouped

  private[execution] def longPairsOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.LongPairs(parts) if parts.nonEmpty => parts
    case Columns.LongPairs(_) =>
      throw new IllegalStateException("columnar long pairs have no partitions")
    case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.Pairs(_) |
        Columns.LongCounts(_) | Columns.CountSums(_) | Columns.LongCountSums(_) |
        Columns.StringCountSums(_) | Columns.Triples(_) | Columns.LongTriples(_) |
        Columns.Strings(_) | Columns.Bools(_) | Columns.StringPairs(_) | Columns.StringTriples(_) |
        Columns.Outer(_, _) | Columns.GroupedInts(_) | Columns.GroupedStrings(_) |
        Columns.GroupedLongs(_) | Columns.GroupedIntLongs(_) =>
      throw new IllegalStateException("columnar op expected long pairs")
