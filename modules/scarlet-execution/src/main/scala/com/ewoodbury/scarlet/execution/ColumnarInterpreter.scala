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
 * [[ColumnarFallback]] is the named form of a deliberate decline, including a null value on a
 * left, right, or full outer join: the value columns are already encoded, and the kernel is not
 * chosen. A null value on a long inner hash join stays columnar. A clear validity bit is a null
 * value, not an absence bit. A selected null on an int inner join, and a null on the dense side of
 * a mixed int hash join, stay a clear validity bit. That bit is not absence. A null on a dropped
 * row is not a decline. A null value on a string inner hash join stays columnar. String
 * `reduceByKey` and string `groupByKey` with a live null value are that fallback. Outer joins
 * still decline. A string-pair filter keeps the null int. A value projection that kept a null int
 * is that fallback too. With spill off, an over-budget int, long, or string `countByKey` throws
 * `ClosedKernelRefusal`, which does not extend `ClassCastException`, so the action fails instead
 * of rerunning on rows. With spill on, an over-budget post-exchange bucket spills. A count table
 * that still does not fit throws `ClosedKernelRefusal`. The long estimate charges 8-byte keys. The
 * string estimate charges dictionary bytes. The running partial is `O(distinct keys)`.
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
    val projected = interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)
    val columns = projectColumn(pairBatches(child, projected), ordinal)
    StringPairWalk.wrapColumn(child.kind, columns, ordinal)

  /** The pair batches `child` produced. A wrong column type is a planner bug, not a fallback. */
  private def pairBatches(child: PhysicalOp, columns: Columns): Vector[ColumnBatch] =
    child.kind match
      case PhysicalOp.Kind.LongPairs => longPairsOf(columns)
      case PhysicalOp.Kind.Pairs => pairsOf(columns)
      case PhysicalOp.Kind.StringPairs => StringPairWalk.batches(columns)
      case _ =>
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
    case Columns.Triples(parts) => parts
    case Columns.LongTriples(parts) => parts
    case Columns.Strings(parts) => parts
    case Columns.Bools(parts) => parts
    case Columns.StringPairs(parts) => parts
    case Columns.StringTriples(parts) => parts
    case Columns.Outer(parts, _) => parts
    case Columns.GroupedInts(_) | Columns.GroupedStrings(_) | Columns.GroupedLongs(_) =>
      throw new IllegalStateException("grouped batches are not column batches")

  /** Number of groups. Other columns are not a grouped result. */
  private[execution] def groupCount(columns: Columns): Long = columns match
    case Columns.GroupedInts(parts) =>
      parts.foldLeft(0L)((total, batch) => total + batch.length.toLong)
    case Columns.GroupedStrings(parts) =>
      parts.foldLeft(0L)((total, batch) => total + batch.length.toLong)
    case Columns.GroupedLongs(parts) =>
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

  private def selectPartition[A](
      partitions: Seq[Partition[A]],
      selectedPartition: Option[Int],
  ): Seq[Partition[A]] = selectedPartition match
    case None => partitions
    case Some(index) => partitions.slice(index, index + 1)

  /**
   * Decode `columns`. Int pairs use [[BatchCodec.decodeIntPairsPreservingAbsence]] only for
   * [[PhysicalOp.SortByIntKey]]. Every other int-pair op uses [[BatchCodec.decodeIntPairs]], which
   * rejects a clear bit. Long pairs use [[BatchCodec.decodeLongPairs]], which already keeps a
   * clear bit. [[PhysicalOp.SortByLongKey]] returns long pairs, so collect and take use that
   * decoder and do not call the int-pair function. A long column projected from a long pair keeps
   * a clear bit via [[BatchCodec.decodeLongsPreservingAbsence]]. An element long scan still uses
   * [[BatchCodec.decodeLongs]], which rejects a clear bit. Int triples use
   * [[BatchCodec.decodeIntTriplesPreservingAbsence]] for a [[PhysicalOp.Project]] and for an int
   * inner join: [[PhysicalOp.HashJoin]], [[PhysicalOp.BroadcastJoin]], or
   * [[PhysicalOp.SortMergeJoin]]. A map or that join may keep a null component. A filter or other
   * triple op uses [[BatchCodec.decodeIntTriples]], which rejects a clear bit. A whole-row filter
   * drops those rows before decode. Outer joins are [[Columns.Outer]] and do not use this decoder.
   * Long triples are only a long inner join and use
   * [[BatchCodec.decodeLongTriplesPreservingAbsence]]. A clear validity bit is a null value, not
   * an absence bit. A fully present batch still decodes to the same longs. String triples are a
   * string inner hash join and use [[BatchCodec.decodeStringJoinKeepingNulls]]. A clear value bit
   * is a null int, not an absence bit. A clear key bit is still an error. A fully present batch
   * still decodes to the same strings and ints.
   */
  private[execution] def decodeFor(columns: Columns, op: PhysicalOp): Seq[Partition[_]] =
    decodeWith(columns, intPairDecoder(op), longDecoder(op), tripleDecoder(op))

  /**
   * Prefix of [[decodeFor]]. The same op chooses the int-pair decoder and the long decoder.
   * `limit <= 0` returns empty without reading a batch.
   */
  private[execution] def decodePrefixFor(columns: Columns, limit: Int, op: PhysicalOp): Seq[Any] =
    decodePrefixWith(columns, limit, intPairDecoder(op), longDecoder(op), tripleDecoder(op))

  /**
   * Long decoder for `op`. A key or value column of a long pair keeps a null. Every other long
   * column rejects one.
   */
  private def longDecoder(op: PhysicalOp): (ColumnBatch, Int) => Seq[Long] =
    if (absentLongColumn(op)) then BatchCodec.decodeLongsPreservingAbsence
    else (batch, rows) => BatchCodec.decodeLongs(batch, rows)

  private def absentLongColumn(op: PhysicalOp): Boolean = op match
    case PhysicalOp.Column(child, _) =>
      child.kind match
        case PhysicalOp.Kind.LongPairs => true
        case _ => false
    case _ => false

  /**
   * Int-pair decoder for `op`. [[PhysicalOp.SortByLongKey]] and [[PhysicalOp.SortByStringKey]] are
   * their own arms. Those decoders do not call the returned function, and these arms must not fall
   * through to [[BatchCodec.decodeIntPairs]], which rejects a clear bit.
   */
  private def intPairDecoder(op: PhysicalOp): (ColumnBatch, Int) => Seq[(Int, Int)] =
    op match
      case _: PhysicalOp.SortByIntKey => BatchCodec.decodeIntPairsPreservingAbsence
      case _: PhysicalOp.SortByLongKey =>
        (_, _) =>
          throw new IllegalStateException(
            "SortByLongKey is decoded as long pairs, not int pairs",
          )
      case _: PhysicalOp.SortByStringKey =>
        (_, _) =>
          throw new IllegalStateException(
            "SortByStringKey is decoded as string pairs, not int pairs",
          )
      case _ => (batch, rows) => BatchCodec.decodeIntPairs(batch, rows)

  /**
   * An int inner join or a triple map may keep a null component. A filter or other triple op
   * rejects a clear bit. A whole-row filter drops that row before decode. Outer joins are
   * [[Columns.Outer]] and do not use this decoder.
   */
  private def tripleDecoder(op: PhysicalOp): (ColumnBatch, Int) => Seq[(Int, (Int, Int))] =
    op match
      case _: PhysicalOp.Project | _: PhysicalOp.HashJoin | _: PhysicalOp.BroadcastJoin |
          _: PhysicalOp.SortMergeJoin =>
        BatchCodec.decodeIntTriplesPreservingAbsence
      case _ => (batch, rows) => BatchCodec.decodeIntTriples(batch, rows)

  private[execution] def decodePrefix(columns: Columns, limit: Int): Seq[Any] =
    decodePrefixWith(
      columns,
      limit,
      (batch, rows) => BatchCodec.decodeIntPairs(batch, rows),
      (batch, rows) => BatchCodec.decodeLongs(batch, rows),
      (batch, rows) => BatchCodec.decodeIntTriples(batch, rows),
    )

  private def decodePrefixWith(
      columns: Columns,
      limit: Int,
      decodePairs: (ColumnBatch, Int) => Seq[(Int, Int)],
      decodeLongs: (ColumnBatch, Int) => Seq[Long],
      decodeTriples: (ColumnBatch, Int) => Seq[(Int, (Int, Int))],
  ): Seq[Any] =
    if (limit <= 0) then Seq.empty
    else
      columns match
        case Columns.Ints(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeInts(batch, rows))
        case Columns.Longs(parts) =>
          prefixRows(parts, limit, decodeLongs)
        case Columns.Floats(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeFloat64s(batch, rows))
        case Columns.Pairs(parts) =>
          prefixRows(parts, limit, decodePairs)
        case Columns.LongCounts(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeLongCounts(batch, rows))
        case Columns.LongPairs(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeLongPairs(batch, rows))
        case Columns.Triples(parts) =>
          // Joins are not partition-preserving, so `take` does not reach this arm.
          prefixRows(parts, limit, decodeTriples)
        case Columns.LongTriples(parts) =>
          // Joins are not partition-preserving, so `take` does not reach this arm.
          prefixRows(parts, limit, BatchCodec.decodeLongTriplesPreservingAbsence)
        case Columns.Outer(parts, absent) =>
          prefixRows(parts, limit, (batch, rows) => decodeOuterBatch(batch, absent, rows))
        case Columns.Strings(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeStrings(batch, rows))
        case Columns.StringPairs(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeStringPairs(batch, rows))
        case Columns.StringTriples(parts) =>
          // Joins are not partition-preserving, so `take` does not reach this arm.
          prefixRows(parts, limit, BatchCodec.decodeStringJoinKeepingNulls)
        case Columns.Bools(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeBools(batch, rows))
        case Columns.GroupedInts(_) | Columns.GroupedStrings(_) | Columns.GroupedLongs(_) =>
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
      if (selectedPartition.nonEmpty) then
        throw new ColumnarFallback("a partition selector cannot run a distinct")
      val batches =
        batchesOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals))
      val width = ColumnarPlanner.shuffleWidth
      child.kind match
        case PhysicalOp.Kind.Ints => Columns.Ints(DistinctKernel.ints(batches, width))
        case PhysicalOp.Kind.Strings => Columns.Strings(DistinctKernel.strings(batches, width))
        case _ =>
          throw new IllegalStateException(
            s"distinct column is ${child.kind}, expected int or string",
          )
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
            PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter =>
          throw new IllegalStateException(
            s"physical tree does not match ${plan.getClass.getSimpleName}",
          )
    // A string-pair filter is not [[FilterChain.accepts]]. A key or value stack stays in
    // [[StringPairWalk]]. A whole-row string stack is one selection pass there. A long-pair row
    // filter still chains one kernel per filter on this path. Collect reads
    // [[FilterChain.selected]].
    // `accepts` walks the logical plan, so it runs only for a filter node.
    case (logical, physical @ PhysicalOp.Filter(child, _))
        if child.kind eq PhysicalOp.Kind.StringPairs =>
      StringPairWalk.filter(logical, physical, selectedPartition, preserveAbsence, ordinals)
    case (logical, physical)
        if physical.isInstanceOf[PhysicalOp.Filter] && FilterChain.accepts(logical, physical) =>
      FilterChain.run(logical, physical, selectedPartition, preserveAbsence, ordinals)
    case (Plan.MapOp(source, mapper), PhysicalOp.Project(child)) =>
      project(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals), mapper)
    case (logical, PhysicalOp.Pipeline(child, length)) =>
      interpretPipeline(logical, child, length, selectedPartition, preserveAbsence, ordinals)
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
      child.kind match
        case PhysicalOp.Kind.LongPairs => mapLongValues(input, mapper)
        case PhysicalOp.Kind.StringPairs => StringPairWalk.mapValues(input, mapper)
        case _ => mapIntValues(input, mapper)
    case (
          Plan.ReduceByKeyOp(source, reduceFunc),
          PhysicalOp.HashAggregate(PhysicalOp.Exchange(child)),
        ) =>
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
      GroupByArms.columns(source, child, selectedPartition, preserveAbsence, ordinals)
    case (
          Plan.CountByKeyOp(source),
          PhysicalOp.CountByKey(PhysicalOp.Exchange(child)),
        ) =>
      CountByKeyArms.columns(source, child, selectedPartition, preserveAbsence, ordinals)
    case (
          Plan.JoinOp(left, right, strategy),
          PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
        ) if JoinLowering.acceptsShuffleHash(strategy) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
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
    case (Plan.SortByIntKeyOp(source), PhysicalOp.SortByIntKey(child)) =>
      // Boundary guard: take runs the whole sort, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a sort")
      // preserveAbsence keeps a null key and a null value. A null key still orders as zero.
      // A slot-filter stack sorts survivor ids, including one filter, and does not gather first.
      // The `None` path is the only one that interprets that child. RankSplitRuns chooses the
      // in-memory split or a spill of the survivor index. Over the budget, with spill on, that
      // index is written and dropped rows are not. Take decodes the prefix.
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
          RankSplitRuns.sortAndSplitSelected(parts, width, workers, ScarletConf.get)
        case None =>
          val batches = pairsOf(
            interpretAt(
              source,
              child,
              selectedPartition = None,
              preserveAbsence = true,
              ordinals,
            ),
          )
          RankSplitRuns.sortAndSplit(batches, width, workers, ScarletConf.get)
      Columns.Pairs(sorted)
    case (Plan.SortByLongKeyOp(source), PhysicalOp.SortByLongKey(child)) =>
      // Boundary guard: take runs the whole sort, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a sort")
      // Long-pair encode always keeps a null key and a null value. preserveAbsence is passed for
      // symmetry with the int sort. It does not change the long encode. A null key orders as zero
      // and stays null. A key, value, or row filter sorts survivor ids and does not gather first.
      // The physical op stays SortByLongKey. RankSplitLongRuns chooses the in-memory split
      // or a spill of the survivor index. Take decodes the prefix.
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
          RankSplitLongRuns.sortAndSplitSelected(parts, width, workers, ScarletConf.get)
        case None =>
          val batches = longPairsOf(
            interpretAt(
              source,
              child,
              selectedPartition = None,
              preserveAbsence = true,
              ordinals,
            ),
          )
          RankSplitLongRuns.sortAndSplit(batches, width, workers, ScarletConf.get)
      Columns.LongPairs(sorted)
    case (Plan.SortByStringKeyOp(source), PhysicalOp.SortByStringKey(child)) =>
      // Boundary guard: take runs the whole sort, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a sort")
      // String-pair encode keeps a null key. preserveAbsence matches the other sorts. A null key
      // orders as "" and stays null. A key, value, or row filter sorts survivor ids and does not
      // gather first. The `None` path is the scan. StringRankSplitRuns chooses the in-memory split
      // or a spill of the
      // survivor index. Take decodes the prefix.
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
          Columns.Triples(_) | Columns.LongTriples(_) | Columns.StringPairs(_) |
          Columns.StringTriples(_) | Columns.Outer(_, _) | Columns.GroupedInts(_) |
          Columns.GroupedStrings(_) | Columns.GroupedLongs(_) =>
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
    case Columns.Bools(_) | Columns.Pairs(_) | Columns.LongPairs(_) | Columns.LongCounts(_) |
        Columns.LongTriples(_) | Columns.StringPairs(_) | Columns.StringTriples(_) |
        Columns.Outer(_, _) | Columns.GroupedInts(_) | Columns.GroupedStrings(_) |
        Columns.GroupedLongs(_) =>
      throw new IllegalStateException(
        s"columnar map expects ${elementColumnList(includeBooleans = false)}",
      )

  private[execution] def pairsOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Pairs(parts) if parts.nonEmpty => parts
    case Columns.Pairs(_) =>
      throw new IllegalStateException("columnar pairs have no partitions")
    case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.LongPairs(_) |
        Columns.LongCounts(_) | Columns.Triples(_) | Columns.LongTriples(_) | Columns.Strings(_) |
        Columns.Bools(_) | Columns.StringPairs(_) | Columns.StringTriples(_) |
        Columns.Outer(_, _) | Columns.GroupedInts(_) | Columns.GroupedStrings(_) |
        Columns.GroupedLongs(_) =>
      throw new IllegalStateException("columnar op expected int pairs")

  private[execution] def triplesOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Triples(parts) if parts.nonEmpty => parts
    case Columns.Triples(_) =>
      throw new IllegalStateException("columnar triples have no partitions")
    case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.Pairs(_) |
        Columns.LongPairs(_) | Columns.LongCounts(_) | Columns.LongTriples(_) |
        Columns.Strings(_) | Columns.Bools(_) | Columns.StringPairs(_) | Columns.StringTriples(_) |
        Columns.Outer(_, _) | Columns.GroupedInts(_) | Columns.GroupedStrings(_) |
        Columns.GroupedLongs(_) =>
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
        Columns.LongCounts(_) | Columns.Triples(_) | Columns.LongTriples(_) | Columns.Strings(_) |
        Columns.Bools(_) | Columns.StringPairs(_) | Columns.StringTriples(_) |
        Columns.Outer(_, _) | Columns.GroupedInts(_) | Columns.GroupedStrings(_) |
        Columns.GroupedLongs(_) =>
      throw new IllegalStateException("columnar op expected long pairs")

  private def decodeOuterBatch(
      batch: ColumnBatch,
      absent: AbsentSide,
      maxRows: Int,
  ): Seq[Any] =
    if (StringOuterDecode.isStringOuter(batch)) then
      StringOuterDecode.decode(batch, absent, maxRows)
    else LongOuterDecode.decode(batch, absent, maxRows)

  private def decodeWith(
      columns: Columns,
      decodePairs: (ColumnBatch, Int) => Seq[(Int, Int)],
      decodeLongs: (ColumnBatch, Int) => Seq[Long],
      decodeTriples: (ColumnBatch, Int) => Seq[(Int, (Int, Int))],
  ): Seq[Partition[_]] = columns match
    case Columns.Ints(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeInts(batch)))
    case Columns.Longs(parts) =>
      parts.map(batch => Partition(decodeLongs(batch, batch.length)))
    case Columns.Floats(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeFloat64s(batch)))
    case Columns.Pairs(parts) =>
      parts.map(batch => Partition(decodePairs(batch, batch.length)))
    case Columns.LongCounts(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeLongCounts(batch)))
    case Columns.LongPairs(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeLongPairs(batch)))
    case Columns.Triples(parts) =>
      parts.map(batch => Partition(decodeTriples(batch, batch.length)))
    case Columns.LongTriples(parts) =>
      // Named erasure boundary for the long join. The decoded rows are `(Long, (Long, Long))`.
      // [[ColumnarPlanner.runLowered]] casts the partition sequence to the plan's element type.
      parts.map(batch => Partition(BatchCodec.decodeLongTriplesPreservingAbsence(batch)))
    case Columns.Outer(parts, absent) =>
      parts.map(batch => Partition(decodeOuterBatch(batch, absent, batch.length)))
    case Columns.Strings(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeStrings(batch)))
    case Columns.StringPairs(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeStringPairs(batch)))
    case Columns.StringTriples(parts) =>
      // Named erasure boundary for the string join. The decoded rows are `(String, (Int, Int))`.
      // [[ColumnarPlanner.runLowered]] casts the partition sequence to the plan's element type.
      parts.map(batch => Partition(BatchCodec.decodeStringJoinKeepingNulls(batch, batch.length)))
    case Columns.Bools(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeBools(batch)))
    case Columns.GroupedInts(parts) =>
      parts.map(batch => Partition(batch.rows))
    case Columns.GroupedStrings(parts) =>
      parts.map(batch => Partition(batch.rows))
    case Columns.GroupedLongs(parts) =>
      parts.map(batch => Partition(batch.rows))
