package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.IntJoinTriplePredicate
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
 * returns another value, is still a fallback, and that function may already have run. A null value
 * on a left, right, or full outer join is the same fallback: the value columns are already
 * encoded, and the kernel is not chosen. A null value on a long inner join is that fallback too.
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
      selectedPartition: Option[Int] = None,
      preserveAbsence: Boolean = false,
  ): Columns =
    val columns = pairsOf(interpret(source, child, selectedPartition, preserveAbsence)).map {
      batch =>
        ColumnBatches.takeColumn(batch, ordinal)
    }
    Columns.Ints(columns)

  private[execution] def batchesOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Ints(parts) => parts
    case Columns.Longs(parts) => parts
    case Columns.Floats(parts) => parts
    case Columns.Pairs(parts) => parts
    case Columns.LongPairs(parts) => parts
    case Columns.Triples(parts) => parts
    case Columns.LongTriples(parts) => parts
    case Columns.Strings(parts) => parts
    case Columns.Bools(parts) => parts
    case Columns.Outer(parts, _) => parts

  private[execution] enum Columns:
    case Ints(parts: Vector[ColumnBatch])
    case Longs(parts: Vector[ColumnBatch])
    case Floats(parts: Vector[ColumnBatch])
    case Pairs(parts: Vector[ColumnBatch])
    case LongPairs(parts: Vector[ColumnBatch])
    case Triples(parts: Vector[ColumnBatch])
    case LongTriples(parts: Vector[ColumnBatch])
    case Strings(parts: Vector[ColumnBatch])
    case Bools(parts: Vector[ColumnBatch])

    /**
     * Outer-join batches stay `(key, left, right)`. `absent` tells [[BatchCodec.decodeOuter]]
     * which value column is the `Option`. [[AbsentSide.Right]] is a left outer join.
     * [[AbsentSide.Left]] is a right outer join. [[AbsentSide.Both]] is a full outer join.
     */
    case Outer(parts: Vector[ColumnBatch], absent: AbsentSide)

  private def selectPartition[A](
      partitions: Seq[Partition[A]],
      selectedPartition: Option[Int],
  ): Seq[Partition[A]] = selectedPartition match
    case None => partitions
    case Some(index) => partitions.slice(index, index + 1)

  /**
   * Decode `columns`. Int pairs use [[BatchCodec.decodeIntPairsPreservingAbsence]] only for
   * [[PhysicalOp.SortByIntKey]]. Every other op uses [[BatchCodec.decodeIntPairs]], which rejects
   * a clear bit.
   */
  private[execution] def decodeFor(columns: Columns, op: PhysicalOp): Seq[Partition[_]] =
    decodeWith(columns, intPairDecoder(op))

  /**
   * Prefix of [[decodeFor]]. The same op chooses the int-pair decoder. `limit <= 0` returns empty
   * without reading a batch.
   */
  private[execution] def decodePrefixFor(columns: Columns, limit: Int, op: PhysicalOp): Seq[Any] =
    decodePrefixWith(columns, limit, intPairDecoder(op))

  private def intPairDecoder(op: PhysicalOp): (ColumnBatch, Int) => Seq[(Int, Int)] =
    op match
      case _: PhysicalOp.SortByIntKey => BatchCodec.decodeIntPairsPreservingAbsence
      case _ => (batch, rows) => BatchCodec.decodeIntPairs(batch, rows)

  private[execution] def decodePrefix(columns: Columns, limit: Int): Seq[Any] =
    decodePrefixWith(columns, limit, (batch, rows) => BatchCodec.decodeIntPairs(batch, rows))

  private def decodePrefixWith(
      columns: Columns,
      limit: Int,
      decodePairs: (ColumnBatch, Int) => Seq[(Int, Int)],
  ): Seq[Any] =
    if (limit <= 0) then Seq.empty
    else
      columns match
        case Columns.Ints(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeInts(batch, rows))
        case Columns.Longs(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeLongs(batch, rows))
        case Columns.Floats(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeFloat64s(batch, rows))
        case Columns.Pairs(parts) =>
          prefixRows(parts, limit, decodePairs)
        case Columns.LongPairs(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeLongPairs(batch, rows))
        case Columns.Triples(parts) =>
          // Joins are not partition-preserving, so `take` does not reach this arm.
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeIntTriples(batch, rows))
        case Columns.LongTriples(parts) =>
          // Joins are not partition-preserving, so `take` does not reach this arm.
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeLongTriples(batch, rows))
        case Columns.Outer(parts, absent) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeOuter(batch, absent, rows))
        case Columns.Strings(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeStrings(batch, rows))
        case Columns.Bools(parts) =>
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeBools(batch, rows))

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
  ): Columns = (plan, op) match
    case (Plan.Source(partitions, _), PhysicalOp.Scan(kind, _)) =>
      val selected = selectPartition(partitions, selectedPartition)
      kind match
        case PhysicalOp.Kind.Ints => Columns.Ints(encodeInts(selected))
        case PhysicalOp.Kind.Longs => Columns.Longs(encodeLongs(selected))
        case PhysicalOp.Kind.Floats => Columns.Floats(encodeFloats(selected))
        case PhysicalOp.Kind.Pairs =>
          // preserveAbsence keeps a null key or a null value in the pair columns. It is not the
          // probe side. Other pair plans still unbox.
          val encoded =
            if (preserveAbsence) then encodeGroupedPairs(selected) else encodePairs(selected)
          Columns.Pairs(encoded)
        case PhysicalOp.Kind.LongPairs => Columns.LongPairs(encodeLongPairs(selected))
        case PhysicalOp.Kind.Strings => Columns.Strings(encodeStrings(selected))
        case PhysicalOp.Kind.Bools => Columns.Bools(encodeBools(selected))
        case PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples =>
          throw new IllegalStateException(
            s"physical tree does not match ${plan.getClass.getSimpleName}",
          )
    case (Plan.FilterOp(source, predicate), PhysicalOp.Filter(child, slot)) =>
      applyFilter(interpret(source, child, selectedPartition, preserveAbsence), predicate, slot)
    case (Plan.MapOp(source, mapper), PhysicalOp.Project(child)) =>
      project(interpret(source, child, selectedPartition, preserveAbsence), mapper)
    case (logical, PhysicalOp.Pipeline(child, length)) =>
      interpretPipeline(logical, child, length, selectedPartition, preserveAbsence)
    case (Plan.FilterKeysOp(source, predicate), PhysicalOp.Filter(child, PhysicalOp.Slot.Key)) =>
      applyFilter(
        interpret(source, child, selectedPartition, preserveAbsence),
        predicate,
        PhysicalOp.Slot.Key,
      )
    case (
          Plan.FilterValuesOp(source, predicate),
          PhysicalOp.Filter(child, PhysicalOp.Slot.Value),
        ) =>
      applyFilter(
        interpret(source, child, selectedPartition, preserveAbsence),
        predicate,
        PhysicalOp.Slot.Value,
      )
    case (Plan.KeysOp(source), PhysicalOp.Column(child, 0)) =>
      pairProjection(
        source,
        child,
        ordinal = 0,
        selectedPartition = selectedPartition,
        preserveAbsence = preserveAbsence,
      )
    case (Plan.ValuesOp(source), PhysicalOp.Column(child, 1)) =>
      pairProjection(
        source,
        child,
        ordinal = 1,
        selectedPartition = selectedPartition,
        preserveAbsence = preserveAbsence,
      )
    case (Plan.MapValuesOp(source, mapper), PhysicalOp.MapValue(child)) =>
      // Named erasure boundary. The descriptor check verified the declared `Int => Int` shape
      // before lowering, so this cast is safe.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val fn = mapper.asInstanceOf[Int => Int]
      Columns.Pairs(
        onEachMorsel(pairsOf(interpret(source, child, selectedPartition, preserveAbsence))) {
          morsel =>
            ColumnKernel.projectInt32(
              morsel.batch,
              1,
              morsel.start,
              morsel.length,
              value => fn(value),
            )
        },
      )
    case (
          Plan.ReduceByKeyOp(source, reduceFunc),
          PhysicalOp.HashAggregate(PhysicalOp.Exchange(child)),
        ) =>
      // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
      // Named erasure boundary. Grouped pair reduction declares `(Int, Int) => Int`.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val op = reduceFunc.asInstanceOf[(Int, Int) => Int]
      Columns.Pairs(
        HashReduce.byKey(
          pairsOf(interpret(source, child, selectedPartition, preserveAbsence)),
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
          ColumnarPlanner.batchSize,
          (left, right) => op(left, right),
        ),
      )
    case (
          Plan.ReduceByKeyOp(source, reduceFunc),
          PhysicalOp.LongHashAggregate(PhysicalOp.Exchange(child)),
        ) =>
      // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
      // Named erasure boundary. Grouped long-pair reduction declares `(Long, Long) => Long`.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val op = reduceFunc.asInstanceOf[(Long, Long) => Long]
      Columns.LongPairs(
        LongHashReduce.byKey(
          longPairsOf(interpret(source, child, selectedPartition, preserveAbsence)),
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
          ColumnarPlanner.batchSize,
          (left, right) => op(left, right),
        ),
      )
    case (
          Plan.JoinOp(left, right, strategy),
          PhysicalOp.HashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
        ) if ColumnarPlanner.acceptsShuffleHash(strategy) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      Columns.Triples(
        ColumnPipeline.innerJoin(
          pairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence)),
          pairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence)),
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
          ColumnarPlanner.batchSize,
        ),
      )
    case (
          Plan.JoinOp(left, right, strategy),
          PhysicalOp.LongHashJoin(PhysicalOp.Exchange(leftOp), PhysicalOp.Exchange(rightOp)),
        ) if ColumnarPlanner.acceptsShuffleHash(strategy) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      val leftBatches = longPairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence))
      val rightBatches = longPairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence))
      // Validity words are already on the columns. A null value is not a decoded long, so the
      // kernel is not chosen.
      val valuesPresent =
        ColumnPipeline.longValuesFullyPresent(leftBatches) &&
          ColumnPipeline.longValuesFullyPresent(rightBatches)
      if (!valuesPresent) then
        throw new ClassCastException(
          "a null value on a long inner join stays on the row path",
        )
      Columns.LongTriples(
        ColumnPipeline.longInnerJoin(
          leftBatches,
          rightBatches,
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
          ColumnarPlanner.batchSize,
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
      Columns.Triples(
        ColumnPipeline.sortMergeJoin(
          pairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence)),
          pairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence)),
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
          ColumnarPlanner.batchSize,
        ),
      )
    case (
          Plan.JoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
          PhysicalOp.BroadcastJoin(leftOp, rightOp, buildSide),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      val leftBatches = pairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence))
      val rightBatches = pairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence))
      val (probe, build) = buildSide match
        case PhysicalOp.BuildSide.Right => (leftBatches, rightBatches)
        case PhysicalOp.BuildSide.Left => (rightBatches, leftBatches)
      val joined = ColumnPipeline.broadcastJoin(
        probe,
        build,
        ColumnarPlanner.parallelism,
        ScarletConf.get.broadcastBuildBudgetBytes,
      )
      // The kernel writes (key, probe value, build value). Building the left child makes that
      // (key, right, left), so the value columns are swapped back to (key, left, right).
      val ordered = buildSide match
        case PhysicalOp.BuildSide.Right => joined
        case PhysicalOp.BuildSide.Left => swapValueColumns(joined)
      Columns.Triples(ordered)
    case (
          Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
          PhysicalOp.BroadcastLeftOuter(leftOp, rightOp),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      // preserveAbsence keeps null keys. It is not the probe side. The right input is the build.
      val probe = pairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence = true))
      val build = pairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence = true))
      // Validity words are already on the columns. A null value is not a probe-side row with no
      // partner, and this path cannot represent one, so the kernel is not chosen.
      val valuesPresent =
        ColumnPipeline.valuesFullyPresent(probe) && ColumnPipeline.valuesFullyPresent(build)
      if (!valuesPresent) then
        throw new ClassCastException(
          "a null value on a left outer join stays on the row path",
        )
      // The kernel writes (key, probe value, build value). The probe side is the left input, so
      // the batch is already (key, left, right). A clear build-value bit is absence.
      val joined = ColumnPipeline.broadcastLeftOuterJoin(
        probe,
        build,
        ColumnarPlanner.parallelism,
        ScarletConf.get.broadcastBuildBudgetBytes,
      )
      Columns.Outer(joined, AbsentSide.Right)
    case (
          Plan.LeftOuterJoinOp(left, right, strategy),
          PhysicalOp.HashLeftOuter(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) if ColumnarPlanner.acceptsShuffleHash(strategy) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      // preserveAbsence keeps null keys. The left input is the probe side. Its exchange carries a
      // null key. The right input is the build side. Its exchange drops a null key.
      val probe = pairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence = true))
      val build = pairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence = true))
      // Validity words are already on the columns. A null value is not a probe-side row with no
      // partner, and this path cannot represent one, so the kernel is not chosen.
      val valuesPresent =
        ColumnPipeline.valuesFullyPresent(probe) && ColumnPipeline.valuesFullyPresent(build)
      if (!valuesPresent) then
        throw new ClassCastException(
          "a null value on a left outer join stays on the row path",
        )
      // The kernel writes (key, probe value, build value). The probe side is the left input, so
      // the batch is already (key, left, right). A clear build-value bit is absence.
      val joined = ColumnPipeline.hashLeftOuterJoin(
        probe,
        build,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
        ScarletConf.get.broadcastBuildBudgetBytes,
      )
      Columns.Outer(joined, AbsentSide.Right)
    case (
          Plan.RightOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
          PhysicalOp.BroadcastRightOuter(leftOp, rightOp),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      // preserveAbsence keeps null keys. The left input is the build. The right input is the
      // probe, so unmatched right rows are kept.
      val build = pairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence = true))
      val probe = pairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence = true))
      rightOuter(probe, build) { (probeBatches, buildBatches) =>
        ColumnPipeline.broadcastLeftOuterJoin(
          probeBatches,
          buildBatches,
          ColumnarPlanner.parallelism,
          ScarletConf.get.broadcastBuildBudgetBytes,
        )
      }
    case (
          Plan.RightOuterJoinOp(left, right, strategy),
          PhysicalOp.HashRightOuter(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) if ColumnarPlanner.acceptsShuffleHash(strategy) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      // preserveAbsence keeps null keys. The right input is the probe side. Its exchange carries
      // a null key. The left input is the build side. Its exchange drops a null key.
      val build = pairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence = true))
      val probe = pairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence = true))
      rightOuter(probe, build) { (probeBatches, buildBatches) =>
        ColumnPipeline.hashLeftOuterJoin(
          probeBatches,
          buildBatches,
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
          ColumnarPlanner.batchSize,
          ScarletConf.get.broadcastBuildBudgetBytes,
        )
      }
    case (
          Plan.FullOuterJoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
          PhysicalOp.BroadcastFullOuter(leftOp, rightOp),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      /* preserveAbsence keeps null keys. The left input is the probe. The right input is the
       * build. */
      val probe = pairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence = true))
      val build = pairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence = true))
      fullOuter(probe, build) { (probeBatches, buildBatches) =>
        ColumnPipeline.broadcastFullOuterJoin(
          probeBatches,
          buildBatches,
          ColumnarPlanner.parallelism,
          ScarletConf.get.broadcastBuildBudgetBytes,
        )
      }
    case (
          Plan.FullOuterJoinOp(left, right, strategy),
          PhysicalOp.HashFullOuter(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) if ColumnarPlanner.acceptsShuffleHash(strategy) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      // preserveAbsence keeps null keys. Both exchanges carry a null key. The right input is the
      // build side. A null build key is carried outside the table.
      val probe = pairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence = true))
      val build = pairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence = true))
      fullOuter(probe, build) { (probeBatches, buildBatches) =>
        ColumnPipeline.hashFullOuterJoin(
          probeBatches,
          buildBatches,
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
          ColumnarPlanner.batchSize,
          ScarletConf.get.broadcastBuildBudgetBytes,
        )
      }
    case (
          Plan.LeftOuterJoinOp(left, right, Some(Plan.JoinStrategy.SortMerge)),
          PhysicalOp.SortMergeLeftOuter(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
          ),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      // preserveAbsence keeps null keys. The left exchange carries a null key. The right exchange
      // drops one. The kernel publishes (key, left, right).
      val probe = pairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence = true))
      val build = pairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence = true))
      val valuesPresent =
        ColumnPipeline.valuesFullyPresent(probe) && ColumnPipeline.valuesFullyPresent(build)
      if (!valuesPresent) then
        throw new ClassCastException(
          "a null value on a left outer join stays on the row path",
        )
      Columns.Outer(
        ColumnPipeline.sortMergeLeftOuterJoin(
          probe,
          build,
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
          ColumnarPlanner.batchSize,
        ),
        AbsentSide.Right,
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
      // preserveAbsence keeps null keys. The kernel swaps the value columns back. Do not swap
      // again here.
      val leftBatches = pairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence = true))
      val rightBatches =
        pairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence = true))
      val valuesPresent =
        ColumnPipeline.valuesFullyPresent(leftBatches) &&
          ColumnPipeline.valuesFullyPresent(rightBatches)
      if (!valuesPresent) then
        throw new ClassCastException(
          "a null value on a right outer join stays on the row path",
        )
      Columns.Outer(
        ColumnPipeline.sortMergeRightOuterJoin(
          leftBatches,
          rightBatches,
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
          ColumnarPlanner.batchSize,
        ),
        AbsentSide.Left,
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
      // preserveAbsence keeps null keys. Both exchanges carry a null key.
      val probe = pairsOf(interpret(left, leftOp, selectedPartition, preserveAbsence = true))
      val build = pairsOf(interpret(right, rightOp, selectedPartition, preserveAbsence = true))
      fullOuter(probe, build) { (probeBatches, buildBatches) =>
        ColumnPipeline.sortMergeFullOuterJoin(
          probeBatches,
          buildBatches,
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
          ColumnarPlanner.batchSize,
        )
      }
    case (Plan.SortByIntKeyOp(source), PhysicalOp.SortByIntKey(child)) =>
      // Boundary guard: take runs the whole sort, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a sort")
      // preserveAbsence keeps a null key and a null value. A null key still orders as zero.
      // The physical op stays SortByIntKey. Run and take both reach this arm. RankSplitRuns
      // chooses the in-memory split or spilled runs. Take decodes only the prefix of the result.
      val batches =
        pairsOf(interpret(source, child, selectedPartition = None, preserveAbsence = true))
      val sorted = RankSplitRuns.sortAndSplit(
        batches,
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ScarletConf.get,
      )
      Columns.Pairs(sorted)
    case _ =>
      throw new IllegalStateException(
        s"physical tree does not match ${plan.getClass.getSimpleName}",
      )

  /**
   * Run the left-outer kernel with `probe` as the right input and `build` as the left input, then
   * restore `(key, left, right)`. The kernel writes `(key, probe value, build value)`, so the
   * value columns arrive as `(right, left)`. The cleared build-value bit moves with the left
   * column. That bit is absence. A null value is not absence, and this path cannot represent one.
   */
  private def rightOuter(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
  )(join: (Vector[ColumnBatch], Vector[ColumnBatch]) => Vector[ColumnBatch]): Columns =
    val valuesPresent =
      ColumnPipeline.valuesFullyPresent(probe) && ColumnPipeline.valuesFullyPresent(build)
    if (!valuesPresent) then
      throw new ClassCastException(
        "a null value on a right outer join stays on the row path",
      )
    Columns.Outer(swapValueColumns(join(probe, build)), AbsentSide.Left)

  /**
   * Run a full outer join. The kernel writes `(key, probe value, build value)`. The probe is the
   * left input, so the batch is already `(key, left, right)`. A clear bit on either value column
   * is absence. A null value is not absence, and this path cannot represent one.
   */
  private def fullOuter(
      probe: Vector[ColumnBatch],
      build: Vector[ColumnBatch],
  )(join: (Vector[ColumnBatch], Vector[ColumnBatch]) => Vector[ColumnBatch]): Columns =
    val valuesPresent =
      ColumnPipeline.valuesFullyPresent(probe) && ColumnPipeline.valuesFullyPresent(build)
    if (!valuesPresent) then
      throw new ClassCastException(
        "a null value on a full outer join stays on the row path",
      )
    Columns.Outer(join(probe, build), AbsentSide.Both)

  private def interpretPipeline(
      plan: Plan[_],
      child: PhysicalOp,
      length: Int,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
  ): Columns =
    val (source, steps) = ColumnarPlanner.elementSteps(plan, length)
    if (steps.length < length) then
      // Lowering uses this same collector, so this only guards a malformed physical tree.
      throw new IllegalStateException("pipeline does not match logical element steps")
    pipeline(interpret(source, child, selectedPartition, preserveAbsence), steps)

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
      case Columns.Bools(_) | Columns.Pairs(_) | Columns.LongPairs(_) | Columns.Triples(_) |
          Columns.LongTriples(_) | Columns.Outer(_, _) =>
        rejected

  private def triplePredicate(
      slot: PhysicalOp.Slot,
      keep: IntJoinTriplePredicate[?],
  ): PairKernel.IntTriplePredicate = slot match
    case PhysicalOp.Slot.TripleKey => (key, _, _) => keep.keep(key)
    case PhysicalOp.Slot.TripleLeftValue => (_, left, _) => keep.keep(left)
    case PhysicalOp.Slot.TripleRightValue => (_, _, right) => keep.keep(right)
    case PhysicalOp.Slot.Element | PhysicalOp.Slot.Row | PhysicalOp.Slot.Key |
        PhysicalOp.Slot.Value =>
      throw new IllegalStateException("triple filter has a non-triple slot")

  private def applyFilter(columns: Columns, predicate: Any, slot: PhysicalOp.Slot): Columns =
    slot match
      case PhysicalOp.Slot.Element =>
        columns match
          case Columns.Ints(parts) =>
            val keep = predicate.asInstanceOf[Int => Boolean]
            Columns.Ints(onEachMorsel(parts) { morsel =>
              ColumnKernel.filterInt32(
                morsel.batch,
                0,
                morsel.start,
                morsel.length,
                value => keep(value),
              )
            })
          case Columns.Longs(parts) =>
            val keep = predicate.asInstanceOf[Long => Boolean]
            Columns.Longs(onEachMorsel(parts) { morsel =>
              ColumnKernel.filterInt64(
                morsel.batch,
                0,
                morsel.start,
                morsel.length,
                value => keep(value),
              )
            })
          case Columns.Floats(parts) =>
            val keep = predicate.asInstanceOf[Double => Boolean]
            Columns.Floats(onEachMorsel(parts) { morsel =>
              ColumnKernel.filterFloat64(
                morsel.batch,
                0,
                morsel.start,
                morsel.length,
                value => keep(value),
              )
            })
          case Columns.Strings(parts) =>
            ColumnarPlanner.dictionaryStringLiterals(predicate) match
              case Some(literals) =>
                Columns.Strings(onEachMorsel(parts) { morsel =>
                  ColumnKernel.filterUtf8ByValues(
                    morsel.batch,
                    0,
                    morsel.start,
                    morsel.length,
                    literals,
                  )
                })
              case None =>
                val keep = predicate.asInstanceOf[String => Boolean]
                Columns.Strings(onEachMorsel(parts) { morsel =>
                  ColumnKernel.filterUtf8(
                    morsel.batch,
                    0,
                    morsel.start,
                    morsel.length,
                    value => keep(value),
                  )
                })
          case Columns.Bools(parts) =>
            Columns.Bools(onEachMorsel(parts) { morsel =>
              BoolPipeline.filter(morsel.batch, morsel.start, morsel.length, predicate)
            })
          case Columns.Pairs(_) | Columns.LongPairs(_) | Columns.Triples(_) |
              Columns.LongTriples(_) | Columns.Outer(_, _) =>
            throw new IllegalStateException(
              s"columnar element filter expects ${elementColumnList(includeBooleans = true)}",
            )
      case PhysicalOp.Slot.Row =>
        val keep = predicate.asInstanceOf[((Int, Int)) => Boolean]
        // `keep` is `(Int, Int) => Boolean`, so the pair is boxed for the call.
        Columns.Pairs(onEachMorsel(pairsOf(columns)) { morsel =>
          PairKernel.filter(
            morsel.batch,
            morsel.start,
            morsel.length,
            (key, value) => keep((key, value)),
          )
        })
      case PhysicalOp.Slot.Key =>
        val keep = predicate.asInstanceOf[Int => Boolean]
        Columns.Pairs(onEachMorsel(pairsOf(columns)) { morsel =>
          ColumnKernel.filterInt32(
            morsel.batch,
            0,
            morsel.start,
            morsel.length,
            value => keep(value),
          )
        })
      case PhysicalOp.Slot.Value =>
        val keep = predicate.asInstanceOf[Int => Boolean]
        Columns.Pairs(onEachMorsel(pairsOf(columns)) { morsel =>
          ColumnKernel.filterInt32(
            morsel.batch,
            1,
            morsel.start,
            morsel.length,
            value => keep(value),
          )
        })
      case PhysicalOp.Slot.TripleKey | PhysicalOp.Slot.TripleLeftValue |
          PhysicalOp.Slot.TripleRightValue =>
        val keep = predicate match
          case recognized: IntJoinTriplePredicate[?] => recognized
          case _ => throw new IllegalStateException("triple filter lost its slot predicate")
        val select = triplePredicate(slot, keep)
        Columns.Triples(onEachMorsel(triplesOf(columns)) { morsel =>
          PairKernel.filterTriple(morsel.batch, morsel.start, morsel.length, select)
        })

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
      // turns that into `ClassCastException`, which is the row-path fallback.
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
    case Columns.Bools(_) | Columns.Pairs(_) | Columns.LongPairs(_) | Columns.Triples(_) |
        Columns.LongTriples(_) | Columns.Outer(_, _) =>
      throw new IllegalStateException(
        s"columnar map expects ${elementColumnList(includeBooleans = false)}",
      )

  /**
   * Kernel probe output is `(key, probe value, build value)`. Exchange the two value columns so a
   * left-side build publishes `(key, left, right)`.
   */
  private def swapValueColumns(batches: Vector[ColumnBatch]): Vector[ColumnBatch] =
    batches.map { batch =>
      batch.columns match
        case Seq(key, probeValue, buildValue) =>
          new ColumnBatch(
            Vector(key.logicalType, buildValue.logicalType, probeValue.logicalType),
            Vector(key, buildValue, probeValue),
            batch.length,
          )
        case other =>
          throw new IllegalArgumentException(
            s"join output has ${other.size} columns",
          )
    }

  private[execution] def pairsOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Pairs(parts) if parts.nonEmpty => parts
    case Columns.Pairs(_) =>
      throw new IllegalStateException("columnar pairs have no partitions")
    case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.LongPairs(_) |
        Columns.Triples(_) | Columns.LongTriples(_) | Columns.Strings(_) | Columns.Bools(_) |
        Columns.Outer(_, _) =>
      throw new IllegalStateException("columnar op expected int pairs")

  private def triplesOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Triples(parts) if parts.nonEmpty => parts
    case Columns.Triples(_) =>
      throw new IllegalStateException("columnar triples have no partitions")
    case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.Pairs(_) |
        Columns.LongPairs(_) | Columns.LongTriples(_) | Columns.Strings(_) | Columns.Bools(_) |
        Columns.Outer(_, _) =>
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
  private def onEachMorsel(
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

  private[execution] def encodeFloats(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.DoubleArrays

    partitions.map { partition =>
      partition.data match
        case owned: DoubleArrays => BatchCodec.copiedFloat64s(owned.values)
        case other =>
          // Named erasure boundary. Classification already selected double elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Double]]
          BatchCodec.float64s(rows)
    }.toVector

  private[execution] def encodeLongs(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.LongArrays

    partitions.map { partition =>
      partition.data match
        case owned: LongArrays => BatchCodec.copiedLongs(owned.values)
        case other =>
          // Named erasure boundary. Classification already selected long elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Long]]
          BatchCodec.longs(rows)
    }.toVector

  private[execution] def encodeBools(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.BoolArrays

    partitions.map { partition =>
      partition.data match
        case owned: BoolArrays => BatchCodec.copiedBools(owned.values)
        case other =>
          // Named erasure boundary. Classification already selected boolean elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Boolean]]
          BatchCodec.bools(rows)
    }.toVector

  private[execution] def encodeInts(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.IntArrays

    partitions.map { partition =>
      partition.data match
        case owned: IntArrays => BatchCodec.copiedInts(owned.values)
        case other =>
          // Named erasure boundary. Classification already selected int elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[Int]]
          BatchCodec.ints(rows)
    }.toVector

  private def encodePairs(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    encodePairPartitions(partitions, rows => BatchCodec.intPairs(rows))

  private def encodeLongPairs(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.LongPairArrays

    partitions.map { partition =>
      partition.data match
        case pairs: LongPairArrays => BatchCodec.copiedLongPairs(pairs.keys, pairs.values)
        case other =>
          // Named erasure boundary. Classification already selected `(Long, Long)` elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[(Long, Long)]]
          BatchCodec.longPairs(rows)
    }.toVector

  private def longPairsOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.LongPairs(parts) if parts.nonEmpty => parts
    case Columns.LongPairs(_) =>
      throw new IllegalStateException("columnar long pairs have no partitions")
    case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.Pairs(_) |
        Columns.Triples(_) | Columns.LongTriples(_) | Columns.Strings(_) | Columns.Bools(_) |
        Columns.Outer(_, _) =>
      throw new IllegalStateException("columnar op expected long pairs")

  /**
   * Pair encode for grouped sum. [[BatchCodec.intPairsPreservingAbsence]] keeps a null component
   * absent and rejects any other non-int cell. An owned pair array has no nulls and is copied like
   * [[encodePairs]].
   */
  private[execution] def encodeGroupedPairs(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    encodePairPartitions(partitions, rows => BatchCodec.intPairsPreservingAbsence(rows))

  private def encodePairPartitions(
      partitions: Seq[Partition[_]],
      encodeBoxed: Iterable[(Int, Int)] => ColumnBatch,
  ): Vector[ColumnBatch] =
    import com.ewoodbury.scarlet.core.IntPairArrays

    partitions.map { partition =>
      partition.data match
        case pairs: IntPairArrays => BatchCodec.copiedIntPairs(pairs.keys, pairs.values)
        case other =>
          // Named erasure boundary. Classification already selected `(Int, Int)` elements.
          @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
          val rows = other.asInstanceOf[Iterable[(Int, Int)]]
          encodeBoxed(rows)
    }.toVector

  private def encodeStrings(partitions: Seq[Partition[_]]): Vector[ColumnBatch] =
    partitions.map { partition =>
      // Named erasure boundary. Classification already selected string elements.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val rows = partition.data.asInstanceOf[Iterable[String]]
      BatchCodec.strings(rows)
    }.toVector

  private def decodeWith(
      columns: Columns,
      decodePairs: (ColumnBatch, Int) => Seq[(Int, Int)],
  ): Seq[Partition[_]] = columns match
    case Columns.Ints(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeInts(batch)))
    case Columns.Longs(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeLongs(batch)))
    case Columns.Floats(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeFloat64s(batch)))
    case Columns.Pairs(parts) =>
      parts.map(batch => Partition(decodePairs(batch, batch.length)))
    case Columns.LongPairs(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeLongPairs(batch)))
    case Columns.Triples(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeIntTriples(batch)))
    case Columns.LongTriples(parts) =>
      // Named erasure boundary for the long join. The decoded rows are `(Long, (Long, Long))`.
      // [[ColumnarPlanner.runLowered]] casts the partition sequence to the plan's element type.
      parts.map(batch => Partition(BatchCodec.decodeLongTriples(batch)))
    case Columns.Outer(parts, absent) =>
      parts.map(batch => Partition(BatchCodec.decodeOuter(batch, absent)))
    case Columns.Strings(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeStrings(batch)))
    case Columns.Bools(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeBools(batch)))
