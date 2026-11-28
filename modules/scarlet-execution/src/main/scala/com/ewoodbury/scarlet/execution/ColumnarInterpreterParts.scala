package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan}

import ColumnarInterpreter.Columns

/** Decode and pair projection for [[ColumnarInterpreter]]. */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw",
  ),
)
private[execution] trait ColumnarInterpreterParts:
  protected def pairProjection(
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
      val projected = ColumnarInterpreter.interpretAt(
        source,
        child,
        selectedPartition,
        preserveAbsence,
        ordinals,
      )
      val columns = projectColumn(pairBatches(child, projected), ordinal)
      StringPairWalk.wrapColumn(child.kind, columns, ordinal)

  /** The pair batches `child` produced. A wrong column type is a planner bug, not a fallback. */
  protected def pairBatches(child: PhysicalOp, columns: Columns): Vector[ColumnBatch] =
    ExecutionRoute.pairArm(child) match
      case Some(ExecutionRoute.PairArm.LongPairs) => ColumnarInterpreter.longPairsOf(columns)
      case Some(ExecutionRoute.PairArm.IntPairs) => ColumnarInterpreter.pairsOf(columns)
      case Some(ExecutionRoute.PairArm.StringPairs) => StringPairWalk.batches(columns)
      case None =>
        throw new IllegalStateException("columnar projection expected a pair")

  protected def projectColumn(parts: Vector[ColumnBatch], ordinal: Int): Vector[ColumnBatch] =
    parts.map { batch =>
      // A singleton encode stores the demanded ordinal at index 0.
      ColumnBatches.takeColumn(batch, encodedIndex(batch, ordinal))
    }

  protected def mapIntValues(columns: Columns, mapper: Any): Columns =
    // Named erasure boundary. Lowering checked the declared `Int => Int` shape, so this cast
    // is safe.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val fn = mapper.asInstanceOf[Int => Int]
    Columns.Pairs(
      ColumnarInterpreter.onEachMorsel(ColumnarInterpreter.pairsOf(columns)) { morsel =>
        ColumnKernel.projectInt32(
          morsel.batch,
          1,
          morsel.start,
          morsel.length,
          value => fn(value),
        )
      },
    )

  protected def mapLongValues(columns: Columns, mapper: Any): Columns =
    // Named erasure boundary. Lowering checked `Long => Long` via FunctionWitness, so this cast
    // is safe. A null value stays null and is not passed to the mapper. A null key is left as it
    // is.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val fn = mapper.asInstanceOf[Long => Long]
    Columns.LongPairs(
      ColumnarInterpreter.onEachMorsel(ColumnarInterpreter.longPairsOf(columns)) { morsel =>
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
  protected def encodedIndex(batch: ColumnBatch, ordinal: Int): Int =
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

  protected def selectPartition[A](
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
    decodeFor(ColumnarInterpreter.interpret(plan, op), op, plan)

  protected def decodeRouted(
      columns: Columns,
      op: PhysicalOp,
      plan: Option[Plan[_]],
  ): Seq[Partition[_]] =
    if (ExecutionRoute.decodesAsIntLongSum(plan, op)) then intLongSums(columns)
    else if (IntLongMapRun.decodes(op)) then IntLongMapRun.partitions(columns)
    else if (IntLongJoinColumns.decodes(op)) then IntLongJoinColumns.partitions(columns)
    else if (op.kind eq PhysicalOp.Kind.Float32s) then Float32Decode.partitions(columns, op)
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
  protected def intLongSums(columns: Columns): Seq[Partition[_]] = columns match
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
    else if (IntLongJoinColumns.decodes(op)) then IntLongJoinColumns.prefix(columns, limit)
    else if (op.kind eq PhysicalOp.Kind.Float32s) then Float32Decode.prefix(columns, limit, op)
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

  protected def decodePrefixWith(
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

  protected def prefixRows[A](
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

  /** Hash join of two exchanges. A selector never reaches this arm: take refuses a join. */
  protected def hashJoinTriples(
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

  protected def interpretPipeline(
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
    pipeline(
      ColumnarInterpreter.interpretAt(source, child, selectedPartition, preserveAbsence, ordinals),
      steps,
    )

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

  protected def project(columns: Columns, mapper: Any): Columns = columns match
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
    case Columns.Floats(parts) => FloatProject.run(parts, mapper)(onEachMorsel)
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
