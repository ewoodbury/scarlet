package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.IntJoinTriplePredicate
import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Partition, Plan}

/**
 * Runs a lowered [[PhysicalOp]] tree and encodes source partitions into columns.
 *
 * Named erasure boundary, shared with [[ColumnarPlanner]]. [[ColumnarPlanner.lower]] chooses the
 * tree and does not call user functions. [[interpret]] walks that same logical plan and applies
 * the operator recorded on each physical node. A mismatch throws `IllegalStateException`, which
 * [[ColumnarPlanner.tryRun]] does not turn into a row-path fallback. A partition selector on an
 * aggregate or a join fails a `require` guard with `IllegalArgumentException`, and that is not a
 * fallback either. A `ClassCastException` from a filter, or from a string map that declares
 * `String` and returns another value, is still a fallback, and that function may already have run.
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
  ): Columns =
    val columns = pairsOf(interpret(source, child, selectedPartition)).map { batch =>
      ColumnBatches.takeColumn(batch, ordinal)
    }
    Columns.Ints(columns)

  private[execution] def batchesOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Ints(parts) => parts
    case Columns.Longs(parts) => parts
    case Columns.Floats(parts) => parts
    case Columns.Pairs(parts) => parts
    case Columns.Triples(parts) => parts
    case Columns.Strings(parts) => parts
    case Columns.Bools(parts) => parts

  private[execution] enum Columns:
    case Ints(parts: Vector[ColumnBatch])
    case Longs(parts: Vector[ColumnBatch])
    case Floats(parts: Vector[ColumnBatch])
    case Pairs(parts: Vector[ColumnBatch])
    case Triples(parts: Vector[ColumnBatch])
    case Strings(parts: Vector[ColumnBatch])
    case Bools(parts: Vector[ColumnBatch])

  private def selectPartition[A](
      partitions: Seq[Partition[A]],
      selectedPartition: Option[Int],
  ): Seq[Partition[A]] = selectedPartition match
    case None => partitions
    case Some(index) => partitions.slice(index, index + 1)

  private[execution] def decodePrefix(columns: Columns, limit: Int): Seq[Any] =
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
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeIntPairs(batch, rows))
        case Columns.Triples(parts) =>
          // Joins are not partition-preserving, so `take` does not reach this arm.
          prefixRows(parts, limit, (batch, rows) => BatchCodec.decodeIntTriples(batch, rows))
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
  ): Columns = (plan, op) match
    case (Plan.Source(partitions, _), PhysicalOp.Scan(kind, _)) =>
      val selected = selectPartition(partitions, selectedPartition)
      kind match
        case PhysicalOp.Kind.Ints => Columns.Ints(encodeInts(selected))
        case PhysicalOp.Kind.Longs => Columns.Longs(encodeLongs(selected))
        case PhysicalOp.Kind.Floats => Columns.Floats(encodeFloats(selected))
        case PhysicalOp.Kind.Pairs => Columns.Pairs(encodePairs(selected))
        case PhysicalOp.Kind.Strings => Columns.Strings(encodeStrings(selected))
        case PhysicalOp.Kind.Bools => Columns.Bools(encodeBools(selected))
        case PhysicalOp.Kind.Triples =>
          throw new IllegalStateException(
            s"physical tree does not match ${plan.getClass.getSimpleName}",
          )
    case (Plan.FilterOp(source, predicate), PhysicalOp.Filter(child, slot)) =>
      applyFilter(interpret(source, child, selectedPartition), predicate, slot)
    case (Plan.MapOp(source, mapper), PhysicalOp.Project(child)) =>
      project(interpret(source, child, selectedPartition), mapper)
    case (logical, PhysicalOp.Pipeline(child, length)) =>
      interpretPipeline(logical, child, length, selectedPartition)
    case (Plan.FilterKeysOp(source, predicate), PhysicalOp.Filter(child, PhysicalOp.Slot.Key)) =>
      applyFilter(interpret(source, child, selectedPartition), predicate, PhysicalOp.Slot.Key)
    case (
          Plan.FilterValuesOp(source, predicate),
          PhysicalOp.Filter(child, PhysicalOp.Slot.Value),
        ) =>
      applyFilter(interpret(source, child, selectedPartition), predicate, PhysicalOp.Slot.Value)
    case (Plan.KeysOp(source), PhysicalOp.Column(child, 0)) =>
      pairProjection(source, child, ordinal = 0, selectedPartition = selectedPartition)
    case (Plan.ValuesOp(source), PhysicalOp.Column(child, 1)) =>
      pairProjection(source, child, ordinal = 1, selectedPartition = selectedPartition)
    case (Plan.MapValuesOp(source, mapper), PhysicalOp.MapValue(child)) =>
      // Named erasure boundary. The descriptor check verified the declared `Int => Int` shape
      // before lowering, so this cast is safe.
      @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
      val fn = mapper.asInstanceOf[Int => Int]
      Columns.Pairs(onEachMorsel(pairsOf(interpret(source, child, selectedPartition))) { morsel =>
        ColumnKernel.projectInt32(morsel.batch, 1, morsel.start, morsel.length, value => fn(value))
      })
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
          pairsOf(interpret(source, child, selectedPartition)),
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
          pairsOf(interpret(left, leftOp, selectedPartition)),
          pairsOf(interpret(right, rightOp, selectedPartition)),
          ColumnarPlanner.shuffleWidth,
          ColumnarPlanner.parallelism,
          ColumnarPlanner.batchSize,
        ),
      )
    case (
          Plan.JoinOp(left, right, Some(Plan.JoinStrategy.Broadcast)),
          PhysicalOp.BroadcastJoin(probe, build),
        ) =>
      // Boundary guard: take refuses a join, so a selector never reaches this arm.
      require(selectedPartition.isEmpty, "a partition selector cannot run a join")
      Columns.Triples(
        ColumnPipeline.broadcastJoin(
          pairsOf(interpret(left, probe, selectedPartition)),
          pairsOf(interpret(right, build, selectedPartition)),
          ColumnarPlanner.parallelism,
        ),
      )
    case _ =>
      throw new IllegalStateException(
        s"physical tree does not match ${plan.getClass.getSimpleName}",
      )

  private def interpretPipeline(
      plan: Plan[_],
      child: PhysicalOp,
      length: Int,
      selectedPartition: Option[Int],
  ): Columns =
    val (source, steps) = ColumnarPlanner.elementSteps(plan, length)
    if (steps.length < length) then
      // Lowering uses this same collector, so this only guards a malformed physical tree.
      throw new IllegalStateException("pipeline does not match logical element steps")
    pipeline(interpret(source, child, selectedPartition), steps)

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
      case Columns.Bools(_) | Columns.Pairs(_) | Columns.Triples(_) => rejected

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
          case Columns.Pairs(_) | Columns.Triples(_) =>
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
    case Columns.Bools(_) | Columns.Pairs(_) | Columns.Triples(_) =>
      throw new IllegalStateException(
        s"columnar map expects ${elementColumnList(includeBooleans = false)}",
      )

  private[execution] def pairsOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Pairs(parts) if parts.nonEmpty => parts
    case Columns.Pairs(_) =>
      throw new IllegalStateException("columnar pairs have no partitions")
    case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.Triples(_) |
        Columns.Strings(_) | Columns.Bools(_) =>
      throw new IllegalStateException("columnar op expected int pairs")

  private def triplesOf(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Triples(parts) if parts.nonEmpty => parts
    case Columns.Triples(_) =>
      throw new IllegalStateException("columnar triples have no partitions")
    case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.Pairs(_) |
        Columns.Strings(_) | Columns.Bools(_) =>
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

  private[execution] def decode(columns: Columns): Seq[Partition[_]] = columns match
    case Columns.Ints(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeInts(batch)))
    case Columns.Longs(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeLongs(batch)))
    case Columns.Floats(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeFloat64s(batch)))
    case Columns.Pairs(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeIntPairs(batch)))
    case Columns.Triples(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeIntTriples(batch)))
    case Columns.Strings(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeStrings(batch)))
    case Columns.Bools(parts) =>
      parts.map(batch => Partition(BatchCodec.decodeBools(batch)))
