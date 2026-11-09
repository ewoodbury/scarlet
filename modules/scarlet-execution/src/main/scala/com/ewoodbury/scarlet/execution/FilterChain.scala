package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns
import PredicatePlan.{Atom, Compiled}

/**
 * Peels a stack of consecutive unfused filters.
 *
 * [[PredicatePlan.compile]] casts every predicate before a kernel runs, and stops at a map. The
 * child is interpreted first, so a later mistyped predicate still does not run the earlier ones.
 * [[run]] chains [[PredicatePlan.applyAtom]], one kernel per filter. A pair or triple stack of two
 * or more slot atoms is the exception: it compacts through [[PredicatePlan.selectionSteps]] in one
 * pass. An int, long, float, string, or boolean stack always chains, including a lone filter. The
 * survivor index is four bytes per row, which costs more than the intermediate column a chained
 * kernel writes for the next filter to read. A boolean column is one bit per row. [[run]] still
 * chains a long-pair row filter, one [[LongPairKernel]] per filter. [[selected]] keeps that
 * filter's survivor index, including a stack of one. A whole-row triple filter chains too: it is
 * not a slot atom, so it does not compact through [[Selection]]. A string key or value stays in
 * [[StringPairWalk]]. A whole-row string stack is one selection pass there. Long triples and outer
 * batches still refuse. A map or a pipeline is not a filter and stops the peel.
 *
 * [[PredicatePlan.applyAtom]] does not take the compiled stack. Encoders, including the
 * absence-preserving copy under an outer join, do not run the predicate.
 *
 * [[selected]] keeps the survivor index for an int-pair, long-pair, int-triple, or string-pair
 * stack, including a stack of one. A long-pair row filter and a whole-row string filter are in
 * that index. [[compact]] still serves [[run]].
 */
@SuppressWarnings(Array("org.wartremover.warts.Throw"))
private[execution] object FilterChain:

  def accepts(plan: Plan[_], op: PhysicalOp): Boolean =
    (op.kind ne PhysicalOp.Kind.StringPairs) && PredicatePlan.accepts(plan, op)

  def run(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    val (source, child) = PredicatePlan.childOf(plan, op)
    val columns = ColumnarInterpreter.interpretAt(
      source,
      child,
      selectedPartition,
      preserveAbsence,
      ordinals,
    )
    columns match
      case Columns.LongCounts(_) | Columns.LongTriples(_) | Columns.StringTriples(_) |
          Columns.GroupedInts(_) | Columns.GroupedStrings(_) | Columns.GroupedLongs(_) =>
        PredicatePlan.unsupported(opening(plan, op))
      // An outer batch is a triple with an absent side. This must stay an IllegalStateException:
      // the planner catches ClassCastException, including ColumnarFallback, and falls back to
      // rows, where the predicate would unbox the absent side and throw a null pointer.
      case Columns.Outer(_, _) =>
        throw new IllegalStateException(PredicatePlan.outerMessage)
      case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.Strings(_) |
          Columns.Bools(_) | Columns.Pairs(_) | Columns.LongPairs(_) | Columns.Triples(_) |
          Columns.StringPairs(_) =>
        val compiled = PredicatePlan.compile(plan, op).getOrElse(missing)
        applyCompiled(presentSlotStack(columns, compiled), compiled, ordinals)

  /**
   * Survivor ids for an int-pair, long-pair, int-triple, or string-pair stack, including one
   * filter. A long-pair row filter and a whole-row string filter are defined, including a stack of
   * one. Every other kind returns `None` and is not interpreted. A `Some` interprets the source
   * under the stack once. Each batch gets one [[Selected]]: morsels append absolute ids into
   * `rows`, in morsel order, and nothing is concatenated or compacted. Long pairs use the int rule
   * and do not rewrite ordinals: a long-pair root already carries both columns. A whole-row step
   * reads both columns and is not moved to ordinal 0.
   */
  def selected(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Option[Vector[Selected]] =
    op.kind match
      case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.Triples |
          PhysicalOp.Kind.StringPairs =>
        PredicatePlan.compile(plan, op) match
          case Some(compiled) if PredicatePlan.selectionSteps(compiled).nonEmpty =>
            val steps =
              PredicatePlan
                .selectionSteps(PredicatePlan.encoded(compiled, ordinals))
                .getOrElse(
                  missing,
                )
            val (source, child) = PredicatePlan.childOf(plan, op)
            val columns = ColumnarInterpreter.interpretAt(
              source,
              child,
              selectedPartition,
              preserveAbsence,
              ordinals,
            )
            // An outer batch is a triple with an absent side. A selection cannot represent that
            // absence. Return `None`; the dense path re-runs the source and throws the same
            // `IllegalStateException` it always did. Matching the interpreted child catches an
            // outer join under any wrapper, not just at the top of the stack.
            columns match
              case Columns.Outer(_, _) => None
              case _ =>
                val batches = kindBatches(op.kind, columns)
                // A pure slot stack rejects a clear bit. Publish the stored 0 before that kernel.
                // A whole-row step is not in `steps`, so this does not run for one.
                val ready =
                  if (steps.exists(tripleSlot)) then batches.map(ColumnBatches.presentStoredZeros)
                  else batches
                Some(selectBatches(ready, steps))
          case _ => None
      case _ => None

  /** One kernel per filter. The intermediate column is the survivor set the next kernel reads. */
  private def applyCompiled(columns: Columns, compiled: Compiled, ordinals: Set[Int]): Columns =
    columns match
      case Columns.Pairs(_) if compiled.atoms.length > 1 =>
        Columns.Pairs(compact(ColumnarInterpreter.pairsOf(columns), compiled, ordinals))
      case Columns.Triples(_) if compiled.atoms.length > 1 =>
        // A whole-row atom is not a slot atom. Two of them chain. Slot atoms still compact once.
        PredicatePlan.selectionSteps(compiled) match
          case Some(_) =>
            Columns.Triples(compact(ColumnarInterpreter.triplesOf(columns), compiled, ordinals))
          case None =>
            chain(columns, compiled.atoms)
      case _ =>
        chain(columns, compiled.atoms)

  private def chain(columns: Columns, atoms: Vector[Atom]): Columns =
    atoms.foldLeft(columns) { (current, atom) =>
      val filtered = ColumnarInterpreter.onEachMorsel(inputs(current)) { morsel =>
        PredicatePlan.applyAtom(morsel.batch, morsel.start, morsel.length, atom)
      }
      copy(current, filtered)
    }

  private def compact(
      parts: Vector[ColumnBatch],
      compiled: Compiled,
      ordinals: Set[Int],
  ): Vector[ColumnBatch] =
    val steps =
      PredicatePlan.selectionSteps(PredicatePlan.encoded(compiled, ordinals)).getOrElse(missing)
    ColumnarInterpreter.onEachMorsel(parts) { morsel =>
      Selection(morsel.batch, morsel.start, morsel.length, steps)
    }

  private def kindBatches(kind: PhysicalOp.Kind, columns: Columns): Vector[ColumnBatch] =
    kind match
      case PhysicalOp.Kind.Pairs => ColumnarInterpreter.pairsOf(columns)
      case PhysicalOp.Kind.LongPairs => ColumnarInterpreter.longPairsOf(columns)
      case PhysicalOp.Kind.Triples => ColumnarInterpreter.triplesOf(columns)
      case PhysicalOp.Kind.StringPairs => StringPairWalk.batches(columns)
      case _ => missing

  /**
   * One index per input batch. Morsels of that batch run in parallel, then their absolute ids are
   * appended in morsel order. The id array is the batch length. `count` is the survivor total.
   */
  private def selectBatches(
      parts: Vector[ColumnBatch],
      steps: Vector[Selection.Step],
  ): Vector[Selected] =
    val cuts = parts.map(part => Morsels.of(part, ColumnarPlanner.batchSize))
    val flat = MorselScheduler.map(cuts.flatten, ColumnarPlanner.parallelism) { (_, morsel) =>
      Selection.applySelected(morsel.batch, morsel.start, morsel.length, steps)
    }
    val consumed = (flat, Vector.empty[Vector[Selected]])
    val (rest, groups) = cuts.foldLeft(consumed) { (state, morsels) =>
      val (remaining, acc) = state
      val (group, tail) = remaining.splitAt(morsels.length)
      (tail, acc :+ group)
    }
    require(rest.isEmpty, "selection morsels did not cover the batches")
    parts.zip(groups).map { (batch, morsels) =>
      assemble(batch, morsels)
    }

  private def assemble(batch: ColumnBatch, morsels: Vector[Selected]): Selected =
    val rows = new Array[Int](batch.length)
    val total = morsels.foldLeft(0) { (written, morsel) =>
      require(
        written + morsel.count <= rows.length,
        s"selection morsels exceed capacity ${rows.length}",
      )
      System.arraycopy(morsel.rows, 0, rows, written, morsel.count)
      written + morsel.count
    }
    Selected(batch, rows, total)

  private def inputs(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Pairs(_) => ColumnarInterpreter.pairsOf(columns)
    case Columns.LongPairs(_) => ColumnarInterpreter.longPairsOf(columns)
    case Columns.Triples(_) => ColumnarInterpreter.triplesOf(columns)
    case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.Strings(_) |
        Columns.Bools(_) | Columns.StringPairs(_) =>
      ColumnarInterpreter.batchesOf(columns)
    case Columns.LongCounts(_) | Columns.LongTriples(_) | Columns.StringTriples(_) |
        Columns.Outer(_, _) | Columns.GroupedInts(_) | Columns.GroupedStrings(_) |
        Columns.GroupedLongs(_) =>
      missing

  private def copy(columns: Columns, parts: Vector[ColumnBatch]): Columns = columns match
    case Columns.Ints(_) => Columns.Ints(parts)
    case Columns.Longs(_) => Columns.Longs(parts)
    case Columns.Floats(_) => Columns.Floats(parts)
    case Columns.Strings(_) => Columns.Strings(parts)
    case Columns.Bools(_) => Columns.Bools(parts)
    case Columns.Pairs(_) => Columns.Pairs(parts)
    case Columns.LongPairs(_) => Columns.LongPairs(parts)
    case Columns.Triples(_) => Columns.Triples(parts)
    case Columns.StringPairs(_) => Columns.StringPairs(parts)
    case Columns.LongCounts(_) | Columns.LongTriples(_) | Columns.StringTriples(_) |
        Columns.Outer(_, _) | Columns.GroupedInts(_) | Columns.GroupedStrings(_) |
        Columns.GroupedLongs(_) =>
      missing

  /**
   * A pure triple-slot stack rejects a clear bit, so the stored `0` is published first. A stack
   * that contains a whole-row step has no selection steps and is left alone: that step drops the
   * null row. A slot atom later in the chain publishes zeros for itself.
   */
  private def presentSlotStack(columns: Columns, compiled: Compiled): Columns =
    val slots = PredicatePlan.selectionSteps(compiled).exists(_.exists(tripleSlot))
    if (!slots) then columns
    else
      columns match
        case Columns.Triples(parts) => Columns.Triples(parts.map(ColumnBatches.presentStoredZeros))
        case other => other

  private def tripleSlot(step: Selection.Step): Boolean = step match
    case Selection.Step.IntTriple(_, _) => true
    case Selection.Step.Int32(_, _) | Selection.Step.Int64(_, _) | Selection.Step.Float64(_, _) |
        Selection.Step.Bool(_, _) | Selection.Step.Utf8(_, _) | Selection.Step.IntPair(_) |
        Selection.Step.LongPair(_) | Selection.Step.StringPair(_) =>
      false

  private def opening(plan: Plan[_], op: PhysicalOp): PhysicalOp.Slot =
    PredicatePlan.openingSlot(plan, op).getOrElse(missing)

  private def missing: Nothing =
    throw new IllegalStateException("filter stack has no predicate")
