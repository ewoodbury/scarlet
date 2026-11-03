package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnBatch, Selection}
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns
import PredicatePlan.{Atom, Compiled}

/**
 * Peels a stack of consecutive unfused filters.
 *
 * [[PredicatePlan.compile]] casts every predicate before a kernel runs, and stops at a map. The
 * child is interpreted first, so a later mistyped predicate still does not run the earlier ones. A
 * lone filter is one [[PredicatePlan.applyAtom]] on the existing kernel. Two or more pair or
 * triple filters compact through [[PredicatePlan.selectionSteps]] in one pass. An int, long,
 * float, string, or boolean stack chains [[PredicatePlan.applyAtom]], one kernel per filter. The
 * survivor index is four bytes per row, which costs more than the intermediate column a chained
 * kernel writes for the next filter to read. A boolean column is one bit per row. A long-pair row
 * filter chains the same way, one [[LongPairKernel]] per filter. A whole-row triple filter chains
 * too: it is not a slot atom, so it does not compact through [[Selection]]. Long triples and outer
 * batches still refuse. A map or a pipeline is not a filter and stops the peel.
 *
 * [[PredicatePlan.applyAtom]] does not take the compiled stack. Encoders, including the
 * absence-preserving copy under an outer join, do not run the predicate.
 */
@SuppressWarnings(Array("org.wartremover.warts.Throw"))
private[execution] object FilterChain:

  def accepts(plan: Plan[_], op: PhysicalOp): Boolean = PredicatePlan.accepts(plan, op)

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
      case Columns.LongTriples(_) =>
        PredicatePlan.unsupported(opening(plan, op))
      // An outer batch is a triple with an absent side. This must stay an IllegalStateException:
      // the planner catches ClassCastException, including ColumnarFallback, and falls back to
      // rows, where the predicate would unbox the absent side and throw a null pointer.
      case Columns.Outer(_, _) =>
        throw new IllegalStateException(PredicatePlan.outerMessage)
      case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.Strings(_) |
          Columns.Bools(_) | Columns.Pairs(_) | Columns.LongPairs(_) | Columns.Triples(_) =>
        val compiled = PredicatePlan.compile(plan, op).getOrElse(missing)
        applyCompiled(columns, compiled, ordinals)

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
    val steps = PredicatePlan.selectionSteps(encoded(compiled, ordinals)).getOrElse(missing)
    ColumnarInterpreter.onEachMorsel(parts) { morsel =>
      Selection(morsel.batch, morsel.start, morsel.length, steps)
    }

  /**
   * A one-column encode stores the value at index 0. [[PredicatePlan.compile]] records logical
   * ordinal 1. The root ordinal set is what decided the encode, and it is not on the atom.
   */
  private def encoded(compiled: Compiled, ordinals: Set[Int]): Compiled =
    if (ordinals.size == 1) then
      Compiled(compiled.atoms.map {
        case Atom.PairValue(_, keep) => Atom.PairValue(0, keep)
        case atom => atom
      })
    else compiled

  private def inputs(columns: Columns): Vector[ColumnBatch] = columns match
    case Columns.Pairs(_) => ColumnarInterpreter.pairsOf(columns)
    case Columns.LongPairs(_) => ColumnarInterpreter.longPairsOf(columns)
    case Columns.Triples(_) => ColumnarInterpreter.triplesOf(columns)
    case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.Strings(_) |
        Columns.Bools(_) =>
      ColumnarInterpreter.batchesOf(columns)
    case Columns.LongTriples(_) | Columns.Outer(_, _) =>
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
    case Columns.LongTriples(_) | Columns.Outer(_, _) =>
      missing

  private def opening(plan: Plan[_], op: PhysicalOp): PhysicalOp.Slot =
    PredicatePlan.openingSlot(plan, op).getOrElse(missing)

  private def missing: Nothing =
    throw new IllegalStateException("filter stack has no predicate")
