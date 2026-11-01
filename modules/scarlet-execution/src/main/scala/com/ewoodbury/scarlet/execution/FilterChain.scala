package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.api.IntJoinTriplePredicate
import com.ewoodbury.scarlet.columnar.{ColumnBatch, ColumnKernel, PairKernel, Selection}
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns

/**
 * Peels a stack of consecutive unfused filters and compacts it once per morsel.
 *
 * Named erasure boundary, same as [[ColumnarInterpreter]]. The planner chose the slot and did not
 * call the function. The cast checks the erased type. A wrong type throws `ClassCastException`,
 * and [[ColumnarPlanner.tryRun]] returns `None`. A column kind this stack cannot compact throws
 * that same exception, so the query degrades to the row path. The casts run after the child is
 * interpreted and before any morsel, so a bad later predicate does not run the earlier ones.
 *
 * A lone filter is handed to [[ColumnarInterpreter.applyFilter]], which keeps [[ColumnKernel]],
 * [[PairKernel]], and [[BoolPipeline.filter]]. Two or more filters interpret the non-filter child
 * once, then [[Selection]] applies every predicate to that batch. A map or a pipeline is not a
 * filter and stops the peel.
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Throw",
  ),
)
private[execution] object FilterChain:

  private final case class Layer(predicate: Any, slot: PhysicalOp.Slot)

  private final case class Peeled(plan: Plan[_], op: PhysicalOp, layers: List[Layer])

  def accepts(plan: Plan[_], op: PhysicalOp): Boolean = layerOf(plan, op).isDefined

  def run(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
  ): Columns =
    val peeled = peel(plan, op)
    val columns = ColumnarInterpreter.interpret(
      peeled.plan,
      peeled.op,
      selectedPartition,
      preserveAbsence,
    )
    peeled.layers match
      case Nil =>
        throw new IllegalStateException("filter stack has no predicate")
      case layer :: Nil =>
        ColumnarInterpreter.applyFilter(columns, layer.predicate, layer.slot)
      case stacked =>
        select(columns, stacked)

  @tailrec
  private def walk(plan: Plan[_], op: PhysicalOp, acc: List[Layer]): Peeled =
    layerOf(plan, op) match
      case Some((source, child, layer)) => walk(source, child, layer :: acc)
      case None => Peeled(plan, op, acc)

  private def peel(plan: Plan[_], op: PhysicalOp): Peeled = walk(plan, op, Nil)

  private def layerOf(plan: Plan[_], op: PhysicalOp): Option[(Plan[_], PhysicalOp, Layer)] =
    (plan, op) match
      case (Plan.FilterOp(source, predicate), PhysicalOp.Filter(child, slot)) =>
        Some((source, child, Layer(predicate, slot)))
      case (Plan.FilterKeysOp(source, predicate), PhysicalOp.Filter(child, PhysicalOp.Slot.Key)) =>
        Some((source, child, Layer(predicate, PhysicalOp.Slot.Key)))
      case (
            Plan.FilterValuesOp(source, predicate),
            PhysicalOp.Filter(child, PhysicalOp.Slot.Value),
          ) =>
        Some((source, child, Layer(predicate, PhysicalOp.Slot.Value)))
      case _ => None

  private def select(columns: Columns, layers: List[Layer]): Columns = columns match
    case Columns.Ints(parts) =>
      Columns.Ints(finish(parts, layers.map(stepOnInts)))
    case Columns.Longs(parts) =>
      Columns.Longs(finish(parts, layers.map(stepOnLongs)))
    case Columns.Floats(parts) =>
      Columns.Floats(finish(parts, layers.map(stepOnFloats)))
    case Columns.Strings(parts) =>
      Columns.Strings(finish(parts, layers.map(stepOnStrings)))
    case Columns.Bools(parts) =>
      Columns.Bools(finish(parts, layers.map(stepOnBools)))
    case Columns.Pairs(_) =>
      val steps = layers.map(stepOnPairs)
      Columns.Pairs(finish(ColumnarInterpreter.pairsOf(columns), steps))
    case Columns.Triples(_) =>
      val steps = layers.map(stepOnTriples)
      Columns.Triples(finish(ColumnarInterpreter.triplesOf(columns), steps))
    case Columns.LongPairs(_) | Columns.LongTriples(_) | Columns.Outer(_, _) =>
      refuse(layers)

  private def finish(parts: Vector[ColumnBatch], steps: Seq[Selection.Step]): Vector[ColumnBatch] =
    val prepared = parts.map(part => part -> Selection.prepare(part, steps)).toMap
    ColumnarInterpreter.onEachMorsel(parts) { morsel =>
      val resolved = prepared.getOrElse(
        morsel.batch,
        throw new IllegalStateException("morsel batch has no resolved filter steps"),
      )
      Selection(morsel.batch, morsel.start, morsel.length, resolved)
    }

  /**
   * `ClassCastException` is the row-path fallback. [[ColumnarPlanner.tryRun]] catches only that
   * type. A kind this stack cannot compact degrades instead of failing the query.
   */
  private def refuse(layers: List[Layer]): Nothing =
    val layer = layers match
      case head :: _ => head
      case Nil => throw new IllegalStateException("filter stack has no predicate")
    val message = layer.slot match
      case PhysicalOp.Slot.Element =>
        "columnar element filter expects ints, longs, floats, booleans, or strings"
      case PhysicalOp.Slot.Row | PhysicalOp.Slot.Key | PhysicalOp.Slot.Value =>
        "columnar op expected int pairs"
      case PhysicalOp.Slot.TripleKey | PhysicalOp.Slot.TripleLeftValue |
          PhysicalOp.Slot.TripleRightValue =>
        "columnar op expected int triples"
    throw new ClassCastException(message)

  private def stepOnInts(layer: Layer): Selection.Step =
    elementStep(layer) { predicate =>
      val keep = predicate.asInstanceOf[Int => Boolean]
      val adapted: ColumnKernel.Int32Predicate = value => keep(value)
      Selection.Step.Int32(0, adapted)
    }

  private def stepOnLongs(layer: Layer): Selection.Step =
    elementStep(layer) { predicate =>
      val keep = predicate.asInstanceOf[Long => Boolean]
      val adapted: ColumnKernel.Int64Predicate = value => keep(value)
      Selection.Step.Int64(0, adapted)
    }

  private def stepOnFloats(layer: Layer): Selection.Step =
    elementStep(layer) { predicate =>
      val keep = predicate.asInstanceOf[Double => Boolean]
      val adapted: ColumnKernel.Float64Predicate = value => keep(value)
      Selection.Step.Float64(0, adapted)
    }

  private def stepOnBools(layer: Layer): Selection.Step =
    elementStep(layer) { predicate =>
      val keep = predicate.asInstanceOf[Boolean => Boolean]
      val adapted: ColumnKernel.BoolPredicate = value => keep(value)
      Selection.Step.Bool(0, adapted)
    }

  private def stepOnStrings(layer: Layer): Selection.Step =
    elementStep(layer) { predicate =>
      ColumnarPlanner.dictionaryStringLiterals(predicate) match
        case Some(literals) => Selection.Step.Utf8Codes(0, literals)
        case None =>
          val keep = predicate.asInstanceOf[String => Boolean]
          val adapted: ColumnKernel.Utf8Predicate = value => keep(value)
          Selection.Step.Utf8(0, adapted)
    }

  private def elementStep(layer: Layer)(build: Any => Selection.Step): Selection.Step =
    layer.slot match
      case PhysicalOp.Slot.Element => build(layer.predicate)
      case PhysicalOp.Slot.Row | PhysicalOp.Slot.Key | PhysicalOp.Slot.Value =>
        throw new IllegalStateException("columnar op expected int pairs")
      case PhysicalOp.Slot.TripleKey | PhysicalOp.Slot.TripleLeftValue |
          PhysicalOp.Slot.TripleRightValue =>
        throw new IllegalStateException("columnar op expected int triples")

  private def stepOnPairs(layer: Layer): Selection.Step = layer.slot match
    case PhysicalOp.Slot.Key => intOrdinal(layer.predicate, 0)
    case PhysicalOp.Slot.Value => intOrdinal(layer.predicate, 1)
    case PhysicalOp.Slot.Row =>
      val keep = layer.predicate.asInstanceOf[((Int, Int)) => Boolean]
      val adapted: PairKernel.IntPairPredicate = (key, value) => keep((key, value))
      Selection.Step.IntPair(adapted)
    case PhysicalOp.Slot.Element =>
      throw new IllegalStateException(
        "columnar element filter expects ints, longs, floats, booleans, or strings",
      )
    case PhysicalOp.Slot.TripleKey | PhysicalOp.Slot.TripleLeftValue |
        PhysicalOp.Slot.TripleRightValue =>
      throw new IllegalStateException("columnar op expected int triples")

  private def intOrdinal(predicate: Any, ordinal: Int): Selection.Step =
    val keep = predicate.asInstanceOf[Int => Boolean]
    val adapted: ColumnKernel.Int32Predicate = value => keep(value)
    Selection.Step.Int32(ordinal, adapted)

  private def stepOnTriples(layer: Layer): Selection.Step = layer.slot match
    case PhysicalOp.Slot.TripleKey => tripleOrdinal(layer.predicate, 0)
    case PhysicalOp.Slot.TripleLeftValue => tripleOrdinal(layer.predicate, 1)
    case PhysicalOp.Slot.TripleRightValue => tripleOrdinal(layer.predicate, 2)
    case PhysicalOp.Slot.Element =>
      throw new IllegalStateException(
        "columnar element filter expects ints, longs, floats, booleans, or strings",
      )
    case PhysicalOp.Slot.Row | PhysicalOp.Slot.Key | PhysicalOp.Slot.Value =>
      throw new IllegalStateException("columnar op expected int pairs")

  private def tripleOrdinal(predicate: Any, ordinal: Int): Selection.Step =
    val keep = predicate match
      case recognized: IntJoinTriplePredicate[?] => recognized
      case _ => throw new IllegalStateException("triple filter lost its slot predicate")
    val adapted: ColumnKernel.Int32Predicate = value => keep.keep(value)
    Selection.Step.IntTriple(ordinal, adapted)
