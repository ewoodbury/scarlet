package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.api.IntJoinTriplePredicate
import com.ewoodbury.scarlet.columnar.{ColumnBatch, ColumnKernel, PairKernel, Selection}
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.Columns

/**
 * Peels a stack of consecutive unfused filters.
 *
 * Named erasure boundary, same as [[ColumnarInterpreter]]. The planner chose the slot and did not
 * call the function. The cast checks the erased type. A wrong type throws `ClassCastException`,
 * and [[ColumnarPlanner.tryRun]] returns `None`. A column kind this stack cannot compact throws
 * that same exception, so the query degrades to the row path. Pair and triple steps are cast after
 * the child is interpreted and before any morsel. A single-column chain resolves every step's
 * typed function, including a dictionary literal set, before any morsel. A bad later predicate
 * does not run the earlier ones.
 *
 * A lone filter is handed to [[ColumnarInterpreter.applyFilter]], which keeps [[ColumnKernel]],
 * [[PairKernel]], and [[BoolPipeline.filter]]. Two or more filters interpret the non-filter child
 * once. [[Selection]] then compacts a pair or triple batch in one pass. An int, long, float,
 * string, or boolean batch chains [[ColumnarInterpreter.applyFilter]], one kernel per filter. The
 * survivor index is four bytes per row, which costs more than the intermediate column a chained
 * kernel writes for the next filter to read. A boolean column is one bit per row. Long pairs, long
 * triples, and outer batches still refuse. A map or a pipeline is not a filter and stops the peel.
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
      ordinals: Set[Int],
  ): Columns =
    val peeled = peel(plan, op)
    val columns = ColumnarInterpreter.interpretAt(
      peeled.plan,
      peeled.op,
      selectedPartition,
      preserveAbsence,
      ordinals,
    )
    peeled.layers match
      case Nil =>
        throw new IllegalStateException("filter stack has no predicate")
      case layer :: Nil =>
        ColumnarInterpreter.applyFilter(columns, layer.predicate, layer.slot)
      case stacked =>
        select(columns, stacked, ordinals)

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

  private def select(columns: Columns, layers: List[Layer], ordinals: Set[Int]): Columns =
    columns match
      case Columns.Ints(_) | Columns.Longs(_) | Columns.Floats(_) | Columns.Strings(_) |
          Columns.Bools(_) =>
        chain(columns, layers)
      case Columns.Pairs(_) =>
        val steps = layers.map(layer => stepOnPairs(layer, ordinals))
        Columns.Pairs(finish(ColumnarInterpreter.pairsOf(columns), steps))
      case Columns.Triples(_) =>
        val steps = layers.map(stepOnTriples)
        Columns.Triples(finish(ColumnarInterpreter.triplesOf(columns), steps))
      case Columns.LongPairs(_) | Columns.LongTriples(_) | Columns.Outer(_, _) =>
        refuse(layers)

  /** One kernel per filter. The intermediate column is the survivor set the next kernel reads. */
  private def chain(columns: Columns, layers: List[Layer]): Columns =
    val resolved = layers.map(layer => resolve(columns, layer))
    resolved.foldLeft(columns) { (current, predicate) =>
      ColumnarInterpreter.applyFilter(current, predicate, PhysicalOp.Slot.Element)
    }

  /**
   * Casts one element predicate, or reads its dictionary literals, and does not call it. Every
   * layer is resolved before [[chain]] runs a kernel.
   */
  private def resolve(columns: Columns, layer: Layer): Any =
    layer.slot match
      case PhysicalOp.Slot.Element =>
        resolveElement(columns, layer.predicate)
        layer.predicate
      case PhysicalOp.Slot.Row | PhysicalOp.Slot.Key | PhysicalOp.Slot.Value =>
        throw new IllegalStateException("columnar op expected int pairs")
      case PhysicalOp.Slot.TripleKey | PhysicalOp.Slot.TripleLeftValue |
          PhysicalOp.Slot.TripleRightValue =>
        throw new IllegalStateException("columnar op expected int triples")

  private def resolveElement(columns: Columns, predicate: Any): Unit = columns match
    case Columns.Ints(_) =>
      requireSpecialized(predicate, "apply$mcZI$sp", classOf[Int], classOf[Boolean])
    case Columns.Longs(_) =>
      requireSpecialized(predicate, "apply$mcZJ$sp", classOf[Long], classOf[Boolean])
    case Columns.Floats(_) =>
      requireSpecialized(predicate, "apply$mcZD$sp", classOf[Double], classOf[Boolean])
    case Columns.Bools(_) =>
      if (!acceptsBoolean(predicate)) then rejectPredicate(predicate)
      val _ = predicate.asInstanceOf[Boolean => Boolean]
    case Columns.Strings(_) =>
      ColumnarPlanner.dictionaryStringLiterals(predicate) match
        case Some(_) => ()
        case None =>
          if (specializedApply(predicate)) then rejectPredicate(predicate)
          val _ = predicate.asInstanceOf[String => Boolean]
    case Columns.Pairs(_) | Columns.LongPairs(_) | Columns.Triples(_) | Columns.LongTriples(_) |
        Columns.Outer(_, _) =>
      throw new IllegalStateException(
        "columnar element filter expects ints, longs, floats, booleans, or strings",
      )

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def requireSpecialized(
      predicate: Any,
      specialized: String,
      argument: Class[_],
      result: Class[_],
  ): Unit =
    val found = predicate.getClass.getDeclaredMethods.exists { method =>
      !method.isBridge &&
      method.getName == specialized &&
      method.getParameterCount == 1 &&
      method.getParameterTypes()(0).eq(argument) &&
      method.getReturnType.eq(result)
    }
    if (!found) then rejectPredicate(predicate)

  /** A primitive specialized `apply` is the implementation. A bridge does not count. */
  private def specializedApply(predicate: Any): Boolean =
    predicate.getClass.getDeclaredMethods.exists { method =>
      method.getName.startsWith("apply$mc") && !method.isBridge
    }

  private def acceptsBoolean(predicate: Any): Boolean =
    !specializedApply(predicate) && predicate.getClass.getDeclaredMethods.exists(booleanApply)

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def booleanApply(method: java.lang.reflect.Method): Boolean =
    !method.isBridge &&
      method.getName == "apply" &&
      method.getParameterCount == 1 &&
      (primitiveBoolean(method) || erasedBoolean(method) || boxedBoolean(method))

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def primitiveBoolean(method: java.lang.reflect.Method): Boolean =
    method.getParameterTypes()(0).eq(classOf[Boolean]) &&
      method.getReturnType.eq(classOf[Boolean])

  /** A lambda declares `apply(Object)` and returns a primitive boolean. */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def erasedBoolean(method: java.lang.reflect.Method): Boolean =
    method.getParameterTypes()(0).eq(classOf[Object]) &&
      method.getReturnType.eq(classOf[Boolean])

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def boxedBoolean(method: java.lang.reflect.Method): Boolean =
    method.getParameterTypes()(0).eq(classOf[Object]) &&
      method.getReturnType.eq(classOf[java.lang.Boolean])

  private def rejectPredicate(predicate: Any): Nothing =
    throw new ClassCastException(s"columnar filter predicate is ${predicate.getClass.getName}")

  private def finish(parts: Vector[ColumnBatch], steps: Seq[Selection.Step]): Vector[ColumnBatch] =
    ColumnarInterpreter.onEachMorsel(parts) { morsel =>
      Selection(morsel.batch, morsel.start, morsel.length, steps)
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

  private def stepOnPairs(layer: Layer, ordinals: Set[Int]): Selection.Step = layer.slot match
    case PhysicalOp.Slot.Key => intOrdinal(layer.predicate, 0)
    case PhysicalOp.Slot.Value =>
      // A one-column encode stores that column at index 0. Both columns keep the value at 1.
      val index = if (ordinals.size == 1) then 0 else 1
      intOrdinal(layer.predicate, index)
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
