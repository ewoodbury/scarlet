package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.core.Plan
import com.ewoodbury.scarlet.execution.schema.ColumnBinding.{Operator, ReducerWitness}
import com.ewoodbury.scarlet.execution.schema.{Describe, KernelChoice}

/**
 * Builds the aggregate roots [[AggregateBinding]] does not already have.
 *
 * The child is a stack of two or more key or value slots, then exactly one [[Plan.MapIntKeyOp]] or
 * one same-type [[Plan.MapValuesOp]], over a child [[ColumnarPlanner.lower]] already accepts. The
 * binder is asked only after those slots compile to selection steps. A whole-row filter, a second
 * map, and an `Int => Long` value map stay empty. The filter plan itself is not lowered here: only
 * the aggregate parent builds this tree. [[JoinProbeAggregate]] and [[JoinAggregateBinding]] are
 * not on this path.
 */
private[execution] object AggregateAdmission:

  /** Two slots. One slot above a map already lowers, so it is not a new root. */
  private val MinimumStack = 2

  def admit(
      source: Plan[_],
      lowerChild: Plan[_] => Option[PhysicalOp],
      operator: Operator,
      reducer: ReducerWitness,
  ): Option[PhysicalOp] =
    val (slots, under) = peel(source, Nil)
    if (slots.length < MinimumStack) then None
    else
      mapped(under, lowerChild).flatMap { mappedOp =>
        val filtered = stack(slots, mappedOp)
        if (!compiles(source, filtered)) then None
        else AggregateBinding.choiceOf(mappedOp, operator, reducer).flatMap(root(_, filtered))
      }

  /**
   * One map whose child lowered and does not already hold a map. The base plan is not interpreted.
   * A second [[Plan.MapIntKeyOp]] or [[Plan.MapValuesOp]] stays empty.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def mapped(
      plan: Plan[_],
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    plan match
      case Plan.MapIntKeyOp(base, _) =>
        lowerChild(base).filter(intPairs).filterNot(mappedAlready).map(PhysicalOp.MapKey(_))
      case Plan.MapValuesOp(base, function) =>
        lowerChild(base)
          .filter(child => sameType(child.kind, function))
          .filterNot(mappedAlready)
          .map {
            PhysicalOp.MapValue(_)
          }
      case _ => None

  private def intPairs(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Pairs => true
    case _ => false

  /**
   * `Int => Int` on int or string pairs, or `Long => Long` on long pairs. `Int => Long` is
   * neither.
   */
  private def sameType(kind: PhysicalOp.Kind, function: Any): Boolean = kind match
    case PhysicalOp.Kind.Pairs | PhysicalOp.Kind.StringPairs =>
      FunctionWitness.declaredElement(PhysicalOp.Kind.Ints, function)
    case PhysicalOp.Kind.LongPairs =>
      FunctionWitness.declaredElement(PhysicalOp.Kind.Longs, function)
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Triples | PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.Strings |
        PhysicalOp.Kind.Bools | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.LongCounts |
        PhysicalOp.Kind.GroupedInts | PhysicalOp.Kind.GroupedStrings |
        PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter | PhysicalOp.Kind.StringOuter |
        PhysicalOp.Kind.CountSums | PhysicalOp.Kind.LongCountSums |
        PhysicalOp.Kind.StringCountSums | PhysicalOp.Kind.IntLongSums |
        PhysicalOp.Kind.GroupedIntLongs | PhysicalOp.Kind.IntLongTriples |
        PhysicalOp.Kind.IntLongOuter | PhysicalOp.Kind.Float32s | PhysicalOp.Kind.Columns =>
      false

  private def mappedAlready(op: PhysicalOp): Boolean =
    MapIntKey.inTree(op) || mapValueInTree(op)

  private def mapValueInTree(op: PhysicalOp): Boolean = op match
    case _: PhysicalOp.MapValue => true
    case other => other.children.exists(mapValueInTree)

  /** Outside slot first. A whole-row filter stops the walk, so that predicate is not dropped. */
  @tailrec
  private def peel(
      plan: Plan[_],
      slots: List[PhysicalOp.Slot],
  ): (List[PhysicalOp.Slot], Plan[_]) =
    plan match
      case Plan.FilterKeysOp(source, _) =>
        peel(source, PhysicalOp.Slot.Key :: slots)
      case Plan.FilterValuesOp(source, _) =>
        peel(source, PhysicalOp.Slot.Value :: slots)
      case other =>
        (slots.reverse, other)

  /** Outer filter is the head of `slots`. */
  private def stack(slots: List[PhysicalOp.Slot], child: PhysicalOp): PhysicalOp =
    slots.foldRight(child)((slot, current) => PhysicalOp.Filter(current, slot))

  /**
   * Steps for the slot stack. Missing steps, or a cast the compiler rejects, is no root: the
   * predicate is not dropped.
   */
  private def compiles(plan: Plan[_], op: PhysicalOp): Boolean =
    try PredicatePlan.compile(plan, op).flatMap(PredicatePlan.selectionSteps).exists(_.nonEmpty)
    catch
      case _: ClassCastException => false
      case _: IllegalStateException => false

  /**
   * The exchange is the aggregate's child, as today's roots are. A tag this parent does not build,
   * including [[KernelChoice.CountThroughProbe]] and [[KernelChoice.IntLongProjectSum]], is empty.
   * [[Describe.tag]] reports the tag stored on the node. The child schema does not rename it.
   */
  private def root(tag: KernelChoice, child: PhysicalOp): Option[PhysicalOp] =
    val exchanged = PhysicalOp.Exchange(child)
    val built = tag match
      case KernelChoice.CountByKey => Some(PhysicalOp.CountByKey(exchanged))
      case KernelChoice.IntHashAggregate =>
        Some(PhysicalOp.HashAggregate(exchanged, PhysicalOp.HashAggregateTag.IntHash))
      case KernelChoice.LongHashAggregate => Some(PhysicalOp.LongHashAggregate(exchanged))
      case KernelChoice.StringHashAggregate => Some(PhysicalOp.StringHashAggregate(exchanged))
      case KernelChoice.CountSum => Some(PhysicalOp.CountSum(exchanged))
      case _ => None
    built.filter(agrees(_, tag))

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  private def agrees(op: PhysicalOp, tag: KernelChoice): Boolean =
    Describe.tag(op) == tag
