package com.ewoodbury.scarlet.execution

import scala.annotation.tailrec

import com.ewoodbury.scarlet.columnar.{ColumnKernel, Morsel}
import com.ewoodbury.scarlet.core.Plan

/**
 * Fold an int filter/map chain into `sum` or `count` when `lower` has split it.
 *
 * The chain starts at an int scan or at [[PhysicalOp.Column]]: `keys`, `values`, or
 * `mapValues(...).values`. The physical tree stays as `lower` built it. The action reads that base
 * once, applies every step, and does not write the pipeline column or a trailing projection. `sum`
 * uses this past four steps, where the specialized loops stop. `count` uses it from two steps up,
 * including a short chain that ends in a filter. One trailing int project is summed and counted
 * through the same shape gate. `collect` still writes the column. A long chain uses the same
 * split: [[sumLong]] from five steps and one trailing project, [[count]] from two steps. Both call
 * [[ElementSum.sumLongs]] or [[ElementSum.countLongs]] and do not write the chain. A string scan
 * under only dictionary literal filters is folded the same way. The physical wrappers name the
 * same number of steps, the base is the scan `lower` built, and the count reads codes from the
 * intersected mask. A map, or a filter that is not a literal list, is not this chain.
 */
private[execution] object TrailingProject:

  def sum(plan: Plan[_], op: PhysicalOp): Option[Long] =
    chain(plan, op, minSteps = 5)
      .map { case (basePlan, baseOp, steps) =>
        foldBase(basePlan, baseOp)(morsel => ElementSum.sumLonger(morsel, steps))
      }
      .orElse(sumProject(plan, op))

  def count(plan: Plan[_], op: PhysicalOp): Option[Long] =
    chain(plan, op, minSteps = 2)
      .map { case (basePlan, baseOp, steps) =>
        foldBase(basePlan, baseOp)(morsel => ElementSum.countLonger(morsel, steps))
      }
      .orElse(countProject(plan, op))
      .orElse(countLong(plan, op))
      .orElse(countLiteralStrings(plan, op))

  /** Long sum past four steps, or one trailing long project. Does not write the chain. */
  def sumLong(plan: Plan[_], op: PhysicalOp): Option[Long] =
    longChain(plan, op, minSteps = 5)
      .map { case (basePlan, baseOp, steps) =>
        foldBase(basePlan, baseOp)(morsel => ElementSum.sumLongs(morsel, steps))
      }
      .orElse(foldLongProject(plan, op, ProjectFold.Sum))

  private def countLong(plan: Plan[_], op: PhysicalOp): Option[Long] =
    longChain(plan, op, minSteps = 2)
      .map { case (basePlan, baseOp, steps) =>
        foldBase(basePlan, baseOp)(morsel => ElementSum.countLongs(morsel, steps))
      }
      .orElse(foldLongProject(plan, op, ProjectFold.Count))

  /**
   * Element steps at or above `minSteps`. The physical wrappers have to name the same number of
   * steps, and the base has to be the int scan or the pair column those steps read. Anything else
   * stays on the interpreter, including a shape [[ElementSum.countLonger]] cannot run.
   */
  private def chain(
      plan: Plan[_],
      op: PhysicalOp,
      minSteps: Int,
  ): Option[(Plan[_], PhysicalOp, List[ColumnarPlanner.ElementStep])] =
    intColumnBase(op) match
      case Some(baseOp) =>
        val (basePlan, steps) = ColumnarPlanner.elementSteps(plan, Int.MaxValue)
        val longEnough = steps.length >= minSteps
        val sameShape = elementDepth(op) == steps.length
        if (longEnough && sameShape && aligned(basePlan, baseOp)) then
          Some((basePlan, baseOp, steps))
        else None
      case None => None

  /**
   * String scan under only dictionary literal filters, when the physical wrappers name the same
   * number of steps. `None` means count should read the finished batches. The base op is the scan
   * [[lower]] built, and the groups are the literals those steps named.
   */
  private def literalChain(
      plan: Plan[_],
      op: PhysicalOp,
  ): Option[(Plan[_], PhysicalOp, Vector[Vector[String]])] =
    stringColumnBase(op).flatMap { baseOp =>
      val (basePlan, steps) = ColumnarPlanner.elementSteps(plan, Int.MaxValue)
      val sameShape = elementDepth(op) == steps.length
      if (sameShape && stringAligned(basePlan, baseOp)) then
        literalStringGroups(steps).map(groups => (basePlan, baseOp, groups))
      else None
    }

  private def countLiteralStrings(plan: Plan[_], op: PhysicalOp): Option[Long] =
    literalChain(plan, op).map { case (basePlan, baseOp, groups) =>
      ColumnarPlanner.foldWholeBatches(basePlan, baseOp) { batch =>
        ColumnKernel.countByGroups(batch, 0, groups)
      }
    }

  /**
   * Literal groups in source order. `None` means a step is a project or a filter that is not a
   * dictionary literal list. An empty step list is not a chain.
   */
  private def literalStringGroups(
      steps: List[ColumnarPlanner.ElementStep],
  ): Option[Vector[Vector[String]]] =
    def gather(
        remaining: List[ColumnarPlanner.ElementStep],
        acc: List[Vector[String]],
    ): Option[List[Vector[String]]] =
      remaining match
        case Nil => Some(acc.reverse)
        case ColumnarPlanner.ElementStep.Filter(predicate) :: tail =>
          ColumnarPlanner.dictionaryStringLiterals(predicate) match
            case Some(literals) => gather(tail, literals :: acc)
            case None => None
        case ColumnarPlanner.ElementStep.Project(_) :: _ => None

    if (steps.isEmpty) then None
    else gather(steps, Nil).map(_.toVector)

  private def sumProject(plan: Plan[_], op: PhysicalOp): Option[Long] =
    foldProject(plan, op, ProjectFold.Sum)

  /** Count one trailing int project. Interpreting that project would write its column. */
  private def countProject(plan: Plan[_], op: PhysicalOp): Option[Long] =
    foldProject(plan, op, ProjectFold.Count)

  /**
   * One trailing int project over an int child. `sum` and `count` share this gate, so a change to
   * the shape cannot fold one action and write the column for the other.
   */
  private def foldProject(
      plan: Plan[_],
      op: PhysicalOp,
      action: ProjectFold,
  ): Option[Long] = (plan, op) match
    case (Plan.MapOp(source, mapper), PhysicalOp.Project(child)) if ColumnarPlanner.isInt(child) =>
      val total = ColumnarPlanner.foldInterpreted(source, child) { morsel =>
        action match
          case ProjectFold.Sum => ElementSum.sumMapped(morsel, mapper)
          case ProjectFold.Count => ElementSum.countMapped(morsel, mapper)
      }
      Some(total)
    case _ => None

  private enum ProjectFold:
    case Sum, Count

  private def foldBase(plan: Plan[_], op: PhysicalOp)(fn: Morsel => Long): Long =
    ColumnarPlanner.foldInterpreted(plan, op)(fn)

  private def aligned(plan: Plan[_], op: PhysicalOp): Boolean = (plan, op) match
    case (_: Plan.Source[_], PhysicalOp.Scan(PhysicalOp.Kind.Ints, _)) => true
    case (Plan.KeysOp(_), PhysicalOp.Column(_, 0)) => true
    case (Plan.ValuesOp(_), PhysicalOp.Column(_, 1)) => true
    case _ => false

  private def longChain(
      plan: Plan[_],
      op: PhysicalOp,
      minSteps: Int,
  ): Option[(Plan[_], PhysicalOp, List[ColumnarPlanner.ElementStep])] =
    longColumnBase(op) match
      case Some(baseOp) =>
        val (basePlan, steps) = ColumnarPlanner.elementSteps(plan, Int.MaxValue)
        val longEnough = steps.length >= minSteps
        val sameShape = elementDepth(op) == steps.length
        if (longEnough && sameShape && longAligned(basePlan, baseOp)) then
          Some((basePlan, baseOp, steps))
        else None
      case None => None

  private def foldLongProject(
      plan: Plan[_],
      op: PhysicalOp,
      action: ProjectFold,
  ): Option[Long] = (plan, op) match
    case (Plan.MapOp(source, mapper), PhysicalOp.Project(child))
        if ColumnarPlanner.isLong(child) =>
      val total = ColumnarPlanner.foldInterpreted(source, child) { morsel =>
        val steps = List(ColumnarPlanner.ElementStep.Project(mapper))
        action match
          case ProjectFold.Sum => ElementSum.sumLongs(morsel, steps)
          case ProjectFold.Count => ElementSum.countLongs(morsel, steps)
      }
      Some(total)
    case _ => None

  private def longAligned(plan: Plan[_], op: PhysicalOp): Boolean = (plan, op) match
    case (_: Plan.Source[_], PhysicalOp.Scan(PhysicalOp.Kind.Longs, _)) => true
    case _ => false

  private def stringAligned(plan: Plan[_], op: PhysicalOp): Boolean = (plan, op) match
    case (_: Plan.Source[_], PhysicalOp.Scan(PhysicalOp.Kind.Strings, _)) => true
    case _ => false

  @tailrec
  private def stringColumnBase(op: PhysicalOp): Option[PhysicalOp] = op match
    case scan @ PhysicalOp.Scan(PhysicalOp.Kind.Strings, _) => Some(scan)
    case PhysicalOp.Project(child) => stringColumnBase(child)
    case PhysicalOp.Pipeline(child, _) => stringColumnBase(child)
    case PhysicalOp.Filter(child, PhysicalOp.Slot.Element) => stringColumnBase(child)
    case _ => None

  @tailrec
  private def longColumnBase(op: PhysicalOp): Option[PhysicalOp] = op match
    case scan @ PhysicalOp.Scan(PhysicalOp.Kind.Longs, _) => Some(scan)
    case PhysicalOp.Project(child) => longColumnBase(child)
    case PhysicalOp.Pipeline(child, _) => longColumnBase(child)
    case PhysicalOp.Filter(child, PhysicalOp.Slot.Element) => longColumnBase(child)
    case _ => None

  @tailrec
  private def intColumnBase(op: PhysicalOp): Option[PhysicalOp] = op match
    case scan @ PhysicalOp.Scan(PhysicalOp.Kind.Ints, _) => Some(scan)
    case column @ PhysicalOp.Column(_, _) => Some(column)
    case PhysicalOp.Project(child) => intColumnBase(child)
    case PhysicalOp.Pipeline(child, _) => intColumnBase(child)
    case PhysicalOp.Filter(child, PhysicalOp.Slot.Element) => intColumnBase(child)
    case _ => None

  private def elementDepth(op: PhysicalOp): Int =
    depthFrom(op, 0)

  @tailrec
  private def depthFrom(op: PhysicalOp, seen: Int): Int = op match
    case PhysicalOp.Project(child) => depthFrom(child, seen + 1)
    case PhysicalOp.Pipeline(child, length) => depthFrom(child, seen + length)
    case PhysicalOp.Filter(child, PhysicalOp.Slot.Element) => depthFrom(child, seen + 1)
    case _ => seen
