package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.execution.schema.ColumnBinding.Operator
import com.ewoodbury.scarlet.execution.schema.{ColumnBinding, KernelChoice}

/**
 * Same-type element `map` asks [[ColumnBinding]] on the child layout.
 *
 * One project builds [[PhysicalOp.Project]] when the tag is [[KernelChoice.Project]]. A longer
 * ordinary group builds [[PhysicalOp.Pipeline]] when the tag is [[KernelChoice.Pipeline]]. A
 * binder [[scala.None]] is [[scala.None]]. The witness is [[FunctionWitness.declaredElement]]:
 * `apply$mcII$sp`, `apply$mcJJ$sp`, `apply$mcDD$sp`, or `apply(String): String`. A function that
 * fails that witness is [[scala.None]] and does not ask. A boolean map has no specialized apply,
 * so it stays [[scala.None]]. Pair projects are not this decision.
 *
 * One filter step does not ask here. A group of two or more ordinary steps asks for
 * [[KernelChoice.Pipeline]], including a group that is only filters. [[FilterBinding.element]]
 * still gates a filter. The grouping is the one the element pipeline already used: four ordinary
 * steps, and a dictionary string filter splits out as its own [[PhysicalOp.Filter]].
 */
private[execution] object ProjectBinding:

  /** Element chain for an int, long, double, or string child. Other kinds are [[scala.None]]. */
  def chain(child: PhysicalOp, steps: List[ColumnarPlanner.ElementStep]): Option[PhysicalOp] =
    if (!elementKind(child.kind) || !projectsMatch(child, steps)) then None
    else if (!FilterBinding.element(child, steps)) then None
    else assemble(child, steps)

  /**
   * One [[PhysicalOp.Project]]. The tag has to be [[KernelChoice.Project]].
   *
   * [[Layout.AssumedIntLong]] is [[scala.None]]. A kind match that builds the class anyway fails
   * this, and a kind match that never asks leaves [[ColumnBinding.chooseCallsForTest]] at zero.
   */
  def project(child: PhysicalOp): Option[PhysicalOp] =
    tagged(child, Operator.Project, KernelChoice.Project, PhysicalOp.Project(child))

  /**
   * One [[PhysicalOp.Pipeline]] of `length` steps. The tag has to be [[KernelChoice.Pipeline]].
   */
  def pipeline(child: PhysicalOp, length: Int): Option[PhysicalOp] =
    tagged(
      child,
      Operator.Pipeline,
      KernelChoice.Pipeline,
      PhysicalOp.Pipeline(child, length),
    )

  private def tagged(
      child: PhysicalOp,
      operator: Operator,
      tag: KernelChoice,
      built: PhysicalOp,
  ): Option[PhysicalOp] =
    if (ColumnBinding.asked(child, operator).contains(tag)) then Some(built) else None

  private def elementKind(kind: PhysicalOp.Kind): Boolean = kind match
    case PhysicalOp.Kind.Ints | PhysicalOp.Kind.Longs | PhysicalOp.Kind.Floats |
        PhysicalOp.Kind.Strings =>
      true
    case PhysicalOp.Kind.Bools | PhysicalOp.Kind.Pairs | PhysicalOp.Kind.Triples |
        PhysicalOp.Kind.LongPairs | PhysicalOp.Kind.StringPairs | PhysicalOp.Kind.LongCounts |
        PhysicalOp.Kind.LongTriples | PhysicalOp.Kind.StringTriples | PhysicalOp.Kind.GroupedInts |
        PhysicalOp.Kind.GroupedStrings | PhysicalOp.Kind.GroupedLongs | PhysicalOp.Kind.LongOuter |
        PhysicalOp.Kind.StringOuter | PhysicalOp.Kind.CountSums | PhysicalOp.Kind.LongCountSums |
        PhysicalOp.Kind.StringCountSums | PhysicalOp.Kind.IntLongSums |
        PhysicalOp.Kind.GroupedIntLongs =>
      false

  /**
   * True when every project declares the signature of the child kind. A filter does not change the
   * element type. One failing project refuses the chain. This does not call the functions.
   */
  private def projectsMatch(child: PhysicalOp, steps: List[ColumnarPlanner.ElementStep]): Boolean =
    steps.forall {
      case ColumnarPlanner.ElementStep.Filter(_) => true
      case ColumnarPlanner.ElementStep.Project(mapper) =>
        FunctionWitness.declaredElement(child.kind, mapper)
    }

  private def assemble(
      child: PhysicalOp,
      steps: List[ColumnarPlanner.ElementStep],
  ): Option[PhysicalOp] =
    def lower(
        current: PhysicalOp,
        remaining: List[ColumnarPlanner.ElementStep],
    ): Option[PhysicalOp] =
      val (ordinary, rest) = remaining.span {
        case ColumnarPlanner.ElementStep.Filter(predicate) =>
          child.kind match
            case PhysicalOp.Kind.Strings =>
              ColumnarPlanner.dictionaryStringLiterals(predicate).isEmpty
            case _ => true
        case ColumnarPlanner.ElementStep.Project(_) => true
      }
      val chunked = ordinary.grouped(4).foldLeft(Option(current)) { (parent, group) =>
        parent.flatMap { op =>
          group match
            case List(ColumnarPlanner.ElementStep.Filter(_)) =>
              Some(PhysicalOp.Filter(op, PhysicalOp.Slot.Element))
            case List(ColumnarPlanner.ElementStep.Project(_)) =>
              project(op)
            case _ =>
              pipeline(op, group.length)
        }
      }
      chunked.flatMap { built =>
        rest match
          case ColumnarPlanner.ElementStep.Filter(_) :: tail =>
            lower(PhysicalOp.Filter(built, PhysicalOp.Slot.Element), tail)
          case Nil => Some(built)
          case _ =>
            throw new IllegalStateException("only a dictionary string filter splits a pipeline")
      }

    lower(child, steps)
