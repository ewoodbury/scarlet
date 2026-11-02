package com.ewoodbury.scarlet.execution

/**
 * Boolean element filters.
 *
 * `Function1` has no `apply$mcZZ$sp`, so a `Boolean => Boolean` map cannot be distinguished from
 * `Boolean => Int` by its declared signature. This walker does not look at a project and does not
 * call it. Any project refuses the chain. Filters nest as [[PhysicalOp.Filter]] nodes, one per
 * step, including a single filter. The chain does not enter the element pipeline. A boolean column
 * there throws `IllegalStateException`, and [[ColumnarPlanner.tryRun]] catches only
 * `ClassCastException`. The predicate cast lives on [[PredicatePlan]].
 */
private[execution] object BoolPipeline:

  def lower(
      child: PhysicalOp,
      steps: List[ColumnarPlanner.ElementStep],
  ): Option[PhysicalOp] =
    steps.foldLeft(Option(child)) { (current, step) =>
      step match
        case ColumnarPlanner.ElementStep.Filter(_) =>
          current.map(op => PhysicalOp.Filter(op, PhysicalOp.Slot.Element))
        case ColumnarPlanner.ElementStep.Project(_) => None
    }
