package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{ColumnBatch, ColumnKernel}

/**
 * Boolean element filters.
 *
 * `Function1` has no `apply$mcZZ$sp`, so a `Boolean => Boolean` map cannot be distinguished from
 * `Boolean => Int` by its declared signature. This walker does not look at a project and does not
 * call it. Any project refuses the chain. Filters nest as [[PhysicalOp.Filter]] nodes, one per
 * step, including a single filter. The chain does not enter the element pipeline. A boolean column
 * there throws `IllegalStateException`, and [[ColumnarPlanner.tryRun]] catches only
 * `ClassCastException`.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.AsInstanceOf"))
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

  /**
   * One boolean filter. The cast is the erasure boundary: a function that is not
   * `Boolean => Boolean` throws `ClassCastException`, and [[ColumnarPlanner.tryRun]] returns
   * `None`.
   */
  def filter(batch: ColumnBatch, predicate: Any): ColumnBatch =
    filter(batch, 0, batch.length, predicate)

  /** One boolean filter over `[start, start + length)`. The cast is the same erasure boundary. */
  def filter(batch: ColumnBatch, start: Int, length: Int, predicate: Any): ColumnBatch =
    val keep = predicate.asInstanceOf[Boolean => Boolean]
    ColumnKernel.filterBool(batch, 0, start, length, value => keep(value))
