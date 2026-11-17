package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.{JoinSumColumn, JoinSumProjection, LongSum}
import com.ewoodbury.scarlet.columnar.{ColumnBatch, ColumnHash, JoinProbeSum}
import com.ewoodbury.scarlet.core.{Partition, Plan}

/**
 * Lowers a keyed sum of one int join column into [[PhysicalOp.JoinProbeSum]].
 *
 * The logical plan stays `map` then `reduceByKey`. Only [[JoinSumProjection]] and [[LongSum]] name
 * the column and the addition. Any other map or reducer returns `None`, and so does a join that is
 * not an unhinted or shuffle-hash int inner join. Execution reads the projection again and updates
 * [[com.ewoodbury.scarlet.columnar.HashAggregate]] from the probe.
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Throw"))
private[execution] object JoinProbeAggregate:

  def lower(
      source: Plan[_],
      reduce: Any,
      lowerChild: Plan[_] => Option[PhysicalOp],
  ): Option[PhysicalOp] =
    reduce match
      case LongSum =>
        source match
          case Plan.MapOp(Plan.JoinOp(left, right, strategy), projection: JoinSumProjection[?])
              if JoinLowering.acceptsShuffleHash(strategy) =>
            (lowerChild(left), lowerChild(right)) match
              case (Some(leftOp), Some(rightOp)) if accepts(leftOp) && accepts(rightOp) =>
                Some(
                  PhysicalOp.JoinProbeSum(
                    PhysicalOp.Exchange(leftOp),
                    PhysicalOp.Exchange(rightOp),
                    measureOf(projection.column),
                  ),
                )
              case _ => None
          case _ => None
      case _ => None

  def partitions(plan: Plan[_], op: PhysicalOp): Seq[Partition[Any]] =
    batches(plan, op).map { batch =>
      Partition[Any](decode(batch))
    }

  /** Number of groups. This does not decode the pairs. */
  def count(plan: Plan[_], op: PhysicalOp): Long =
    batches(plan, op).foldLeft(0L)((total, batch) => total + batch.length.toLong)

  private def accepts(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Pairs => !MapIntKey.inTree(op)
    case _ => false

  private def measureOf(column: JoinSumColumn): PhysicalOp.ProbeMeasure = column match
    case JoinSumColumn.Left => PhysicalOp.ProbeMeasure.Left
    case JoinSumColumn.Right => PhysicalOp.ProbeMeasure.Right

  private def batches(plan: Plan[_], op: PhysicalOp): Vector[ColumnBatch] = (plan, op) match
    case (
          Plan.ReduceByKeyOp(
            Plan.MapOp(Plan.JoinOp(left, right, strategy), projection: JoinSumProjection[?]),
            LongSum,
          ),
          PhysicalOp.JoinProbeSum(
            PhysicalOp.Exchange(leftOp),
            PhysicalOp.Exchange(rightOp),
            measure,
          ),
        ) if JoinLowering.acceptsShuffleHash(strategy) =>
      val column = measureOf(projection.column)
      val agreed = (column, measure) match
        case (PhysicalOp.ProbeMeasure.Left, PhysicalOp.ProbeMeasure.Left) => true
        case (PhysicalOp.ProbeMeasure.Right, PhysicalOp.ProbeMeasure.Right) => true
        case _ => false
      if (!agreed) then
        throw new IllegalStateException("join probe sum column does not match the plan")
      val kernel = measure match
        case PhysicalOp.ProbeMeasure.Left => JoinProbeSum.Measure.Left
        case PhysicalOp.ProbeMeasure.Right => JoinProbeSum.Measure.Right
      JoinArms.sum(left, right, leftOp, rightOp, kernel, ColumnUse.ordinals(plan, op))
    case _ =>
      throw new IllegalStateException("join probe sum is a witnessed reduce over an int join")

  private def decode(batch: ColumnBatch): Vector[(Int, Long)] =
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int64At(batch, 1)
    val rows = keys.length
    Vector.tabulate(rows)(row => (keys.values(row), values.values(row)))
