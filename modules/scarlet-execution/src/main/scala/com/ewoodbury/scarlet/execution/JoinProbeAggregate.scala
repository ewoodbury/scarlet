package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.{JoinSumColumn, JoinSumProjection, LongSum}
import com.ewoodbury.scarlet.columnar.{ColumnBatch, ColumnHash, JoinProbeSum}
import com.ewoodbury.scarlet.core.{Partition, Plan}

/**
 * Lowers a keyed sum of one int join column into [[PhysicalOp.JoinProbeSum]].
 *
 * The logical plan stays `map` then `reduceByKey`. Only [[JoinSumProjection]] and [[LongSum]] name
 * the column and the addition. Any other map or reducer returns `None`, and so does a join that is
 * not an int inner join with no hint, a shuffle-hash hint, or an explicit sort-merge hint.
 * Sort-merge emits [[PhysicalOp.JoinProbeSum]] over two hash exchanges. It does not sort and it
 * does not emit [[PhysicalOp.SortMergeJoin]]. The left child stays the build, so
 * [[PhysicalOp.ProbeMeasure.Left]] is still the left column. Broadcast stays on the row path. A
 * [[PhysicalOp.MapKey]] on either arm is admitted. Its kind stays [[PhysicalOp.Kind.Pairs]]. The
 * exchange drops a null key, the function is not called on that key, and the key is not a probe
 * hit. The function is not inspected. A filter, `keys`, or `values` above the map still declines
 * in those parents. Execution reads the projection again and updates
 * [[com.ewoodbury.scarlet.columnar.HashAggregate]] from the probe. The published pairs are the
 * groups, not the match columns.
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
              if admits(strategy) =>
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
      Partition[Any](decodeSums(batch))
    }

  /** Number of groups. This does not decode the pairs. */
  def count(plan: Plan[_], op: PhysicalOp): Long =
    batches(plan, op).foldLeft(0L)((total, batch) => total + batch.length.toLong)

  /**
   * Groups in one sum batch. Every row must be present in both columns, and the columns must have
   * the same length. A hole is a bad batch, not a zero pair.
   */
  private[execution] def decodeSums(batch: ColumnBatch): Vector[(Int, Long)] =
    val keys = ColumnHash.int32At(batch, 0)
    val values = ColumnHash.int64At(batch, 1)
    require(keys.length == values.length, "key and value columns differ in length")
    val rows = keys.length
    Vector.tabulate(rows) { row =>
      require(keys.isValid(row) && values.isValid(row), s"null sum pair at row $row")
      (keys.values(row), values.values(row))
    }

  /**
   * No hint, shuffle-hash, or an explicit sort-merge. Broadcast is a different physical join.
   * Sort-merge is admitted here only: both sides are hash-partitioned, and the merge is not run.
   */
  private def admits(strategy: Option[Plan.JoinStrategy]): Boolean =
    strategy match
      case None | Some(Plan.JoinStrategy.ShuffleHash) | Some(Plan.JoinStrategy.SortMerge) =>
        true
      case Some(Plan.JoinStrategy.Broadcast) => false

  /**
   * Int pairs, including a key map. This sum does not publish a null key, so it does not consult
   * [[MapIntKey.inTree]]. A parent that would publish that key still declines.
   */
  private def accepts(op: PhysicalOp): Boolean = op.kind match
    case PhysicalOp.Kind.Pairs => true
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
        ) if admits(strategy) =>
      val kernel = (measureOf(projection.column), measure) match
        case (PhysicalOp.ProbeMeasure.Left, PhysicalOp.ProbeMeasure.Left) =>
          JoinProbeSum.Measure.Left
        case (PhysicalOp.ProbeMeasure.Right, PhysicalOp.ProbeMeasure.Right) =>
          JoinProbeSum.Measure.Right
        case _ =>
          throw new IllegalStateException("join probe sum column does not match the plan")
      JoinArms.sum(left, right, leftOp, rightOp, kernel, ColumnUse.ordinals(plan, op))
    case _ =>
      throw new IllegalStateException("join probe sum is a witnessed reduce over an int join")
