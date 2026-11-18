package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.api.{JoinSumColumn, JoinSumProjection, LongSum}
import com.ewoodbury.scarlet.columnar.{ColumnBatch, ColumnHash, JoinProbeSum}
import com.ewoodbury.scarlet.core.{Partition, Plan}

/**
 * Lowers a keyed sum of one int join column into [[PhysicalOp.JoinProbeSum]].
 *
 * The logical plan stays `map` then `reduceByKey`. Only [[JoinSumProjection]] and [[LongSum]] name
 * the column and the addition. Any other map or reducer returns `None`, and so does a join that is
 * not an unhinted, shuffle-hash, or broadcast int inner join. Sort-merge stays on the row path.
 * Shuffle-hash exchanges both children. Broadcast does not: the left child is the build and the
 * right child is the probe, so [[JoinSumColumn.Left]] is still the left column. Execution reads
 * the projection again and updates [[com.ewoodbury.scarlet.columnar.HashAggregate]] from the
 * probe. The published pairs are the groups, not the match columns.
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
                Some(node(strategy, leftOp, rightOp, measureOf(projection.column)))
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

  /** Unhinted, shuffle-hash, and broadcast. Sort-merge stays on the row path. */
  private def admits(strategy: Option[Plan.JoinStrategy]): Boolean = strategy match
    case None | Some(Plan.JoinStrategy.ShuffleHash) | Some(Plan.JoinStrategy.Broadcast) => true
    case Some(Plan.JoinStrategy.SortMerge) => false

  /**
   * Shuffle-hash exchanges both children. Broadcast keeps them, so the left child stays the build
   * and [[PhysicalOp.ProbeMeasure.Left]] stays the left column.
   */
  private def node(
      strategy: Option[Plan.JoinStrategy],
      leftOp: PhysicalOp,
      rightOp: PhysicalOp,
      measure: PhysicalOp.ProbeMeasure,
  ): PhysicalOp = strategy match
    case Some(Plan.JoinStrategy.Broadcast) =>
      PhysicalOp.JoinProbeSum(leftOp, rightOp, measure)
    case None | Some(Plan.JoinStrategy.ShuffleHash) =>
      PhysicalOp.JoinProbeSum(
        PhysicalOp.Exchange(leftOp),
        PhysicalOp.Exchange(rightOp),
        measure,
      )
    case Some(Plan.JoinStrategy.SortMerge) =>
      throw new IllegalStateException("sort-merge join sum stays on the row path")

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
          PhysicalOp.JoinProbeSum(leftChild, rightChild, measure),
        ) =>
      val kernel = (measureOf(projection.column), measure) match
        case (PhysicalOp.ProbeMeasure.Left, PhysicalOp.ProbeMeasure.Left) =>
          JoinProbeSum.Measure.Left
        case (PhysicalOp.ProbeMeasure.Right, PhysicalOp.ProbeMeasure.Right) =>
          JoinProbeSum.Measure.Right
        case _ =>
          throw new IllegalStateException("join probe sum column does not match the plan")
      val ordinals = ColumnUse.ordinals(plan, op)
      strategy match
        case Some(Plan.JoinStrategy.Broadcast) =>
          JoinArms.broadcastSum(left, right, leftChild, rightChild, kernel, ordinals)
        case None | Some(Plan.JoinStrategy.ShuffleHash) =>
          (leftChild, rightChild) match
            case (PhysicalOp.Exchange(build), PhysicalOp.Exchange(probe)) =>
              JoinArms.sum(left, right, build, probe, kernel, ordinals)
            case _ =>
              throw new IllegalStateException("shuffle join sum exchanges both int sides")
        case Some(Plan.JoinStrategy.SortMerge) =>
          throw new IllegalStateException("join probe sum is a witnessed reduce over an int join")
    case _ =>
      throw new IllegalStateException("join probe sum is a witnessed reduce over an int join")
