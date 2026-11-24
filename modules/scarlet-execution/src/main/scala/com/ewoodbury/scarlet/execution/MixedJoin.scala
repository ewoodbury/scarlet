package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.{Partition, Plan}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * A join whose children are not both lowered.
 *
 * Exactly one child lowers. That side is decoded and the join runs on the row executor. There is
 * no hash join of one columnar side and one row side. [[ColumnarPlanner.tryRun]] of a null `Int32`
 * pair throws `IllegalArgumentException`, and a lowered `filterKeys` can drop a null the row
 * executor keeps. Either of those results returns `None` here. The caller then runs the original
 * plan on rows. A present-key side still publishes its batches before the row join.
 *
 * This is not a kernel and it does not build a [[PhysicalOp]].
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Throw",
  ),
)
private[execution] object MixedJoin:

  def partitions[A](plan: Plan[A]): Option[Seq[Partition[A]]] =
    bound(plan) match
      case None => None
      case Some(side) =>
        val part = child(plan, side)
        decode(part) match
          case Some(decoded) if sameRows(decoded, part) =>
            Some(runRow(rewrite(plan, side, decoded)))
          case _ => None

  private enum Side:
    case Left, Right

  private def bound(plan: Plan[_]): Option[Side] = plan match
    case Plan.JoinOp(left, right, _) => one(left, right)
    case Plan.LeftOuterJoinOp(left, right, _) => one(left, right)
    case Plan.RightOuterJoinOp(left, right, _) => one(left, right)
    case Plan.FullOuterJoinOp(left, right, _) => one(left, right)
    case _ => None

  private def one(left: Plan[_], right: Plan[_]): Option[Side] =
    (ColumnarPlanner.lower(left).isDefined, ColumnarPlanner.lower(right).isDefined) match
      case (true, false) => Some(Side.Left)
      case (false, true) => Some(Side.Right)
      case _ => None

  private def child(plan: Plan[_], side: Side): Plan[_] = plan match
    case Plan.JoinOp(left, right, _) => picked(side, left, right)
    case Plan.LeftOuterJoinOp(left, right, _) => picked(side, left, right)
    case Plan.RightOuterJoinOp(left, right, _) => picked(side, left, right)
    case Plan.FullOuterJoinOp(left, right, _) => picked(side, left, right)
    case _ => throw new IllegalStateException("mixed join has no side")

  private def picked(side: Side, left: Plan[_], right: Plan[_]): Plan[_] = side match
    case Side.Left => left
    case Side.Right => right

  /** `None` when [[ColumnarPlanner.tryRun]] refuses the side or rejects a null `Int32` pair. */
  private def decode(plan: Plan[_]): Option[Seq[Partition[_]]] =
    try ColumnarPlanner.tryRun(plan)
    catch case _: IllegalArgumentException => None

  /**
   * True when the decoded multiset is the multiset the row executor produces.
   *
   * Named erasure boundary. `productElement` reads a null component without unboxing it, so a null
   * key is not the same row as key `0`. A string `"null"` is not a null cell.
   */
  private def sameRows(decoded: Seq[Partition[_]], plan: Plan[_]): Boolean =
    counts(decoded) == counts(rowPartitions(plan))

  private def counts(parts: Seq[Partition[_]]): Map[Cell, Int] =
    parts.iterator
      .flatMap(_.data.iterator)
      .map(showCell)
      .toSeq
      .groupBy(identity)
      .map((cell, rows) => cell -> rows.size)

  private enum Cell:
    case Null
    case Absent
    case Wrapped(inner: Cell)
    case Node(children: Vector[Cell])
    case Leaf(kind: String, text: String)

  private def showCell(cell: Any): Cell = cell match
    case null => Cell.Null
    case None => Cell.Absent
    case Some(value) => Cell.Wrapped(showCell(value))
    case product: Product =>
      val children = (0 until product.productArity).map { index =>
        showCell(product.productElement(index))
      }
      Cell.Node(children.toVector)
    case other =>
      val boxed = other.asInstanceOf[AnyRef]
      Cell.Leaf(boxed.getClass.getName, String.valueOf(other))

  /**
   * Named erasure boundary. The decoded partitions replace one `(K, V)` child. The row executor
   * reads those rows; the static join type is the caller's.
   */
  private def rewrite[A](plan: Plan[A], side: Side, parts: Seq[Partition[_]]): Plan[A] =
    val rows = parts.asInstanceOf[Seq[Partition[(Any, Any)]]]
    val source = Plan.Source(rows)
    val rewritten: Plan[_] = plan match
      case Plan.JoinOp(left, right, strategy) =>
        joined(side, source, left, right, Plan.JoinOp(_, _, strategy))
      case Plan.LeftOuterJoinOp(left, right, strategy) =>
        joined(side, source, left, right, Plan.LeftOuterJoinOp(_, _, strategy))
      case Plan.RightOuterJoinOp(left, right, strategy) =>
        joined(side, source, left, right, Plan.RightOuterJoinOp(_, _, strategy))
      case Plan.FullOuterJoinOp(left, right, strategy) =>
        joined(side, source, left, right, Plan.FullOuterJoinOp(_, _, strategy))
      case _ => throw new IllegalStateException("mixed join has no side")
    rewritten.asInstanceOf[Plan[A]]

  private def joined(
      side: Side,
      source: Plan[(Any, Any)],
      left: Plan[_],
      right: Plan[_],
      build: (Plan[(Any, Any)], Plan[(Any, Any)]) => Plan[_],
  ): Plan[_] =
    val other = (if side == Side.Left then right else left).asInstanceOf[Plan[(Any, Any)]]
    if side == Side.Left then build(source, other) else build(other, source)

  private def rowPartitions(plan: Plan[_]): Seq[Partition[_]] = runRow(plan)

  private def runRow[A](plan: Plan[A]): Seq[Partition[A]] =
    val runtime = ScarletRuntime.get
    val scheduler = new DAGScheduler(runtime.shuffle, runtime.scheduler, runtime.partitioner)
    scheduler.executePartitions(plan)
