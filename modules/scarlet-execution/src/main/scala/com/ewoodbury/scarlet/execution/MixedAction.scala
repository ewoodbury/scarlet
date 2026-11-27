package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.{Partition, Plan}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * Partitions for a `mapPartitions` child, and for `take` and `count` after the columnar actions
 * miss.
 *
 * The original plan is split here. `take` does not wrap that plan in `MapPartitionsOp` first: the
 * wrapper does not lower, and a row run of it publishes no child batch. A fully bound sort stays
 * on [[ColumnarPlanner.tryTake]]. A fully bound count stays on [[ColumnarPlanner.tryCount]].
 *
 * A bare source is already the stored rows. It is not decoded. [[ColumnarPlanner.tryRun]] of a
 * null `Int32` pair throws `IllegalArgumentException`, and a lowered `filterKeys` or
 * `filterValues` drops a null the row predicate unboxes to `0` and keeps. A non-source child is
 * kept only when every decoded partition equals the row executor's partition, in order. A refused
 * decode, a rejected null pair, or a different partition runs on rows. A decode that ran has
 * already published its batches. [[MixedJoin]] still owns a join. This is not a kernel and it does
 * not build a [[PhysicalOp]].
 */
@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Product",
  ),
)
private[execution] object MixedAction:

  /** Output partitions of `plan` when `tryTake` or `tryCount` has already missed. */
  def partitions[A](plan: Plan[A]): Seq[Partition[A]] = plan match
    case source: Plan.Source[A] => source.partitions
    case Plan.MapPartitionsOp(inner, f) => tail(inner, f)
    case join: Plan.WideJoinOp =>
      freeze(
        ColumnarPlanner
          .tryRun(join)
          .getOrElse(WideJoinRows.partitions(join))
          .asInstanceOf[Seq[Partition[A]]],
      )
    case other =>
      freeze(MixedJoin.partitions(other).getOrElse(runRow(other)))

  /**
   * Child partitions under one `mapPartitions`. A nested tail is split again. A source is not
   * passed to [[ColumnarPlanner.tryRun]].
   */
  def tail[A, B](source: Plan[A], f: Iterator[A] => Iterator[B]): Seq[Partition[B]] =
    MixedTail.partitions(child(source), f)

  private def child[A](plan: Plan[A]): Seq[Partition[A]] = plan match
    case source: Plan.Source[A] => source.partitions
    case Plan.MapPartitionsOp(inner, f) => tail(inner, f)
    case other => prefer(other)

  /**
   * Columnar rows when they are the row executor's rows in partition order. A refused decode, a
   * null pair the decoder rejects, or a different prefix runs on rows instead.
   */
  private def prefer[A](plan: Plan[A]): Seq[Partition[A]] =
    decode(plan) match
      case None =>
        freeze(MixedJoin.partitions(plan).getOrElse(runRow(plan)))
      case Some(decoded) =>
        val columns = freeze(decoded)
        val rows = freeze(runRow(plan))
        if sameOrder(columns, rows) then columns else rows

  /** `None` when [[ColumnarPlanner.tryRun]] refuses the plan or rejects a null `Int32` pair. */
  private def decode[A](plan: Plan[A]): Option[Seq[Partition[A]]] =
    try ColumnarPlanner.tryRun(plan)
    catch case _: IllegalArgumentException => None

  private def sameOrder(decoded: Seq[Partition[_]], rows: Seq[Partition[_]]): Boolean =
    decoded.lengthCompare(rows.length) == 0 && decoded.zip(rows).forall { (left, right) =>
      cells(left.data) == cells(right.data)
    }

  private def cells(rows: Iterable[_]): Vector[Cell] =
    rows.iterator.map(showCell).toVector

  private def freeze[A](parts: Seq[Partition[A]]): Seq[Partition[A]] =
    parts.map(part => Partition(part.data.iterator.toVector))

  private enum Cell:
    case Null
    case Absent
    case Wrapped(inner: Cell)
    case Node(children: Vector[Cell])
    case Leaf(kind: String, text: String)

  /**
   * One cell, with a null kept distinct from `0`.
   *
   * Named erasure boundary. `productElement` reads a null component without unboxing it. A string
   * `"null"` is not a null cell.
   */
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

  private def runRow[A](plan: Plan[A]): Seq[Partition[A]] =
    val runtime = ScarletRuntime.get
    val scheduler = new DAGScheduler(runtime.shuffle, runtime.scheduler, runtime.partitioner)
    scheduler.executePartitions(plan)
