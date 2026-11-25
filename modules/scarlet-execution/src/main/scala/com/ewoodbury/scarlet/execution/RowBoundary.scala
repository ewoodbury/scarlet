package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.core.{Partition, Plan}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * A row parent that keeps a columnar child.
 *
 * This is not a kernel and it does not build a [[PhysicalOp]]. Decode is
 * [[ColumnarPlanner.tryRun]] of the child. [[IllegalArgumentException]] is a failed decode,
 * including `null Int32 pair at row N`. The decoded partitions must equal the row executor's
 * partitions of that same child, in order. Anything else is [[scala.None]], and the caller runs
 * the original plan on rows. This does not wrap that plan in another operator. `flatMapValues`
 * uses the same decode. [[MixedAction]] and [[MixedJoin]] stay on their own paths.
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
private[execution] object RowBoundary:

  /** `flatMap` over a child whose decoded partitions match the row executor. */
  def flatMap[A, B](source: Plan[A], f: A => IterableOnce[B]): Option[Seq[Partition[B]]] =
    matched(source).map(parts => parts.map(part => Partition(applyOnce(part.data, f))))

  /**
   * `flatMapValues` over a child whose decoded partitions match the row executor.
   *
   * The value function runs on the decoded pairs. This is the same decode as [[flatMap]].
   */
  def flatMapValues[K, V, B](
      source: Plan[(K, V)],
      f: V => IterableOnce[B],
  ): Option[Seq[Partition[(K, B)]]] =
    matched(source).map(parts => parts.map(part => Partition(applyValues(part.data, f))))

  /**
   * Decoded partitions when they are the row executor's partitions, in order.
   *
   * [[scala.None]] is a refused decode, a null pair the decoder rejects, or a different row.
   */
  private def matched[A](source: Plan[A]): Option[Seq[Partition[A]]] =
    decode(source) match
      case None => None
      case Some(decoded) =>
        val columns = freeze(decoded)
        val rows = freeze(runRow(source))
        if sameOrder(columns, rows) then Some(columns) else None

  /** [[scala.None]] when [[ColumnarPlanner.tryRun]] refuses the plan or rejects a null pair. */
  private def decode[A](plan: Plan[A]): Option[Seq[Partition[A]]] =
    try ColumnarPlanner.tryRun(plan)
    catch case _: IllegalArgumentException => None

  private def applyOnce[A, B](rows: Iterable[A], f: A => IterableOnce[B]): Vector[B] =
    rows.iterator.flatMap(row => f(row).iterator).toVector

  /**
   * One output pair per value the function yields. The key is the decoded key.
   *
   * This runs only after [[matched]] agreed with the row executor. The row stage extracts the pair
   * the same way.
   */
  private def applyValues[K, V, B](
      rows: Iterable[(K, V)],
      f: V => IterableOnce[B],
  ): Vector[(K, B)] =
    rows.iterator.flatMap { case (key, value) =>
      f(value).iterator.map(out => (key, out))
    }.toVector

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
