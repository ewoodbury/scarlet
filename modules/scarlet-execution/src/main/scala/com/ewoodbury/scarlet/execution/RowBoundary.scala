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
 * uses the same decode. Untyped `sortBy` sorts those partitions with the row scheduler. Typed
 * sorts stay on their kernels. `cogroup` decodes each child on its own and runs the row cogroup.
 * One disagreement runs the original plan on rows. A refused `map` or `mapValues` uses this same
 * decode. A plan [[ColumnarPlanner.tryRun]] accepts never reaches it. [[MixedAction]] and
 * [[MixedJoin]] stay on their own paths.
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
   * Untyped `sortBy` over a child whose decoded partitions match the row executor.
   *
   * The row scheduler sorts a source of those partitions, so the order is the row `sortBy` order.
   * This does not call [[SortLowering]] and it does not add a sort kernel.
   */
  def sortBy[A, B](
      source: Plan[A],
      keyFunc: A => B,
      ordering: Ordering[B],
  ): Option[Seq[Partition[A]]] =
    matched(source).map { parts =>
      runRow(Plan.SortByOp(Plan.Source(parts), keyFunc, ordering))
    }

  /**
   * `map` over a child whose decoded partitions match the row executor.
   *
   * The caller asks [[ColumnarPlanner.tryRun]] on the whole map first. This runs only when that
   * returns [[scala.None]].
   */
  def map[A, B](source: Plan[A], f: A => B): Option[Seq[Partition[B]]] =
    matched(source).map(parts => parts.map(part => Partition(applyMap(part.data, f))))

  /**
   * `mapValues` over a child whose decoded partitions match the row executor.
   *
   * The value function runs on the decoded pairs. This is the same decode as [[map]]. The caller
   * asks [[ColumnarPlanner.tryRun]] on the whole map first.
   */
  def mapValues[K, V, B](
      source: Plan[(K, V)],
      f: V => B,
  ): Option[Seq[Partition[(K, B)]]] =
    matched(source).map(parts => parts.map(part => Partition(applyMapValues(part.data, f))))

  /**
   * `cogroup` over children decoded on their own.
   *
   * A child that matches becomes a source of those partitions. A child that does not lower stays
   * that plan, and the row cogroup executes it. A null mismatch, or a decode that throws, is
   * [[scala.None]] for the whole cogroup. This is not a join.
   */
  def cogroup[K, V, W](
      left: Plan[(K, V)],
      right: Plan[(K, W)],
  ): Option[Seq[Partition[(K, (Iterable[V], Iterable[W]))]]] =
    val leftSide = childOf(left)
    val rightSide = childOf(right)
    (leftSide, rightSide) match
      case (Child.Disagree(), _) | (_, Child.Disagree()) => None
      case _ if !ready(leftSide) && !ready(rightSide) => None
      case _ =>
        Some(runRow(Plan.CoGroupOp(asPlan(leftSide), asPlan(rightSide))))

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

  /** One cogroup child. [[Child.Disagree]] refuses the whole cogroup. */
  private enum Child[A]:
    case Ready(parts: Seq[Partition[A]])
    case Kept(plan: Plan[A])
    case Disagree[B]() extends Child[B]

  /**
   * A matching decode, the original plan when the child does not lower, or a disagreement.
   *
   * [[IllegalArgumentException]] is a disagreement, not a row child. A refused `tryRun` keeps the
   * plan.
   */
  private def childOf[A](plan: Plan[A]): Child[A] =
    try
      ColumnarPlanner.tryRun(plan) match
        case None => Child.Kept(plan)
        case Some(parts) =>
          val columns = freeze(parts)
          val rows = freeze(runRow(plan))
          if sameOrder(columns, rows) then Child.Ready(columns) else Child.Disagree()
    catch case _: IllegalArgumentException => Child.Disagree()

  private def ready[A](side: Child[A]): Boolean = side match
    case Child.Ready(_) => true
    case Child.Kept(_) | Child.Disagree() => false

  private def asPlan[A](side: Child[A]): Plan[A] = side match
    case Child.Ready(parts) => Plan.Source(parts)
    case Child.Kept(plan) => plan
    case Child.Disagree() =>
      throw new IllegalStateException("a disagreement does not build a cogroup child")

  private def applyOnce[A, B](rows: Iterable[A], f: A => IterableOnce[B]): Vector[B] =
    rows.iterator.flatMap(row => f(row).iterator).toVector

  /** One output per decoded row. This runs only after [[matched]] agreed with the row executor. */
  private def applyMap[A, B](rows: Iterable[A], f: A => B): Vector[B] =
    rows.iterator.map(f).toVector

  /**
   * One output pair per decoded pair. The key is the decoded key.
   *
   * This runs only after [[matched]] agreed with the row executor. The row stage pairs the key and
   * the mapped value the same way.
   */
  private def applyMapValues[K, V, B](rows: Iterable[(K, V)], f: V => B): Vector[(K, B)] =
    rows.iterator.map { case (key, value) => (key, f(value)) }.toVector

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
