package com.ewoodbury.scarlet.execution

import java.util.concurrent.atomic.AtomicLong

import com.ewoodbury.scarlet.core.{Partition, Plan}
import com.ewoodbury.scarlet.runtime.ScarletRuntime

/**
 * A row parent that keeps a columnar child.
 *
 * This is not a kernel and it does not build a [[PhysicalOp]]. Decode is
 * [[ColumnarPlanner.tryRun]] of the child. [[IllegalArgumentException]] is a failed decode,
 * including `null Int32 pair at row N`. A numeric array source has no null cell, so a successful
 * decode is kept without running that child again. A boxed source, a string source, or a join
 * still compares the decode with the row executor, in order. A disagreement is [[scala.None]], and
 * the caller runs the original plan on rows. This does not wrap that plan in another operator.
 * `flatMapValues` uses the same decode. Untyped `sortBy` sorts the kept partitions with the row
 * scheduler. Typed sorts stay on their kernels. `cogroup` decodes each child on its own and runs
 * the row cogroup. One disagreement runs the original plan on rows. A refused `map` or `mapValues`
 * uses this same decode. A plan [[ColumnarPlanner.tryRun]] accepts never reaches it.
 * [[MixedAction]] and [[MixedJoin]] stay on their own paths.
 *
 * [[childOracleRunsForTest]] counts the row-child compare. A numeric array child must not
 * increment it. A boxed child that still decodes must.
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

  private val childOracleRuns = new AtomicLong

  /** Times the row executor re-ran a decoded child. A numeric array child stays at zero. */
  private[execution] def childOracleRunsForTest: Long = childOracleRuns.get()

  /** Test hook. Production does not call this. */
  private[execution] def resetChildOracleRunsForTest(): Unit = childOracleRuns.set(0L)

  private def noteChildOracle(): Unit =
    val updated = childOracleRuns.incrementAndGet()
    if updated < 0L then throw new IllegalStateException("oracle count wrapped")

  /** `flatMap` over a child whose decoded partitions are kept. */
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
   * Decoded partitions.
   *
   * A numeric array child is kept. Anything else is kept only when it matches the row executor.
   * [[scala.None]] is a refused decode, a null pair the decoder rejects, or a different row.
   */
  private def matched[A](source: Plan[A]): Option[Seq[Partition[A]]] =
    decode(source) match
      case None => None
      case Some(decoded) if trusted(source) => Some(freeze(decoded))
      case Some(decoded) =>
        noteChildOracle()
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
        case Some(parts) if trusted(plan) => Child.Ready(freeze(parts))
        case Some(parts) =>
          noteChildOracle()
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

  /**
   * A child whose sources are numeric arrays and which is not a join.
   *
   * Those arrays have no null cell, so the decode cannot disagree by unboxing a null to `0`. A
   * string array can hold null. A boxed source is [[Plan.SourceDescriptor.Unknown]]. Both keep the
   * row compare. A join can publish an absent partner, so it keeps the compare too.
   */
  private def trusted(plan: Plan[?]): Boolean =
    plan match
      case Plan.Source(_, descriptor) => numeric(descriptor)
      case _: Plan.JoinOp[?, ?, ?] => false
      case _: Plan.WideJoinOp => false
      case _: Plan.WideGroupOp => false
      case _: Plan.LeftOuterJoinOp[?, ?, ?] => false
      case _: Plan.RightOuterJoinOp[?, ?, ?] => false
      case _: Plan.FullOuterJoinOp[?, ?, ?] => false
      case _: Plan.MapPartitionsOp[?, ?] => false
      case Plan.UnionOp(left, right) => trusted(left) && trusted(right)
      case Plan.CoGroupOp(left, right) => trusted(left) && trusted(right)
      case Plan.RepartitionOp(source, _) => trusted(source)
      case Plan.CoalesceOp(source, _) => trusted(source)
      case Plan.PartitionByOp(source, _) => trusted(source)
      case Plan.MapOp(source, _) => trusted(source)
      case Plan.FilterOp(source, _) => trusted(source)
      case Plan.FlatMapOp(source, _) => trusted(source)
      case Plan.DistinctOp(source) => trusted(source)
      case Plan.KeysOp(source) => trusted(source)
      case Plan.ValuesOp(source) => trusted(source)
      case Plan.MapValuesOp(source, _) => trusted(source)
      case Plan.MapIntKeyOp(source, _) => trusted(source)
      case Plan.FilterKeysOp(source, _) => trusted(source)
      case Plan.FilterValuesOp(source, _) => trusted(source)
      case Plan.FlatMapValuesOp(source, _) => trusted(source)
      case Plan.GroupByKeyOp(source) => trusted(source)
      case Plan.CountByKeyOp(source) => trusted(source)
      case Plan.CountSumByKeyOp(source) => trusted(source)
      case Plan.CountSumByLongKeyOp(source) => trusted(source)
      case Plan.CountSumByStringKeyOp(source) => trusted(source)
      case Plan.CountSumByIntLongKeyOp(source) => trusted(source)
      case Plan.ExtremumByKeyOp(source, _) => trusted(source)
      case Plan.WideFilterOp(source, _, _) => trusted(source)
      case Plan.WideProjectOp(source, _) => trusted(source)
      case Plan.WideComputeOp(source, _, _) => trusted(source)
      case Plan.ReduceByKeyOp(source, _) => trusted(source)
      case Plan.SortByOp(source, _, _) => trusted(source)
      case Plan.SortByIntKeyOp(source) => trusted(source)
      case Plan.SortByIntValueDescOp(source) => trusted(source)
      case Plan.SortByLongKeyOp(source) => trusted(source)
      case Plan.SortByStringKeyOp(source) => trusted(source)
      case Plan.SortByLongValueDescOp(source) => trusted(source)
      case Plan.SortByStringValueDescOp(source) => trusted(source)
      case Plan.SortByIntLongKeyOp(source) => trusted(source)
      case Plan.SortByIntLongValueDescOp(source) => trusted(source)

  private def numeric(descriptor: Plan.SourceDescriptor): Boolean =
    descriptor match
      case Plan.SourceDescriptor.Ints | Plan.SourceDescriptor.Longs |
          Plan.SourceDescriptor.Doubles | Plan.SourceDescriptor.Float32s |
          Plan.SourceDescriptor.Pairs | Plan.SourceDescriptor.Bools |
          Plan.SourceDescriptor.LongPairs =>
        true
      case Plan.SourceDescriptor.Unknown | Plan.SourceDescriptor.Strings |
          Plan.SourceDescriptor.Columns =>
        false

  private def runRow[A](plan: Plan[A]): Seq[Partition[A]] =
    val runtime = ScarletRuntime.get
    val scheduler = new DAGScheduler(runtime.shuffle, runtime.scheduler, runtime.partitioner)
    scheduler.executePartitions(plan)
