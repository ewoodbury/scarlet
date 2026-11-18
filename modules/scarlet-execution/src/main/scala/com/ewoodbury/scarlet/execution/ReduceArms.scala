package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, longPairsOf, pairsOf}

/**
 * `reduceByKey` for int pairs, long pairs, and string pairs.
 *
 * A slot-filter stack on an int or string child partial-reduces survivor ids, including a stack of
 * one. A long key, value, or row stack folds those predicates into the partial table and does not
 * build the index. An owned long-pair source is read in place. A boxed source is still encoded,
 * which is what keeps a null key. An int-pair row filter and a whole-row string filter still use
 * the index. The exchange sees dense partials. A long-pair map is reduced from the project's input
 * morsels: the projected key and value go into the long hash table, and the pair batch is not
 * published. An int map, a scan, and a non-pair child stay on the dense reduce. The `None` path is
 * the only one that interprets a child the project fold and the filter fold did not already scan.
 */
private[execution] object ReduceArms:

  def intByKey[V](
      source: Plan[_],
      child: PhysicalOp,
      reduceFunc: (V, V) => V,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    // Named erasure boundary. Grouped pair reduction declares `(Int, Int) => Int`.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val op = reduceFunc.asInstanceOf[(Int, Int) => Int]
    // A slot-filter stack scans survivor ids, including one filter. An int-pair row filter does
    // the same. The `None` path is the only one that interprets that child.
    val binOp: HashReduce.IntBinOp = (left: Int, right: Int) => op(left, right)
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    val reduced = FilterChain.selected(
      source,
      child,
      selectedPartition,
      preserveAbsence,
      ordinals,
    ) match
      case Some(parts) =>
        HashReduce.byKeySelected(parts, width, workers, batchSize, binOp)
      case None =>
        HashReduce.byKey(
          pairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)),
          width,
          workers,
          batchSize,
          binOp,
        )
    Columns.Pairs(reduced)

  def longByKey[V](
      source: Plan[_],
      child: PhysicalOp,
      reduceFunc: (V, V) => V,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    // Named erasure boundary. Grouped long-pair reduction declares `(Long, Long) => Long`.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val op = reduceFunc.asInstanceOf[(Long, Long) => Long]
    // A long-pair project inserts each projected key and value into the partial table and does not
    // publish the pair batch. A long key, value, or row stack folds the predicates into that
    // table. The index path remains for a stack the fold does not admit. The `None` path is the
    // only one that interprets a child those folds did not already scan. The same plan function
    // keeps one adapter, so a retained partial table accepts the next collect.
    val binOp = longBinOp(op)
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    val reduced = reduceProjected(
      source,
      child,
      binOp,
      selectedPartition,
      preserveAbsence,
      ordinals,
      width,
      workers,
      batchSize,
    ).orElse {
      foldLong(
        source,
        child,
        binOp,
        selectedPartition,
        preserveAbsence,
        ordinals,
        width,
        workers,
        batchSize,
      )
    }.getOrElse {
      FilterChain.selected(
        source,
        child,
        selectedPartition,
        preserveAbsence,
        ordinals,
      ) match
        case Some(parts) =>
          LongHashReduce.byKeySelected(parts, width, workers, batchSize, binOp)
        case None =>
          LongHashReduce.byKey(
            longPairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)),
            width,
            workers,
            batchSize,
            binOp,
          )
    }
    Columns.LongPairs(reduced)

  /**
   * One long-pair [[Plan.MapOp]]. The input of the project is interpreted, then each morsel is
   * inserted through [[LongPairProject]] into [[LongHashReduce]]. A null projected key is dropped.
   * A null projected value stays missing until another value arrives. The pair batch is not
   * published and [[LongHashReduce.byKey]] is not called. The function body is not read. `None`
   * when this child is not that project.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def reduceProjected(
      source: Plan[_],
      child: PhysicalOp,
      binOp: LongHashReduce.LongBinOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    longPairProject(source, child).map { (inner, under, fn) =>
      val adapted = LongPairProject.adapt(fn)
      val columns =
        try interpretAt(inner, under, selectedPartition, preserveAbsence, ordinals)
        catch
          case _: NullPointerException =>
            throw new ColumnarFallback("reduceByKey source has a null pair component")
      columns match
        case Columns.LongPairs(batches) =>
          ProjectedLongPairReduce.reduce(batches, adapted, width, workers, batchSize, binOp)
        case _ =>
          throw new IllegalStateException("pair project reduce expected long pairs")
    }

  /** The plan under a long-pair project and the function the interpreter would adapt. */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def longPairProject(
      source: Plan[_],
      child: PhysicalOp,
  ): Option[(Plan[_], PhysicalOp, Any)] =
    (source, child) match
      case (Plan.MapOp(inner, fn), PhysicalOp.Project(under)) => Some((inner, under, fn))
      case _ => None

  /**
   * Scan the child under the filter stack once and fold `steps` into the partial table. `None`
   * when the stack is not a long key, value, or row selection. The caller then keeps the index
   * path, which interprets that child itself. An owned long-pair scan is not encoded first.
   */
  private def foldLong(
      source: Plan[_],
      child: PhysicalOp,
      binOp: LongHashReduce.LongBinOp,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
      width: Int,
      workers: Int,
      batchSize: Int,
  ): Option[Vector[ColumnBatch]] =
    longSteps(source, child, ordinals).map { steps =>
      val (underPlan, underOp) = PredicatePlan.childOf(source, child)
      val batches = sharedLongPairs(underPlan, underOp, selectedPartition, ordinals).getOrElse(
        longPairsOf(
          interpretAt(underPlan, underOp, selectedPartition, preserveAbsence, ordinals),
        ),
      )
      FoldedLongReduce.reduce(batches, steps, width, workers, batchSize, binOp)
    }

  /**
   * Owned long-pair partitions for an in-place fold. `None` when the child is not that scan, a
   * selector asked for one partition, or the demand is a single column. A boxed partition stays on
   * the encoder.
   */
  private def sharedLongPairs(
      plan: Plan[_],
      op: PhysicalOp,
      selectedPartition: Option[Int],
      ordinals: Set[Int],
  ): Option[Vector[ColumnBatch]] =
    if (selectedPartition.nonEmpty) then None
    else if (ordinals.size == 1 && (ordinals.contains(0) || ordinals.contains(1))) then None
    else
      (plan, op) match
        case (Plan.Source(partitions, _), PhysicalOp.Scan(PhysicalOp.Kind.LongPairs, _)) =>
          SourceEncode.shareOwnedLongPairs(partitions)
        case _ => None

  /**
   * One adapter per plan function. A fresh lambda on every collect would miss the retained table,
   * because reuse requires the same operator instance. The plan holds `op`, so the weak key dies
   * with the plan. The adapter does not keep the key alive.
   */
  private val longOpLock = new Object
  private val longOps =
    new java.util.WeakHashMap[(Long, Long) => Long, LongHashReduce.LongBinOp]

  private def longBinOp(op: (Long, Long) => Long): LongHashReduce.LongBinOp =
    longOpLock.synchronized {
      Option(longOps.get(op)).getOrElse {
        val created: LongHashReduce.LongBinOp = (left, right) => op(left, right)
        longOps.put(op, created)
        created
      }
    }

  /** Long selection steps, in source order. Empty and non-long stacks are `None`. */
  private def longSteps(
      source: Plan[_],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): Option[List[Selection.Step]] =
    PredicatePlan.compile(source, child).flatMap { compiled =>
      PredicatePlan
        .selectionSteps(PredicatePlan.encoded(compiled, ordinals))
        .map(_.toList)
        .filter(FoldedLongReduce.admits)
    }

  def stringByKey[V](
      source: Plan[_],
      child: PhysicalOp,
      reduceFunc: (V, V) => V,
      selectedPartition: Option[Int],
      preserveAbsence: Boolean,
      ordinals: Set[Int],
  ): Columns =
    // Boundary guard: take refuses an aggregate, so a selector never reaches this arm.
    require(selectedPartition.isEmpty, "a partition selector cannot run an aggregate")
    // Named erasure boundary. Grouped string-pair reduction declares `(Int, Int) => Int`.
    @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
    val op = reduceFunc.asInstanceOf[(Int, Int) => Int]
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    FilterChain.selected(
      source,
      child,
      selectedPartition,
      preserveAbsence,
      ordinals,
    ) match
      case Some(parts) =>
        StringPairWalk.refuseSelectedNullValues(parts)
        Columns.StringPairs(StringHashReduce.byKeySelected(parts, width, workers, batchSize, op))
      case None =>
        val batches = StringPairWalk.batches(
          interpretAt(
            source,
            child,
            selectedPartition,
            preserveAbsence,
            ordinals,
          ),
        )
        StringPairWalk.refuseNullValues(batches)
        Columns.StringPairs(StringHashReduce.byKey(batches, width, workers, batchSize, op))
