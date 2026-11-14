package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, longPairsOf, pairsOf}

/**
 * `reduceByKey` for int pairs, long pairs, and string pairs.
 *
 * A slot-filter stack on an int or string child partial-reduces survivor ids, including a stack of
 * one. A long key, value, or row stack folds those predicates into the partial table and does not
 * build the index. An int-pair row filter and a whole-row string filter still use the index. The
 * exchange sees dense partials. A map, a scan, and a non-pair child stay on the dense reduce. The
 * `None` path is the only one that interprets a child the fold did not already scan.
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
    // A long key, value, or row stack folds the predicates into the partial table. The index path
    // remains for a stack the fold does not admit. The `None` path is the only one that
    // interprets a child the fold did not already scan.
    val binOp: LongHashReduce.LongBinOp = (left: Long, right: Long) => op(left, right)
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val batchSize = ColumnarPlanner.batchSize
    val reduced = foldLong(
      source,
      child,
      binOp,
      selectedPartition,
      preserveAbsence,
      ordinals,
      width,
      workers,
      batchSize,
    ).getOrElse {
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
   * Scan the child under the filter stack once and fold `steps` into the partial table. `None`
   * when the stack is not a long key, value, or row selection. The caller then keeps the index
   * path, which interprets that child itself.
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
      val batches = longPairsOf(
        interpretAt(underPlan, underOp, selectedPartition, preserveAbsence, ordinals),
      )
      FoldedLongReduce.reduce(batches, steps, width, workers, batchSize, binOp)
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
