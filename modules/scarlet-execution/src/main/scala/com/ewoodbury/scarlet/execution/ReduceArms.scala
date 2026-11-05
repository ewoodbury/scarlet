package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.{HashReduce, LongHashReduce, StringHashReduce}
import com.ewoodbury.scarlet.core.Plan

import ColumnarInterpreter.{Columns, interpretAt, longPairsOf, pairsOf}

/**
 * `reduceByKey` for int pairs, long pairs, and string pairs.
 *
 * A two-filter key or value stack on an int or string child partial-reduces survivor ids. The
 * exchange sees dense partials. A single filter, a whole-row string filter, and every other child
 * stay on the dense reduce. The `None` path is the only one that interprets that child. The long
 * arm stays on the dense reduce.
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
    // A two-filter int-pair stack scans survivor ids. The `None` path is the only one that
    // interprets that child.
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
    Columns.LongPairs(
      LongHashReduce.byKey(
        longPairsOf(interpretAt(source, child, selectedPartition, preserveAbsence, ordinals)),
        ColumnarPlanner.shuffleWidth,
        ColumnarPlanner.parallelism,
        ColumnarPlanner.batchSize,
        (left, right) => op(left, right),
      ),
    )

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
        Columns.StringPairs(StringHashReduce.byKey(batches, width, workers, batchSize, op))
