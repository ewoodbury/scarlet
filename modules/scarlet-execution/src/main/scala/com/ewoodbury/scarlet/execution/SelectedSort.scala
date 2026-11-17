package com.ewoodbury.scarlet.execution

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}

/**
 * Selected rank split for an int or long key sort.
 *
 * [[FilterChain.selected]] keeps the survivor index and the source batches. The in-memory path
 * copies that prefix, then drops the [[scala.Option]] that owns the selections, and only then
 * allocates the radix scratch. A named local that still pointed at the selections would keep those
 * batches alive through the scratch and the merge. Spill still reads the selections: that path
 * writes one run at a time and does not hold every radix scratch at once.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[execution] object SelectedSort:

  /**
   * Int `sortByIntKey` over a filter selection or a scan. A null key orders as zero and stays
   * null. A null value stays null. A slot-filter stack sorts survivor ids and does not gather
   * before this split. The `None` path interprets the child and sorts those batches. Over the
   * budget, with spill on, the survivor index is written and dropped rows are not.
   */
  def intKey(
      source: Plan[(Int, Int)],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): ColumnarInterpreter.Columns =
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    val scanStarted = System.nanoTime()
    var selectedOpt = FilterChain.selected(
      source,
      child,
      selectedPartition = None,
      preserveAbsence = true,
      ordinals,
    )
    val sorted =
      if selectedOpt.isEmpty then
        val batches = ColumnarInterpreter.pairsOf(
          ColumnarInterpreter.interpretAt(
            source,
            child,
            selectedPartition = None,
            preserveAbsence = true,
            ordinals,
          ),
        )
        RankSplitTrace.addScan(System.nanoTime() - scanStarted)
        RankSplitRuns.sortAndSplit(batches, width, workers, ScarletConf.get)
      else
        RankSplitTrace.addScan(System.nanoTime() - scanStarted)
        if RankSplitRuns.fitsSelected(selectedOpt, ScarletConf.get) then
          val staged = RankSplit.stageSelected(selectedOpt, workers)
          selectedOpt = None
          RankSplit.finishSelected(staged, width)
        else
          RankSplitRuns.sortAndSplitSelected(
            selectedOpt.getOrElse(noSelection),
            width,
            workers,
            ScarletConf.get,
          )
    ColumnarInterpreter.Columns.Pairs(sorted)

  /**
   * Long `sortByLongKey`. Same handoff as [[intKey]]. A null key orders as zero and stays null.
   * `preserveAbsence` is passed for symmetry with the int sort. It does not change the long
   * encode.
   */
  def longKey(
      source: Plan[(Long, Long)],
      child: PhysicalOp,
      ordinals: Set[Int],
  ): ColumnarInterpreter.Columns =
    val width = ColumnarPlanner.shuffleWidth
    val workers = ColumnarPlanner.parallelism
    var selectedOpt = FilterChain.selected(
      source,
      child,
      selectedPartition = None,
      preserveAbsence = true,
      ordinals,
    )
    val sorted =
      if selectedOpt.isEmpty then
        val batches = ColumnarInterpreter.longPairsOf(
          ColumnarInterpreter.interpretAt(
            source,
            child,
            selectedPartition = None,
            preserveAbsence = true,
            ordinals,
          ),
        )
        RankSplitLongRuns.sortAndSplit(batches, width, workers, ScarletConf.get)
      else if RankSplitLongRuns.fitsSelected(selectedOpt, ScarletConf.get) then
        val staged = RankSplitLong.stageSelected(selectedOpt, workers)
        selectedOpt = None
        RankSplitLong.finishSelected(staged, width)
      else
        RankSplitLongRuns.sortAndSplitSelected(
          selectedOpt.getOrElse(noSelection),
          width,
          workers,
          ScarletConf.get,
        )
    ColumnarInterpreter.Columns.LongPairs(sorted)

  private def noSelection: Vector[Selected] =
    throw new IllegalArgumentException("sortAndSplitSelected needs at least one batch")
