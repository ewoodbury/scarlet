package com.ewoodbury.scarlet.columnar

import java.util.concurrent.atomic.AtomicLong

/**
 * Phase counters for one int rank split.
 *
 * [[RankSplit]] adds the sort and the merge. The interpreter adds the scan that produced the runs
 * and the decode of a [[com.ewoodbury.scarlet.execution.PhysicalOp.SortByIntKey]] result. A caller
 * that wants one action resets first. Run time is the wall clock of the parallel local sorts.
 * Index time and gather time are CPU time, summed across workers, so they can exceed that wall
 * clock. Merge time includes the rank scatter that writes the output batches. Runs are the
 * non-empty inputs of each merge. Index passes are the most passes any one local sort took. Spill
 * file and spill pass counts stay on [[RankSplitRuns]].
 */
private[scarlet] object RankSplitTrace:

  private val scanNs = new AtomicLong
  private val runNs = new AtomicLong
  private val indexNs = new AtomicLong
  private val gatherNs = new AtomicLong
  private val mergeNs = new AtomicLong
  private val outputNs = new AtomicLong
  private val runCount = new AtomicLong
  private val indexPassCount = new AtomicLong

  def reset(): Unit =
    scanNs.set(0L)
    runNs.set(0L)
    indexNs.set(0L)
    gatherNs.set(0L)
    mergeNs.set(0L)
    outputNs.set(0L)
    runCount.set(0L)
    indexPassCount.set(0L)

  def addScan(nanos: Long): Unit =
    val _ = scanNs.addAndGet(nanos)

  def addRun(nanos: Long): Unit =
    val _ = runNs.addAndGet(nanos)

  def addIndex(nanos: Long): Unit =
    val _ = indexNs.addAndGet(nanos)

  def addGather(nanos: Long): Unit =
    val _ = gatherNs.addAndGet(nanos)

  def addMerge(nanos: Long): Unit =
    val _ = mergeNs.addAndGet(nanos)

  def addOutput(nanos: Long): Unit =
    val _ = outputNs.addAndGet(nanos)

  def addRuns(count: Long): Unit =
    val _ = runCount.addAndGet(count)

  /** Keeps the larger pass count. Parallel runs publish independently. */
  def notePasses(passes: Int): Unit =
    val _ = indexPassCount.accumulateAndGet(
      passes.toLong,
      (left, right) => math.max(left, right),
    )

  def scanNanos: Long = scanNs.get

  def runNanos: Long = runNs.get

  def indexNanos: Long = indexNs.get

  def gatherNanos: Long = gatherNs.get

  def mergeNanos: Long = mergeNs.get

  def outputNanos: Long = outputNs.get

  def runs: Long = runCount.get

  def indexPasses: Long = indexPassCount.get
