package com.ewoodbury.scarlet.columnar

/**
 * Wall time of one int rank split, recorded at phase boundaries.
 *
 * Off unless a bench sets [[enabled]]. The comparison loop and the gather do not read this. A
 * disabled split pays one flag check per phase and does not call `nanoTime`. [[sample]] is the
 * last split that ran with the flag on: scan, local run sort, and the in-memory merge. Output
 * decode is whatever the action spent outside those three.
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
private[scarlet] object SortPhases:

  /** One split. Times are nanoseconds. `passes` is 0 when the split did not merge. */
  final case class Sample(scanNs: Long, runNs: Long, mergeNs: Long, runs: Int, passes: Int)

  @volatile var enabled: Boolean = false

  private var scanNs: Long = 0L
  private var runNs: Long = 0L
  private var mergeNs: Long = 0L
  private var runs: Int = 0
  private var passes: Int = 0

  def reset(): Unit =
    scanNs = 0L
    runNs = 0L
    mergeNs = 0L
    runs = 0
    passes = 0

  /** `Long.MinValue` when recording is off, so the matching `end` call records nothing. */
  def begin(): Long =
    if (enabled) System.nanoTime() else Long.MinValue

  def endScan(start: Long): Unit =
    if (start != Long.MinValue) then scanNs += System.nanoTime() - start

  def endRun(start: Long): Unit =
    if (start != Long.MinValue) then runNs += System.nanoTime() - start

  def endMerge(start: Long): Unit =
    if (start != Long.MinValue) then mergeNs += System.nanoTime() - start

  /** Run count and merge passes of the split that just finished. Ignored when recording is off. */
  def note(runCount: Int, passCount: Int): Unit =
    if (enabled) then
      runs = runCount
      passes = passCount

  def sample(): Sample = Sample(scanNs, runNs, mergeNs, runs, passes)
