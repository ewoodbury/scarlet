package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.Locale

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{RankSplit, RankSplitRuns, RankSplitTrace}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Phase split of cross-engine `filter-sort`. Not a test, and not part of `make test`.
 *
 * The input matches [[CrossEngineBench]]: permutation keys, the same LCG values, two source
 * partitions, batch 8192, shuffle width 4, two filters (`key % 3 != 0`, `value >= 0`), spill on.
 * The action is the sort. The checksum is the same `key * 31 + value` sum, read from the published
 * int arrays by [[SortChecksum]]. It does not decode pairs. `scan` is the encode and the survivor
 * index. `run` is the wall clock of the local sorts. `index` and `gather` are CPU time inside that
 * wall clock, summed across workers. `merge` is the k-way merge and the rank scatter. `output` is
 * the pair decoder's clock, which this harness does not enter. `other` is the action minus those
 * four wall times: the checksum loop and the planner. `gc_ms` is the collector's own millisecond
 * counter. `alloc_mb` is the median of five process-wide thread-byte deltas after the timed
 * rounds.
 *
 * `steady_*` is the upper median of the last 10 rounds, with the same verdict rule as
 * [[CrossEngineBench]]. Runs and passes are read after the last measured round. One in-memory sort
 * records one merge. Spill passes stay on [[RankSplitRuns]].
 *
 * {{{
 * SORT_ROWS=10000000 SORT_WARMUP=80 SORT_ROUNDS=30 \
 *   java -Xmx4g -cp "$CP" com.ewoodbury.scarlet.execution.SortPhaseBench
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Equals"))
object SortPhaseBench:

  private val seed = 0x12345678
  private val shuffleSeed = 58L
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val threads = readInt("SORT_THREADS", 4)
  private val rows = readInt("SORT_ROWS", 1000000)
  private val warmup = readInt("SORT_WARMUP", 80)
  private val rounds = readInt("SORT_ROUNDS", 30)
  private val allocRounds = 5

  private val known = Map(
    1000000 -> 5343278082631L,
    10000000 -> 518322534674927L,
  )

  def main(args: Array[String]): Unit =
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(threads)
    var status = 0
    try
      ScarletConf.set(
        savedConf.copy(
          threadPoolSize = threads,
          columnarExecution = true,
          columnarBatchSize = batchSize,
          defaultShufflePartitions = 4,
          spillEnabled = true,
        ),
      )
      ScarletRuntime.set(savedRuntime.copy(scheduler = scheduler))
      val action = build()
      val samples = measure(action)
      val allocMb = sampleAlloc(action)
      val steady = steadyOf(samples.action)
      val line = render(samples, steady, allocMb)
      println(line)
      known.get(rows).foreach { expected =>
        if (samples.checksum != expected) then
          System.err.println(s"checksum ${samples.checksum} != $expected")
          status = 1
      }
    finally
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
    if (status != 0) then sys.exit(status)

  private def build(): () => Long =
    val keys = permutation(rows)
    val values = lcgValues(rows)
    val sorted = DistCollection
      .ofIntPairs(keys, values, sourcePartitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .sortByIntKey
    () => SortChecksum.of(sorted)

  private final class Samples(val rounds: Int):
    val action = new Array[Long](rounds)
    val scan = new Array[Long](rounds)
    val run = new Array[Long](rounds)
    val index = new Array[Long](rounds)
    val gather = new Array[Long](rounds)
    val merge = new Array[Long](rounds)
    val output = new Array[Long](rounds)
    val gc = new Array[Long](rounds)
    var checksum = 0L
    var runs = 0L
    var indexPasses = 0L
    var merges = 0
    var spillFiles = 0L
    var spillPasses = 0L

  private def measure(action: () => Long): Samples =
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      val _ = action()
      round += 1
    val samples = new Samples(rounds)
    round = 0
    while (round < rounds) do
      RankSplitTrace.reset()
      RankSplit.resetForTest()
      RankSplitRuns.resetForTest()
      ScarletRuntime.get.shuffle.clear()
      val gcBefore = gcMillis()
      val started = System.nanoTime()
      val checksum = action()
      val elapsed = System.nanoTime() - started
      samples.action(round) = elapsed
      samples.scan(round) = RankSplitTrace.scanNanos
      samples.run(round) = RankSplitTrace.runNanos
      samples.index(round) = RankSplitTrace.indexNanos
      samples.gather(round) = RankSplitTrace.gatherNanos
      samples.merge(round) = RankSplitTrace.mergeNanos
      samples.output(round) = RankSplitTrace.outputNanos
      samples.gc(round) = gcMillis() - gcBefore
      samples.checksum = checksum
      samples.runs = RankSplitTrace.runs
      samples.indexPasses = RankSplitTrace.indexPasses
      samples.merges = RankSplit.mergePassesForTest
      samples.spillFiles = RankSplitRuns.filesWrittenForTest
      samples.spillPasses = RankSplitRuns.mergePassesForTest
      round += 1
    samples

  private final case class Steady(ms: Double, head: Double, verdict: String)

  private def steadyOf(samples: Array[Long]): Steady =
    val n = samples.length
    val tailN = math.min(10, n)
    val headN = math.min(10, n)
    val tail = medianRange(samples, n - tailN, n)
    val head = medianRange(samples, 0, headN)
    if (n < 20) then Steady(tail, head, "short")
    else
      val mid = n - tailN / 2
      val left = medianRange(samples, n - tailN, mid)
      val right = medianRange(samples, mid, n)
      val low = math.min(left, right)
      val high = math.max(left, right)
      val verdict =
        if (high > low * 1.10) then "machine"
        else if (head > tail * 1.10) then "compilation"
        else "ok"
      Steady(tail, head, verdict)

  private def medianRange(samples: Array[Long], from: Int, until: Int): Double =
    val copy = java.util.Arrays.copyOfRange(samples, from, until)
    java.util.Arrays.sort(copy)
    copy(copy.length / 2).toDouble / 1000000.0

  private def render(samples: Samples, steady: Steady, allocMb: Double): String =
    val from = samples.rounds - math.min(10, samples.rounds)
    val until = samples.rounds
    val scan = medianRange(samples.scan, from, until)
    val run = medianRange(samples.run, from, until)
    val index = medianRange(samples.index, from, until)
    val gather = medianRange(samples.gather, from, until)
    val merge = medianRange(samples.merge, from, until)
    val output = medianRange(samples.output, from, until)
    val other = steady.ms - scan - run - merge - output
    val gc = medianRange(samples.gc.map(_ * 1000000L), from, until)
    val fields = List(
      s"rows=$rows",
      s"threads=$threads",
      s"steady_ms=${fmt(steady.ms)}",
      s"steady=${steady.verdict}",
      s"head_ms=${fmt(steady.head)}",
      s"scan_ms=${fmt(scan)}",
      s"run_ms=${fmt(run)}",
      s"index_ms=${fmt(index)}",
      s"gather_ms=${fmt(gather)}",
      s"merge_ms=${fmt(merge)}",
      s"output_ms=${fmt(output)}",
      s"other_ms=${fmt(other)}",
      s"gc_ms=${fmt(gc)}",
      s"runs=${samples.runs}",
      s"index_passes=${samples.indexPasses}",
      s"merges=${samples.merges}",
      s"spill_files=${samples.spillFiles}",
      s"spill_passes=${samples.spillPasses}",
      s"alloc_mb=${fmt(allocMb)}",
      s"checksum=${samples.checksum}",
    )
    "PHASE " + fields.mkString(" ")

  private def fmt(millis: Double): String =
    String.format(Locale.ENGLISH, "%.3f", millis)

  /** Middle sample of five process-wide byte deltas, in decimal megabytes. */
  private def sampleAlloc(action: () => Long): Double =
    startProbe() match
      case None => Double.NaN
      case Some(probe) =>
        try
          val taken = new Array[Double](allocRounds)
          var round = 0
          while (round < allocRounds) do
            val (_, bytes) = probe.account {
              ScarletRuntime.get.shuffle.clear()
              val _ = action()
            }
            taken(round) = bytes.toDouble / 1000000.0
            round += 1
          java.util.Arrays.sort(taken)
          taken(taken.length / 2)
        finally probe.stop()

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def startProbe(): Option[AllocProbe] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean =>
        bean.setThreadAllocatedMemoryEnabled(true)
        if (bean.isThreadAllocatedMemoryEnabled) then Some(new AllocProbe(bean)) else None
      case _ => None

  private def gcMillis(): Long =
    val beans = ManagementFactory.getGarbageCollectorMXBeans.iterator
    var total = 0L
    while (beans.hasNext) do
      val collected = beans.next().getCollectionTime
      if (collected > 0L) then total += collected
    total

  private def lcgValues(count: Int): Array[Int] =
    val values = new Array[Int](count)
    var index = 0
    var state = seed
    while (index < count) do
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    values

  private def permutation(count: Int): Array[Int] =
    val order = Array.tabulate(count)(identity)
    var state = shuffleSeed
    var index = count - 1
    while (index > 0) do
      state = (state * 1664525 + 1013904223) & 0xffffffffL
      val other = (state % (index + 1)).toInt
      val held = order(index)
      order(index) = order(other)
      order(other) = held
      index -= 1
    order

  private def readInt(name: String, default: Int): Int =
    sys.env.get(name).flatMap(_.toIntOption).getOrElse(default)
