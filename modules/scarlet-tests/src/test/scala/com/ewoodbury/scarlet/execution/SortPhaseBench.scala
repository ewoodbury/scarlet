package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.concurrent.locks.LockSupport

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{RankSplitRuns, SortPhases}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Phase split of `filter-sort`. Not a test, and not part of `make test`.
 *
 * The input matches [[CrossEngineBench]]: a Fisher-Yates permutation, the shared LCG values, two
 * filters, two source partitions, spill on, budget 64 MiB. The timed action is the same checksum,
 * `collect` then `key * 31 + value`. Warmup and rounds default to 80 and 30. `steady_ms` is the
 * upper median of the last 10 rounds. Each phase uses that same slot. `output_ms` is the action
 * minus scan, run generation, and the merge. Allocation is a separate five-round pass with the
 * phase clock off.
 *
 * Known checksums: 1e6 is `5343278082631`, 1e7 is `518322534674927`. Any other total fails the
 * process. `runs` and `passes` are the in-memory rank split. `spill_files` and `spill_passes`
 * come from [[RankSplitRuns]].
 *
 * {{{
 * SORT_ROWS=10000000 SORT_THREADS=4 SORT_WARMUP=80 SORT_ROUNDS=30 \
 *   java -Xmx4g -cp "$CP" com.ewoodbury.scarlet.execution.SortPhaseBench
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Var"))
object SortPhaseBench:

  private val seed = 0x12345678
  private val shuffleSeed = 58L
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val allocRounds = 5
  private val checksums = Map(
    1000000 -> 5343278082631L,
    10000000 -> 518322534674927L,
  )

  private final case class Round(
      totalMs: Double,
      scanMs: Double,
      runMs: Double,
      mergeMs: Double,
      outputMs: Double,
      runs: Int,
      passes: Int,
      spillFiles: Long,
      spillPasses: Long,
      checksum: Long,
  )

  def main(args: Array[String]): Unit =
    val rows = readInt("SORT_ROWS", 1000000)
    val threads = readInt("SORT_THREADS", 4)
    val warmup = readInt("SORT_WARMUP", 80)
    val rounds = readInt("SORT_ROUNDS", 30)
    require(threads > 0 && warmup >= 0 && rounds >= 20, s"bad window threads=$threads warmup=$warmup rounds=$rounds")
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val scheduler = new LocalTaskScheduler(threads)
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
      val action = build(rows)
      val samples = measure(action, warmup, rounds)
      val alloc = sampleAlloc(action)
      println(render(rows, threads, warmup, rounds, samples, alloc))
    finally
      SortPhases.enabled = false
      try scheduler.shutdown().unsafeRunSync()
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  private def build(rows: Int): () => Long =
    val order = permutation(rows)
    val values = lcgValues(rows)
    val sorted = DistCollection
      .ofIntPairs(order, values, sourcePartitions)
      .filterKeys((key: Int) => key % 3 != 0)
      .filterValues((value: Int) => value >= 0)
      .sortByIntKey
    () => sorted.collect().map { case (key, value) => key * 31L + value }.sum

  private def measure(action: () => Long, warmup: Int, rounds: Int): Array[Round] =
    var round = 0
    while (round < warmup) do
      ScarletRuntime.get.shuffle.clear()
      val _ = action()
      round += 1
    val samples = new Array[Round](rounds)
    round = 0
    while (round < rounds) do
      samples(round) = one(action)
      round += 1
    val checksum = samples(0).checksum
    samples.foreach { sample =>
      if (sample.checksum != checksum) then
        fail(s"checksum changed from $checksum to ${sample.checksum}")
    }
    samples

  private def one(action: () => Long): Round =
    ScarletRuntime.get.shuffle.clear()
    RankSplitRuns.resetForTest()
    SortPhases.reset()
    SortPhases.enabled = true
    val started = System.nanoTime()
    val checksum = action()
    val totalNs = System.nanoTime() - started
    val phases = SortPhases.sample()
    SortPhases.enabled = false
    val scan = phases.scanNs
    val run = phases.runNs
    val merge = phases.mergeNs
    val output = totalNs - scan - run - merge
    Round(
      ms(totalNs),
      ms(scan),
      ms(run),
      ms(merge),
      ms(output),
      phases.runs,
      phases.passes,
      RankSplitRuns.filesWrittenForTest,
      RankSplitRuns.mergePassesForTest,
      checksum,
    )

  private def render(
      rows: Int,
      threads: Int,
      warmup: Int,
      rounds: Int,
      samples: Array[Round],
      allocMb: Double,
  ): String =
    val totals = samples.map(_.totalMs)
    val steady = steadyOf(totals)
    val checksum = samples(samples.length - 1).checksum
    checksums.get(rows).foreach { expected =>
      if (checksum != expected) then fail(s"checksum $checksum != $expected at $rows rows")
    }
    val last = samples(samples.length - 1)
    val line = String.format(
      java.util.Locale.US,
      "phase rows=%d threads=%d warmup=%d rounds=%d steady_ms=%.3f verdict=%s checksum=%d " +
        "alloc_mb=%.3f scan_ms=%.3f run_ms=%.3f merge_ms=%.3f output_ms=%.3f " +
        "runs=%d passes=%d spill_files=%d spill_passes=%d",
      rows,
      threads,
      warmup,
      rounds,
      Double.box(steady.ms),
      steady.verdict,
      Long.box(checksum),
      Double.box(allocMb),
      Double.box(steadyOf(samples.map(_.scanMs)).ms),
      Double.box(steadyOf(samples.map(_.runMs)).ms),
      Double.box(steadyOf(samples.map(_.mergeMs)).ms),
      Double.box(steadyOf(samples.map(_.outputMs)).ms),
      Int.box(last.runs),
      Int.box(last.passes),
      Long.box(last.spillFiles),
      Long.box(last.spillPasses),
    )
    System.err.println(s"SAMPLES ${totals.mkString(",")}")
    line

  private final case class Steady(ms: Double, verdict: String)

  private def steadyOf(samples: Array[Double]): Steady =
    val n = samples.length
    val tailN = math.min(10, n)
    val headN = math.min(10, n)
    val tail = medianRange(samples, n - tailN, n)
    val head = medianRange(samples, 0, headN)
    if (n < 20) then Steady(tail, "short")
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
      Steady(tail, verdict)

  private def medianRange(samples: Array[Double], from: Int, until: Int): Double =
    val copy = java.util.Arrays.copyOfRange(samples, from, until)
    java.util.Arrays.sort(copy)
    copy(copy.length / 2)

  private def sampleAlloc(action: () => Long): Double =
    val bean = ManagementFactory.getThreadMXBean match
      case probe: ThreadMXBean if probe.isThreadAllocatedMemorySupported =>
        probe.setThreadAllocatedMemoryEnabled(true)
        if (probe.isThreadAllocatedMemoryEnabled) then Some(probe) else None
      case _ => None
    bean match
      case None => Double.NaN
      case Some(probe) =>
        val watch = new AllocProbe(probe)
        try
          val samples = new Array[Double](allocRounds)
          var round = 0
          while (round < allocRounds) do
            val bytes = watch.account {
              ScarletRuntime.get.shuffle.clear()
              action()
            }
            samples(round) = bytes.toDouble / 1_000_000.0
            round += 1
          java.util.Arrays.sort(samples)
          samples(samples.length / 2)
        finally watch.stop()

  private def ms(nanos: Long): Double = nanos / 1_000_000.0

  private def fail(message: String): Nothing =
    System.err.println(message)
    sys.exit(1)

  private def readInt(name: String, default: Int): Int =
    sys.env.get(name) match
      case None => default
      case Some(text) =>
        text.toIntOption match
          case Some(value) => value
          case None => fail(s"$name must be an integer, found $text")

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
      state = (state * 1664525 + 1013904223) & 0xFFFFFFFFL
      val other = (state % (index + 1)).toInt
      val held = order(index)
      order(index) = order(other)
      order(other) = held
      index -= 1
    order

  private final class AllocProbe(bean: ThreadMXBean):
    private val capacity = 1024
    private val ids = new Array[Long](capacity)
    private val baseline = new Array[Long](capacity)
    private val high = new Array[Long](capacity)
    private var count = 0
    @volatile private var running = true
    @volatile private var probeId = 0L
    private val thread = new Thread(() => watch(), "scarlet-alloc-probe")

    thread.setDaemon(true)
    thread.start()
    while (probeId == 0L) LockSupport.parkNanos(10000L)

    def stop(): Unit =
      running = false
      LockSupport.unpark(thread)
      thread.join(1000L)
      ()

    def account(body: => Long): Long =
      open()
      try
        val _ = body
        close()
      catch
        case thrown: Throwable =>
          val _ = closeQuietly()
          throw thrown

    private def watch(): Unit =
      probeId = Thread.currentThread().getId
      while (running) {
        observe(setBaseline = false)
        LockSupport.parkNanos(50000L)
      }

    private def open(): Unit = this.synchronized {
      count = 0
      observe(setBaseline = true)
    }

    private def close(): Long = this.synchronized {
      observe(setBaseline = false)
      var index = 0
      var total = 0L
      while (index < count) {
        if (ids(index) != probeId) then
          val base = if (baseline(index) < 0L) 0L else baseline(index)
          val delta = high(index) - base
          if (delta > 0L) then total += delta
        index += 1
      }
      total
    }

    private def closeQuietly(): Long =
      try close()
      catch case _: IllegalStateException => 0L

    private def observe(setBaseline: Boolean): Unit = this.synchronized {
      val live = bean.getAllThreadIds
      var index = 0
      while (index < live.length) {
        val id = live(index)
        val bytes = bean.getThreadAllocatedBytes(id)
        if (bytes >= 0L) then note(id, bytes, setBaseline)
        index += 1
      }
    }

    private def note(id: Long, bytes: Long, setBaseline: Boolean): Unit =
      val at = slot(id)
      if (bytes > high(at)) then high(at) = bytes
      if (setBaseline) then baseline(at) = bytes

    private def slot(id: Long): Int =
      val existing = indexOf(id)
      if (existing >= 0) then existing
      else if (count >= capacity) then
        throw new IllegalStateException(s"alloc probe filled $capacity thread slots")
      else
        val placed = count
        ids(placed) = id
        baseline(placed) = -1L
        high(placed) = 0L
        count = placed + 1
        placed

    private def indexOf(id: Long): Int =
      var index = 0
      var found = -1
      while (index < count && found < 0) {
        if (ids(index) == id) then found = index
        index += 1
      }
      found
