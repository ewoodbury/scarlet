package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path}
import java.util.Locale
import java.util.concurrent.locks.LockSupport

import scala.util.Random
import scala.util.control.NonFatal

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, RankSplitLongRuns, RankSplitRuns}
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Sort-at-scale mode of [[QueryBench]]. Not a test, and not part of `make test`.
 *
 * Two cases sort 5e6 rows on the columnar path at widths 1 and 4: `int-sort-key-large` and
 * `long-sort-key-large`. Construction matches the default sort queries, with the row count set
 * to 5e6. Two warmup rounds are discarded. Ten rounds are measured, fewer than the default forty.
 * A local stable sort produces the checksum. That comparison runs before warmup and again after
 * the measured rounds, outside the timer, and again after every sample.
 *
 * The row path is not run at 5e6. That is a scope decision. The existing 1e6 row results are the
 * row figures. Scaling those per-type allocation numbers by five is an extrapolated cumulative
 * allocation of about 2.3 GB for int pairs and 2.5 GB for long pairs, not a peak-heap measurement.
 * The sbt JVM heap is about 3 GB and is left unchanged.
 *
 * Two stress cases follow, at 1e6 rows, columnar, width 1: `int-sort-key-spill` and
 * `long-sort-key-spill`. `spillEnabled` is true and `spillBudgetBytes` is 64 KiB. Expected files
 * and passes come from the two source partitions and `spillFanIn`: each partition is one run, and
 * runs merge until one file remains. With the default fan-in that is 3 files and 1 pass. A stress
 * case that does not match fails the run, including one that wrote a file and recorded 0 passes,
 * and no table is printed. Scale cases leave spill off and must write nothing.
 * [[RankSplitRuns]] and [[RankSplitLongRuns]] are reset before every attempt, and again after
 * [[ScarletConf]] and [[ScarletRuntime]] are restored. `files_written` is a publish count. The
 * rank-split store deletes every run at close.
 *
 * `alloc_mb` uses the same process-wide estimate as [[QueryBench]]. Nearest-rank percentiles use
 * 1-based rank `ceil(p / 100 * n)` with no interpolation.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.QueryBench large"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object SortScaleBench:

  private val schemaName = "sort-scale-1"
  private val warmup = 2
  private val measure = 10
  private val measureOffset = 1
  private val allocRounds = 5
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val scaleRows = 5000000
  private val spillRows = 1000000
  private val stressBudgetBytes = 64L * 1024L
  private val lcgSeed = 0x12345678
  private val threadCounts = Vector(1, 4)
  private val spillDirectoryName = "scarlet-sort-scale"

  private final case class Counts(files: Long, passes: Long)

  private final case class Session(
      baseline: ScarletConf,
      runtime: ScarletRuntime.RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      directory: Path,
  )

  private final class BenchCase(
      val name: String,
      val rows: Int,
      val spills: Boolean,
      val intKey: Boolean,
      val expected: Long,
      val run: () => Long,
  ):
    val elapsed: Array[Array[Long]] = Array.ofDim[Long](2, measure)
    val allocBytes: Array[Array[Long]] = Array.ofDim[Long](2, allocRounds)
    val files: Array[Long] = Array.fill(2)(-1L)
    val passes: Array[Long] = Array.fill(2)(-1L)

    (0 until 2).foreach { width =>
      java.util.Arrays.fill(elapsed(width), -1L)
      java.util.Arrays.fill(allocBytes(width), -1L)
    }

  def run(): Unit =
    val started = System.nanoTime()
    try
      checkHarness()
      val savedConf = ScarletConf.get
      val savedRuntime = ScarletRuntime.get
      execute(savedConf, savedRuntime)
    finally
      val wall = System.nanoTime() - started
      System.err.println(s"wall_s=${seconds(wall)}")

  /**
   * The directory and the schedulers are created here, inside the caller's timer, so a
   * construction failure still reaches the matching cleanup. `wall_s` is printed after this
   * returns, which includes that cleanup.
   */
  private def execute(savedConf: ScarletConf, savedRuntime: ScarletRuntime.RuntimeComponents): Unit =
    val directory = freshSpillDirectory()
    try
      val schedulers = startSchedulers()
      try
        val session = Session(savedConf, savedRuntime, schedulers, directory)
        install(session, threads = 1, spills = false)
        val cases = build()
        val scale = cases.filterNot(_.spills)
        val stress = cases.filter(_.spills)
        verify(session, cases, "before")
        System.err.println(s"warm $warmup scale")
        rounds(session, scale, warmup, record = false, offset = 0, label = "scale")
        System.err.println(s"measure $measure scale")
        rounds(session, scale, measure, record = true, offset = measureOffset, label = "scale")
        verify(session, scale, "after-scale")
        System.err.println(s"warm $warmup spill")
        rounds(session, stress, warmup, record = false, offset = 0, label = "spill")
        System.err.println(s"measure $measure spill")
        rounds(session, stress, measure, record = true, offset = measureOffset, label = "spill")
        verify(session, stress, "after-spill")
        System.err.println(s"allocate $allocRounds")
        val allocOn = allocate(session, cases)
        report(allocOn, cases)
      finally schedulers.values.foreach(_.shutdown().unsafeRunSync())
    finally
      try
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)
      finally
        try resetCounts()
        finally deleteSpillDirectory(directory)

  private def checkHarness(): Unit =
    require(warmup == 2 && measure == 10, s"warmup=$warmup measure=$measure")
    require(measure < 40, s"measure=$measure is not fewer than the default 40")
    require(warmup > 0 && allocRounds == 5, "allocation rounds drifted from the default estimate")
    require(rankIndex(measure, 50) == 4, "p50 rank drifted")
    require(rankIndex(measure, 95) == measure - 1, "p95 rank drifted")
    require(scaleRows == 5000000 && spillRows == 1000000, "row counts drifted")
    require(stressBudgetBytes == 64L * 1024L, "stress budget drifted")
    require(sourcePartitions == 2 && batchSize == 8192, "sort construction drifted")
    val fanIn = ScarletConf().spillFanIn
    require(fanIn >= 2, s"spillFanIn drifted to $fanIn")
    val planned = spillPlan(sourcePartitions, fanIn)
    require(
      planned.files == 3L && planned.passes == 1L,
      s"spill plan drifted to files=${planned.files} passes=${planned.passes}",
    )
    threadCounts match
      case Vector(1, 4) => ()
      case _ => throw new IllegalStateException("width vector drifted")
    val leads = Array(0, 0)
    (0 until measure).foreach { round =>
      val phase = round + measureOffset
      leads(widthIndex(leader(widthsFor(phase)))) += 1
    }
    require(leads(0) == leads(1), "width leaders are unbalanced")
    val widthRestarts = leader(widthsFor(0)) == leader(widthsFor(measureOffset))
    require(!widthRestarts, "measure restarts in the warmup width order")

  private def build(): Vector[BenchCase] =
    val scale = pairSorts(rows = scaleRows, spills = false)
    val stress = pairSorts(rows = spillRows, spills = true)
    val cases = scale ++ stress
    requireNames(
      cases,
      Vector(
        "int-sort-key-large",
        "long-sort-key-large",
        "int-sort-key-spill",
        "long-sort-key-spill",
      ),
    )
    requireSameChecksum(scale, "scale")
    requireSameChecksum(stress, "spill")
    cases

  private def requireNames(cases: Vector[BenchCase], names: Vector[String]): Unit =
    require(cases.length == names.length, "case count drifted")
    cases.zip(names).foreach { case (bench, name) =>
      require(sameText(bench.name, name), s"case ${bench.name} drifted from $name")
    }

  private def requireSameChecksum(cases: Vector[BenchCase], label: String): Unit =
    cases match
      case Vector(left, right) =>
        require(left.expected == right.expected, s"$label checksums diverged")
      case _ => throw new IllegalStateException(s"$label case count drifted")

  /**
   * Same inputs as the default sort queries: a Fisher-Yates permutation of `0` until `rows`,
   * seeded by `0x12345678`, and values from the shared generator. Long pairs widen those ints.
   * The keys are unique.
   */
  private def pairSorts(rows: Int, spills: Boolean): Vector[BenchCase] =
    val generated = generate(rows)
    val sortKeys = permutation(rows)
    require(!isSorted(sortKeys), s"sort keys stayed in order at $rows rows")
    val intSort = DistCollection.ofIntPairs(sortKeys, generated, sourcePartitions).sortByIntKey
    val longKeys = widen(sortKeys)
    val longValues = widen(generated)
    val longSort =
      DistCollection.ofLongPairs(longKeys, longValues, sourcePartitions).sortByLongKey
    val suffix = if (spills) then "spill" else "large"
    Vector(
      benchCase(
        s"int-sort-key-$suffix",
        rows,
        "SortByIntKey(Scan)",
        spills,
        intKey = true,
        expectSortedInts(sortKeys, generated),
        intSort.plan,
        checksumIntPairs(intSort.collect()),
      ),
      benchCase(
        s"long-sort-key-$suffix",
        rows,
        "SortByLongKey(Scan)",
        spills,
        intKey = false,
        expectSortedLongs(longKeys, longValues),
        longSort.plan,
        checksumLongPairs(longSort.collect()),
      ),
    )

  private def benchCase(
      name: String,
      rows: Int,
      shape: String,
      spills: Boolean,
      intKey: Boolean,
      expected: Long,
      plan: Plan[_],
      body: => Long,
  ): BenchCase =
    requireShape(name, shape, plan)
    new BenchCase(name, rows, spills, intKey, expected, () => body)

  private def requireShape(name: String, shape: String, plan: Plan[_]): Unit =
    val got = ColumnarPlanner.lower(plan).map(_.shape).getOrElse("refused")
    require(sameText(got, shape), s"$name shape $got != $shape")

  private def verify(session: Session, cases: Vector[BenchCase], label: String): Unit =
    cases.foreach { bench =>
      widthsOf(bench).foreach { threads =>
        sample(session, bench, threads, round = 0, record = false)
      }
    }
    System.err.println(s"checksum $label")

  private def rounds(
      session: Session,
      cases: Vector[BenchCase],
      count: Int,
      record: Boolean,
      offset: Int,
      label: String,
  ): Unit =
    (0 until count).foreach { round =>
      val phase = round + offset
      widthsFor(phase).foreach { threads =>
        cases.foreach { bench =>
          if (widthsOf(bench).contains(threads)) then
            sample(session, bench, threads, round, record)
        }
      }
      val phaseName = if (record) then "measure" else "warm"
      System.err.println(s"$label $phaseName ${round + 1}/$count")
    }

  private def sample(
      session: Session,
      bench: BenchCase,
      threads: Int,
      round: Int,
      record: Boolean,
  ): Unit =
    install(session, threads, bench.spills)
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      resetCounts()
      ScarletRuntime.get.shuffle.clear()
      val started = System.nanoTime()
      val got = bench.run()
      val elapsed = System.nanoTime() - started
      val counts = countsOf(bench)
      requirePath(bench, threads, sink.batchCount)
      require(got == bench.expected, s"${bench.name} t$threads checksum $got != ${bench.expected}")
      requireCounts(bench, threads, counts)
      if (record) then keep(bench, threads, round, elapsed, counts)
    finally ColumnMetrics.restore(previous)

  private def requirePath(bench: BenchCase, threads: Int, batches: Long): Unit =
    require(
      batches > 0L,
      s"${bench.name} columnar t$threads published no column batch",
    )

  private def requireCounts(bench: BenchCase, threads: Int, counts: Counts): Unit =
    val expected =
      if (bench.spills) then spillPlan(sourcePartitions, ScarletConf.get.spillFanIn)
      else Counts(0L, 0L)
    require(
      counts.files == expected.files && counts.passes == expected.passes,
      s"${bench.name} t$threads files=${counts.files} passes=${counts.passes}, " +
        s"expected files=${expected.files} passes=${expected.passes}",
    )

  /**
   * Each source partition is one published run. A pass merges at most `fanIn` runs and publishes
   * one file per group of two or more. A leftover run of one is carried forward and is not a new
   * file. Passes stop when one run remains. Two partitions and a fan-in of at least 2 publish 3
   * files in 1 pass.
   */
  private def spillPlan(partitions: Int, fanIn: Int): Counts =
    require(partitions >= 1, s"partitions must be positive, got $partitions")
    require(fanIn >= 2, s"spillFanIn must be >= 2, got $fanIn")
    def step(open: Int, files: Long, passes: Long): Counts =
      if (open <= 1) then Counts(files, passes)
      else
        val groups = (open + fanIn - 1) / fanIn
        val carried = if (open % fanIn == 1) then 1 else 0
        step(groups, files + (groups - carried).toLong, passes + 1L)
    step(partitions, partitions.toLong, 0L)

  private def keep(
      bench: BenchCase,
      threads: Int,
      round: Int,
      elapsed: Long,
      counts: Counts,
  ): Unit =
    val width = widthIndex(threads)
    val prior = bench.files(width)
    if (prior < 0L) then
      bench.files(width) = counts.files
      bench.passes(width) = counts.passes
    else
      require(
        prior == counts.files && bench.passes(width) == counts.passes,
        s"${bench.name} t$threads counters changed across rounds",
      )
    bench.elapsed(width)(round) = elapsed

  private def resetCounts(): Unit =
    RankSplitRuns.resetForTest()
    RankSplitLongRuns.resetForTest()

  /**
   * Reads the counter for this key type. The other sort's counters must still be zero after the
   * reset that opened the attempt.
   */
  private def countsOf(bench: BenchCase): Counts =
    val intFiles = RankSplitRuns.filesWrittenForTest
    val intPasses = RankSplitRuns.mergePassesForTest
    val longFiles = RankSplitLongRuns.filesWrittenForTest
    val longPasses = RankSplitLongRuns.mergePassesForTest
    if (bench.intKey) then
      require(longFiles == 0L && longPasses == 0L, s"${bench.name} touched the long spill counters")
      Counts(intFiles, intPasses)
    else
      require(intFiles == 0L && intPasses == 0L, s"${bench.name} touched the int spill counters")
      Counts(longFiles, longPasses)

  private def allocate(session: Session, cases: Vector[BenchCase]): Boolean =
    startProbe() match
      case None =>
        System.err.println("allocate unavailable")
        false
      case Some(probe) =>
        try
          (0 until allocRounds).foreach { round =>
            val phase = round + measureOffset
            widthsFor(phase).foreach { threads =>
              cases.foreach { bench =>
                if (widthsOf(bench).contains(threads)) then
                  install(session, threads, bench.spills)
                  val sink = new CountingSink
                  val previous = ColumnMetrics.install(sink)
                  try
                    resetCounts()
                    val (got, bytes) = probe.account {
                      ScarletRuntime.get.shuffle.clear()
                      bench.run()
                    }
                    val counts = countsOf(bench)
                    requirePath(bench, threads, sink.batchCount)
                    require(got == bench.expected, s"${bench.name} alloc checksum $got")
                    requireCounts(bench, threads, counts)
                    bench.allocBytes(widthIndex(threads))(round) = bytes
                  finally ColumnMetrics.restore(previous)
              }
            }
            System.err.println(s"allocate ${round + 1}/$allocRounds")
          }
          true
        finally probe.stop()

  private def report(allocOn: Boolean, cases: Vector[BenchCase]): Unit =
    val header = Vector(
      preamble(allocOn),
      "",
      "| query | layout | threads | rows | p50 ms | p95 ms | max ms | alloc mb | files_written | merge_passes |",
      "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    )
    val body = cases.flatMap { bench =>
      widthsOf(bench).map(threads => tableRow(bench, threads, allocOn))
    }
    val notes = cases.map(bench => s"${bench.name} checksum=${bench.expected}")
    System.err.println((header ++ body ++ Vector("") ++ notes ++ Vector("")).mkString("\n"))
    cases.foreach { bench =>
      widthsOf(bench).foreach { threads =>
        println(jsonRow(bench, threads, allocOn))
      }
    }

  private def tableRow(bench: BenchCase, threads: Int, allocOn: Boolean): String =
    val (median, tail, highest) = percentiles(bench, threads)
    val counts = recorded(bench, threads)
    s"| ${bench.name} | columnar | $threads | ${bench.rows} | " +
      s"${millis(median)} | ${millis(tail)} | ${millis(highest)} | " +
      s"${allocText(bench, threads, allocOn)} | ${counts.files} | ${counts.passes} |"

  private def jsonRow(bench: BenchCase, threads: Int, allocOn: Boolean): String =
    val (median, tail, highest) = percentiles(bench, threads)
    val counts = recorded(bench, threads)
    "{" +
      s""""schema":"$schemaName",""" +
      s""""query":"${bench.name}",""" +
      """"layout":"columnar",""" +
      s""""threads":$threads,""" +
      s""""rows":${bench.rows},""" +
      s""""batch":$batchSize,""" +
      s""""rounds":$measure,""" +
      s""""p50_ms":${millis(median)},""" +
      s""""p95_ms":${millis(tail)},""" +
      s""""max_ms":${millis(highest)},""" +
      s""""alloc_mb":${allocJson(bench, threads, allocOn)},""" +
      s""""files_written":${counts.files},""" +
      s""""merge_passes":${counts.passes},""" +
      s""""checksum":${bench.expected}}"""

  private def percentiles(bench: BenchCase, threads: Int): (Long, Long, Long) =
    val samples = bench.elapsed(widthIndex(threads)).clone()
    require(samples.forall(_ >= 0L), s"${bench.name} t$threads missing samples")
    java.util.Arrays.sort(samples)
    val count = samples.length
    (samples(rankIndex(count, 50)), samples(rankIndex(count, 95)), samples(count - 1))

  private def recorded(bench: BenchCase, threads: Int): Counts =
    val width = widthIndex(threads)
    val files = bench.files(width)
    val passes = bench.passes(width)
    require(files >= 0L && passes >= 0L, s"${bench.name} t$threads missing counters")
    Counts(files, passes)

  private def preamble(allocOn: Boolean): String =
    val runtime = ManagementFactory.getRuntimeMXBean
    val heap = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getMax
    val alloc = if (allocOn) then "process-wide-estimate" else "unavailable"
    val p50Rank = rankIndex(measure, 50) + 1
    val p95Rank = rankIndex(measure, 95) + 1
    val above = measure - p95Rank
    val tailNote =
      if (above == 0) then "p95 is the largest sample, the same rank as max."
      else s"$above samples sit strictly above p95. max is the largest sample."
    s"sort scale warmup=$warmup measure=$measure alloc_rounds=$allocRounds " +
      s"batch=$batchSize partitions=$sourcePartitions rows=$scaleRows spill_rows=$spillRows " +
      s"stress_budget_bytes=$stressBudgetBytes alloc=$alloc\n" +
      s"measure=$measure is fewer than the default 40 measured rounds.\n" +
      s"percentile=nearest-rank ceil(p/100*n) no-interpolation n=$measure " +
      s"p50_rank=$p50Rank p95_rank=$p95Rank max_rank=$measure\n" +
      s"$tailNote\n" +
      "runMain executes in the sbt JVM with Test / fork left false. Absolute numbers carry " +
      "sbt and JIT noise. Comparisons within this run are the signal.\n" +
      "Columnar only. Widths 1 and 4 share one JVM. Width order flips each round, and each " +
      "width leads half of the measured rounds. Measure round 0 uses the width order of " +
      "warmup round 1. The spill stress is width 1.\n" +
      "The row path was not run at 5e6. That is a scope decision. The existing 1e6 row " +
      "results are the row figures. Scaling those per-type allocation numbers by five is an " +
      "extrapolated cumulative allocation of about 2.3 GB for int pairs and 2.5 GB for long " +
      s"pairs, not a peak-heap measurement. The sbt JVM heap max is $heap bytes. The heap is " +
      "left unchanged.\n" +
      "Sort queries checksum the collected key then value, mixed by 31, in output order. The " +
      "expected value is a local stable sort of the same pairs. The comparison runs before " +
      "warmup and after the measured rounds, outside the timer.\n" +
      "Each sample fails the run unless columnar published at least one column batch.\n" +
      "Scale rows leave spill off and record files_written 0 and merge_passes 0. Spill rows " +
      "set spillEnabled and a 64 KiB budget. Expected files and passes are derived from the " +
      "two source partitions and spillFanIn. With the default fan-in that is 3 files and 1 " +
      "pass. A spill row that does not match fails the run, including a row that wrote a file " +
      "and recorded 0 passes, and no table is printed. files_written is a publish count. The " +
      "rank-split store deletes every run at close. RankSplitRuns and RankSplitLongRuns are " +
      "reset before the attempt and again after ScarletConf and ScarletRuntime are restored.\n" +
      "alloc_mb is the nearest-rank p50 of process-wide thread bytes, in decimal megabytes " +
      "(bytes / 1e6). The probe excludes itself. A short-lived worker missed between reads is " +
      "absent. The same method is used at both widths. The numbers need not match.\n" +
      s"java=${runtime.getVmName} ${runtime.getVmVersion}"

  private def install(session: Session, threads: Int, spills: Boolean): Unit =
    val scheduler = session.schedulers.getOrElse(
      threads,
      throw new IllegalStateException(s"no scheduler for width $threads"),
    )
    val budget = if (spills) then stressBudgetBytes else session.baseline.spillBudgetBytes
    ScarletConf.set(
      session.baseline.copy(
        threadPoolSize = threads,
        columnarExecution = true,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = 4,
        spillEnabled = spills,
        spillBudgetBytes = budget,
        spillDirectory = session.directory.toString,
      ),
    )
    ScarletRuntime.set(session.runtime.copy(scheduler = scheduler))

  private def widthsOf(bench: BenchCase): Vector[Int] =
    if (bench.spills) then Vector(1) else threadCounts

  private def widthsFor(phase: Int): Vector[Int] =
    if (phase % 2 == 0) then threadCounts else threadCounts.reverse

  private def leader(order: Vector[Int]): Int =
    order match
      case first +: _ => first
      case _ => throw new IllegalStateException("order is empty")

  /** 1-based rank `ceil(percent / 100 * count)`, returned as a 0-based index. */
  private def rankIndex(count: Int, percent: Int): Int =
    val rank = (percent * count + 99) / 100
    math.max(1, math.min(count, rank)) - 1

  private def widthIndex(threads: Int): Int =
    if (threads == 1) then 0
    else if (threads == 4) then 1
    else throw new IllegalStateException(s"width $threads is not measured")

  private def allocText(bench: BenchCase, threads: Int, allocOn: Boolean): String =
    allocMedian(bench, threads, allocOn) match
      case None => "n/a"
      case Some(bytes) => megabytes(bytes)

  private def allocJson(bench: BenchCase, threads: Int, allocOn: Boolean): String =
    allocMedian(bench, threads, allocOn) match
      case None => "null"
      case Some(bytes) => megabytes(bytes)

  private def allocMedian(bench: BenchCase, threads: Int, allocOn: Boolean): Option[Long] =
    if (!allocOn) then None
    else
      val samples = bench.allocBytes(widthIndex(threads)).clone()
      java.util.Arrays.sort(samples)
      if (samples.exists(_ < 0L)) then None
      else Some(samples(rankIndex(samples.length, 50)))

  /**
   * Copied from [[QueryBench]], along with the permutation, checksum, and widen helpers below.
   * Sharing them is a follow-up. This bench keeps its own copies.
   */
  private def generate(rows: Int): Array[Int] =
    val values = new Array[Int](rows)
    var index = 0
    var state = lcgSeed
    while (index < rows) {
      state = state * 1664525 + 1013904223
      val mixed = state & 0x7fffffff
      values(index) = (mixed % 2000001) - 1000000
      index += 1
    }
    values

  private def permutation(rows: Int): Array[Int] =
    val keys = Array.tabulate(rows)(identity)
    fisherYates(keys, 0, rows, new Random(lcgSeed.toLong))
    keys

  private def fisherYates(values: Array[Int], start: Int, length: Int, random: Random): Unit =
    var offset = length - 1
    while (offset > 0) {
      val swap = random.nextInt(offset + 1)
      val left = start + offset
      val right = start + swap
      val held = values(left)
      values(left) = values(right)
      values(right) = held
      offset -= 1
    }

  private def isSorted(keys: Array[Int]): Boolean =
    var index = 1
    var ordered = true
    while (index < keys.length && ordered) {
      if (keys(index - 1) > keys(index)) then ordered = false
      index += 1
    }
    ordered

  private def widen(values: Array[Int]): Array[Long] =
    val widened = new Array[Long](values.length)
    var index = 0
    while (index < values.length) {
      widened(index) = values(index).toLong
      index += 1
    }
    widened

  /** Local stable sort, then a rolling mix of key then value. Runs outside the timer. */
  private def expectSortedInts(keys: Array[Int], values: Array[Int]): Long =
    val order = Array.tabulate(keys.length)(identity)
    scala.util.Sorting.stableSort(order, (left: Int, right: Int) => keys(left) < keys(right))
    checksumOrdered(order, row => keys(row).toLong, row => values(row).toLong)

  /** Local stable sort, then a rolling mix of key then value. Runs outside the timer. */
  private def expectSortedLongs(keys: Array[Long], values: Array[Long]): Long =
    val order = Array.tabulate(keys.length)(identity)
    scala.util.Sorting.stableSort(order, (left: Int, right: Int) => keys(left) < keys(right))
    checksumOrdered(order, row => keys(row), row => values(row))

  private def checksumOrdered(order: Array[Int], keyAt: Int => Long, valueAt: Int => Long): Long =
    var index = 0
    var hash = 0L
    while (index < order.length) {
      val row = order(index)
      hash = hash * 31L + keyAt(row)
      hash = hash * 31L + valueAt(row)
      index += 1
    }
    hash

  private def checksumIntPairs(rows: Iterable[(Int, Int)]): Long =
    val iterator = rows.iterator
    var hash = 0L
    while (iterator.hasNext) {
      val pair = iterator.next()
      hash = hash * 31L + pair._1.toLong
      hash = hash * 31L + pair._2.toLong
    }
    hash

  private def checksumLongPairs(rows: Iterable[(Long, Long)]): Long =
    val iterator = rows.iterator
    var hash = 0L
    while (iterator.hasNext) {
      val pair = iterator.next()
      hash = hash * 31L + pair._1
      hash = hash * 31L + pair._2
    }
    hash

  private def freshSpillDirectory(): Path =
    Files.createTempDirectory(
      Path.of(System.getProperty("java.io.tmpdir")),
      spillDirectoryName + "-",
    )

  /**
   * A delete failure must not mask the bench result. Stores are already closed. The failure is
   * reported on stderr with the directory path.
   */
  private def deleteSpillDirectory(directory: Path): Unit =
    try deleteTree(directory)
    catch
      case NonFatal(thrown) =>
        val detail = Option(thrown.getMessage).getOrElse(thrown.getClass.getName)
        System.err.println(s"spill directory delete failed: $directory ($detail)")

  /** Opens one scheduler per width. A later constructor failure shuts down the ones already open. */
  private def startSchedulers(): Map[Int, LocalTaskScheduler] =
    def open(
        pending: Vector[Int],
        opened: Map[Int, LocalTaskScheduler],
    ): Map[Int, LocalTaskScheduler] =
      pending.headOption match
        case None => opened
        case Some(threads) =>
          val scheduler =
            try new LocalTaskScheduler(threads)
            catch
              case NonFatal(thrown) =>
                opened.values.foreach(_.shutdown().unsafeRunSync())
                throw thrown
          open(pending.drop(1), opened.updated(threads, scheduler))
    open(threadCounts, Map.empty)

  private def deleteTree(path: Path): Unit =
    if (Files.isDirectory(path)) then
      val stream = Files.newDirectoryStream(path)
      try stream.forEach(deleteTree)
      finally stream.close()
    val _ = Files.deleteIfExists(path)

  private def millis(nanos: Long): String =
    String.format(Locale.ENGLISH, "%.3f", nanos.toDouble / 1000000.0)

  private def megabytes(bytes: Long): String =
    String.format(Locale.ENGLISH, "%.3f", bytes.toDouble / 1000000.0)

  private def seconds(nanos: Long): String =
    String.format(Locale.ENGLISH, "%.3f", nanos.toDouble / 1000000000.0)

  private def sameText(left: String, right: String): Boolean =
    left.compareTo(right) == 0

  private def startProbe(): Option[AllocProbe] =
    threadBean.filter(_.isThreadAllocatedMemorySupported).flatMap { bean =>
      bean.setThreadAllocatedMemoryEnabled(true)
      if (bean.isThreadAllocatedMemoryEnabled) then Some(new AllocProbe(bean)) else None
    }

  private def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None

  /**
   * Sums bytes on every live thread except the probe. Morsel workers have exited when the action
   * returns, so a before/after pair on the caller misses them. The watch reads while the action
   * runs. A worker that is never seen is missing. The caller's before/after pair is included
   * because the caller is one of the live threads.
   */
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
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

    def account[A](body: => A): (A, Long) =
      open()
      try
        val value = body
        (value, close())
      catch
        case thrown: Throwable =>
          closeQuietly()
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

    private def closeQuietly(): Unit =
      try
        val _ = close()
      catch
        case _: IllegalStateException => ()

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
