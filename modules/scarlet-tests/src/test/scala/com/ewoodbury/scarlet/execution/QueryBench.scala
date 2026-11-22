package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.Locale

import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.columnar.*
import com.ewoodbury.scarlet.core.{Plan, ScarletConf}
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Times one query per supported columnar type, plus grouped-key patterns, a two-filter count,
 * two-filter int `countByKey`, two-filter int and string `groupByKey` and `reduceByKey`, int and
 * string inner joins, and int sort, plus unfiltered int-pair and long-pair sorts. Not a test, and
 * not part of `make test`.
 *
 * Each query is built once. The timed region is the action. The scalar checksum is compared after
 * the timer stops, against a local loop and one row-path execution. A mismatch fails the run.
 * Grouped queries also checksum per-group sums outside the timer. `grouped-shuffled` must differ
 * from the repeating checksum. `grouped-sparse` and `grouped-shifting` spread groups so the
 * partial hashes, and one morsel of each must not be sorted.
 *
 * Every sample installs a [[CountingSink]] outside the timer. A columnar sample must publish at
 * least one batch. A row sample must publish none.
 *
 * `runMain` executes in the sbt JVM. `Test / fork` is left false, so absolute times carry sbt and
 * JIT noise. Comparisons within one run are the signal. Width and layout order are
 * counterbalanced. The measure phase starts one step later than warmup. One [[LocalTaskScheduler]]
 * per width is built for the run and shut down at the end.
 *
 * An optional `large` argument runs [[SortScaleBench]] instead: int-key and long-key sorts at 5e6
 * rows, columnar only, widths 1 and 4, then a 1e6 spill stress at width 1. Two warmup rounds are
 * discarded and ten are measured. The row path is not run. Any other argument prints usage to
 * stderr and exits nonzero.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.QueryBench"
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.QueryBench large"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object QueryBench:

  private val schemaName = "query-bench-1"
  private val warmup = 5
  private val measure = 40
  private val measureOffset = 1
  private val allocRounds = 5
  private val batchSize = QueryBenchCases.batchSize
  private val sourcePartitions = QueryBenchCases.sourcePartitions
  private val threadCounts = Vector(1, 4)
  private val layouts = Vector(true, false)

  private[execution] enum Answer:
    case Whole(value: Long)
    case Real(value: Double)

  /** Per-group sums folded in key order. `total` is the grand sum of those groups. */
  private[execution] final case class GroupFold(hash: Long, total: Long)

  private[execution] final class Query(
      val name: String,
      val rows: Int,
      val shape: String,
      val expected: Answer,
      val run: () => Answer,
      val groupHash: Option[Long],
      val rowGroups: () => GroupFold,
      val columnarGroups: () => GroupFold,
  ):
    val elapsed: Array[Array[Array[Long]]] = Array.ofDim[Long](2, 2, measure)
    val allocBytes: Array[Array[Array[Long]]] = Array.ofDim[Long](2, 2, allocRounds)

    (0 until 2).foreach { layout =>
      (0 until 2).foreach { width =>
        java.util.Arrays.fill(allocBytes(layout)(width), -1L)
      }
    }

  def main(args: Array[String]): Unit =
    args.toList match
      case Nil => runDefault(None)
      case "large" :: Nil => SortScaleBench.run()
      case query :: Nil => runDefault(Some(query))
      case other =>
        System.err.println(s"usage: QueryBench [large|query], got ${other.mkString(" ")}")
        System.err.println("accepted: no arguments, large, or one query name")
        sys.exit(1)

  /** The suite timed by `QueryBench` with no arguments. One name times only that query. */
  private def runDefault(only: Option[String]): Unit =
    checkHarness()
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val schedulers = threadCounts.map { threads =>
      threads -> new LocalTaskScheduler(threads)
    }.toMap
    try
      install(schedulers, threads = 1, columnar = true)
      val queries = only match
        case Some(name) => buildNamed(name)
        case None => build()
      verifyRowOracle(queries)
      checkGroups(queries)
      System.err.println(s"warm $warmup")
      rounds(queries, warmup, record = false, offset = 0)
      System.err.println(s"measure $measure")
      rounds(queries, measure, record = true, offset = measureOffset)
      checkGroups(queries)
      System.err.println(s"allocate $allocRounds")
      val allocOn = allocate(queries)
      report(allocOn, queries)
    finally
      try schedulers.values.foreach(_.shutdown())
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  private def buildNamed(name: String): Vector[Query] =
    QueryBenchCases.buildNamed(name)

  private def build(): Vector[Query] =
    QueryBenchCases.build()

  private[execution] def whole(
      name: String,
      rows: Int,
      shape: String,
      expected: Long,
      plan: Plan[_],
      body: => Long,
  ): Query =
    requireShape(name, shape, plan)
    new Query(
      name,
      rows,
      shape,
      Answer.Whole(expected),
      () => Answer.Whole(body),
      None,
      () => unusedGroups(name),
      () => unusedGroups(name),
    )

  private[execution] def real(
      name: String,
      rows: Int,
      shape: String,
      expected: Double,
      plan: Plan[_],
      body: => Double,
  ): Query =
    requireShape(name, shape, plan)
    new Query(
      name,
      rows,
      shape,
      Answer.Real(expected),
      () => Answer.Real(body),
      None,
      () => unusedGroups(name),
      () => unusedGroups(name),
    )

  private def unusedGroups(name: String): GroupFold =
    throw new IllegalStateException(s"$name has no group checksum")

  private[execution] def requireShape(name: String, shape: String, plan: Plan[_]): Unit =
    val got =
      if (shape.startsWith("GroupedSum")) then
        ColumnarPlanner.groupedSumPeel(plan).getOrElse("refused")
      else ColumnarPlanner.lower(plan).map(_.shape).getOrElse("refused")
    require(sameText(got, shape), s"$name shape $got != $shape")

  private def verifyRowOracle(queries: Vector[Query]): Unit =
    install(threads = 1, columnar = false)
    queries.foreach { query =>
      val got = runObserved(query, threads = 1, columnar = false)
      require(matches(got, query.expected), s"${query.name} row oracle ${show(got)}")
    }

  /**
   * Row folds must reproduce the stored checksum. Columnar folds `sumByKey` at this width and must
   * reproduce it too. The public action's grand total is checked on both layouts.
   */
  private def checkGroups(queries: Vector[Query]): Unit =
    threadCounts.foreach { threads =>
      queries.foreach { query =>
        query.groupHash.foreach { expected =>
          install(threads, columnar = false)
          val rowFold = query.rowGroups()
          require(
            rowFold.hash == expected,
            s"${query.name} row t$threads group checksum ${rowFold.hash} != $expected",
          )
          val rowTotal = runObserved(query, threads, columnar = false)
          require(matches(rowTotal, query.expected), s"${query.name} row t$threads total")
          install(threads, columnar = true)
          val columnarFold = observedGroups(query, threads)
          require(
            columnarFold.hash == expected,
            s"${query.name} columnar t$threads group checksum ${columnarFold.hash} != $expected",
          )
          require(
            columnarFold.total == rowFold.total,
            s"${query.name} columnar t$threads group total ${columnarFold.total}",
          )
          val columnarTotal = runObserved(query, threads, columnar = true)
          require(
            matches(columnarTotal, query.expected),
            s"${query.name} columnar t$threads total",
          )
        }
      }
    }

  private def observedGroups(query: Query, threads: Int): GroupFold =
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      val fold = query.columnarGroups()
      require(
        sink.batchCount > 0L || sink.fusedCount > 0L,
        s"${query.name} columnar t$threads group check published no batch and folded nothing",
      )
      fold
    finally ColumnMetrics.restore(previous)

  private def rounds(queries: Vector[Query], count: Int, record: Boolean, offset: Int): Unit =
    var round = 0
    while (round < count) {
      val phase = round + offset
      val widthOrder = widthsFor(phase)
      val layoutOrder = layoutsFor(phase)
      widthOrder.foreach { threads =>
        layoutOrder.foreach { columnar =>
          install(threads, columnar)
          queries.foreach(query => sample(query, threads, columnar, round, record))
        }
      }
      val phaseName = if (record) then "measure" else "warm"
      System.err.println(s"$phaseName ${round + 1}/$count")
      round += 1
    }

  private def sample(
      query: Query,
      threads: Int,
      columnar: Boolean,
      round: Int,
      record: Boolean,
  ): Unit =
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      ScarletRuntime.get.shuffle.clear()
      val started = System.nanoTime()
      val got = query.run()
      val elapsed = System.nanoTime() - started
      requirePath(query.name, columnar, threads, sink.batchCount, sink.fusedCount)
      require(
        matches(got, query.expected),
        s"${query.name} ${layoutName(columnar)} t$threads ${show(got)} != ${show(query.expected)}",
      )
      if (record) then query.elapsed(layoutIndex(columnar))(widthIndex(threads))(round) = elapsed
    finally ColumnMetrics.restore(previous)

  private def runObserved(query: Query, threads: Int, columnar: Boolean): Answer =
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      ScarletRuntime.get.shuffle.clear()
      val got = query.run()
      requirePath(query.name, columnar, threads, sink.batchCount, sink.fusedCount)
      got
    finally ColumnMetrics.restore(previous)

  private def requirePath(
      name: String,
      columnar: Boolean,
      threads: Int,
      batches: Long,
      fused: Long,
  ): Unit =
    val layout = layoutName(columnar)
    // A count or sum over source columns publishes no batch and folds in place. Either note proves
    // the columnar arm ran; neither may appear on a row run.
    val why = s"$name $layout t$threads published $batches batches and $fused folds"
    if (columnar) then require(batches > 0L || fused > 0L, why)
    else require(batches == 0L && fused == 0L, why)

  /**
   * Process-wide bytes for every width and layout. The probe runs only in this pass, so it does
   * not sit on the timed samples.
   */
  private def allocate(queries: Vector[Query]): Boolean =
    startProbe() match
      case None =>
        System.err.println("allocate unavailable")
        false
      case Some(probe) =>
        try
          var round = 0
          while (round < allocRounds) {
            val phase = round + measureOffset
            widthsFor(phase).foreach { threads =>
              layoutsFor(phase).foreach { columnar =>
                install(threads, columnar)
                queries.foreach { query =>
                  val sink = new CountingSink
                  val previous = ColumnMetrics.install(sink)
                  try
                    val (got, bytes) = probe.account {
                      ScarletRuntime.get.shuffle.clear()
                      query.run()
                    }
                    requirePath(query.name, columnar, threads, sink.batchCount, sink.fusedCount)
                    require(
                      matches(got, query.expected),
                      s"${query.name} alloc ${show(got)}",
                    )
                    query.allocBytes(layoutIndex(columnar))(widthIndex(threads))(round) = bytes
                  finally ColumnMetrics.restore(previous)
                }
              }
            }
            System.err.println(s"allocate ${round + 1}/$allocRounds")
            round += 1
          }
          true
        finally probe.stop()

  private def report(allocOn: Boolean, queries: Vector[Query]): Unit =
    val header = Vector(
      preamble(allocOn),
      "",
      "| query | layout | threads | rows | p50 ms | p95 ms | max ms | alloc mb |",
      "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    )
    val body = queries.flatMap { query =>
      layouts.flatMap { columnar =>
        threadCounts.map(threads => tableRow(query, columnar, threads, allocOn))
      }
    }
    System.err.println((header ++ body ++ Vector("", losses(queries), "")).mkString("\n"))
    queries.foreach { query =>
      layouts.foreach { columnar =>
        threadCounts.foreach { threads =>
          println(jsonRow(query, columnar, threads, allocOn))
        }
      }
    }

  private def tableRow(query: Query, columnar: Boolean, threads: Int, allocOn: Boolean): String =
    val (median, tail, highest) = percentiles(query, columnar, threads)
    val alloc = allocText(query, columnar, threads, allocOn)
    s"| ${query.name} | ${layoutName(columnar)} | $threads | ${query.rows} | " +
      s"${millis(median)} | ${millis(tail)} | ${millis(highest)} | $alloc |"

  private def jsonRow(query: Query, columnar: Boolean, threads: Int, allocOn: Boolean): String =
    val (median, tail, highest) = percentiles(query, columnar, threads)
    val alloc = allocJson(query, columnar, threads, allocOn)
    val checksum = query.groupHash.getOrElse(checksumOf(query.expected))
    "{" +
      s""""schema":"$schemaName",""" +
      s""""query":"${query.name}",""" +
      s""""layout":"${layoutName(columnar)}",""" +
      s""""threads":$threads,""" +
      s""""rows":${query.rows},""" +
      s""""batch":$batchSize,""" +
      s""""rounds":$measure,""" +
      s""""p50_ms":${millis(median)},""" +
      s""""p95_ms":${millis(tail)},""" +
      s""""max_ms":${millis(highest)},""" +
      s""""alloc_mb":$alloc,""" +
      s""""checksum":$checksum}"""

  private def losses(queries: Vector[Query]): String =
    val lines = queries.flatMap { query =>
      threadCounts.flatMap { threads =>
        val columnar = percentiles(query, columnar = true, threads)._1
        val row = percentiles(query, columnar = false, threads)._1
        val gap = columnar - row
        if (gap > 0L) then Some(s"- ${query.name} threads=$threads ${signedMillis(gap)} ms")
        else None
      }
    }
    if (lines.isEmpty) then "Columnar p50 is at or below the row p50 on every query and width."
    else "Columnar p50 above the row p50:\n" + lines.mkString("\n")

  private def percentiles(query: Query, columnar: Boolean, threads: Int): (Long, Long, Long) =
    val samples = query.elapsed(layoutIndex(columnar))(widthIndex(threads)).clone()
    java.util.Arrays.sort(samples)
    val count = samples.length
    (samples(rankIndex(count, 50)), samples(rankIndex(count, 95)), samples(count - 1))

  private def allocText(
      query: Query,
      columnar: Boolean,
      threads: Int,
      allocOn: Boolean,
  ): String =
    allocMedian(query, columnar, threads, allocOn) match
      case None => "n/a"
      case Some(bytes) => megabytes(bytes)

  private def allocJson(
      query: Query,
      columnar: Boolean,
      threads: Int,
      allocOn: Boolean,
  ): String =
    allocMedian(query, columnar, threads, allocOn) match
      case None => "null"
      case Some(bytes) => megabytes(bytes)

  private def allocMedian(
      query: Query,
      columnar: Boolean,
      threads: Int,
      allocOn: Boolean,
  ): Option[Long] =
    if (!allocOn) then None
    else
      val samples = query.allocBytes(layoutIndex(columnar))(widthIndex(threads)).clone()
      java.util.Arrays.sort(samples)
      if (samples.exists(_ < 0L)) then None
      else Some(samples(rankIndex(samples.length, 50)))

  /** 1-based rank `ceil(percent / 100 * count)`, returned as a 0-based index. */
  private def rankIndex(count: Int, percent: Int): Int =
    val rank = (percent * count + 99) / 100
    math.max(1, math.min(count, rank)) - 1

  private def checkHarness(): Unit =
    require(measure % 4 == 0, s"measure=$measure does not split width and layout evenly")
    require(warmup > 0 && allocRounds > 0, "warmup and allocation rounds must be positive")
    val p50 = rankIndex(measure, 50)
    val p95 = rankIndex(measure, 95)
    require(p50 < p95, s"p50 index $p50 is not below p95 index $p95")
    require(p95 < measure - 2, s"p95 index $p95 is in the top two samples of $measure")
    val columnarLeads = Array(0, 0)
    val widthLeads = Array(0, 0)
    var round = 0
    while (round < measure) {
      val phase = round + measureOffset
      columnarLeads(layoutIndex(leader(layoutsFor(phase)))) += 1
      widthLeads(widthIndex(leader(widthsFor(phase)))) += 1
      round += 1
    }
    require(columnarLeads(0) == columnarLeads(1), "layout leaders are unbalanced")
    require(widthLeads(0) == widthLeads(1), "width leaders are unbalanced")
    val layoutRestarts = leader(layoutsFor(0)) == leader(layoutsFor(measureOffset))
    val widthRestarts = leader(widthsFor(0)) == leader(widthsFor(measureOffset))
    require(!(layoutRestarts && widthRestarts), "measure restarts in the warmup order")

  private def leader[A](order: Vector[A]): A =
    order match
      case head +: _ => head
      case _ => throw new IllegalStateException("order is empty")

  /** Width order follows bit 0. Layout order follows bit 1, so the two are not tied together. */
  private def widthsFor(phase: Int): Vector[Int] =
    if (phase % 2 == 0) then threadCounts else threadCounts.reverse

  private def layoutsFor(phase: Int): Vector[Boolean] =
    if ((phase / 2) % 2 == 0) then layouts else layouts.reverse

  private def install(threads: Int, columnar: Boolean): Unit =
    install(liveSchedulers, threads, columnar)

  private var liveSchedulers: Map[Int, LocalTaskScheduler] = Map.empty

  private def install(
      schedulers: Map[Int, LocalTaskScheduler],
      threads: Int,
      columnar: Boolean,
  ): Unit =
    liveSchedulers = schedulers
    val scheduler = schedulers.getOrElse(
      threads,
      throw new IllegalStateException(s"no scheduler for width $threads"),
    )
    val current = ScarletConf.get
    ScarletConf.set(
      current.copy(
        threadPoolSize = threads,
        columnarExecution = columnar,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = 4,
      ),
    )
    val runtime = ScarletRuntime.get
    ScarletRuntime.set(runtime.copy(scheduler = scheduler))

  private def preamble(allocOn: Boolean): String =
    val runtime = ManagementFactory.getRuntimeMXBean
    val alloc = if (allocOn) then "process-wide-estimate" else "unavailable"
    val p50Rank = rankIndex(measure, 50) + 1
    val p95Rank = rankIndex(measure, 95) + 1
    s"query bench warmup=$warmup measure=$measure alloc_rounds=$allocRounds " +
      s"batch=$batchSize partitions=$sourcePartitions alloc=$alloc\n" +
      s"percentile=nearest-rank ceil(p/100*n) no-interpolation n=$measure " +
      s"p50_rank=$p50Rank p95_rank=$p95Rank max_rank=$measure\n" +
      s"p95 is rank $p95Rank of $measure, so ${measure - p95Rank} sample(s) sit strictly above it. " +
      "It is not a stable tail. max is the largest sample.\n" +
      "runMain executes in the sbt JVM with Test / fork left false. Absolute numbers carry " +
      "sbt and JIT noise. Comparisons within this run are the signal.\n" +
      "Widths 1 and 4 share one JVM. Width order and layout order flip on different bits of " +
      "the round, and each leads on half of the measured rounds. Measure round 0 uses the " +
      "order of warmup round 1. This is not a fresh-JVM isolation.\n" +
      "Each sample fails the run unless columnar published at least one column batch and row " +
      "published none.\n" +
      "Grouped checksums fold (key * 31) xor the per-group sum, in key order. The row oracle " +
      "reads the pairs. The columnar check reads sumByKey. Both run before warmup and after " +
      "the measured rounds, at each width.\n" +
      "Sort queries checksum the collected key then value, mixed by 31, in output order. The " +
      "expected value is a local stable sort of the kept pairs, computed before timing.\n" +
      "filter-count, filter-groupbykey, filter-countbykey, filter-string-groupbykey, " +
      "filter-reducebykey, filter-string-reducebykey, filter-join, filter-string-join, and " +
      "filter-sort are two filters, at 1e6 rows, two source partitions, and batch 8192. String " +
      "keys are `(row % 1024).toString`. The int join filters the build side and probes an " +
      "unfiltered unique key. filter-string-join filters the build side and probes one row per " +
      "string key, with that integer as the right value. groupByKey checksums (key * 31) xor " +
      "the value sum xor the value count. countByKey checksums (key * 31) xor the kept-row " +
      "count over the same kept rows. String cases use key.hashCode. reduceByKey checksums " +
      "(key * 31) xor the wrapped int sum. The join checksums (key * 31) xor the left value " +
      "xor the right value. The string join uses key.hashCode in place of the key. " +
      "filter-sort uses the sort checksum. filter-count checksums the kept row count. " +
      "p50 and alloc_mb are recorded. There is no wall-clock gate.\n" +
      "alloc_mb is the nearest-rank p50 of process-wide thread bytes, in decimal megabytes " +
      "(bytes / 1e6). The probe excludes itself. A short-lived worker missed between reads is " +
      "absent. The same estimate is reported for row and columnar, at both widths.\n" +
      s"java=${runtime.getVmName} ${runtime.getVmVersion}"

  private def checksumOf(answer: Answer): Long = answer match
    case Answer.Whole(value) => value
    case Answer.Real(value) => java.lang.Double.doubleToLongBits(value)

  private def matches(got: Answer, expected: Answer): Boolean = (got, expected) match
    case (Answer.Whole(left), Answer.Whole(right)) => left == right
    case (Answer.Real(left), Answer.Real(right)) =>
      java.lang.Double.doubleToLongBits(left) == java.lang.Double.doubleToLongBits(right)
    case _ => false

  private def show(answer: Answer): String = answer match
    case Answer.Whole(value) => value.toString
    case Answer.Real(value) => String.format(Locale.ENGLISH, "%.6f", value)

  private def millis(nanos: Long): String =
    String.format(Locale.ENGLISH, "%.3f", nanos.toDouble / 1000000.0)

  private def signedMillis(nanos: Long): String =
    String.format(Locale.ENGLISH, "%+.3f", nanos.toDouble / 1000000.0)

  private def megabytes(bytes: Long): String =
    String.format(Locale.ENGLISH, "%.3f", bytes.toDouble / 1000000.0)

  private def sameText(left: String, right: String): Boolean =
    left.compareTo(right) == 0

  private def layoutName(columnar: Boolean): String =
    if (columnar) then "columnar" else "row"

  private def layoutIndex(columnar: Boolean): Int =
    if (columnar) then 0 else 1

  private def widthIndex(threads: Int): Int =
    if (threads == 1) then 0 else 1

  private def startProbe(): Option[AllocProbe] =
    threadBean.filter(_.isThreadAllocatedMemorySupported).flatMap { bean =>
      bean.setThreadAllocatedMemoryEnabled(true)
      if (bean.isThreadAllocatedMemoryEnabled) then Some(new AllocProbe(bean)) else None
    }

  private def threadBean: Option[ThreadMXBean] =
    ManagementFactory.getThreadMXBean match
      case bean: ThreadMXBean => Some(bean)
      case _ => None
