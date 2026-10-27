package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.util.Locale
import java.util.concurrent.locks.LockSupport

import scala.util.control.NonFatal

import cats.effect.unsafe.implicits.global
import com.sun.management.ThreadMXBean

import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Times broadcast and hash joins built through [[com.ewoodbury.scarlet.api.DistCollection]].
 * Not a test, and not part of `make test`.
 *
 * The timed region is the action. Warmup rounds are discarded. The multiset digest is compared
 * outside the timer: per-key pair counts and a checksum over `(key, leftValue, rightValue)`. An
 * outer join's missing side is `None` in that mix. Row and columnar digests must agree whenever
 * both paths return. A mismatch fails the run. A case that cannot show that equality fails
 * instead of printing a time. Raw order is not compared.
 *
 * The printed `strategy` column is the path that layout ran. For an inner join the row-path value
 * comes from the case's declared hint and sizes. The columnar value is `broadcast` when the
 * physical tree is a broadcast join, and `shuffle-hash` otherwise. An outer join's row path is
 * always `shuffle-hash`: the row executor ignores the hint. The columnar path follows the hint.
 * Absent is `unhinted` and still shuffle-hashes. Sort-merge stays on the row path. A null value
 * lowers to an outer shape and then stays on the row path; that case publishes no column batch,
 * so the run fails it instead of labeling the row path as columnar.
 * `broadcastJoinThreshold` and `enableSortMergeJoin` are restored after every case.
 *
 * `runMain` executes in the sbt JVM. `Test / fork` is left false, so absolute times carry sbt and
 * JIT noise. Comparisons within one run are the signal. Width 4 shares that JVM with width 1.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.JoinBench"
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.JoinBench outer"
 * }}}
 */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
object JoinBench:

  private val schemaName = "join-bench-2"
  private[execution] val warmup = 2
  private[execution] val measure = 5
  private val measureOffset = 1
  private val allocRounds = 2
  private val batchSize = 8192
  private val shufflePartitions = 4
  private val threadCounts = Vector(1, 4)
  private val layouts = Vector(true, false)

  /** One timed case. `digest` is the row digest from the oracle, filled before timing. */
  private final class Measured(
      val spec: JoinCases.JoinSpec,
      val accepted: Boolean,
      val shape: String,
  ):
    val elapsed: Array[Array[Array[Long]]] = Array.ofDim[Long](2, 2, measure)
    val allocBytes: Array[Array[Array[Long]]] = Array.ofDim[Long](2, 2, allocRounds)
    var digest: Option[JoinDigest.Digest] = None
    var failure: Option[Throwable] = None

    (0 until 2).foreach { layout =>
      (0 until 2).foreach { width =>
        java.util.Arrays.fill(allocBytes(layout)(width), -1L)
        java.util.Arrays.fill(elapsed(layout)(width), -1L)
      }
    }

    def runnable: Boolean = failure.isEmpty

  private[execution] final case class Observation(digest: JoinDigest.Digest, batches: Long)

  def main(args: Array[String]): Unit =
    val started = System.nanoTime()
    checkHarness()
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val schedulers = threadCounts.map { threads =>
      threads -> new LocalTaskScheduler(threads)
    }.toMap
    val selected = suites(args)
    try
      install(schedulers, threads = 1, columnar = true)
      val (fitting, overflow) = selected.partition(fitsJoinBuffer)
      val cases = prepare(fitting)
      val runnable = cases.filter(_.runnable)
      val blocked = cases.filterNot(_.runnable)
      if (runnable.nonEmpty) then
        System.err.println(s"warm $warmup")
        rounds(runnable, warmup, record = false, offset = 0)
        System.err.println(s"measure $measure")
        rounds(runnable, measure, record = true, offset = measureOffset)
        verify(runnable, "after")
        System.err.println(s"allocate $allocRounds")
        val allocOn = allocate(runnable)
        report(allocOn, runnable, blocked, started)
      else report(allocOn = false, runnable, blocked, started)
      overflow.foreach(attemptOverflow)
      if (blocked.nonEmpty) then failClosed(blocked)
    finally
      val wall = System.nanoTime() - started
      System.err.println(s"wall_s=${seconds(wall)}")
      try schedulers.values.foreach(_.shutdown().unsafeRunSync())
      finally
        ScarletConf.set(savedConf)
        ScarletRuntime.set(savedRuntime)

  private def suites(args: Array[String]): Vector[JoinCases.JoinSpec] =
    args.toList match
      case Nil | "inner" :: Nil => JoinCases.full
      case "outer" :: Nil => JoinCases.outer
      case other =>
        throw new IllegalArgumentException(
          s"usage: JoinBench [inner|outer], got ${other.mkString(" ")}",
        )

  private[execution] def checkHarness(): Unit =
    require(warmup == 2 && measure == 5, s"warmup=$warmup measure=$measure")
    require(warmup > 0 && allocRounds > 0, "warmup and allocation rounds must be positive")
    require(rankIndex(measure, 50) == 2, "p50 rank drifted")
    require(rankIndex(measure, 99) == measure - 1, "p99 rank drifted")
    require(rankIndex(measure, 50) < rankIndex(measure, 99), "p50 is not below p99")

  /** 1-based rank `ceil(percent / 100 * count)`, returned as a 0-based index. */
  private[execution] def rankIndex(count: Int, percent: Int): Int =
    val rank = (percent * count + 99) / 100
    math.max(1, math.min(count, rank)) - 1

  private[execution] def mean(samples: Array[Long]): Double =
    var index = 0
    var total = 0L
    while (index < samples.length) {
      total += samples(index)
      index += 1
    }
    total.toDouble / samples.length.toDouble

  /** Records `nanos` only when `record` is true, so warmup can call the same path. */
  private[execution] def assign(
      samples: Array[Long],
      round: Int,
      nanos: Long,
      record: Boolean,
  ): Unit =
    if (record) then samples(round) = nanos

  /**
   * Restores `broadcastJoinThreshold` and `enableSortMergeJoin` to the values from before the
   * call, including when `body` throws or changes them.
   */
  private[execution] def withJoinSettings[A](
      threshold: Long,
      sortMerge: Boolean,
      threads: Int,
      columnar: Boolean,
  )(body: => A): A =
    val current = ScarletConf.get
    try
      ScarletConf.set(
        current.copy(
          threadPoolSize = threads,
          columnarExecution = columnar,
          columnarBatchSize = batchSize,
          defaultShufflePartitions = shufflePartitions,
          broadcastJoinThreshold = threshold,
          enableSortMergeJoin = sortMerge,
        ),
      )
      body
    finally
      val after = ScarletConf.get
      ScarletConf.set(
        after.copy(
          broadcastJoinThreshold = current.broadcastJoinThreshold,
          enableSortMergeJoin = current.enableSortMergeJoin,
        ),
      )

  /** One execution. The caller restores the process conf if this changed more than join settings. */
  private[execution] def digestOf(
      spec: JoinCases.JoinSpec,
      threads: Int,
      columnar: Boolean,
  ): Observation =
    val baseline = ScarletConf.get
    try
      if (liveSchedulers.nonEmpty) then install(threads, columnar)
      withJoinSettings(spec.threshold, spec.sortMerge, threads, columnar) {
        val sink = new CountingSink
        val previous = ColumnMetrics.install(sink)
        try
          ScarletRuntime.get.shuffle.clear()
          val digest = JoinCases.digest(spec)
          Observation(digest, sink.batchCount)
        finally ColumnMetrics.restore(previous)
      }
    finally ScarletConf.set(baseline)

  /** `None` when the hint keeps the plan on the row path. Sort-merge is the remaining refusal. */
  private[execution] def columnarShape(spec: JoinCases.JoinSpec): Option[String] =
    val saved = ScarletConf.get
    try
      ScarletConf.set(saved.copy(columnarExecution = true))
      ColumnarPlanner.lower(spec.plan).map(_.shape)
    finally ScarletConf.set(saved)

  /** The join writes an `Int`-length buffer, so a larger defined output cannot be returned. */
  private def fitsJoinBuffer(spec: JoinCases.JoinSpec): Boolean =
    spec.expectedPairs <= Int.MaxValue.toLong

  private def prepare(specs: Vector[JoinCases.JoinSpec]): Vector[Measured] =
    specs.map { spec =>
      val shape = columnarShape(spec)
      val measured = new Measured(spec, shape.isDefined, shape.getOrElse("refused"))
      System.err.println(
        s"case ${spec.name} rows=${spec.leftRows}x${spec.rightRows} " +
          s"hint=${JoinCases.hintLabel(spec)} strategy=${JoinCases.rowStrategy(spec)} " +
          s"columnar=${measured.shape}",
      )
      if (!measured.accepted) then
        val why = JoinCases.hint(spec) match
          case Some(strategy) => s"${JoinCases.strategyName(strategy)} hint"
          case None => "plan"
        System.err.println(s"${spec.name} columnar refused: $why")
      loadOracle(measured)
      measured
    }

  /**
   * Collects both paths outside the timer. An engine exception skips the case. A returned pair of
   * digests that disagree, or a pair count other than the generator's, fails the run.
   */
  private def loadOracle(item: Measured): Unit =
    val attempts = threadCounts.map { threads =>
      threads -> readPair(item, threads)
    }
    val failure = attempts.collectFirst { case (_, Left(thrown)) => thrown }
    failure match
      case Some(thrown) =>
        item.failure = Some(thrown)
        System.err.println(s"${item.spec.name} not measured: ${showFailure(thrown)}")
      case None =>
        val digests = attempts.collect { case (threads, Right(digest)) => (threads, digest) }
        digests.foreach { case (threads, digest) =>
          require(
            digest.total == item.spec.expectedPairs,
            s"${item.spec.name} before t$threads pairs ${digest.total} != ${item.spec.expectedPairs}",
          )
        }
        digests.sliding(2).foreach {
          case Seq((_, left), (threads, right)) =>
            requireAgree(item, left, right, "before", threads)
          case _ => ()
        }
        item.digest = digests.headOption.map(_._2)

  @SuppressWarnings(Array("org.wartremover.warts.Nothing"))
  private def attempt(
      spec: JoinCases.JoinSpec,
      threads: Int,
      columnar: Boolean,
  ): Either[Throwable, Observation] =
    try
      val observed: Observation = digestOf(spec, threads, columnar)
      Right(observed)
    catch case NonFatal(thrown) => Left(thrown)

  @SuppressWarnings(Array("org.wartremover.warts.Nothing"))
  private def readPair(item: Measured, threads: Int): Either[Throwable, JoinDigest.Digest] =
    attempt(item.spec, threads, columnar = false) match
      case Left(thrown) => Left(thrown)
      case Right(observed) =>
        requirePath(item, columnar = false, threads, observed.batches)
        if (!item.accepted) then Right(observed.digest)
        else
          attempt(item.spec, threads, columnar = true) match
            case Left(thrown) => Left(thrown)
            case Right(other) =>
              requirePath(item, columnar = true, threads, other.batches)
              requireAgree(item, observed.digest, other.digest, "before", threads)
              Right(observed.digest)

  /**
   * Runs after the table. The defined output does not fit an `Int`-length join buffer. The attempt
   * is still made so the failure text is the engine's. `OutOfMemoryError` is printed and not
   * rethrown; the summary exception carries the same text.
   */
  private def attemptOverflow(spec: JoinCases.JoinSpec): Unit =
    System.err.println(
      s"attempt ${spec.name} defined_pairs=${spec.expectedPairs} " +
        s"strategy=${JoinCases.rowStrategy(spec)}",
    )
    System.err.flush()
    try
      val observed: Observation = digestOf(spec, threads = 1, columnar = false)
      val message = s"${spec.name} returned ${observed.digest.total} pairs"
      System.err.println(message)
      throw new IllegalStateException(message)
    catch
      case thrown: IllegalStateException => throw thrown
      case thrown: Throwable =>
        val message = s"${spec.name} not measured: ${showFailure(thrown)}"
        System.err.println(message)
        System.err.flush()
        throw new IllegalStateException(message)

  private def verify(cases: Vector[Measured], phase: String): Unit =
    threadCounts.foreach { threads =>
      cases.foreach { item =>
        val row = checkedDigest(item, threads, columnar = false, phase)
        item.digest match
          case None => item.digest = Some(row)
          case Some(previous) => requireAgree(item, previous, row, phase, threads)
        if (item.accepted) then
          val columnar = checkedDigest(item, threads, columnar = true, phase)
          requireAgree(item, row, columnar, phase, threads)
      }
    }

  private def checkedDigest(
      item: Measured,
      threads: Int,
      columnar: Boolean,
      phase: String,
  ): JoinDigest.Digest =
    val observed = digestOf(item.spec, threads, columnar)
    requirePath(item, columnar, threads, observed.batches)
    require(
      observed.digest.total == item.spec.expectedPairs,
      s"${item.spec.name} $phase ${layoutName(columnar)} t$threads " +
        s"pairs ${observed.digest.total} != ${item.spec.expectedPairs}",
    )
    observed.digest

  private def requireAgree(
      item: Measured,
      left: JoinDigest.Digest,
      right: JoinDigest.Digest,
      phase: String,
      threads: Int,
  ): Unit =
    require(
      JoinDigest.agrees(left, right),
      s"${item.spec.name} $phase t$threads ${JoinDigest.difference(left, right)}",
    )

  private def rounds(
      cases: Vector[Measured],
      count: Int,
      record: Boolean,
      offset: Int,
  ): Unit =
    var round = 0
    while (round < count) {
      val phase = round + offset
      widthsFor(phase).foreach { threads =>
        layoutsFor(phase).foreach { columnar =>
          cases.foreach { item =>
            if (columnar && !item.accepted) then ()
            else sample(item, threads, columnar, round, record)
          }
        }
      }
      val phaseName = if (record) then "measure" else "warm"
      System.err.println(s"$phaseName ${round + 1}/$count")
      round += 1
    }

  private def sample(
      item: Measured,
      threads: Int,
      columnar: Boolean,
      round: Int,
      record: Boolean,
  ): Unit =
    install(threads, columnar)
    withJoinSettings(item.spec.threshold, item.spec.sortMerge, threads, columnar) {
      val sink = new CountingSink
      val previous = ColumnMetrics.install(sink)
      try
        ScarletRuntime.get.shuffle.clear()
        val started = System.nanoTime()
        val pairs = JoinCases.count(item.spec)
        val elapsed = System.nanoTime() - started
        requirePath(item, columnar, threads, sink.batchCount)
        val expected = item.digest.getOrElse(JoinDigest.empty).total
        require(
          pairs == expected,
          s"${item.spec.name} ${layoutName(columnar)} t$threads pairs $pairs != $expected",
        )
        assign(item.elapsed(layoutIndex(columnar))(widthIndex(threads)), round, elapsed, record)
      finally ColumnMetrics.restore(previous)
    }

  private def requirePath(item: Measured, columnar: Boolean, threads: Int, batches: Long): Unit =
    val layout = layoutName(columnar)
    if (columnar) then
      require(
        batches > 0L,
        s"${item.spec.name} $layout t$threads published no column batch",
      )
    else
      require(
        batches == 0L,
        s"${item.spec.name} $layout t$threads published $batches column batches",
      )

  private def allocate(cases: Vector[Measured]): Boolean =
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
                cases.foreach { item =>
                  if (columnar && !item.accepted) then ()
                  else
                    install(threads, columnar)
                    withJoinSettings(item.spec.threshold, item.spec.sortMerge, threads, columnar) {
                      val sink = new CountingSink
                      val previous = ColumnMetrics.install(sink)
                      try
                        val (pairs, bytes) = probe.account {
                          ScarletRuntime.get.shuffle.clear()
                          JoinCases.count(item.spec)
                        }
                        requirePath(item, columnar, threads, sink.batchCount)
                        val expected = item.digest.getOrElse(JoinDigest.empty).total
                        require(pairs == expected, s"${item.spec.name} alloc pairs $pairs")
                        item.allocBytes(layoutIndex(columnar))(widthIndex(threads))(round) = bytes
                      finally ColumnMetrics.restore(previous)
                    }
                }
              }
            }
            System.err.println(s"allocate ${round + 1}/$allocRounds")
            round += 1
          }
          true
        finally probe.stop()

  private def report(
      allocOn: Boolean,
      cases: Vector[Measured],
      blocked: Vector[Measured],
      started: Long,
  ): Unit =
    val header = Vector(
      preamble(allocOn),
      "",
      "| case | layout | threads | rows | hint | strategy | p50 ms | mean ms | p99 ms | " +
        "alloc mb |",
      "| --- | --- | ---: | --- | --- | --- | ---: | ---: | ---: | ---: |",
    )
    val body = cases.flatMap { item =>
      val rows = layouts.flatMap { columnar =>
        if (columnar && !item.accepted) then Vector.empty[String]
        else threadCounts.map(threads => tableRow(item, columnar, threads, allocOn))
      }
      rows
    }
    val notes = checksumNotes(cases) ++ blockedNotes(blocked)
    val soFar = System.nanoTime() - started
    System.err.println((header ++ body ++ notes ++ Vector(s"wall_s=${seconds(soFar)}")).mkString("\n"))
    cases.foreach { item =>
      layouts.foreach { columnar =>
        if (!(columnar && !item.accepted)) then
          threadCounts.foreach { threads =>
            println(jsonRow(item, columnar, threads, allocOn))
          }
      }
    }

  private def failClosed(blocked: Vector[Measured]): Nothing =
    val text = blocked.map { item =>
      s"${item.spec.name}: ${item.failure.fold("unspecified")(showFailure)}"
    }.mkString("; ")
    throw new IllegalStateException(text)

  private def tableRow(
      item: Measured,
      columnar: Boolean,
      threads: Int,
      allocOn: Boolean,
  ): String =
    val (median, average, tail) = percentiles(item, columnar, threads)
    val alloc = allocText(item, columnar, threads, allocOn)
    s"| ${item.spec.name} | ${layoutName(columnar)} | $threads | ${rowsOf(item)} | " +
      s"${JoinCases.hintLabel(item.spec)} | ${strategyOf(item, columnar)} | " +
      s"${millis(median)} | ${meanMillis(average)} | ${millis(tail)} | $alloc |"

  private def jsonRow(
      item: Measured,
      columnar: Boolean,
      threads: Int,
      allocOn: Boolean,
  ): String =
    val (median, average, tail) = percentiles(item, columnar, threads)
    val alloc = allocJson(item, columnar, threads, allocOn)
    val digest = item.digest.getOrElse(JoinDigest.empty)
    "{" +
      s""""schema":"$schemaName",""" +
      s""""case":"${item.spec.name}",""" +
      s""""layout":"${layoutName(columnar)}",""" +
      s""""threads":$threads,""" +
      s""""left_rows":${item.spec.leftRows},""" +
      s""""right_rows":${item.spec.rightRows},""" +
      s""""hint":"${JoinCases.hintLabel(item.spec)}",""" +
      s""""strategy":"${strategyOf(item, columnar)}",""" +
      s""""rounds":$measure,""" +
      s""""p50_ms":${millis(median)},""" +
      s""""mean_ms":${meanMillis(average)},""" +
      s""""p99_ms":${millis(tail)},""" +
      s""""alloc_mb":$alloc,""" +
      s""""pairs":${digest.total},""" +
      s""""checksum":${digest.checksum}}"""

  private def percentiles(item: Measured, columnar: Boolean, threads: Int): (Long, Double, Long) =
    val samples = item.elapsed(layoutIndex(columnar))(widthIndex(threads)).clone()
    require(samples.forall(_ >= 0L), s"${item.spec.name} missing sample")
    val average = mean(samples)
    java.util.Arrays.sort(samples)
    val count = samples.length
    (samples(rankIndex(count, 50)), average, samples(rankIndex(count, 99)))

  private def meanMillis(averageNanos: Double): String =
    String.format(Locale.ENGLISH, "%.3f", averageNanos / 1000000.0)

  private def strategyOf(item: Measured, columnar: Boolean): String =
    if (!columnar) then JoinCases.rowStrategy(item.spec)
    else if (item.shape.startsWith("Broadcast")) then "broadcast"
    else "shuffle-hash"

  private def columnarPath(item: Measured): String =
    if (!item.accepted) then "refused" else strategyOf(item, columnar = true)

  private def rowsOf(item: Measured): String =
    s"${item.spec.leftRows}x${item.spec.rightRows}"

  private def checksumNotes(cases: Vector[Measured]): Vector[String] =
    val lines = cases.map { item =>
      val digest = item.digest.getOrElse(JoinDigest.empty)
      s"${item.spec.name} pairs=${digest.total} checksum=${digest.checksum} " +
        s"hint=${JoinCases.hintLabel(item.spec)} row=${JoinCases.rowStrategy(item.spec)} " +
        s"columnar=${columnarPath(item)} shape=${item.shape}"
    }
    Vector("") ++ lines

  private def blockedNotes(blocked: Vector[Measured]): Vector[String] =
    if (blocked.isEmpty) then Vector.empty
    else
      Vector("") ++ blocked.map { item =>
        s"${item.spec.name} not measured: ${item.failure.fold("unspecified")(showFailure)}"
      }

  private def showFailure(thrown: Throwable): String =
    val message = Option(thrown.getMessage).getOrElse("")
    s"${thrown.getClass.getSimpleName}: $message"

  private def allocText(
      item: Measured,
      columnar: Boolean,
      threads: Int,
      allocOn: Boolean,
  ): String =
    allocMedian(item, columnar, threads, allocOn) match
      case None => "n/a"
      case Some(bytes) => megabytes(bytes)

  private def allocJson(
      item: Measured,
      columnar: Boolean,
      threads: Int,
      allocOn: Boolean,
  ): String =
    allocMedian(item, columnar, threads, allocOn) match
      case None => "null"
      case Some(bytes) => megabytes(bytes)

  private def allocMedian(
      item: Measured,
      columnar: Boolean,
      threads: Int,
      allocOn: Boolean,
  ): Option[Long] =
    if (!allocOn) then None
    else
      val samples = item.allocBytes(layoutIndex(columnar))(widthIndex(threads)).clone()
      java.util.Arrays.sort(samples)
      if (samples.exists(_ < 0L)) then None
      else Some(samples(rankIndex(samples.length, 50)))

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
        defaultShufflePartitions = shufflePartitions,
      ),
    )
    val runtime = ScarletRuntime.get
    ScarletRuntime.set(runtime.copy(scheduler = scheduler))

  private def preamble(allocOn: Boolean): String =
    val runtime = ManagementFactory.getRuntimeMXBean
    val alloc = if (allocOn) then "process-wide-estimate" else "unavailable"
    val heapMb = megabytes(Runtime.getRuntime.maxMemory)
    val p50Rank = rankIndex(measure, 50) + 1
    val p99Rank = rankIndex(measure, 99) + 1
    s"join bench warmup=$warmup measure=$measure alloc_rounds=$allocRounds " +
      s"batch=$batchSize partitions=${JoinCases.sourcePartitions} " +
      s"shuffle=$shufflePartitions alloc=$alloc heap_mb=$heapMb\n" +
      s"percentile=nearest-rank ceil(p/100*n) no-interpolation n=$measure " +
      s"p50_rank=$p50Rank p99_rank=$p99Rank\n" +
      s"p99 is rank $p99Rank of $measure, so it is the largest sample.\n" +
      "runMain executes in the sbt JVM with Test / fork left false. Absolute numbers carry " +
      "sbt and JIT noise. Comparisons within this run are the signal.\n" +
      "Widths 1 and 4 share one JVM. Width order and layout order flip on different bits of " +
      "the round. Measure round 0 uses the order of warmup round 1.\n" +
      "The digest sums a mix of (key, left value, right value) and counts pairs per key, null " +
      "included. A missing outer-join side is None, which is not the value 0 and not a null " +
      "value. Row and columnar digests are compared outside the timer. A mismatch fails the " +
      "run.\n" +
      "hint is the plan's joinStrategy. unhinted means the hint is absent. strategy is the " +
      "path that layout ran. An outer join's row path is shuffle-hash for every hint. The " +
      "columnar path is broadcast when the physical tree is a broadcast join, and " +
      "shuffle-hash otherwise.\n" +
      "alloc_mb is the nearest-rank p50 of process-wide thread bytes, in decimal megabytes " +
      "(bytes / 1e6). The probe excludes itself.\n" +
      s"java=${runtime.getVmName} ${runtime.getVmVersion}"

  private def millis(nanos: Long): String =
    String.format(Locale.ENGLISH, "%.3f", nanos.toDouble / 1000000.0)

  private def megabytes(bytes: Long): String =
    String.format(Locale.ENGLISH, "%.3f", bytes.toDouble / 1000000.0)

  private def seconds(nanos: Long): String =
    String.format(Locale.ENGLISH, "%.3f", nanos.toDouble / 1000000000.0)

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

  /**
   * Sums bytes on every live thread except the probe. The watch reads while the action runs.
   * A worker that is never seen is missing. The caller's before/after pair is included.
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
