package com.ewoodbury.scarlet.execution

import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path}
import java.util.Locale

import scala.util.control.NonFatal

import cats.effect.unsafe.implicits.global

import com.ewoodbury.scarlet.api.DistCollection
import com.ewoodbury.scarlet.columnar.{ColumnMetrics, CountingSink, SortMergeRuns, SumSpill}
import com.ewoodbury.scarlet.core.ScarletConf
import com.ewoodbury.scarlet.runtime.ScarletRuntime
import com.ewoodbury.scarlet.runtime.local.LocalTaskScheduler

/**
 * Times grouped sum and sort-merge with spill off, at the default budget, and at a 64 KiB stress
 * budget. Not a test, and not part of `make test`.
 *
 * The timed region is the action. Two warmup rounds are discarded. Five rounds are measured.
 * Nearest-rank percentiles use 1-based rank `ceil(p / 100 * n)` with no interpolation. For five
 * samples, p50 is rank 3 and p95 is rank 5, the same sample as max.
 *
 * A [[CountingSink]] is installed outside the timer. A columnar run must publish at least one
 * column batch. The row oracle must publish none. Counters decide the spill path before a time is
 * kept. Stress must publish the file count [[checkHarness]] derives from the row sizing, and a
 * spilled sort-merge must complete the derived pass count. Spill off, and spill on at the default
 * budget, must publish no file. The matching `resetForTest` runs before every attempt. A run that
 * misses its batch count or its file count fails, and no time is printed.
 *
 * Before any sample, the stress result is compared with the in-memory columnar result and with
 * the row path (`columnarExecution` false). Grouped sum compares the folded total. Sort-merge
 * checks every joined row against the generators and compares the order-independent multiset
 * digest. Each measured result is compared again, outside the timer. A mismatch fails the run.
 *
 * Grouped sum is 1e6 rows in two source partitions. Keys are `row % groupedKeys`. The partial sum
 * folds a source partition before the spill check, so the exchanged bucket is what has to exceed
 * the stress budget. Sort-merge uses the default fan-in of 8. Keys `0` until the row count split
 * evenly across two shuffle partitions, and each side of a partition is one morsel past that
 * fan-in, so the merge depth is two.
 *
 * The default budget is [[ScarletConf]]'s 64 MiB. These inputs fit in it, so that row is spill
 * turned on with nothing written. It runs at width 1. The 64 KiB row is the forced-spill case and
 * runs at widths 1 and 4.
 *
 * `runMain` executes in the sbt JVM. `Test / fork` is left false, so absolute times carry sbt and
 * JIT noise. Comparisons within one run are the signal. Width 4 shares that JVM with width 1.
 *
 * {{{
 * sbt "scarlet-tests/Test/runMain com.ewoodbury.scarlet.execution.SpillBench"
 * }}}
 */
object SpillBench:

  private val schemaName = "spill-bench-2"
  private val warmup = 2
  private val measure = 5
  private val confDefaults = new ScarletConf()
  private val defaultBudgetBytes = confDefaults.spillBudgetBytes
  private val stressBudgetBytes = 64L * 1024L
  private val spillDirectoryName = "scarlet-spill-bench"
  private val fanIn = 8
  private val batchSize = 8192
  private val sourcePartitions = 2
  private val shufflePartitions = 2
  private val groupedRows = 1000000
  private val groupedKeys = 16384
  private val threadCounts = Vector(1, 4)
  private val works = Vector(Grouped, SortMerge)
  private val modes = Vector(Off, DefaultBudget, Stress)

  /**
   * `(fanIn + 1)` morsels on each side of each shuffle partition. Sequential keys split evenly
   * when the partition count is 2, so this is the row count that produces those morsels.
   */
  private val sortMergeRows = (fanIn + 1) * batchSize * shufflePartitions
  private val sideRows = (fanIn + 1) * batchSize
  private val groupedBucketRows = groupedKeys / shufflePartitions * sourcePartitions

  private sealed trait Work

  private case object Grouped extends Work

  private case object SortMerge extends Work

  private sealed trait Mode

  /** Spill off. The budget is the default and is not consulted. */
  private case object Off extends Mode

  /** Spill on at [[ScarletConf]]'s default budget. Width 1 only. These inputs write nothing. */
  private case object DefaultBudget extends Mode

  /** Spill on at 64 KiB, so every measured bucket and sort-merge partition spills. */
  private case object Stress extends Mode

  private final case class Counts(files: Long, passes: Long)

  private final case class Oracle(grouped: Long, joined: JoinDigest.Digest)

  private final case class GroupedSpill(pieces: Int, files: Long, maxPieceRows: Int)

  private final case class SortMergeSpill(files: Long, passes: Long, depth: Int)

  private final class Samples:
    val elapsed: Array[Array[Array[Array[Long]]]] =
      Array.ofDim[Long](works.length, modes.length, threadCounts.length, measure)
    val files: Array[Array[Array[Long]]] =
      Array.fill(works.length, modes.length, threadCounts.length)(-1L)
    val passes: Array[Array[Array[Long]]] =
      Array.fill(works.length, modes.length, threadCounts.length)(-1L)

    elapsed.indices.foreach { item =>
      elapsed(item).indices.foreach { mode =>
        elapsed(item)(mode).indices.foreach { width =>
          java.util.Arrays.fill(elapsed(item)(mode)(width), -1L)
        }
      }
    }

  def main(args: Array[String]): Unit =
    require(args.isEmpty, s"usage: SpillBench, got ${args.mkString(" ")}")
    val started = System.nanoTime()
    checkHarness()
    val savedConf = ScarletConf.get
    val savedRuntime = ScarletRuntime.get
    val directory = freshSpillDirectory()
    val schedulers = threadCounts.map { threads =>
      threads -> new LocalTaskScheduler(threads)
    }.toMap
    try
      val grouped = groupedSource()
      val (left, right) = sortMergeSides()
      val oracle = verify(savedConf, savedRuntime, schedulers, directory, grouped, left, right)
      val samples = new Samples
      System.err.println(s"warm $warmup")
      rounds(
        savedConf,
        savedRuntime,
        schedulers,
        directory,
        grouped,
        left,
        right,
        oracle,
        samples,
        warmup,
        record = false,
      )
      System.err.println(s"measure $measure")
      rounds(
        savedConf,
        savedRuntime,
        schedulers,
        directory,
        grouped,
        left,
        right,
        oracle,
        samples,
        measure,
        record = true,
      )
      report(samples, oracle)
    finally
      val wall = System.nanoTime() - started
      System.err.println(s"wall_s=${seconds(wall)}")
      try schedulers.values.foreach(_.shutdown().unsafeRunSync())
      finally
        try
          ScarletConf.set(savedConf)
          ScarletRuntime.set(savedRuntime)
        finally deleteSpillDirectory(directory)

  private def checkHarness(): Unit =
    require(warmup == 2 && measure == 5, s"warmup=$warmup measure=$measure")
    require(rankIndex(measure, 50) == 2, "p50 rank drifted")
    require(rankIndex(measure, 95) == measure - 1, "p95 rank drifted")
    works match
      case Vector(Grouped, SortMerge) => ()
      case _ => throw new IllegalStateException("work vector drifted")
    modes match
      case Vector(Off, DefaultBudget, Stress) => ()
      case _ => throw new IllegalStateException("mode vector drifted")
    threadCounts match
      case Vector(1, 4) => ()
      case _ => throw new IllegalStateException("width vector drifted")
    require(threadCounts.distinct.length == threadCounts.length, "duplicate width")
    require(widthsOf(Off).sameElements(Vector(1, 4)), "off widths drifted")
    widthsOf(DefaultBudget) match
      case Vector(1) => ()
      case _ => throw new IllegalStateException("default budget is width 1")
    require(widthsOf(Stress).sameElements(Vector(1, 4)), "stress widths drifted")
    threadCounts.zipWithIndex.foreach { case (threads, index) =>
      require(widthIndex(threads) == index, s"width $threads overwrites slot $index")
    }
    val shape = new Samples
    require(shape.elapsed.length == works.length, "work dimension drifted")
    require(shape.elapsed(0).length == modes.length, "mode dimension drifted")
    require(shape.elapsed(0)(0).length == threadCounts.length, "width dimension drifted")
    require(shape.elapsed(0)(0)(0).length == measure, "sample dimension drifted")
    require(shape.elapsed(0)(0)(0).forall(sample => sample < 0L), "samples start unset")
    require(shape.files.length == works.length && shape.passes.length == works.length)
    require(shape.files(0).length == modes.length && shape.passes(0).length == modes.length)
    require(
      shape.files(0)(0).length == threadCounts.length &&
        shape.passes(0)(0).length == threadCounts.length,
      "counter width dimension drifted",
    )
    require(shape.files(0)(0).forall(count => count < 0L), "file slots start unset")
    require(shape.passes(0)(0).forall(count => count < 0L), "pass slots start unset")
    require(defaultBudgetBytes == 64L * 1024L * 1024L, "default budget left 64 MiB")
    require(stressBudgetBytes == 64L * 1024L, "stress budget is 64 KiB")
    require(confDefaults.spillMaxFiles == 1024, "spillMaxFiles left its default")
    require(confDefaults.spillMaxBytes == 8L * 1024L * 1024L * 1024L, "spillMaxBytes left 8 GiB")
    require(fanIn == 8 && confDefaults.spillFanIn == fanIn, "fan-in left the default")
    require(spillDirectoryName.nonEmpty, "spill directory is unnamed")
    require(groupedRows == 1000000 && sourcePartitions == 2, "grouped input drifted")
    require(groupedKeys > 0 && groupedKeys % shufflePartitions == 0, "keys do not split")
    require(
      groupedBucketRows == groupedKeys / shufflePartitions * sourcePartitions,
      "grouped bucket rows drifted",
    )
    val groupedEstimate = SumSpill.retainedBytes(groupedBucketRows)
    require(groupedEstimate > stressBudgetBytes, s"grouped bucket of $groupedBucketRows does not exceed the stress budget")
    require(groupedEstimate <= defaultBudgetBytes, "grouped bucket exceeds the default budget")
    val grouped = groupedSpill(stressBudgetBytes)
    require(grouped.pieces >= 2, s"grouped split is ${grouped.pieces}")
    require(grouped.maxPieceRows > 0 && grouped.maxPieceRows <= batchSize, "piece morsels drifted")
    require(
      grouped.files == shufflePartitions.toLong * grouped.pieces.toLong,
      s"grouped files ${grouped.files} drifted from ${grouped.pieces} pieces",
    )
    require(grouped.files >= shufflePartitions.toLong, "a spilled bucket has no file")
    require(sortMergeRows == sideRows * shufflePartitions, "sort-merge rows drifted")
    val onOneSide = (0 until sortMergeRows).count { key =>
      Math.floorMod(key, shufflePartitions) == 0
    }
    require(onOneSide == sideRows, s"one side has $onOneSide rows, wanted $sideRows")
    require(sideRows % batchSize == 0, "sort-merge side is not a whole number of morsels")
    require(sideRows / batchSize == fanIn + 1, "sort-merge does not pass the fan-in by one")
    val pairBytes = SortMergeRuns.retainedBytes(sideRows) + SortMergeRuns.retainedBytes(sideRows)
    require(pairBytes > stressBudgetBytes, s"sort-merge pair of $pairBytes bytes does not exceed the stress budget")
    require(pairBytes <= defaultBudgetBytes, "sort-merge pair exceeds the default budget")
    val merge = sortMergeSpill()
    require(merge.depth == 2, s"merge depth ${merge.depth}")
    require(
      merge.passes == shufflePartitions.toLong * 2L * merge.depth.toLong,
      s"merge passes ${merge.passes} drifted",
    )
    require(merge.files >= shufflePartitions.toLong * 2L, "a spilled side has no file")
    require(expectedFiles(Grouped, Stress) == grouped.files, "grouped expectation drifted")
    require(expectedFiles(SortMerge, Stress) == merge.files, "sort-merge expectation drifted")
    require(expectedPasses(SortMerge, Stress) == merge.passes, "pass expectation drifted")
    require(expectedFiles(Grouped, Off) == 0L && expectedFiles(Grouped, DefaultBudget) == 0L)
    require(expectedFiles(SortMerge, Off) == 0L && expectedFiles(SortMerge, DefaultBudget) == 0L)
    require(expectedPasses(Grouped, Stress) == 0L, "grouped sum has no pass counter")

  /** 1-based rank `ceil(percent / 100 * count)`, returned as a 0-based index. */
  private def rankIndex(count: Int, percent: Int): Int =
    val rank = (percent * count + 99) / 100
    math.max(1, math.min(count, rank)) - 1

  /**
   * How many ways a stress bucket is split. Independent oracle for the bench, mirroring
   * [[SpillStore.splitCount]]: at least two pieces, `ceil(estimate / budget)`, and no more than
   * the row count or `spillMaxFiles - 1`.
   */
  private def pieceCount(rows: Int, estimated: Long, budget: Long): Int =
    val room = math.max(1, confDefaults.spillMaxFiles - 1)
    val cap = math.min(math.max(rows, 1), room)
    val splits = divideCeil(estimated, math.max(budget, 1L))
    val wanted = if (rows >= 2) then math.max(2, splits) else 1
    math.min(wanted, cap)

  private def divideCeil(numerator: Long, denominator: Long): Int =
    val quotient = math.max(0L, (numerator + denominator - 1L) / denominator)
    if (quotient > Int.MaxValue.toLong) then Int.MaxValue else quotient.toInt

  /**
   * Files a stress grouped sum publishes. Each source partition contributes one partial per key.
   * `floorMod` places that key, and each non-empty piece is cut into morsels of `batchSize`.
   */
  private def groupedSpill(budget: Long): GroupedSpill =
    val pieces = pieceCount(groupedBucketRows, SumSpill.retainedBytes(groupedBucketRows), budget)
    val counts = Array.fill(shufflePartitions, pieces)(0)
    (0 until groupedKeys).foreach { key =>
      val part = Math.floorMod(key, shufflePartitions)
      val piece = Math.floorMod(key, pieces)
      counts(part)(piece) += sourcePartitions
    }
    val maxPieceRows = counts.foldLeft(0) { (highest, part) =>
      part.foldLeft(highest)((soFar, rows) => if (rows > soFar) then rows else soFar)
    }
    val files = counts.map { part =>
      part.map(rows => morselCount(rows)).sum
    }.sum.toLong
    GroupedSpill(pieces, files, maxPieceRows)

  private def morselCount(rows: Int): Int =
    if (rows <= 0) then 0 else (rows + batchSize - 1) / batchSize

  /**
   * Files and completed passes a stress sort-merge publishes. Each side of each partition starts
   * as `sideRows / batchSize` runs. A pass merges at most `fanIn` runs and publishes a file only
   * for a group of two or more. A singleton group keeps its file. Passes sum over both sides.
   */
  private def sortMergeSpill(): SortMergeSpill =
    val runs = sideRows / batchSize
    val (extra, depth) = reduceShape(runs, fanIn)
    val sides = shufflePartitions.toLong * 2L
    SortMergeSpill(sides * (runs.toLong + extra.toLong), sides * depth.toLong, depth)

  private def reduceShape(runs: Int, fanIn: Int): (Int, Int) =
    def loop(current: Int, extra: Int, depth: Int): (Int, Int) =
      if (current <= 1) then (extra, depth)
      else
        val groups = (current + fanIn - 1) / fanIn
        val published = (0 until groups).count { index =>
          val start = index * fanIn
          math.min(fanIn, current - start) > 1
        }
        loop(groups, extra + published, depth + 1)
    loop(runs, 0, 0)

  private def expectedFiles(work: Work, mode: Mode): Long =
    mode match
      case Stress =>
        work match
          case Grouped => groupedSpill(stressBudgetBytes).files
          case SortMerge => sortMergeSpill().files
      case Off | DefaultBudget => 0L

  private def expectedPasses(work: Work, mode: Mode): Long =
    (work, mode) match
      case (SortMerge, Stress) => sortMergeSpill().passes
      case _ => 0L

  private def freshSpillDirectory(): Path =
    Files.createTempDirectory(
      Path.of(System.getProperty("java.io.tmpdir")),
      spillDirectoryName + "-",
    )

  /** A delete failure must not mask the bench result. Stores are already closed. */
  private def deleteSpillDirectory(directory: Path): Unit =
    try deleteTree(directory)
    catch case NonFatal(_) => ()

  private def deleteTree(path: Path): Unit =
    if (Files.isDirectory(path)) then
      val stream = Files.newDirectoryStream(path)
      try stream.forEach(deleteTree)
      finally stream.close()
    val _ = Files.deleteIfExists(path)

  private def groupedSource(): DistCollection[(Int, Int)] =
    val keys = Array.tabulate(groupedRows)(row => row % groupedKeys)
    val values = Array.tabulate(groupedRows)(row => row)
    DistCollection.ofIntPairs(keys, values, sourcePartitions)

  private def sortMergeSides(): (DistCollection[(Int, Int)], DistCollection[(Int, Int)]) =
    val keys = Array.tabulate(sortMergeRows)(row => row)
    val leftValues = Array.tabulate(sortMergeRows)(row => row + 1)
    val rightValues = Array.tabulate(sortMergeRows)(row => row * 3 + 7)
    val left = DistCollection.ofIntPairs(keys, leftValues, sourcePartitions)
    val right = DistCollection.ofIntPairs(keys, rightValues, sourcePartitions)
    (left, right)

  private def verify(
      baseline: ScarletConf,
      runtime: ScarletRuntime.RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      directory: Path,
      grouped: DistCollection[(Int, Int)],
      left: DistCollection[(Int, Int)],
      right: DistCollection[(Int, Int)],
  ): Oracle =
    val expected = groupedRows.toLong * (groupedRows - 1L) / 2L
    val spilledSum = groupedTotal(baseline, runtime, schedulers, directory, grouped, Stress)
    val memorySum = groupedTotal(baseline, runtime, schedulers, directory, grouped, Off)
    val rowSum = groupedOnRows(baseline, runtime, schedulers, directory, grouped)
    require(
      spilledSum == expected && memorySum == expected && rowSum == expected,
      s"grouped sum spilled=$spilledSum memory=$memorySum row=$rowSum expected=$expected",
    )
    val spilledJoin = joined(baseline, runtime, schedulers, directory, left, right, Stress, true)
    val memoryJoin = joined(baseline, runtime, schedulers, directory, left, right, Off, true)
    val rowJoin = joined(baseline, runtime, schedulers, directory, left, right, Off, false)
    requireGenerated(spilledJoin)
    requireGenerated(memoryJoin)
    requireGenerated(rowJoin)
    val spilledDigest = JoinDigest.from(spilledJoin)
    val memoryDigest = JoinDigest.from(memoryJoin)
    val rowDigest = JoinDigest.from(rowJoin)
    require(
      JoinDigest.agrees(spilledDigest, memoryDigest) && JoinDigest.agrees(spilledDigest, rowDigest),
      "sort-merge digest disagreed: " +
        s"spilled/memory ${JoinDigest.difference(spilledDigest, memoryDigest)}, " +
        s"spilled/row ${JoinDigest.difference(spilledDigest, rowDigest)}",
    )
    require(spilledDigest.total == sortMergeRows.toLong, s"pairs ${spilledDigest.total}")
    Oracle(expected, spilledDigest)

  /** One row per key: left is `key + 1`, right is `key * 3 + 7`. */
  private def requireGenerated(rows: Vector[(Int, (Int, Int))]): Unit =
    require(rows.length == sortMergeRows, s"pairs ${rows.length} != $sortMergeRows")
    val seen = Array.fill(sortMergeRows)(false)
    rows.foreach { case (key, (leftValue, rightValue)) =>
      require(key >= 0 && key < sortMergeRows, s"key $key outside 0 until $sortMergeRows")
      require(!seen(key), s"duplicate key $key")
      seen(key) = true
      require(leftValue == key + 1, s"key $key left $leftValue")
      require(rightValue == key * 3 + 7, s"key $key right $rightValue")
    }
    require(seen.forall(present => present), "a sort-merge key is missing")

  private def groupedTotal(
      baseline: ScarletConf,
      runtime: ScarletRuntime.RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      directory: Path,
      source: DistCollection[(Int, Int)],
      mode: Mode,
  ): Long =
    val attempt = runGrouped(baseline, runtime, schedulers, directory, source, threads = 1, mode)
    prove(Grouped, mode, threads = 1, attempt.counts, attempt.batches)
    attempt.total

  private def groupedOnRows(
      baseline: ScarletConf,
      runtime: ScarletRuntime.RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      directory: Path,
      source: DistCollection[(Int, Int)],
  ): Long =
    val attempt =
      runGrouped(baseline, runtime, schedulers, directory, source, threads = 1, Off, columnar = false)
    require(attempt.batches == 0L, s"row grouped sum published ${attempt.batches} column batches")
    require(attempt.counts.files == 0L && attempt.counts.passes == 0L, "row grouped sum spilled")
    attempt.total

  private def joined(
      baseline: ScarletConf,
      runtime: ScarletRuntime.RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      directory: Path,
      left: DistCollection[(Int, Int)],
      right: DistCollection[(Int, Int)],
      mode: Mode,
      columnar: Boolean,
  ): Vector[(Int, (Int, Int))] =
    val attempt = runSortMerge(
      baseline,
      runtime,
      schedulers,
      directory,
      left,
      right,
      threads = 1,
      mode,
      columnar,
    )
    if (columnar) then prove(SortMerge, mode, threads = 1, attempt.counts, attempt.batches)
    else
      require(attempt.batches == 0L, s"row sort-merge published ${attempt.batches} column batches")
      require(attempt.counts.files == 0L && attempt.counts.passes == 0L, "row sort-merge spilled")
    attempt.rows

  private def rounds(
      baseline: ScarletConf,
      runtime: ScarletRuntime.RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      directory: Path,
      grouped: DistCollection[(Int, Int)],
      left: DistCollection[(Int, Int)],
      right: DistCollection[(Int, Int)],
      oracle: Oracle,
      samples: Samples,
      times: Int,
      record: Boolean,
  ): Unit =
    (0 until times).foreach { round =>
      val label = if (record) then "measure" else "warm"
      System.err.println(s"$label ${round + 1}/$times")
      works.foreach { work =>
        modes.foreach { mode =>
          widthsOf(mode).foreach { threads =>
            attempt(
              baseline,
              runtime,
              schedulers,
              directory,
              grouped,
              left,
              right,
              oracle,
              samples,
              work,
              mode,
              threads,
              round,
              record,
            )
          }
        }
      }
    }

  private def attempt(
      baseline: ScarletConf,
      runtime: ScarletRuntime.RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      directory: Path,
      grouped: DistCollection[(Int, Int)],
      left: DistCollection[(Int, Int)],
      right: DistCollection[(Int, Int)],
      oracle: Oracle,
      samples: Samples,
      work: Work,
      mode: Mode,
      threads: Int,
      round: Int,
      record: Boolean,
  ): Unit =
    work match
      case Grouped =>
        val run = runGrouped(baseline, runtime, schedulers, directory, grouped, threads, mode)
        prove(Grouped, mode, threads, run.counts, run.batches)
        require(run.total == oracle.grouped, s"grouped checksum ${run.total}")
        keep(samples, Grouped, mode, threads, round, run.elapsed, run.counts, record)
      case SortMerge =>
        val run =
          runSortMerge(baseline, runtime, schedulers, directory, left, right, threads, mode, true)
        prove(SortMerge, mode, threads, run.counts, run.batches)
        require(
          JoinDigest.agrees(run.digest, oracle.joined),
          JoinDigest.difference(run.digest, oracle.joined),
        )
        keep(samples, SortMerge, mode, threads, round, run.elapsed, run.counts, record)

  private final case class GroupedRun(elapsed: Long, counts: Counts, total: Long, batches: Long)

  private def runGrouped(
      baseline: ScarletConf,
      runtime: ScarletRuntime.RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      directory: Path,
      source: DistCollection[(Int, Int)],
      threads: Int,
      mode: Mode,
      columnar: Boolean = true,
  ): GroupedRun =
    install(baseline, runtime, schedulers, directory, threads, columnar, mode)
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      SumSpill.resetForTest()
      val started = System.nanoTime()
      val total = source.groupedSum
      val elapsed = System.nanoTime() - started
      val counts = Counts(SumSpill.filesWrittenForTest, passes = 0L)
      GroupedRun(elapsed, counts, total, sink.batchCount)
    finally ColumnMetrics.restore(previous)

  private final case class SortMergeRun(
      elapsed: Long,
      counts: Counts,
      digest: JoinDigest.Digest,
      rows: Vector[(Int, (Int, Int))],
      batches: Long,
  )

  private def runSortMerge(
      baseline: ScarletConf,
      runtime: ScarletRuntime.RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      directory: Path,
      left: DistCollection[(Int, Int)],
      right: DistCollection[(Int, Int)],
      threads: Int,
      mode: Mode,
      columnar: Boolean,
  ): SortMergeRun =
    install(baseline, runtime, schedulers, directory, threads, columnar, mode)
    val sink = new CountingSink
    val previous = ColumnMetrics.install(sink)
    try
      SortMergeRuns.resetForTest()
      val started = System.nanoTime()
      val rows = left.sortMergeJoin(right).collect().toVector
      val elapsed = System.nanoTime() - started
      val counts = Counts(SortMergeRuns.filesWrittenForTest, SortMergeRuns.mergePassesForTest)
      SortMergeRun(elapsed, counts, JoinDigest.from(rows), rows, sink.batchCount)
    finally ColumnMetrics.restore(previous)

  /**
   * Keeps `elapsed` only after the counters agree with the rounds already kept. Warmup passes
   * `record` false and leaves the sample unset. A failed check writes nothing.
   */
  private def keep(
      samples: Samples,
      work: Work,
      mode: Mode,
      threads: Int,
      round: Int,
      elapsed: Long,
      counts: Counts,
      record: Boolean,
  ): Unit =
    if (record) then
      val item = caseIndex(work)
      val modeSlot = modeIndex(mode)
      val width = widthIndex(threads)
      val prior = samples.files(item)(modeSlot)(width)
      if (prior < 0L) then
        samples.files(item)(modeSlot)(width) = counts.files
        samples.passes(item)(modeSlot)(width) = counts.passes
      else
        require(
          prior == counts.files && samples.passes(item)(modeSlot)(width) == counts.passes,
          s"${nameOf(work)} counters changed across rounds",
        )
      samples.elapsed(item)(modeSlot)(width)(round) = elapsed

  private def prove(work: Work, mode: Mode, threads: Int, counts: Counts, batches: Long): Unit =
    val files = expectedFiles(work, mode)
    val passes = expectedPasses(work, mode)
    val ok = batches > 0L && counts.files == files && counts.passes == passes
    if (!ok) then
      throw new IllegalStateException(
        s"${nameOf(work)} ${modeName(mode)} threads=$threads batches=$batches " +
          s"files=${counts.files} expected=$files passes=${counts.passes} expectedPasses=$passes",
      )

  private def install(
      baseline: ScarletConf,
      runtime: ScarletRuntime.RuntimeComponents,
      schedulers: Map[Int, LocalTaskScheduler],
      directory: Path,
      threads: Int,
      columnar: Boolean,
      mode: Mode,
  ): Unit =
    val scheduler = schedulers.getOrElse(
      threads,
      throw new IllegalStateException(s"no scheduler for width $threads"),
    )
    ScarletConf.set(
      baseline.copy(
        threadPoolSize = threads,
        columnarExecution = columnar,
        columnarBatchSize = batchSize,
        defaultShufflePartitions = shufflePartitions,
        spillEnabled = spills(mode),
        spillBudgetBytes = budgetBytesOf(mode),
        spillFanIn = fanIn,
        spillDirectory = directory.toString,
        spillMaxFiles = confDefaults.spillMaxFiles,
        spillMaxBytes = confDefaults.spillMaxBytes,
      ),
    )
    ScarletRuntime.set(runtime.copy(scheduler = scheduler))
    ScarletRuntime.get.shuffle.clear()

  private def budgetBytesOf(mode: Mode): Long =
    mode match
      case Stress => stressBudgetBytes
      case Off | DefaultBudget => defaultBudgetBytes

  private def report(samples: Samples, oracle: Oracle): Unit =
    val header = Vector(
      preamble(),
      "",
      "| case | spill | budget | threads | rows | p50 ms | p95 ms | max ms | files_written | merge_passes |",
      "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    )
    val body = works.flatMap { work =>
      modes.flatMap { mode =>
        widthsOf(mode).map(threads => tableRow(samples, work, mode, threads))
      }
    }
    val notes = Vector(
      "",
      s"grouped-sum checksum=${oracle.grouped}",
      s"sort-merge pairs=${oracle.joined.total} checksum=${oracle.joined.checksum}",
    )
    System.err.println((header ++ body ++ notes).mkString("\n"))
    works.foreach { work =>
      modes.foreach { mode =>
        widthsOf(mode).foreach { threads =>
          println(jsonRow(samples, oracle, work, mode, threads))
        }
      }
    }

  private def tableRow(samples: Samples, work: Work, mode: Mode, threads: Int): String =
    val (median, tail, highest) = percentiles(samples, work, mode, threads)
    val counts = recorded(samples, work, mode, threads)
    s"| ${nameOf(work)} | ${spillLabel(mode)} | ${budgetLabel(mode)} | $threads | ${rowsOf(work)} | " +
      s"${millis(median)} | ${millis(tail)} | ${millis(highest)} | ${counts.files} | ${counts.passes} |"

  private def jsonRow(
      samples: Samples,
      oracle: Oracle,
      work: Work,
      mode: Mode,
      threads: Int,
  ): String =
    val (median, tail, highest) = percentiles(samples, work, mode, threads)
    val counts = recorded(samples, work, mode, threads)
    val checksum = work match
      case Grouped => oracle.grouped
      case SortMerge => oracle.joined.checksum
    "{" +
      s""""schema":"$schemaName",""" +
      s""""case":"${nameOf(work)}",""" +
      s""""spill":${spills(mode)},""" +
      s""""mode":"${modeName(mode)}",""" +
      s""""budget_bytes":${budgetBytesOf(mode)},""" +
      s""""threads":$threads,""" +
      s""""rows":${rowsOf(work)},""" +
      s""""rounds":$measure,""" +
      s""""p50_ms":${millis(median)},""" +
      s""""p95_ms":${millis(tail)},""" +
      s""""max_ms":${millis(highest)},""" +
      s""""files_written":${counts.files},""" +
      s""""merge_passes":${counts.passes},""" +
      s""""checksum":$checksum}"""

  private def percentiles(
      samples: Samples,
      work: Work,
      mode: Mode,
      threads: Int,
  ): (Long, Long, Long) =
    val taken = samples.elapsed(caseIndex(work))(modeIndex(mode))(widthIndex(threads)).clone()
    require(taken.forall(sample => sample >= 0L), s"${nameOf(work)} ${modeName(mode)} t$threads missing")
    java.util.Arrays.sort(taken)
    (
      taken(rankIndex(taken.length, 50)),
      taken(rankIndex(taken.length, 95)),
      taken(taken.length - 1),
    )

  private def recorded(samples: Samples, work: Work, mode: Mode, threads: Int): Counts =
    val item = caseIndex(work)
    val modeSlot = modeIndex(mode)
    val width = widthIndex(threads)
    val files = samples.files(item)(modeSlot)(width)
    val passes = samples.passes(item)(modeSlot)(width)
    require(files >= 0L && passes >= 0L, s"${nameOf(work)} missing counters")
    Counts(files, passes)

  private def preamble(): String =
    val runtime = ManagementFactory.getRuntimeMXBean
    val p50Rank = rankIndex(measure, 50) + 1
    val p95Rank = rankIndex(measure, 95) + 1
    "spill bench warmup=" + warmup + " measure=" + measure +
      " batch=" + batchSize + " stress_budget_bytes=" + stressBudgetBytes +
      " default_budget_bytes=" + defaultBudgetBytes +
      " fan_in=" + fanIn + " shuffle=" + shufflePartitions + "\n" +
      "percentile=nearest-rank ceil(p/100*n) no-interpolation n=" + measure +
      " p50_rank=" + p50Rank + " p95_rank=" + p95Rank + " max_rank=" + measure + "\n" +
      "p95 is rank " + p95Rank + " of " + measure + ", so it is the largest sample.\n" +
      "runMain executes in the sbt JVM with Test / fork left false. Absolute numbers carry " +
      "sbt and JIT noise. Comparisons within this run are the signal.\n" +
      "Widths 1 and 4 share one JVM. Off runs before the default budget, then stress. " +
      "The default budget is width 1.\n" +
      "grouped-sum checksum is the folded total. sort-merge checksum mixes (key, left, right) " +
      "and ignores order. Both are compared outside the timer. Sort-merge rows are also checked " +
      "against the generators.\n" +
      "java=" + runtime.getVmName + " " + runtime.getVmVersion

  private def nameOf(work: Work): String =
    work match
      case Grouped => "grouped-sum"
      case SortMerge => "sort-merge"

  private def rowsOf(work: Work): Int =
    work match
      case Grouped => groupedRows
      case SortMerge => sortMergeRows

  private def spillLabel(mode: Mode): String =
    if (spills(mode)) then "on" else "off"

  private def spills(mode: Mode): Boolean =
    mode match
      case Off => false
      case DefaultBudget | Stress => true

  private def budgetLabel(mode: Mode): String =
    mode match
      case Off => "—"
      case DefaultBudget => "64 MiB"
      case Stress => "64 KiB"

  private def modeName(mode: Mode): String =
    mode match
      case Off => "off"
      case DefaultBudget => "default"
      case Stress => "stress"

  private def widthsOf(mode: Mode): Vector[Int] =
    mode match
      case DefaultBudget => Vector(1)
      case Off | Stress => threadCounts

  private def caseIndex(work: Work): Int =
    val index = works.indexOf(work)
    require(index >= 0, s"unknown case $work")
    index

  private def modeIndex(mode: Mode): Int =
    val index = modes.indexOf(mode)
    require(index >= 0, s"unknown mode $mode")
    index

  private def widthIndex(threads: Int): Int =
    val index = threadCounts.indexOf(threads)
    require(index >= 0, s"width $threads is not measured")
    index

  private def millis(nanos: Long): String =
    String.format(Locale.ENGLISH, "%.3f", nanos.toDouble / 1000000.0)

  private def seconds(nanos: Long): String =
    String.format(Locale.ENGLISH, "%.3f", nanos.toDouble / 1000000000.0)
